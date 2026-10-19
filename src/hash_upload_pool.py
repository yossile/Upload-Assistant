from __future__ import annotations

import logging
import multiprocessing
import os
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import import_module
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path

logger = logging.getLogger(__name__)


def _worker_main(connection: Connection, uploader_module: str) -> None:
    try:
        uploader = import_module(uploader_module)
        connection.send("ready")
        arguments = connection.recv()
        connection.close()
        os.environ["UA_REPORT_UPLOAD_SUCCESS"] = "1"
        project_root = Path(__file__).resolve().parent.parent
        os.chdir(project_root)
        sys.argv = [str(project_root / "upload.py"), *arguments]
        uploader._startup_started = time.monotonic()
        report_startup = getattr(uploader, "_report_hash_upload_startup", None)
        if report_startup is not None:
            report_startup("Using preloaded upload worker")
        uploader.run()
    except EOFError:
        return
    finally:
        connection.close()


class UploadProcess:
    def __init__(self, process: BaseProcess) -> None:
        self.pid = process.pid
        self._process = process
        self._returncode: int | None = None
        self._lock = threading.Lock()

    def poll(self) -> int | None:
        with self._lock:
            if self._returncode is None:
                self._returncode = self._process.exitcode
                if self._returncode is not None:
                    self._process.join()
                    self._process.close()
            return self._returncode

    def terminate(self) -> None:
        with self._lock:
            if self._returncode is None:
                self._process.terminate()

    def kill(self) -> None:
        with self._lock:
            if self._returncode is None:
                self._process.kill()

    def wait(self, timeout: float | None = None) -> int:
        deadline = None if timeout is None else time.monotonic() + timeout
        while (returncode := self.poll()) is None:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            if remaining == 0:
                raise subprocess.TimeoutExpired(f"upload worker {self.pid}", timeout)
            with self._lock:
                if self._returncode is None:
                    self._process.join(timeout=remaining)
        return returncode


@dataclass
class _Worker:
    process: UploadProcess
    connection: Connection
    ready: bool = False
    busy: bool = False


class UploadPool:
    def __init__(self, size: int = 5, *, uploader_module: str = "upload") -> None:
        if size < 1:
            raise ValueError("Upload worker count must be greater than zero")
        if "forkserver" not in multiprocessing.get_all_start_methods():
            raise ValueError("Warm upload workers require forkserver support; use --workers 0 on this platform")
        self.size = size
        self._uploader_module = uploader_module
        self._context = multiprocessing.get_context("forkserver")
        self._context.set_forkserver_preload([uploader_module])
        self._workers: list[_Worker] = []
        self._condition = threading.Condition()
        self._stopping = False
        self._thread: threading.Thread | None = None
        self._failure: BaseException | None = None

    def _spawn(self) -> None:
        parent, child = self._context.Pipe()
        process = self._context.Process(target=_worker_main, args=(child, self._uploader_module))
        try:
            process.start()
        except BaseException:
            parent.close()
            process.close()
            raise
        finally:
            child.close()
        self._workers.append(_Worker(UploadProcess(process), parent))

    def start(self, timeout: float = 120.0) -> None:
        try:
            with self._condition:
                for _ in range(self.size):
                    self._spawn()
                self._thread = threading.Thread(target=self._maintain, name="hash-upload-pool", daemon=True)
                self._thread.start()
                ready = self._condition.wait_for(
                    lambda: self._failure is not None or all(worker.ready for worker in self._workers), timeout=timeout
                )
                if self._failure is not None:
                    raise RuntimeError("Upload worker initialization failed") from self._failure
                if not ready:
                    raise TimeoutError("Timed out warming upload workers")
        except BaseException:
            self.close()
            raise

    def _maintain(self) -> None:
        with self._condition:
            while not self._stopping:
                try:
                    for worker in list(self._workers):
                        returncode = worker.process.poll()
                        if returncode is not None:
                            worker.connection.close()
                            self._workers.remove(worker)
                            if not worker.ready:
                                raise RuntimeError(f"Upload worker exited during initialization (code {returncode})")
                            self._spawn()
                        elif not worker.ready and worker.connection.poll():
                            if worker.connection.recv() != "ready":
                                raise RuntimeError("Invalid upload worker readiness response")
                            worker.ready = True
                    self._condition.notify_all()
                except Exception as exc:
                    self._failure = exc
                    logger.exception("Could not maintain warm upload workers")
                    self._condition.notify_all()
                    return
                self._condition.wait(timeout=0.1)

    def submit(self, arguments: Sequence[str]) -> UploadProcess | None:
        with self._condition:
            if self._stopping:
                raise OSError("Upload worker pool is stopping")
            if self._failure is not None:
                raise OSError("Upload worker pool is unavailable") from self._failure
            for worker in self._workers:
                if worker.ready and not worker.busy and worker.process.poll() is None:
                    worker.connection.send(list(arguments))
                    worker.busy = True
                    self._condition.notify_all()
                    return worker.process
            return None

    def close(self) -> None:
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join()
        for worker in self._workers:
            worker.connection.close()
            if worker.process.poll() is None:
                worker.process.terminate()
                try:
                    worker.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    worker.process.kill()
                    worker.process.wait()
        self._workers.clear()
