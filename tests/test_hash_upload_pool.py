# ruff: noqa: S101

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from src.hash_upload_pool import UploadPool

_import_pid = os.getpid()
_job_count = 0


def run() -> None:
    global _job_count
    _job_count += 1
    if sys.argv[1] == "preloaded":
        raise SystemExit(10 if _import_pid != os.getpid() and _job_count == 1 else 1)
    if sys.argv[1] == "block":
        threading.Event().wait()
    if sys.argv[1] == "prompt":
        raise SystemExit(10 if input("Worker answer: ") == "continue" else 1)
    raise SystemExit(int(sys.argv[1]))


def submit_when_ready(pool: UploadPool, arguments: list[str]):
    deadline = time.monotonic() + 10
    with pool._condition:
        while (process := pool.submit(arguments)) is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                pytest.fail("Replacement upload worker did not become ready")
            pool._condition.wait(timeout=remaining)
    return process


@pytest.mark.skipif(os.name != "posix", reason="Warm upload workers need forkserver")
def test_warm_pool_replenishes_isolated_jobs_and_reports_exit_codes() -> None:
    pool = UploadPool(2, uploader_module=__name__)
    try:
        pool.start(timeout=15)
        first = pool.submit(["10"])
        second = pool.submit(["11"])
        assert first is not None and second is not None
        assert first.pid != second.pid
        assert first.wait(timeout=10) == 10
        assert second.wait(timeout=10) == 11
        replacement = submit_when_ready(pool, ["preloaded"])
        assert replacement.pid not in {first.pid, second.pid}
        assert replacement.wait(timeout=10) == 10
        another = submit_when_ready(pool, ["preloaded"])
        assert another.wait(timeout=10) == 10
    finally:
        pool.close()


@pytest.mark.skipif(os.name != "posix", reason="Warm upload workers need forkserver")
def test_worker_waits_for_input_from_server_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    read_fd, write_fd = os.pipe()
    with os.fdopen(read_fd, encoding="utf-8") as stdin, os.fdopen(write_fd, "w", encoding="utf-8") as answers:
        monkeypatch.setattr(sys, "stdin", stdin)
        pool = UploadPool(1, uploader_module=__name__)
        try:
            pool.start(timeout=15)
            process = pool.submit(["prompt"])
            assert process is not None
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.1)
            answers.write("continue\n")
            answers.flush()
            assert process.wait(timeout=10) == 10
            replacement = submit_when_ready(pool, ["prompt"])
            with pytest.raises(subprocess.TimeoutExpired):
                replacement.wait(timeout=0.1)
            answers.write("continue\n")
            answers.flush()
            assert replacement.wait(timeout=10) == 10
        finally:
            pool.close()


@pytest.mark.skipif(os.name != "posix", reason="Warm upload workers need forkserver")
def test_pool_capacity_cancellation_and_shutdown() -> None:
    pool = UploadPool(1, uploader_module=__name__)
    try:
        pool.start(timeout=15)
        process = pool.submit(["block"])
        assert process is not None
        assert pool.submit(["0"]) is None
        with pytest.raises(subprocess.TimeoutExpired):
            process.wait(timeout=0.01)
        process.terminate()
        assert process.wait(timeout=5) == -signal.SIGTERM
        replacement = submit_when_ready(pool, ["block"])
    finally:
        pool.close()
    assert replacement.poll() is not None
    with pytest.raises(OSError, match="stopping"):
        pool.submit(["0"])


@pytest.mark.skipif(os.name != "posix", reason="Warm upload workers need forkserver")
def test_failed_worker_initialization_cleans_up() -> None:
    pool = UploadPool(1, uploader_module="nonexistent_test_uploader_module")
    with pytest.raises(RuntimeError, match="initialization failed"):
        pool.start(timeout=10)
    assert pool._workers == []
    assert pool._thread is not None and not pool._thread.is_alive()


@pytest.mark.skipif(os.name != "posix", reason="Warm upload workers need forkserver")
def test_real_uploader_can_be_preloaded_without_running_an_upload() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from src.hash_upload_pool import UploadPool\n"
            "pool = UploadPool(5)\n"
            "try:\n"
            "    pool.start()\n"
            "    print('warm slots:', len(pool._workers), flush=True)\n"
            "finally:\n"
            "    pool.close()\n",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "warm slots: 5" in result.stdout
