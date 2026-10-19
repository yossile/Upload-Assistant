from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import signal
import subprocess
import sys
import threading
from collections.abc import Sequence
from contextlib import suppress
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from flask import Flask, jsonify, request

from src.hash_upload_pool import UploadPool, UploadProcess

if TYPE_CHECKING:
    from qbittorrentapi import Client

PROJECT_ROOT = Path(__file__).resolve().parent
UPLOAD_SCRIPT = PROJECT_ROOT / "upload.py"
TORRENT_HASH_RE = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
ALLOW_CONCURRENT_UPLOADS = True
UploadHandle = subprocess.Popen[bytes] | UploadProcess
_upload_lock = threading.Lock()
_upload_pool: UploadPool | None = None
_active_upload_processes: dict[str, UploadHandle] = {}
_submitted_upload_hashes: set[str] = set()
_tagged_upload_hashes: set[str] = set()
_completed_upload_hashes: dict[str, str] = {}
_server_stopping = threading.Event()

app = Flask(__name__)


def _collect_completed_uploads() -> None:
    for torrent_hash, process in list(_active_upload_processes.items()):
        returncode = process.poll()
        if returncode is not None:
            del _active_upload_processes[torrent_hash]
            if returncode in (10, 11):
                _completed_upload_hashes[torrent_hash] = "Uploaded" if returncode == 10 else "Cleanup"


def _retag_completed_uploads(qbt_client: Client) -> None:
    with _upload_lock:
        if _server_stopping.is_set():
            return
        _collect_completed_uploads()
        for torrent_hash, tag in sorted(_completed_upload_hashes.items()):
            try:
                qbt_client.torrents_add_tags(torrent_hashes=torrent_hash, tags=tag)
                qbt_client.torrents_remove_tags(torrent_hashes=torrent_hash, tags="Upload")
            except Exception:
                app.logger.warning("Could not update tags for completed torrent %s; will retry", torrent_hash)
                continue
            _completed_upload_hashes.pop(torrent_hash, None)
            _tagged_upload_hashes.add(torrent_hash)
            app.logger.info("Replaced Upload tag with %s for %s", tag, torrent_hash)


def _stop_upload_process(process: UploadHandle) -> None:
    with suppress(ProcessLookupError):
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _toggle_upload_process(
    command: list[str], torrent_hash: str, *, cancel_running: bool = True
) -> tuple[UploadHandle | None, str]:
    upload_key = torrent_hash.lower()
    with _upload_lock:
        if _server_stopping.is_set():
            return None, "stopping"
        if not cancel_running and upload_key in _submitted_upload_hashes:
            return None, "submitted"
        _collect_completed_uploads()

        process = _active_upload_processes.get(upload_key)
        if process is not None:
            if not cancel_running:
                return process, "running"
            _stop_upload_process(process)
            del _active_upload_processes[upload_key]
            return process, "cancelled"

        if not ALLOW_CONCURRENT_UPLOADS and _active_upload_processes:
            return None, "busy"
        if _upload_pool is None:
            process = subprocess.Popen(command, cwd=PROJECT_ROOT, env={**os.environ, "UA_REPORT_UPLOAD_SUCCESS": "1"})
        else:
            process = _upload_pool.submit(command[2:])
            if process is None:
                return None, "busy"
        _completed_upload_hashes.pop(upload_key, None)
        _active_upload_processes[upload_key] = process
        _submitted_upload_hashes.add(upload_key)
        return process, "started"


def _upload_command(torrent_hash: str, *, debug: bool = False) -> list[str]:
    command = [sys.executable, str(UPLOAD_SCRIPT), torrent_hash, "--unattended"]
    if debug:
        command.append("--debug")
    return command


def _shutdown_uploads() -> None:
    with _upload_lock:
        for torrent_hash, process in list(_active_upload_processes.items()):
            try:
                if process.poll() is None:
                    _stop_upload_process(process)
                del _active_upload_processes[torrent_hash]
            except OSError:
                app.logger.exception("Could not stop upload %s during shutdown", torrent_hash)
        _completed_upload_hashes.clear()


def _handle_shutdown(_signum: int, _frame: object) -> None:
    raise SystemExit(0)


def _confirm_existing_tagged(torrents: dict[str, Any], policy: str) -> bool:
    if policy != "ask":
        return policy == "start"
    if not sys.stdin.isatty():
        app.logger.warning("Skipping existing Upload tags without a terminal; use --existing-tagged start to submit them")
        return False
    listing = "\n".join(f"  {getattr(torrent, 'name', torrent_hash)} ({torrent_hash})" for torrent_hash, torrent in torrents.items())
    try:
        answer = input(f"Found {len(torrents)} torrents already tagged Upload:\n{listing}\nStart these uploads? [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in {"y", "yes"}


def _poll_upload_tag(qbt_client: Client, *, existing_tagged: str | None = None) -> None:
    _retag_completed_uploads(qbt_client)
    torrents = {
        torrent.hash.lower(): torrent
        for torrent in qbt_client.torrents_info(tag="Upload")
        if isinstance(torrent.hash, str) and TORRENT_HASH_RE.fullmatch(torrent.hash)
    }
    skip_existing = bool(torrents) and existing_tagged is not None and not _confirm_existing_tagged(torrents, existing_tagged)
    with _upload_lock:
        for torrent_hash in sorted(_tagged_upload_hashes - torrents.keys()):
            process = _active_upload_processes.get(torrent_hash)
            try:
                if process is not None:
                    if process.poll() is None:
                        _stop_upload_process(process)
                    del _active_upload_processes[torrent_hash]
            except OSError:
                app.logger.exception("Could not cancel untagged torrent %s; will retry", torrent_hash)
                continue
            _submitted_upload_hashes.discard(torrent_hash)
            _tagged_upload_hashes.discard(torrent_hash)
        _tagged_upload_hashes.update(torrents)
        if skip_existing:
            _submitted_upload_hashes.update(torrents)

    for torrent_hash in torrents:
        try:
            _, status = _toggle_upload_process(_upload_command(torrent_hash), torrent_hash, cancel_running=False)
        except OSError:
            app.logger.exception("Could not start tagged torrent upload")
            continue
        if status == "started":
            app.logger.info("Started upload for tagged torrent %s", torrent_hash)


async def _watch_upload_tag(stop: threading.Event, poll_interval: int, existing_tagged: str = "ask", watch_tags: bool = True) -> None:
    from src.clients import Clients

    config = cast(dict[str, Any], import_module("data.config").config)
    client_name = config.get("DEFAULT", {}).get("default_torrent_client")
    client_config = config.get("TORRENT_CLIENTS", {}).get(client_name, {})
    if str(client_config.get("torrent_client", "")).lower() != "qbit":
        app.logger.warning("Upload tag watcher disabled: default torrent client is not qBittorrent")
        return

    if watch_tags:
        print(f"Watching qBittorrent client '{client_name}' for tag 'Upload' (polling every {poll_interval}s).", flush=True)
    clients = Clients(config)
    qbt_client = None
    startup_pending = True
    while not stop.is_set():
        try:
            if qbt_client is None:
                qbt_client = await clients.init_qbittorrent_client(client_config)
            if qbt_client is not None and not stop.is_set():
                if watch_tags:
                    _poll_upload_tag(qbt_client, existing_tagged=existing_tagged if startup_pending else None)
                    startup_pending = False
                else:
                    _retag_completed_uploads(qbt_client)
        except Exception:
            app.logger.warning("Could not poll qBittorrent Upload tag; will retry")
        stop.wait(poll_interval)


def _run_tag_watcher(stop: threading.Event, poll_interval: int, existing_tagged: str = "ask", watch_tags: bool = True) -> None:
    try:
        asyncio.run(_watch_upload_tag(stop, poll_interval, existing_tagged, watch_tags))
    except Exception:
        app.logger.warning("Upload tag watcher could not initialize")


@app.get("/upload")
def upload_hash():
    hashes = request.args.getlist("hash")
    if len(hashes) != 1 or not TORRENT_HASH_RE.fullmatch(hashes[0]):
        return jsonify(success=False, error="Provide one 40- or 64-character hexadecimal hash"), 400
    debug_values = request.args.getlist("debug")
    if len(debug_values) > 1 or (debug_values and debug_values[0].lower() not in {"true", "false"}):
        return jsonify(success=False, error="debug must be true or false"), 400

    torrent_hash = hashes[0]
    debug = bool(debug_values) and debug_values[0].lower() == "true"
    try:
        command = _upload_command(torrent_hash, debug=debug)
        process, status = _toggle_upload_process(command, torrent_hash)
    except OSError:
        app.logger.exception("Could not start or cancel Upload Assistant")
        return jsonify(success=False, error="Could not start or cancel Upload Assistant"), 500
    if process is None:
        if status == "busy" and _upload_pool is not None and ALLOW_CONCURRENT_UPLOADS:
            return jsonify(success=False, error="All upload workers are busy; retry later"), 409
        return jsonify(success=False, error="An upload is already running"), 409

    return jsonify(success=True, status=status, hash=torrent_hash, pid=process.pid), 200 if status == "cancelled" else 202


def _start_upload_pool(workers: int) -> UploadPool | None:
    if workers == 0:
        return None
    from src.app_paths import CONFIG_PATH, bundled_example_config_path, ensure_legacy_config_absent, ensure_user_config
    from src.config_sync import ConfigSyncError, sync_user_config

    ensure_legacy_config_absent()
    if ensure_user_config():
        raise OSError(f"Configuration created at {CONFIG_PATH}; configure it before starting upload workers")
    try:
        result = sync_user_config(CONFIG_PATH, bundled_example_config_path())
        if result.changed:
            app.logger.info("Configuration updated with %s new settings; backup: %s", len(result.added_paths), result.backup_path)
    except (ConfigSyncError, OSError) as exc:
        app.logger.warning("Configuration was not automatically updated: %s", exc)
    pool = UploadPool(workers)
    app.logger.info("Warming %s upload workers; Python modules are loaded once in the preload process", workers)
    pool.start()
    app.logger.info("%s upload workers ready", workers)
    return pool


def main(argv: Sequence[str] | None = None) -> int:
    global _upload_pool
    parser = argparse.ArgumentParser(description="Upload qBittorrent torrents by hash over HTTP or by watching tags.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (default: 8765)")
    parser.add_argument("--poll-interval", type=int, default=5, help="Seconds between Upload tag checks (default: 5)")
    parser.add_argument("--workers", type=int, default=5, help="Warm upload slots (default: 5; 0 uses a new Python interpreter per upload)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--no-tag-watch", action="store_true", help="Disable polling the default qBittorrent client for the Upload tag")
    mode.add_argument("--watch-only", action="store_true", help="Watch Upload tags without starting the HTTP listener")
    parser.add_argument("--existing-tagged", choices=("ask", "start", "skip"), default="ask", help="Handle existing Upload tags at startup (default: ask; skip without a terminal)")
    args = parser.parse_args(argv)
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be greater than zero")
    if args.workers < 0:
        parser.error("--workers must be zero or greater")
    app.logger.setLevel(logging.INFO)
    _server_stopping.clear()
    stop = threading.Event()
    watcher = None
    previous_sigterm = signal.signal(signal.SIGTERM, _handle_shutdown)
    try:
        try:
            _upload_pool = _start_upload_pool(args.workers)
        except (OSError, RuntimeError, ValueError) as exc:
            app.logger.error("Could not start upload workers: %s", exc)
            return 1
        if args.watch_only:
            _run_tag_watcher(stop, args.poll_interval, args.existing_tagged)
        else:
            watcher_args = (stop, args.poll_interval, args.existing_tagged) if not args.no_tag_watch else (stop, args.poll_interval, args.existing_tagged, False)
            watcher = threading.Thread(target=_run_tag_watcher, args=watcher_args, daemon=True)
            watcher.start()
            app.run(host=args.host, port=args.port)
    except KeyboardInterrupt:
        pass
    finally:
        _server_stopping.set()
        stop.set()
        if watcher is not None:
            watcher.join(timeout=5)
        _shutdown_uploads()
        if _upload_pool is not None:
            _upload_pool.close()
            _upload_pool = None
        signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


if __name__ == "__main__":
    sys.exit(main())
