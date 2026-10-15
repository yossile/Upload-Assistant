from __future__ import annotations

import argparse
import re
import subprocess
import sys
import threading
from contextlib import suppress
from pathlib import Path
from typing import Sequence

from flask import Flask, jsonify, request


PROJECT_ROOT = Path(__file__).resolve().parent
UPLOAD_SCRIPT = PROJECT_ROOT / "upload.py"
TORRENT_HASH_RE = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
ALLOW_CONCURRENT_UPLOADS = True
_upload_lock = threading.Lock()
_active_upload_processes: dict[str, subprocess.Popen[bytes]] = {}

app = Flask(__name__)


def _toggle_upload_process(
    command: list[str], torrent_hash: str
) -> tuple[subprocess.Popen[bytes] | None, str]:
    upload_key = torrent_hash.lower()
    with _upload_lock:
        for key, process in list(_active_upload_processes.items()):
            if process.poll() is not None:
                del _active_upload_processes[key]

        process = _active_upload_processes.get(upload_key)
        if process is not None:
            with suppress(ProcessLookupError):
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            del _active_upload_processes[upload_key]
            return process, "cancelled"

        if not ALLOW_CONCURRENT_UPLOADS and _active_upload_processes:
            return None, "busy"
        process = subprocess.Popen(command, cwd=PROJECT_ROOT)
        _active_upload_processes[upload_key] = process
        return process, "started"


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
        command = [sys.executable, str(UPLOAD_SCRIPT), torrent_hash, "--unattended"]
        if debug:
            command.append("--debug")
        process, status = _toggle_upload_process(command, torrent_hash)
    except OSError:
        app.logger.exception("Could not start or cancel Upload Assistant")
        return jsonify(success=False, error="Could not start or cancel Upload Assistant"), 500
    if process is None:
        return jsonify(success=False, error="An upload is already running"), 409

    return jsonify(success=True, status=status, hash=torrent_hash, pid=process.pid), 200 if status == "cancelled" else 202


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upload a qBittorrent torrent by hash over HTTP.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (default: 8765)")
    args = parser.parse_args(argv)
    app.run(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())