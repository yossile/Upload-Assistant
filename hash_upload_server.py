from __future__ import annotations

import argparse
import re
import subprocess
import sys
import threading
from pathlib import Path
from typing import Sequence

from flask import Flask, jsonify, request


PROJECT_ROOT = Path(__file__).resolve().parent
UPLOAD_SCRIPT = PROJECT_ROOT / "upload.py"
TORRENT_HASH_RE = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
ALLOW_CONCURRENT_UPLOADS = True
_upload_lock = threading.Lock()
_active_upload_process: subprocess.Popen[bytes] | None = None

app = Flask(__name__)


def _start_upload_process(command: list[str]) -> subprocess.Popen[bytes] | None:
    global _active_upload_process
    if ALLOW_CONCURRENT_UPLOADS:
        return subprocess.Popen(command, cwd=PROJECT_ROOT)

    with _upload_lock:
        if _active_upload_process is not None and _active_upload_process.poll() is None:
            return None
        _active_upload_process = subprocess.Popen(command, cwd=PROJECT_ROOT)
        return _active_upload_process


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
        process = _start_upload_process(command)
    except OSError:
        app.logger.exception("Could not start Upload Assistant")
        return jsonify(success=False, error="Could not start Upload Assistant"), 500
    if process is None:
        return jsonify(success=False, error="An upload is already running"), 409

    return jsonify(success=True, status="started", hash=torrent_hash, pid=process.pid), 202


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upload a qBittorrent torrent by hash over HTTP.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (default: 8765)")
    args = parser.parse_args(argv)
    app.run(host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())