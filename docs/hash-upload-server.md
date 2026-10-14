# Hash Upload Server

Run the standalone server from the repository root. The default bind address is `127.0.0.1`:

```bash
python hash_upload_server.py --port 8765
```

Choose a different bind address with `--host`, for example `0.0.0.0` to listen on all interfaces:

```bash
python hash_upload_server.py --host 0.0.0.0 --port 8765
```

Submit a qBittorrent torrent hash as the `hash` query parameter:

```bash
curl "http://127.0.0.1:8765/upload?hash=0123456789abcdef0123456789abcdef01234567"
```

The server passes the hash as `upload.py`'s positional input and adds `--unattended`. Add `&debug=true` to also pass `--debug`:

```bash
curl "http://127.0.0.1:8765/upload?hash=0123456789abcdef0123456789abcdef01234567&debug=true"
```

Upload Assistant resolves the hash through the configured qBittorrent search client. The request returns HTTP `202` as soon as the process starts, with its PID; it does not wait for the upload to finish. A successful response means the process started, not that the upload eventually succeeded. A process-start failure returns HTTP `500`.

By default, only one upload runs at a time; overlapping requests receive HTTP `409`. Set `ALLOW_CONCURRENT_UPLOADS = True` near the top of `hash_upload_server.py` and restart the server to allow concurrent processes. Concurrent requests are neither queued nor deduplicated.

The server has no authentication. Binding to `0.0.0.0` can expose the upload endpoint to other machines on reachable networks; use it only behind appropriate network access controls.