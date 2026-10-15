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

Send a request with the same hash again while its upload is running to cancel it. Requests match only by hash (case-insensitive), regardless of debug mode. The `debug` parameter only affects logging when starting an upload. Cancellation returns HTTP `200` after the process stops:

```json
{"success": true, "status": "cancelled", "hash": "0123456789abcdef0123456789abcdef01234567", "pid": 1234}
```

The server terminates the upload process and forcibly kills it if it does not exit within five seconds. Cancellation stops the running process; it does not undo uploads already submitted to trackers. Sending the request after cancellation or after the process has finished starts a new upload. Process-start and cancellation failures return HTTP `500`.

Concurrent uploads are enabled by default. Set `ALLOW_CONCURRENT_UPLOADS = False` near the top of `hash_upload_server.py` and restart the server to allow only one upload at a time. In that mode, a different request receives HTTP `409` while an upload is running, but a matching request still cancels it. Requests are not queued. Active uploads are tracked only by the server process that started them.

The server has no authentication. Binding to `0.0.0.0` can expose the upload endpoint to other machines on reachable networks; use it only behind appropriate network access controls.