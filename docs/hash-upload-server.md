# Hash Upload Server

Run the standalone server from the repository root. The default bind address is `127.0.0.1`:

```bash
python hash_upload_server.py --port 8765
```

To only watch qBittorrent tags without opening an HTTP listener, use `--watch-only`:

```bash
python hash_upload_server.py --watch-only
```

Startup confirmation, tag-removal cancellation, and shutdown cleanup work in this mode too. It supports `--poll-interval` and `--existing-tagged`, but cannot be combined with `--no-tag-watch`. The `--host` and `--port` settings have no effect in this mode.

The server also polls the configured `DEFAULT.default_torrent_client` every five seconds for torrents tagged exactly `Upload`. The default client must be a qBittorrent (`qbit`) client. Queries are filtered by tag on qBittorrent, rather than fetching all torrents. Each matching hash starts the same unattended upload as an HTTP request, without debug mode.

The `Upload` tag controls the upload lifecycle:

- Adding it starts an upload on the next successful poll.
- Once UA exits after at least one confirmed tracker upload, the server adds `Uploaded` and removes `Upload`. Other tags are preserved. Failed tag changes are retried on subsequent polls.
- Leaving it in place does not repeat a finished, failed, or manually cancelled upload.
- Removing it cancels an active upload, using the same terminate/kill behavior as the HTTP endpoint. Removing it after completion does not start another upload.
- Removing and re-adding it allows a fresh upload. Both changes must be observed by separate polls.

Only successful polls update tag membership; a connection failure never counts as tag removal. Failed process starts, failed cancellations, and torrents blocked by single-upload mode are retried on subsequent polls. Removing a torrent from qBittorrent also removes it from the tagged results and cancels its active upload. HTTP-only uploads whose tags have never been observed are not affected.

On the first successful poll after startup, the server lists existing tagged torrents and asks whether to start them. The default answer is **no**. Declined hashes stay ignored until their tags are removed and re-added; newly tagged torrents are still processed automatically. Without an interactive terminal, existing tags are skipped unless explicitly enabled:

```bash
python hash_upload_server.py --existing-tagged start
python hash_upload_server.py --existing-tagged skip
```

Only server children opt in via `UA_REPORT_UPLOAD_SUCCESS=1`: exit code `10` means a successful upload (`Uploaded`), while `11` means a confirmed dupe without a successful upload (`Cleanup`). Both replace `Upload`, preserving other tags and retrying failed tag changes. Normal CLI exits are unchanged. Skips, debug runs, cancellations, and other failures leave tags unchanged.

Tags serve as the restart state; no separate history file is written. Restarting clears in-memory tracking and asks again about existing `Upload` tags. Consequently, starting existing tags can resubmit uploads whose completion was not retagged before shutdown, as well as interrupted ones. Normal shutdown, Ctrl+C, and `SIGTERM` stop tracked uploads. `SIGKILL`, crashes, and power loss cannot run cleanup; killing only the server process does not guarantee its children stop, so ensure old uploads have stopped before starting existing tags again.

Change the polling interval with `--poll-interval`, or disable automatic tag-based submissions and cancellations with `--no-tag-watch`. Completion retagging for HTTP-started uploads remains enabled:

```bash
python hash_upload_server.py --poll-interval 10
python hash_upload_server.py --no-tag-watch
```

The interval is the wait between completed polling passes, not a deadline for detecting a tag. Slow qBittorrent requests, completion retagging, or cancellations can extend it. The startup confirmation prompt also pauses polling until answered. The server logs `Started upload for tagged torrent <hash>` immediately after launching each tagged upload; the child uploader's own progress may appear later while it initializes and resolves the hash. Use the server's launch message to distinguish polling delays from uploader startup delays.

If the default client is not qBittorrent or watcher initialization fails, HTTP uploads remain available. Temporary polling failures are retried on the next interval.

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