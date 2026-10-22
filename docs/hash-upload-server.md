# Hash Upload Server

Run the standalone server from the repository root. The default bind address is `127.0.0.1`:

```bash
python hash_upload_server.py --port 8765
```

## Warm Upload Workers

By default, the server warms **five upload workers before accepting requests or watching categories**. Python dependencies and uploader modules load once in a separate preload process. Ready workers inherit those cached modules rather than starting a fresh Python interpreter for each torrent.

Each worker handles one upload and then exits. The pool automatically replaces it from the preloaded process, preserving fresh globals, event loops, and cleanup state for every job without repeating the expensive imports. Cancelled or crashed workers are replaced the same way. The HTTP response's PID belongs to the assigned upload worker, not the preload process.

Worker uploads still receive `--unattended`, but inherit the server's stdin just like subprocess uploads. If a tracker still needs input, such as PTP's release-type selection, the worker waits for your answer in the server's terminal. Keep that terminal available for prompts. Without terminal or piped input, interactive prompts cannot be answered.

Change the number of warm slots with `--workers`:

```bash
python hash_upload_server.py --workers 5
python hash_upload_server.py --watch-only --workers 3
```

When all slots are occupied or replacements are still becoming ready, HTTP submissions receive `409` and must be retried. Torrents in category `Upload` are retried on subsequent polls. Requests are not queued. Per-upload work such as configuration reload, update checks, qBittorrent requests, and download waiting still runs for each job. Restart the server after changing application code so the preload process uses the new modules.

Warm workers require Python's `forkserver` support, available on Linux. On platforms without it, or to restore the original subprocess-per-upload behavior, use:

```bash
python hash_upload_server.py --workers 0
```

Worker warm-up is paid once at server startup. Initialization failures stop startup rather than silently falling back to slow uploads. Shutdown also stops idle workers and workers still warming up.

## Category Watching

The server watches the qBittorrent category `Upload` by default. No enabling flag is needed, and tags are never watched or changed:

```bash
python hash_upload_server.py
```

Create the categories `Upload`, `Uploaded`, and `Cleanup` in qBittorrent before starting the server. Only the exact `Upload` category is watched, not subcategories. A confirmed successful upload changes the category to `Uploaded`; a confirmed dupe without a successful upload changes it to `Cleanup`. Failed category updates are retried.

qBittorrent categories can control save paths. Check the categories' save paths and Automatic Torrent Management settings first, since changing category may move torrent data.

To only watch qBittorrent categories without opening an HTTP listener, use `--watch-only`:

```bash
python hash_upload_server.py --watch-only
```

Startup confirmation, category-change cancellation, and shutdown cleanup work in this mode too. It supports `--poll-interval` and `--existing-category`, but cannot be combined with `--no-category-watch`. The `--host` and `--port` settings have no effect in this mode.

The server polls the configured `DEFAULT.default_torrent_client` every five seconds for torrents in category `Upload`. The default client must be a qBittorrent (`qbit`) client. Queries are filtered by category on qBittorrent, rather than fetching all torrents. Each matching hash starts the same unattended upload as an HTTP request, without debug mode.

The `Upload` category controls the upload lifecycle:

- Assigning it starts an upload on the next successful poll.
- Once UA exits after at least one confirmed tracker upload, the server changes the category to `Uploaded`. Failed category changes are retried on subsequent polls.
- Leaving it assigned does not repeat a finished, failed, or manually cancelled upload.
- Changing away from it cancels an active upload, using the same terminate/kill behavior as the HTTP endpoint. Changing it after completion does not start another upload.
- Changing away and assigning it again allows a fresh upload. Both changes must be observed by separate polls.

Only successful polls update category membership; a connection failure never counts as a category change. Failed process starts, failed cancellations, and torrents blocked by single-upload mode are retried on subsequent polls. Removing a torrent from qBittorrent also removes it from the watched results and cancels its active upload. HTTP-only uploads never observed in category `Upload` are not affected by category-based cancellation.

On the first successful poll after startup, the server lists existing torrents in category `Upload` and asks whether to start them. The default answer is **no**. Declined hashes stay ignored until their category changes away and back; newly assigned torrents are still processed automatically. Without an interactive terminal, existing torrents are skipped unless explicitly enabled:

```bash
python hash_upload_server.py --existing-category start
python hash_upload_server.py --existing-category skip
```

Only server children opt in via `UA_REPORT_UPLOAD_SUCCESS=1`: exit code `10` means a successful upload (`Uploaded`), while `11` means a confirmed dupe without a successful upload (`Cleanup`). Both replace category `Upload`, retrying failed category changes. Normal CLI exits are unchanged. Skips, debug runs, cancellations, and other failures leave categories unchanged.

Categories serve as the restart state; no separate history file is written. Restarting clears in-memory tracking and asks again about existing torrents in category `Upload`. Consequently, starting existing torrents can resubmit uploads whose completion category was not updated before shutdown, as well as interrupted ones. Normal shutdown, Ctrl+C, and `SIGTERM` stop tracked uploads. `SIGKILL`, crashes, and power loss cannot run cleanup; killing only the server process does not guarantee its children stop, so ensure old uploads have stopped before starting existing torrents again.

Change the polling interval with `--poll-interval`, or disable automatic category-based submissions and cancellations with `--no-category-watch`. Completion category updates for HTTP-started uploads remain enabled:

```bash
python hash_upload_server.py --poll-interval 10
python hash_upload_server.py --no-category-watch
```

The interval is the wait between completed polling passes, not a deadline for detecting a category change. Slow qBittorrent requests, completion category updates, or cancellations can extend it. The startup confirmation prompt also pauses polling until answered. The server logs `Started upload for categorized torrent <hash>` immediately after dispatching each watched upload. With warm workers, module imports have already finished; per-upload initialization and hash resolution still follow. Use the server's launch message to distinguish polling delays from uploader startup delays.

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

Upload Assistant resolves the hash through the configured qBittorrent search client. The request returns HTTP `202` as soon as a worker accepts the job (or a subprocess starts with `--workers 0`), with its PID; it does not wait for the upload to finish. A successful response means the upload was dispatched, not that it eventually succeeded. Dispatch failures return HTTP `500`.

Send a request with the same hash again while its upload is running to cancel it. Requests match only by hash (case-insensitive), regardless of debug mode. The `debug` parameter only affects logging when starting an upload. Cancellation returns HTTP `200` after the process stops:

```json
{"success": true, "status": "cancelled", "hash": "0123456789abcdef0123456789abcdef01234567", "pid": 1234}
```

The server terminates the upload process and forcibly kills it if it does not exit within five seconds. Cancellation stops the running process; it does not undo uploads already submitted to trackers. Sending the request after cancellation or after the process has finished starts a new upload. Process-start and cancellation failures return HTTP `500`.

Concurrent uploads are enabled by default, limited to five warm slots unless `--workers` changes that limit. With `--workers 0`, there is no pool-imposed concurrency limit. Set `ALLOW_CONCURRENT_UPLOADS = False` near the top of `hash_upload_server.py` and restart the server to allow only one upload at a time. In that mode, a different request receives HTTP `409` while an upload is running, but a matching request still cancels it. Requests are not queued. Active uploads are tracked only by the server process that started them.

The server has no authentication. Binding to `0.0.0.0` can expose the upload endpoint to other machines on reachable networks; use it only behind appropriate network access controls.