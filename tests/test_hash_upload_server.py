from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

import hash_upload_server as server


@pytest.fixture(autouse=True)
def reset_upload_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "_active_upload_processes", {})
    monkeypatch.setattr(server, "_submitted_upload_hashes", set())
    monkeypatch.setattr(server, "_tagged_upload_hashes", set())
    monkeypatch.setattr(server, "_completed_upload_hashes", {})
    monkeypatch.setattr(server, "_server_stopping", server.threading.Event())


@pytest.mark.parametrize("allow_concurrent", [True, False])
def test_repeated_request_cancels_upload(monkeypatch: pytest.MonkeyPatch, allow_concurrent: bool) -> None:
    calls = []
    process = SimpleNamespace(
        pid=1234,
        poll=lambda: None,
        terminate=lambda: calls.append("terminate"),
        wait=lambda **_kwargs: calls.append("wait"),
    )
    monkeypatch.setattr(server, "ALLOW_CONCURRENT_UPLOADS", allow_concurrent)
    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: (calls.append("start") or process))
    client = server.app.test_client()
    url = f"/upload?hash={'a' * 40}&debug=true"

    assert client.get(url).status_code == 202
    response = client.get(url)

    assert response.status_code == 200
    assert response.json == {"success": True, "status": "cancelled", "hash": "a" * 40, "pid": 1234}
    assert calls == ["start", "terminate", "wait"]
    assert client.get(url).status_code == 202
    assert calls[-1] == "start"


def test_cancellation_kills_process_that_ignores_termination(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    process.wait.side_effect = [server.subprocess.TimeoutExpired("upload", 5), 0]
    monkeypatch.setattr(server.subprocess, "Popen", Mock(return_value=process))
    client = server.app.test_client()
    url = f"/upload?hash={'a' * 40}"

    assert client.get(url).status_code == 202
    assert client.get(url).status_code == 200

    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_count == 2
    assert not server._active_upload_processes


def test_cancellation_failure_keeps_process_tracked(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    process.terminate.side_effect = OSError("permission denied")
    monkeypatch.setattr(server.subprocess, "Popen", Mock(return_value=process))
    client = server.app.test_client()
    url = f"/upload?hash={'a' * 40}"

    assert client.get(url).status_code == 202
    assert client.get(url).status_code == 500
    assert server._active_upload_processes["a" * 40] is process


def test_completed_upload_starts_again_instead_of_cancelling(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = 0
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    client = server.app.test_client()
    url = f"/upload?hash={'a' * 40}"

    assert client.get(url).status_code == 202
    assert client.get(url).status_code == 202
    assert popen.call_count == 2
    process.terminate.assert_not_called()


def test_request_matching_normalizes_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    client = server.app.test_client()

    assert client.get(f"/upload?hash={'a' * 40}").status_code == 202
    assert client.get(f"/upload?debug=FALSE&hash={'A' * 40}").status_code == 200
    popen.assert_called_once()


@pytest.mark.parametrize("allow_concurrent", [True, False])
@pytest.mark.parametrize(
    ("start_debug", "cancel_debug"),
    [("", "&debug=true"), ("&debug=true", ""), ("&debug=true", "&debug=false"), ("&debug=false", "&debug=true")],
)
def test_cancellation_matches_hash_regardless_of_debug(
    monkeypatch: pytest.MonkeyPatch, allow_concurrent: bool, start_debug: str, cancel_debug: str
) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    monkeypatch.setattr(server, "ALLOW_CONCURRENT_UPLOADS", allow_concurrent)
    client = server.app.test_client()
    url = f"/upload?hash={'a' * 40}"

    assert client.get(f"{url}{start_debug}").status_code == 202
    response = client.get(f"{url}{cancel_debug}")

    assert response.status_code == 200
    assert response.json["pid"] == 1234
    popen.assert_called_once()
    process.terminate.assert_called_once_with()


def test_simultaneous_matching_requests_start_then_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    ready = Barrier(2)
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)

    def send_request():
        with server.app.test_client() as client:
            ready.wait(timeout=3)
            return client.get(f"/upload?hash={'a' * 40}")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(send_request) for _ in range(2)]
        responses = [future.result(timeout=5) for future in futures]

    assert sorted(response.status_code for response in responses) == [200, 202]
    popen.assert_called_once()
    process.terminate.assert_called_once_with()


def test_main_passes_host_and_port_to_server(monkeypatch: pytest.MonkeyPatch) -> None:
    run_args = []
    monkeypatch.setattr(server.app, "run", lambda **kwargs: run_args.append(kwargs))

    assert server.main(["--host", "0.0.0.0", "--port", "9000", "--no-tag-watch"]) == 0

    assert run_args == [{"host": "0.0.0.0", "port": 9000}]


def test_main_enables_upload_start_logs(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(server.app.logger, "level", logging.NOTSET)
    monkeypatch.setattr(logging.getLogger(), "level", logging.WARNING)
    monkeypatch.setattr(server, "_run_tag_watcher", Mock())
    client = Mock()
    client.torrents_info.return_value = [SimpleNamespace(hash="a" * 40)]
    process = Mock(pid=1234)
    process.poll.return_value = None
    monkeypatch.setattr(server.subprocess, "Popen", Mock(return_value=process))
    monkeypatch.setattr(server.app, "run", lambda **_kwargs: server._poll_upload_tag(client))

    assert server.main(["--no-tag-watch"]) == 0

    assert server.app.logger.isEnabledFor(logging.INFO)
    assert f"Started upload for tagged torrent {'a' * 40}" in caplog.text


@pytest.mark.parametrize("interrupt", [False, True])
def test_watch_only_runs_without_http_and_cleans_up(monkeypatch: pytest.MonkeyPatch, interrupt: bool) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    server._active_upload_processes["a" * 40] = process
    watch = Mock(side_effect=KeyboardInterrupt if interrupt else None)
    run = Mock()
    thread_factory = Mock()
    monkeypatch.setattr(server, "_run_tag_watcher", watch)
    monkeypatch.setattr(server.app, "run", run)
    monkeypatch.setattr(server.threading, "Thread", thread_factory)
    previous_sigterm = server.signal.getsignal(server.signal.SIGTERM)

    assert server.main(["--watch-only", "--poll-interval", "10", "--existing-tagged", "skip"]) == 0

    watch.assert_called_once()
    stop, interval, policy = watch.call_args.args
    assert stop.is_set()
    assert interval == 10
    assert policy == "skip"
    run.assert_not_called()
    thread_factory.assert_not_called()
    process.terminate.assert_called_once_with()
    assert not server._active_upload_processes
    assert server.signal.getsignal(server.signal.SIGTERM) == previous_sigterm


def test_watch_only_rejects_disabled_tag_watcher(monkeypatch: pytest.MonkeyPatch) -> None:
    run = Mock()
    watch = Mock()
    monkeypatch.setattr(server.app, "run", run)
    monkeypatch.setattr(server, "_run_tag_watcher", watch)

    with pytest.raises(SystemExit) as error:
        server.main(["--watch-only", "--no-tag-watch"])

    assert error.value.code == 2
    run.assert_not_called()
    watch.assert_not_called()


def test_hash_endpoint_runs_upload_with_positional_hash_and_unattended(monkeypatch: pytest.MonkeyPatch) -> None:
    torrent_hash = "a" * 40
    calls = []

    def fake_popen(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(pid=1234, poll=lambda: None)

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    response = server.app.test_client().get(f"/upload?hash={torrent_hash}")

    assert response.status_code == 202
    assert response.json == {"success": True, "status": "started", "hash": torrent_hash, "pid": 1234}
    assert calls == [([server.sys.executable, str(server.UPLOAD_SCRIPT), torrent_hash, "--unattended"], {"cwd": server.PROJECT_ROOT, "env": ANY})]
    assert calls[0][1]["env"]["UA_REPORT_UPLOAD_SUCCESS"] == "1"
    assert "--torrenthash" not in calls[0][0]


@pytest.mark.parametrize("debug_value", ["true", "TRUE"])
def test_hash_endpoint_adds_debug_flag_when_requested(monkeypatch: pytest.MonkeyPatch, debug_value: str) -> None:
    torrent_hash = "b" * 40
    calls = []
    monkeypatch.setattr(
        server.subprocess,
        "Popen",
        lambda command, **_kwargs: (calls.append(command) or SimpleNamespace(pid=1234, poll=lambda: None)),
    )

    response = server.app.test_client().get(f"/upload?hash={torrent_hash}&debug={debug_value}")

    assert response.status_code == 202
    assert calls == [[server.sys.executable, str(server.UPLOAD_SCRIPT), torrent_hash, "--unattended", "--debug"]]


@pytest.mark.parametrize("query", ["", "?hash=bad", f"?hash={'a' * 40}&hash={'b' * 40}"])
def test_hash_endpoint_rejects_invalid_hashes_without_running_upload(monkeypatch: pytest.MonkeyPatch, query: str) -> None:
    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("Upload must not start"))

    response = server.app.test_client().get(f"/upload{query}")

    assert response.status_code == 400
    assert response.json["success"] is False


@pytest.mark.parametrize("debug", ["?debug=maybe", "?debug=true&debug=false"])
def test_hash_endpoint_rejects_invalid_debug_values(monkeypatch: pytest.MonkeyPatch, debug: str) -> None:
    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("Upload must not start"))

    response = server.app.test_client().get(f"/upload?hash={'a' * 40}{debug}")

    assert response.status_code == 400
    assert response.json["success"] is False


def test_hash_endpoint_reports_process_start_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_start(*_args, **_kwargs):
        raise OSError("spawn failed")

    monkeypatch.setattr(server.subprocess, "Popen", fail_to_start)

    response = server.app.test_client().get(f"/upload?hash={'c' * 64}")

    assert response.status_code == 500
    assert response.json == {"success": False, "error": "Could not start or cancel Upload Assistant"}


def test_hash_endpoint_starts_concurrent_processes_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    both_started = Barrier(2)
    commands = []

    def fake_popen(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(pid=1000 + len(commands), poll=lambda: None)

    def send_request(torrent_hash: str):
        with server.app.test_client() as client:
            both_started.wait(timeout=3)
            return client.get(f"/upload?hash={torrent_hash}")

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(server, "ALLOW_CONCURRENT_UPLOADS", True)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(send_request, "d" * 40)
        second = executor.submit(send_request, "e" * 40)
        responses = [first.result(timeout=5), second.result(timeout=5)]

    assert [response.status_code for response in responses] == [202, 202]
    assert len(commands) == 2


def test_hash_endpoint_rejects_overlapping_processes_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    process = SimpleNamespace(pid=4321, poll=lambda: None)
    commands = []

    def fake_popen(command, **_kwargs):
        commands.append(command)
        return process

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(server, "ALLOW_CONCURRENT_UPLOADS", False)
    client = server.app.test_client()

    first_response = client.get(f"/upload?hash={'f' * 40}")
    second_response = client.get(f"/upload?hash={'1' * 40}")

    assert first_response.status_code == 202
    assert second_response.status_code == 409
    assert len(commands) == 1


def test_single_upload_mode_accepts_new_request_after_previous_process_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    processes = [
        SimpleNamespace(pid=1111, poll=lambda: 0),
        SimpleNamespace(pid=2222, poll=lambda: None),
    ]
    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: processes.pop(0))
    monkeypatch.setattr(server, "ALLOW_CONCURRENT_UPLOADS", False)
    client = server.app.test_client()

    first_response = client.get(f"/upload?hash={'2' * 40}")
    second_response = client.get(f"/upload?hash={'3' * 40}")

    assert first_response.status_code == 202
    assert second_response.status_code == 202


def test_tag_poll_starts_each_hash_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    torrent_hash = "a" * 40
    qbt_client = Mock()
    qbt_client.torrents_info.return_value = [SimpleNamespace(hash=torrent_hash)]
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)

    server._poll_upload_tag(qbt_client)
    server._poll_upload_tag(qbt_client)
    process.poll.return_value = 0
    server._poll_upload_tag(qbt_client)

    qbt_client.torrents_info.assert_called_with(tag="Upload")
    popen.assert_called_once_with(server._upload_command(torrent_hash), cwd=server.PROJECT_ROOT, env=ANY)
    process.terminate.assert_not_called()


@pytest.mark.parametrize("returncode", [None, 0, 1, -15, 10, 11])
def test_only_success_or_dupe_exit_replaces_upload_tag(monkeypatch: pytest.MonkeyPatch, returncode):
    torrent_hash = "a" * 40
    tags = {"Upload", "Keep"}
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    client = Mock()
    client.torrents_info.side_effect = lambda **_kwargs: [SimpleNamespace(hash=torrent_hash)] if "Upload" in tags else []
    client.torrents_add_tags.side_effect = lambda **kwargs: tags.add(kwargs["tags"])
    client.torrents_remove_tags.side_effect = lambda **kwargs: tags.discard(kwargs["tags"])

    server._poll_upload_tag(client)
    process.poll.return_value = returncode
    server._poll_upload_tag(client)
    server._poll_upload_tag(client)

    assert tags == {{10: "Uploaded", 11: "Cleanup"}.get(returncode, "Upload"), "Keep"}
    popen.assert_called_once()
    process.terminate.assert_not_called()
    assert client.torrents_add_tags.call_count == (returncode in (10, 11))
    assert client.torrents_remove_tags.call_count == (returncode in (10, 11))


@pytest.mark.parametrize("failed_method", ["torrents_add_tags", "torrents_remove_tags"])
@pytest.mark.parametrize(("returncode", "tag"), [(10, "Uploaded"), (11, "Cleanup")])
def test_retag_failure_is_retried(failed_method, returncode, tag):
    torrent_hash = "a" * 40
    process = Mock(pid=1234)
    process.poll.return_value = returncode
    server._active_upload_processes[torrent_hash] = process
    client = Mock()
    getattr(client, failed_method).side_effect = [RuntimeError("offline"), None]
    server._retag_completed_uploads(client)

    assert server._completed_upload_hashes == {torrent_hash: tag}
    assert not server._active_upload_processes
    if failed_method == "torrents_add_tags":
        client.torrents_remove_tags.assert_not_called()

    server._retag_completed_uploads(client)

    assert not server._completed_upload_hashes
    assert getattr(client, failed_method).call_count == 2
    assert client.method_calls[-2:] == [
        ("torrents_add_tags", (), {"torrent_hashes": torrent_hash, "tags": tag}),
        ("torrents_remove_tags", (), {"torrent_hashes": torrent_hash, "tags": "Upload"}),
    ]


def test_completion_polling_without_tag_watch(monkeypatch: pytest.MonkeyPatch):
    client_config = {"torrent_client": "qbit"}
    config: dict[str, object] = {"DEFAULT": {"default_torrent_client": "source"}, "TORRENT_CLIENTS": {"source": client_config}}
    client = Mock()
    clients = Mock(init_qbittorrent_client=AsyncMock(return_value=client))
    monkeypatch.setitem(server.sys.modules, "data.config", SimpleNamespace(config=config))
    monkeypatch.setitem(server.sys.modules, "src.clients", SimpleNamespace(Clients=Mock(return_value=clients)))
    process = Mock(pid=1234)
    process.poll.return_value = 10
    server._active_upload_processes["a" * 40] = process
    stop = server.threading.Event()
    monkeypatch.setattr(stop, "wait", lambda _interval: stop.set())

    server.asyncio.run(server._watch_upload_tag(stop, 5, watch_tags=False))

    client.torrents_info.assert_not_called()
    client.torrents_add_tags.assert_called_once_with(torrent_hashes="a" * 40, tags="Uploaded")
    client.torrents_remove_tags.assert_called_once_with(torrent_hashes="a" * 40, tags="Upload")


def test_tag_poll_does_not_repeat_http_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = 0
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    assert server.app.test_client().get(f"/upload?hash={'A' * 40}").status_code == 202
    qbt_client = Mock()
    qbt_client.torrents_info.return_value = [SimpleNamespace(hash="a" * 40)]

    server._poll_upload_tag(qbt_client)

    popen.assert_called_once()


def test_tag_poll_retries_busy_torrents(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "ALLOW_CONCURRENT_UPLOADS", False)
    first = Mock(pid=1234)
    first.poll.return_value = None
    second = Mock(pid=5678)
    second.poll.return_value = None
    popen = Mock(side_effect=[first, second])
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    qbt_client.torrents_info.return_value = [SimpleNamespace(hash="a" * 40), SimpleNamespace(hash="b" * 40)]

    server._poll_upload_tag(qbt_client)
    assert server._submitted_upload_hashes == {"a" * 40}
    first.poll.return_value = 0
    server._poll_upload_tag(qbt_client)

    assert popen.call_count == 2
    assert server._submitted_upload_hashes == {"a" * 40, "b" * 40}


def test_tag_poll_retries_spawn_failures_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(side_effect=[OSError("spawn failed"), process, process])
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    qbt_client.torrents_info.return_value = [SimpleNamespace(hash="a" * 40), SimpleNamespace(hash="b" * 40)]

    server._poll_upload_tag(qbt_client)
    assert server._submitted_upload_hashes == {"b" * 40}
    server._poll_upload_tag(qbt_client)

    assert popen.call_count == 3
    assert server._submitted_upload_hashes == {"a" * 40, "b" * 40}


def test_tag_poll_picks_up_new_hashes_and_skips_invalid_hashes(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    qbt_client.torrents_info.side_effect = [
        [SimpleNamespace(hash="a" * 40)],
        [SimpleNamespace(hash="a" * 40), SimpleNamespace(hash="b" * 64), SimpleNamespace(hash="invalid")],
    ]

    server._poll_upload_tag(qbt_client)
    server._poll_upload_tag(qbt_client)

    assert [call.args[0][2] for call in popen.call_args_list] == ["a" * 40, "b" * 64]


def test_main_starts_five_second_watcher_and_stops_it(monkeypatch: pytest.MonkeyPatch) -> None:
    thread = Mock()
    thread_factory = Mock(return_value=thread)
    monkeypatch.setattr(server.threading, "Thread", thread_factory)
    monkeypatch.setattr(server.app, "run", Mock())

    assert server.main([]) == 0

    kwargs = thread_factory.call_args.kwargs
    assert kwargs["target"] is server._run_tag_watcher
    assert kwargs["args"][1] == 5
    assert kwargs["args"][0].is_set()
    assert kwargs["daemon"] is True
    thread.start.assert_called_once_with()
    thread.join.assert_called_once_with(timeout=5)


@pytest.mark.parametrize("interval", ["0", "-1"])
def test_main_rejects_nonpositive_poll_interval(monkeypatch: pytest.MonkeyPatch, interval: str) -> None:
    run = Mock()
    monkeypatch.setattr(server.app, "run", run)

    with pytest.raises(SystemExit) as error:
        server.main(["--poll-interval", interval])

    assert error.value.code == 2
    run.assert_not_called()


def test_watcher_uses_default_client_and_recovers_from_poll_failure(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    client_config = {"torrent_client": "qbit"}
    config = {
        "DEFAULT": {"default_torrent_client": "default-qbit", "searching_client_list": ["other"]},
        "TORRENT_CLIENTS": {"default-qbit": client_config, "other": {"torrent_client": "deluge"}},
    }
    qbt_client = Mock()
    qbt_client.torrents_info.side_effect = [RuntimeError("unavailable"), []]
    clients = Mock(init_qbittorrent_client=AsyncMock(return_value=qbt_client))
    clients_factory = Mock(return_value=clients)
    monkeypatch.setitem(server.sys.modules, "data.config", SimpleNamespace(config=config))
    monkeypatch.setitem(server.sys.modules, "src.clients", SimpleNamespace(Clients=clients_factory))
    waits = []
    stop = SimpleNamespace(is_set=lambda: len(waits) >= 2, wait=waits.append)

    server.asyncio.run(server._watch_upload_tag(stop, 5))

    clients_factory.assert_called_once_with(config)
    clients.init_qbittorrent_client.assert_awaited_once_with(client_config)
    assert qbt_client.torrents_info.call_count == 2
    assert waits == [5, 5]
    assert capsys.readouterr().out == "Watching qBittorrent client 'default-qbit' for tag 'Upload' (polling every 5s).\n"


def test_watcher_skips_non_qbittorrent_default(monkeypatch: pytest.MonkeyPatch) -> None:
    config = {
        "DEFAULT": {"default_torrent_client": "other"},
        "TORRENT_CLIENTS": {"other": {"torrent_client": "deluge"}},
    }
    clients_factory = Mock()
    monkeypatch.setitem(server.sys.modules, "data.config", SimpleNamespace(config=config))
    monkeypatch.setitem(server.sys.modules, "src.clients", SimpleNamespace(Clients=clients_factory))

    server.asyncio.run(server._watch_upload_tag(server.threading.Event(), 5))

    clients_factory.assert_not_called()


def test_removing_tag_cancels_and_retagging_restarts(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    torrent = SimpleNamespace(hash="A" * 40)
    qbt_client.torrents_info.side_effect = [[torrent], [], [], [torrent]]

    server._poll_upload_tag(qbt_client)
    server._poll_upload_tag(qbt_client)
    assert not server._active_upload_processes
    assert not server._submitted_upload_hashes
    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=5)
    server._poll_upload_tag(qbt_client)
    assert popen.call_count == 1
    server._poll_upload_tag(qbt_client)
    assert popen.call_count == 2


def test_removing_completed_torrent_tag_does_not_restart_it(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = 0
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    qbt_client.torrents_info.side_effect = [[SimpleNamespace(hash="a" * 40)], []]

    server._poll_upload_tag(qbt_client)
    server._poll_upload_tag(qbt_client)

    popen.assert_called_once()
    process.terminate.assert_not_called()
    assert not server._submitted_upload_hashes


def test_failed_poll_does_not_cancel_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    monkeypatch.setattr(server.subprocess, "Popen", Mock(return_value=process))
    qbt_client = Mock()
    qbt_client.torrents_info.side_effect = [[SimpleNamespace(hash="a" * 40)], RuntimeError("offline")]

    server._poll_upload_tag(qbt_client)
    with pytest.raises(RuntimeError, match="offline"):
        server._poll_upload_tag(qbt_client)

    process.terminate.assert_not_called()
    assert server._tagged_upload_hashes == {"a" * 40}


@pytest.mark.parametrize("answer", ["y", "YES", "n", ""])
def test_startup_confirmation_lists_existing_torrents(monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: True)
    prompt = Mock(return_value=answer)
    monkeypatch.setattr("builtins.input", prompt)
    qbt_client = Mock()
    qbt_client.torrents_info.return_value = [SimpleNamespace(hash="a" * 40, name="Existing release")]

    server._poll_upload_tag(qbt_client, existing_tagged="ask")
    server._poll_upload_tag(qbt_client)

    assert popen.call_count == int(answer.lower() in {"y", "yes"})
    prompt.assert_called_once()
    assert "Existing release" in prompt.call_args.args[0]
    assert "a" * 40 in prompt.call_args.args[0]


def test_skipped_startup_tags_allow_new_tags_and_retagging(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    old = SimpleNamespace(hash="a" * 40)
    new = SimpleNamespace(hash="b" * 40)
    qbt_client.torrents_info.side_effect = [[old], [old, new], [new], [old, new]]

    server._poll_upload_tag(qbt_client, existing_tagged="skip")
    popen.assert_not_called()
    server._poll_upload_tag(qbt_client)
    assert popen.call_count == 1
    server._poll_upload_tag(qbt_client)
    server._poll_upload_tag(qbt_client)
    assert popen.call_count == 2


def test_startup_without_terminal_skips_existing_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: False)
    popen = Mock()
    prompt = Mock()
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    monkeypatch.setattr("builtins.input", prompt)
    qbt_client = Mock()
    qbt_client.torrents_info.return_value = [SimpleNamespace(hash="a" * 40)]

    server._poll_upload_tag(qbt_client, existing_tagged="ask")

    popen.assert_not_called()
    prompt.assert_not_called()


def test_failed_tag_cancellation_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    process.terminate.side_effect = [OSError("permission denied"), None]
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    qbt_client = Mock()
    qbt_client.torrents_info.side_effect = [[SimpleNamespace(hash="a" * 40)], [], []]

    server._poll_upload_tag(qbt_client)
    server._poll_upload_tag(qbt_client)
    assert server._tagged_upload_hashes == {"a" * 40}
    assert server._active_upload_processes["a" * 40] is process
    server._poll_upload_tag(qbt_client)

    assert not server._tagged_upload_hashes
    assert not server._active_upload_processes
    assert not server._submitted_upload_hashes
    assert process.terminate.call_count == 2
    popen.assert_called_once()


def test_normal_shutdown_stops_active_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    server._active_upload_processes["a" * 40] = process
    monkeypatch.setattr(server.app, "run", Mock())
    previous_sigterm = server.signal.getsignal(server.signal.SIGTERM)

    assert server.main(["--no-tag-watch"]) == 0

    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=5)
    assert not server._active_upload_processes
    assert server._server_stopping.is_set()
    assert server.signal.getsignal(server.signal.SIGTERM) == previous_sigterm


def test_sigterm_stops_uploads_and_restores_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    process = Mock(pid=1234)
    process.poll.return_value = None
    server._active_upload_processes["a" * 40] = process
    previous_sigterm = server.signal.getsignal(server.signal.SIGTERM)

    def run(**_kwargs):
        server.signal.getsignal(server.signal.SIGTERM)(server.signal.SIGTERM, None)

    monkeypatch.setattr(server.app, "run", run)
    with pytest.raises(SystemExit) as error:
        server.main(["--no-tag-watch"])

    assert error.value.code == 0
    process.terminate.assert_called_once_with()
    assert not server._active_upload_processes
    assert server.signal.getsignal(server.signal.SIGTERM) == previous_sigterm


def test_shutdown_prevents_new_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    popen = Mock()
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    server._server_stopping.set()

    process, status = server._toggle_upload_process(server._upload_command("a" * 40), "a" * 40)

    assert process is None
    assert status == "stopping"
    popen.assert_not_called()


def test_startup_confirmation_survives_connection_and_query_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    client_config = {"torrent_client": "qbit"}
    config = {"DEFAULT": {"default_torrent_client": "qbit"}, "TORRENT_CLIENTS": {"qbit": client_config}}
    old = SimpleNamespace(hash="a" * 40, name="Existing release")
    new = SimpleNamespace(hash="b" * 40, name="New release")
    qbt_client = Mock()
    qbt_client.torrents_info.side_effect = [RuntimeError("offline"), [old], [old, new]]
    clients = Mock(init_qbittorrent_client=AsyncMock(side_effect=[None, qbt_client]))
    monkeypatch.setitem(server.sys.modules, "data.config", SimpleNamespace(config=config))
    monkeypatch.setitem(server.sys.modules, "src.clients", SimpleNamespace(Clients=Mock(return_value=clients)))
    confirm = Mock(return_value=False)
    monkeypatch.setattr(server, "_confirm_existing_tagged", confirm)
    process = Mock(pid=1234)
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(server.subprocess, "Popen", popen)
    waits = []
    stop = SimpleNamespace(is_set=lambda: len(waits) >= 4, wait=waits.append)

    server.asyncio.run(server._watch_upload_tag(stop, 5))

    assert clients.init_qbittorrent_client.await_count == 2
    confirm.assert_called_once_with({"a" * 40: old}, "ask")
    popen.assert_called_once_with(server._upload_command("b" * 40), cwd=server.PROJECT_ROOT, env=ANY)
