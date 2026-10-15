from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import hash_upload_server as server


@pytest.fixture(autouse=True)
def reset_upload_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, "_active_upload_processes", {})


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

    assert server.main(["--host", "0.0.0.0", "--port", "9000"]) == 0

    assert run_args == [{"host": "0.0.0.0", "port": 9000}]


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
    assert calls == [([server.sys.executable, str(server.UPLOAD_SCRIPT), torrent_hash, "--unattended"], {"cwd": server.PROJECT_ROOT})]
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