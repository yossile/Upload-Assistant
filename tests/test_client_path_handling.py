# ruff: noqa: S101

import asyncio
import os
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import bencode
import pytest
from qbittorrentapi.torrents import TorrentInfoList
from torf import Torrent

from src.clients import Clients
from src.exceptions import TorrentRemovedError
from src.get_tracker_data import TrackerDataManager
from src.meta import Meta
from src.prep_helpers import _imdb_tmdb_prefetch_matches, _tracker_prefetch_matches
from src.torrent_clients.path_utils import coerce_str_list, is_path_under, map_remote_to_local_path, map_save_path, tracker_directory
from src.torrent_clients.qbittorrent import _link_torrent_files, create_cross_seed_links


@pytest.mark.asyncio
async def test_qbittorrent_rejects_mismatched_loose_file_root_without_linking(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    video = downloads / "fictional.mkv"
    video.write_bytes(b"fictional video")
    torrent = Torrent(path=downloads)
    torrent.name = "Fictional.Movie.2025"
    torrent.generate()
    meta = Meta({"path": str(video), "filelist": [str(video)], "isdir": False})
    client = Clients({})
    with patch.object(client, "init_qbittorrent_client", AsyncMock()) as init_client, pytest.raises(ValueError, match="does not match source directory"):
        await client.qbittorrent(str(video), torrent, str(tmp_path), str(tmp_path), {}, "", meta.filelist, meta, "EXAMPLE")
    init_client.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_directory_links_only_torrent_files_with_different_root_name(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    video = downloads / "fictional.mkv"
    subtitle = downloads / "fictional.srt"
    unrelated = downloads / "unrelated.mkv"
    for file in (video, subtitle, unrelated):
        file.write_bytes(file.name.encode())
    torrent = Torrent(path=downloads, exclude_globs=["*unrelated*"])
    torrent.name = "Fictional.Movie.2025"
    torrent.generate()
    links = tmp_path / "links"
    assert await _link_torrent_files(torrent, downloads, links, use_hardlink=True)
    assert await _link_torrent_files(torrent, downloads, links, use_hardlink=True)
    assert sorted(file.name for file in (links / torrent.name).iterdir()) == sorted([video.name, subtitle.name])
    for file in (video, subtitle):
        assert (links / torrent.name / file.name).read_bytes() == file.read_bytes()


@pytest.mark.asyncio
async def test_torrent_links_validate_all_sources_before_creating_links(tmp_path):
    release = tmp_path / "release"
    release.mkdir()
    video = release / "fictional.mkv"
    subtitle = release / "fictional.srt"
    video.write_bytes(b"video")
    subtitle.write_bytes(b"subtitle")
    torrent = Torrent(path=release)
    torrent.generate()
    subtitle.unlink()
    with patch("src.torrent_clients.qbittorrent.async_link_directory", AsyncMock()) as link:
        assert not await _link_torrent_files(torrent, release, tmp_path / "links", use_hardlink=True)
    link.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("file_path", ["release/../outside.mkv", "../outside.mkv"])
async def test_torrent_links_reject_paths_outside_roots(tmp_path, file_path):
    release = tmp_path / "release"
    release.mkdir()
    (tmp_path / "outside.mkv").write_bytes(b"outside")
    torrent = SimpleNamespace(name="release", files=[file_path])
    with patch("src.torrent_clients.qbittorrent.async_link_directory", AsyncMock()) as link:
        assert not await _link_torrent_files(torrent, release, tmp_path / "links", use_hardlink=True)
    link.assert_not_awaited()


@pytest.mark.asyncio
async def test_torrent_links_reject_existing_different_file(tmp_path):
    release = tmp_path / "release"
    release.mkdir()
    video = release / "fictional.mkv"
    video.write_bytes(b"source")
    torrent = Torrent(path=release)
    torrent.generate()
    links = tmp_path / "links"
    destination = links / release.name / video.name
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"different file")
    with patch("src.torrent_clients.qbittorrent.async_link_directory", AsyncMock()) as link:
        assert not await _link_torrent_files(torrent, release, links, use_hardlink=True)
    link.assert_not_awaited()
    assert destination.read_bytes() == b"different file"


@pytest.mark.asyncio
@pytest.mark.parametrize("use_proxy", [False, True])
@pytest.mark.parametrize("delay,cross", [(5, False), (0, False), (5, True)])
async def test_qbittorrent_delay_happens_between_stopped_add_and_start(tmp_path, use_proxy, delay, cross):
    video = tmp_path / "fictional.mkv"
    video.write_bytes(b"fictional video")
    torrent = Torrent(path=video)
    torrent.generate()
    meta = Meta(path=str(video), filelist=[str(video)])
    client = Clients({"DEFAULT": {"inject_delay": delay}, "TRACKERS": {"EXAMPLE": {}}})
    events = []

    async def record_add(*args):
        options = args[3] if use_proxy else args[2]
        if use_proxy:
            assert options["paused"] == options["stopped"] == str(cross or delay > 0).lower()
        else:
            assert options["is_paused"] == options["is_stopped"] == options["paused"] == (cross or delay > 0)
        events.append("add")

    async def record_sleep(seconds):
        assert seconds == delay
        events.append("sleep")

    async def record_start(*_args, **_kwargs):
        events.append("start")
        return SimpleNamespace(status_code=200)

    qbt = SimpleNamespace(torrents_resume=lambda *_args: None)

    async def record_operation(_operation, description, **_kwargs):
        if description == "Resume torrent":
            events.append("start")
        return [object()]

    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(status_code=200, json=lambda: [{"hash": torrent.infohash}])),
        post=AsyncMock(side_effect=record_start),
        aclose=AsyncMock(),
    )
    config = {"qui_proxy_url": "https://qbit.invalid"} if use_proxy else {}
    with (
        patch.object(client, "init_qbittorrent_client", AsyncMock(return_value=qbt)),
        patch.object(client, "_add_torrent_direct", AsyncMock(side_effect=record_add)),
        patch.object(client, "_add_torrent_via_proxy", AsyncMock(side_effect=record_add)),
        patch.object(client, "retry_qbt_operation", AsyncMock(side_effect=record_operation)),
        patch.object(client, "create_ssl_context_for_client", return_value=None),
        patch("src.torrent_clients.qbittorrent.httpx.AsyncClient", return_value=session),
        patch("src.torrent_clients.qbittorrent.asyncio.sleep", side_effect=record_sleep),
    ):
        await client.qbittorrent(str(video), torrent, str(tmp_path), str(tmp_path), config, "", meta.filelist, meta, "EXAMPLE", cross)

    assert events == (["add"] if cross else ["add", "sleep", "start"] if delay else ["add", "start"])


@pytest.mark.asyncio
@pytest.mark.parametrize("layout", ["singlefile", "multifile-video", "multifile-subtitles"])
@pytest.mark.parametrize("file_input", [False, True])
@pytest.mark.parametrize("keep_folder", [False, True])
@pytest.mark.parametrize("linking", [None, "hardlink", "symlink"])
@pytest.mark.parametrize("use_proxy", [False, True])
@pytest.mark.parametrize("content_layout", ["Original", "NoSubfolder"])
async def test_qbittorrent_destination_matches_torrent_layout(tmp_path, layout, file_input, keep_folder, linking, use_proxy, content_layout):
    release = tmp_path / "Fictional.Movie.2025-GROUP"
    release.mkdir()
    video = release / "fictional.mkv"
    subtitle = release / "fictional.pt-BR.srt"
    video.write_bytes(b"fictional video")
    subtitle.write_text("fictional subtitle", encoding="utf-8")
    unrelated = release / "unrelated.txt"
    unrelated.write_text("not part of this torrent", encoding="utf-8")
    torrent = Torrent(
        path=video if layout == "singlefile" else release,
        exclude_globs=["*.txt", "*.srt"] if layout == "multifile-video" else ["*.txt"],
    )
    torrent.generate()
    input_path = video if file_input else release
    meta = Meta({"path": str(input_path), "filelist": [str(video)], "subtitle_files": [str(subtitle)], "keep_folder": keep_folder})
    client = Clients({"TRACKERS": {"EXAMPLE": {}}})
    add = AsyncMock()
    link = AsyncMock(return_value=True)
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(status_code=200, json=lambda: [{"hash": torrent.infohash}])),
        post=AsyncMock(return_value=SimpleNamespace(status_code=200)),
        aclose=AsyncMock(),
    )
    config = {"linking": linking, "linked_folder": [str(tmp_path / "links")], "content_layout": content_layout}
    if use_proxy:
        config["qui_proxy_url"] = "https://qbit.invalid"
    with (
        patch.object(client, "init_qbittorrent_client", AsyncMock(return_value=object())),
        patch.object(client, "_add_torrent_direct", add),
        patch.object(client, "_add_torrent_via_proxy", add),
        patch.object(client, "retry_qbt_operation", AsyncMock(return_value=[object()])),
        patch.object(client, "create_ssl_context_for_client", return_value=None),
        patch("src.torrent_clients.qbittorrent.httpx.AsyncClient", return_value=session),
        patch("src.torrent_clients.qbittorrent.async_link_directory", link) if linking == "symlink" else nullcontext(),
    ):
        await client.qbittorrent(
            str(input_path),
            torrent,
            str(tmp_path),
            str(tmp_path),
            config,
            "",
            meta.filelist,
            meta,
            "EXAMPLE",
        )

    add.assert_awaited_once()
    save_path = Path(add.call_args.args[3]["savepath"] if use_proxy else add.call_args.args[2]["save_path"])
    expected = tmp_path / "links" / "EXAMPLE" if linking else release if layout == "singlefile" else tmp_path
    link_base = tmp_path / "links" / "EXAMPLE"
    if layout != "singlefile" and content_layout == "NoSubfolder":
        expected = link_base / torrent.name if linking else release
    assert save_path == expected
    if linking == "symlink":
        assert link.await_count == len(torrent.files)
        for file in torrent.files:
            source = release / Path(str(file)).relative_to(torrent.name) if layout != "singlefile" else video
            link.assert_any_await(src=str(source), dst=link_base / str(file), use_hardlink=False)
    else:
        # The actual file layout must be usable by the client, including subtitles.
        for file in torrent.files:
            relative_file = Path(str(file)).relative_to(torrent.name) if layout != "singlefile" and content_layout == "NoSubfolder" else Path(str(file))
            destination = save_path / relative_file
            source = release / Path(str(file)).relative_to(torrent.name) if layout != "singlefile" else video
            assert destination.read_bytes() == source.read_bytes()
        if linking:
            assert not (link_base / release.name / unrelated.name).exists()


def test_qbittorrent_coerce_str_list_parses_stringified_paths() -> None:
    assert coerce_str_list("['/local', '/remote']") == ["/local", "/remote"]
    assert coerce_str_list("/local") == ["/local"]


def test_qbittorrent_map_save_path_accepts_path_objects() -> None:
    mapped_path = map_save_path(Path("/local/links/EXAMPLE"), Path("/local"), Path("/remote"))

    assert mapped_path == "/remote/links/EXAMPLE/"


def test_map_save_path_does_not_rewrite_sibling_paths() -> None:
    mapped_path = map_save_path("/locality/release", "/local", "/remote")

    assert mapped_path == "/locality/release/"


def test_map_save_path_preserves_case_insensitive_mapping_and_client_format() -> None:
    assert map_save_path("/Local/Release", "/local", "/remote") == "/remote/Release/"
    assert map_save_path("/local/Release", "/local", "/remote", trailing_slash=False) == "/remote/Release"


def test_map_remote_to_local_path_uses_paired_roots_and_path_boundaries() -> None:
    assert map_remote_to_local_path("/remote-b/Release/movie.mkv", ["/local-a", "/local-b"], ["/remote-a", "/remote-b"]) == "/local-b/Release/movie.mkv"
    assert map_remote_to_local_path("/remote-bak/Release", ["/local-a"], ["/remote-b"]) == "/remote-bak/Release"


@pytest.mark.asyncio
async def test_resolve_qbittorrent_hash_path_maps_content_path(tmp_path: Path) -> None:
    media = tmp_path / "downloads" / "Release" / "movie.mkv"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"media")
    infohash = "a" * 40
    client = Clients(
        {
            "DEFAULT": {"default_torrent_client": "qbit"},
            "TORRENT_CLIENTS": {
                "qbit": {
                    "torrent_client": "qbit",
                    "local_path": str(tmp_path / "downloads"),
                    "remote_path": "/remote/downloads",
                }
            },
        }
    )
    def torrents_info(**kwargs):
        assert kwargs == {"torrent_hashes": infohash}
        return TorrentInfoList(
            [
                {
                    "hash": infohash,
                    "content_path": "/remote/downloads/Release/movie.mkv",
                    "save_path": "",
                    "name": "Release",
                    "state": "seeding",
                    "progress": 1.0,
                    "comment": "comment from info response",
                }
            ]
        )

    qbt_client = SimpleNamespace(torrents_info=torrents_info)
    with (
        patch.object(client, "init_qbittorrent_client", AsyncMock(return_value=qbt_client)),
        patch.object(client, "_start_hash_metadata_prefetch") as start_prefetch,
    ):
        meta = Meta(base_dir=str(tmp_path), uuid="Release")
        resolved = await client.resolve_qbittorrent_hash_path(meta, infohash)

    start_prefetch.assert_not_called()
    assert resolved == str(media)
    assert meta.source_torrent_hash == infohash
    assert meta.torrent_comments == [{"hash": infohash, "name": "Release", "comment": "comment from info response"}]

    meta.infohash = infohash
    meta.path = resolved
    with (
        patch.object(client, "init_qbittorrent_client", AsyncMock()) as init_client,
        patch.object(client, "_extract_tracker_ids_from_comment", return_value={"PTP": "123"}) as extract_comment,
    ):
        await client.get_ptp_from_hash_qbit(meta, client.config["TORRENT_CLIENTS"]["qbit"], pathed=True)

    init_client.assert_not_awaited()
    extract_comment.assert_called_once_with("comment from info response")
    assert meta.get_tracker_id("PTP") == "123"
    assert meta.source_torrent_client == "qbit"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("searching_clients", "requested_client", "expected_clients"),
    [
        ([], None, ["default"]),
        (["", "none"], None, ["default"]),
        (["search"], None, ["default"]),
        (["missing", "search"], None, ["default"]),
        (["search"], "explicit", ["explicit"]),
    ],
)
async def test_resolve_qbittorrent_hash_respects_client_selection(tmp_path: Path, searching_clients, requested_client, expected_clients) -> None:
    media = tmp_path / "downloads" / "Release" / "movie.mkv"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"media")
    infohash = "b" * 40
    client_names = ["other", "default", "missing", "search", "explicit"]
    clients = Clients(
        {
            "DEFAULT": {"searching_client_list": searching_clients, "default_torrent_client": "default"},
            "TORRENT_CLIENTS": {
                name: {
                    "torrent_client": "qbit",
                    "local_path": str(tmp_path / "downloads"),
                    "remote_path": "/remote/downloads",
                }
                for name in client_names
            },
        }
    )
    search_client = SimpleNamespace(torrents_info=lambda **_kwargs: [])
    actual_client = SimpleNamespace(
        torrents_info=lambda **_kwargs: TorrentInfoList([{"hash": infohash, "content_path": "/remote/downloads/Release/movie.mkv", "state": "seeding", "progress": 1.0}])
    )

    async def init_client(client_config):
        return search_client if client_config is clients.config["TORRENT_CLIENTS"]["missing"] else actual_client

    meta = Meta(client=requested_client)
    with patch.object(clients, "init_qbittorrent_client", AsyncMock(side_effect=init_client)) as initialize:
        resolved = await clients.resolve_qbittorrent_hash_path(meta, infohash)

    assert resolved == str(media)
    assert meta.source_torrent_client == expected_clients[-1]
    assert initialize.await_args_list == [((clients.config["TORRENT_CLIENTS"][name],), {}) for name in expected_clients]


@pytest.mark.asyncio
@pytest.mark.parametrize("searching_clients", [[], ["search"]])
@pytest.mark.parametrize("requested_client", [None, "explicit"])
async def test_resolve_qbittorrent_hash_does_not_search_unselected_clients(searching_clients, requested_client) -> None:
    clients = Clients(
        {
            "DEFAULT": {"searching_client_list": searching_clients, "default_torrent_client": "default"},
            "TORRENT_CLIENTS": {name: {"torrent_client": "qbit"} for name in ["search", "default", "other", "explicit"]},
        }
    )
    qbt_client = SimpleNamespace(torrents_info=lambda **_kwargs: [])
    with patch.object(clients, "init_qbittorrent_client", AsyncMock(return_value=qbt_client)) as initialize:
        resolved = await clients.resolve_qbittorrent_hash_path(Meta(client=requested_client), "b" * 40)

    assert resolved is None
    selected = requested_client or "default"
    initialize.assert_awaited_once_with(clients.config["TORRENT_CLIENTS"][selected])


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_fails", [False, True])
async def test_resolve_qbittorrent_hash_waits_by_supplied_hash(tmp_path: Path, wait_fails: bool) -> None:
    media = tmp_path / "downloads" / "Release" / "movie.mkv"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"media")
    infohash = "c" * 40
    client_config = {
        "torrent_client": "qbit",
        "local_path": str(tmp_path / "downloads"),
        "remote_path": "/remote/downloads",
    }
    clients = Clients(
        {
            "DEFAULT": {"default_torrent_client": "qbit"},
            "TORRENT_CLIENTS": {"qbit": client_config},
        }
    )
    qbt_client = SimpleNamespace(
        torrents_info=lambda **_kwargs: TorrentInfoList(
            [
                {
                    "hash": infohash,
                    "content_path": "/remote/downloads/Release/movie.mkv",
                    "name": "Release",
                    "state": "downloading",
                    "progress": 0.5,
                    "comment": "https://aither.cc/torrents/123",
                }
            ]
        )
    )
    with (
        patch.object(clients, "init_qbittorrent_client", AsyncMock(return_value=qbt_client)) as init_client,
        patch.object(clients, "_wait_for_qbittorrent_download", AsyncMock()) as wait_for_download,
        patch("src.pre_upload_screenshots.prepare_tracker_screenshots", AsyncMock()) as prepare_screenshots,
        patch("src.trackers.GAZELLE.passthepopcorn.PassThePopcorn.start_auth_prefetch") as start_auth,
        patch("src.trackers.GAZELLE.passthepopcorn.PassThePopcorn.cancel_auth_prefetch", AsyncMock()) as cancel_auth,
    ):
        async def check_screenshots_prepared(*_args):
            start_auth.assert_called_once_with(meta, clients.config)
            screenshot_meta, screenshot_config, tracker_ids = prepare_screenshots.await_args.args
            assert screenshot_meta.uuid == "movie.mkv"
            assert screenshot_config is clients.config
            assert tracker_ids == {"aither": "123"}
            if wait_fails:
                raise RuntimeError("download failed")

        wait_for_download.side_effect = check_screenshots_prepared
        meta = Meta()
        if wait_fails:
            with pytest.raises(RuntimeError, match="download failed"):
                await clients.resolve_qbittorrent_hash_path(meta, infohash)
            cancel_auth.assert_awaited_once_with(meta)
            return
        resolved = await clients.resolve_qbittorrent_hash_path(meta, infohash)
        cancel_auth.assert_not_awaited()

    assert resolved == str(media)
    assert meta.source_torrent_hash == infohash
    assert meta.torrent_comments == [{"hash": infohash, "name": "Release", "comment": "https://aither.cc/torrents/123"}]
    wait_for_download.assert_awaited_once_with(client_config, infohash)

    meta.infohash = infohash
    meta.path = resolved
    with patch.object(clients, "_extract_tracker_ids_from_comment", return_value={"PTP": "456"}) as extract_comment:
        await clients.get_ptp_from_hash_qbit(meta, client_config, pathed=True)

    extract_comment.assert_called_once_with("https://aither.cc/torrents/123")
    assert meta.get_tracker_id("PTP") == "456"
    init_client.assert_awaited_once()


@pytest.mark.asyncio
async def test_hash_wait_prefetches_tracker_ids_and_carries_them_forward(tmp_path: Path) -> None:
    media = tmp_path / "downloads" / "Release.mkv"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"media")
    infohash = "1" * 40
    client_config = {
        "torrent_client": "qbit",
        "local_path": str(tmp_path / "downloads"),
        "remote_path": "/remote/downloads",
    }
    clients = Clients(
        {
            "DEFAULT": {"default_torrent_client": "qbit"},
            "TORRENT_CLIENTS": {"qbit": client_config},
        }
    )
    qbt_client = SimpleNamespace(
        torrents_info=lambda **_kwargs: TorrentInfoList(
            [
                {
                    "hash": infohash,
                    "content_path": "/remote/downloads/Release.mkv",
                    "name": "Release.mkv",
                    "state": "downloading",
                    "progress": 0.5,
                    "comment": "https://lst.gg/torrents/262607",
                }
            ]
        )
    )
    tracker_started = asyncio.Event()
    finish_tracker = asyncio.Event()
    tracker_finished = asyncio.Event()

    async def lookup_tracker(_video, prefetched_meta, search_term, search_file_folder, category, **kwargs):
        assert search_term == "Release.mkv"
        assert search_file_folder == "file"
        assert category == "MOVIE"
        assert prefetched_meta.get_tracker_id("LST") == "262607"
        assert kwargs["skip_tracker_descriptions"] is True
        tracker_started.set()
        await finish_tracker.wait()
        prefetched_meta.imdb_id = 12345
        prefetched_meta.matched_tracker = "PASSTHEPOPCORN"
        prefetched_meta.tracker_search_attempted = True
        tracker_finished.set()

    async def wait_for_download(*_args):
        await asyncio.wait_for(tracker_started.wait(), timeout=2)
        assert not tracker_finished.is_set()

    with (
        patch.object(clients, "init_qbittorrent_client", AsyncMock(return_value=qbt_client)),
        patch.object(clients, "_wait_for_qbittorrent_download", AsyncMock(side_effect=wait_for_download)),
        patch.object(clients, "_extract_tracker_ids_from_comment", return_value={"LST": "262607"}),
        patch("src.get_tracker_data.TrackerDataManager.get_tracker_data", new=AsyncMock(side_effect=lookup_tracker)),
    ):
        meta = Meta(
            {
                "base_dir": str(tmp_path),
                "uuid": "Release.mkv",
                "unattended": True,
                "only_id": True,
                "category": "MOVIE",
            }
        )
        resolved = await asyncio.wait_for(clients.resolve_qbittorrent_hash_path(meta, infohash), timeout=2)
        assert not tracker_finished.is_set()
        meta.resolution = "1080p"
        meta.imdb_id = 0
        finish_tracker.set()
        await Clients(clients.config).finish_hash_metadata_prefetch(meta)

    assert resolved == str(media)
    assert meta.resolution == "1080p"
    assert meta.imdb_id == 12345
    assert meta.matched_tracker == "PASSTHEPOPCORN"
    assert meta.tracker_prefetch_completed is True
    assert meta.tracker_prefetch_search_term == "Release.mkv"
    assert meta.tracker_prefetch_category == "MOVIE"
    assert meta.get_tracker_id("LST") == "262607"
    assert meta.tracker_prefetch_ids == {"LST": "262607"}


@pytest.mark.asyncio
async def test_hash_wait_prefetches_imdb_tmdb_without_trackers(tmp_path: Path) -> None:
    media = tmp_path / "downloads" / "Some.Movie.2024.mkv"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"media")
    infohash = "2" * 40
    client_config = {
        "torrent_client": "qbit",
        "local_path": str(tmp_path / "downloads"),
        "remote_path": "/remote/downloads",
    }
    clients = Clients(
        {
            "DEFAULT": {"default_torrent_client": "qbit", "tracker_description_mode": "ids", "tmdb_api": "test-api-key"},
            "TORRENT_CLIENTS": {"qbit": client_config},
        }
    )
    qbt_client = SimpleNamespace(
        torrents_info=lambda **_kwargs: TorrentInfoList(
            [
                {
                    "hash": infohash,
                    "content_path": "/remote/downloads/Some.Movie.2024.mkv",
                    "name": "Some.Movie.2024.mkv",
                    "state": "downloading",
                    "progress": 0.5,
                }
            ]
        )
    )
    tmdb_started = asyncio.Event()
    imdb_started = asyncio.Event()
    finish_lookups = asyncio.Event()

    async def get_tmdb_id(*_args, **_kwargs):
        tmdb_started.set()
        await finish_lookups.wait()
        return 272726, "MOVIE"

    async def search_imdb(*_args, **_kwargs):
        imdb_started.set()
        await finish_lookups.wait()
        return 3109200

    async def wait_for_download(*_args):
        await asyncio.wait_for(asyncio.gather(tmdb_started.wait(), imdb_started.wait()), timeout=2)

    with (
        patch.object(clients, "init_qbittorrent_client", AsyncMock(return_value=qbt_client)),
        patch.object(clients, "_wait_for_qbittorrent_download", AsyncMock(side_effect=wait_for_download)),
        patch("src.get_name.NameManager.extract_title_and_year", new=AsyncMock(return_value=("Some Movie", None, "2024"))),
        patch("src.tmdb.TmdbManager.get_tmdb_id", new=AsyncMock(side_effect=get_tmdb_id)) as tmdb_search,
        patch("src.imdb.imdb_manager.search_imdb", new=AsyncMock(side_effect=search_imdb)) as imdb_search,
    ):
        meta = Meta(
            {
                "base_dir": str(tmp_path),
                "uuid": "Some.Movie.2024.mkv",
                "unattended": True,
                "only_id": True,
                "category": "MOVIE",
            }
        )
        resolved = await asyncio.wait_for(clients.resolve_qbittorrent_hash_path(meta, infohash), timeout=2)
        finish_lookups.set()
        await clients.finish_hash_metadata_prefetch(meta)

    assert resolved == str(media)
    assert meta.tmdb_id == 272726
    assert meta.imdb_id == 3109200
    assert meta.imdb_tmdb_prefetch_completed is True
    assert tmdb_search.await_count == 1
    assert imdb_search.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_field, value", [("imdb_id", 99999), ("category", "TV"), ("edit", True), ("no_imdb", True), ("imdb_manual", "99999")])
async def test_delayed_hash_prefetch_rejects_changed_identity(changed_field, value):
    client = Clients({})
    meta = Meta(base_dir="test-delayed-prefetch", source_torrent_hash="a" * 40, category="MOVIE")
    original = meta.copy()
    prefetched = meta.copy()
    prefetched.imdb_id = 12345
    prefetched.tracker_search_attempted = True
    prefetched.tracker_prefetch_category = "MOVIE"
    task = asyncio.create_task(AsyncMock(return_value=prefetched)())
    client._hash_metadata_prefetch_tasks[(meta.base_dir, meta.source_torrent_hash)] = (task, original, "Release", {})
    meta[changed_field] = value

    await client.finish_hash_metadata_prefetch(meta)

    assert not meta.tracker_prefetch_completed
    assert meta.imdb_id == (99999 if changed_field == "imdb_id" else original.imdb_id)
    assert not client._hash_metadata_prefetch_tasks


@pytest.mark.asyncio
async def test_delayed_hash_prefetch_cancellation_drains_task():
    started = asyncio.Event()
    client = Clients({})
    meta = Meta(base_dir="test-delayed-prefetch", source_torrent_hash="a" * 40)

    async def lookup():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(lookup())
    client._hash_metadata_prefetch_tasks[(meta.base_dir, meta.source_torrent_hash)] = (task, meta.copy(), "Release", {})
    await started.wait()
    await client.cancel_hash_metadata_prefetch(meta)

    assert task.cancelled()
    assert not client._hash_metadata_prefetch_tasks


def test_tracker_prefetch_reuse_requires_matching_search_inputs() -> None:
    meta = Meta(
        {
            "category": "MOVIE",
            "tracker_prefetch_completed": True,
            "tracker_prefetch_search_term": "Release.mkv",
            "tracker_prefetch_category": "MOVIE",
        }
    )

    assert _tracker_prefetch_matches(meta, "Release.mkv")
    assert not _tracker_prefetch_matches(meta, "Other.Release.mkv")
    meta.tracker_prefetch_ids = {"LST": "262607"}
    meta.set_tracker_ids({"LST": "262607"})
    assert _tracker_prefetch_matches(meta, "Other.Release.mkv")
    meta.set_tracker_ids({"LST": "999999"})
    assert not _tracker_prefetch_matches(meta, "Other.Release.mkv")
    meta.category = "TV"
    assert not _tracker_prefetch_matches(meta, "Release.mkv")


def test_imdb_tmdb_prefetch_reuse_requires_matching_search_inputs() -> None:
    meta = Meta(
        {
            "imdb_tmdb_prefetch_completed": True,
            "imdb_tmdb_prefetch_title": "Some Movie",
            "imdb_tmdb_prefetch_year": "2024",
            "imdb_tmdb_prefetch_category": "MOVIE",
            "imdb_tmdb_prefetch_secondary_title": "",
            "imdb_tmdb_prefetch_untouched_filename": "Some.Movie.2024.mkv",
        }
    )

    assert _imdb_tmdb_prefetch_matches(meta, "Some Movie", "2024", "MOVIE", None, "Some.Movie.2024.mkv")
    assert not _imdb_tmdb_prefetch_matches(meta, "Other Movie", "2024", "MOVIE", None, "Some.Movie.2024.mkv")


@pytest.mark.asyncio
async def test_id_only_prefetch_does_not_claim_skipped_tracker_search_completed() -> None:
    manager = TrackerDataManager({"DEFAULT": {"tracker_comment_only": True}})
    meta = Meta()

    await manager.get_tracker_data(None, meta, "Release.mkv", "file", "MOVIE", skip_tracker_descriptions=True, id_only_prefetch=True)

    assert meta.tracker_search_attempted is False


@pytest.mark.asyncio
@pytest.mark.parametrize("use_proxy", [False, True])
@pytest.mark.parametrize("show_progress", [False, True])
async def test_qbittorrent_download_removal_cancels_wait(use_proxy, show_progress):
    clients = Clients({})
    torrent_hash = "e" * 40
    responses = [[{"state": "downloading", "progress": 0.5}], []] if show_progress else [[]]
    client_config = {"qui_proxy_url": "https://qbit.example"} if use_proxy else {}
    session = AsyncMock()
    session.get.side_effect = [SimpleNamespace(status_code=200, json=Mock(return_value=response)) for response in responses]

    with (
        patch.object(clients, "init_qbittorrent_client", AsyncMock(return_value=object())),
        patch.object(clients, "retry_qbt_operation", AsyncMock(side_effect=responses)),
        patch.object(clients, "create_ssl_context_for_client", return_value=True),
        patch("src.torrent_clients.qbittorrent.httpx.AsyncClient", return_value=session),
        patch("src.torrent_clients.qbittorrent.asyncio.sleep", new_callable=AsyncMock),
        pytest.raises(TorrentRemovedError, match=f"Torrent {torrent_hash} was removed from qBittorrent; upload cancelled"),
    ):
        await clients._wait_for_qbittorrent_download(client_config, torrent_hash)

    if use_proxy:
        session.aclose.assert_awaited_once()
    else:
        session.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_upload_handles_removed_torrent_without_traceback(tmp_path):
    import upload

    torrent_hash = "e" * 40
    message = f"Torrent {torrent_hash} was removed from qBittorrent; upload cancelled."
    meta = Meta(path=torrent_hash)
    parser = Mock()
    parser.parse.return_value = (meta, None, None)

    with (
        patch.dict(sys.modules, {"data.config": None}),
        patch.object(sys, "argv", ["upload.py", torrent_hash]),
        patch.object(upload, "load_heavy_globals"),
        patch.object(upload, "config", {"DEFAULT": {"sanitize_meta": False}}, create=True),
        patch.object(upload, "parser", parser, create=True),
        patch.object(upload, "Meta", Meta, create=True),
        patch.object(upload, "subprocess_roots", return_value=None),
        patch.object(upload, "read_paths_from_stdin", return_value=([torrent_hash], [])),
        patch.object(upload, "update_notification", AsyncMock(return_value=None)),
        patch("src.stats.configure_stats"),
        patch("src.configvalidator.validate_config", return_value=(True, [], [])),
        patch.object(Clients, "resolve_qbittorrent_hash_path", AsyncMock(side_effect=TorrentRemovedError(message))) as resolve_path,
        patch.object(Clients, "cancel_hash_metadata_prefetch", AsyncMock()) as cancel_prefetch,
        patch.object(upload.PassThePopcorn, "cancel_prefetch", AsyncMock()) as cancel_ptp_prefetch,
        patch.object(upload.cleanup_manager, "reset_terminal"),
        patch.object(upload, "logger") as log,
        patch.object(upload.traceback, "format_exc") as format_traceback,
    ):
        await upload.do_the_thing(str(tmp_path))

    resolve_path.assert_awaited_once_with(meta, torrent_hash)
    log.info.assert_called_once_with(f"[yellow]{message}[/yellow]")
    format_traceback.assert_not_called()
    cancel_prefetch.assert_awaited_once_with(meta)
    cancel_ptp_prefetch.assert_awaited_once_with(meta)


@pytest.mark.asyncio
async def test_preflight_skips_name_search_for_hash_input() -> None:
    clients = Clients(
        {
            "DEFAULT": {"searching_client_list": ["qbit"]},
            "TORRENT_CLIENTS": {"qbit": {"torrent_client": "qbit", "enable_search": True}},
        }
    )
    meta = Meta({"source_torrent_hash": "d" * 40})
    with patch.object(clients, "init_qbittorrent_client", AsyncMock()) as init_client:
        await clients.preflight_qbittorrent_search(meta, [])

    init_client.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_fails", [False, True])
async def test_preflight_path_input_waits_for_matching_torrent(tmp_path: Path, wait_fails: bool) -> None:
    content_path = tmp_path / "Release"
    client_config = {"torrent_client": "qbit", "enable_search": True}
    clients = Clients(
        {
            "DEFAULT": {"searching_client_list": ["qbit"]},
            "TORRENT_CLIENTS": {"qbit": client_config},
        }
    )
    torrent_hash = "e" * 40
    matching_torrent = SimpleNamespace(
        hash=torrent_hash,
        content_path=str(content_path),
        name="Release",
        state="downloading",
        progress=0.5,
        comment="https://aither.cc/torrents/123",
    )
    meta = Meta({"path": str(content_path), "uuid": "Release"})

    with (
        patch.object(clients, "init_qbittorrent_client", AsyncMock(return_value=object())),
        patch.object(clients, "_fetch_qbit_search_results", AsyncMock(return_value=[matching_torrent])) as fetch_results,
        patch.object(clients, "_wait_for_qbittorrent_download", AsyncMock()) as wait_for_download,
        patch.object(clients, "_start_hash_metadata_prefetch") as start_prefetch,
        patch("src.pre_upload_screenshots.prepare_tracker_screenshots", AsyncMock()) as prepare_screenshots,
        patch("src.trackers.GAZELLE.passthepopcorn.PassThePopcorn.start_auth_prefetch") as start_auth,
        patch("src.trackers.GAZELLE.passthepopcorn.PassThePopcorn.cancel_auth_prefetch", AsyncMock()) as cancel_auth,
    ):
        async def check_screenshots_prepared(*_args):
            start_auth.assert_called_once_with(meta, clients.config)
            prepare_screenshots.assert_awaited_once_with(meta, clients.config, {"aither": "123"})
            if wait_fails:
                raise RuntimeError("download failed")

        wait_for_download.side_effect = check_screenshots_prepared
        if wait_fails:
            with pytest.raises(RuntimeError, match="download failed"):
                await clients.preflight_qbittorrent_search(meta, [])
            cancel_auth.assert_awaited_once_with(meta)
        else:
            await clients.preflight_qbittorrent_search(meta, [])
            cancel_auth.assert_not_awaited()

    fetch_results.assert_awaited_once()
    wait_for_download.assert_awaited_once_with(client_config, torrent_hash)
    start_prefetch.assert_not_called()


@pytest.mark.asyncio
async def test_source_hash_reuse_avoids_qbittorrent_list_search(tmp_path: Path) -> None:
    media = tmp_path / "Release" / "movie.mkv"
    media.parent.mkdir()
    media.write_bytes(b"media")
    torrent_hash = "f" * 40
    storage = tmp_path / "torrent_storage"
    storage.mkdir()
    torrent = Torrent(path=media)
    torrent.generate()
    torrent_path = storage / f"{torrent_hash}.torrent"
    torrent.write(torrent_path)

    source_client_config = {"torrent_client": "qbit", "torrent_storage_dir": str(storage), "enable_search": True}
    clients = Clients(
        {
            "DEFAULT": {"searching_client_list": ["other"]},
            "TORRENT_CLIENTS": {
                "other": {"torrent_client": "qbit", "enable_search": True},
                "source": source_client_config,
            },
        }
    )
    meta = Meta(
        base_dir=str(tmp_path),
        uuid="Release",
        path=str(media),
        filelist=[str(media)],
        source_torrent_hash=torrent_hash,
        source_torrent_client="source",
        client="none",
    )

    with (
        patch.object(clients, "is_valid_torrent", AsyncMock(return_value=(True, str(torrent_path)))),
        patch.object(clients, "search_qbit_for_torrent", AsyncMock()) as list_search,
    ):
        reused_paths = await clients.find_existing_torrents(meta)

    assert reused_paths
    assert meta.reuse_torrent_client == "source"
    list_search.assert_not_awaited()


def test_clients_remote_path_map_parses_stringified_path_lists() -> None:
    async def exercise() -> tuple[str, str]:
        clients = Clients({"TORRENT_CLIENTS": {}})
        meta = Meta({"path": "/local/content/release"})
        return await clients.remote_path_map(
            meta,
            {"local_path": "['/local', '/other']", "remote_path": "['/remote', '/elsewhere']"},
        )

    assert asyncio.run(exercise()) == (os.path.normpath("/local"), os.path.normpath("/remote"))


def test_rtorrent_coerce_str_list_parses_stringified_paths() -> None:
    assert coerce_str_list("['/local', '/remote']") == ["/local", "/remote"]


def test_rtorrent_keeps_multifile_release_directory_as_base(tmp_path: Path) -> None:
    for category in ("BOOK", "GAME"):
        release_dir = tmp_path / category
        release_dir.mkdir()
        filelist = [release_dir / "part1.bin", release_dir / "part2.bin"]
        for file_path in filelist:
            file_path.write_bytes(b"x")

        torrent_path = tmp_path / f"{category}.torrent"
        bencode.bwrite(
            {
                "announce": "https://tracker.invalid/announce",
                "info": {
                    "files": [
                        {"length": 1, "path": ["part1.bin"]},
                        {"length": 1, "path": ["part2.bin"]},
                    ],
                    "name": category,
                    "piece length": 1,
                    "pieces": b"0" * 40,
                },
            },
            str(torrent_path),
        )

        start_verbose = Mock()
        rtorrent_server = SimpleNamespace(load=SimpleNamespace(start_verbose=start_verbose))
        meta = Meta(
            {
                "category": category,
                "filelist": [str(file_path) for file_path in filelist],
                "path": str(release_dir),
            }
        )
        with (
            patch("src.torrent_clients.rtorrent.xmlrpc.client.Server", return_value=rtorrent_server),
            patch("src.torrent_clients.rtorrent.time.sleep"),
        ):
            Clients({}).rtorrent(
                str(release_dir),
                str(torrent_path),
                SimpleNamespace(),
                meta,
                str(tmp_path),
                str(tmp_path),
                {"linking": None, "rtorrent_label": None, "rtorrent_url": "https://rtorrent.invalid"},
                "TRACKER",
            )

        assert start_verbose.call_args.args[2] == f"d.directory_base.set={release_dir}"


def test_tracker_directory_falls_back_to_tracker_name() -> None:
    assert tracker_directory("/links", "", "EXAMPLE") == Path("/links/EXAMPLE")


def test_tracker_directory_rejects_paths_outside_link_root() -> None:
    for directory_name in (
        "/outside/exposed",
        "../exposed",
        "nested/exposed",
        "C:tmp",
        "C:",
        "C:/tmp",
        "C:\\tmp",
        "CON",
        "NUL",
        "AUX",
        "COM1",
        "LPT1",
        "CON.txt",
        "CON.foo.bar",
        "NUL.tar.gz",
        "COM1.backup.txt",
        "LPT9.archive.part",
    ):
        try:
            tracker_directory("/links", directory_name, "EXAMPLE")
        except ValueError:
            continue
        raise AssertionError(f"accepted unsafe tracker directory: {directory_name}")


def test_automatic_management_paths_require_path_boundaries() -> None:
    assert is_path_under("/media/local/release", "/media/local")
    assert not is_path_under("/media/locality/release", "/media/local")


def test_cross_seed_links_normalize_component_paths(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "episode.mkv").write_bytes(b"episode")
    torrent = SimpleNamespace(
        metainfo={
            "info": {
                "name": "Release",
                "files": [{"path": ["Season 1", "episode.mkv"], "length": 7}],
            }
        },
        name="Release",
    )
    meta = Meta({"path": str(source_dir), "filelist": [str(source_dir / "episode.mkv")]})

    async def exercise() -> bool:
        with patch("src.torrent_clients.qbittorrent.async_link_directory", new=AsyncMock(return_value=True)):
            return await create_cross_seed_links(meta, torrent, str(tmp_path / "tracker"), use_hardlink=False)

    assert asyncio.run(exercise())
