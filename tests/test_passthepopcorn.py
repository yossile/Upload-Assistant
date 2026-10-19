import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from src.artwork import select_ptp_cover_url
from src.meta import Meta
from src.tracker_images import set_tracker_image_collection
from src.trackers.GAZELLE.passthepopcorn import PassThePopcorn


class _Response:
    status_code = 200
    headers = {"content-type": "application/json"}
    text = ""

    def json(self):
        return {"Page": "Details", "GroupId": 1234, "Name": "Example", "Year": 2024}


class _SearchResponse(_Response):
    def json(self):
        return {
            "TotalResults": 20,
            "Movies": [
                {"TmdbId": 1126341, "GroupId": 1234, "Title": "Example", "Year": 2024},
                {"TmdbId": 9999999, "GroupId": 5678, "Title": "Unrelated", "Year": 2023},
            ],
        }


class _Client:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, *, url, headers, params):
        assert params == {"tmdb": 9876, "json": "noredirect"}  # noqa: S101
        return _Response()


class _SearchClient(_Client):
    async def get(self, *, url, headers, params):
        return _SearchResponse()


def test_tmdb_group_lookup_uses_tmdb_identifier(monkeypatch) -> None:
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: _Client())
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user = "user"
    tracker.api_key = "key"
    tracker.user_agent = "agent"

    assert asyncio.run(tracker.get_group_by_tmdb(9876)) == "1234"  # noqa: S101


def test_tmdb_group_lookup_filters_broad_results(monkeypatch) -> None:
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: _SearchClient())
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user = "user"
    tracker.api_key = "key"
    tracker.user_agent = "agent"

    assert asyncio.run(tracker.get_group_by_tmdb(1126341)) == "1234"  # noqa: S101


def test_get_type_supports_tmdb_only_metadata() -> None:
    tracker = object.__new__(PassThePopcorn)
    meta = Meta(tmdb_type="movie", runtime=120, keywords=[])

    assert tracker.get_type({}, meta) == "Feature Film"  # noqa: S101


def test_select_ptp_cover_url_falls_back_to_tmdb_poster() -> None:
    meta = Meta(tmdb_poster_path="/poster.jpg")

    assert select_ptp_cover_url(meta) == "https://image.tmdb.org/t/p/original/poster.jpg"  # noqa: S101


def test_select_ptp_cover_url_prefers_imdb_cover(monkeypatch) -> None:
    meta = Meta(imdb_info={"cover": "https://imdb.example/poster.jpg"}, tmdb_poster_path="/poster.jpg")

    assert select_ptp_cover_url(meta) == "https://imdb.example/poster.jpg"  # noqa: S101


def test_select_ptp_cover_url_prefers_explicit_poster(monkeypatch) -> None:
    monkeypatch.setattr("src.artwork.is_public_http_url", lambda _url: True)
    meta = Meta(explicit_poster="https://custom.example/poster.jpg", imdb_info={"cover": "https://imdb.example/poster.jpg"})

    assert select_ptp_cover_url(meta) == "https://custom.example/poster.jpg"  # noqa: S101


def test_select_ptp_cover_url_uses_artwork_before_tmdb_poster() -> None:
    meta = Meta(artwork_url="https://custom.example/poster.jpg", tmdb_poster_path="/poster.jpg")

    assert select_ptp_cover_url(meta) == "https://custom.example/poster.jpg"  # noqa: S101


def test_convert_bbcode_removes_unsupported_font_tags() -> None:
    tracker = object.__new__(PassThePopcorn)

    converted = tracker.convert_bbcode("Before [font=Courier New]formatted text[/font] after")

    assert converted == "Before formatted text after"  # noqa: S101


def test_convert_bbcode_clamps_unsupported_size_tags() -> None:
    tracker = object.__new__(PassThePopcorn)

    converted = tracker.convert_bbcode("[size=4]Normal[/size] [size=16]Oversized[/size]")

    assert converted == "[size=4]Normal[/size] [size=10]Oversized[/size]"  # noqa: S101


def test_find_non_ascii_torrent_paths_includes_root_and_files() -> None:
    torrent = SimpleNamespace(name="El Bumbún", files=["El Bumbún/video.mkv", "subs/español.srt"])

    assert PassThePopcorn._find_non_ascii_torrent_paths(torrent) == [  # noqa: S101
        "El Bumbún",
        "El Bumbún/video.mkv",
        "subs/español.srt",
    ]


def test_find_non_ascii_torrent_paths_allows_ascii_paths() -> None:
    torrent = SimpleNamespace(name="El.Bumbun", files=["El.Bumbun/video.mkv"])

    assert PassThePopcorn._find_non_ascii_torrent_paths(torrent) == []  # noqa: S101


def test_extract_upload_error_message_from_alert_html() -> None:
    html = '<div class="alert alert--error"><div>Invalid piece size<br />See <a href="/upload.php">the rules</a>.</div></div>'

    assert PassThePopcorn._extract_upload_error_message(html) == "Invalid piece size See the rules."  # noqa: S101


def test_disc_menu_images_precede_movie_screenshots_in_description() -> None:
    tracker = object.__new__(PassThePopcorn)
    meta = Meta(
        is_disc="DVD",
        image_list=[{"raw_url": "https://imgbox.com/movie.png"}],
        menu_images=[{"raw_url": "https://imgbox.com/original-menu.png"}],
    )
    rehosted_menu = {"raw_url": "https://imgbox.com/menu.png"}
    set_tracker_image_collection(meta, tracker.tracker, "menu_images", [rehosted_menu])

    assert tracker._description_images(meta) == [rehosted_menu, meta.image_list[0]]  # noqa: S101
    assert tracker._description_image_limit(meta, 6) == 7  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_id, result, expired, expected", [(False, "1234", False, "1234"), (True, "1234", False, "5678"), (False, None, False, "5678"), (False, "1234", True, "5678")])
async def test_group_prefetch_reuses_only_fresh_positive_identity_matches(monkeypatch, changed_id, result, expired, expected):
    import time

    tracker = object.__new__(PassThePopcorn)
    tracker.get_group_by_imdb = AsyncMock(return_value="5678")
    meta = Meta(base_dir="test-group-prefetch", uuid="release", imdb_id=12345)
    task = asyncio.create_task(AsyncMock(return_value=result)())
    age = tracker._prefetch_max_age + 1 if expired else 300
    PassThePopcorn._group_prefetch_tasks[tracker._prefetch_key(meta)] = (tracker._group_identity(meta), time.monotonic() - age, task)
    if changed_id:
        meta.imdb_id = 99999

    assert await tracker.get_group_for_meta(meta) == expected  # noqa: S101
    assert tracker.get_group_by_imdb.await_count == (0 if expected == "1234" else 1)  # noqa: S101
    assert not PassThePopcorn._group_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
async def test_early_group_lookup_is_noninteractive_and_cancelled(monkeypatch):
    started = asyncio.Event()
    blocked = asyncio.Event()

    async def lookup(*args, **kwargs):
        assert kwargs["allow_selection"] is False  # noqa: S101
        started.set()
        await blocked.wait()

    monkeypatch.setattr(PassThePopcorn, "_get_group_by_identifier", lookup)
    meta = Meta(base_dir="test-group-prefetch", uuid="release", imdb_id=12345, category="MOVIE", unattended=True, trackers=["PASSTHEPOPCORN"])
    PassThePopcorn.start_group_prefetch(meta, {"DEFAULT": {}, "TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})
    task = PassThePopcorn._group_prefetch_tasks[PassThePopcorn._prefetch_key(meta)][2]
    await asyncio.wait_for(started.wait(), timeout=2)
    await PassThePopcorn.cancel_group_prefetch(meta)

    assert task.cancelled()  # noqa: S101
    assert not PassThePopcorn._group_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["fresh", "expired", "failed", "empty", "cancelled"])
async def test_auth_prefetch_reuses_only_fresh_success(monkeypatch, outcome):
    import time

    tracker = object.__new__(PassThePopcorn)
    token = AsyncMock(return_value="early-token" if outcome != "empty" else "")
    if outcome == "failed":
        token.side_effect = RuntimeError("authentication unavailable")
    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    monkeypatch.setattr(PassThePopcorn, "get_anti_csrf_token", token)
    meta = Meta(base_dir="test-auth-prefetch", source_torrent_hash="hash", unattended=True, trackers=["PTP"])
    config = {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}}
    PassThePopcorn.start_auth_prefetch(meta, config)
    PassThePopcorn.start_auth_prefetch(meta, config)
    task = PassThePopcorn._auth_prefetch_tasks[PassThePopcorn._prefetch_key(meta)]
    if outcome == "cancelled":
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    if outcome == "expired":
        expired_at = time.monotonic() + tracker._prefetch_max_age + 1
        monkeypatch.setattr("src.trackers.GAZELLE.passthepopcorn.time.monotonic", lambda: expired_at)
    tracker.get_anti_csrf_token = AsyncMock(return_value="fallback-token")

    try:
        expected = "early-token" if outcome == "fresh" else "fallback-token"
        assert await tracker.get_upload_anti_csrf_token(meta) == expected  # noqa: S101
        assert tracker.get_anti_csrf_token.await_count == (0 if outcome == "fresh" else 1)  # noqa: S101
        assert token.await_count == (0 if outcome == "cancelled" else 1)  # noqa: S101
        assert not PassThePopcorn._auth_prefetch_tasks  # noqa: S101
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
async def test_auth_prefetch_is_cancelled_with_other_ptp_prefetch(monkeypatch):
    started = asyncio.Event()

    async def authenticate(_self, _meta):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    monkeypatch.setattr(PassThePopcorn, "get_anti_csrf_token", authenticate)
    meta = Meta(base_dir="test-auth-cancellation", uuid="release", unattended=True, trackers=["PASSTHEPOPCORN"])
    PassThePopcorn.start_auth_prefetch(meta, {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})
    task = PassThePopcorn._auth_prefetch_tasks[PassThePopcorn._prefetch_key(meta)]
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
    finally:
        await PassThePopcorn.cancel_prefetch(meta)

    assert task.cancelled()  # noqa: S101
    assert not PassThePopcorn._auth_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
async def test_auth_prefetch_survives_source_hash_discovery(monkeypatch):
    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    token = AsyncMock(return_value="early-token")
    monkeypatch.setattr(PassThePopcorn, "get_anti_csrf_token", token)
    meta = Meta(base_dir="test-auth-path", uuid="release", unattended=True, trackers=["PASSTHEPOPCORN"])
    PassThePopcorn.start_auth_prefetch(meta, {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})
    meta.source_torrent_hash = "discovered-hash"
    tracker = object.__new__(PassThePopcorn)
    try:
        assert await tracker.get_upload_anti_csrf_token(meta) == "early-token"  # noqa: S101
        token.assert_awaited_once()
        assert not PassThePopcorn._auth_prefetch_tasks  # noqa: S101
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
async def test_upload_form_consumes_download_time_authentication(monkeypatch):
    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    token = AsyncMock(return_value="early-token")
    monkeypatch.setattr(PassThePopcorn, "get_anti_csrf_token", token)
    meta = Meta(base_dir="test-auth-form", uuid="release", unattended=True, trackers=["PASSTHEPOPCORN"])
    PassThePopcorn.start_auth_prefetch(meta, {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})
    tracker = object.__new__(PassThePopcorn)

    async def form(_group_id, _meta, prerequisites):
        return "url", {"AntiCsrfToken": await prerequisites["csrf"]}

    tracker._fill_upload_form = form
    tracker._prepare_upload_torrent = AsyncMock(return_value=b"torrent")
    try:
        result = await asyncio.wait_for(tracker.fill_upload_form("1234", meta), timeout=2)
        assert result == ("url", {"AntiCsrfToken": "early-token"})  # noqa: S101
        token.assert_awaited_once()
        assert not PassThePopcorn._auth_prefetch_tasks  # noqa: S101
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{"trackers": ["AITHER"]}, {"unattended": False}, {"unattended_confirm": True}, {"debug": True}, {"edit": True}])
async def test_auth_prefetch_skips_ineligible_uploads(overrides):
    meta = Meta(base_dir="test-auth-skipped", uuid="release", unattended=True, trackers=["PASSTHEPOPCORN"])
    meta.update(overrides)

    PassThePopcorn.start_auth_prefetch(meta, {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})

    assert PassThePopcorn._prefetch_key(meta) not in PassThePopcorn._auth_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
async def test_form_and_torrent_preparation_overlap_and_cache_bytes():
    tracker = object.__new__(PassThePopcorn)
    torrent_started = asyncio.Event()
    form_started = asyncio.Event()

    async def form(*_args):
        form_started.set()
        await torrent_started.wait()
        return "upload-url", {"groupid": "1234"}

    async def torrent(*_args):
        torrent_started.set()
        await form_started.wait()
        return b"torrent"

    tracker._fill_upload_form = form
    tracker._prepare_upload_torrent = torrent
    meta = Meta(base_dir="test", uuid="release")
    result = await asyncio.wait_for(tracker.fill_upload_form("1234", meta), timeout=2)

    assert result == ("upload-url", {"groupid": "1234"})  # noqa: S101
    assert tracker._prepared_upload_torrent == ("test/tmp/release/[PASSTHEPOPCORN].torrent", b"torrent")  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("media_kind", ["files", "dvd"])
async def test_multifile_description_capture_does_not_cancel_upload(monkeypatch, tmp_path, media_kind):
    tracker = object.__new__(PassThePopcorn)
    tracker.config = {"DEFAULT": {}}
    tracker.web_source = False
    tracker.approved_image_hosts = []
    capture = AsyncMock()
    tracker.takescreens_manager = SimpleNamespace(screenshots=capture, dvd_screenshots=capture)
    meta = Meta(
        base_dir=str(tmp_path),
        uuid="release",
        filelist=["first.mkv", "second.mkv"],
        image_list=[],
        retry_count=0,
    )
    if media_kind == "dvd":
        meta.discs = [{"type": "DVD", "name": name, "ifo_mi_full": "IFO", "vob_mi_full": "VOB"} for name in ("first", "second")]
    release_dir = tmp_path / "tmp" / meta.uuid
    release_dir.mkdir(parents=True)
    (release_dir / "MEDIAINFO.txt").write_text("First MediaInfo", encoding="utf-8")
    monkeypatch.setattr("src.description_review.get_base_description", lambda _meta: "")
    monkeypatch.setattr("src.trackers.GAZELLE.passthepopcorn.MediaInfo.parse", lambda *_args, **_kwargs: "Second MediaInfo")

    await tracker.edit_desc(meta)

    capture.assert_awaited_once()
    assert capture.call_args.kwargs["cleanup_after_capture"] is False  # noqa: S101


@pytest.mark.asyncio
async def test_form_failure_cancels_torrent_preparation():
    tracker = object.__new__(PassThePopcorn)
    torrent_started = asyncio.Event()
    cancelled = asyncio.Event()

    async def form(*_args):
        await torrent_started.wait()
        raise ValueError("invalid form")

    async def torrent(*_args):
        torrent_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    tracker._fill_upload_form = form
    tracker._prepare_upload_torrent = torrent
    with pytest.raises(ValueError, match="invalid form"):
        await asyncio.wait_for(tracker.fill_upload_form("1234", Meta()), timeout=2)

    assert cancelled.is_set()  # noqa: S101
    assert tracker._prepared_upload_torrent is None  # noqa: S101


@pytest.mark.asyncio
async def test_upload_success_does_not_wait_for_seeding_setup(monkeypatch):
    import src.trackers.GAZELLE.passthepopcorn as ptp_module

    tracker = object.__new__(PassThePopcorn)
    tracker.config = {}
    tracker.user_agent = "test"
    tracker.announce_url = "https://please.passthepopcorn.me/test/announce"
    tracker.cookie_validator = SimpleNamespace(_load_cookies_dict_secure=lambda _path: {})
    tracker._prepare_upload_torrent = AsyncMock(side_effect=AssertionError("cached torrent must be reused"))
    tracker._extract_upload_error_message = Mock(side_effect=AssertionError("success page must not be parsed for upload errors"))
    meta = Meta(base_dir="test", uuid="release", tracker_status={"PASSTHEPOPCORN": {}})
    tracker._prepared_upload_torrent = ("test/tmp/release/[PASSTHEPOPCORN].torrent", b"torrent")
    result_url = f"{tracker.base_url}/torrents.php?id=1234&torrentid=5678"

    def handle(request):
        if request.method == "POST":
            return httpx.Response(302, headers={"Location": result_url})
        assert str(request.url) == result_url  # noqa: S101
        return httpx.Response(200, text="uploaded")

    original_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original_client(transport=httpx.MockTransport(handle), **kwargs))
    monkeypatch.setattr("src.cookie_auth.find_cookie_file", lambda *_args: "unused")
    rewrite = AsyncMock(side_effect=AssertionError("must run after upload success"))
    monkeypatch.setattr(ptp_module.Common, "create_torrent_ready_to_seed", rewrite)

    assert await tracker.upload(meta, "https://passthepopcorn.me/upload.php", {}) is True  # noqa: S101
    assert meta.tracker_status[tracker.tracker]["upload_success"] is True  # noqa: S101
    assert meta.tracker_status[tracker.tracker]["status_message"] == result_url  # noqa: S101
    rewrite.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", [None, "rewrite", "client"])
async def test_ptp_result_precedes_seeding_and_followup_errors_preserve_success(monkeypatch, failure_stage):
    import src.trackerhandle as handler

    events = []
    meta = Meta(name="release", trackers=["PASSTHEPOPCORN"], print_tracker_messages=True, tracker_status={"PASSTHEPOPCORN": {"upload": True}})
    result_url = "https://passthepopcorn.me/torrents.php?id=1234&torrentid=5678"

    async def upload(*_args):
        meta.tracker_status["PASSTHEPOPCORN"]["status_message"] = result_url
        events.append("upload")
        return True

    async def rewrite(*_args):
        events.append("rewrite")
        if failure_stage == "rewrite":
            raise OSError("rewrite failed")

    async def add(*_args):
        events.append("client")
        if failure_stage == "client":
            raise RuntimeError("client failed")

    tracker = SimpleNamespace(tracker="PASSTHEPOPCORN", fill_upload_form=AsyncMock(return_value=("url", {})), upload=upload, prepare_for_seeding=rewrite)
    monkeypatch.setattr(handler, "PassThePopcorn", lambda **_kwargs: tracker)
    monkeypatch.setattr(handler, "TrackerSetup", lambda **_kwargs: SimpleNamespace(trackers_enabled=lambda _meta: ["PASSTHEPOPCORN"]))
    monkeypatch.setattr(handler, "provision_tracker_torrents", AsyncMock())
    monkeypatch.setattr(handler, "check_tracker_image_hosts", AsyncMock())
    monkeypatch.setattr(handler, "record_event_async", AsyncMock())
    monkeypatch.setattr(handler.logger, "info", lambda message, **_kwargs: events.append("result") if result_url in str(message) else None)

    await handler.process_trackers(meta, {"DEFAULT": {"smart_image_host_selection": False}}, SimpleNamespace(add_to_client=add), [], {}, [], [])

    assert events[:3] == ["upload", "result", "rewrite"]  # noqa: S101
    assert meta.tracker_status["PASSTHEPOPCORN"]["upload_success"] is True  # noqa: S101
    if failure_stage:
        assert meta.tracker_status["PASSTHEPOPCORN"]["post_upload_error"] == f"{failure_stage} failed"  # noqa: S101
    if failure_stage == "rewrite":
        assert "client" not in events  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("group_id, changed_genres, expected_calls", [(None, False, 1), ("1234", False, 0), (None, True, 2)])
async def test_title_prefetch_skips_existing_groups_and_rejects_changed_tags(monkeypatch, group_id, changed_genres, expected_calls):
    import time

    monkeypatch.setattr(PassThePopcorn, "__init__", lambda self, config: None)
    title_lookup = AsyncMock(return_value={"title": "Example", "tags": "drama"})
    monkeypatch.setattr(PassThePopcorn, "_lookup_title_info", title_lookup)
    meta = Meta(base_dir="test-title-prefetch", uuid="release", category="MOVIE", unattended=True, trackers=["PASSTHEPOPCORN"], imdb_id=12345, genres=["Drama"])
    key = PassThePopcorn._prefetch_key(meta)
    group_task = asyncio.create_task(AsyncMock(return_value=group_id)())
    PassThePopcorn._group_prefetch_tasks[key] = (PassThePopcorn._group_identity(meta), time.monotonic(), group_task)
    PassThePopcorn.start_title_prefetch(meta, {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})
    await asyncio.wait_for(PassThePopcorn._title_prefetch_tasks[key][2], timeout=2)
    if changed_genres:
        meta.genres = ["Comedy"]
    try:
        if group_id is None:
            result = await PassThePopcorn({}).get_title_info_for_meta(meta)
            assert result["title"] == "Example"  # noqa: S101
        assert title_lookup.await_count == expected_calls  # noqa: S101
    finally:
        await PassThePopcorn.cancel_prefetch(meta)

    assert not PassThePopcorn._title_prefetch_tasks  # noqa: S101
    assert not PassThePopcorn._group_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
async def test_unattended_form_prerequisites_overlap_without_mutating_live_host(monkeypatch):
    tracker = object.__new__(PassThePopcorn)
    started = {stage: asyncio.Event() for stage in ("csrf", "metadata", "poster", "torrent")}
    release = asyncio.Event()
    meta = Meta(unattended=True, base_dir="test", uuid="release", imdb_id=12345, imghost="imgbb")

    async def csrf(_meta):
        started["csrf"].set()
        await release.wait()
        return "token"

    async def metadata(_meta):
        started["metadata"].set()
        await release.wait()
        return {"title": "Example"}

    async def poster(snapshot, _cover):
        snapshot.imghost = "ptscreens"
        started["poster"].set()
        await release.wait()
        return "https://images.example/rehosted.jpg"

    async def torrent(_meta):
        started["torrent"].set()
        await release.wait()
        return b"torrent"

    async def form(_group_id, _meta, prerequisites):
        await asyncio.gather(*(event.wait() for event in started.values()))
        assert meta.imghost == "imgbb"  # noqa: S101
        release.set()
        results = await asyncio.gather(*(prerequisites[name] for name in ("csrf", "metadata", "poster")))
        assert results == ["token", {"title": "Example"}, "https://images.example/rehosted.jpg"]  # noqa: S101
        return "url", {}

    tracker.get_anti_csrf_token = csrf
    tracker.get_title_info_for_meta = metadata
    tracker.rehost_poster_to_selected_host = poster
    tracker._prepare_upload_torrent = torrent
    tracker._fill_upload_form = form
    monkeypatch.setattr("src.trackers.GAZELLE.passthepopcorn.select_ptp_cover_url", lambda _meta: "https://images.example/poster.jpg")

    assert await asyncio.wait_for(tracker.fill_upload_form(None, meta), timeout=2) == ("url", {})  # noqa: S101
    assert meta.imghost == "imgbb"  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_message", "is_dupe"),
    [
        ("Invalid piece size.", False),
        ('The same base save path "movie.mkv" already exists on the site!', True),
        ("An unrelated upload error already exists on the site!", False),
    ],
)
async def test_upload_failure_preserves_error_message_and_response_html(monkeypatch, tmp_path, error_message, is_dupe):
    tracker = object.__new__(PassThePopcorn)
    tracker.config = {}
    tracker.user_agent = "test"
    tracker.cookie_validator = SimpleNamespace(_load_cookies_dict_secure=lambda _path: {})
    meta = Meta(base_dir=str(tmp_path), uuid="release", tracker_status={"PASSTHEPOPCORN": {}})
    release_dir = tmp_path / "tmp" / "release"
    release_dir.mkdir(parents=True)
    tracker._prepared_upload_torrent = (str(release_dir / "[PASSTHEPOPCORN].torrent"), b"torrent")
    html = f'<div class="alert--error">{error_message}</div>'
    original_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original_client(transport=httpx.MockTransport(lambda _request: httpx.Response(200, text=html)), **kwargs))
    monkeypatch.setattr("src.cookie_auth.find_cookie_file", lambda *_args: "unused")

    assert await tracker.upload(meta, "https://passthepopcorn.me/upload.php", {}) is False  # noqa: S101
    assert error_message in meta.tracker_status[tracker.tracker]["status_message"]  # noqa: S101
    assert bool(meta.tracker_status[tracker.tracker].get("dupe")) is is_dupe  # noqa: S101
    assert not meta.tracker_status[tracker.tracker].get("upload_success")  # noqa: S101
    assert (release_dir / "[PASSTHEPOPCORN]PTP_upload_failure.html").read_text() == html  # noqa: S101
