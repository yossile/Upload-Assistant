import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from src.artwork import select_ptp_cover_url
from src.meta import Meta
from src.tracker_images import set_tracker_image_collection
from src.trackers.GAZELLE.passthepopcorn import PassThePopcorn, PTPGroupLookup, PTPGroupLookupError


@pytest.fixture
def submission_flow(monkeypatch, tmp_path):
    import src.trackerhandle as handler

    tracker = SimpleNamespace(
        tracker="PASSTHEPOPCORN", lookup_group_for_meta=AsyncMock(return_value=PTPGroupLookup(None, [])),
        duplicate_candidates=PassThePopcorn.duplicate_candidates, fill_upload_form=AsyncMock(return_value=("url", {})),
        upload=AsyncMock(return_value=True), prepare_for_seeding=AsyncMock(),
    )
    client = SimpleNamespace(add_to_client=AsyncMock())
    monkeypatch.setattr(handler, "PassThePopcorn", lambda **_kwargs: tracker)
    monkeypatch.setattr(handler, "TrackerSetup", lambda **_kwargs: SimpleNamespace(trackers_enabled=lambda _meta: ["PASSTHEPOPCORN"]))
    monkeypatch.setattr(handler, "provision_tracker_torrents", AsyncMock())
    monkeypatch.setattr(handler, "check_tracker_image_hosts", AsyncMock())
    monkeypatch.setattr(handler, "record_event_async", AsyncMock())
    monkeypatch.setattr(handler, "evaluate_slots", AsyncMock(return_value=None))
    monkeypatch.setattr(handler, "DupeChecker", lambda _config: SimpleNamespace(filter_dupes=AsyncMock(return_value=[])))

    def make_meta(resolution="720p"):
        return Meta(
            base_dir=str(tmp_path), imdb_id=12345, name=f"Example.{resolution}", uuid=resolution, resolution=resolution,
            unattended=True, trackers=["PASSTHEPOPCORN"], tracker_status={"PASSTHEPOPCORN": {"upload": True}},
        )

    async def run(meta):
        await handler.process_trackers(meta, {"DEFAULT": {"smart_image_host_selection": False}}, client, [], {}, [], [])

    return tracker, client, make_meta, run


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_group", [None, "419991"])
@pytest.mark.parametrize("imdb_id,no_imdb", [(12345, False), (0, False), (None, False), (12345, True)])
async def test_early_lookup_is_reused_for_eligibility_but_submission_refreshes(monkeypatch, submission_flow, initial_group, imdb_id, no_imdb):
    import src.trackerstatus as status_module

    submission_tracker, _client, make_meta, run = submission_flow
    initial = PTPGroupLookup(initial_group, [
        {"Quality": "High Definition", "Resolution": "1080p", "ReleaseName": "Example.1080p"},
    ] if initial_group else [])
    refreshed = PTPGroupLookup("419991", [])
    lookup = AsyncMock(side_effect=[initial, refreshed])
    wrong_lookup = AsyncMock(side_effect=AssertionError("Unexpected group lookup route"))
    use_imdb = bool(imdb_id) and not no_imdb
    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, config: None)
    monkeypatch.setattr(PassThePopcorn, "lookup_group", lookup if use_imdb else wrong_lookup)
    monkeypatch.setattr(PassThePopcorn, "lookup_group_by_title", wrong_lookup if use_imdb else lookup)
    monkeypatch.setattr(PassThePopcorn, "get_additional_checks", AsyncMock(return_value=True))
    monkeypatch.setattr(PassThePopcorn, "get_name", AsyncMock(return_value=None))
    monkeypatch.setattr(PassThePopcorn, "start_poster_prefetch", Mock())
    monkeypatch.setattr(status_module, "TrackerSetup", lambda **_kwargs: SimpleNamespace(
        filter_unsupported_trackers=lambda meta: meta,
        check_banned_group=AsyncMock(return_value=False), get_torrent_claims=AsyncMock(return_value=False),
    ))
    monkeypatch.setattr(status_module, "AvistaZNetworkRouter", lambda *_args: SimpleNamespace(apply=AsyncMock()))
    monkeypatch.setattr(status_module, "UploadHelper", lambda _config: SimpleNamespace(
        dupe_check=AsyncMock(side_effect=lambda _dupes, meta, *_args: (False, meta)),
    ))
    monkeypatch.setattr(status_module, "DupeChecker", lambda _config: SimpleNamespace(filter_dupes=AsyncMock(return_value=[])))
    monkeypatch.setattr(status_module, "evaluate_slots", AsyncMock(return_value=None))
    monkeypatch.setattr(status_module, "record_event_async", AsyncMock())
    monkeypatch.setitem(status_module.tracker_class_map, "PASSTHEPOPCORN", PassThePopcorn)
    meta = make_meta()
    meta.category = "MOVIE"
    meta.imdb_id = imdb_id
    meta.no_imdb = no_imdb
    meta.title = "Example"
    meta.year = 2026
    config = {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}}
    submission_tracker.lookup_group_for_meta = PassThePopcorn(config).lookup_group_for_meta
    try:
        PassThePopcorn.start_group_prefetch(meta, config)
        await PassThePopcorn._group_prefetch_tasks[PassThePopcorn._prefetch_key(meta)][2]
        assert lookup.await_count == 1  # noqa: S101
        wrong_lookup.assert_not_awaited()

        assert await status_module.TrackerStatusManager(config).process_all_trackers(meta) == 1  # noqa: S101
        assert lookup.await_count == 1  # noqa: S101
        assert meta.ptp_groupid == initial_group  # noqa: S101
        assert meta.initial_dupes["PASSTHEPOPCORN"] == PassThePopcorn.duplicate_candidates(initial, meta)  # noqa: S101

        await run(meta)
        assert lookup.await_count == 2  # noqa: S101
        assert meta.ptp_groupid == refreshed.group_id  # noqa: S101
        submission_tracker.upload.assert_awaited_once()
        wrong_lookup.assert_not_awaited()
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
async def test_concurrent_submissions_recheck_group_after_first_post_and_unlock_before_seeding(submission_flow):
    from src.resource_coordinator import exclusive_slot

    tracker, client, make_meta, run = submission_flow
    posting = asyncio.Event()
    release_post = asyncio.Event()
    group_id = None
    events = []

    async def lookup(meta):
        events.append(("lookup", meta.resolution, group_id))
        return PTPGroupLookup(group_id, [])

    async def upload(meta, _url, _data):
        nonlocal group_id
        events.append(("post", meta.resolution))
        if group_id is None:
            posting.set()
            await release_post.wait()
            group_id = "419991"
        return True

    async def seed(meta):
        async def acquire():
            async with exclusive_slot("ptp-submission", Path(meta.base_dir) / "tmp" / "locks"):
                events.append(("seed", meta.resolution))
        await asyncio.wait_for(acquire(), timeout=2)

    tracker.lookup_group_for_meta.side_effect = lookup
    tracker.upload.side_effect = upload
    tracker.prepare_for_seeding.side_effect = seed
    first, second = make_meta(), make_meta("1080p")
    first_task = asyncio.create_task(run(first))
    await asyncio.wait_for(posting.wait(), timeout=2)
    second_task = asyncio.create_task(run(second))
    await asyncio.sleep(0)
    assert tracker.lookup_group_for_meta.await_count == 1  # noqa: S101
    release_post.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=5)

    assert [call.args[0] for call in tracker.fill_upload_form.await_args_list] == [None, "419991"]  # noqa: S101
    assert events.index(("post", "720p")) < events.index(("lookup", "1080p", "419991"))  # noqa: S101
    assert client.add_to_client.await_count == 2  # noqa: S101


@pytest.mark.asyncio
async def test_final_lookup_failure_never_builds_or_submits_form(submission_flow):
    tracker, _client, make_meta, run = submission_flow
    tracker.lookup_group_for_meta.side_effect = PTPGroupLookupError("lookup failed")
    meta = make_meta()
    await run(meta)
    tracker.fill_upload_form.assert_not_awaited()
    tracker.upload.assert_not_awaited()
    assert meta.tracker_status["PASSTHEPOPCORN"]["upload"] is False  # noqa: S101


@pytest.mark.asyncio
async def test_new_duplicate_found_under_lock_blocks_submission(monkeypatch, submission_flow):
    import src.trackerhandle as handler

    tracker, _client, make_meta, run = submission_flow
    tracker.lookup_group_for_meta.return_value = PTPGroupLookup("419991", [
        {"Quality": "High Definition", "Resolution": "720p", "ReleaseName": "Example.720p"},
    ])
    monkeypatch.setattr(handler, "DupeChecker", lambda _config: SimpleNamespace(filter_dupes=AsyncMock(return_value=["Example.720p"])))
    meta = make_meta()
    await run(meta)
    tracker.upload.assert_not_awaited()
    assert meta.ptp_groupid == "419991"  # noqa: S101
    assert meta.tracker_status["PASSTHEPOPCORN"]["dupe"] is True  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("imdb_id,no_imdb", [(12345, False), (0, False), (None, False), (12345, True)])
async def test_uncertain_post_blocks_later_submission_and_releases_lock(submission_flow, cancelled, imdb_id, no_imdb):
    from src.resource_coordinator import exclusive_slot

    tracker, _client, make_meta, run = submission_flow
    posting = asyncio.Event()

    async def upload(*_args):
        posting.set()
        if cancelled:
            await asyncio.Event().wait()
        raise httpx.ReadTimeout("outcome unknown")

    tracker.upload.side_effect = upload
    meta = make_meta()
    meta.imdb_id = imdb_id
    meta.no_imdb = no_imdb
    meta.title = "Example"
    meta.year = 2026
    task = asyncio.create_task(run(meta))
    await asyncio.wait_for(posting.wait(), timeout=2)
    if cancelled:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        await task
    markers = list((Path(meta.base_dir) / "tmp" / "locks").glob("ptp-pending-*.json"))
    assert len(markers) == 1  # noqa: S101
    if imdb_id and not no_imdb:
        assert markers[0].name == "ptp-pending-12345.json"  # noqa: S101
    else:
        assert markers[0].name.startswith("ptp-pending-title-")  # noqa: S101
    async with exclusive_slot("ptp-submission", Path(meta.base_dir) / "tmp" / "locks"):
        pass
    retry = meta.copy()
    retry.resolution = "1080p"
    retry.tracker_status = {"PASSTHEPOPCORN": {"upload": True}}
    await run(retry)
    assert tracker.upload.await_count == 1  # noqa: S101
    assert tracker.lookup_group_for_meta.await_count == 2  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("imdb_id,no_imdb", [(0, False), (None, False), (12345, True)])
@pytest.mark.parametrize("other_title,other_year", [("Different", 2026), ("Example", 2025)])
async def test_uncertain_imdb_free_post_does_not_block_other_movies(submission_flow, imdb_id, no_imdb, other_title, other_year):
    tracker, _client, make_meta, run = submission_flow
    tracker.upload.side_effect = [httpx.ReadTimeout("outcome unknown"), True]
    first = make_meta()
    first.imdb_id = imdb_id
    first.no_imdb = no_imdb
    first.title = "Example"
    first.year = 2026
    await run(first)
    assert tracker.upload.await_count == 1  # noqa: S101

    second = make_meta("1080p")
    second.imdb_id = imdb_id
    second.no_imdb = no_imdb
    second.title = other_title
    second.year = other_year
    await run(second)
    assert tracker.upload.await_count == 2  # noqa: S101
    assert second.tracker_status["PASSTHEPOPCORN"]["upload_success"] is True  # noqa: S101
    assert len(list((Path(first.base_dir) / "tmp" / "locks").glob("ptp-pending-*.json"))) == 1  # noqa: S101


@pytest.mark.asyncio
async def test_changed_cover_cancels_blocked_obsolete_poster_without_waiting():
    tracker = object.__new__(PassThePopcorn)
    tracker.config = {"DEFAULT": {"img_host_1": "imgbb"}}
    tracker.rehost_poster_to_selected_host = AsyncMock(return_value="https://host.example/new.jpg")
    snapshot = Meta(base_dir="test-stale-cover", uuid="release", imdb_id=12345, artwork_url="https://images.example/old.jpg")
    current = snapshot.copy()
    current.artwork_url = "https://images.example/new.jpg"
    old_task = asyncio.create_task(asyncio.Event().wait())
    tracker._poster_prefetch_tasks[tracker._prefetch_key(current)] = (snapshot, time.monotonic(), old_task)
    try:
        result = await asyncio.wait_for(tracker.get_poster_for_meta(current, current.artwork_url), timeout=1)
        assert result == "https://host.example/new.jpg"  # noqa: S101
        assert old_task.cancelled()  # noqa: S101
    finally:
        old_task.cancel()
        await asyncio.gather(old_task, return_exceptions=True)
        await tracker.cancel_prefetch(current)


@pytest.mark.asyncio
async def test_concurrent_empty_uuid_posters_use_distinct_files(monkeypatch, tmp_path):
    all_hosting = asyncio.Event()
    paths = []

    async def get(image_url):
        return httpx.Response(200, content=image_url.encode(), request=httpx.Request("GET", image_url))

    async def host(_meta, _count, _total, _offset, _limit, files, _images):
        path = Path(files[0])
        paths.append(path)
        if len(paths) == 2:
            all_hosting.set()
        await all_hosting.wait()
        return [{"raw_url": path.read_bytes().decode()}], 1

    client = _Client()
    client.get = get
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    tracker = object.__new__(PassThePopcorn)
    tracker.config = {"DEFAULT": {"img_host_1": "imgbb"}}
    tracker.uploadscreens_manager = SimpleNamespace(upload_screens=host)
    covers = ["https://images.example/first.jpg", "https://images.example/second.jpg"]
    metas = [Meta(base_dir=str(tmp_path), uuid="", source_torrent_hash=f"hash-{index}") for index in range(2)]
    results = await asyncio.wait_for(asyncio.gather(*(
        tracker.rehost_poster_to_selected_host(meta, cover) for meta, cover in zip(metas, covers, strict=True)
    )), timeout=2)
    assert paths[0] != paths[1]  # noqa: S101
    assert results == covers  # noqa: S101


class _Client:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

@pytest.mark.asyncio
async def test_combined_imdb_lookup_returns_group_and_nested_torrents(monkeypatch):
    torrents = [{"Quality": "High Definition", "Resolution": "720p", "ReleaseName": "Example.720p"}]
    payload = {"Page": "Browse", "TotalResults": "1", "Movies": [{"ImdbId": "9876", "GroupId": "1234", "Torrents": torrents}]}
    client = _Client()
    client.get = AsyncMock(return_value=httpx.Response(200, json=payload, request=httpx.Request("GET", "https://example.test")))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user = "user"
    tracker.api_key = "key"
    tracker.user_agent = "agent"

    result = await tracker.lookup_group(9876)
    assert result.group_id == "1234"  # noqa: S101
    assert tracker.duplicate_candidates(result, Meta(resolution="1080p")) == ["[720p] Example.720p"]  # noqa: S101
    assert client.get.await_count == 1  # noqa: S101
    assert client.get.await_args.kwargs["params"] == {  # noqa: S101
        "action": "advanced", "searchstr": "https://www.imdb.com/title/tt0009876/", "json": "noredirect", "jsontrumpable": "1"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    {}, {"Page": "Browse"}, {"Page": "Browse", "TotalResults": "1", "Movies": []},
    {"Page": "Browse", "TotalResults": "1", "Movies": [{"ImdbId": "9999", "GroupId": "1234", "Torrents": []}]},
    {"Page": "Browse", "TotalResults": "2", "Movies": [{"ImdbId": "9876"}, {"ImdbId": "9876"}]},
])
async def test_group_lookup_never_treats_unverified_results_as_absent(monkeypatch, payload):
    client = _Client()
    client.get = AsyncMock(return_value=httpx.Response(200, json=payload, request=httpx.Request("GET", "https://example.test")))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user = "user"
    tracker.api_key = "key"
    tracker.user_agent = "agent"

    with pytest.raises(PTPGroupLookupError):
        await tracker.lookup_group(9876)
    assert not tracker._group_lookup_succeeded  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("movie_fields", [{}, {"Movies": None}, {"Movies": []}])
async def test_group_lookup_accepts_only_confirmed_zero_results(monkeypatch, movie_fields):
    client = _Client()
    payload = {"Page": "Browse", "TotalResults": "0", **movie_fields}
    client.get = AsyncMock(return_value=httpx.Response(200, json=payload, request=httpx.Request("GET", "https://example.test")))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user, tracker.api_key, tracker.user_agent = "user", "key", "agent"
    assert (await tracker.lookup_group(9876)).group_id is None  # noqa: S101
    assert tracker._group_lookup_succeeded  # noqa: S101
    assert (await tracker.lookup_group_for_meta(Meta(title="Example", year=2026, tmdb_id=123))).group_id is None  # noqa: S101
    assert client.get.await_args.kwargs["params"] == {  # noqa: S101
        "action": "advanced", "searchstr": "Example", "json": "noredirect", "jsontrumpable": "1",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("details", [False, True])
@pytest.mark.parametrize("no_imdb", [False, True])
async def test_imdb_free_group_lookup_matches_title_and_year(monkeypatch, details, no_imdb):
    torrents = [{"Quality": "High Definition", "Resolution": "720p", "ReleaseName": "Example.720p"}]
    movie = {"Title": "Example", "Year": "2026", "ImdbId": "0", "GroupId": "1234", "Torrents": torrents}
    payload = {"Page": "Details", **movie} if details else {"Page": "Browse", "TotalResults": "1", "Movies": [movie]}
    client = _Client()
    client.get = AsyncMock(return_value=httpx.Response(200, json=payload, request=httpx.Request("GET", "https://example.test")))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user, tracker.api_key, tracker.user_agent = "user", "key", "agent"

    result = await tracker.lookup_group_for_meta(Meta(title="Example", year=2026, no_imdb=no_imdb))
    assert result == PTPGroupLookup("1234", torrents)  # noqa: S101
    assert tracker._group_lookup_succeeded  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("movies", [
    [{"Title": "Example", "Year": "2026", "GroupId": "1234"}],
    [{"GroupId": "1234", "Torrents": []}],
    [{"Title": "Example", "Year": "2026", "GroupId": "1234", "Torrents": []}] * 2,
])
async def test_title_lookup_rejects_unverified_groups(monkeypatch, movies):
    client = _Client()
    payload = {"Page": "Browse", "TotalResults": str(len(movies)), "Movies": movies}
    client.get = AsyncMock(return_value=httpx.Response(200, json=payload, request=httpx.Request("GET", "https://example.test")))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    tracker = object.__new__(PassThePopcorn)
    tracker.api_user, tracker.api_key, tracker.user_agent = "user", "key", "agent"

    with pytest.raises(PTPGroupLookupError):
        await tracker.lookup_group_for_meta(Meta(title="Example", year=2026))
    assert not tracker._group_lookup_succeeded  # noqa: S101


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
@pytest.mark.parametrize("changed_id, result, expired, expected", [(False, PTPGroupLookup("1234", []), False, "1234"), (False, PTPGroupLookup(None, []), False, None), (True, PTPGroupLookup("1234", []), False, "5678"), (False, None, False, "5678"), (False, PTPGroupLookup("1234", []), True, "5678")])
async def test_group_prefetch_reuses_only_fresh_identity_matches(monkeypatch, changed_id, result, expired, expected):
    import time

    tracker = object.__new__(PassThePopcorn)
    tracker.lookup_group_for_meta = AsyncMock(return_value=PTPGroupLookup("5678", []))
    meta = Meta(base_dir="test-group-prefetch", uuid="release", imdb_id=12345)
    task = asyncio.create_task(AsyncMock(return_value=result)())
    age = tracker._prefetch_max_age + 1 if expired else 300
    PassThePopcorn._group_prefetch_tasks[tracker._prefetch_key(meta)] = (tracker._group_identity(meta), time.monotonic() - age, task)
    if changed_id:
        meta.imdb_id = 99999

    assert await tracker.get_group_for_meta(meta) == expected  # noqa: S101
    assert tracker.lookup_group_for_meta.await_count == (1 if expected == "5678" else 0)  # noqa: S101
    assert not PassThePopcorn._group_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
async def test_early_group_lookup_is_noninteractive_and_cancelled(monkeypatch):
    started = asyncio.Event()
    blocked = asyncio.Event()

    async def lookup(*_args, **_kwargs):
        started.set()
        await blocked.wait()

    monkeypatch.setattr(PassThePopcorn, "lookup_group", lookup)
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
async def test_ptp_result_precedes_seeding_and_followup_errors_preserve_success(monkeypatch, tmp_path, failure_stage):
    import src.trackerhandle as handler

    events = []
    meta = Meta(base_dir=str(tmp_path), imdb_id=12345, name="release", trackers=["PASSTHEPOPCORN"], print_tracker_messages=True, tracker_status={"PASSTHEPOPCORN": {"upload": True}})
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

    tracker = SimpleNamespace(
        tracker="PASSTHEPOPCORN", fill_upload_form=AsyncMock(return_value=("url", {})), upload=upload, prepare_for_seeding=rewrite,
        lookup_group_for_meta=AsyncMock(return_value=PTPGroupLookup("1234", [])), duplicate_candidates=PassThePopcorn.duplicate_candidates,
    )
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
    group_task = asyncio.create_task(AsyncMock(return_value=PTPGroupLookup(group_id, []))())
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
@pytest.mark.parametrize("group_id, succeeded, expected_calls", [(None, True, 1), ("123", True, 0), (None, False, 0)])
async def test_early_poster_starts_only_for_confirmed_new_groups(monkeypatch, group_id, succeeded, expected_calls):
    async def lookup(tracker, *_args, **_kwargs):
        if not succeeded:
            raise PTPGroupLookupError("lookup failed")
        return PTPGroupLookup(group_id, [])

    monkeypatch.setattr(PassThePopcorn, "lookup_group", lookup)
    start_poster = Mock()
    monkeypatch.setattr(PassThePopcorn, "start_poster_prefetch", start_poster)
    meta = Meta(base_dir="test-poster-prefetch", uuid="release", imdb_id=12345, category="MOVIE", unattended=True, trackers=["PTP"])
    config = {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}}
    try:
        PassThePopcorn.start_group_prefetch(meta, config)
        await PassThePopcorn._group_prefetch_tasks[PassThePopcorn._prefetch_key(meta)][2]
        assert start_poster.call_count == expected_calls  # noqa: S101
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["none", "identity", "cover", "host", "expired"])
async def test_early_poster_is_hosted_before_submission_and_reused_only_when_matching(monkeypatch, changed):
    import time

    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    rehost = AsyncMock(return_value="https://host.example/hosted.jpg")
    monkeypatch.setattr(PassThePopcorn, "rehost_poster_to_selected_host", rehost)
    meta = Meta(
        base_dir="test-poster-prefetch", uuid="release", imdb_id=12345, category="MOVIE", unattended=True,
        trackers=["PTP"], artwork_url="https://images.example/original.jpg", imghost="imgbb",
    )
    config = {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}}
    tracker = object.__new__(PassThePopcorn)
    tracker.config = config
    try:
        PassThePopcorn.start_poster_prefetch(meta, config)
        key = PassThePopcorn._prefetch_key(meta)
        snapshot, _started_at, task = PassThePopcorn._poster_prefetch_tasks[key]
        await task
        rehost.assert_awaited_once()
        if changed == "identity":
            meta.imdb_id = 99999
        elif changed == "cover":
            meta.artwork_url = "https://images.example/other.jpg"
        elif changed == "host":
            meta.imghost = "imgbox"
        elif changed == "expired":
            PassThePopcorn._poster_prefetch_tasks[key] = (snapshot, time.monotonic() - tracker._prefetch_max_age - 1, task)
        assert await tracker.get_poster_for_meta(meta, meta.artwork_url) == "https://host.example/hosted.jpg"  # noqa: S101
        assert rehost.await_count == (1 if changed == "none" else 2)  # noqa: S101
        assert not PassThePopcorn._poster_prefetch_tasks  # noqa: S101
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
async def test_early_poster_discovers_imdb_cover_before_submission(monkeypatch):
    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    cover = "https://images.example/original.jpg"
    imdb_lookup = AsyncMock(return_value={"cover": cover})
    monkeypatch.setattr("src.imdb.ImdbManager.get_imdb_info_api", imdb_lookup)
    rehost = AsyncMock(return_value="https://host.example/hosted.jpg")
    monkeypatch.setattr(PassThePopcorn, "rehost_poster_to_selected_host", rehost)
    meta = Meta(base_dir="test-poster-discovery", uuid="release", imdb_id=12345, category="MOVIE", unattended=True, trackers=["PTP"])
    config = {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}}
    tracker = object.__new__(PassThePopcorn)
    tracker.config = config
    try:
        PassThePopcorn.start_poster_prefetch(meta, config)
        await PassThePopcorn._poster_prefetch_tasks[PassThePopcorn._prefetch_key(meta)][2]
        imdb_lookup.assert_awaited_once()
        rehost.assert_awaited_once()
        meta.imdb_info = {"cover": cover}
        assert await tracker.get_poster_for_meta(meta, cover) == "https://host.example/hosted.jpg"  # noqa: S101
        rehost.assert_awaited_once()
    finally:
        await PassThePopcorn.cancel_prefetch(meta)


@pytest.mark.asyncio
async def test_cancel_prefetch_cancels_background_poster(monkeypatch):
    monkeypatch.setattr(PassThePopcorn, "__init__", lambda _self, _config: None)
    started = asyncio.Event()
    release = asyncio.Event()

    async def rehost(_tracker, _meta, _cover):
        started.set()
        await release.wait()

    monkeypatch.setattr(PassThePopcorn, "rehost_poster_to_selected_host", rehost)
    meta = Meta(
        base_dir="test-poster-cancellation", uuid="release", imdb_id=12345, category="MOVIE", unattended=True,
        trackers=["PTP"], artwork_url="https://images.example/original.jpg",
    )
    try:
        PassThePopcorn.start_poster_prefetch(meta, {"TRACKERS": {"PASSTHEPOPCORN": {"api_key": "test"}}})
        task = PassThePopcorn._poster_prefetch_tasks[PassThePopcorn._prefetch_key(meta)][2]
        await asyncio.wait_for(started.wait(), timeout=2)
    finally:
        await PassThePopcorn.cancel_prefetch(meta)
    assert task.cancelled()  # noqa: S101
    assert not PassThePopcorn._poster_prefetch_tasks  # noqa: S101


@pytest.mark.asyncio
@pytest.mark.parametrize("hosted", [True, False])
async def test_ptp_poster_preserves_original_bytes_and_url_without_shared_size_validation(monkeypatch, tmp_path, hosted):
    cover = "https://images.example/original.jpg"
    content = b"original poster bytes forwarded without decoding"
    response = httpx.Response(200, content=content, headers={"content-length": "100000000"}, request=httpx.Request("GET", cover))
    client = _Client()
    client.get = AsyncMock(return_value=response)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setattr("src.artwork.is_valid_image_bytes", Mock(side_effect=AssertionError("PTP must not apply shared image limits")))
    tracker = object.__new__(PassThePopcorn)
    tracker.config = {"DEFAULT": {"img_host_1": "imgbb"}}
    uploaded = [{"raw_url": "https://host.example/hosted.jpg"}] if hosted else []
    upload_images = AsyncMock(return_value=(uploaded, len(uploaded)))
    tracker.uploadscreens_manager = SimpleNamespace(upload_screens=upload_images)
    meta = Meta(base_dir=str(tmp_path), uuid="poster", imghost="imgbb")

    result = await tracker.rehost_poster_to_selected_host(meta, cover)

    assert result == ("https://host.example/hosted.jpg" if hosted else cover)  # noqa: S101
    upload_images.assert_awaited_once()
    poster_path = Path(upload_images.await_args.args[5][0])
    assert poster_path.parent == tmp_path / "tmp" / "poster" / "artwork"  # noqa: S101
    assert poster_path.read_bytes() == content  # noqa: S101
    assert meta.imghost == "imgbb"  # noqa: S101


@pytest.mark.asyncio
async def test_existing_group_form_does_not_prepare_poster(monkeypatch):
    tracker = object.__new__(PassThePopcorn)
    meta = Meta(unattended=True, base_dir="test", uuid="existing-group", imdb_id=12345)
    tracker.get_upload_anti_csrf_token = AsyncMock(return_value="token")
    tracker.get_title_info_for_meta = AsyncMock()
    tracker.rehost_poster_to_selected_host = AsyncMock()
    tracker._prepare_upload_torrent = AsyncMock(return_value=b"torrent")
    tracker._fill_upload_form = AsyncMock(return_value=("url", {"groupid": "123"}))
    select_cover = Mock(side_effect=AssertionError("existing groups must not select a cover"))
    monkeypatch.setattr("src.trackers.GAZELLE.passthepopcorn.select_ptp_cover_url", select_cover)

    assert await tracker.fill_upload_form("123", meta) == ("url", {"groupid": "123"})  # noqa: S101
    select_cover.assert_not_called()
    tracker.get_title_info_for_meta.assert_not_awaited()
    tracker.rehost_poster_to_selected_host.assert_not_awaited()


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
