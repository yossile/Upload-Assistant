# ruff: noqa: S101

import asyncio

import httpx
import pytest

from src.get_tracker_data import TrackerDataManager
from src.meta import Meta
from src.pre_upload_screenshots import AitherScreenshotSource
from src.tracker_api import fetch_unit3d_torrent
from src.trackers.common import Common


class _FakeTrackerMetadataManager:
    def __init__(self) -> None:
        self.calls = 0

    async def update_metadata_from_tracker(
        self,
        _tracker,
        _instance,
        meta,
        _search_term,
        _search_file_folder,
        _skip_descriptions,
        *,
        torrent_id="",
    ):
        self.calls += 1
        assert torrent_id == "12345"
        meta.imdb_id = 9876543
        meta.description = "Cached tracker description"
        return meta, True


def test_explicit_tracker_id_reuses_cached_metadata(tmp_path):
    async def run():
        config = {
            "DEFAULT": {
                "tracker_metadata_cache_enabled": True,
                "tracker_metadata_cache_dir": "tracker-cache",
                "tracker_metadata_cache_ttl_hours": 24,
            },
            "TRACKERS": {},
        }
        first_manager = TrackerDataManager(config)
        first_fake = _FakeTrackerMetadataManager()
        first_manager.tracker_meta_manager = first_fake
        first_meta = Meta({"base_dir": str(tmp_path), "tracker_ids": {"PASSTHEPOPCORN": "12345"}})
        _, first_match = await first_manager.update_metadata_from_explicit_tracker("PASSTHEPOPCORN", object(), first_meta, "Invented Romance", "Invented Romance", False)

        second_manager = TrackerDataManager(config)
        second_fake = _FakeTrackerMetadataManager()
        second_manager.tracker_meta_manager = second_fake
        second_meta = Meta({"base_dir": str(tmp_path), "tracker_ids": {"PASSTHEPOPCORN": "12345"}})
        _, second_match = await second_manager.update_metadata_from_explicit_tracker("PASSTHEPOPCORN", object(), second_meta, "Invented Romance", "Invented Romance", False)

        assert first_match and second_match
        assert first_fake.calls == 1
        assert second_fake.calls == 0
        assert second_meta.imdb_id == 9876543
        assert second_meta.description == "Cached tracker description"

    asyncio.run(run())


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["screenshots_first", "metadata_first", "concurrent"])
async def test_screenshots_and_ids_share_one_detail_request(monkeypatch, order):
    requests = []
    request_started = asyncio.Event()
    response_ready = asyncio.Event()
    screenshot_url = "https://file.aither.cc/u/screenshot.png"

    async def handler(request):
        requests.append(request)
        request_started.set()
        await response_ready.wait()
        return httpx.Response(200, json={"attributes": {
            "tmdb_id": 949,
            "imdb_id": 113277,
            "category": "Movie",
            "resolution": "720p",
            "description": f"[img=500]{screenshot_url}[/img]",
        }})

    client_class = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client_class(transport=httpx.MockTransport(handler)))
    settings = {"api_key": "test-key", "pre_upload_screenshots": True}
    config = {"TRACKERS": {"AITHER": settings}}

    async def screenshots():
        async with httpx.AsyncClient() as session:
            return await AitherScreenshotSource().discover(session, "123", settings)

    async def metadata():
        return await Common(config).unit3d_torrent_info(
            "AITHER", "https://aither.cc/api/torrents/", "https://aither.cc/api/torrents/filter",
            Meta(unattended=True), id="123", skip_tracker_descriptions=True,
        )

    if order == "concurrent":
        pending = asyncio.gather(screenshots(), metadata())
        await request_started.wait()
        response_ready.set()
        candidates, result = await pending
    else:
        response_ready.set()
        if order == "screenshots_first":
            candidates = await screenshots()
            result = await metadata()
        else:
            result = await metadata()
            candidates = await screenshots()

    assert len(requests) == 1
    assert result[:2] == (949, 113277)
    assert candidates.urls == [screenshot_url]
    assert candidates.height == 720
    assert requests[0].headers["Authorization"] == "Bearer test-key"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["http", "json", "empty"])
async def test_failed_tracker_detail_response_can_be_retried(failure):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            if failure == "http":
                return httpx.Response(429)
            if failure == "json":
                return httpx.Response(200, content=b"not json")
            return httpx.Response(200, json={"attributes": {}})
        return httpx.Response(200, json={"attributes": {"tmdb_id": 949}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as session:
        if failure == "json":
            with pytest.raises(ValueError):
                await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")
        else:
            await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")
        response = await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")
        reused = await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")

    assert len(requests) == 2
    assert response is reused
    assert response.json()["attributes"]["tmdb_id"] == 949


@pytest.mark.asyncio
async def test_tracker_detail_response_is_scoped_to_endpoint_and_account(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"attributes": {"tmdb_id": 949}})

    now = 0.0
    monkeypatch.setattr("src.tracker_api.time.monotonic", lambda: now)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as session:
        await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")
        await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")
        await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/456", "test-key")
        await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "another-key")
        assert len(requests) == 3
        now = 24 * 60 * 60 + 1
        await fetch_unit3d_torrent(session, "https://aither.cc/api/torrents/123", "test-key")
    assert len(requests) == 4
