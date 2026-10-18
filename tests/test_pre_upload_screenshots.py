# ruff: noqa: S101

from io import BytesIO
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from PIL import Image

from src.meta import Meta
from src.pre_upload_screenshots import SCREENSHOT_SOURCES, AitherScreenshotSource, ScreenshotCandidates, prepare_tracker_screenshots
from src.screenshot_manifest import files


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "logo,position,thumbnail_size",
    [("aaC2N2nf0VBxR2Mp.png", "before", 500), ("y6nfEPrCxpW57juD.png", "after", 350)],
)
@pytest.mark.parametrize("has_screenshots", [False, True])
async def test_aither_discovery_excludes_standalone_logos(logo, position, thumbnail_size, has_screenshots):
    logo_url = f"https://file.aither.cc/u/{logo}"
    artwork = f"[center][img=200]{logo_url}[/img][/center]"
    screenshot_urls = [f"https://file.aither.cc/u/shot{index}.png" for index in range(6)] if has_screenshots else []
    screenshots = "[center]" + "\n".join(
        f"[url=https://onlyimage.org/image/shot{index}]\n[img={thumbnail_size}]{url}[/img]\n[/url]"
        for index, url in enumerate(screenshot_urls)
    ) + "[/center]"
    description = artwork + screenshots if position == "before" else screenshots + artwork

    def handler(_request):
        return httpx.Response(200, json={"attributes": {"category": "Movie", "resolution": "1080p", "description": description}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as session:
        candidates = await AitherScreenshotSource().discover(session, "123", {"api_key": "test-key"})

    assert candidates.urls == screenshot_urls
    assert logo_url not in candidates.urls


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_source_images_registered_without_media(tmp_path, wrapped):
    image_data = BytesIO()
    Image.new("RGB", (1280, 720)).save(image_data, format="PNG")
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.host == "aither.cc":
            payload = {"attributes": {"category": "Movie", "resolution": "720p", "description": "".join(
                f"[url=https://other.example/{index}.png][img=500]https://file.aither.cc/u/shot{index}.png[/img][/url]" for index in range(6)
            )}}
            return httpx.Response(200, json={"data": payload} if wrapped else payload)
        return httpx.Response(200, content=image_data.getvalue())

    session = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    meta = Meta(base_dir=str(tmp_path), uuid="release", screens=4)
    config = {"DEFAULT": {"img_host_1": "imgbb"}, "TRACKERS": {"AITHER": {"api_key": "test-key", "pre_upload_screenshots": True}}}

    async def rehost(upload_meta, *_args):
        upload_meta.image_list = [{"img_url": f"https://host.example/{index}.png", "raw_url": f"https://host.example/{index}.png", "web_url": f"https://host.example/{index}"} for index in range(4)]
        return upload_meta.image_list, 4

    with (
        patch("src.pre_upload_screenshots.httpx.AsyncClient", return_value=session),
        patch("src.pre_upload_screenshots.UploadScreensManager.upload_screens", new_callable=AsyncMock, side_effect=rehost) as upload,
    ):
        await prepare_tracker_screenshots(meta, config, {"AITHER": "123"})

    assert len(files(tmp_path, "release", "main")) == 4
    assert len(requests) == 5
    assert all(request.url.host == "file.aither.cc" for request in requests[1:])
    upload.assert_awaited_once()
    assert len(meta.image_list) == 4
    assert all(image["raw_url"].startswith("https://host.example/") for image in meta.image_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("tracker,enabled", [("AITHER", False), ("LST", True)])
async def test_beta_is_opt_in_and_aither_only(tmp_path, tracker, enabled):
    meta = Meta(base_dir=str(tmp_path), uuid="release", screens=4)
    config = {"TRACKERS": {tracker: {"api_key": "test-key", "pre_upload_screenshots": enabled}}}
    with patch("src.pre_upload_screenshots.httpx.AsyncClient") as session:
        await prepare_tracker_screenshots(meta, config, {tracker: "123"})
    session.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["partial", "api", "host", "partial_host"])
async def test_failures_keep_local_capture_or_upload_fallback(tmp_path, failure):
    image_data = BytesIO()
    Image.new("RGB", (1280, 720)).save(image_data, format="PNG")
    downloaded = []

    def handler(request):
        if request.url.host == "aither.cc":
            if failure == "api":
                return httpx.Response(503)
            description = "".join(f"[url=https://images.example/{name}][img]https://file.aither.cc/u/{name}.png[/img][/url]" for name in ("broken", "banner", "valid", "valid", "second"))
            return httpx.Response(200, json={"attributes": {"category": "Movie", "resolution": "720p", "description": description}})
        downloaded.append(str(request.url))
        if "broken" in request.url.path:
            return httpx.Response(200, content=b"not an image")
        if "banner" in request.url.path:
            banner = BytesIO()
            Image.new("RGB", (1280, 100)).save(banner, format="PNG")
            return httpx.Response(200, content=banner.getvalue())
        return httpx.Response(200, content=image_data.getvalue())

    meta = Meta(base_dir=str(tmp_path), uuid="release", screens=4 if failure == "partial" else 2)
    config = {"DEFAULT": {"img_host_1": "imgbb"}, "TRACKERS": {"AITHER": {"api_key": "test-key", "pre_upload_screenshots": True}}}

    async def rehost(upload_meta, *_args):
        if failure == "host":
            raise RuntimeError("host unavailable")
        upload_meta.image_list.append({"raw_url": "https://host.example/one.png"})
        return upload_meta.image_list, 1

    with (
        patch("src.pre_upload_screenshots.httpx.AsyncClient", return_value=httpx.AsyncClient(transport=httpx.MockTransport(handler))),
        patch("src.pre_upload_screenshots.random.SystemRandom.shuffle", side_effect=lambda _urls: None),
        patch("src.pre_upload_screenshots.UploadScreensManager.upload_screens", new_callable=AsyncMock, side_effect=rehost) as upload,
    ):
        await prepare_tracker_screenshots(meta, config, {"aither": "123"})
    assert len(files(tmp_path, "release", "main")) == (0 if failure == "api" else 2)
    assert len(downloaded) == len(set(downloaded))
    assert meta.image_list == []
    if failure in ("host", "partial_host"):
        upload.assert_awaited_once()
    else:
        upload.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{"screens": 0}, {"manual_frames": "1,2"}, {"frame_overlay": True}, {"retake": True}, {"category": "BOOK"}])
async def test_explicit_capture_options_skip_source_images(tmp_path, overrides):
    meta = Meta({"base_dir": str(tmp_path), "uuid": "release", "screens": 4, **overrides})
    config = {"TRACKERS": {"AITHER": {"api_key": "test-key", "pre_upload_screenshots": True}}}
    with patch("src.pre_upload_screenshots.httpx.AsyncClient") as session:
        await prepare_tracker_screenshots(meta, config, {"AITHER": "123"})
    session.assert_not_called()


@pytest.mark.asyncio
async def test_additional_adapter_does_not_require_shared_downloader_changes(tmp_path, monkeypatch):
    image_data = BytesIO()
    Image.new("RGB", (1920, 1080)).save(image_data, format="JPEG")

    class OtherSource:
        async def discover(self, _session, torrent_id, _settings):
            assert torrent_id == "456"
            return ScreenshotCandidates(["https://other.example/image?id=42"], 1080)

    monkeypatch.setitem(SCREENSHOT_SOURCES, "OTHER", OtherSource())
    session = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200, content=image_data.getvalue())))
    meta = Meta(base_dir=str(tmp_path), uuid="release", screens=1)
    config = {"TRACKERS": {"OTHER": {"api_key": "test-key", "pre_upload_screenshots": True}}}
    with patch("src.pre_upload_screenshots.httpx.AsyncClient", return_value=session):
        await prepare_tracker_screenshots(meta, config, {"OTHER": "456"})
    screenshots = files(tmp_path, "release", "main")
    assert len(screenshots) == 1
    assert screenshots[0].suffix == ".jpg"

