"""Tracker-specific screenshot discovery with shared download and reuse logic."""

import asyncio
import random
import re
from collections.abc import Mapping
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol

import httpx
from PIL import Image

from src.config_helpers import parse_bool
from src.console import logger
from src.meta import Meta
from src.screenshot_manifest import files, register
from src.screenshot_overlays import overlays_active
from src.temp_paths import screenshots_dir
from src.tracker_api import fetch_unit3d_torrent
from src.uploadscreens import UploadScreensManager


@dataclass(frozen=True)
class ScreenshotCandidates:
    urls: list[str]
    height: int


class ScreenshotSource(Protocol):
    async def discover(self, session: httpx.AsyncClient, torrent_id: str, settings: Mapping[str, Any]) -> ScreenshotCandidates: ...


class AitherScreenshotSource:
    async def discover(self, session: httpx.AsyncClient, torrent_id: str, settings: Mapping[str, Any]) -> ScreenshotCandidates:
        response = await fetch_unit3d_torrent(session, f"https://aither.cc/api/torrents/{int(torrent_id)}", str(settings["api_key"]))
        response.raise_for_status()
        payload = response.json()
        attributes = payload.get("data", payload).get("attributes", {})
        resolution = re.fullmatch(r"(\d+)[pi]", str(attributes.get("resolution", "")))
        if resolution is None or attributes.get("category") not in ("Movie", "TV"):
            return ScreenshotCandidates([], 0)
        urls = re.findall(
            r"\[url=https?://[^\]]+\]\s*\[img(?:=[^\]]*)?\]\s*"
            r"(https://file\.aither\.cc/u/[A-Za-z0-9_-]+\.(?:png|jpe?g))\s*\[/img\]\s*\[/url\]",
            attributes.get("description") or "",
            re.IGNORECASE,
        )
        return ScreenshotCandidates(list(dict.fromkeys(urls)), int(resolution.group(1)))


SCREENSHOT_SOURCES: dict[str, ScreenshotSource] = {"AITHER": AitherScreenshotSource()}
MAX_IMAGE_BYTES = 20 * 1024 * 1024


async def prepare_tracker_screenshots(meta: Meta, config: Mapping[str, Any], tracker_ids: Mapping[str, str]) -> None:
    """Prepare source images without requiring any downloaded media bytes."""
    defaults = config.get("DEFAULT", {})
    if (
        meta.screens <= 0
        or meta.category in ("BOOK", "GAME", "MUSIC")
        or meta.manual_frames
        or meta.frame_overlay
        or overlays_active(defaults)
        or meta.retake
    ):
        return
    count = meta.screens - len(files(meta.base_dir, meta.uuid, "main"))
    if count <= 0 or meta.image_list:
        return
    for tracker, torrent_id in tracker_ids.items():
        tracker = tracker.upper()
        source = SCREENSHOT_SOURCES.get(tracker)
        settings = config.get("TRACKERS", {}).get(tracker, {})
        if source is None or not parse_bool(settings.get("pre_upload_screenshots", False)) or not settings.get("api_key"):
            continue
        try:
            async with httpx.AsyncClient(timeout=15.0) as session:
                candidates = await source.discover(session, torrent_id, settings)
                urls = list(dict.fromkeys(candidates.urls))
                random.SystemRandom().shuffle(urls)
                directory = screenshots_dir(meta.base_dir, meta.uuid)
                directory.mkdir(parents=True, exist_ok=True)
                for index, url in enumerate(urls):
                    if count <= 0:
                        break
                    try:
                        content = bytearray()
                        async with session.stream("GET", url) as response:
                            response.raise_for_status()
                            async for chunk in response.aiter_bytes():
                                content.extend(chunk)
                                if len(content) > MAX_IMAGE_BYTES:
                                    raise ValueError("Screenshot exceeds download limit")
                        with Image.open(BytesIO(content)) as image:
                            if image.format not in ("PNG", "JPEG") or not candidates.height * 0.70 <= image.height <= candidates.height or image.width <= image.height:
                                continue
                            suffix = ".png" if image.format == "PNG" else ".jpg"
                            image.verify()
                        destination = directory / f"source-{index}{suffix}"
                        await asyncio.to_thread(destination.write_bytes, content)
                        register(meta.base_dir, meta.uuid, [destination], "main")
                        count -= 1
                    except (httpx.HTTPError, OSError, ValueError, Image.DecompressionBombError):
                        logger.debug(f"{tracker} screenshot could not be downloaded or validated; trying another image.")
            logger.info(f"[cyan]Prepared {meta.screens - count} source screenshots before download completion.[/cyan]")
        except Exception:
            logger.warning(f"[yellow]{tracker} pre-upload screenshots unavailable; local capture remains enabled.[/yellow]")
        if count <= 0:
            try:
                upload_meta = meta.copy()
                upload_meta.imghost = upload_meta.imghost or str(defaults.get("img_host_1") or "")
                if upload_meta.imghost:
                    images, _uploaded_count = await UploadScreensManager(dict(config)).upload_screens(upload_meta, meta.screens, 1, 0, meta.screens, [], {})
                    if len(images) >= meta.screens:
                        meta.image_list = images
                        meta.image_sizes = upload_meta.image_sizes
                        meta.imghost = upload_meta.imghost
            except Exception:
                logger.warning("[yellow]Early screenshot re-hosting failed; normal upload will retry using the downloaded images.[/yellow]")
            break
