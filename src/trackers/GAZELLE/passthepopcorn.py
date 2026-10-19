# Upload Assistant © 2025 Audionut & wastaken7 — Licensed under UAPL v1.0
import asyncio
import contextlib
import glob
import io
import json
import platform
import re
import time
from collections.abc import Awaitable
from pathlib import Path
from typing import Any, ClassVar, cast
from urllib.parse import urlparse

import aiofiles
import cli_ui
import click
import httpx
from bs4 import BeautifulSoup
from rich.markup import escape
from torf import Torrent

from src.artwork import select_ptp_cover_url
from src.bbcode import BBCODE
from src.cogs.redaction import PathAwareEncoder, Redaction
from src.console import logger, prompt_in_thread
from src.cookie_auth import CookieValidator
from src.exceptions import *  # noqa F403
from src.exceptions import UploadError
from src.mediainfo import MediaInfo
from src.meta import Meta
from src.ptp_timing import log_ptp_timing
from src.rehostimages import ImageHostPolicy, RehostImagesManager
from src.screenshot_manifest import files as manifest_files
from src.takescreens import TakeScreensManager
from src.temp_paths import artwork_dir, screenshots_dir
from src.torrent_policy import PASSTHEPOPCORN_POLICY
from src.tracker_images import get_tracker_image_collection
from src.trackers.common import Common
from src.uploadscreens import UploadScreensManager


class PassThePopcorn:
    """
    PTP Private Torrent Tracker
    """

    base_url = "https://passthepopcorn.me"

    tracker = "PASSTHEPOPCORN"
    display_name = "PassThePopcorn"
    allows_bloated_audio = True
    source_flag = "PTP"
    banned_groups = (
        "aXXo",
        "BMDru",
        "BRrip",
        "CM8",
        "CrEwSaDe",
        "CTFOH",
        "d3g",
        "DNL",
        "FaNGDiNG0",
        "HD2DVD",
        "HDT",
        "HDTime",
        "ION10",
        "iPlanet",
        "KiNGDOM",
        "mHD",
        "mSD",
        "nHD",
        "nikt0",
        "nSD",
        "NhaNc3",
        "OFT",
        "PRODJi",
        "SANTi",
        "SPiRiT",
        "STUTTERSHIT",
        "ViSION",
        "VXT",
        "WAF",
        "x0r",
        "YIFY",
        "LAMA",
        "WORLD",
    )
    approved_image_hosts = ("pixhost", "imgbb", "onlyimage", "ptscreens", "passtheimage")
    image_host_policy = ImageHostPolicy(
        {
            "pixhost.to": "pixhost",
            "ibb.co": "imgbb",
            "imgbb.com": "imgbb",
            "onlyimage.org": "onlyimage",
            "ptscreens.com": "ptscreens",
            "passtheima.ge": "passtheimage",
        },
        approved_image_hosts,
    )
    sub_lang_map: ClassVar[dict[tuple[str, ...], int]] = {
        ("Arabic", "ara", "ar"): 22,
        ("Brazilian Portuguese", "Brazilian", "Portuguese-BR", "pt-br", "pt-BR"): 49,
        ("Bulgarian", "bul", "bg"): 29,
        ("Chinese", "chi", "zh", "Chinese (Simplified)", "Chinese (Traditional)", "cmn-Hant", "cmn-Hans", "yue-Hant", "yue-Hans"): 14,
        ("Croatian", "hrv", "hr", "scr"): 23,
        ("Czech", "cze", "cz", "cs"): 30,
        ("Danish", "dan", "da"): 10,
        ("Dutch", "dut", "nl"): 9,
        ("English", "eng", "en", "en-US", "en-GB", "English (CC)", "English - SDH"): 3,
        ("English - Forced", "English (Forced)", "en (Forced)", "en-US (Forced)"): 50,
        ("English Intertitles", "English (Intertitles)", "English - Intertitles", "en (Intertitles)", "en-US (Intertitles)"): 51,
        ("Estonian", "est", "et"): 38,
        ("Finnish", "fin", "fi"): 15,
        ("French", "fre", "fr", "fr-FR", "fr-CA"): 5,
        ("German", "ger", "de"): 6,
        ("Greek", "gre", "el"): 26,
        ("Hebrew", "heb", "he"): 40,
        ("Hindi", "hin", "hi"): 41,
        ("Hungarian", "hun", "hu"): 24,
        ("Icelandic", "ice", "is"): 28,
        ("Indonesian", "ind", "id"): 47,
        ("Italian", "ita", "it"): 16,
        ("Japanese", "jpn", "ja"): 8,
        ("Korean", "kor", "ko"): 19,
        ("Latvian", "lav", "lv"): 37,
        ("Lithuanian", "lit", "lt"): 39,
        ("Norwegian", "nor", "no"): 12,
        ("Polish", "pol", "pl"): 17,
        ("Portuguese", "por", "pt", "pt-PT"): 21,
        ("Romanian", "rum", "ro"): 13,
        ("Russian", "rus", "ru"): 7,
        ("Serbian", "srp", "sr", "scc"): 31,
        ("Slovak", "slo", "sk"): 42,
        ("Slovenian", "slv", "sl"): 43,
        ("Spanish", "spa", "es", "es-ES", "es-419"): 4,
        ("Swedish", "swe", "sv"): 11,
        ("Thai", "tha", "th"): 20,
        ("Turkish", "tur", "tr"): 18,
        ("Ukrainian", "ukr", "uk"): 34,
        ("Vietnamese", "vie", "vi"): 25,
    }
    supported_categories = ("MOVIE",)
    torrent_policy = PASSTHEPOPCORN_POLICY
    tracker_urls = ("passthepopcorn.me",)
    _prefetch_max_age = 3600.0
    _group_prefetch_tasks: ClassVar[dict[tuple[str, str], tuple[tuple[str, str], float, asyncio.Task[str | None]]]] = {}
    _title_prefetch_tasks: ClassVar[dict[tuple[str, str], tuple[Meta, float, asyncio.Task[dict[str, Any] | None]]]] = {}
    _auth_prefetch_tasks: ClassVar[dict[tuple[str, str], asyncio.Task[tuple[str, float] | None]]] = {}

    @staticmethod
    def _prefetch_key(meta: Meta) -> tuple[str, str]:
        return str(meta.base_dir), str(meta.source_torrent_hash or meta.uuid)

    @staticmethod
    def _group_identity(meta: Meta) -> tuple[str, str] | None:
        if meta.imdb_id and not meta.no_imdb:
            return "imdb", str(meta.imdb_id).removeprefix("tt").zfill(7)
        if meta.tmdb_id:
            return "tmdb", str(meta.tmdb_id)
        return None

    @classmethod
    def _can_prefetch(cls, meta: Meta, config: dict[str, Any]) -> bool:
        selected = meta.trackers or config.get("TRACKERS", {}).get("default_trackers", "")
        selected = selected.split(",") if isinstance(selected, str) else selected
        return (
            bool(meta.unattended)
            and not meta.edit
            and meta.category == "MOVIE"
            and any(Meta.canonical_tracker_name(str(tracker).strip()) == cls.tracker for tracker in selected)
            and bool(config.get("TRACKERS", {}).get(cls.tracker))
        )

    @classmethod
    def start_group_prefetch(cls, meta: Meta, config: dict[str, Any]) -> None:
        if not cls._can_prefetch(meta, config):
            return
        identity = cls._group_identity(meta)
        if identity is None:
            return
        key = cls._prefetch_key(meta)
        if key in cls._group_prefetch_tasks:
            return

        async def lookup() -> str | None:
            try:
                tracker = cls(config)
                started_at = time.perf_counter()
                group_id = await tracker._get_group_by_identifier(
                    {identity[0]: identity[1], "json": "noredirect"},
                    f"{identity[0].upper()}: {identity[1]}",
                    "TmdbId" if identity[0] == "tmdb" else None,
                    identity[1] if identity[0] == "tmdb" else None,
                    allow_selection=False,
                )
                log_ptp_timing(meta, "early group lookup", started_at)
                return group_id
            except Exception as error:
                logger.debug(f"{cls.tracker}: Early group lookup failed: {error}")
                return None

        cls._group_prefetch_tasks[key] = (identity, time.monotonic(), asyncio.create_task(lookup()))

    @classmethod
    async def cancel_group_prefetch(cls, meta: Meta) -> None:
        pending = cls._group_prefetch_tasks.pop(cls._prefetch_key(meta), None)
        if pending is not None:
            task = pending[2]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def get_group_for_meta(self, meta: Meta) -> str | None:
        pending = self._group_prefetch_tasks.pop(self._prefetch_key(meta), None)
        identity = self._group_identity(meta)
        if pending is not None:
            cached_identity, started_at, task = pending
            if identity == cached_identity and time.monotonic() - started_at < self._prefetch_max_age and not task.cancelled():
                group_id = await task
                if group_id is not None:
                    return group_id
            else:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if identity is None:
            return None
        if identity[0] == "imdb":
            return await self.get_group_by_imdb(identity[1])
        return await self.get_group_by_tmdb(identity[1])

    @classmethod
    def start_title_prefetch(cls, meta: Meta, config: dict[str, Any]) -> None:
        if not cls._can_prefetch(meta, config) or cls._group_identity(meta) is None:
            return
        key = cls._prefetch_key(meta)
        if key in cls._title_prefetch_tasks:
            return
        snapshot = meta.copy()
        pending_group = cls._group_prefetch_tasks.get(key)

        async def lookup() -> dict[str, Any] | None:
            try:
                if pending_group is not None and pending_group[0] == cls._group_identity(snapshot):
                    group_id = await asyncio.shield(pending_group[2])
                    if group_id is not None:
                        return None
                started_at = time.perf_counter()
                result = await cls(config)._lookup_title_info(snapshot)
                log_ptp_timing(meta, "early PTP title metadata lookup", started_at)
                return result
            except Exception as error:
                logger.debug(f"{cls.tracker}: Early title metadata lookup failed: {error}")
                return None

        cls._title_prefetch_tasks[key] = (snapshot, time.monotonic(), asyncio.create_task(lookup()))

    @classmethod
    async def cancel_title_prefetch(cls, meta: Meta) -> None:
        pending = cls._title_prefetch_tasks.pop(cls._prefetch_key(meta), None)
        if pending is not None:
            task = pending[2]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    @classmethod
    def start_auth_prefetch(cls, meta: Meta, config: dict[str, Any]) -> None:
        selected = meta.trackers or config.get("TRACKERS", {}).get("default_trackers", "")
        selected = selected.split(",") if isinstance(selected, str) else selected
        if (
            not meta.unattended
            or meta.unattended_confirm
            or meta.edit
            or meta.debug
            or not any(Meta.canonical_tracker_name(str(tracker).strip()) == cls.tracker for tracker in selected)
            or not config.get("TRACKERS", {}).get(cls.tracker)
        ):
            return
        key = cls._prefetch_key(meta)
        if key in cls._auth_prefetch_tasks:
            return
        snapshot = meta.copy()

        async def authenticate() -> tuple[str, float] | None:
            try:
                started_at = time.perf_counter()
                token = await cls(config).get_anti_csrf_token(snapshot)
                log_ptp_timing(snapshot, "early session and CSRF token", started_at)
                return (token, time.monotonic()) if token else None
            except Exception:
                logger.debug(f"{cls.tracker}: Early authentication failed; authentication will be retried at upload time.")
                return None

        cls._auth_prefetch_tasks[key] = asyncio.create_task(authenticate())

    @classmethod
    async def cancel_auth_prefetch(cls, meta: Meta) -> None:
        keys = {cls._prefetch_key(meta), (str(meta.base_dir), str(meta.uuid))}
        for key in keys:
            task = cls._auth_prefetch_tasks.pop(key, None)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def get_upload_anti_csrf_token(self, meta: Meta) -> str:
        task = self._auth_prefetch_tasks.pop(self._prefetch_key(meta), None)
        if task is None:
            task = self._auth_prefetch_tasks.pop((str(meta.base_dir), str(meta.uuid)), None)
        if task is not None and not task.cancelled():
            result = await task
            if result is not None and time.monotonic() - result[1] < self._prefetch_max_age:
                return result[0]
        return await self.get_anti_csrf_token(meta)

    @classmethod
    async def cancel_prefetch(cls, meta: Meta) -> None:
        await cls.cancel_auth_prefetch(meta)
        await cls.cancel_title_prefetch(meta)
        await cls.cancel_group_prefetch(meta)

    async def _lookup_title_info(self, meta: Meta) -> dict[str, Any]:
        identity = self._group_identity(meta)
        if identity is not None and identity[0] == "imdb":
            return await self.get_torrent_info(identity[1], meta)
        return await self.get_torrent_info_tmdb(meta)

    async def get_title_info_for_meta(self, meta: Meta) -> dict[str, Any]:
        pending = self._title_prefetch_tasks.pop(self._prefetch_key(meta), None)
        if pending is not None:
            snapshot, started_at, task = pending
            fields = ("imdb_id", "tmdb_id", "no_imdb", "genres", "keywords", "imdb_info", "title", "year", "overview")
            unchanged = all(meta.get(name) == snapshot.get(name) for name in fields)
            if unchanged and time.monotonic() - started_at < self._prefetch_max_age and not task.cancelled():
                result = await task
                if result is not None:
                    return result
            else:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        return await self._lookup_title_info(meta)

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.rehost_images_manager = RehostImagesManager(config)
        self.takescreens_manager = TakeScreensManager(config)
        self.uploadscreens_manager = UploadScreensManager(config)
        self.api_user = config["TRACKERS"][self.tracker].get("ApiUser", "").strip()
        self.api_key = config["TRACKERS"][self.tracker].get("api_key", "").strip()
        announce_url = config["TRACKERS"][self.tracker].get("announce_url", "").strip()
        if announce_url and announce_url.startswith("http://"):
            logger.info(f"{self.tracker}: [red]announce URL is using plaintext HTTP.\n")
            logger.info(
                f"{self.tracker}: [red]is turning off their plaintext HTTP tracker soon. You must update your announce URLS. See PassThePopcorn/forums.php?page=1&action=viewthread&threadid=46663"
            )
            logger.info(f"{self.tracker}: [yellow]Modifying the url to use HTTPS. Update your config file to avoid this message in the future.")
            self.announce_url = announce_url.replace("http://", "https://").replace(":2710", "")
        else:
            self.announce_url = announce_url
        self.username = config["TRACKERS"][self.tracker].get("username", "").strip()
        self.password = config["TRACKERS"][self.tracker].get("password", "").strip()
        self.web_source = self._is_true(config["TRACKERS"][self.tracker].get("add_web_source_to_desc", True))
        self.user_agent = f"Upload-Assistant/2.3 ({platform.system()} {platform.release()})"

        self.cookie_validator = CookieValidator(config)

    def _is_true(self, value: Any) -> bool:
        return str(value).strip().lower() in {"true", "1", "yes"}

    async def get_additional_checks(self, meta: Meta) -> bool:
        if meta.tag:
            tag_clean = meta.tag.strip().lower()
            banned_groups_lower = {g.lower() for g in self.banned_groups}
            if tag_clean in banned_groups_lower:
                logger.info(f"{self.tracker}: [red]Release group {meta.tag} is banned. Skipping upload.[/red]")
                return False

        if not self.api_user:
            logger.info(f"{self.tracker}: [red]API User is missing in config. Skipping upload.[/red]")
            return False

        if not self.username or not self.password:
            logger.info(f"{self.tracker}: [red]Username or Password is missing in config. Skipping upload.[/red]")
            return False

        passkey_match = re.match(r"https?://please\.passthepopcorn\.me:?\d*/(.+)/announce", self.announce_url)
        if not passkey_match:
            logger.info(f"{self.tracker}: [red]Failed to extract passkey from PassThePopcorn announce URL. Skipping upload.[/red]")
            return False

        return True

    async def get_ptp_id_imdb(
        self,
        search_term: str,
        _search_file_folder: str,
        _meta: dict[str, Any],
    ) -> tuple[int | None, int | str | None, str | None]:
        headers = {
            "ApiUser": self.api_user,
            "ApiKey": self.api_key,
            "User-Agent": self.user_agent,
        }
        url = f"{self.base_url}/torrents.php"
        search_value = search_term or _search_file_folder
        params = {
            "searchstr": search_value,
            "json": "noredirect",
        }

        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url=url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                movies = cast(list[dict[str, Any]], data.get("Movies", []))
                for movie in movies:
                    imdb_value = movie.get("ImdbId")
                    torrents = cast(list[dict[str, Any]], movie.get("Torrents", []) or [])
                    ptp_torrent_id: int | str | None = None
                    ptp_torrent_hash: str | None = None

                    normalized_search = search_value or "".lower()
                    if normalized_search:
                        for torrent in torrents:
                            release_name = str(torrent.get("ReleaseName", "")).lower()
                            if normalized_search in release_name:
                                ptp_torrent_id = torrent.get("Id")
                                ptp_torrent_hash = torrent.get("InfoHash")
                                break

                    if ptp_torrent_id is None and torrents:
                        first = torrents[0]
                        ptp_torrent_id = first.get("Id")
                        ptp_torrent_hash = first.get("InfoHash")

                    if imdb_value:
                        return int(imdb_value or 0), ptp_torrent_id, ptp_torrent_hash

                logger.info(f"{self.tracker}: [yellow]Could not find any release matching [bold yellow]{search_value}[/bold yellow] on PassThePopcorn")
                return None, None, None

            if response.status_code in [400, 401, 403]:
                logger.info(f"{self.tracker}: [bold red]Error: 400/401/403 - Invalid request or authentication failed[/bold red]")
                return None, None, None
            if response.status_code == 503:
                logger.info(f"{self.tracker}: [bold yellow]Unavailable (503)")
                return None, None, None
            return None, None, None
        except Exception as e:
            logger.info(f"{self.tracker}: [red]An error occurred: {e!s}[/red]")
            return None, None, None

    async def get_imdb_from_torrent_id(self, ptp_torrent_id: int | str) -> tuple[int | None, str | None]:
        params = {"torrentid": ptp_torrent_id, "json": "noredirect"}
        headers = {"ApiUser": self.api_user, "ApiKey": self.api_key, "User-Agent": self.user_agent}
        url = f"{self.base_url}/torrents.php"
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers=headers)
        await asyncio.sleep(1)
        try:
            if response.status_code == 200:
                response = response.json()
                imdb_id = int(response.get("ImdbId", 0) or 0)
                ptp_infohash = None
                for torrent in response["Torrents"]:
                    if torrent.get("Id", 0) == str(ptp_torrent_id):
                        ptp_infohash = torrent.get("InfoHash", None)
                return imdb_id, ptp_infohash
            if response.status_code in [400, 401, 403]:
                logger.info(response.text)
                return None, None
            if response.status_code == 503:
                logger.info(f"{self.tracker}: [bold yellow]Unavailable (503)")
                return None, None
            return None, None
        except Exception:
            return None, None

    async def get_ptp_description(self, ptp_torrent_id: int | str, meta: Meta, is_disc: str) -> list[Any]:
        params = {"id": ptp_torrent_id, "action": "get_description"}
        headers = {"ApiUser": self.api_user, "ApiKey": self.api_key, "User-Agent": self.user_agent}
        url = f"{self.base_url}/torrents.php"
        logger.info(f"{self.tracker}: [yellow]Requesting description from {url} with ID {ptp_torrent_id}")
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers=headers)
        await asyncio.sleep(1)

        ptp_desc = response.text
        # console.print(f"[yellow]Raw description received:\n{ptp_desc}...")  # Show first 500 characters for brevity
        desc = None
        imagelist: list[Any] = []
        bbcode = BBCODE()
        desc, imagelist = bbcode.clean_ptp_description(ptp_desc, is_disc)

        if not meta.skip_tracker_descriptions:
            logger.info(f"{self.tracker}: [bold green]Successfully grabbed description from PassThePopcorn")
            logger.info(f"{self.tracker}: Description after cleaning:\n{desc[:1000]}...", extra={"markup": False})  # Show first 1000 characters for brevity

            if not meta.skipit and not meta.unattended:
                # Allow user to edit or discard the description
                logger.info(f"{self.tracker}: [cyan]Do you want to edit, discard or keep the description?[/cyan]")
                edit_choice = await prompt_in_thread(cli_ui.ask_string, "Enter 'e' to edit, 'd' to discard, or press Enter to keep it as is: ")

                if (edit_choice or "").lower() == "e":
                    edited_description = cast(str | None, click.edit(cast(Any, desc)))
                    if edited_description:
                        desc = edited_description.strip()
                        meta.description = desc
                        meta.saved_description = True
                    logger.info(f"{self.tracker}: [green]Final description after editing:[/green] {escape(str(desc))}")
                elif (edit_choice or "").lower() == "d":
                    desc = None
                    logger.info(f"{self.tracker}: [yellow]Description discarded.[/yellow]")
                else:
                    logger.info(f"{self.tracker}: [green]Keeping the original description.[/green]")
                    meta.description = desc
                    meta.saved_description = True
            else:
                meta.description = desc
                meta.saved_description = True
        return imagelist if meta.keep_images else []

    async def get_group_by_imdb(self, imdb: int | str) -> str | None:
        params = {
            "imdb": imdb,
            "json": "noredirect",
        }
        return await self._get_group_by_identifier(params, f"IMDb: tt{imdb}")

    async def get_group_by_tmdb(self, tmdb: int | str) -> str | None:
        params = {
            "tmdb": tmdb,
            "json": "noredirect",
        }
        return await self._get_group_by_identifier(params, f"TMDB: {tmdb}", "TmdbId", tmdb, allow_selection=False)

    async def _get_group_by_identifier(
        self,
        params: dict[str, int | str],
        identifier_label: str,
        response_identifier_key: str | None = None,
        response_identifier: int | str | None = None,
        allow_selection: bool = True,
    ) -> str | None:
        headers = {"ApiUser": self.api_user, "ApiKey": self.api_key, "User-Agent": self.user_agent}
        url = f"{self.base_url}/torrents.php"
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url=url, headers=headers, params=params)
        await asyncio.sleep(1)
        try:
            if response.status_code != 200:
                logger.info(f"{self.tracker}: [red]group lookup failed with HTTP {response.status_code}[/red]")
                if response.text:
                    logger.info(f"{self.tracker}: [red]Response body (truncated): {response.text[:200]}[/red]")
                return None

            try:
                response_data = response.json()
            except json.JSONDecodeError:
                content_type = response.headers.get("content-type", "unknown")
                logger.info(f"{self.tracker}: [red]group lookup returned non-JSON content (content-type: {content_type})[/red]")
                if response.text:
                    logger.info(f"{self.tracker}: [red]Response body (truncated): {response.text[:200]}[/red]")
                return None

            if response_data.get("TotalResults"):  # Search results page
                movies = cast(list[dict[str, Any]], response_data.get("Movies", []))
                if response_identifier_key is not None and response_identifier is not None:
                    expected_identifier = str(response_identifier)
                    movies = [movie for movie in movies if str(movie.get(response_identifier_key, "")) == expected_identifier]
                total_results = len(movies)
                if total_results == 0:
                    logger.info(f"{self.tracker}: [yellow]No results found for {identifier_label}[/yellow]")
                    return None
                if total_results == 1:
                    # Single result - use it
                    movie = movies[0]
                    group_id: str | None = str(movie.get("GroupId")) if movie.get("GroupId") is not None else None
                    title = movie.get("Title", "Unknown")
                    year = movie.get("Year", "Unknown")
                    logger.info(f"{self.tracker}: [green]Found single match for {identifier_label} -> Group ID: [yellow]{group_id}[/yellow][/green]")
                    logger.info(f"{self.tracker}: [green]Title: [yellow]{title}[/yellow] ([yellow]{year}[/yellow])")
                    return group_id
                if not allow_selection:
                    logger.info(f"{self.tracker}: [yellow]Multiple groups matched {identifier_label}; creating a new group instead of prompting.[/yellow]")
                    return None
                # Multiple results - let user choose
                logger.info(f"{self.tracker}: [yellow]Found {total_results} matches for {identifier_label}[/yellow]")
                choices: list[str] = []
                for _i, movie in enumerate(movies):
                    title = movie.get("Title", "Unknown")
                    year = movie.get("Year", "Unknown")
                    group_id = movie.get("GroupId", "Unknown")
                    choice_text = f"{title} ({year}) - Group ID: {group_id}"
                    choices.append(choice_text)

                choices.append("Skip - Don't use any of these matches")

                try:
                    selected = await prompt_in_thread(cli_ui.ask_choice, "Select the correct movie:", choices=choices)
                    if selected == "Skip - Don't use any of these matches":
                        logger.info(f"{self.tracker}: [yellow]User chose to skip all matches[/yellow]")
                        return None

                    # Match selection directly to movie data to avoid index issues from cli_ui sorting
                    group_id = None
                    for movie in movies:
                        title = movie.get("Title", "Unknown")
                        year = movie.get("Year", "Unknown")
                        group_id = movie.get("GroupId", "Unknown")
                        if f"{title} ({year}) - Group ID: {group_id}" == selected:
                            group_id = str(group_id)
                            break

                    logger.info(f"{self.tracker}: [green]User selected: Group ID [yellow]{group_id}[/yellow][/green]")
                    return group_id

                except KeyboardInterrupt:
                    logger.info(f"{self.tracker}: [yellow]Selection cancelled by user[/yellow]")
                    return None
            elif response_data.get("Page") == "Browse":  # No Releases on Site with ID
                return None
            elif response_data.get("Page") == "Details":  # Group Found
                group_id = response_data.get("GroupId")
                logger.info(f"{self.tracker}: [green]Matched {identifier_label} to Group ID: [yellow]{group_id}[/yellow][/green]")
                logger.info(f"{self.tracker}: [green]Title: [yellow]{response_data.get('Name')}[/yellow] ([yellow]{response_data.get('Year')}[/yellow])")
                return str(group_id) if group_id is not None else None
        except Exception:
            logger.info(f"{self.tracker}: [red]An error has occurred trying to find a group ID")
            logger.info(f"{self.tracker}: [red]Please check that the site is online and your ApiUser/api_key values are correct")
            return None

        return None

    async def get_torrent_info(self, imdb: int | str, meta: Meta) -> dict[str, Any]:
        params = {"imdb": imdb, "action": "torrent_info", "fast": 1}
        headers = {"ApiUser": self.api_user, "ApiKey": self.api_key, "User-Agent": self.user_agent}
        url = f"{self.base_url}/ajax.php"
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url=url, params=params, headers=headers)
        await asyncio.sleep(1)
        tinfo = {}
        with contextlib.suppress(Exception):
            response = response.json()
            # console.print(f"[blue]Raw info API Response: {response}[/blue]")
            # title, plot, art, year, tags, Countries, Languages
            tinfo = {key: value for key, value in response[0].items() if value not in (None, "")}
            if not tinfo.get("tags"):
                tags = await self.get_tags([meta.genres, meta.keywords, meta.imdb_info["genres"]])
                tinfo["tags"] = ", ".join(tags)
        return tinfo

    async def get_torrent_info_tmdb(self, meta: Meta) -> dict[str, Any]:
        tinfo = {
            "title": meta.title,
            "year": meta.year,
            "album_desc": meta.overview,
        }
        tags = await self.get_tags([meta.genres, meta.keywords])
        tinfo["tags"] = ", ".join(tags)
        return tinfo

    async def get_tags(self, check_against: Any) -> list[str]:
        tags: list[str] = []
        ptp_tags = [
            "action",
            "adventure",
            "animation",
            "arthouse",
            "asian",
            "biography",
            "camp",
            "comedy",
            "crime",
            "cult",
            "documentary",
            "drama",
            "experimental",
            "exploitation",
            "family",
            "fantasy",
            "film.noir",
            "history",
            "horror",
            "martial.arts",
            "musical",
            "mystery",
            "performance",
            "philosophy",
            "politics",
            "romance",
            "sci.fi",
            "short",
            "silent",
            "sport",
            "thriller",
            "video.art",
            "war",
            "western",
        ]

        check_against_list = check_against if isinstance(check_against, list) else [check_against]
        normalized_check_against: list[str] = []
        for item in check_against_list:
            if isinstance(item, list):
                normalized_check_against.extend(x.lower().replace(" ", "").replace("-", "") for x in item if isinstance(x, str) and x.strip())
            elif isinstance(item, str) and item.strip():
                normalized_check_against.extend(x.strip().lower().replace(" ", "").replace("-", "") for x in item.split(",") if x.strip())
        for each in ptp_tags:
            clean_tag = each.replace(".", "")
            if any(clean_tag in item for item in normalized_check_against):
                tags.append(each)

        return tags

    async def search_existing(self, group_id: int | str, meta: Meta) -> list[str]:
        # Map resolutions to SD / HD / UHD
        quality = None
        if meta.sd == 1:  # 1 is SD
            quality = "Standard Definition"
        elif meta.resolution in ["1440p", "1080p", "1080i", "720p"]:
            quality = "High Definition"
        elif meta.resolution in ["2160p", "4320p", "8640p"]:
            quality = "Ultra High Definition"

        # Prepare request parameters and headers
        params = {
            "id": group_id,
            "json": "1",
            "jsontrumpable": "1",
        }
        headers = {"ApiUser": self.api_user, "ApiKey": self.api_key, "User-Agent": self.user_agent}
        url = f"{self.base_url}/torrents.php"

        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(url, headers=headers, params=params)
            await asyncio.sleep(1)  # Mimic server-friendly delay
            response.raise_for_status()
            existing: list[str] = []
            try:
                data = response.json()
            except Exception as e:
                logger.info(f"{self.tracker}: [red]Error searching for duplicates, response was not valid JSON: {e}[/red]")
                if response.text:
                    logger.info(f"{self.tracker}: [red]Response body (truncated): {response.text[:200]}[/red]")
                return existing

            torrents = cast(list[dict[str, Any]], data.get("Torrents", []))
            existing.extend(
                f"[{torrent.get('Resolution')}] {torrent.get('ReleaseName', 'RELEASE NAME NOT FOUND')}"
                for torrent in torrents
                if torrent.get("Quality") == quality and quality is not None
            )
            return existing

    def _selected_poster_host(self, meta: Meta) -> str:
        default_config = cast(dict[str, Any], self.config.get("DEFAULT", {}))
        return str(meta.imghost or default_config.get("img_host_1") or "").strip()

    def _poster_already_on_selected_host(self, image_url: str, selected_host: str) -> bool:
        if not selected_host:
            return False
        hostname = (urlparse(image_url).hostname or "").lower()
        host_aliases = {
            "imgbb": ("ibb.co", "imgbb.com"),
            "imgbox": ("imgbox.com",),
            "pixhost": ("pixhost.to",),
            "lensdump": ("lensdump.com",),
            "onlyimage": ("onlyimage.org",),
            "ptscreens": ("ptscreens.com",),
            "passtheimage": ("passtheima.ge",),
            "seedpool_cdn": ("cdn.seedpool.org",),
            "utppm": ("utp.pm",),
        }
        aliases = host_aliases.get(selected_host, (selected_host,))
        return any(hostname == alias or hostname.endswith(f".{alias}") for alias in aliases)

    def _poster_extension(self, image_url: str, content_type: str) -> str:
        url_extension = Path(urlparse(image_url).path).suffix.lower()
        if url_extension in {".jpg", ".jpeg", ".png", ".webp"}:
            return url_extension

        content_type = content_type.split(";", 1)[0].strip().lower()
        content_type_extensions = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
        }
        return content_type_extensions.get(content_type, ".jpg")

    async def rehost_poster_to_selected_host(self, meta: Meta, image_url: str) -> str:
        selected_host = self._selected_poster_host(meta)
        if not selected_host or meta.skip_imghost_upload:
            return image_url
        if self._poster_already_on_selected_host(image_url, selected_host):
            return image_url

        tmp_dir = artwork_dir(meta.base_dir, meta.uuid)
        try:
            tmp_dir.mkdir(parents=True, exist_ok=True)
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(image_url)
                response.raise_for_status()
            poster_path = tmp_dir / f"PTP_POSTER{self._poster_extension(image_url, response.headers.get('content-type', ''))}"
            await asyncio.to_thread(poster_path.write_bytes, response.content)

            original_imghost = meta.imghost
            meta.imghost = selected_host
            try:
                uploaded_images, _ = await self.uploadscreens_manager.upload_screens(meta, 1, 1, 0, 1, [str(poster_path)], {})
            finally:
                if original_imghost is None:
                    meta.pop("imghost", None)
                else:
                    meta.imghost = original_imghost

            if uploaded_images:
                uploaded_url = uploaded_images[0].get("raw_url") or uploaded_images[0].get("img_url")
                if isinstance(uploaded_url, str) and uploaded_url:
                    return uploaded_url
        except Exception as e:
            logger.info(f"{self.tracker}: [red]poster rehost to {selected_host} failed: {e}")

        return image_url

    def get_type(self, imdb_info: dict[str, Any], meta: Meta) -> str | None:
        ptp_type = None
        if imdb_info.get("type") is not None:
            imdb_type = imdb_info.get("type", "movie").lower()
            if imdb_type in ("movie", "tv movie", "tvmovie"):
                ptp_type = "Feature Film" if int(imdb_info.get("runtime", "60")) >= 45 or int(imdb_info.get("runtime", "60")) == 0 else "Short Film"
            if imdb_type == "short":
                ptp_type = "Short Film"
            elif imdb_type == "tv mini series":
                ptp_type = "Miniseries"
            elif imdb_type == "comedy":
                ptp_type = "Stand-up Comedy"
            elif imdb_type == "concert":
                ptp_type = "Live Performance"
        else:
            keywords = [k.lower() for k in meta.keywords]
            tmdb_type = (meta.tmdb_type if meta.tmdb_type is not None else "movie").lower()
            if tmdb_type == "movie":
                try:
                    runtime = (meta.runtime) if meta.runtime is not None else 60
                except ValueError, TypeError:
                    runtime = 60
                ptp_type = "Feature Film" if runtime >= 45 or runtime == 0 else "Short Film"
            if tmdb_type == "miniseries" or "miniseries" in keywords:
                ptp_type = "Miniseries"
            if "short" in keywords or "short film" in keywords:
                ptp_type = "Short Film"
            elif "stand-up comedy" in keywords:
                ptp_type = "Stand-up Comedy"
            elif "concert" in keywords:
                ptp_type = "Live Performance"
        if ptp_type is None and (meta.mode if meta.mode is not None else "non_cli") == "cli":
            ptp_type_list = ["Feature Film", "Short Film", "Miniseries", "Stand-up Comedy", "Concert", "Movie Collection"]
            ptp_type = cli_ui.ask_choice("Select the proper type", choices=ptp_type_list)
            if ptp_type == "Concert":
                ptp_type = "Live Performance"
        return ptp_type

    def get_codec(self, meta: Meta) -> str:
        codec = ""
        if meta.is_disc == "BDMV":
            bdinfo = meta.bdinfo
            bd_sizes = [25, 50, 66, 100]
            for each in bd_sizes:
                if bdinfo["size"] < each:
                    codec = f"BD{each}"
                    break
            if not codec:
                codec = f"BD{bd_sizes[-1]}"
        elif meta.is_disc == "DVD":
            if "DVD5" in meta.dvd_size:
                codec = "DVD5"
            elif "DVD9" in meta.dvd_size:
                codec = "DVD9"
        else:
            codecmap = {
                "AVC": "H.264",
                "H.264": "H.264",
                "HEVC": "H.265",
                "H.265": "H.265",
            }
            searchcodec_value = meta.video_codec if meta.video_codec is not None else meta.video_encode
            searchcodec = searchcodec_value if isinstance(searchcodec_value, str) else ""
            codec = codecmap.get(searchcodec, searchcodec)
            if meta.has_encode_settings is True:
                codec = codec.replace("H.", "x")
        return codec

    def get_resolution(self, meta: Meta) -> tuple[str, str | None]:
        other_res = None
        res = meta.resolution if meta.resolution is not None else "OTHER"
        if (res == "OTHER" and meta.is_disc != "BDMV") or (meta.sd == 1 and meta.type == "WEBDL") or (meta.sd == 1 and meta.type == "DVDRIP"):
            other_res = f"{meta.video_width}x{meta.video_height}" if meta.video_width and meta.video_height else None
            res = "Other"
        if meta.is_disc == "DVD":
            res = meta.source or "".replace(" DVD", "")
        return res, other_res

    def get_container(self, meta: Meta) -> str | None:
        container = None
        if meta.is_disc == "BDMV":
            container = "m2ts"
        elif meta.is_disc == "DVD":
            container = "VOB IFO"
        else:
            ext = Path(meta.filelist[0]).suffix
            containermap = {".mkv": "MKV", ".mp4": "MP4"}
            container = containermap.get(ext, "Other")
        return container

    def get_source(self, source: str) -> str:
        sources = {"Blu-ray": "Blu-ray", "BluRay": "Blu-ray", "HD DVD": "HD-DVD", "HDDVD": "HD-DVD", "Web": "WEB", "HDTV": "HDTV", "UHDTV": "HDTV", "NTSC": "DVD", "PAL": "DVD"}
        return sources.get(source, "OtherR")

    def get_subtitles(self, meta: Meta) -> list[int]:
        sub_lang_map = self.sub_lang_map

        sub_langs: list[int] = []
        if meta.is_disc != "BDMV":
            mi = meta.mediainfo
            if meta.is_disc == "DVD":
                mi = json.loads(MediaInfo.parse(meta.discs[0]["ifo"], output="JSON"))
            for track in mi["media"]["track"]:
                if track["@type"] == "Text":
                    language = track.get("Language_String2", track.get("Language"))
                    if language == "en":
                        if track.get("Forced", "") == "Yes":
                            language = "en (Forced)"
                        title = track.get("Title", "")
                        if isinstance(title, str) and "intertitles" in title.lower():
                            language = "en (Intertitles)"
                    for lang, sub_id in sub_lang_map.items():
                        if language in lang and sub_id not in sub_langs:
                            sub_langs.append(sub_id)
        else:
            for language in meta.bdinfo["subtitles"]:
                for lang, sub_id in sub_lang_map.items():
                    if language in lang and sub_id not in sub_langs:
                        sub_langs.append(sub_id)

        if sub_langs == []:
            sub_langs = [44]  # No Subtitle
        return sub_langs

    def get_trumpable(self, sub_langs: list[int]) -> tuple[list[int] | None, list[int]]:
        trumpable_values = {
            "English Hardcoded Subs (Full)": 4,
            "English Hardcoded Subs (Forced)": 50,
            "No English Subs": 14,
            "English Softsubs Exist (Mislabeled)": None,
            "Hardcoded Subs (Non-English)": "OTHER",
        }
        opts = cli_ui.select_choices("Please select any/all applicable options:", choices=list(trumpable_values.keys()))
        trumpable_list: list[int] = []
        for opt in opts:
            v = trumpable_values.get(opt)
            if v is None:
                continue
            if v == 4:
                trumpable_list.append(4)
                if 3 not in sub_langs:
                    sub_langs.append(3)
                if 44 in sub_langs:
                    sub_langs.remove(44)
            elif v == 50:
                trumpable_list.append(50)
                if 50 not in sub_langs:
                    sub_langs.append(50)
                if 44 in sub_langs:
                    sub_langs.remove(44)
            elif v == 14:
                trumpable_list.append(14)
            elif v == "OTHER":
                trumpable_list.append(15)
                hc_sub_langs = (cli_ui.ask_string("Enter language code for HC Subtitle languages") or "").strip()
                if hc_sub_langs:
                    for lang, sub_id in self.sub_lang_map.items():
                        if any(hc_sub_langs == x for x in list(lang)) and sub_id not in sub_langs:
                            sub_langs.append(sub_id)
        sub_langs_result = list({*sub_langs})
        trumpable_unique = list({*trumpable_list})
        trumpable_result: list[int] | None = trumpable_unique if trumpable_unique else None
        return trumpable_result, sub_langs_result

    def get_remaster_title(self, meta: Meta) -> str:
        remaster_title: list[str] = []
        # Collections
        # Masters of Cinema, The Criterion Collection, Warner Archive Collection
        if meta.distributor in ("WARNER ARCHIVE", "WARNER ARCHIVE COLLECTION", "WAC"):
            remaster_title.append("Warner Archive Collection")
        elif meta.distributor in ("CRITERION", "CRITERION COLLECTION", "CC"):
            remaster_title.append("The Criterion Collection")
        elif meta.distributor in ("MASTERS OF CINEMA", "MOC"):
            remaster_title.append("Masters of Cinema")

        # Editions
        # Director's Cut, Extended Edition, Rifftrax, Theatrical Cut, Uncut, Unrated
        if "director's cut" in meta.edition.lower():
            remaster_title.append("Director's Cut")
        elif "extended" in meta.edition.lower():
            remaster_title.append("Extended Edition")
        elif "theatrical" in meta.edition.lower() or "rifftrax" in meta.edition.lower():
            remaster_title.append("Theatrical Cut")
        elif "uncut" in meta.edition.lower():
            remaster_title.append("Uncut")
        elif "unrated" in meta.edition.lower():
            remaster_title.append("Unrated")
        else:
            if meta.edition not in ("", None):
                remaster_title.append(meta.edition)

        # Features
        # 2-Disc Set, 2in1, 2D/3D Edition, 3D Anaglyph, 3D Full SBS, 3D Half OU, 3D Half SBS,
        # 4K Restoration, 4K Remaster,
        # Extras, Remux,
        if meta.type == "REMUX":
            remaster_title.append("Remux")

        # DTS:X, Dolby Atmos, Dual Audio, English Dub, With Commentary,
        if "DTS:X" in meta.audio:
            remaster_title.append("DTS:X")
        if "Atmos" in meta.audio:
            remaster_title.append("Dolby Atmos")
        if "Dual" in meta.audio:
            remaster_title.append("Dual Audio")
        if "Dubbed" in meta.audio:
            remaster_title.append("English Dub")

        # HDR10, HDR10+, Dolby Vision, 10-bit,
        # if "Hi10P" in meta.video_encode:
        #     remaster_title.append('10-bit')
        if meta.hdr.strip() == "" and meta.bit_depth == "10":
            remaster_title.append("10-bit")
        if "DV" in meta.hdr:
            remaster_title.append("Dolby Vision")
        if "HDR" in meta.hdr:
            if "HDR10+" in meta.hdr:
                remaster_title.append("HDR10+")
            else:
                remaster_title.append("HDR10")
        if "HLG" in meta.hdr:
            remaster_title.append("HLG")

        # with commentary always last
        if meta.has_commentary is True:
            remaster_title.append("With Commentary")

        return " / ".join(remaster_title) if remaster_title != [] else ""

    def convert_bbcode(self, desc: str) -> str:
        desc = desc.replace("[spoiler", "[hide").replace("[/spoiler]", "[/hide]")
        desc = desc.replace("[center]", "[align=center]").replace("[/center]", "[/align]")
        desc = desc.replace("[left]", "[align=left]").replace("[/left]", "[/align]")
        desc = desc.replace("[right]", "[align=right]").replace("[/right]", "[/align]")
        desc = desc.replace("[sup]", "").replace("[/sup]", "")
        desc = desc.replace("[sub]", "").replace("[/sub]", "")
        desc = desc.replace("[alert]", "").replace("[/alert]", "")
        desc = desc.replace("[note]", "").replace("[/note]", "")
        desc = desc.replace("[h1]", "[u][b]").replace("[/h1]", "[/b][/u]")
        desc = desc.replace("[h2]", "[u][b]").replace("[/h2]", "[/b][/u]")
        desc = desc.replace("[h3]", "[u][b]").replace("[/h3]", "[/b][/u]")
        desc = desc.replace("[list]", "").replace("[/list]", "")
        desc = desc.replace("[ul]", "").replace("[/ul]", "")
        desc = desc.replace("[ol]", "").replace("[/ol]", "")
        desc = re.sub(r"\[(?:font(?:=[^\]]*)?|/font)\]", "", desc, flags=re.IGNORECASE)
        desc = re.sub(r"\[img=[^\]]+\]", "[img]", desc)
        return BBCODE().clamp_size_tags(desc)

    def _description_images(self, meta: Meta) -> list[dict[str, Any]]:
        if meta.skip_imghost_upload:
            return []

        screenshots = get_tracker_image_collection(meta, self.tracker, "screenshots")
        if not meta.is_disc:
            return cast(list[dict[str, Any]], screenshots)

        menu_images = get_tracker_image_collection(meta, self.tracker, "menu_images")
        return cast(list[dict[str, Any]], [*menu_images, *screenshots])

    def _description_image_limit(self, meta: Meta, screenshot_limit: int) -> int:
        if not meta.is_disc:
            return screenshot_limit
        return screenshot_limit + len(get_tracker_image_collection(meta, self.tracker, "menu_images"))

    async def edit_desc(self, meta: Meta) -> None:
        from src.description_review import get_base_description

        base = get_base_description(meta)
        if meta.scene_nfo_file:
            # Remove NFO from description
            meta_description = re.sub(
                r"\[center\]\[spoiler=.*? NFO:\]\[code\](.*?)\[/code\]\[/spoiler\]\[/center\]",
                "",
                base,
                flags=re.DOTALL,
            )
            base = meta_description
        multi_screens = int(self.config["DEFAULT"].get("multiScreens", 2))
        if multi_screens < 2:
            multi_screens = 2
            logger.info(f"{self.tracker}: [yellow]requires at least 2 screenshots for multi disc/file content, overriding config")

        image_list = self._description_images(meta)
        images: list[dict[str, Any]] = image_list

        # Check for saved pack_image_links.json file
        pack_images_file = Path(meta.base_dir) / "tmp" / meta.uuid / "pack_image_links.json"
        pack_images_data: dict[str, Any] = {}
        if Path(pack_images_file).exists():
            try:
                async with aiofiles.open(pack_images_file, encoding="utf-8") as f:
                    content = await f.read()
                    pack_images_data = cast(dict[str, Any], json.loads(content)) if content.strip() else {}

                    # Filter out keys with non-approved image hosts
                    keys = cast(dict[str, Any], pack_images_data.get("keys", {}))
                    keys_to_remove: list[str] = []
                    for key_name, key_data in keys.items():
                        key_data_dict = cast(dict[str, Any], key_data)
                        images_to_keep: list[dict[str, Any]] = []
                        for img in cast(list[dict[str, Any]], key_data_dict.get("images", [])):
                            raw_url = str(img.get("raw_url", ""))
                            # Extract hostname from URL (e.g., pixhost.to -> pixhost)
                            try:
                                parsed_url = urlparse(raw_url)
                                hostname = parsed_url.netloc
                                # Get the main domain name (first part before the dot)
                                host_key = hostname.split(".")[0] if hostname else ""

                                if host_key in self.approved_image_hosts:
                                    images_to_keep.append(img)
                                elif meta.debug:
                                    logger.info(f"{self.tracker}: [yellow]Filtering out image from non-approved host: {hostname}[/yellow]")
                            except Exception:
                                # If URL parsing fails, skip this image
                                logger.debug(f"{self.tracker}: [yellow]Could not parse URL: {raw_url}[/yellow]")
                                continue

                        if images_to_keep:
                            # Update the key with only approved images
                            pack_images_data["keys"][key_name]["images"] = images_to_keep
                            pack_images_data["keys"][key_name]["count"] = len(images_to_keep)
                        else:
                            # Mark key for removal if no approved images
                            keys_to_remove.append(key_name)

                    # Remove keys with no approved images
                    for key_name in keys_to_remove:
                        del pack_images_data["keys"][key_name]
                        logger.debug(f"{self.tracker}: [yellow]Removed key '{key_name}' - no approved image hosts[/yellow]")

                    # Recalculate total count
                    keys = cast(dict[str, Any], pack_images_data.get("keys", {}))
                    pack_images_data["total_count"] = sum(cast(dict[str, Any], key_data).get("count", 0) for key_data in keys.values())

                    if pack_images_data.get("total_count", 0) < 3:
                        pack_images_data = {}  # Invalidate if less than 3 images total
                        logger.debug(f"{self.tracker}: [yellow]Invalidating pack images - less than 3 approved images total[/yellow]")
                    else:
                        logger.debug(f"{self.tracker}: [green]Loaded previously uploaded images from {pack_images_file}")
                        logger.debug(
                            f"{self.tracker}: [blue]Found {pack_images_data.get('total_count', 0)} approved images across {len(pack_images_data.get('keys', {}))} keys[/blue]"
                        )
            except Exception as e:
                logger.warning(f"{self.tracker}: [yellow]Warning: Could not load pack image data: {e!s}[/yellow]")

        desc = io.StringIO()
        discs = cast(list[dict[str, Any]], meta.discs)
        filelist = cast(list[str], meta.filelist)

        # Handle single disc case
        if len(discs) == 1:
            each = discs[0]
            new_screens: list[str] = []
            bdinfo_keys: list[str] = []
            if each["type"] == "BDMV":
                bdinfo_keys = [key for key in each if key.startswith("bdinfo")]
                bdinfo = meta.bdinfo
                if len(bdinfo_keys) > 1:
                    edition = str(bdinfo.get("edition", "Unknown Edition"))
                    desc.write(f"[b]{edition}[/b]\n\n")
                desc.write(f"[mediainfo]{each['summary']}[/mediainfo]\n\n")
                base2ptp = self.convert_bbcode(base)
                if base2ptp.strip() != "":
                    desc.write(base2ptp)
                    desc.write("\n\n")
                try:
                    if meta.tonemapped and self.config["DEFAULT"].get("tonemapped_header", None):
                        tonemapped_header = self.config["DEFAULT"].get("tonemapped_header")
                        tonemapped_header = self.convert_bbcode(tonemapped_header)
                        desc.write(tonemapped_header)
                        desc.write("\n\n")
                except Exception as e:
                    logger.warning(f"{self.tracker}: [yellow]Warning: Error setting tonemapped header: {e!s}[/yellow]")
                for img_index in range(len(images[: self._description_image_limit(meta, meta.screens)])):
                    raw_url = str(image_list[img_index].get("raw_url", ""))
                    desc.write(f"[img]{raw_url}[/img]\n")
                desc.write("\n")
            elif each["type"] == "DVD":
                desc.write(f"[b][size=3]{each['name']}:[/size][/b]\n")
                desc.write(f"[mediainfo]{each['ifo_mi_full']}[/mediainfo]\n")
                desc.write(f"[mediainfo]{each['vob_mi_full']}[/mediainfo]\n\n")
                base2ptp = self.convert_bbcode(base)
                if base2ptp.strip() != "":
                    desc.write(base2ptp)
                    desc.write("\n\n")
                for img_index in range(len(images[: self._description_image_limit(meta, meta.screens)])):
                    raw_url = image_list[img_index]["raw_url"]
                    desc.write(f"[img]{raw_url}[/img]\n")
                desc.write("\n")
            if len(bdinfo_keys) > 1:
                meta.retry_count = meta.retry_count

                for i, key in enumerate(bdinfo_keys[1:], start=1):  # Skip the first bdinfo
                    new_images_key = f"new_images_playlist_{i}"
                    bdinfo = each[key]
                    edition = bdinfo.get("edition", "Unknown Edition")

                    # Find the corresponding summary for this bdinfo
                    summary_key = f"summary_{i}" if i > 0 else "summary"
                    summary = each.get(summary_key, "No summary available")

                    # Check for saved images first
                    if pack_images_data and "keys" in pack_images_data and new_images_key in pack_images_data["keys"]:
                        saved_images = cast(list[dict[str, Any]], pack_images_data["keys"][new_images_key]["images"])
                        if saved_images:
                            logger.debug(f"{self.tracker}: [yellow]Using saved images from pack_image_links.json for {new_images_key}")

                            meta[new_images_key] = []
                            for img in saved_images:
                                meta[new_images_key].append(
                                    {"img_url": str(img.get("img_url", "")), "raw_url": str(img.get("raw_url", "")), "web_url": str(img.get("web_url", ""))}
                                )

                    if meta.get(new_images_key):
                        desc.write(f"\n[b]{edition}[/b]\n\n")
                        # Use the summary corresponding to the current bdinfo
                        desc.write(f"[mediainfo]{summary}[/mediainfo]\n\n")
                        logger.debug(f"{self.tracker}: [yellow]Using original uploaded images for first disc")
                        for img in meta[new_images_key]:
                            raw_url = str(img.get("raw_url", ""))
                            desc.write(f"[img]{raw_url}[/img]\n")
                    else:
                        desc.write(f"\n[b]{edition}[/b]\n")
                        # Use the summary corresponding to the current bdinfo
                        desc.write(f"[mediainfo]{summary}[/mediainfo]\n\n")
                        meta.retry_count += 1
                        meta[new_images_key] = []
                        new_screens = [f.name for f in manifest_files(meta.base_dir, meta.uuid, f"PLAYLIST_{i}")]
                        if not new_screens:
                            logger.warning(f"{self.tracker}: Missing prepared screenshots for PLAYLIST_{i}; skipping its images.")
                        uploaded_images: list[dict[str, Any]] = []
                        if new_screens and not meta.skip_imghost_upload:
                            uploaded_images, _ = await self.uploadscreens_manager.upload_screens(
                                meta, multi_screens, 1, 0, multi_screens, new_screens, {new_images_key: meta[new_images_key]}, allowed_hosts=self.approved_image_hosts
                            )
                            if uploaded_images and not meta.skip_imghost_upload:
                                await self.save_image_links(meta, new_images_key, uploaded_images)
                            for img in uploaded_images:
                                meta[new_images_key].append(
                                    {"img_url": str(img.get("img_url", "")), "raw_url": str(img.get("raw_url", "")), "web_url": str(img.get("web_url", ""))}
                                )

                            for img in uploaded_images:
                                raw_url = str(img.get("raw_url", ""))
                                desc.write(f"[img]{raw_url}[/img]\n")

                        meta_filename = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/meta.json"
                        async with aiofiles.open(meta_filename, "w", encoding="utf-8") as f:
                            await f.write(json.dumps(meta.to_dict(), indent=4, cls=PathAwareEncoder))

        # Handle multiple discs case
        elif len(discs) > 1:
            if "retry_count" not in meta:
                meta.retry_count = 0
            for i, each in enumerate(discs):
                new_images_key = f"new_images_disc_{i}"
                if each["type"] == "BDMV":
                    if i == 0:
                        desc.write(f"[mediainfo]{each['summary']}[/mediainfo]\n\n")
                        base2ptp = self.convert_bbcode(base)
                        if base2ptp.strip() != "":
                            desc.write(base2ptp)
                            desc.write("\n\n")
                        try:
                            if meta.tonemapped and self.config["DEFAULT"].get("tonemapped_header", None):
                                tonemapped_header = self.config["DEFAULT"].get("tonemapped_header")
                                tonemapped_header = self.convert_bbcode(tonemapped_header)
                                desc.write(tonemapped_header)
                                desc.write("\n\n")
                        except Exception as e:
                            logger.warning(f"{self.tracker}: [yellow]Warning: Error setting tonemapped header: {e!s}[/yellow]")
                        for img_index in range(min(self._description_image_limit(meta, multi_screens), len(image_list))):
                            raw_url = str(image_list[img_index].get("raw_url", ""))
                            desc.write(f"[img]{raw_url}[/img]\n")
                        desc.write("\n")
                    else:
                        desc.write(f"[mediainfo]{each['summary']}[/mediainfo]\n\n")
                        base2ptp = self.convert_bbcode(base)
                        if base2ptp.strip() != "":
                            desc.write(base2ptp)
                            desc.write("\n\n")
                        # Check for saved images first
                        if pack_images_data and "keys" in pack_images_data and new_images_key in pack_images_data["keys"]:
                            saved_images = cast(list[dict[str, Any]], pack_images_data["keys"][new_images_key]["images"])
                            if saved_images:
                                logger.debug(f"{self.tracker}: [yellow]Using saved images from pack_image_links.json for {new_images_key}")

                                meta[new_images_key] = []
                                for img in saved_images:
                                    meta[new_images_key].append(
                                        {"img_url": str(img.get("img_url", "")), "raw_url": str(img.get("raw_url", "")), "web_url": str(img.get("web_url", ""))}
                                    )
                        if meta.get(new_images_key):
                            for img in meta[new_images_key]:
                                raw_url = str(img.get("raw_url", ""))
                                desc.write(f"[img]{raw_url}[/img]\n")
                            desc.write("\n")
                        else:
                            meta.retry_count += 1
                            meta[new_images_key] = []
                        new_screens = [f.name for f in manifest_files(meta.base_dir, meta.uuid, f"FILE_{i}")]
                        if not new_screens:
                            logger.warning(f"{self.tracker}: Missing prepared screenshots for FILE_{i}; skipping its images.")
                        uploaded_images: list[dict[str, Any]] = []
                        if new_screens and not meta.skip_imghost_upload:
                            uploaded_images, _ = await self.uploadscreens_manager.upload_screens(
                                meta, multi_screens, 1, 0, multi_screens, new_screens, {new_images_key: meta[new_images_key]}, allowed_hosts=self.approved_image_hosts
                            )
                        if uploaded_images and not meta.skip_imghost_upload:
                            await self.save_image_links(meta, new_images_key, uploaded_images)
                            for img in uploaded_images:
                                meta[new_images_key].append(
                                    {"img_url": str(img.get("img_url", "")), "raw_url": str(img.get("raw_url", "")), "web_url": str(img.get("web_url", ""))}
                                )
                                raw_url = str(img.get("raw_url", ""))
                                desc.write(f"[img]{raw_url}[/img]\n")
                            desc.write("\n")

                            meta_filename = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/meta.json"
                            async with aiofiles.open(meta_filename, "w", encoding="utf-8") as f:
                                await f.write(json.dumps(meta.to_dict(), indent=4, cls=PathAwareEncoder))

                elif each["type"] == "DVD":
                    if i == 0:
                        desc.write(f"[b][size=3]{each['name']}:[/size][/b]\n")
                        desc.write(f"[mediainfo]{each['ifo_mi_full']}[/mediainfo]\n")
                        desc.write(f"[mediainfo]{each['vob_mi_full']}[/mediainfo]\n\n")
                        base2ptp = self.convert_bbcode(base)
                        if base2ptp.strip() != "":
                            desc.write(base2ptp)
                            desc.write("\n\n")
                        for img_index in range(min(self._description_image_limit(meta, multi_screens), len(image_list))):
                            raw_url = image_list[img_index]["raw_url"]
                            desc.write(f"[img]{raw_url}[/img]\n")
                        desc.write("\n")
                    else:
                        desc.write(f"[b][size=3]{each['name']}:[/size][/b]\n")
                        desc.write(f"[mediainfo]{each['ifo_mi_full']}[/mediainfo]\n")
                        desc.write(f"[mediainfo]{each['vob_mi_full']}[/mediainfo]\n\n")
                        base2ptp = self.convert_bbcode(base)
                        if base2ptp.strip() != "":
                            desc.write(base2ptp)
                            desc.write("\n\n")
                        # Check for saved images first
                        if pack_images_data and "keys" in pack_images_data and new_images_key in pack_images_data["keys"]:
                            saved_images = pack_images_data["keys"][new_images_key]["images"]
                            if saved_images:
                                logger.debug(f"{self.tracker}: [yellow]Using saved images from pack_image_links.json for {new_images_key}")

                                meta[new_images_key] = []
                                for img in saved_images:
                                    meta[new_images_key].append({"img_url": img.get("img_url", ""), "raw_url": img.get("raw_url", ""), "web_url": img.get("web_url", "")})
                        if meta.get(new_images_key):
                            for img in meta[new_images_key]:
                                raw_url = img["raw_url"]
                                desc.write(f"[img]{raw_url}[/img]\n")
                            desc.write("\n")
                        else:
                            meta.retry_count += 1
                            meta[new_images_key] = []
                            new_screens = [f.name for f in screenshots_dir(meta.base_dir, meta.uuid).glob(f"{glob.escape(meta.discs[i]['name'])}-*.png")]
                            if not new_screens:
                                try:
                                    await self.takescreens_manager.dvd_screenshots(meta, i, multi_screens, True, cleanup_after_capture=False)
                                except Exception as e:
                                    logger.info(f"{self.tracker}: Error during DVD screenshot capture: {e}", extra={"markup": False})
                            new_screens = [f.name for f in screenshots_dir(meta.base_dir, meta.uuid).glob(f"{glob.escape(meta.discs[i]['name'])}-*.png")]
                            uploaded_images: list[dict[str, Any]] = []
                            if new_screens and not meta.skip_imghost_upload:
                                uploaded_images, _ = await self.uploadscreens_manager.upload_screens(
                                    meta, multi_screens, 1, 0, multi_screens, new_screens, {new_images_key: meta[new_images_key]}, allowed_hosts=self.approved_image_hosts
                                )
                            if uploaded_images and not meta.skip_imghost_upload:
                                await self.save_image_links(meta, new_images_key, uploaded_images)
                                for img in uploaded_images:
                                    meta[new_images_key].append({"img_url": img["img_url"], "raw_url": img["raw_url"], "web_url": img["web_url"]})
                                    raw_url = img["raw_url"]
                                    desc.write(f"[img]{raw_url}[/img]\n")
                                desc.write("\n")

                        meta_filename = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/meta.json"
                        async with aiofiles.open(meta_filename, "w", encoding="utf-8") as f:
                            await f.write(json.dumps(meta.to_dict(), indent=4, cls=PathAwareEncoder))

        # Handle single file case
        elif len(filelist) == 1:
            if meta.type == "WEBDL" and meta.service_longname != "" and not meta.description and self.web_source is True:
                desc.write(f"[quote][align=center]This release is sourced from {meta.service_longname}[/align][/quote]")
            async with aiofiles.open(f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/MEDIAINFO.txt", encoding="utf-8") as mi_file:
                mi_dump = await mi_file.read()
            desc.write(f"[mediainfo]{mi_dump}[/mediainfo]\n")
            base2ptp = self.convert_bbcode(base)
            if base2ptp.strip() != "":
                desc.write(base2ptp)
                desc.write("\n\n")
            if meta.comparison and "comparison_groups" in meta and meta.comparison_groups:
                desc.write("\n")

                comparison_groups = meta.comparison_groups if isinstance(meta.comparison_groups, dict) else {}
                group_keys = sorted(comparison_groups.keys(), key=lambda x: int(x))
                comparison_names = [comparison_groups[key].get("name", f"Group {key}") for key in group_keys]
                comparison_header = ", ".join(comparison_names)
                desc.write(f"[comparison={comparison_header}]\n")

                num_images = min([len(comparison_groups[key]["urls"]) for key in group_keys])

                for img_index in range(num_images):
                    for key in group_keys:
                        group = comparison_groups[key]
                        if img_index < len(group["urls"]):
                            img_data = group["urls"][img_index]
                            raw_url = img_data.get("raw_url", "")
                            if raw_url:
                                desc.write(f"[img]{raw_url}[/img] ")
                    desc.write("\n")

                desc.write("[/comparison]\n\n")

            try:
                if meta.tonemapped and self.config["DEFAULT"].get("tonemapped_header", None):
                    tonemapped_header = self.config["DEFAULT"].get("tonemapped_header")
                    tonemapped_header = self.convert_bbcode(tonemapped_header)
                    desc.write(tonemapped_header)
                    desc.write("\n\n")
            except Exception as e:
                logger.warning(f"{self.tracker}: [yellow]Warning: Error setting tonemapped header: {e!s}[/yellow]")

            for img_index in range(len(images[: self._description_image_limit(meta, meta.screens)])):
                raw_url = image_list[img_index]["raw_url"]
                desc.write(f"[img]{raw_url}[/img]\n")
            desc.write("\n")

        # Handle multiple files case
        elif len(filelist) > 1:
            for i, file in enumerate(filelist):
                if i == 0:
                    if meta.type == "WEBDL" and meta.service_longname != "" and not meta.description and self.web_source is True:
                        desc.write(f"[quote][align=center]This release is sourced from {meta.service_longname}[/align][/quote]")
                    base2ptp = self.convert_bbcode(base)
                    if base2ptp.strip() != "":
                        desc.write(base2ptp)
                        desc.write("\n\n")
                    async with aiofiles.open(f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/MEDIAINFO.txt", encoding="utf-8") as mi_file:
                        mi_dump = await mi_file.read()
                    desc.write(f"[mediainfo]{mi_dump}[/mediainfo]\n")
                    try:
                        if meta.tonemapped and self.config["DEFAULT"].get("tonemapped_header", None):
                            tonemapped_header = self.config["DEFAULT"].get("tonemapped_header")
                            tonemapped_header = self.convert_bbcode(tonemapped_header)
                            desc.write(tonemapped_header)
                            desc.write("\n\n")
                    except Exception as e:
                        logger.warning(f"{self.tracker}: [yellow]Warning: Error setting tonemapped header: {e!s}[/yellow]")
                    for img_index in range(min(self._description_image_limit(meta, multi_screens), len(image_list))):
                        raw_url = image_list[img_index]["raw_url"]
                        desc.write(f"[img]{raw_url}[/img]\n")
                    desc.write("\n")
                else:
                    mi_dump = MediaInfo.parse(file, output="STRING", full=False)
                    temp_mi_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/TEMP_PTP_MEDIAINFO.txt"
                    async with aiofiles.open(temp_mi_path, "w", newline="", encoding="utf-8") as f:
                        await f.write(mi_dump.replace(file, Path(file).name))
                    async with aiofiles.open(temp_mi_path, encoding="utf-8") as mi_file:
                        mi_dump = await mi_file.read()
                    desc.write(f"[mediainfo]{mi_dump}[/mediainfo]\n")
                    new_images_key = f"new_images_file_{i}"
                    # Check for saved images first
                    if pack_images_data and "keys" in pack_images_data and new_images_key in pack_images_data["keys"]:
                        saved_images = pack_images_data["keys"][new_images_key]["images"]
                        if saved_images:
                            logger.debug(f"{self.tracker}: [yellow]Using saved images from pack_image_links.json for {new_images_key}")

                            meta[new_images_key] = []
                            for img in saved_images:
                                meta[new_images_key].append({"img_url": img.get("img_url", ""), "raw_url": img.get("raw_url", ""), "web_url": img.get("web_url", "")})
                    if meta.get(new_images_key):
                        for img in meta[new_images_key]:
                            raw_url = img["raw_url"]
                            desc.write(f"[img]{raw_url}[/img]\n")
                        desc.write("\n")
                    else:
                        meta.retry_count = meta.retry_count + 1
                        meta[new_images_key] = []
                        new_screens = [f.name for f in manifest_files(meta.base_dir, meta.uuid, f"FILE_{i}")]
                        if not new_screens:
                            try:
                                await self.takescreens_manager.screenshots(
                                    file, f"FILE_{i}", meta.uuid, meta.base_dir, meta, multi_screens, True, "", cleanup_after_capture=False, capture_group=f"FILE_{i}"
                                )
                            except Exception as e:
                                logger.info(f"{self.tracker}: Error during generic screenshot capture: {e}", extra={"markup": False})
                        new_screens = [f.name for f in manifest_files(meta.base_dir, meta.uuid, f"FILE_{i}")]
                        if new_screens and not meta.skip_imghost_upload:
                            uploaded_images, _ = await self.uploadscreens_manager.upload_screens(
                                meta, multi_screens, 1, 0, multi_screens, new_screens, {new_images_key: meta[new_images_key]}, allowed_hosts=self.approved_image_hosts
                            )
                            if uploaded_images and not meta.skip_imghost_upload:
                                await self.save_image_links(meta, new_images_key, uploaded_images)
                            for img in uploaded_images:
                                meta[new_images_key].append({"img_url": img["img_url"], "raw_url": img["raw_url"], "web_url": img["web_url"]})
                                raw_url = img["raw_url"]
                                desc.write(f"[img]{raw_url}[/img]\n")
                            desc.write("\n")

                    meta_filename = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/meta.json"
                    async with aiofiles.open(meta_filename, "w", encoding="utf-8") as f:
                        await f.write(json.dumps(meta.to_dict(), indent=4, cls=PathAwareEncoder))

        async with aiofiles.open(
            f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{self.tracker}]DESCRIPTION.txt",
            "w",
            encoding="utf-8",
        ) as desc_file:
            await desc_file.write(desc.getvalue())

    async def save_image_links(
        self,
        meta: Meta,
        image_key: str,
        image_list: list[dict[str, Any]] | None = None,
    ) -> Path | None:
        if image_list is None:
            logger.info(f"{self.tracker}: [yellow]No image links to save.[/yellow]")
            return None

        output_dir = Path(meta.base_dir) / "tmp" / meta.uuid
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        output_file = Path(output_dir) / "pack_image_links.json"

        # Load existing data if the file exists
        existing_data: dict[str, Any] = {}
        if Path(output_file).exists():
            try:
                async with aiofiles.open(output_file, encoding="utf-8") as f:
                    content = await f.read()
                    existing_data = cast(dict[str, Any], json.loads(content)) if content.strip() else {}
            except Exception as e:
                logger.warning(f"{self.tracker}: [yellow]Warning: Could not load existing image data: {e!s}[/yellow]")

        # Create data structure if it doesn't exist yet
        if not existing_data:
            existing_data = {"keys": {}, "total_count": 0}

        # Update the data with the new images under the specific key
        keys = cast(dict[str, Any], existing_data.get("keys", {}))
        if image_key not in keys:
            keys[image_key] = {"count": 0, "images": []}
            existing_data["keys"] = keys

        # Add new images to the specific key
        for idx, img in enumerate(image_list):
            image_entry = {
                "index": cast(dict[str, Any], keys[image_key]).get("count", 0) + idx,
                "raw_url": str(img.get("raw_url", "")),
                "web_url": str(img.get("web_url", "")),
                "img_url": str(img.get("img_url", "")),
            }
            cast(list[dict[str, Any]], cast(dict[str, Any], keys[image_key]).get("images", [])).append(image_entry)

        # Update counts
        key_images = cast(list[dict[str, Any]], cast(dict[str, Any], keys[image_key]).get("images", []))
        cast(dict[str, Any], keys[image_key])["count"] = len(key_images)
        existing_data["total_count"] = sum(cast(dict[str, Any], key_data).get("count", 0) for key_data in keys.values())

        try:
            async with aiofiles.open(output_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(existing_data, indent=2))

            logger.debug(f"{self.tracker}: [green]Saved {len(image_list)} new images for key '{image_key}' (total: {existing_data['total_count']}):[/green]")
            logger.debug(f"{self.tracker}: [blue]  - JSON: {output_file}[/blue]")

            return output_file
        except Exception as e:
            logger.info(f"{self.tracker}: [bold red]Error saving image links: {e}[/bold red]")
            return None

    async def get_anti_csrf_token(self, meta: Meta) -> str:
        if not Path(f"{meta.base_dir}/data/cookies").exists():
            Path(f"{meta.base_dir}/data/cookies").mkdir(parents=True, exist_ok=True)
        from src.cookie_auth import find_cookie_file

        cookiefile = find_cookie_file(meta.base_dir, self.tracker, self.config)
        logged_in = False
        uploadresponse: httpx.Response | None = None
        cookies: dict[str, str] = {}
        if Path(cookiefile).exists():
            raw_cookies = self.cookie_validator._load_cookies_dict_secure(cookiefile)  # pyright: ignore[reportPrivateUsage]
            cookies = {name: str(data.get("value", "")) for name, data in raw_cookies.items()}
            async with httpx.AsyncClient(cookies=cookies, timeout=30.0, follow_redirects=True) as client:
                uploadresponse = await client.get(f"{self.base_url}/upload.php")
                logged_in = await self.validate_login(uploadresponse)
                if logged_in is True:
                    token_match = re.search(r'data-AntiCsrfToken="(.*)"', uploadresponse.text)
                    if token_match:
                        return token_match.group(1)
            # Cookies are expired/invalid — discard them so the login POST is clean
            logger.info(f"{self.tracker}: [yellow]session expired. Clearing cookies and re-authenticating.")
            cookies = {}
            with contextlib.suppress(OSError):
                Path(cookiefile).unlink()
        else:
            logger.info(f"{self.tracker}: [yellow]Cookies not found. Creating new session.")

        passkey_match = re.match(r"https?://please\.passthepopcorn\.me:?\d*/(.+)/announce", self.announce_url)
        if not passkey_match:
            raise LoginError("Failed to extract passkey from PassThePopcorn announce URL.")  # noqa F405
        pass_key = passkey_match.group(1)
        data = {
            "username": self.username,
            "password": self.password,
            "passkey": pass_key,
            "keeplogged": "1",
        }
        headers = {"User-Agent": self.user_agent}
        async with httpx.AsyncClient(cookies=cookies, timeout=30.0, follow_redirects=True) as client:
            loginresponse = await client.post(f"{self.base_url}/ajax.php?action=login", data=data, headers=headers)
            await asyncio.sleep(2)
            try:
                resp = loginresponse.json()
                if resp["Result"] == "TfaRequired":
                    if meta.unattended and not meta.unattended_confirm:
                        raise LoginError(f"{self.tracker}: 2FA is required in unattended mode.")  # noqa: F405
                    data["TfaType"] = "normal"
                    data["TfaCode"] = await prompt_in_thread(cli_ui.ask_string, "2FA Required: Please enter PassThePopcorn 2FA code")
                    loginresponse = await client.post(f"{self.base_url}/ajax.php?action=login", data=data, headers=headers)
                    await asyncio.sleep(2)
                    resp = loginresponse.json()
                try:
                    if resp["Result"] != "Ok":
                        raise LoginError("Failed to login to PassThePopcorn. Probably due to the bad user name, password, announce url, or 2FA code.")  # noqa F405
                    anti_csrf_token = resp["AntiCsrfToken"]
                    self.cookie_validator._save_cookies_secure(client.cookies.jar, cookiefile)  # pyright: ignore[reportPrivateUsage]
                except Exception:
                    try:
                        parsed = json.loads(loginresponse.text)
                        redacted = Redaction.redact_private_info(parsed)
                        redacted_text = json.dumps(redacted)
                    except json.JSONDecodeError:
                        redacted_text = Redaction.redact_private_info(loginresponse.text)
                    raise LoginError(f"Got exception while loading JSON login response from PassThePopcorn. Response: {redacted_text}")  # noqa F405
            except Exception:
                try:
                    parsed = json.loads(loginresponse.text)
                    redacted = Redaction.redact_private_info(parsed)
                    redacted_text = json.dumps(redacted)
                except json.JSONDecodeError:
                    redacted_text = Redaction.redact_private_info(loginresponse.text)
                raise LoginError(f"Got exception while loading JSON login response from PassThePopcorn. Response: {redacted_text}")  # noqa F405
        return anti_csrf_token

    async def validate_login(self, response: httpx.Response) -> bool:
        logged_in = False
        if response.text.find("""<a href="login.php?act=recover">""") != -1:
            logger.info(f"{self.tracker}: Looks like you are not logged in to PassThePopcorn. Probably due to the bad user name, password, or expired session.")
        elif "Your popcorn quota has been reached, come back later!" in response.text:
            raise LoginError("Your PassThePopcorn request/popcorn quota has been reached, try again later")  # noqa F405
        else:
            logged_in = True
        return logged_in

    async def fill_upload_form(self, group_id: int | str | None, meta: Meta) -> tuple[str, dict[str, Any]]:
        self._prepared_upload_torrent: tuple[str, bytes] | None = None
        prerequisites: dict[str, asyncio.Task[Any]] = {}

        async def timed(stage: str, operation: Awaitable[Any]) -> Any:
            started_at = time.perf_counter()
            try:
                return await operation
            finally:
                log_ptp_timing(meta, stage, started_at)

        if meta.unattended and not meta.unattended_confirm:
            prerequisites["csrf"] = asyncio.create_task(timed("session and CSRF token", self.get_upload_anti_csrf_token(meta)))
            if group_id is None:
                snapshot = meta.copy()
                prerequisites["metadata"] = asyncio.create_task(timed("PTP title metadata lookup", self.get_title_info_for_meta(snapshot)))
                cover = select_ptp_cover_url(snapshot)
                if cover is not None:
                    prerequisites["poster"] = asyncio.create_task(timed("poster download and hosting", self.rehost_poster_to_selected_host(snapshot, cover)))
        form_task = asyncio.create_task(self._fill_upload_form(group_id, meta, prerequisites))
        torrent_task = asyncio.create_task(self._prepare_upload_torrent(meta))
        tasks = [form_task, torrent_task, *prerequisites.values()]
        try:
            form, torrent_bytes = await asyncio.gather(form_task, torrent_task)
            torrent_path = str(Path(meta.base_dir) / "tmp" / meta.uuid / f"[{self.tracker}].torrent")
            self._prepared_upload_torrent = torrent_path, torrent_bytes
            return form
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.cancel_prefetch(meta)

    async def _fill_upload_form(
        self, group_id: int | str | None, meta: Meta, prerequisites: dict[str, asyncio.Task[Any]]
    ) -> tuple[str, dict[str, Any]]:
        resolution, other_resolution = self.get_resolution(meta)
        description_started_at = time.perf_counter()
        await self.edit_desc(meta)
        log_ptp_timing(meta, "description generation", description_started_at)
        file_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{self.tracker}]DESCRIPTION.txt"
        desc = ""
        try:
            Path(file_path).stat()  # Ensures the file is accessible
            async with aiofiles.open(file_path, encoding="utf-8") as f:
                desc = await f.read()
        except OSError as e:
            logger.info(f"{self.tracker}: File error: {e}", extra={"markup": False})
        ptp_subtitles = self.get_subtitles(meta)
        no_audio_found = False
        english_audio = False
        if meta.is_disc == "BDMV":
            bdinfo = meta.bdinfo
            audio_tracks = bdinfo.get("audio", [])
            if audio_tracks:
                first_language = str(audio_tracks[0].get("Language", "")).lower()
                if not first_language:
                    no_audio_found = True
                elif first_language.startswith("en"):
                    english_audio = True
                else:
                    english_audio = False
        else:
            mediainfo = meta.mediainfo
            audio_tracks = [track for track in mediainfo.get("media", {}).get("track", []) if track.get("@type") == "Audio"]
            logger.debug(f"{self.tracker}: [Debug] Found {len(audio_tracks)} audio tracks")

            if not audio_tracks:
                no_audio_found = True
                logger.info(f"{self.tracker}: [yellow]No audio tracks found in mediainfo")
            else:
                first_language = str(audio_tracks[0].get("Language", "")).lower()
                logger.debug(f"{self.tracker}: [Debug] First audio track language: {first_language}")

                if not first_language:
                    no_audio_found = True
                elif first_language.startswith("en"):
                    english_audio = True
                else:
                    english_audio = False

        ptp_trumpable = None
        if meta.hardcoded_subs:
            ptp_trumpable, ptp_subtitles = self.get_trumpable(ptp_subtitles)
            if ptp_trumpable and 50 in ptp_trumpable:
                ptp_trumpable.remove(50)
                ptp_trumpable.append(4)
            if ptp_trumpable and 14 in ptp_trumpable and 44 in ptp_subtitles:
                ptp_subtitles.remove(44)
            if ptp_trumpable and 15 in ptp_trumpable:
                ptp_trumpable.remove(15)
                ptp_trumpable.append(4)
                if 44 in ptp_subtitles:
                    ptp_subtitles.remove(44)
                if not english_audio and (not any(x in [3, 50] for x in ptp_subtitles)):
                    ptp_trumpable.append(14)

        elif no_audio_found and (not any(x in [3, 50] for x in ptp_subtitles)):
            cli_ui.info("No English subs and no audio tracks found should this be trumpable?")
            if (not meta.unattended or meta.unattended_confirm) and await prompt_in_thread(cli_ui.ask_yes_no, "Mark trumpable?", default=True):
                ptp_trumpable, ptp_subtitles = self.get_trumpable(ptp_subtitles)
        elif not english_audio and (not any(x in [3, 50] for x in ptp_subtitles)):
            cli_ui.info("No English subs and English audio is not the first audio track, should this be trumpable?")
            if (not meta.unattended or meta.unattended_confirm) and await prompt_in_thread(cli_ui.ask_yes_no, "Mark trumpable?", default=True):
                ptp_trumpable, ptp_subtitles = self.get_trumpable(ptp_subtitles)

        logger.debug(f"{self.tracker}: ptp_trumpable: {ptp_trumpable}")
        logger.debug(f"{self.tracker}: ptp_subtitles: {ptp_subtitles}")
        csrf_started_at = time.perf_counter()
        if "csrf" in prerequisites:
            anti_csrf_token = await prerequisites["csrf"]
        else:
            anti_csrf_token = await self.get_upload_anti_csrf_token(meta)
            log_ptp_timing(meta, "session and CSRF token", csrf_started_at)
        data: dict[str, Any] = {
            "submit": "true",
            "remaster_year": "",
            "remaster_title": self.get_remaster_title(meta),  # Eg.: Hardcoded English
            "type": self.get_type(meta.imdb_info, meta),
            "codec": "Other",  # Sending the codec as custom.
            "other_codec": self.get_codec(meta),
            "container": "Other",
            "other_container": self.get_container(meta),
            "resolution": resolution,
            "source": "Other",  # Sending the source as custom.
            "other_source": self.get_source(meta.source or ""),
            "release_desc": desc,
            "nfo_text": "",
            "subtitles[]": ptp_subtitles,
            "trumpable[]": ptp_trumpable,
            "AntiCsrfToken": anti_csrf_token,
        }
        if data["remaster_year"] != "" or data["remaster_title"] != "":
            data["remaster"] = "on"
        if meta.scene is True:
            data["scene"] = "on"
        if resolution == "Other":
            data["other_resolution"] = other_resolution
        if meta.personalrelease is True:
            data["internalrip"] = "on"
        # IF SPECIAL (idk how to check for this automatically)
        # data["special"] = "on"
        imdb_id_value = meta.imdb_id
        imdb_id_int = imdb_id_value if isinstance(imdb_id_value, (int, str)) else 0
        if imdb_id_int == 0:
            data["imdb"] = "0"
        else:
            data["imdb"] = str(imdb_id_int).zfill(7)
        if group_id is None:  # If need to make new group
            url = f"{self.base_url}/upload.php"
            if "metadata" in prerequisites:
                tinfo = await prerequisites["metadata"]
            else:
                metadata_started_at = time.perf_counter()
                tinfo = await self.get_title_info_for_meta(meta)
                log_ptp_timing(meta, "PTP title metadata lookup", metadata_started_at)
            if meta.youtube is None or "youtube" not in meta.youtube:
                youtube = (
                    ""
                    if meta.unattended
                    else await prompt_in_thread(
                        cli_ui.ask_string, "Unable to find youtube trailer, please link one e.g.(https://www.youtube.com/watch?v=dQw4w9WgXcQ)", default=""
                    )
                )
                meta.youtube = youtube
            cover = select_ptp_cover_url(meta)
            if "poster" in prerequisites:
                cover = await prerequisites["poster"]
            elif cover is not None:
                poster_started_at = time.perf_counter()
                cover = await self.rehost_poster_to_selected_host(meta, cover)
                log_ptp_timing(meta, "poster download and hosting", poster_started_at)
            while cover is None:
                if meta.unattended and not meta.unattended_confirm:
                    meta.skipping = self.tracker
                    raise UploadError(f"{self.tracker}: Cover is required in unattended mode.")
                cover_input = await prompt_in_thread(cli_ui.ask_string, "No Cover was found. Please input a link to a cover: \n", default="") or "".strip()
                if not cover_input:
                    continue
                if not cover_input.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                    logger.info(f"{self.tracker}: [red]Cover URL must end with .jpg, .jpeg, .png, or .webp")
                    continue
                cover = await self.rehost_poster_to_selected_host(meta, cover_input)
            tmdb_id = meta.tmdb_id or meta.tmdb
            tmdb_type = (meta.tmdb_type or "movie").lower()
            tmdb_path = "tv" if tmdb_type in ("tv", "series", "tv_show") else "movie"
            album_desc = tinfo.get("plot", tinfo.get("album_desc", meta.overview)) or ""
            if tmdb_id:
                album_desc = f"{album_desc}\n\nhttps://www.themoviedb.org/{tmdb_path}/{tmdb_id}"
            new_data = {
                "title": tinfo.get("title", meta.imdb_info.get("title", meta.title)),
                "year": tinfo.get("year", meta.imdb_info.get("year", meta.year)),
                "image": cover,
                "tags": tinfo.get("tags", ""),
                "album_desc": album_desc,
                "trailer": meta.youtube,
            }
            if new_data["year"] in ["", "0", 0, None] and meta.manual_year not in [0, "", None]:
                new_data["year"] = meta.manual_year
            if not new_data["tags"]:
                if (meta.mode if meta.mode is not None else "non_cli") == "cli":
                    if meta.unattended and not meta.unattended_confirm:
                        logger.info(f"{self.tracker}: [yellow]Unattended mode: Unable to match any tags. Skipping {self.tracker} upload.[/yellow]")
                        meta.skipping = f"{self.tracker}"
                        raise UploadError(f"{self.tracker}: Unable to match any tags in unattended mode.")
                    while not new_data["tags"]:
                        logger.info(f"{self.tracker}: [yellow]Unable to match any tags")
                        logger.info(f"{self.tracker}: Valid tags can be found on the PassThePopcorn upload form")
                        new_data["tags"] = await prompt_in_thread(cli_ui.ask_string, "Please enter at least one tag. Comma separated (action, animation, short):")
                else:
                    raise UploadError("PassThePopcorn requires at least one valid tag.")
            data.update(new_data)
            imdb_info = meta.imdb_info
            directors: list[str] | tuple[str, ...] | None = None
            directors_value = imdb_info.get("directors")
            if isinstance(directors_value, (list, tuple)):
                director_names = [director for director in cast(list[Any], directors_value) if isinstance(director, str)]
                directors = tuple(director_names)
            if directors:
                data["artist[]"] = directors
                data["importance[]"] = "1"
        else:  # Upload on existing group
            url = f"{self.base_url}/upload.php?groupid={group_id}"
            data["groupid"] = group_id

        return url, data

    async def get_name(self, meta: Meta) -> str:
        # PTP doesn't use a custom releasename in the upload form, so just return the filename
        return meta.name

    @staticmethod
    def _find_non_ascii_torrent_paths(torrent: Torrent) -> list[str]:
        paths = [torrent.name, *(str(path) for path in torrent.files)]
        return list(dict.fromkeys(path for path in paths if not path.isascii()))

    @staticmethod
    def _extract_upload_error_message(response_text: str) -> str:
        alert = BeautifulSoup(response_text, "html.parser").select_one(".alert--error")
        if alert is None:
            return ""
        message = re.sub(r"\s+", " ", alert.get_text(" ", strip=True))
        return re.sub(r"\s+([.,!?;:])", r"\1", message)

    async def _prepare_upload_torrent(self, meta: Meta) -> bytes:
        common = Common(config=self.config)
        torrent_file_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{self.tracker}].torrent"
        torrent_started_at = time.perf_counter()
        await common.create_torrent_for_upload(meta, self.tracker, self.source_flag)
        log_ptp_timing(meta, "torrent file creation", torrent_started_at)

        torrent = await asyncio.to_thread(Torrent.read, torrent_file_path)
        invalid_paths = self._find_non_ascii_torrent_paths(torrent)
        if invalid_paths:
            paths = ", ".join(repr(path) for path in invalid_paths)
            raise UploadError(
                f"{self.display_name} cannot upload torrent paths containing non-ASCII characters: {paths}. "
                "Rename the files to use ASCII characters and recreate the torrent."
            )

        async with aiofiles.open(torrent_file_path, "rb") as torrent_file_handle:
            return await torrent_file_handle.read()

    async def upload(self, meta: Meta, url: str, data: dict[str, Any]) -> bool:
        common = Common(config=self.config)
        torrent_file_path = str(Path(meta.base_dir) / "tmp" / meta.uuid / f"[{self.tracker}].torrent")
        prepared = getattr(self, "_prepared_upload_torrent", None)
        self._prepared_upload_torrent = None
        if prepared is not None and prepared[0] == torrent_file_path:
            torrent_bytes = prepared[1]
        else:
            torrent_bytes = await self._prepare_upload_torrent(meta)
        files = {"file_input": ("placeholder.torrent", torrent_bytes, "application/x-bittorent")}
        headers = {
            # 'ApiUser' : self.api_user,
            # 'api_key' : self.api_key,
            "User-Agent": self.user_agent
        }
        if meta.debug:
            debug_data = data.copy()
            # Redact the AntiCsrfToken
            if "AntiCsrfToken" in debug_data:
                debug_data["AntiCsrfToken"] = "[REDACTED]"
            logger.debug(url)
            logger.debug(Redaction.redact_private_info(debug_data))
            meta.tracker_status[self.tracker]["status_message"] = "Debug mode enabled, not uploading."
            debug_torrent_started_at = time.perf_counter()
            await common.create_torrent_for_upload(meta, f"{self.tracker}" + "_DEBUG", f"{self.tracker}" + "_DEBUG", announce_url="https://fake.tracker")
            log_ptp_timing(meta, "debug torrent file creation", debug_torrent_started_at)
            return True  # Debug mode - simulated success
        failure_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{self.tracker}]PTP_upload_failure.html"
        from src.cookie_auth import find_cookie_file

        cookiefile = find_cookie_file(meta.base_dir, self.tracker, self.config)
        raw_cookies = self.cookie_validator._load_cookies_dict_secure(cookiefile)  # pyright: ignore[reportPrivateUsage]
        cookies = {name: str(data.get("value", "")) for name, data in raw_cookies.items()}
        async with httpx.AsyncClient(cookies=cookies, timeout=60.0, follow_redirects=True) as client:
            request_started_at = time.perf_counter()
            try:
                response = await client.post(url=url, data=data, headers=headers, files=files)
            finally:
                log_ptp_timing(meta, "upload HTTP request", request_started_at)
        logger.info(f"{self.tracker}: [cyan]{response.url}")
        responsetext = response.text

        # URL format in case of successful upload: https://passthepopcorn.me/torrents.php?id=9329&torrentid=91868
        expected_host = urlparse(self.base_url).netloc
        match = re.match(rf".*?{re.escape(expected_host)}/torrents\.php\?id=(\d+)&torrentid=(\d+)", str(response.url))
        if match is None:
            error_message = self._extract_upload_error_message(responsetext)
            async with aiofiles.open(failure_path, "w", encoding="utf-8") as f:
                await f.write(responsetext)
            status_message = f"data error: see {failure_path}"
            if error_message:
                status_message += f" | {error_message}"
            meta.tracker_status[self.tracker]["status_message"] = status_message
            if error_message and re.search(r'\bthe same base save path\s+"[^"]+"\s+already exists on the site\b', error_message, re.IGNORECASE):
                meta.tracker_status[self.tracker]["dupe"] = True
            return False

        if match:
            status = meta.tracker_status[self.tracker]
            status["status_message"] = str(response.url)
            status["upload_success"] = True
            return True
        return False

    async def prepare_for_seeding(self, meta: Meta) -> None:
        if meta.debug:
            return
        started_at = time.perf_counter()
        try:
            await Common(config=self.config).create_torrent_ready_to_seed(
                meta, self.tracker, self.source_flag, self.announce_url, str(meta.tracker_status[self.tracker]["status_message"])
            )
        finally:
            log_ptp_timing(meta, "local torrent finalization", started_at)
