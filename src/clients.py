# Upload Assistant © 2025 Audionut & wastaken7 — Licensed under UAPL v1.0
import asyncio
import os
import re
import shutil
import time
import urllib.parse
from collections.abc import Sequence
from pathlib import Path
from typing import Any, ClassVar, cast

import defusedxml.xmlrpc
import httpx
import qbittorrentapi
from torf import Torrent

from src.console import logger
from src.meta import Meta
from src.stats import record_event_async
from src.torrent_clients import DelugeClientMixin, QbittorrentClientMixin, RtorrentClientMixin, TransmissionClientMixin
from src.torrent_clients.path_utils import coerce_str_list, is_path_under, map_remote_to_local_path
from src.torrent_manifest import TorrentManifest
from src.torrent_policy import HDBITS_POLICY, TorrentStats, generic_reuse_allowed
from src.torrentcreate import DISC_EXCLUDE_GLOBS, SUBTITLE_EXTENSIONS

# Secure XML-RPC client using defusedxml to prevent XML attacks
defusedxml.xmlrpc.monkey_patch()


class Clients(QbittorrentClientMixin, RtorrentClientMixin, DelugeClientMixin, TransmissionClientMixin):
    _qbit_search_results_cache: dict[tuple[int, str], list[Any]]
    _hash_metadata_prefetch_tasks: ClassVar[dict[tuple[str, str], tuple[asyncio.Task[Meta], Meta, str, dict[str, str]]]] = {}

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize torrent-client operations with the application config."""
        self.config = config
        self._tracker_comment_hosts: dict[str, tuple[str, ...]] | None = None
        self._qbit_search_results_cache: dict[tuple[int, str], list[Any]] = {}

    @staticmethod
    def _client_infohash(candidate: str | Path, torrent: Torrent) -> str:
        """Keep the client's hash when its exported torrent is named by infohash."""
        candidate_stem = Path(candidate).stem.strip().lower()
        if re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", candidate_stem):
            return candidate_stem
        return str(torrent.infohash)

    @staticmethod
    def _matches_tracker_host(host: str, tracker_hosts: dict[str, tuple[str, ...]]) -> str | None:
        for tracker_name, domains in tracker_hosts.items():
            if any(host == domain or host.endswith(f".{domain}") for domain in domains):
                return tracker_name
        return None

    def _get_tracker_comment_hosts(self) -> dict[str, tuple[str, ...]]:
        if self._tracker_comment_hosts is None:
            from src.trackersetup import get_tracker_comment_hosts

            self._tracker_comment_hosts = get_tracker_comment_hosts(self.config)
        return self._tracker_comment_hosts

    def _extract_tracker_ids_from_comment(self, comment: str) -> dict[str, str]:
        """Extract known tracker IDs from a torrent comment URL set."""
        if not comment:
            return {}

        def _last_path_id(path: str) -> str | None:
            """Extract a numeric tracker ID from the end of a URL path."""
            match = re.search(r"/(\d+)$", path)
            return match.group(1) if match else None

        def _query_id(query: str, key: str) -> str | None:
            """Extract the first value for key from a URL query string."""
            values = urllib.parse.parse_qs(query).get(key)
            return values[0] if values else None

        tracker_ids: dict[str, str] = {}
        urls: list[str] = re.findall(r"https?://[^\s\"'<>]+", comment)
        tracker_hosts = self._get_tracker_comment_hosts()
        for url in urls:
            parsed = urllib.parse.urlparse(url)
            host = (parsed.hostname or "").lower()
            path = parsed.path

            matched_tracker = self._matches_tracker_host(host, tracker_hosts)

            if not matched_tracker:
                continue

            # Canonical-class-name → established metadata key mapping
            _tracker_key_aliases: dict[str, str] = {
                "PASSTHEPOPCORN": "ptp",
                "HDBITS": "hdb",
                "BEYONDHD": "bhd",
                "BLUTOPIA": "blu",
                "ONLYENCODES": "oe",
                "BROADCASTHENET": "broadcasthenet",
            }
            tracker_key = _tracker_key_aliases.get(matched_tracker, matched_tracker.lower())

            if matched_tracker == "PASSTHEPOPCORN":
                ptp_id = _query_id(parsed.query, "torrentid")
                if ptp_id:
                    tracker_ids[tracker_key] = ptp_id
            elif matched_tracker == "HDBITS":
                hdb_id = _query_id(parsed.query, "id")
                if hdb_id:
                    tracker_ids[tracker_key] = hdb_id
            elif matched_tracker == "BROADCASTHENET":
                btn_id = _query_id(parsed.query, "id")
                if btn_id:
                    tracker_ids[tracker_key] = btn_id
            elif matched_tracker in {"BeyondHD", "BEYONDHD"}:
                match = re.search(r"/details/(\d+)", path)
                if match:
                    tracker_ids[tracker_key] = match.group(1)
            elif matched_tracker == "ORPHEUS":
                torrent_id = _query_id(parsed.query, "torrentid")
                if torrent_id:
                    tracker_ids[tracker_key] = torrent_id
            else:
                # UNIT3D style: last path ID
                tracker_id = _last_path_id(path)
                if tracker_id:
                    tracker_ids[tracker_key] = tracker_id

        return tracker_ids

    async def add_to_client(self, meta: Meta, tracker: str, cross: bool = False) -> None:
        """Add the prepared torrent to each configured client."""
        if meta.path is None:
            logger.info("[bold red]meta.path is None, cannot add to client")
            return
        if cross:
            torrent_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{tracker}_cross].torrent"
        elif meta.debug:
            torrent_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{tracker}_DEBUG].torrent"
        else:
            torrent_path = f"{meta.base_dir}{'/' + 'tmp' + '/'}{meta.uuid}/[{tracker}].torrent"
        if meta.no_seed is True:
            logger.info("[bold red]--no-seed was passed, so the torrent will not be added to the client")
            logger.info("[bold yellow]Add torrent manually to the client")
            return
        if Path(torrent_path).exists():
            torrent = Torrent.read(torrent_path)
        else:
            logger.info(f"[bold red]Torrent file {torrent_path} does not exist, cannot add to client")
            return

        inject_clients: list[str] = []
        client_value = meta.client
        if isinstance(client_value, str) and client_value != "none":
            inject_clients = [client_value]
            logger.debug(f"[cyan]DEBUG: Using client from meta: {inject_clients}[/cyan]")
        elif client_value == "none":
            logger.debug("[cyan]DEBUG: meta client is 'none', skipping adding to client[/cyan]")
            return
        else:
            try:
                inject_clients_config = self.config["DEFAULT"].get("injecting_client_list")
                if isinstance(inject_clients_config, str) and inject_clients_config.strip():
                    inject_clients = [inject_clients_config]
                    logger.debug(f"[cyan]DEBUG: Converted injecting_client_list string to list: {inject_clients}[/cyan]")
                elif isinstance(inject_clients_config, list):
                    # Filter out empty strings and whitespace-only strings
                    inject_clients_list = cast(list[Any], inject_clients_config)
                    inject_clients = [str(c).strip() for c in inject_clients_list if str(c).strip()]
                    logger.debug(f"[cyan]DEBUG: Using injecting_client_list from config: {inject_clients}[/cyan]")
                else:
                    inject_clients = []
            except Exception as e:
                logger.debug(f"[cyan]DEBUG: Error reading injecting_client_list from config: {e}[/cyan]")

            if not inject_clients:
                default_client = self.config["DEFAULT"].get("default_torrent_client")
                if isinstance(default_client, str) and default_client != "none":
                    logger.debug(f"[cyan]DEBUG: Falling back to default_torrent_client: {default_client}[/cyan]")
                    inject_clients = [default_client]

        if not inject_clients:
            logger.debug("[cyan]DEBUG: No clients configured for injecting[/cyan]")
            return

        logger.debug(f"[cyan]DEBUG: Clients to inject into: {inject_clients}[/cyan]")

        clients_config = self.config.get("TORRENT_CLIENTS", {})
        for client_name in inject_clients:
            client_to_skip = self.config["TRACKERS"][tracker].get("client_to_skip", [])
            if client_name in client_to_skip:
                logger.debug(f"[cyan]DEBUG: Skipping client '{client_name}' for tracker '{tracker}' as it's in client_to_skip list[/cyan]")
                continue
            if client_name == "none" or not client_name:
                continue

            if client_name not in clients_config:
                logger.info(f"[bold red]Torrent client '{client_name}' not found in config.")
                continue

            client = clients_config[client_name]
            torrent_client = client["torrent_client"]
            await self.inject_delay(meta, tracker, client_name)

            # Must pass client_name to remote_path_map
            local_path, remote_path = await self.remote_path_map(meta, client_name)

            logger.debug(f"[bold green]Adding to {client_name} ({torrent_client})")

            operation_started = time.monotonic()
            try:
                if torrent_client.lower() == "rtorrent":
                    self.rtorrent(meta.path, torrent_path, torrent, meta, local_path, remote_path, client, tracker)
                elif torrent_client == "qbit":
                    await self.qbittorrent(meta.path, torrent, local_path, remote_path, client, meta.is_disc, meta.filelist, meta, tracker, cross)
                elif torrent_client.lower() == "deluge":
                    self.deluge(meta.path, torrent_path, torrent, local_path, remote_path, client)
                elif torrent_client.lower() == "transmission":
                    self.transmission(meta.path, torrent, local_path, remote_path, client, meta)
                elif torrent_client.lower() == "watch":
                    shutil.copy(torrent_path, client["watch_folder"])
                await record_event_async(
                    "api", service=client_name, operation="torrent_client_add", outcome="success", duration_ms=(time.monotonic() - operation_started) * 1000
                )
            except Exception as e:
                await record_event_async("api", service=client_name, operation="torrent_client_add", outcome="error", duration_ms=(time.monotonic() - operation_started) * 1000)
                logger.info(f"[bold red]Failed to add torrent to {client_name}: {e}")
        return

    async def inject_delay(self, meta: Meta, tracker: str, client_name: str) -> None:
        """
        Applies an optional delay before injecting a torrent into the client.

        The delay can be configured either per tracker or globally in the default settings.
        When both are defined, the tracker-specific value takes precedence over the client setting.

        This mechanism exists to handle cases where a tracker requires a short amount
        of time to register the uploaded torrent hash. Injecting the torrent too early
        may cause connectivity issues, such as failing to discover peers even though
        they are already available.

        By waiting before injection, this function helps ensure proper tracker
        synchronization and more reliable peer discovery.
        """
        tracker_cfg = self.config.get("TRACKERS", {}).get(tracker, {})
        # None retains the setting while inheriting DEFAULT; 0 still disables it.
        has_tracker_delay = isinstance(tracker_cfg, dict) and tracker_cfg.get("inject_delay") is not None
        inject_delay = tracker_cfg.get("inject_delay") if has_tracker_delay else self.config["DEFAULT"].get("inject_delay", 0)
        if inject_delay is None or (isinstance(inject_delay, str) and not inject_delay.strip()):
            return

        try:
            inject_delay = int(inject_delay)
        except ValueError, TypeError:
            if has_tracker_delay:
                logger.info(f"{tracker}: [bold red]CONFIG ERROR: 'inject_delay' must be an integer")
            else:
                logger.info("[bold red]CONFIG ERROR: 'inject_delay' must be an integer")
            inject_delay = 0

        if inject_delay < 0:
            logger.info("[bold red]CONFIG ERROR: 'inject_delay' must be >= 0")
            inject_delay = 0
        if inject_delay > 0:
            if meta.debug or inject_delay > 5:
                if has_tracker_delay:
                    logger.info(f"{tracker}: [cyan]Waiting {inject_delay} seconds before adding to client '{client_name}'[/cyan]")
                else:
                    logger.info(f"[cyan]Waiting {inject_delay} seconds before adding to client '{client_name}'[/cyan]")
            await asyncio.sleep(inject_delay)

    async def find_existing_torrents(self, meta: Meta) -> list[str]:
        """Collect and register reusable variants from every configured search client."""
        if meta.get("skip_auto_torrent", False):
            return []
        requested = meta.client
        if meta.source_torrent_hash and meta.source_torrent_client:
            client_names = [meta.source_torrent_client]
        elif isinstance(requested, str) and requested != "none":
            client_names = [requested]
        else:
            configured = self.config["DEFAULT"].get("searching_client_list", [])
            client_names = [str(name) for name in configured if str(name) and str(name) != "none"] if isinstance(configured, list) else []
            default = str(self.config["DEFAULT"].get("default_torrent_client", "none"))
            if not client_names and default != "none":
                client_names = [default]

        manifest = TorrentManifest(meta.base_dir, meta.uuid)
        paths: list[str] = []
        original_client = meta.client
        try:
            for client_name in client_names:
                if client_name not in self.config.get("TORRENT_CLIENTS", {}):
                    logger.info(f"[yellow]Client '{client_name}' not found in TORRENT_CLIENTS config, skipping...")
                    continue
                meta.client = client_name
                operation_started = time.monotonic()
                outcome = "error"
                try:
                    result = await self._search_single_client_for_torrent(meta, client_name, False, False, None, True)
                    outcome = "success"
                finally:
                    await record_event_async(
                        "api", service=client_name, operation="torrent_client_search", outcome=outcome, duration_ms=(time.monotonic() - operation_started) * 1000
                    )
                candidates = result if isinstance(result, list) else [result] if isinstance(result, str) else []
                for candidate in candidates:
                    if meta.subtitle_files and not self._torrent_includes_all_local_subtitles(candidate, meta) and not self._torrent_has_no_subtitles(candidate):
                        continue
                    torrent = Torrent.read(candidate)
                    has_subs = any(Path(str(file)).suffix.casefold() in SUBTITLE_EXTENSIONS for file in torrent.files)
                    entry = manifest.register(
                        candidate,
                        "base_subs" if has_subs else "base",
                        f"client:{client_name}",
                        client_infohash=self._client_infohash(candidate, torrent),
                    )
                    managed = str(manifest.entry_path(entry))
                    if managed not in paths:
                        paths.append(managed)
                    meta.reuse_torrent_client = client_name
                    candidate_path = Path(candidate)
                    client_staging = Path(meta.base_dir) / "tmp" / meta.uuid / "torrents" / ".client"
                    if client_staging.resolve() in candidate_path.resolve().parents:
                        candidate_path.unlink(missing_ok=True)
        finally:
            meta.client = original_client
        return paths

    async def find_existing_torrent(self, meta: Meta) -> str | None:
        """Return the preferred registered variant for compatibility with callers."""
        paths = await self.find_existing_torrents(meta)
        if not paths:
            return None
        manifest = TorrentManifest(meta.base_dir, meta.uuid)
        entries = [entry for path in paths if (entry := manifest.entry_for_path(path)) is not None]
        if meta.subtitle_files and any(entry.layout == "base_subs" for entry in entries):
            entries = [entry for entry in entries if entry.layout == "base_subs"]
        if self.config["DEFAULT"].get("prefer_max_16_torrent", False):
            entries.sort(key=lambda entry: entry.piece_size)
        chosen = entries[0]
        if chosen.origin.startswith("client:"):
            meta.reuse_torrent_client = chosen.origin.removeprefix("client:")
        meta.reuse_torrent_infohash = chosen.client_infohash
        return str(manifest.entry_path(chosen))

    async def _find_existing_torrent(self, meta: Meta) -> str | None:
        """Find a reusable torrent matching the prepared metadata."""
        if meta.get("skip_auto_torrent", False):
            return None

        # Determine piece size preferences
        piece_limit = bool(self.config["DEFAULT"].get("prefer_max_16_torrent", False))
        prefer_small_pieces = piece_limit
        best_match = None  # Track the best match for fallback if prefer_small_pieces is enabled
        video_only_fallback: tuple[str, str] | None = None

        default_torrent_client = cast(str, self.config["DEFAULT"]["default_torrent_client"])

        clients_to_search: list[str]
        meta_client = meta.client
        if isinstance(meta_client, str) and meta_client != "none":
            clients_to_search = [meta_client]
            logger.debug(f"[cyan]DEBUG: Using client from meta: {clients_to_search}[/cyan]")
        else:
            searching_list = self.config["DEFAULT"].get("searching_client_list", [])
            searching_list_values = cast(list[Any], searching_list) if isinstance(searching_list, list) else []

            if searching_list_values:
                clients_to_search = [str(c) for c in searching_list_values if str(c) and str(c) != "none"]
                logger.debug(f"[cyan]DEBUG: Using searching_client_list from config: {clients_to_search}[/cyan]")
            else:
                clients_to_search = []

            if not clients_to_search:
                if default_torrent_client and default_torrent_client != "none":
                    clients_to_search = [default_torrent_client]
                    logger.debug(f"[cyan]DEBUG: Falling back to default_torrent_client: {default_torrent_client}[/cyan]")
                else:
                    logger.info("[yellow]No clients configured for searching...[/yellow]")
                    return None

        for client_name in clients_to_search:
            if client_name not in self.config["TORRENT_CLIENTS"]:
                logger.info(f"[yellow]Client '{client_name}' not found in TORRENT_CLIENTS config, skipping...")
                continue

            result = await self._search_single_client_for_torrent(meta, client_name, prefer_small_pieces, piece_limit, best_match)

            if result:
                candidate_path = result.get("torrent_path") if isinstance(result, dict) else result
                if meta.subtitle_files and isinstance(candidate_path, str) and not self._torrent_includes_all_local_subtitles(candidate_path, meta):
                    # Only a subtitle-free torrent can safely provide BASE.torrent.
                    # A partially subtitle-bearing torrent would produce an invalid
                    # BASE_SUBS.torrent and omit selected local subtitles.
                    if self._torrent_has_no_subtitles(candidate_path) and (
                        video_only_fallback is None or not prefer_small_pieces or self._is_preferred_piece_size_candidate(candidate_path, video_only_fallback[0], piece_limit)
                    ):
                        video_only_fallback = (candidate_path, client_name)
                    continue
                if isinstance(result, dict):
                    # Got a valid torrent but not ideal piece size
                    best_match = {**result, "client_name": client_name}
                    # If prefer_small_pieces is False, we don't care about piece size optimization
                    # so stop searching after finding the first valid torrent
                    if not prefer_small_pieces:
                        logger.info(f"[green]Found valid torrent in client '{client_name}', stopping search[/green]")
                        torrent_path = best_match.get("torrent_path")
                        meta.reuse_torrent_client = client_name
                        return torrent_path if isinstance(torrent_path, str) else None
                else:
                    # Got a path - this means we found a torrent with ideal piece size
                    logger.debug(f"[green]Found valid torrent with preferred piece size in client '{client_name}', stopping search[/green]")
                    meta.reuse_torrent_client = client_name
                    return result

        if prefer_small_pieces and best_match:
            logger.info(f"[yellow]Using best match torrent with hash: [bold yellow]{best_match['torrenthash']}[/bold yellow]")
            torrent_path = best_match.get("torrent_path")
            meta.reuse_torrent_client = cast(str | None, best_match.get("client_name"))
            return torrent_path if isinstance(torrent_path, str) else None

        if video_only_fallback:
            logger.info("[yellow]No matching torrent with all local subtitles found; using the video-only fallback.[/yellow]")
            meta.reuse_torrent_client = video_only_fallback[1]
            return video_only_fallback[0]

        logger.info("[bold yellow]No Valid .torrent found")
        return None

    async def _search_single_client_for_torrent(
        self,
        meta: Meta,
        client_name: str,
        prefer_small_pieces: bool,
        piece_limit: bool,
        best_match: dict[str, Any] | None,
        collect_all: bool = False,
    ) -> list[str] | dict[str, Any] | str | None:
        """Search a single client for an existing torrent by hash or via API search (qbit only)."""

        client = self.config["TORRENT_CLIENTS"][client_name]
        torrent_client = client.get("torrent_client", "").lower()
        torrent_storage_dir = client.get("torrent_storage_dir")
        qbt_client: qbittorrentapi.Client | None = None
        proxy_url: str | None = None
        candidates: list[str] = []

        # Iterate through pre-specified hashes
        for hash_key in ["source_torrent_hash", "torrenthash", "ext_torrenthash"]:
            hash_value = meta.get(hash_key)
            if hash_value:
                hash_value_str = str(hash_value)
                # If no torrent_storage_dir defined, use saved torrent from qbit
                extracted_torrent_dir = Path(meta.base_dir) / "tmp" / meta.uuid / "torrents" / ".client"

                if torrent_storage_dir:
                    torrent_path = Path(torrent_storage_dir) / f"{hash_value_str}.torrent"
                else:
                    if torrent_client != "qbit":
                        return None

                    try:
                        proxy_url = client.get("qui_proxy_url")
                        export_started = time.perf_counter()
                        if proxy_url:
                            qbt_proxy_url = proxy_url.rstrip("/")
                            async with httpx.AsyncClient() as session:
                                try:
                                    response = await session.post(f"{qbt_proxy_url}/api/v2/torrents/export", data={"hash": hash_value_str})
                                    if response.status_code == 200:
                                        torrent_file_content = response.content
                                    else:
                                        logger.error(f"[red]Failed to export torrent via proxy: {response.status_code}")
                                        continue
                                except Exception as e:
                                    logger.error(f"[red]Error exporting torrent via proxy: {e}")
                                    continue
                        else:
                            potential_qbt_client = await self.init_qbittorrent_client(client)
                            if not potential_qbt_client:
                                continue
                            qbt_client = potential_qbt_client

                            qbt_client_local: qbittorrentapi.Client = qbt_client

                            try:
                                torrent_file_content = await self.retry_qbt_operation(
                                    lambda qbt_client_local=qbt_client_local, hash_value_str=hash_value_str: asyncio.to_thread(
                                        qbt_client_local.torrents_export, torrent_hash=hash_value_str
                                    ),
                                    f"Export torrent {hash_value_str}",
                                )
                            except TimeoutError, qbittorrentapi.APIError:
                                continue
                        logger.info(f"[cyan]Client torrent export for hash {hash_value_str} completed in {time.perf_counter() - export_started:.2f}s[/cyan]")
                        if not torrent_file_content:
                            logger.info(f"[bold red]qBittorrent returned an empty response for hash {hash_value_str}")
                            continue  # Skip to the next hash

                        # Save the .torrent file
                        Path(extracted_torrent_dir).mkdir(parents=True, exist_ok=True)
                        torrent_path = Path(extracted_torrent_dir) / f"{hash_value_str}.torrent"

                        await asyncio.to_thread(Path(torrent_path).write_bytes, torrent_file_content)

                        logger.info(f"[green]Successfully saved .torrent file: {torrent_path}")

                    except qbittorrentapi.APIError as e:
                        logger.info(f"[bold red]Failed to fetch .torrent from qBittorrent for hash {hash_value_str}: {e}")
                        continue

                # Validate the .torrent file
                valid, resolved_path = await self.is_valid_torrent(meta, torrent_path, hash_value_str, torrent_client, client)

                if valid:
                    logger.info(f"[green]Using validated torrent file for hash {hash_value_str}: {resolved_path}[/green]")
                    if not collect_all:
                        return resolved_path
                    if resolved_path not in candidates:
                        candidates.append(resolved_path)
                    if hash_key == "source_torrent_hash":
                        return candidates
                else:
                    logger.info(f"[bold red]Torrent file for hash {hash_value_str} failed validation: {torrent_path}")

        # Search the client if no pre-specified hash matches
        if torrent_client == "qbit" and client.get("enable_search"):
            qbt_session: httpx.AsyncClient | None = None
            try:
                proxy_url = client.get("qui_proxy_url")

                if proxy_url:
                    ssl_context = self.create_ssl_context_for_client(client)
                    qbt_session = httpx.AsyncClient(timeout=10.0, verify=ssl_context)
                else:
                    qbt_client = await self.init_qbittorrent_client(client)

                found = await self.search_qbit_for_torrent(meta, client, qbt_client, qbt_session, proxy_url, collect_all=collect_all)

                # Clean up session if we created one
                if qbt_session:
                    await qbt_session.aclose()

            except KeyboardInterrupt:
                logger.info("[bold red]Search cancelled by user")
                found = [] if collect_all else None
                if qbt_session:
                    await qbt_session.aclose()
            except TimeoutError:
                if qbt_session:
                    await qbt_session.aclose()
                raise
            except Exception as e:
                logger.info(f"[bold red]Error searching qBittorrent: {e}")
                found = [] if collect_all else None
                if qbt_session:
                    await qbt_session.aclose()
            found_hashes = found if isinstance(found, list) else [found] if isinstance(found, str) else []
            for found_hash in found_hashes:
                extracted_torrent_dir = Path(meta.base_dir) / "tmp" / meta.uuid / "torrents" / ".client"

                if torrent_storage_dir:
                    found_torrent_path = Path(torrent_storage_dir) / f"{found_hash}.torrent"
                else:
                    found_torrent_path = Path(extracted_torrent_dir) / f"{found_hash}.torrent"

                    if not Path(found_torrent_path).exists():
                        logger.info(f"[yellow]Exporting .torrent file from qBittorrent for hash: {found_hash}[/yellow]")

                        torrent_file_content: bytes | None = None

                        try:
                            proxy_url = client.get("qui_proxy_url")
                            if proxy_url:
                                qbt_proxy_url = proxy_url.rstrip("/")
                                async with httpx.AsyncClient() as session:
                                    try:
                                        response = await session.post(f"{qbt_proxy_url}/api/v2/torrents/export", data={"hash": found_hash})
                                        if response.status_code == 200:
                                            torrent_file_content = response.content
                                        else:
                                            logger.error(f"[red]Failed to export torrent via proxy: {response.status_code}")
                                            found_hash = None
                                    except Exception as e:
                                        logger.error(f"[red]Error exporting torrent via proxy: {e}")
                                        found_hash = None
                            else:
                                # Reuse or create qbt_client if needed
                                if qbt_client is None:
                                    qbt_client = await self.init_qbittorrent_client(client)
                                    if qbt_client is None:
                                        logger.info("[bold red]Failed to connect to qBittorrent for export.")
                                        found_hash = None

                                if found_hash and qbt_client is not None:  # Only proceed if we still have a hash
                                    active_qbt = qbt_client
                                    try:
                                        torrent_file_content = await self.retry_qbt_operation(
                                            lambda qbt_client=active_qbt, found_hash=found_hash: asyncio.to_thread(qbt_client.torrents_export, torrent_hash=found_hash),
                                            f"Export torrent {found_hash}",
                                        )
                                    except (TimeoutError, qbittorrentapi.APIError) as e:
                                        logger.error(f"[red]Error exporting torrent: {e}")

                            if found_hash:  # Only proceed if export succeeded
                                if not torrent_file_content:
                                    found_hash = None
                                else:
                                    Path(extracted_torrent_dir).mkdir(parents=True, exist_ok=True)
                                    await asyncio.to_thread(Path(found_torrent_path).write_bytes, torrent_file_content)
                                    logger.info(f"[green]Successfully saved .torrent file: {found_torrent_path}")
                        except Exception as e:
                            logger.error(f"[bold red]Unexpected error fetching .torrent from qBittorrent: {e}")
                            logger.info("[cyan]DEBUG: Skipping found_hash due to unexpected error[/cyan]")
                            found_hash = None
                    else:
                        logger.debug(f"[cyan]DEBUG: .torrent file already exists at {found_torrent_path}[/cyan]")

                # Only validate if we still have a hash (export succeeded or file already existed)
                resolved_path = ""
                if found_hash:
                    valid, resolved_path = await self.is_valid_torrent(meta, found_torrent_path, found_hash, torrent_client, client)
                else:
                    valid = False
                    logger.info("[cyan]DEBUG: Skipping validation because found_hash is None[/cyan]")

                if valid:
                    if collect_all:
                        if resolved_path not in candidates:
                            candidates.append(resolved_path)
                        continue
                    torrent = Torrent.read(resolved_path)
                    piece_size = torrent.piece_size
                    piece_in_mib = piece_size / 1024 / 1024

                    if not prefer_small_pieces:
                        logger.debug(f"[green]Found a valid torrent from client search with piece size {piece_in_mib} MiB: [bold yellow]{found_hash}")
                        return resolved_path

                    # Track best match for small pieces
                    if piece_size < 16777216 and piece_limit:  # 16 MiB
                        logger.info(f"[green]Found a valid torrent with piece size under 16 MiB from client search: [bold yellow]{found_hash}")
                        return resolved_path

                    if best_match is None or piece_size < best_match["piece_size"]:
                        best_match = {"torrenthash": found_hash, "torrent_path": resolved_path, "piece_size": piece_size}
                        logger.info(f"[yellow]Storing valid torrent from client search as best match: [bold yellow]{found_hash}")

        if collect_all:
            return candidates
        return best_match

    async def is_valid_torrent(self, meta: Meta, torrent_path: str, torrenthash: str, torrent_client: str, _client: dict[str, Any]) -> tuple[bool, str]:
        """Validate a candidate torrent against files, layout, and piece limits."""
        torrent_path = str(torrent_path)
        valid = False
        filelist = cast(list[str], meta.filelist)
        meta_path = meta.path
        if meta_path is None:
            return False, torrent_path

        # Normalize the torrent hash based on the client
        if torrent_client in ("qbit", "deluge"):
            torrenthash = torrenthash.lower().strip()
            torrent_path = torrent_path.replace(torrenthash.upper(), torrenthash)
        elif torrent_client == "rtorrent":
            torrenthash = torrenthash.upper().strip()
            torrent_path = torrent_path.replace(torrenthash.upper(), torrenthash)

        if meta.debug:
            logger.debug(f"Torrent path after normalization: {torrent_path}")

        # Check if torrent file exists
        if not Path(torrent_path).exists():
            logger.info(f"[bold yellow]{torrent_path} was not found")
            return False, torrent_path
        try:
            torrent = Torrent.read(torrent_path)
        except Exception as e:
            logger.info(f"[bold red]Error reading torrent file: {e}")
            return False, torrent_path

        keep_root = bool(meta.is_disc or (meta.keep_folder and meta.isdir))
        root = Path(meta_path) if meta.isdir or meta.is_disc else Path(meta_path).parent
        torrent_name = torrent.metainfo["info"]["name"]
        if meta.is_disc:
            filelist = [str(path) for path in Torrent(path=root, exclude_globs=DISC_EXCLUDE_GLOBS).filepaths]
            candidates = [filelist, [file for file in filelist if Path(file).suffix.casefold() not in SUBTITLE_EXTENSIONS]]
        else:
            candidates = [filelist]
            if meta.subtitle_files:
                candidates.append(filelist + meta.subtitle_files)

        for cand in candidates:
            if not cand or len(torrent.files) != len(cand):
                continue
            if len(cand) == 1 and not keep_root:
                valid = str(torrent.files[0]) == Path(cand[0]).name
            elif "files" in torrent.metainfo["info"]:
                try:
                    torrent_layout = sorted(Path(file).relative_to(torrent_name).as_posix() for file in torrent.files)
                    candidate_layout = sorted(Path(file).relative_to(root).as_posix() for file in cand)
                except ValueError:
                    continue
                valid = torrent_layout == candidate_layout and (not keep_root or root.name == torrent_name)
            if valid:
                break

        # Additional checks if the torrent is valid so far
        if valid:
            try:
                piece_size = torrent.piece_size
                piece_in_mib = piece_size / 1024 / 1024
                torrent_file_size = Path(torrent_path).stat().st_size
                torrent_file_size_kib = round(torrent_file_size / 1024, 2)
                logger.debug(f"Checking piece size, count and size: pieces={torrent.pieces}, piece_size={piece_in_mib} MiB, .torrent size={torrent_file_size_kib} KiB")

                # Centralized piece-count, piece-size and metainfo-size validation.
                stats = TorrentStats(piece_size, torrent.pieces, torrent.size, torrent_file_size)
                if "HDBITS" in meta.trackers:
                    valid = HDBITS_POLICY.accepts(stats)
                    if not valid:
                        logger.debug("[bold red]Torrent does not meet HDBits piece limits or file requirements")
                    trackers = meta.trackers.split(",") if isinstance(meta.trackers, str) else meta.trackers
                    if not valid or all(tracker == "HDBITS" for tracker in trackers):
                        return valid, torrent_path
                if not generic_reuse_allowed(stats, meta):
                    logger.debug("[bold red]Torrent does not meet the generic reuse policy")
                    valid = False
                else:
                    logger.debug(f"[bold green]REUSING .torrent with infohash: [bold yellow]{torrenthash}")
            except Exception as e:
                logger.info(f"[bold red]Error checking reuse torrent: {e}")
                valid = False

            if meta.debug:
                logger.debug(f"Final validity after piece checks: valid={valid}")
        else:
            if meta.debug:
                logger.debug("[bold yellow]Unwanted Files/Folders Identified")

        return valid, torrent_path

    @staticmethod
    def _torrent_includes_all_local_subtitles(torrent_path: str, meta: Meta) -> bool:
        """Whether a validated torrent includes every subtitle selected locally."""
        if not meta.subtitle_files:
            return True
        try:
            torrent = Torrent.read(torrent_path)
        except Exception:
            return False

        torrent_names = {Path(str(path)).name.casefold() for path in torrent.files}
        subtitle_names = {Path(str(path)).name.casefold() for path in meta.subtitle_files}
        return subtitle_names.issubset(torrent_names)

    @staticmethod
    def _torrent_has_no_subtitles(torrent_path: str) -> bool:
        """Whether a torrent contains no external subtitle files."""
        try:
            torrent = Torrent.read(torrent_path)
        except Exception:
            return False
        return not any(Path(str(path)).suffix.casefold() in SUBTITLE_EXTENSIONS for path in torrent.files)

    @staticmethod
    def _is_preferred_piece_size_candidate(candidate_path: str, current_path: str, piece_limit: bool) -> bool:
        """Whether a candidate outranks the current fallback by configured piece preference."""
        try:
            candidate_piece_size = Torrent.read(candidate_path).piece_size
            current_piece_size = Torrent.read(current_path).piece_size
        except Exception:
            return False

        if piece_limit:
            limit = 16 * 1024 * 1024
            candidate_within_limit = candidate_piece_size <= limit
            current_within_limit = current_piece_size <= limit
            return candidate_within_limit and (not current_within_limit or candidate_piece_size < current_piece_size)
        return False

    async def remote_path_map(self, meta: Meta, torrent_client_name: str | dict[str, Any] | None = None) -> tuple[str, str]:
        """Return the local and remote roots matching the torrent metadata path."""
        if isinstance(torrent_client_name, dict):
            client_config: dict[str, Any] = torrent_client_name
        elif isinstance(torrent_client_name, str) and torrent_client_name:
            try:
                client_config = cast(dict[str, Any], self.config["TORRENT_CLIENTS"][torrent_client_name])
            except KeyError as exc:
                raise KeyError(f"Torrent client '{torrent_client_name}' not found in TORRENT_CLIENTS") from exc
        else:
            raise ValueError("torrent_client_name must be a client name or client config dict")

        local_paths = coerce_str_list(client_config.get("local_path", ["/LocalPath"]))
        remote_paths = coerce_str_list(client_config.get("remote_path", ["/RemotePath"]))
        if not local_paths:
            local_paths = ["/LocalPath"]
        if not remote_paths:
            remote_paths = ["/RemotePath"]

        list_local_path = local_paths[0]
        list_remote_path = remote_paths[0]
        meta_path = str(meta.path)

        for i, local_path_value in enumerate(local_paths):
            if is_path_under(meta_path, local_path_value):
                list_local_path = local_path_value
                list_remote_path = remote_paths[i] if i < len(remote_paths) else remote_paths[0]
                break

        local_path = os.path.normpath(list_local_path)
        remote_path = os.path.normpath(list_remote_path)
        if local_path.endswith(os.sep):
            remote_path = remote_path + os.sep

        return local_path, remote_path

    async def resolve_qbittorrent_hash_path(self, meta: Meta, infohash: str) -> str | None:
        """Resolve a qBittorrent hash to its locally accessible content path."""
        lookup_started = time.monotonic()

        def report_lookup(stage: str) -> None:
            if os.environ.get("UA_REPORT_UPLOAD_SUCCESS") == "1":
                elapsed = time.monotonic() - lookup_started
                print(f"[Hash lookup pid={os.getpid()} hash={infohash} +{elapsed:.1f}s] {stage}", flush=True)

        default_config = cast(dict[str, Any], self.config.get("DEFAULT", {}))
        clients_config = cast(dict[str, Any], self.config.get("TORRENT_CLIENTS", {}))
        requested_client = meta.client
        if isinstance(requested_client, str) and requested_client and requested_client != "none":
            client_names = [requested_client]
        else:
            default_client = default_config.get("default_torrent_client")
            client_names = [default_client] if isinstance(default_client, str) and default_client and default_client != "none" else []

        for client_name in client_names:
            client_value = clients_config.get(client_name)
            if not isinstance(client_value, dict):
                continue
            client_value = cast(dict[str, Any], client_value)
            if str(client_value.get("torrent_client", "")).lower() != "qbit":
                continue

            proxy_url = client_value.get("qui_proxy_url")
            if proxy_url:
                report_lookup("Requesting torrent information via proxy")
                try:
                    ssl_context = self.create_ssl_context_for_client(client_value)
                    async with httpx.AsyncClient(timeout=10.0, verify=ssl_context) as session:
                        response = await session.get(
                            f"{str(proxy_url).rstrip('/')}/api/v2/torrents/info",
                            params={"hashes": infohash},
                        )
                        if response.status_code != 200:
                            logger.info(f"[bold red]qBittorrent hash lookup failed via proxy: HTTP {response.status_code}")
                            continue
                        torrents = response.json()
                except Exception as exc:
                    logger.info(f"[bold red]qBittorrent hash lookup failed via proxy for client '{client_name}': {exc}")
                    continue
            else:
                report_lookup("Connecting and authenticating to qBittorrent")
                qbt_client = await self.init_qbittorrent_client(client_value)
                if qbt_client is None:
                    continue
                report_lookup("Requesting torrent information")
                try:
                    torrents = await self.retry_qbt_operation(
                        lambda qbt_client=qbt_client: asyncio.to_thread(qbt_client.torrents_info, torrent_hashes=infohash),
                        f"Get qBittorrent torrent {infohash}",
                    )
                except Exception as exc:
                    logger.info(f"[bold red]qBittorrent hash lookup failed for client '{client_name}': {exc}")
                    continue

            report_lookup("Torrent information received")
            if not isinstance(torrents, Sequence):
                continue
            for torrent in cast(Sequence[Any], torrents):
                torrent_data = cast(dict[str, Any], torrent) if isinstance(torrent, dict) else None
                torrent_hash = torrent_data.get("hash", "") if torrent_data is not None else getattr(torrent, "hash", "")
                if str(torrent_hash).lower() != infohash.lower():
                    continue

                report_lookup("Matching torrent found; reading comments and mapping content path")
                content_path = torrent_data.get("content_path", "") if torrent_data is not None else getattr(torrent, "content_path", "")
                if not content_path:
                    save_path = torrent_data.get("save_path", "") if torrent_data is not None else getattr(torrent, "save_path", "")
                    torrent_name = torrent_data.get("name", "") if torrent_data is not None else getattr(torrent, "name", "")
                    content_path = str(Path(str(save_path)) / str(torrent_name))
                if not content_path:
                    continue

                comment = torrent_data.get("comment") if torrent_data is not None else getattr(torrent, "comment", None)
                comment_tracker_ids: dict[str, str] = {}
                if isinstance(comment, str):
                    comment_tracker_ids = self._extract_tracker_ids_from_comment(comment)
                    torrent_comments = meta.torrent_comments
                    if not isinstance(torrent_comments, list):
                        torrent_comments = []
                        meta.torrent_comments = torrent_comments
                    has_cached_comment = False
                    for item in cast(list[Any], torrent_comments):
                        if isinstance(item, dict) and str(cast(dict[str, Any], item).get("hash", "")).lower() == infohash.lower():
                            has_cached_comment = True
                            break
                    if not has_cached_comment:
                        torrent_name = torrent_data.get("name", "") if torrent_data is not None else getattr(torrent, "name", "")
                        torrent_comments.append({"hash": infohash, "name": str(torrent_name or ""), "comment": comment})

                local_paths = coerce_str_list(client_value.get("local_path", ["/LocalPath"])) or ["/LocalPath"]
                remote_paths = coerce_str_list(client_value.get("remote_path", ["/RemotePath"])) or ["/RemotePath"]
                local_path = map_remote_to_local_path(str(content_path), local_paths, remote_paths)
                resolved_path = str(Path(local_path).expanduser().resolve())
                prefetch_task: asyncio.Task[Meta] | None = None
                if not self._qbit_torrent_is_complete(torrent):
                    report_lookup("Torrent incomplete; starting metadata and screenshot preparation")
                    meta.source_torrent_hash = infohash
                    torrent_name_value = torrent_data.get("name") if torrent_data is not None else getattr(torrent, "name", None)
                    if isinstance(torrent_name_value, str) and torrent_name_value:
                        prefetch_task = self._start_hash_metadata_prefetch(meta, torrent_name_value, str(content_path), comment_tracker_ids)
                    try:
                        from src.pre_upload_screenshots import prepare_tracker_screenshots
                        from src.trackers.GAZELLE.passthepopcorn import PassThePopcorn

                        PassThePopcorn.start_auth_prefetch(meta, self.config)
                        screenshot_meta = meta.copy()
                        screenshot_meta.uuid = Path(resolved_path).name
                        await prepare_tracker_screenshots(screenshot_meta, self.config, comment_tracker_ids)
                        meta.image_list = screenshot_meta.image_list
                        meta.image_sizes = screenshot_meta.image_sizes
                        meta.imghost = screenshot_meta.imghost
                        report_lookup("Screenshot preparation finished; waiting for download completion")
                        await self._wait_for_qbittorrent_download(client_value, infohash)
                        if prefetch_task is not None:
                            key = (str(meta.base_dir), infohash.lower())
                            self._hash_metadata_prefetch_tasks[key] = (prefetch_task, meta.copy(), str(torrent_name_value), dict(comment_tracker_ids))
                    except BaseException:
                        await PassThePopcorn.cancel_auth_prefetch(meta)
                        if prefetch_task is not None and not prefetch_task.done():
                            prefetch_task.cancel()
                            await asyncio.gather(prefetch_task, return_exceptions=True)
                        raise
                report_lookup("Checking local content path exists")
                if not Path(resolved_path).exists():
                    from src.trackers.GAZELLE.passthepopcorn import PassThePopcorn

                    await PassThePopcorn.cancel_auth_prefetch(meta)
                    if prefetch_task is not None:
                        prefetch_task.cancel()
                        await asyncio.gather(prefetch_task, return_exceptions=True)
                        self._hash_metadata_prefetch_tasks.pop((str(meta.base_dir), infohash.lower()), None)
                    raise ValueError(f"Resolved local content path does not exist: {resolved_path}")
                meta.source_torrent_hash = infohash
                meta.source_torrent_client = client_name
                report_lookup("Hash resolution finished")
                logger.info(f"[green]Resolved torrent hash {infohash} to {resolved_path} using client '{client_name}'")
                return resolved_path

        logger.info(f"[bold red]Torrent hash {infohash} was not found in the configured qBittorrent clients: {', '.join(client_names) or 'none'}")
        return None

    async def finish_hash_metadata_prefetch(self, meta: Meta) -> None:
        key = (str(meta.base_dir), str(meta.source_torrent_hash or "").lower())
        pending = self._hash_metadata_prefetch_tasks.pop(key, None)
        if pending is None:
            return
        task, original_meta, search_term, tracker_ids = pending
        try:
            prefetched_meta = await task
        except Exception as error:
            logger.debug(f"[yellow]Early metadata lookup failed: {error}[/yellow]")
            return
        if meta.edit or prefetched_meta.tracker_prefetch_category != meta.category:
            return
        identity_fields = ("imdb_id", "tmdb_id", "imdb_manual", "tmdb_manual", "manual_category", "no_imdb", "no_tmdb", "tracker_ids")

        def equivalent(name: str, first: Any, second: Any) -> bool:
            if name in {"imdb_id", "tmdb_id", "imdb_manual", "tmdb_manual"}:
                return str(first or "0").removeprefix("tt").lstrip("0") == str(second or "0").removeprefix("tt").lstrip("0")
            if name in {"no_imdb", "no_tmdb"}:
                return bool(first) == bool(second)
            return bool(first == second)

        if any(not equivalent(name, meta.get(name), original_meta.get(name)) and not equivalent(name, meta.get(name), prefetched_meta.get(name)) for name in identity_fields):
            return
        if prefetched_meta.tracker_search_attempted or prefetched_meta.imdb_tmdb_prefetch_completed:
            for name, value in prefetched_meta.items():
                if value != original_meta.get(name) and equivalent(name, meta.get(name), original_meta.get(name)):
                    meta[name] = value
        if prefetched_meta.tracker_search_attempted:
            meta.tracker_prefetch_completed = True
            meta.tracker_prefetch_search_term = search_term
            meta.tracker_prefetch_category = prefetched_meta.tracker_prefetch_category
            meta.tracker_prefetch_ids = tracker_ids

    @classmethod
    async def cancel_hash_metadata_prefetch(cls, meta: Meta) -> None:
        key = (str(meta.base_dir), str(meta.source_torrent_hash or "").lower())
        pending = cls._hash_metadata_prefetch_tasks.pop(key, None)
        if pending is not None:
            task = pending[0]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _start_hash_metadata_prefetch(
        self,
        meta: Meta,
        torrent_name: str,
        content_path: str,
        comment_tracker_ids: dict[str, str] | None = None,
    ) -> asyncio.Task[Meta] | None:
        """Start ID-only tracker and IMDb/TMDb searches during an unattended hash download."""
        default_config = cast(dict[str, Any], self.config.get("DEFAULT", {}))
        description_mode = str(default_config.get("tracker_description_mode", "text")).strip().lower()
        if not meta.unattended:
            return None

        category = str(meta.manual_category or meta.category or "").strip().upper()
        if not category or category == "SPORTS":
            category_source = f"{torrent_name} {meta.uuid}"
            category = "TV" if re.search(r"(?i)(?:s\d{1,2}e\d{1,2}|\b\d{1,2}x\d{2}\b|season\s*\d+|series\s*\d+)", category_source) else "MOVIE"
        if category not in {"MOVIE", "TV", "XXX"}:
            return None

        search_file_folder = "file" if Path(content_path).suffix else "folder"
        prefetched_meta = meta.copy()
        prefetched_meta.category = category
        prefetched_meta.unattended = True
        prefetched_meta.tracker_description_mode = "ids"
        prefetched_meta.keep_images = False
        prefetched_meta.skip_tracker_descriptions = True
        prefetched_meta.tracker_prefetch_category = category
        if comment_tracker_ids:
            prefetched_meta.set_tracker_ids({**comment_tracker_ids, **prefetched_meta.tracker_ids})

        async def lookup() -> Meta:
            if meta.only_id or description_mode == "ids":
                from src.get_tracker_data import TrackerDataManager

                try:
                    await TrackerDataManager(self.config).get_tracker_data(
                        None,
                        prefetched_meta,
                        torrent_name,
                        search_file_folder,
                        category,
                        skip_tracker_descriptions=True,
                        id_only_prefetch=True,
                    )
                except Exception as error:
                    logger.debug(f"[yellow]Early tracker metadata lookup failed: {error}[/yellow]")

            if category not in {"MOVIE", "TV"} or prefetched_meta.tmdb_id or prefetched_meta.imdb_id or (meta.no_tmdb and meta.no_imdb):
                return prefetched_meta

            from src.get_name import NameManager
            from src.imdb import imdb_manager
            from src.tmdb import TmdbManager

            parsing_meta = prefetched_meta.copy()
            parsing_meta.uuid = torrent_name
            title, secondary_title, extracted_year = await NameManager(self.config).extract_title_and_year(parsing_meta, torrent_name)
            if not title:
                return prefetched_meta

            year = meta.manual_year or (meta.search_year or extracted_year or meta.year if category == "TV" else extracted_year or meta.year or meta.search_year)
            year_match = re.search(r"\b(?:18|19|20)\d{2}\b", str(year or ""))
            search_year = year_match.group(0) if year_match else None
            async def search_tmdb() -> tuple[bool, tuple[int, str] | None]:
                try:
                    tmdb_manager = TmdbManager(self.config)
                    result = await tmdb_manager.get_tmdb_id(
                        title,
                        search_year,
                        category,
                        torrent_name,
                        debug=bool(meta.debug),
                        secondary_title=secondary_title,
                        unattended=True,
                    )
                    return True, result
                except Exception as error:
                    logger.debug(f"[yellow]Early TMDb lookup failed: {error}[/yellow]")
                    return False, None

            async def search_imdb() -> tuple[bool, int | None]:
                try:
                    result = await imdb_manager.search_imdb(
                        title,
                        search_year,
                        quickie=True,
                        category=category,
                        secondary_title=secondary_title,
                        untouched_filename=torrent_name,
                        unattended=True,
                    )
                    return True, result
                except Exception as error:
                    logger.debug(f"[yellow]Early IMDb lookup failed: {error}[/yellow]")
                    return False, None

            tmdb_task: asyncio.Task[tuple[bool, tuple[int, str] | None]] | None = None
            imdb_task: asyncio.Task[tuple[bool, int | None]] | None = None
            if not meta.no_tmdb:
                tmdb_task = asyncio.create_task(search_tmdb())
            if not meta.no_imdb:
                imdb_task = asyncio.create_task(search_imdb())

            tmdb_result: tuple[int, str] | None = None
            imdb_result = 0
            query_succeeded = False
            if tmdb_task is not None and imdb_task is not None:
                (tmdb_succeeded, tmdb_result), (imdb_succeeded, imdb_result_value) = await asyncio.gather(tmdb_task, imdb_task)
                query_succeeded = tmdb_succeeded or imdb_succeeded
                imdb_result = imdb_result_value or 0
            elif tmdb_task is not None:
                tmdb_succeeded, tmdb_result = await tmdb_task
                query_succeeded = tmdb_succeeded
            elif imdb_task is not None:
                imdb_succeeded, imdb_result_value = await imdb_task
                query_succeeded = imdb_succeeded
                imdb_result = imdb_result_value or 0

            if tmdb_result is not None:
                tmdb_id, result_category = tmdb_result
                if result_category != category:
                    return prefetched_meta
                if not prefetched_meta.tmdb_id:
                    prefetched_meta.tmdb_id = tmdb_id
            if imdb_result and not prefetched_meta.imdb_id:
                prefetched_meta.imdb_id = imdb_result

            if not query_succeeded:
                return prefetched_meta
            prefetched_meta.quickie_search = True
            prefetched_meta.no_ids = True
            prefetched_meta.imdb_tmdb_prefetch_completed = True
            prefetched_meta.imdb_tmdb_prefetch_title = title
            prefetched_meta.imdb_tmdb_prefetch_year = search_year or ""
            prefetched_meta.imdb_tmdb_prefetch_category = category
            prefetched_meta.imdb_tmdb_prefetch_secondary_title = secondary_title or ""
            prefetched_meta.imdb_tmdb_prefetch_untouched_filename = torrent_name
            return prefetched_meta

        async def prefetch() -> Meta:
            prefetched_meta = await lookup()
            from src.trackers.GAZELLE.passthepopcorn import PassThePopcorn

            PassThePopcorn.start_group_prefetch(prefetched_meta, self.config)
            return prefetched_meta

        return asyncio.create_task(prefetch())

    async def get_ptp_from_hash(self, meta: Meta, pathed: bool = False, client_name: str | None = None) -> Meta:
        """Fetch PTP metadata through the configured torrent client when available."""
        default_config = self.config.get("DEFAULT", {})
        clients_config = self.config.get("TORRENT_CLIENTS", {})
        default_torrent_client = client_name or (default_config.get("default_torrent_client") if isinstance(default_config, dict) else None)
        if not isinstance(default_torrent_client, str) or not default_torrent_client:
            logger.debug("[yellow]Skipping torrent metadata lookup: no default torrent client configured.[/yellow]")
            return meta

        client = clients_config.get(default_torrent_client) if isinstance(clients_config, dict) else None
        if not isinstance(client, dict):
            logger.debug(f"[yellow]Skipping torrent metadata lookup: client '{default_torrent_client}' is not configured.[/yellow]")
            return meta

        torrent_client = client.get("torrent_client")
        if torrent_client == "rtorrent":
            await self.get_ptp_from_hash_rtorrent(meta, pathed, client)
            return meta
        if torrent_client == "qbit":
            return await self.get_ptp_from_hash_qbit(meta, client, pathed)
        return meta
