from __future__ import annotations

import argparse
import asyncio
import copy
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

from src.app_paths import STATE_DIR, LegacyConfigLocationError, bundled_example_config_path, ensure_legacy_config_absent, ensure_user_config
from src.args import Args
from src.check_requirements import check_dependencies
from src.clients import Clients
from src.config_sync import ConfigSyncError, sync_user_config
from src.console import logger
from src.early_tasks import cancel_and_drain_early_artifact_tasks, get_early_artifact_tasks
from src.get_name import NameManager
from src.meta import Meta
from src.prep import Prep
from src.rehostimages import check_tracker_image_hosts
from src.trackers.GAZELLE.passthepopcorn import PassThePopcorn


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare and upload one movie torrent to PassThePopcorn.")
    parser.add_argument("path", type=Path, help="Movie file or release folder")
    parser.add_argument("--imdb", help="IMDb ID, with or without the tt prefix")
    parser.add_argument("--tmdb", help="TMDb movie ID")
    parser.add_argument("--new-group", action="store_true", help="Skip PTP group lookup; create a new group")
    parser.add_argument("--debug", action="store_true", help="Prepare the upload but do not submit it to PTP")
    return parser.parse_args(argv)


def _normalize_imdb_id(value: str) -> str:
    normalized = value.strip().lower()
    if normalized.startswith("tt"):
        normalized = normalized[2:]
    if not normalized.isdigit():
        raise ValueError("IMDb ID must be numeric, optionally prefixed with 'tt'")
    return normalized


def _normalize_tmdb_id(value: str) -> str:
    normalized = value.strip()
    if normalized.lower().startswith("movie/"):
        normalized = normalized.split("/", 1)[1]
    if not normalized.isdigit():
        raise ValueError("TMDb ID must be a numeric movie ID")
    return normalized


def _load_config() -> dict[str, Any]:
    ensure_legacy_config_absent()
    if ensure_user_config():
        raise RuntimeError(f"Created a new configuration at {STATE_DIR / 'data' / 'config.py'}; configure PTP settings and run again.")
    try:
        sync_user_config(STATE_DIR / "data" / "config.py", bundled_example_config_path())
    except (ConfigSyncError, OSError) as error:
        logger.warning(f"Configuration was not automatically updated: {error}")

    from data.config import config as user_config  # pyright: ignore[reportMissingImports,reportUnknownVariableType]

    return cast(dict[str, Any], user_config)


def _fast_prep_config(config: dict[str, Any]) -> dict[str, Any]:
    fast_config = copy.copy(config)
    default_config = copy.copy(config.get("DEFAULT", {}))
    default_config.update(
        {
            "tracker_comment_only": True,
            "tracker_description_mode": "text",
            "injecting_client_list": [],
            "skip_auto_torrent": False,
            "skip_auto_torrent_personalrelease": False,
            "use_radarr": False,
            "use_sonarr": False,
            "ping_unit3d": False,
        }
    )
    fast_config["DEFAULT"] = default_config
    return fast_config


async def _find_group(ptp: PassThePopcorn, meta: Meta, supplied_tmdb: bool) -> str | None:
    if supplied_tmdb and meta.tmdb_id:
        return await ptp.get_group_by_tmdb(meta.tmdb_id)
    if meta.imdb_id:
        return await ptp._get_group_by_identifier(
            {"imdb": meta.imdb_id, "json": "noredirect"},
            f"IMDb: tt{str(meta.imdb_id).zfill(7)}",
            allow_selection=False,
        )
    if meta.tmdb_id:
        return await ptp.get_group_by_tmdb(meta.tmdb_id)
    return None


async def _run(args: argparse.Namespace) -> int:
    check_dependencies()
    config = _load_config()
    if not args.path.exists():
        raise FileNotFoundError(f"Input path does not exist: {args.path}")
    if not config.get("TRACKERS", {}).get("PASSTHEPOPCORN"):
        raise RuntimeError("PASSTHEPOPCORN is not configured")

    imdb_id = _normalize_imdb_id(args.imdb) if args.imdb else None
    tmdb_id = _normalize_tmdb_id(args.tmdb) if args.tmdb else None

    ua_args = [str(args.path.resolve()), "--category", "movie", "--trackers", "PASSTHEPOPCORN"]
    if imdb_id:
        ua_args.extend(("--imdb", imdb_id))
    if tmdb_id:
        ua_args.extend(("--tmdb", tmdb_id))
    if args.debug:
        ua_args.append("--debug")

    meta, _parser, _unknown_args = Args(_fast_prep_config(config)).parse(ua_args, Meta())
    meta.base_dir = str(STATE_DIR)
    image_host = config["DEFAULT"].get("img_host_1")
    if not isinstance(image_host, str) or not image_host.strip():
        raise RuntimeError("DEFAULT.img_host_1 must name a configured image host for PTP screenshots")
    meta.imghost = image_host.strip().lower()
    meta.trackers = ["PASSTHEPOPCORN"]
    meta.category = "MOVIE"
    meta.manual_category = "MOVIE"
    try:
        meta.screens = max(4, int(config["DEFAULT"].get("screens", 4)))
    except (TypeError, ValueError):
        meta.screens = 4
    meta.unattended = True
    meta.unattended_confirm = False
    meta.skip_auto_torrent = False
    meta.skip_tracker_descriptions = True
    meta.keep_images = False
    meta.debug = args.debug
    if tmdb_id and not imdb_id:
        meta.no_imdb = True
    meta.tracker_status = {"PASSTHEPOPCORN": {}}

    ptp = PassThePopcorn(config=config)
    if not await ptp.get_additional_checks(meta):
        raise RuntimeError("PTP configuration checks failed; verify API credentials, username/password, and announce URL")

    prep_config = _fast_prep_config(config)
    prep = Prep(screens=meta.screens, img_host=meta.imghost, config=prep_config)
    try:
        await prep.gather_prep(meta, mode="cli")
        release_id = str(meta.uuid or args.path.name)
        early_tasks = get_early_artifact_tasks(release_id)
        if early_tasks:
            await asyncio.gather(*early_tasks)

        meta.name_notag, meta.name, meta.clean_name, meta.potential_missing = await NameManager(prep_config).get_name(meta)
        if not await ptp.get_additional_checks(meta):
            raise RuntimeError("PTP rejected the release group or required configuration")
        await check_tracker_image_hosts(meta, ptp)

        group_id = None if args.new_group else await _find_group(ptp, meta, supplied_tmdb=bool(tmdb_id))
        if group_id:
            dupes = await ptp.search_existing(group_id, meta)
            if dupes:
                logger.error("Possible PTP duplicates found; stopping without upload:")
                for dupe in dupes:
                    logger.error(f"  {dupe}")
                return 1
        elif not args.new_group:
            logger.info("No matching PTP group found; preparing a new group.")

        upload_url, form_data = await ptp.fill_upload_form(group_id, meta)
        if args.debug:
            logger.info("Debug mode: prepared PTP form and torrent; no upload will be submitted.")
        uploaded = await ptp.upload(meta, upload_url, form_data)
        tracker_status = cast(dict[str, Any], meta.tracker_status["PASSTHEPOPCORN"])
        if not uploaded:
            logger.error(tracker_status.get("status_message", "PTP upload failed"))
            return 1
        if args.debug:
            return 0

        await Clients(config=config).add_to_client(meta, "PASSTHEPOPCORN")
        logger.info(tracker_status.get("status_message", "PTP upload completed"))
        return 0
    finally:
        await PassThePopcorn.cancel_prefetch(meta)
        await cancel_and_drain_early_artifact_tasks(str(meta.uuid or args.path.name))


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
        return asyncio.run(_run(args))
    except (LegacyConfigLocationError, OSError, RuntimeError, ValueError) as error:
        logger.error(str(error))
        return 1
    except KeyboardInterrupt:
        logger.info("Upload cancelled.")
        return 130


if __name__ == "__main__":
    sys.exit(main())