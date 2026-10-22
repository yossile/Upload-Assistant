import asyncio

import pytest

from src.meta import Meta
from src import trackerstatus


class LookupReached(Exception):
    pass


@pytest.mark.parametrize("tmdb_id", [0, 12345])
@pytest.mark.parametrize("imdb_id", [0, None])
def test_douban_lookup_uses_imdb_id_from_attended_prompt(monkeypatch, tmdb_id, imdb_id):
    events = []

    class TrackerSetup:
        def __init__(self, config):
            self.config = config

        def filter_unsupported_trackers(self, meta):
            return meta

    class Router:
        def __init__(self, *_args, **_kwargs):
            pass

        async def apply(self, meta):
            return meta

    async def prompt(*_args, **_kwargs):
        events.append("prompt")
        return "tt1234567"

    async def imdb_info(*_args, **_kwargs):
        return {}

    async def douban_lookup(meta):
        events.append(("douban", meta.imdb_tt))
        raise LookupReached

    monkeypatch.setattr(trackerstatus, "TrackerSetup", TrackerSetup)
    monkeypatch.setattr(trackerstatus, "AvistaZNetworkRouter", Router)
    monkeypatch.setattr(trackerstatus, "UploadHelper", lambda _config: object())
    monkeypatch.setattr(trackerstatus, "DupeChecker", lambda _config: object())
    monkeypatch.setattr(trackerstatus, "prompt_in_thread", prompt)
    monkeypatch.setattr(trackerstatus.imdb_manager, "get_imdb_info_api", imdb_info)
    monkeypatch.setattr(trackerstatus, "get_douban_id", douban_lookup)
    meta = Meta(
        trackers=["PTERCLUB", "PASSTHEPOPCORN"],
        tracker_status={},
        unattended=False,
        no_imdb=False,
        imdb_id=imdb_id,
        tmdb_id=tmdb_id,
        title="Example" if tmdb_id else "",
        year=2026 if tmdb_id else None,
        imdb_tt="",
    )

    with pytest.raises(LookupReached):
        asyncio.run(trackerstatus.TrackerStatusManager({"TRACKERS": {}}).process_all_trackers(meta))

    expected_events = [("douban", "")] if tmdb_id else ["prompt", ("douban", "tt1234567")]
    assert events == expected_events  # noqa: S101
