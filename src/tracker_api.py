"""Share tracker detail responses between preparation tasks in one event loop."""

import asyncio
import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary

import httpx


@dataclass
class _TorrentResponse:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    response: httpx.Response | None = None
    expires_at: float = 0.0
    users: int = 0


_responses: WeakKeyDictionary[asyncio.AbstractEventLoop, OrderedDict[tuple[str, str], _TorrentResponse]] = WeakKeyDictionary()
_RESPONSE_TTL = 24 * 60 * 60
_MAX_RESPONSES = 128


async def fetch_unit3d_torrent(session: httpx.AsyncClient, url: str, api_key: str) -> httpx.Response:
    """Coalesce detail lookups and retain only successful, valid torrent responses."""
    api_key = api_key.strip()
    key = (url, hashlib.sha256(api_key.encode()).hexdigest())
    cache = _responses.setdefault(asyncio.get_running_loop(), OrderedDict())
    entry = cache.setdefault(key, _TorrentResponse())
    cache.move_to_end(key)
    entry.users += 1
    try:
        async with entry.lock:
            if entry.response is not None and entry.expires_at > time.monotonic():
                return entry.response
            entry.response = None
            response = await session.get(
                url=url,
                params={"api_token": api_key},
                headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            )
            if response.is_success:
                payload = response.json()
                torrent = payload.get("data", payload) if isinstance(payload, dict) else None
                if isinstance(torrent, dict) and isinstance(torrent.get("attributes"), dict) and torrent["attributes"]:
                    entry.response = response
                    entry.expires_at = time.monotonic() + _RESPONSE_TTL
            return response
    finally:
        entry.users -= 1
        if entry.users == 0 and entry.response is None:
            cache.pop(key, None)
        for cached_key, cached_entry in list(cache.items()):
            if len(cache) <= _MAX_RESPONSES:
                break
            if cached_entry.users == 0:
                cache.pop(cached_key)
