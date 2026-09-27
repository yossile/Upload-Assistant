import asyncio

import httpx

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


def test_convert_bbcode_removes_unsupported_font_tags() -> None:
    tracker = object.__new__(PassThePopcorn)

    converted = tracker.convert_bbcode("Before [font=Courier New]formatted text[/font] after")

    assert converted == "Before formatted text after"  # noqa: S101


def test_convert_bbcode_clamps_unsupported_size_tags() -> None:
    tracker = object.__new__(PassThePopcorn)

    converted = tracker.convert_bbcode("[size=4]Normal[/size] [size=16]Oversized[/size]")

    assert converted == "[size=4]Normal[/size] [size=10]Oversized[/size]"  # noqa: S101


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
