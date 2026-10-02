import pytest

from src.args import Args
from src.meta import Meta
from src.prep_helpers import _apply_imdb_metadata_fallback, _clear_imdb_metadata


def test_no_imdb_argument_enables_opt_out(tmp_path):
    meta, _, _ = Args({"DEFAULT": {"screens": 4}}).parse([str(tmp_path), "--no-imdb"], Meta())

    assert meta.no_imdb is True  # noqa: S101


def test_no_tmdb_argument_enables_opt_out(tmp_path):
    meta, _, _ = Args({"DEFAULT": {"screens": 4}}).parse([str(tmp_path), "--no-tmdb"], Meta())

    assert meta.no_tmdb is True  # noqa: S101


def test_no_tmdb_cannot_be_combined_with_manual_tmdb(tmp_path):
    with pytest.raises(SystemExit):
        Args({"DEFAULT": {"screens": 4}}).parse([str(tmp_path), "--no-tmdb", "--tmdb", "movie/123"], Meta())


def test_clear_imdb_metadata_removes_stale_values():
    meta = Meta(imdb_manual="tt1234567", imdb_id=1234567, imdb_info={"title": "Example"}, imdb="1234567", imdb_tt="tt1234567", imdb_rating="8.0", automatic_imdb_rejected=True)

    _clear_imdb_metadata(meta)

    assert (meta.imdb_manual, meta.imdb_id, meta.imdb_info, meta.imdb, meta.imdb_tt, meta.imdb_rating) == (0, 0, {}, "0", "", "")  # noqa: S101
    assert meta.automatic_imdb_rejected is False  # noqa: S101


def test_imdb_metadata_fallback_populates_missing_movie_fields():
    meta = Meta(
        category="MOVIE",
        imdb_info={
            "title": "Full Figured Flings",
            "year": 2026,
            "plot": "An overview.",
            "genres": "Comedy, Romance",
            "cover": "https://example.test/poster.jpg",
        },
    )

    _apply_imdb_metadata_fallback(meta)

    assert (meta.title, meta.year, meta.overview, meta.genres, meta.artwork_url) == (  # noqa: S101
        "Full Figured Flings",
        2026,
        "An overview.",
        ["Comedy", "Romance"],
        "https://example.test/poster.jpg",
    )


def test_imdb_metadata_fallback_does_not_replace_existing_fields():
    meta = Meta(
        category="MOVIE",
        title="TMDb title",
        overview="TMDb overview",
        genres=["Drama"],
        imdb_info={"title": "IMDb title", "plot": "IMDb overview", "genres": "Comedy"},
    )

    _apply_imdb_metadata_fallback(meta)

    assert (meta.title, meta.overview, meta.genres) == ("TMDb title", "TMDb overview", ["Drama"])  # noqa: S101


def test_no_imdb_cannot_be_combined_with_manual_imdb(tmp_path):
    with pytest.raises(SystemExit):
        Args({"DEFAULT": {"screens": 4}}).parse([str(tmp_path), "--no-imdb", "--imdb", "tt1234567"], Meta())
