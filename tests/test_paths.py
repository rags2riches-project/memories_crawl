"""Tests for output-root and cache-path resolution."""

from __future__ import annotations

import pytest

from memories_crawl import paths


@pytest.fixture(autouse=True)
def _reset_out_dir(monkeypatch):
    monkeypatch.delenv(paths.ENV_VAR, raising=False)
    paths.set_out_dir(None)
    yield
    paths.set_out_dir(None)


def test_default_out_dir_is_scans() -> None:
    assert paths.out_dir() == paths.DEFAULT_OUT_DIR
    assert paths.archive_dir("zeeland").as_posix() == "scans/zeeland"


def test_env_var_supplies_default(monkeypatch) -> None:
    monkeypatch.setenv(paths.ENV_VAR, "/data/mvs")
    paths.set_out_dir(None)
    assert paths.archive_dir("zeeland").as_posix() == "/data/mvs/zeeland"


def test_set_out_dir_moves_scans_and_caches(tmp_path) -> None:
    paths.set_out_dir(tmp_path / "raw")
    assert paths.archive_dir("gelderland") == tmp_path / "raw" / "gelderland"
    assert paths.cache_dir("gelderland") == tmp_path / "raw" / ".cache" / "gelderland"


def test_cache_file_is_separate_from_images(tmp_path) -> None:
    paths.set_out_dir(tmp_path)
    cache = paths.cache_file("gelderland", "tokens_0022.json")
    assert cache == tmp_path / ".cache" / "gelderland" / "tokens_0022.json"
    # The parent is created so callers can write straight away.
    assert cache.parent.is_dir()
    # …and it is not inside the directory the images go to.
    assert paths.archive_dir("gelderland") not in cache.parents


def test_cache_file_reuses_a_pre_existing_legacy_cache(tmp_path) -> None:
    """A token harvest paid for by an older version must not be thrown away."""
    paths.set_out_dir(tmp_path)
    legacy = tmp_path / "gelderland" / "tokens_0022.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("[]")

    assert paths.cache_file("gelderland", "tokens_0022.json") == legacy


def test_cache_file_prefers_the_new_location_when_both_exist(tmp_path) -> None:
    paths.set_out_dir(tmp_path)
    legacy = tmp_path / "gelderland" / "tokens_0022.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("[]")
    current = tmp_path / ".cache" / "gelderland" / "tokens_0022.json"
    current.parent.mkdir(parents=True)
    current.write_text("[]")

    assert paths.cache_file("gelderland", "tokens_0022.json") == current


def test_cache_file_honours_an_explicit_legacy_location(tmp_path, monkeypatch) -> None:
    """Progress CSVs used to live in the working directory, not under scans/."""
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(tmp_path / "scans")
    from pathlib import Path

    legacy = Path("bhic_progress.csv")
    legacy.write_text("register_id\n")

    assert paths.cache_file("bhic", "bhic_progress.csv", legacy=legacy) == legacy
