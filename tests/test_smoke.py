"""Smoke tests for memories-crawl."""

from __future__ import annotations

import subprocess
import sys


def test_all_pipeline_modules_importable() -> None:
    """All 10 archive pipeline modules import without error."""
    modules = [
        "memories_crawl.friesland",
        "memories_crawl.nationaalarchief",
        "memories_crawl.drentsarchief",
        "memories_crawl.bhic",
        "memories_crawl.overijssel",
        "memories_crawl.utrechtsarchief",
        "memories_crawl.limburg",
        "memories_crawl.noordholland",
        "memories_crawl.zeeland",
        "memories_crawl.gelderland",
    ]
    for mod_name in modules:
        __import__(mod_name)


def test_cli_dispatches_all_pipelines() -> None:
    """The PIPELINES dict contains all 10 archive entries."""
    from memories_crawl.cli import PIPELINES

    expected = {
        "friesland",
        "nationaalarchief",
        "drentsarchief",
        "bhic",
        "overijssel",
        "utrechtsarchief",
        "limburg",
        "noordholland",
        "zeeland",
        "gelderland",
    }
    assert set(PIPELINES) == expected


def test_cli_help_text() -> None:
    """CLI produces help output without error."""
    result = subprocess.run(
        [sys.executable, "-m", "memories_crawl", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "usage:" in result.stdout


def test_cli_accepts_all_archives() -> None:
    """CLI accepts each archive choice (help text only, no real run)."""
    for archive in ["friesland", "bhic", "all"]:
        result = subprocess.run(
            [sys.executable, "-m", "memories_crawl", archive, "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"'{archive}' failed: {result.stderr}"


def test_every_pipeline_main_accepts_out_dir() -> None:
    """--out-dir can be threaded down into each pipeline."""
    import importlib
    import inspect

    for archive in [
        "friesland",
        "nationaalarchief",
        "drentsarchief",
        "bhic",
        "overijssel",
        "utrechtsarchief",
        "limburg",
        "noordholland",
        "zeeland",
        "gelderland",
    ]:
        mod = importlib.import_module(f"memories_crawl.{archive}")
        assert "out_dir" in inspect.signature(mod.main).parameters, archive


def test_cli_exposes_out_dir() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "memories_crawl", "--help"],
        capture_output=True,
        text=True,
    )
    assert "--out-dir" in result.stdout
