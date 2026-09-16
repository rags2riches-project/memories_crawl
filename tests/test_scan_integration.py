"""Scan selection composes with caches, office filters, workers and resume state."""

import csv
import json
from types import SimpleNamespace

import pytest
import requests

from memories_crawl import (
    bhic,
    cli,
    download,
    drentsarchief,
    friesland,
    nationaalarchief,
    paths,
    regcache,
)


@pytest.fixture(autouse=True)
def output_root(tmp_path):
    paths.set_out_dir(tmp_path)
    yield
    paths.set_out_dir(None)


@pytest.mark.parametrize("mod", [bhic, drentsarchief, friesland])
@pytest.mark.parametrize("workers", [1, 4])
def test_count_selection_preserves_full_cache_and_closes_downloader(
    mod, workers, monkeypatch, tmp_path
):
    registers = [
        {
            "id": str(i),
            "metadata": {
                "inventarisnummer": str(i),
                "gemeente": office,
                "naam": f"Memories kantoor {office}",
            },
            "asset": [{}],
        }
        for i, office in [(1, "Assen"), (2, "Sneek")]
    ]
    monkeypatch.setattr(mod, "_paginate", lambda *a: registers)
    if mod is drentsarchief:
        mod._collect_registers(None)
    else:
        mod._load_registers(None)
    counted = []

    def count(session, reg, *args):
        counted.append(reg["id"])
        return (3, 2) if mod is friesland else 2

    monkeypatch.setattr(mod, "_register_counts" if mod is friesland else "_count_scans", count)
    monkeypatch.setattr(mod, "_paginate", lambda *a: pytest.fail("warm listing re-fetched"))
    monkeypatch.setattr(mod, "_download_file", lambda *a: pytest.fail("listing downloaded images"))
    closed = []
    original = download.Downloader.close

    def close(self):
        closed.append(True)
        original(self)

    monkeypatch.setattr(download.Downloader, "close", close)
    out = tmp_path / "listing.csv"
    assert (
        mod.main(
            list_invnrs=True,
            count_scans=True,
            only_digitized=True,
            kantoren={"assen"},
            workers=workers,
            csv_out=str(out),
        )
        is None
    )
    assert counted == ["1"]
    assert closed == [True]
    with out.open() as stream:
        assert [r["invnr"] for r in csv.DictReader(stream)] == ["1"]
    cache = json.loads(paths.cache_file(mod.ARCHIVE, "registers.json").read_text())
    assert len(cache["items"]) == 2


@pytest.mark.parametrize("workers", [1, 4])
def test_friesland_partial_person_selection_can_be_completed(workers, monkeypatch, tmp_path):
    reg = {
        "id": "r",
        "asset": [{}],
        "metadata": {"inventarisnummer": "7", "naam": "Memories kantoor Sneek"},
    }
    deeds = [
        {"id": "d1", "asset": [{"download": "https://example.invalid/a.jp2"}]},
        {"id": "d2", "asset": []},
    ]
    persons = [
        {"id": f"p{i}", "deed_id": f"d{i}", "metadata": {"person_display_name": f"P{i}"}}
        for i in (1, 2)
    ]

    def paginate(session, path, fq, key):
        return {"/register": [reg], "/deed": deeds, "/person": persons}[path]

    def fetch(session, url, dest):
        if dest.exists():
            return "exists"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"scan")
        return "downloaded"

    monkeypatch.setattr(friesland, "_paginate", paginate)
    monkeypatch.setattr(friesland, "_download_file", fetch)
    monkeypatch.setattr(friesland.time, "sleep", lambda _: None)
    run = friesland.main(invnrs={"7"}, kantoren={"sneek"}, only_digitized=True, workers=workers)
    assert run.pages.bytes_written == 4
    assert run.records == 1
    assert "r" not in friesland._load_done()
    assert len(list((tmp_path / "friesland").rglob("metadata.json"))) == 1
    run = friesland.main(invnrs={"7"}, kantoren={"sneek"}, workers=workers)
    assert run.pages.bytes_written == 0
    assert run.pages.skipped == 1
    assert run.records == 2
    assert "r" in friesland._load_done()
    assert len(list((tmp_path / "friesland").rglob("metadata.json"))) == 2


def test_na_migrates_integer_cache_and_refreshes_entries(monkeypatch):
    na = nationaalarchief
    regcache.load_or_collect(
        na.ARCHIVE, na.INVENTORY_CACHE_NAME, lambda: [2276, 2277], key=na.EAD_XML_URL
    )
    entries = [{"invnr": 2276, "kantoor": "Alphen", "has_scans": True}]
    calls = []

    def collect(session):
        calls.append(True)
        return entries

    monkeypatch.setattr(na, "_collect_inventory_entries", collect)
    assert na._fetch_inventory_entries(None) == entries
    assert na._fetch_inventory_entries(None) == entries
    assert len(calls) == 1
    assert na._fetch_inventory_entries(None, refresh_cache=True) == entries
    assert len(calls) == 2


def test_na_fallback_is_unknown_and_never_cached(monkeypatch):
    na = nationaalarchief

    def fail(session):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(na, "_collect_inventory_entries", fail)
    entries = na._fetch_inventory_entries(None)
    assert all(e["has_scans"] is None for e in entries)
    assert not paths.cache_file(na.ARCHIVE, na.INVENTORY_CACHE_NAME).exists()


@pytest.mark.parametrize("payload", [{}, {"metadata": {"pagination": {"total": "bad"}}}])
@pytest.mark.parametrize("mod", [bhic, drentsarchief, friesland])
def test_invalid_count_is_unknown(mod, payload, monkeypatch):
    monkeypatch.setattr(mod, "_get_json", lambda *a: payload)
    assert mod._count(None, "/asset", "register_id:r") is None


@pytest.mark.parametrize(
    "html, expected",
    [
        ("<html>maintenance</html>", None),
        (
            '<script data-drupal-selector="drupal-settings-json">'
            '{"viewer":{"response":{"scans":[]}}}</script>',
            0,
        ),
    ],
)
def test_na_count_distinguishes_invalid_page_from_empty(html, expected):
    session = SimpleNamespace(
        get=lambda *a, **kw: SimpleNamespace(
            status_code=200, text=html, raise_for_status=lambda: None
        )
    )
    assert nationaalarchief._count_scans(session, 2276) == expected


@pytest.mark.parametrize("workers", [1, 4])
def test_na_listing_counts_only_selected_entries(monkeypatch, tmp_path, workers):
    na = nationaalarchief
    entries = [
        {"invnr": 2276, "kantoor": "Alphen", "has_scans": True},
        {"invnr": 2277, "kantoor": "Alphen", "has_scans": False},
        {"invnr": 2278, "kantoor": "Alphen", "has_scans": None},
    ]
    monkeypatch.setattr(na, "_fetch_inventory_entries", lambda *a, **kw: entries)
    counted = []

    def count(session, invnr):
        counted.append(invnr)
        return None

    monkeypatch.setattr(na, "_count_scans", count)
    monkeypatch.setattr(na, "_download_file", lambda *a: pytest.fail("listing downloaded images"))
    monkeypatch.setattr(na.time, "sleep", lambda _: None)
    out = tmp_path / "na.csv"
    assert (
        na.main(
            invnrs={"2277", "2278"},
            list_invnrs=True,
            count_scans=True,
            only_digitized=True,
            csv_out=str(out),
            workers=workers,
        )
        is None
    )
    assert counted == [2278]
    with out.open() as stream:
        assert list(csv.DictReader(stream)) == [
            {"invnr": "2278", "kantoor": "Alphen", "n_scans": "?"}
        ]


@pytest.mark.parametrize("archive", list(cli.PIPELINES))
def test_cli_forwards_combined_flags(archive, monkeypatch, tmp_path):
    import importlib
    import sys

    mod = importlib.import_module(f"memories_crawl.{archive}")
    captured = {}
    monkeypatch.setattr(mod, "main", lambda **kw: captured.update(kw))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "memories-crawl",
            archive,
            "--list-invnrs",
            "--only-digitized",
            "--count-scans",
            "--workers",
            "1",
            "--kantoor",
            "Sneek",
            "--invnr",
            "7",
            "--refresh-cache",
            "--out-dir",
            str(tmp_path),
        ],
    )
    cli.main()
    assert captured["only_digitized"] is captured["count_scans"] is True
    assert captured["workers"] == 1
    assert captured["invnrs"] == {"7"}
    if archive != "nationaalarchief":
        assert captured["kantoren"] == {"Sneek"}
    if archive in cli.CACHED_LISTING_PIPELINES:
        assert captured["refresh_cache"] is True
