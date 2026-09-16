"""Tests for the run-scale reporting of issue #19.

The point of the summary is that the number it prints is the number the run
actually produced, so the cases that could quietly lie are the ones tested:
pages that were already on disk, pages the server would not hand over, a run
narrowed by ``--invnr``, a run that resumed and had nothing left to do, and a
run that died halfway.
"""

from __future__ import annotations

import sys

import pytest

from memories_crawl import cli, download, gelderland, noordholland, paths, zeeland
from memories_crawl.summary import (
    PageTally,
    RunSummary,
    collect,
    grand_total,
    human_bytes,
)

# Two kantoren; the first holds two inventarisnummers, so "aggregated across
# registers" and "aggregated across kantoren" are different assertions.
UNITS: dict[str, list[int]] = {"A": [4, 5], "B": [18]}
ALL_INVNRS = [i for v in UNITS.values() for i in v]
PAGE_BYTES = len(b"png")


# ---------------------------------------------------------------------------
# PageTally / RunSummary in isolation
# ---------------------------------------------------------------------------


def test_tally_keeps_the_three_outcomes_apart(tmp_path) -> None:
    dest = tmp_path / "0001.jpg"
    dest.write_bytes(b"0123456789")

    tally = PageTally()
    tally.record("downloaded", dest)
    tally.record("exists", dest)
    tally.record("missing", dest)
    tally.record("failed", dest)

    assert (tally.downloaded, tally.skipped, tally.missing) == (1, 1, 2)
    assert tally.total == 4
    # Only a page this run fetched counts towards the bytes it cost.
    assert tally.bytes_written == 10


def test_tally_ignores_a_file_that_is_not_there() -> None:
    tally = PageTally()
    tally.record("downloaded", None)
    assert tally.bytes_written == 0


def test_tallies_merge(tmp_path) -> None:
    """Issue #26 will download concurrently; per-thread tallies must add up."""
    total = PageTally()
    for _ in range(3):
        total += PageTally(downloaded=2, skipped=1, missing=1, bytes_written=1000)
    assert (total.downloaded, total.skipped, total.missing) == (6, 3, 3)
    assert total.bytes_written == 3000


def test_describe_reports_pages_and_bytes() -> None:
    tally = PageTally(downloaded=38, bytes_written=21_500_000)
    assert tally.describe() == "38 pages (38 new, 0 existing, 0 missing, 21.5 MB)"
    # A fully-resumed register has nothing to report in bytes.
    assert PageTally(skipped=38).describe() == "38 pages (0 new, 38 existing, 0 missing)"


def test_human_bytes_scales() -> None:
    assert human_bytes(0) == "0 B"
    assert human_bytes(950) == "950 B"
    assert human_bytes(93_000) == "93.0 KB"
    assert human_bytes(3_400_000) == "3.4 MB"
    assert human_bytes(1_020_000_000) == "1.0 GB"


def test_render_lists_every_figure() -> None:
    run = RunSummary("gelderland", "Gelderland", units=21, registers=49)
    run.pages += PageTally(downloaded=1823, skipped=4, missing=2, bytes_written=1_020_000_000)
    text = run.render()

    assert "Gelderland" in text
    assert "kantoren processed" in text and "21" in text
    assert "registers processed" in text and "49" in text
    assert "1,823" in text
    assert "pages already present" in text
    assert "pages missing" in text
    assert "1.0 GB" in text
    assert "INCOMPLETE" not in text


def test_render_hides_the_unit_row_for_a_flat_inventory() -> None:
    """The Nationaal Archief has no kantoor layer; do not invent one."""
    assert "kantoren" not in RunSummary("nationaalarchief", unit_name=None).render()


def test_render_flags_a_run_that_stopped() -> None:
    run = RunSummary("zeeland", "Zeeland", error="connection reset")
    assert "INCOMPLETE" in run.render()
    assert "connection reset" in run.render()


def test_grand_total_adds_up_and_names_the_broken_runs() -> None:
    a = RunSummary("alpha", "Alpha", units=2, registers=5)
    a.pages += PageTally(downloaded=10, skipped=1, bytes_written=2_500_000)
    b = RunSummary("beta", "Beta", units=1, registers=1, error="boom")
    b.pages += PageTally(downloaded=3, missing=4, bytes_written=500_000)

    text = grand_total([a, b])
    assert "2 archives" in text
    assert "13" in text  # 10 + 3 pages downloaded
    assert "3.0 MB" in text  # 2.5 MB + 0.5 MB
    assert "1 pages already present, 4 missing." in text
    assert "INCOMPLETE" in text and "Beta" in text


def test_grand_total_of_nothing() -> None:
    assert "nothing ran" in grand_total([])


def test_collect_captures_a_summary_from_a_call_that_raises() -> None:
    def pipeline() -> None:
        run = RunSummary("alpha", "Alpha")
        run.pages += PageTally(downloaded=2, bytes_written=100)
        raise RuntimeError("halfway")

    with collect() as collected:
        with pytest.raises(RuntimeError):
            pipeline()
    assert len(collected) == 1
    assert collected[0].pages.downloaded == 2


# ---------------------------------------------------------------------------
# End-to-end through the pipelines, with every network call stubbed out
# ---------------------------------------------------------------------------


def _pages(invnr: int) -> list[dict]:
    return [
        {
            "invnr": invnr,
            "page": 1,
            "inv_text": f"{invnr}  1818",
            "thumb_url": f"https://example.invalid/{invnr}_0001.jpg?format=thumb&miadt=1",
            "slug": "",
        }
    ]


def _items(invnrs: list[int]) -> list[dict]:
    return [{"invnr": i, "text": f"{i}  1818", "minr": 1000 + i, "hasScan": True} for i in invnrs]


class Harness:
    """Stands in for the archive: hands out pages, records what was asked for."""

    def __init__(self) -> None:
        self.status = "downloaded"
        self.fail_after: int | None = None
        self.downloads: list[str] = []


@pytest.fixture
def h(monkeypatch, tmp_path):
    """Replace every network/Playwright call with in-memory fakes."""
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)  # default ./scans, i.e. below tmp_path
    harness = Harness()

    def fake_download(session, url, dest):
        if harness.fail_after is not None and len(harness.downloads) >= harness.fail_after:
            raise RuntimeError("the archive hung up")
        harness.downloads.append(str(dest))
        if dest.exists() and dest.stat().st_size > 0:
            return "exists"  # the same short-circuit the real _download_file has
        if harness.status != "downloaded":
            return harness.status
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"png")
        return "downloaded"

    for mod in (gelderland, noordholland, zeeland):
        monkeypatch.setattr(mod, "_download_file", fake_download)
        monkeypatch.setattr(download.time, "sleep", lambda *_: None)

    def harvest(unit: str, items: list[dict] | None = None) -> list[dict]:
        wanted = [it["invnr"] for it in items] if items is not None else UNITS[unit]
        return [p for i in wanted for p in _pages(i)]

    monkeypatch.setattr(gelderland, "KANTOREN", {"Arnhem": "A", "Borculo": "B"})
    monkeypatch.setattr(gelderland, "_discover_invnrs", lambda kantoor, code: _items(UNITS[code]))
    monkeypatch.setattr(
        gelderland,
        "_harvest_page_tokens",
        lambda kantoor, code, items, write_cache=True: harvest(code, items),
    )

    monkeypatch.setattr(
        zeeland,
        "_discover_kantoren",
        lambda: [{"name": "Goes", "minr": "A"}, {"name": "Hulst", "minr": "B"}],
    )
    monkeypatch.setattr(zeeland, "_discover_invnrs", lambda minr: _items(UNITS[minr]))
    monkeypatch.setattr(
        zeeland,
        "_harvest_page_tokens",
        lambda minr, items, write_cache=True: harvest(minr, items),
    )

    monkeypatch.setattr(
        noordholland,
        "_discover_sections",
        lambda: [
            {"period_minr": "A", "kantoor": "Texel", "period_text": "1818-1825"},
            {"period_minr": "B", "kantoor": "Haarlem", "period_text": "1826-1830"},
        ],
    )
    monkeypatch.setattr(noordholland, "_harvest_page_tokens", lambda minr: harvest(minr))

    return harness


ALL = [
    pytest.param(gelderland, id="gelderland"),
    pytest.param(noordholland, id="noordholland"),
    pytest.param(zeeland, id="zeeland"),
]


@pytest.mark.parametrize("mod", ALL)
def test_full_run_totals_every_kantoor_and_register(mod, h) -> None:
    run = mod.main()

    assert run is not None, f"{mod.__name__}: main() returned no summary"
    assert run.archive == mod.ARCHIVE
    assert run.units == len(UNITS)
    assert run.registers == len(ALL_INVNRS)
    assert run.pages.downloaded == len(ALL_INVNRS)
    assert run.pages.skipped == 0
    assert run.pages.missing == 0
    assert run.pages.bytes_written == len(ALL_INVNRS) * PAGE_BYTES


@pytest.mark.parametrize("mod", ALL)
def test_pages_already_on_disk_are_not_counted_as_downloaded(mod, h) -> None:
    """A repeat of a filtered run re-checks the same register, nothing to fetch."""
    first = mod.main(invnrs={"4"})
    assert first.pages.downloaded == 1 and first.pages.skipped == 0

    second = mod.main(invnrs={"4"})
    assert second.pages.downloaded == 0
    assert second.pages.skipped == 1
    assert second.pages.bytes_written == 0
    assert second.registers == 1


@pytest.mark.parametrize("mod", ALL)
def test_pages_the_server_withholds_are_counted_as_missing(mod, h) -> None:
    h.status = "missing"
    run = mod.main()

    assert run.pages.missing == len(ALL_INVNRS)
    assert run.pages.downloaded == 0
    assert run.pages.bytes_written == 0
    # The registers were still visited, which is why "missing" is worth seeing.
    assert run.registers == len(ALL_INVNRS)


@pytest.mark.parametrize("mod", ALL)
def test_filtered_run_reports_only_what_it_touched(mod, h) -> None:
    run = mod.main(invnrs={"4"})

    assert run.units == 1
    assert run.registers == 1
    assert run.pages.downloaded == 1
    assert run.pages.bytes_written == PAGE_BYTES


@pytest.mark.parametrize("mod", ALL)
def test_fully_resumed_run_reports_zeroes(mod, h) -> None:
    """Second unfiltered run: done.txt marks every unit, so nothing is visited."""
    mod.main()
    run = mod.main()

    assert run.units == 0
    assert run.registers == 0
    assert run.pages.total == 0
    assert run.pages.bytes_written == 0


@pytest.mark.parametrize("mod", ALL)
def test_run_that_dies_midway_keeps_its_partial_totals(mod, h) -> None:
    h.fail_after = 1

    with collect() as collected:
        with pytest.raises(RuntimeError):
            mod.main()

    assert len(collected) == 1, f"{mod.__name__}: no summary registered"
    partial = collected[0]
    assert partial.pages.downloaded == 1
    assert partial.pages.bytes_written == PAGE_BYTES
    assert partial.registers >= 1


@pytest.mark.parametrize("mod", ALL)
def test_summary_is_printed_at_the_end_of_a_run(mod, h, capsys) -> None:
    mod.main()
    out = capsys.readouterr().out

    assert "run summary" in out
    assert "pages downloaded" in out
    assert "bytes written" in out
    # ... and the up-front line that says what is coming.
    assert "about to download" in out


# ---------------------------------------------------------------------------
# The CLI's cross-archive total
# ---------------------------------------------------------------------------


def _fake_pipelines():
    def alpha(**_):
        run = RunSummary("alpha", "Alpha", units=2, registers=5)
        run.pages += PageTally(downloaded=10, skipped=1, bytes_written=2_500_000)
        run.report()
        return run

    def beta(**_):
        run = RunSummary("beta", "Beta", units=1, registers=1)
        run.pages += PageTally(downloaded=3, bytes_written=500_000)
        raise RuntimeError("the archive hung up")

    return {"alpha": alpha, "beta": beta}


def test_cli_all_prints_a_grand_total_including_a_failed_pipeline(
    monkeypatch, capsys, tmp_path
) -> None:
    monkeypatch.setattr(cli, "PIPELINES", _fake_pipelines())
    monkeypatch.setattr(sys, "argv", ["memories-crawl", "all", "--out-dir", str(tmp_path)])

    cli.main()
    out = capsys.readouterr().out

    assert "Alpha: run summary" in out
    assert "Beta: run summary" in out  # printed by the CLI, the call never returned
    assert "ALL ARCHIVES" in out
    assert "2 archives" in out
    assert "13" in out  # 10 + 3 pages downloaded before beta died
    assert "3.0 MB" in out
    assert "INCOMPLETE" in out


def test_cli_single_archive_reports_then_reraises(monkeypatch, capsys, tmp_path) -> None:
    monkeypatch.setattr(cli, "PIPELINES", _fake_pipelines())
    monkeypatch.setattr(sys, "argv", ["memories-crawl", "beta", "--out-dir", str(tmp_path)])

    with pytest.raises(RuntimeError):
        cli.main()
    out = capsys.readouterr().out

    assert "Beta: run summary" in out
    assert "INCOMPLETE" in out
    assert "ALL ARCHIVES" not in out  # a single archive needs no cross-archive table


def test_cli_reports_a_pipeline_that_failed_before_counting(monkeypatch, capsys, tmp_path) -> None:
    """Discovery blew up, so there is no summary object -- the row is still owed."""

    def dead(**_):
        raise RuntimeError("no kantoren found")

    monkeypatch.setattr(cli, "PIPELINES", {"alpha": _fake_pipelines()["alpha"], "beta": dead})
    monkeypatch.setattr(sys, "argv", ["memories-crawl", "all", "--out-dir", str(tmp_path)])

    cli.main()
    out = capsys.readouterr().out

    assert "beta: run summary" in out
    assert "no kantoren found" in out
    assert "2 archives" in out


def test_cli_listing_run_prints_no_summary(monkeypatch, capsys, tmp_path) -> None:
    """--list-invnrs downloads nothing; a summary of zeroes would be noise."""

    def lister(**_):
        return None

    monkeypatch.setattr(cli, "PIPELINES", {"alpha": lister})
    monkeypatch.setattr(
        sys, "argv", ["memories-crawl", "alpha", "--list-invnrs", "--out-dir", str(tmp_path)]
    )

    cli.main()
    assert "run summary" not in capsys.readouterr().out
