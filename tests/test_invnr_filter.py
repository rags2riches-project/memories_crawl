"""Regression tests for issue #22.

``gelderland``, ``noordholland`` and ``zeeland`` record completion in a
``done.txt`` marker keyed at a coarser granularity than ``--invnr`` filters at
(kantoor / period section vs. inventarisnummer).  A filtered run must therefore
never write that marker, or every later run silently skips the whole archive
and downloads nothing.

The per-kantoor token caches in ``gelderland`` and ``zeeland`` have the same
shape: they claim to hold every page in a kantoor, so a filtered harvest must
not write one either.
"""

from __future__ import annotations

import pytest

from memories_crawl import gelderland, noordholland, paths, zeeland

# Two units; the first holds two inventarisnummers so that "the filter picked
# one of several in this unit" is exercised.
UNITS: dict[str, list[int]] = {"A": [4, 5], "B": [18]}
ALL_INVNRS = [i for v in UNITS.values() for i in v]


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
    """Records what the stubbed pipeline did."""

    def __init__(self) -> None:
        self.downloads: list[str] = []
        self.harvest_calls: list[dict] = []

    @property
    def downloaded_invnrs(self) -> set[int]:
        # dest paths end in e.g. ".../0004/4-0001.jpg" or ".../4/0001.jpg"
        return {int(d.rsplit("/", 2)[1].lstrip("0") or "0") for d in self.downloads}


@pytest.fixture
def h(monkeypatch, tmp_path):
    """Replace every network/Playwright call with in-memory fakes."""
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)  # default ./scans, i.e. below tmp_path
    harness = Harness()

    def fake_download(session, url, dest):
        harness.downloads.append(str(dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"png")
        return "downloaded"

    for mod in (gelderland, noordholland, zeeland):
        monkeypatch.setattr(mod, "_download_file", fake_download)
        monkeypatch.setattr(mod.time, "sleep", lambda *_: None)

    def harvest(unit: str, items: list[dict] | None, write_cache: bool) -> list[dict]:
        harness.harvest_calls.append({"unit": unit, "write_cache": write_cache})
        wanted = [it["invnr"] for it in items] if items is not None else UNITS[unit]
        return [p for i in wanted for p in _pages(i)]

    # gelderland: 21 hardcoded kantoren, each with its own archief-code
    monkeypatch.setattr(gelderland, "KANTOREN", {"Arnhem": "A", "Borculo": "B"})
    monkeypatch.setattr(gelderland, "_discover_invnrs", lambda kantoor, code: _items(UNITS[code]))
    monkeypatch.setattr(
        gelderland,
        "_harvest_page_tokens",
        lambda kantoor, code, items, write_cache=True: harvest(code, items, write_cache),
    )

    # zeeland: kantoren discovered from the inv3 tree
    monkeypatch.setattr(
        zeeland,
        "_discover_kantoren",
        lambda: [{"name": "Goes", "minr": "A"}, {"name": "Hulst", "minr": "B"}],
    )
    monkeypatch.setattr(zeeland, "_discover_invnrs", lambda minr: _items(UNITS[minr]))
    monkeypatch.setattr(
        zeeland,
        "_harvest_page_tokens",
        lambda minr, items, write_cache=True: harvest(minr, items, write_cache),
    )

    # noordholland: period sections; the --invnr filter lands after the harvest,
    # so its harvest always covers the whole section and its cache stays honest.
    monkeypatch.setattr(
        noordholland,
        "_discover_sections",
        lambda: [
            {"period_minr": "A", "kantoor": "Texel", "period_text": "1818-1825"},
            {"period_minr": "B", "kantoor": "Haarlem", "period_text": "1826-1830"},
        ],
    )
    monkeypatch.setattr(
        noordholland, "_harvest_page_tokens", lambda minr: harvest(minr, None, True)
    )

    return harness


def _done_file(mod, tmp_path):
    return tmp_path / paths.cache_dir(mod.ARCHIVE) / "done.txt"


ALL = [
    pytest.param(gelderland, id="gelderland"),
    pytest.param(noordholland, id="noordholland"),
    pytest.param(zeeland, id="zeeland"),
]
# noordholland harvests a whole period section regardless of --invnr
CACHED = [pytest.param(gelderland, id="gelderland"), pytest.param(zeeland, id="zeeland")]


@pytest.mark.parametrize("mod", ALL)
def test_invnr_filter_does_not_write_done_marker(mod, h, tmp_path):
    """A filtered run must not write done.txt, or later runs silently no-op."""
    mod.main(invnrs={"4"})

    done = _done_file(mod, tmp_path)
    assert not done.exists() or done.read_text().strip() == ""


@pytest.mark.parametrize("mod", ALL)
def test_successive_filtered_runs_both_download(mod, h):
    """Two --invnr runs for different invnrs must both fetch their register."""
    mod.main(invnrs={"4"})
    assert h.downloaded_invnrs == {4}

    h.downloads.clear()
    mod.main(invnrs={"18"})
    assert h.downloaded_invnrs == {18}


@pytest.mark.parametrize("mod", ALL)
def test_filtered_run_does_not_poison_a_later_full_run(mod, h):
    """A full run after a filtered one must still visit every unit."""
    mod.main(invnrs={"4"})
    h.downloads.clear()

    mod.main()
    assert h.downloaded_invnrs == set(ALL_INVNRS)


@pytest.mark.parametrize("mod", ALL)
def test_unfiltered_run_still_records_completion(mod, h, tmp_path):
    """Resume behaviour for full runs is preserved."""
    mod.main()
    done = _done_file(mod, tmp_path)
    assert done.exists()
    assert set(done.read_text().split()) == set(UNITS)

    h.downloads.clear()
    mod.main()
    assert h.downloads == [], f"{mod.__name__}: re-ran units already marked done"


@pytest.mark.parametrize("mod", ALL)
def test_filter_matching_nothing_warns(mod, h, capsys):
    """--invnr 99999 must not look like a successful no-op."""
    mod.main(invnrs={"99999"})
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "99999" in out
    assert h.downloads == []


@pytest.mark.parametrize("mod", CACHED)
def test_filtered_run_does_not_write_token_cache(mod, h):
    """The per-kantoor token cache claims to be complete; a subset must not write it."""
    mod.main(invnrs={"4"})
    assert h.harvest_calls, f"{mod.__name__}: never harvested"
    assert all(not c["write_cache"] for c in h.harvest_calls)

    h.harvest_calls.clear()
    mod.main()
    assert all(c["write_cache"] for c in h.harvest_calls)


@pytest.mark.parametrize("mod", ALL)
def test_warm_token_cache_still_respects_filter(mod, h, monkeypatch):
    """A cache hit returns the whole unit, which --invnr must still narrow."""
    unit_pages = {u: [p for i in v for p in _pages(i)] for u, v in UNITS.items()}

    # A warm cache short-circuits the harvest and hands back every page in the
    # unit, ignoring the (filtered) list of invnrs it was asked for.
    if mod is gelderland:
        stub = lambda kantoor, code, items, write_cache=True: unit_pages[code]  # noqa: E731
    elif mod is zeeland:
        stub = lambda minr, items, write_cache=True: unit_pages[minr]  # noqa: E731
    else:
        stub = lambda minr: unit_pages[minr]  # noqa: E731
    monkeypatch.setattr(mod, "_harvest_page_tokens", stub)

    mod.main(invnrs={"4"})
    assert h.downloaded_invnrs == {4}
