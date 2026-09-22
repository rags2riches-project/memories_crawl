"""Tests for the period columns in ``--list-invnrs`` (issue #38).

``--list-invnrs`` is the only thing a caller can plan a download from, and a
plan is normally period-limited ("registers covering 1877-1928"). Four
pipelines used to emit no date at all, so callers ranked inventarisnummers
against a date span instead -- a guess that put 80% of one 284 GB sample's
deaths outside the target years.

Every listing now carries ``year_from`` / ``year_to``. The rule mirrors the
scan counts: an archive that does not say reports ``?``, never a year derived
from the inventarisnummer.

Everything here runs offline against stubs, in the style of
``tests/test_scan_availability.py``.
"""

from __future__ import annotations

import csv
import inspect
import json

import pytest

from memories_crawl import (
    bhic,
    drentsarchief,
    friesland,
    gelderland,
    limburg,
    listing,
    nationaalarchief,
    noordholland,
    overijssel,
    paths,
    utrechtsarchief,
    zeeland,
)

# ---------------------------------------------------------------------------
# The shared vocabulary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "invnr", "expected"),
    [
        ("1818 jan. - mrt.", None, (1818, 1818)),
        ("Akten, 1826-1830.", None, (1826, 1830)),
        ("140  1895 eerste kwartaal", 140, (1895, 1895)),
        ("1  1818", 1, (1818, 1818)),
        ("Memories kantoor Sneek", None, (None, None)),
        ("", None, (None, None)),
        (None, None, (None, None)),
    ],
)
def test_parse_years_reads_the_span_out_of_a_datering(text, invnr, expected) -> None:
    assert listing.parse_years(text, invnr) == expected


def test_a_four_digit_invnr_is_never_read_as_a_year() -> None:
    """MAIS renders "<invnr>  <datering>"; 1818 the number is not 1818 the year."""
    assert listing.parse_years("1888", 1888) == (None, None)
    assert listing.parse_years("1888  1901 eerste kwartaal", 1888) == (1901, 1901)


def test_years_outside_the_collection_are_not_years() -> None:
    """A memorienummer or a page count must not become a period."""
    assert listing.parse_years("memorienrs. 9299 - 9926") == (None, None)
    assert listing.span([1662, 2024]) == (None, None)


def test_span_folds_dates_ints_and_gaps() -> None:
    assert listing.span([1837, 1838]) == (1837, 1838)
    assert listing.span(["1883-12-12", None, "1884-01-02"]) == (1883, 1884)
    assert listing.span([]) == (None, None)


def test_unknown_period_renders_as_the_marker_not_a_year() -> None:
    assert listing.fmt_year(None) == listing.UNKNOWN
    assert listing.fmt_period(None, None) == listing.UNKNOWN
    assert listing.fmt_period(1818, 1818) == "1818"
    assert listing.fmt_period(1818, 1820) == "1818-1820"
    assert listing.year_row(None, 1900) == {"year_from": "?", "year_to": "1900"}


# ---------------------------------------------------------------------------
# Every listing carries the columns
# ---------------------------------------------------------------------------

LISTING_MODULES = [
    bhic,
    drentsarchief,
    friesland,
    gelderland,
    limburg,
    nationaalarchief,
    noordholland,
    overijssel,
    utrechtsarchief,
    zeeland,
]


@pytest.mark.parametrize("mod", LISTING_MODULES, ids=lambda m: m.ARCHIVE)
def test_every_listing_schema_has_the_year_columns(mod) -> None:
    assert mod.LIST_FIELDS[-2:] == listing.YEAR_FIELDS, mod.ARCHIVE


def test_cli_exposes_the_dates_flag() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "memories_crawl", "--help"], capture_output=True, text=True
    )
    assert "--dates" in result.stdout


@pytest.mark.parametrize("archive", sorted({"drentsarchief", "bhic"}))
def test_query_dated_pipelines_accept_the_flag(archive) -> None:
    import importlib

    from memories_crawl import cli

    params = inspect.signature(importlib.import_module(f"memories_crawl.{archive}").main).parameters
    assert "dates" in params, archive
    assert archive in cli.DATE_QUERY_PIPELINES


def _read_csv(path) -> tuple[list[str], list[dict]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


# ---------------------------------------------------------------------------
# Nationaal Archief: the EAD already carries the period
# ---------------------------------------------------------------------------

_EAD = b"""<ead><dsc>
  <c><did><unitid>2</unitid></did>
    <c><did><unitid>2.4</unitid></did>
      <c><did><unitid>2.4.01</unitid><unittitle>Kantoor Alphen aan de Rijn</unittitle></did>
        <c><did><unitid>2.4.01.1</unitid><unittitle>Memories van successie</unittitle></did>
          <c><did><unitid>2276</unitid><unittitle>
             <unitdate normal="1818-01/1818-03">1818 jan. - mrt.</unitdate>
          </unittitle><dao href="mets://a"/></did></c>
          <c><did><unitid>2389</unitid>
             <unittitle>1849, memories van aangifte, jan. - juni</unittitle>
             <dao href="mets://b"/></did></c>
          <c><did><unitid>2390</unitid>
             <unittitle>l872 okt. - dec.</unittitle><dao href="mets://c"/></did></c>
        </c>
      </c>
    </c>
  </c>
</dsc></ead>"""


def test_ead_dates_come_from_normal_then_from_the_title() -> None:
    entries = {e["invnr"]: e for e in nationaalarchief._parse_ead_entries(_EAD)}

    assert (entries[2276]["year_from"], entries[2276]["year_to"]) == (1818, 1818)
    assert entries[2276]["description"] == "1818 jan. - mrt."
    # No <unitdate normal>: the years are read out of the title text instead.
    assert (entries[2389]["year_from"], entries[2389]["year_to"]) == (1849, 1849)
    # A typo the archive has not fixed ("l872") stays unknown rather than
    # becoming a wrong year.
    assert entries[2390]["year_from"] is None


def test_nationaalarchief_listing_reports_the_period_without_extra_requests(
    tmp_path, capsys
) -> None:
    entries = nationaalarchief._parse_ead_entries(_EAD)
    out = tmp_path / "invnrs.csv"
    nationaalarchief._list_inventory(entries, csv_out=str(out))

    header, rows = _read_csv(out)
    assert header[-2:] == listing.YEAR_FIELDS
    by_invnr = {r["invnr"]: r for r in rows}
    assert by_invnr["2276"]["year_from"] == "1818"
    assert by_invnr["2390"]["year_from"] == listing.UNKNOWN
    assert "1818" in capsys.readouterr().out


def test_nationaalarchief_entry_cache_key_rejects_dateless_rows() -> None:
    """A v1 cache predates the period columns, so it must not be served."""
    assert "#entries-v2" in inspect.getsource(nationaalarchief._fetch_inventory_entries)


def test_nationaalarchief_fallback_rows_have_no_invented_period() -> None:
    entries = nationaalarchief._fallback_entries()
    assert all(e["year_from"] is None and e["year_to"] is None for e in entries[:50])


# ---------------------------------------------------------------------------
# Friesland: the register metadata carries `periode`
# ---------------------------------------------------------------------------


def _frl_register(invnr: str, periode) -> dict:
    return {
        "id": f"reg-{invnr}",
        "asset": [{"dc_title": "scan"}],
        "metadata": {
            "inventarisnummer": invnr,
            "naam": "Memories kantoor Sneek",
            "periode": periode,
        },
    }


def test_friesland_period_is_free_from_the_register_listing(tmp_path) -> None:
    registers = [
        _frl_register("14100", [1883]),
        _frl_register("14045", [1837, 1838]),
        _frl_register("14961", None),
    ]
    out = tmp_path / "invnrs.csv"
    friesland._list_registers(registers, csv_out=str(out))

    _, rows = _read_csv(out)
    by_invnr = {r["invnr"]: r for r in rows}
    assert (by_invnr["14100"]["year_from"], by_invnr["14100"]["year_to"]) == ("1883", "1883")
    assert (by_invnr["14045"]["year_from"], by_invnr["14045"]["year_to"]) == ("1837", "1838")
    assert by_invnr["14045"]["period"] == "1837-1838"
    # Tresoar leaves exactly one register without a periode; it says so.
    assert by_invnr["14961"]["period"] == listing.UNKNOWN


# ---------------------------------------------------------------------------
# Drenthe / BHIC: the period costs a request, so it is opt-in
# ---------------------------------------------------------------------------

MEMORIX_QUERY = [
    pytest.param(drentsarchief, id="drentsarchief"),
    pytest.param(bhic, id="bhic"),
]


def _memorix_register(invnr: str) -> dict:
    return {
        "id": f"reg-{invnr}",
        "asset": [{"dc_title": "scan"}],
        "metadata": {
            "inventarisnummer": invnr,
            "gemeente": "Coevorden",
            "naam": f"Successiememorie Coevorden 0119.07 {invnr}",
        },
    }


@pytest.mark.parametrize("mod", MEMORIX_QUERY)
def test_memorix_register_years_come_from_the_death_dates(mod, monkeypatch) -> None:
    calls: list[str] = []

    def paginate(session, path, fq, key):
        calls.append(path)
        return [
            {"metadata": {"datum_overlijden": "1839-06-07"}},
            {"metadata": {"datum": "1840-01-02"}},
            {"metadata": {}},
        ]

    monkeypatch.setattr(mod, "_paginate", paginate)
    assert mod._register_years(None, _memorix_register("31")) == (1839, 1840)
    assert calls == ["/person"], "one request per register, nothing more"


@pytest.mark.parametrize("mod", MEMORIX_QUERY)
def test_memorix_listing_without_dates_says_unknown(mod, tmp_path) -> None:
    out = tmp_path / "invnrs.csv"
    mod._list_registers([_memorix_register("31")], csv_out=str(out))

    _, rows = _read_csv(out)
    assert rows[0]["year_from"] == rows[0]["year_to"] == listing.UNKNOWN
    assert rows[0]["period"] == listing.UNKNOWN


@pytest.mark.parametrize("mod", MEMORIX_QUERY)
def test_memorix_dates_flag_resolves_the_period(mod, monkeypatch, tmp_path) -> None:
    registers = [_memorix_register("31")]
    monkeypatch.setattr(mod, "_collect_registers", lambda *a, **kw: registers, raising=False)
    monkeypatch.setattr(mod, "_load_registers", lambda *a, **kw: registers, raising=False)
    monkeypatch.setattr(mod, "_register_years", lambda session, reg: (1839, 1840))
    monkeypatch.setattr(mod, "_count_scans", lambda *a: pytest.fail("--dates is not --count-scans"))

    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out), dates=True, out_dir=tmp_path)

    _, rows = _read_csv(out)
    assert (rows[0]["year_from"], rows[0]["year_to"]) == ("1839", "1840")
    assert rows[0]["period"] == "1839-1840"


@pytest.mark.parametrize("mod", MEMORIX_QUERY)
def test_memorix_default_listing_spends_no_request_on_dates(mod, monkeypatch, tmp_path) -> None:
    registers = [_memorix_register("31")]
    monkeypatch.setattr(mod, "_collect_registers", lambda *a, **kw: registers, raising=False)
    monkeypatch.setattr(mod, "_load_registers", lambda *a, **kw: registers, raising=False)
    monkeypatch.setattr(mod, "_register_years", lambda *a: pytest.fail("dated without --dates"))

    mod.main(list_invnrs=True, csv_out=str(tmp_path / "invnrs.csv"), out_dir=tmp_path)


# ---------------------------------------------------------------------------
# Overijssel: the description was on the page all along
# ---------------------------------------------------------------------------


def _ov_tokens(invnr: int, pages: int, inv_text: str) -> list[dict]:
    return [
        {
            "invnr": invnr,
            "page": p,
            "miahd": 1,
            "rdt": "20251205",
            "open": "ABCDE",
            "inv_text": inv_text,
        }
        for p in range(1, pages + 1)
    ]


@pytest.fixture
def overijssel_stub(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)
    monkeypatch.setattr(overijssel, "KANTOOR_MINR", {"Almelo": 2227676})
    return tmp_path


def test_overijssel_listing_keeps_the_scraped_description(overijssel_stub, tmp_path) -> None:
    cache = overijssel._get_token_cache_path(2227676)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(
        json.dumps(_ov_tokens(1, 2, "1  1818") + _ov_tokens(12, 3, "12  1843 jan.-juni")),
        encoding="utf-8",
    )

    out = tmp_path / "ov.csv"
    overijssel.main(list_invnrs=True, csv_out=str(out))

    header, rows = _read_csv(out)
    assert header == overijssel.LIST_FIELDS
    assert "description" in header
    by_invnr = {r["invnr"]: r for r in rows}
    assert by_invnr["1"]["description"] == "1  1818"
    assert (by_invnr["12"]["year_from"], by_invnr["12"]["year_to"]) == ("1843", "1843")


def test_overijssel_cache_from_before_the_fix_reports_unknown(overijssel_stub, tmp_path) -> None:
    """An old token cache has no description; that is a '?', not a wrong year."""
    tokens = _ov_tokens(1, 2, "")
    for tok in tokens:
        del tok["inv_text"]
    cache = overijssel._get_token_cache_path(2227676)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(tokens), encoding="utf-8")

    out = tmp_path / "ov.csv"
    overijssel.main(list_invnrs=True, csv_out=str(out))

    _, rows = _read_csv(out)
    assert rows[0]["description"] == ""
    assert rows[0]["year_from"] == listing.UNKNOWN


def test_overijssel_metadata_carries_the_datering(tmp_path) -> None:
    overijssel._write_metadata(tmp_path, "Almelo", 12, 3, "12  1843 jan.-juni")
    meta = json.loads((tmp_path / "metadata.json").read_text())
    assert meta["datering"] == "1843 jan.-juni"
    assert meta["omschrijving"] == "12  1843 jan.-juni"


def test_overijssel_scrapes_the_same_dom_field_as_gelderland() -> None:
    """Both are MAIS: the tree link text is right there in the DOM."""
    assert "textContent" in overijssel._JS_COLLECT_STK3
    assert "textContent" in gelderland._JS_COLLECT_INVNRS


# ---------------------------------------------------------------------------
# The MAIS pipelines that already had a description
# ---------------------------------------------------------------------------


def _mais_items(invnrs: list[int]) -> list[dict]:
    return [{"invnr": i, "text": f"{i}  1818", "minr": 1000 + i, "hasScan": True} for i in invnrs]


@pytest.mark.parametrize(
    ("mod", "setup"),
    [
        pytest.param(
            zeeland,
            lambda mp: (
                mp.setattr(zeeland, "_discover_kantoren", lambda: [{"name": "Goes", "minr": 1}]),
                mp.setattr(zeeland, "_discover_invnrs", lambda minr: _mais_items([1, 2])),
            ),
            id="zeeland",
        ),
        pytest.param(
            gelderland,
            lambda mp: (
                mp.setattr(gelderland, "KANTOREN", {"Borculo": "0022"}),
                mp.setattr(
                    gelderland, "_discover_invnrs", lambda kantoor, code: _mais_items([1, 2])
                ),
            ),
            id="gelderland",
        ),
    ],
)
def test_mais_listing_dates_each_register_from_its_description(
    mod, setup, monkeypatch, tmp_path
) -> None:
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)
    setup(monkeypatch)

    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out))

    header, rows = _read_csv(out)
    assert header == mod.LIST_FIELDS
    assert {r["year_from"] for r in rows} == {"1818"}


def test_limburg_uses_the_archives_own_datering(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)
    items = [
        {
            "invnr": 1,
            "title": "Amby, 1818-1828",
            "name": "Amby",
            "datering": "1818-1828",
            "minr": 7,
            "hasScan": True,
        }
    ]
    monkeypatch.setattr(limburg, "ARCHIVE_CODES", {"07.D03": limburg.ARCHIVE_CODES["07.D03"]})
    monkeypatch.setattr(limburg, "_harvest_inventory", lambda code: items)

    out = tmp_path / "invnrs.csv"
    limburg.main(list_invnrs=True, csv_out=str(out))

    header, rows = _read_csv(out)
    assert header == limburg.LIST_FIELDS
    assert (rows[0]["year_from"], rows[0]["year_to"]) == ("1818", "1828")


def test_noordholland_register_years_do_not_inherit_the_section(tmp_path) -> None:
    """The section heading spans years the register itself may not cover."""
    assert noordholland.LIST_FIELDS[-2:] == listing.YEAR_FIELDS
    assert "period" in noordholland.LIST_FIELDS
    assert utrechtsarchief.LIST_FIELDS[-2:] == listing.YEAR_FIELDS
