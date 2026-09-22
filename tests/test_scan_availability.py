"""Tests for the scan-availability columns in ``--list-invnrs`` (issue #27).

``--list-invnrs`` exists so a caller can decide what to download *before*
spending anything, so every pipeline reports a count next to each
inventarisnummer. Counts an archive will not hand over cheaply are rendered as
``?`` and must never collapse into ``0``: ``--only-digitized`` drops known
zeroes and keeps unknowns, or it would hide real data.

Everything here runs offline against stubs, in the style of
``tests/test_invnr_filter.py``.
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
    listing,
    nationaalarchief,
    paths,
    zeeland,
)

# ---------------------------------------------------------------------------
# The unknown marker
# ---------------------------------------------------------------------------


def test_unknown_count_renders_as_marker_not_zero() -> None:
    assert listing.fmt_count(None) == listing.UNKNOWN
    assert listing.fmt_count(0) == "0"
    assert listing.fmt_count(17) == "17"


def test_only_digitized_drops_known_zero_but_keeps_unknown() -> None:
    assert listing.has_scans(None), "an unmeasured count must never be filtered away"
    assert listing.has_scans(1)
    assert not listing.has_scans(0)


def test_summary_suffix_flags_only_empty_registers() -> None:
    assert listing.summary_suffix(0) == listing.NOTHING_TO_DOWNLOAD
    assert listing.summary_suffix(3) == ""
    assert listing.summary_suffix(None) == ""


# ---------------------------------------------------------------------------
# Memorix register listings (friesland, bhic, drentsarchief)
# ---------------------------------------------------------------------------


def _memorix_register(
    invnr: str, *, digitized: bool, name: str = "Memories kantoor Lemmer"
) -> dict:
    """A /register search hit; ``asset`` is the archive's free digitized flag."""
    reg = {
        "id": f"reg-{invnr}",
        "metadata": {
            "inventarisnummer": invnr,
            "gemeente": "Lemmer",
            "naam": name,
            "type_title": "Belastingen",
        },
    }
    if digitized:
        reg["asset"] = [{"dc_title": f"scan-{invnr}"}]
    return reg


def _read_csv(path) -> tuple[list[str], list[dict]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


MEMORIX = [
    pytest.param(friesland, "n_with_scans", id="friesland"),
    pytest.param(bhic, "n_scans", id="bhic"),
    pytest.param(drentsarchief, "n_scans", id="drentsarchief"),
]


@pytest.mark.parametrize(("mod", "count_field"), MEMORIX)
def test_listing_csv_header_matches_the_module_schema(mod, count_field, tmp_path) -> None:
    registers = [
        _memorix_register("12038", digitized=False),
        _memorix_register("14008", digitized=True),
    ]
    out = tmp_path / "invnrs.csv"
    mod._list_registers(registers, csv_out=str(out))

    header, rows = _read_csv(out)
    assert header == mod.LIST_FIELDS
    assert count_field in header
    assert len(rows) == 2


@pytest.mark.parametrize(("mod", "count_field"), MEMORIX)
def test_free_digitized_flag_gives_exact_zero_and_honest_unknown(
    mod, count_field, tmp_path
) -> None:
    """Without --count-scans: no assets means 0, assets means '?', never a guess."""
    registers = [
        _memorix_register("12038", digitized=False),
        _memorix_register("14008", digitized=True),
    ]
    out = tmp_path / "invnrs.csv"
    mod._list_registers(registers, csv_out=str(out))

    _, rows = _read_csv(out)
    by_invnr = {r["invnr"]: r for r in rows}
    assert by_invnr["12038"][count_field] == "0"
    assert by_invnr["14008"][count_field] == listing.UNKNOWN


@pytest.mark.parametrize(("mod", "count_field"), MEMORIX)
def test_printed_table_and_csv_agree(mod, count_field, tmp_path, capsys) -> None:
    registers = [
        _memorix_register("12038", digitized=False),
        _memorix_register("14008", digitized=True),
    ]
    out = tmp_path / "invnrs.csv"
    mod._list_registers(registers, csv_out=str(out))
    printed = capsys.readouterr().out

    _, rows = _read_csv(out)
    for row in rows:
        line = next(ln for ln in printed.splitlines() if f" {row['invnr']} " in f" {ln} ")
        assert row[count_field] in line.split()


@pytest.mark.parametrize(("mod", "count_field"), MEMORIX)
def test_only_digitized_drops_registers_without_scans(mod, count_field, tmp_path) -> None:
    registers = [
        _memorix_register("12038", digitized=False),
        _memorix_register("14008", digitized=True),
    ]
    out = tmp_path / "invnrs.csv"
    mod._list_registers(registers, csv_out=str(out), only_digitized=True)

    _, rows = _read_csv(out)
    assert [r["invnr"] for r in rows] == ["14008"]


@pytest.mark.parametrize(("mod", "count_field"), MEMORIX)
def test_only_digitized_keeps_an_unknown_count(mod, count_field, tmp_path) -> None:
    """A count we failed to measure is not a reason to hide the register."""
    registers = [_memorix_register("14008", digitized=True)]
    counts = {"reg-14008": (None, None) if mod is friesland else None}
    out = tmp_path / "invnrs.csv"
    mod._list_registers(registers, csv_out=str(out), counts=counts, only_digitized=True)

    _, rows = _read_csv(out)
    assert [r[count_field] for r in rows] == [listing.UNKNOWN]


def test_friesland_exact_counts_join_persons_to_deeds(monkeypatch) -> None:
    calls = []

    def paginate(session, path, fq, key):
        calls.append(path)
        if path == "/deed":
            return [
                {"id": "d1", "asset": [{}]},
                {"id": "d2", "asset": []},
                {"id": "orphan", "asset": [{}]},
            ]
        return [
            {"deed_id": "d1", "metadata": {"type_title": "Overledene"}},
            {"deed_id": "d1", "metadata": {"type_title": "Overledene"}},
            {"deed_id": "d1", "metadata": {"type_title": "Vermeld"}},
            {"deed_id": "d2", "metadata": {}},
        ]

    monkeypatch.setattr(friesland, "_paginate", paginate)
    assert friesland._register_counts(None, {"id": "r"}, True) == (3, 2)
    assert calls == ["/person", "/deed"]


def test_bhic_exact_count_is_one_asset_request(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_get_json(session, path, params, retries=3):
        calls.append({"path": path, "rows": params["rows"]})
        return {"metadata": {"pagination": {"total": 468}}}

    monkeypatch.setattr(bhic, "_get_json", fake_get_json)
    assert bhic._count_scans(None, _memorix_register("84", digitized=True)) == 468
    assert calls == [{"path": "/asset", "rows": 1}]


# ---------------------------------------------------------------------------
# Friesland per-register summary line
# ---------------------------------------------------------------------------


@pytest.fixture
def friesland_stub(monkeypatch, tmp_path):
    """Two registers: one digitized, one indexed but never scanned."""
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)

    registers = [
        _memorix_register("12038", digitized=False),
        _memorix_register("14008", digitized=True),
    ]
    deeds = {
        "reg-12038": [{"id": "d1", "asset": []}, {"id": "d2", "asset": []}],
        "reg-14008": [{"id": "d3", "asset": [{"download": "https://example.invalid/a.jp2"}]}],
    }
    persons = {
        "reg-12038": [
            {"id": "p1", "deed_id": "d1", "metadata": {"person_display_name": "A"}},
            {"id": "p2", "deed_id": "d2", "metadata": {"person_display_name": "B"}},
        ],
        "reg-14008": [{"id": "p3", "deed_id": "d3", "metadata": {"person_display_name": "C"}}],
    }

    def fake_paginate(session, path, fq, key):
        if path == "/register":
            return registers
        reg_id = fq.split(":", 1)[1]
        return (deeds if path == "/deed" else persons).get(reg_id, [])

    monkeypatch.setattr(friesland, "_paginate", fake_paginate)
    monkeypatch.setattr(friesland, "time", type("T", (), {"sleep": staticmethod(lambda *_: None)}))
    monkeypatch.setattr(
        friesland, "_download_file", lambda session, url, dest, retries=3: "downloaded"
    )
    return tmp_path


def test_friesland_logs_persons_and_with_scans_per_register(friesland_stub, capsys) -> None:
    friesland.main()
    out = capsys.readouterr().out

    assert "Lemmer 12038: 2 persons, 0 with scans — nothing to download" in out
    assert "Lemmer 14008: 1 persons, 1 with scans" in out
    assert "14008: 1 persons, 1 with scans — nothing" not in out


def test_friesland_only_digitized_skips_the_empty_register(friesland_stub, capsys) -> None:
    friesland.main(only_digitized=True)
    out = capsys.readouterr().out

    assert "--only-digitized: 1 of 2 registers have scans." in out
    assert "12038" not in out.split("--only-digitized", 1)[1]
    assert not (friesland_stub / "scans" / "friesland" / "Lemmer" / "12038").exists()


# ---------------------------------------------------------------------------
# Nationaal Archief: kantoor column + the EAD's free digitized marker
# ---------------------------------------------------------------------------

_EAD = b"""<ead><dsc>
  <c><did><unitid>2</unitid></did>
    <c><did><unitid>2.4</unitid></did>
      <c><did><unitid>2.4.01</unitid><unittitle>Kantoor Alphen aan de Rijn</unittitle></did>
        <c><did><unitid>2.4.01.1</unitid><unittitle>Memories van successie</unittitle></did>
          <c><did><unitid>2276</unitid><dao href="mets://a"/></did></c>
          <c><did><unitid>2277</unitid></did></c>
        </c>
        <c><did><unitid>2.4.01.2</unitid><unittitle>Tafel V-bis</unittitle></did>
          <c><did><unitid>9999</unitid><dao href="mets://b"/></did></c>
        </c>
      </c>
    </c>
  </c>
</dsc></ead>"""


def test_ead_carries_kantoor_and_digitized_marker() -> None:
    entries = nationaalarchief._parse_ead_entries(_EAD)

    assert [e["invnr"] for e in entries] == [2276, 2277], "Tafel V-bis must stay excluded"
    assert all(e["kantoor"] == "Kantoor Alphen aan de Rijn" for e in entries)
    assert entries[0]["has_scans"] is True
    assert entries[1]["has_scans"] is False


def test_nationaalarchief_listing_columns(tmp_path, capsys) -> None:
    entries = nationaalarchief._parse_ead_entries(_EAD)
    out = tmp_path / "invnrs.csv"
    nationaalarchief._list_inventory(entries, csv_out=str(out))

    header, rows = _read_csv(out)
    assert (
        header
        == nationaalarchief.LIST_FIELDS
        == ["invnr", "kantoor", "n_scans", "description", "year_from", "year_to"]
    )
    assert {r["invnr"]: r["n_scans"] for r in rows} == {"2276": listing.UNKNOWN, "2277": "0"}
    assert "Kantoor Alphen aan de Rijn" in capsys.readouterr().out


def test_nationaalarchief_only_digitized_uses_the_free_marker(tmp_path) -> None:
    entries = nationaalarchief._parse_ead_entries(_EAD)
    out = tmp_path / "invnrs.csv"
    nationaalarchief._list_inventory(entries, csv_out=str(out), only_digitized=True)

    _, rows = _read_csv(out)
    assert [r["invnr"] for r in rows] == ["2276"]


def test_nationaalarchief_fallback_rows_carry_a_kantoor_and_no_false_zero() -> None:
    entries = nationaalarchief._fallback_entries()
    assert entries, "fallback list must not be empty"
    assert all(e["kantoor"] for e in entries)
    # Without the EAD there is no digitization marker at all, so nothing may
    # claim to be empty.
    assert all(e["has_scans"] is None for e in entries)
    assert all(nationaalarchief._entry_n_scans(e, None) is None for e in entries[:50])


# ---------------------------------------------------------------------------
# MAIS listings: pages from a warm token cache, '?' when cold
# ---------------------------------------------------------------------------


def _mais_items(invnrs: list[int]) -> list[dict]:
    return [{"invnr": i, "text": f"{i}  1818", "minr": 1000 + i, "hasScan": True} for i in invnrs]


def _tokens(invnr: int, pages: int) -> list[dict]:
    return [{"invnr": invnr, "page": p, "inv_text": f"{invnr}  1818"} for p in range(1, pages + 1)]


@pytest.fixture
def mais_stub(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)

    monkeypatch.setattr(zeeland, "_discover_kantoren", lambda: [{"name": "Goes", "minr": 33439946}])
    monkeypatch.setattr(zeeland, "_discover_invnrs", lambda minr: _mais_items([1, 2]))
    monkeypatch.setattr(gelderland, "KANTOREN", {"Borculo": "0022"})
    monkeypatch.setattr(gelderland, "_discover_invnrs", lambda kantoor, code: _mais_items([1, 2]))
    return tmp_path


MAIS = [
    pytest.param(zeeland, lambda: zeeland._token_cache_path(33439946), id="zeeland"),
    pytest.param(gelderland, lambda: gelderland._tokens_path("0022"), id="gelderland"),
]


@pytest.mark.parametrize(("mod", "cache_path"), MAIS)
def test_mais_listing_reports_unknown_pages_without_a_token_cache(
    mod, cache_path, mais_stub, tmp_path
) -> None:
    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out))

    header, rows = _read_csv(out)
    assert header == mod.LIST_FIELDS
    assert "pages" in header
    assert {r["pages"] for r in rows} == {listing.UNKNOWN}


@pytest.mark.parametrize(("mod", "cache_path"), MAIS)
def test_mais_listing_reports_exact_pages_from_a_warm_cache(
    mod, cache_path, mais_stub, tmp_path
) -> None:
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_tokens(1, 3) + _tokens(2, 5)), encoding="utf-8")

    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out))

    _, rows = _read_csv(out)
    assert {r["invnr"]: r["pages"] for r in rows} == {"1": "3", "2": "5"}


@pytest.mark.parametrize(("mod", "cache_path"), MAIS)
def test_mais_complete_cache_makes_a_missing_invnr_a_real_zero(
    mod, cache_path, mais_stub, tmp_path
) -> None:
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_tokens(1, 3)), encoding="utf-8")

    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out), only_digitized=True)

    _, rows = _read_csv(out)
    assert [r["invnr"] for r in rows] == ["1"], "invnr 2 is absent from a complete cache"


@pytest.mark.parametrize(("mod", "cache_path"), MAIS)
def test_mais_partial_cache_never_reports_a_missing_invnr_as_zero(
    mod, cache_path, mais_stub, tmp_path
) -> None:
    """A half-finished harvest says nothing about the invnrs it never reached."""
    complete = cache_path()
    partial = complete.with_name(complete.name.replace(".json", "_partial.json"))
    partial.parent.mkdir(parents=True, exist_ok=True)
    partial.write_text(json.dumps(_tokens(1, 3)), encoding="utf-8")

    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out), only_digitized=True)

    _, rows = _read_csv(out)
    assert {r["invnr"]: r["pages"] for r in rows} == {"1": "3", "2": listing.UNKNOWN}


@pytest.mark.parametrize(("mod", "cache_path"), MAIS)
def test_mais_count_scans_harvests_the_pages(mod, cache_path, mais_stub, tmp_path, monkeypatch):
    harvested = _tokens(1, 4) + _tokens(2, 6)
    if mod is gelderland:
        stub = lambda kantoor, code, items, write_cache=True: harvested  # noqa: E731
    else:
        stub = lambda minr, items, write_cache=True: harvested  # noqa: E731
    monkeypatch.setattr(mod, "_harvest_page_tokens", stub)

    out = tmp_path / "invnrs.csv"
    mod.main(list_invnrs=True, csv_out=str(out), count_scans=True)

    _, rows = _read_csv(out)
    assert {r["invnr"]: r["pages"] for r in rows} == {"1": "4", "2": "6"}


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------

ARCHIVES = [
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
]


@pytest.mark.parametrize("archive", ARCHIVES)
def test_every_pipeline_main_accepts_the_new_flags(archive) -> None:
    import importlib

    params = inspect.signature(importlib.import_module(f"memories_crawl.{archive}").main).parameters
    assert "only_digitized" in params, archive
    assert "count_scans" in params, archive


def test_cli_exposes_the_new_flags() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "memories_crawl", "--help"], capture_output=True, text=True
    )
    assert "--only-digitized" in result.stdout
    assert "--count-scans" in result.stdout


@pytest.mark.parametrize(("mod", "cache_path"), MAIS)
def test_count_scans_preserves_full_cache_when_invnr_selected(
    mod, cache_path, mais_stub, monkeypatch, tmp_path
):
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    contents = json.dumps(_tokens(1, 3) + _tokens(2, 5))
    path.write_text(contents)
    calls = []

    def harvest(*args, write_cache=True):
        calls.append(write_cache)
        assert [it["invnr"] for it in args[-1]] == [1]
        return _tokens(1, 3) + _tokens(2, 5)

    monkeypatch.setattr(mod, "_harvest_page_tokens", harvest)
    out = tmp_path / "selected.csv"
    mod.main(
        invnrs={"1"}, list_invnrs=True, only_digitized=True, count_scans=True, csv_out=str(out)
    )
    assert calls == [False]
    assert path.read_text() == contents
    assert not paths.cache_file(mod.ARCHIVE, "done.txt").exists()
    _, rows = _read_csv(out)
    assert [(r["invnr"], r["pages"]) for r in rows] == [("1", "3")]


def test_gelderland_zero_pages_do_not_imply_office_did_not_match(mais_stub, monkeypatch, capsys):
    monkeypatch.setattr(gelderland, "_harvest_page_tokens", lambda *a, **kw: [])
    gelderland.main(kantoren={"Borculo"}, list_invnrs=True, only_digitized=True, count_scans=True)
    assert "WARNING" not in capsys.readouterr().out
