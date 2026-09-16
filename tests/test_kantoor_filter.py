"""Tests for the ``--kantoor`` filter (issue #24).

``--invnr`` narrows the download but not the search: every pipeline still
walked all of its kantoren to find the register, which on a cold cache is a
full discovery pass per kantoor.  ``--kantoor`` lets the caller hand back what
``--list-invnrs`` told them, and a warm inventory/token cache lets a pipeline
rule a kantoor out by itself.  Both have to happen *before* any discovery work,
which is what these tests pin down.
"""

from __future__ import annotations

import json

import pytest

from memories_crawl import download, filters, gelderland, noordholland, paths, zeeland

# Per archive: the archive's own kantoor identifier -> (kantoor name, invnrs).
# Gelderland keys kantoren by archief-code, Zeeland and Noord-Holland by minr.
CASES: dict[str, dict[str, tuple[str, list[int]]]] = {
    "gelderland": {"0021": ("Arnhem", [4, 5]), "0022": ("Borculo", [18])},
    "zeeland": {"33439946": ("Goes", [4, 5]), "33439947": ("Hulst", [18])},
    "noordholland": {"7001": ("Texel", [4, 5]), "7002": ("Haarlem", [18])},
}
MODULES = {"gelderland": gelderland, "zeeland": zeeland, "noordholland": noordholland}

# Only these two can rule a kantoor out from a cache: Gelderland caches the
# kantoor's inventory, Zeeland its complete token harvest.  Noord-Holland
# caches neither at kantoor granularity.
CACHE_SKIPPERS = {"gelderland", "zeeland"}


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
    """Records which kantoren the stubbed pipeline actually worked on."""

    def __init__(self, name: str, units: dict[str, tuple[str, list[int]]]) -> None:
        self.name = name
        self.mod = MODULES[name]
        self.units = units
        self.downloads: list[str] = []
        self.discovered: list[str] = []

    # -- unit helpers ----------------------------------------------------
    @property
    def first(self) -> str:
        return next(iter(self.units))

    @property
    def second(self) -> str:
        return list(self.units)[1]

    def name_of(self, unit: str) -> str:
        return self.units[unit][0]

    def invnrs_of(self, unit: str) -> list[int]:
        return self.units[unit][1]

    @property
    def downloaded_invnrs(self) -> set[int]:
        return {int(d.rsplit("/", 2)[1].lstrip("0") or "0") for d in self.downloads}

    # -- stubs -----------------------------------------------------------
    def fake_download(self, session, url, dest) -> str:
        self.downloads.append(str(dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"png")
        return "downloaded"

    def discover(self, unit: str) -> list[dict]:
        """Stand-in for the per-kantoor (Playwright) discovery pass."""
        self.discovered.append(unit)
        return _items(self.invnrs_of(unit))

    def pages(self, items: list[dict]) -> list[dict]:
        return [p for it in items for p in _pages(it["invnr"])]

    # -- cache priming ---------------------------------------------------
    def warm_cache(self, unit: str) -> None:
        """Write the cache that lets the pipeline rule ``unit`` in or out."""
        if self.name == "gelderland":
            path = paths.cache_file(self.mod.ARCHIVE, f"inventory_{unit}.json")
            payload: object = _items(self.invnrs_of(unit))
        elif self.name == "zeeland":
            path = paths.cache_file(self.mod.ARCHIVE, f"tokens_minr_{unit}.json")
            payload = [p for i in self.invnrs_of(unit) for p in _pages(i)]
        else:  # pragma: no cover - guarded by CACHE_SKIPPERS
            raise AssertionError(f"{self.name} has no kantoor-level cache")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)


@pytest.fixture(params=sorted(CASES))
def h(request, monkeypatch, tmp_path):
    """Replace every network/Playwright call of one pipeline with fakes."""
    harness = Harness(request.param, CASES[request.param])
    mod = harness.mod

    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)  # default ./scans, i.e. below tmp_path

    monkeypatch.setattr(mod, "_download_file", harness.fake_download)
    monkeypatch.setattr(download.time, "sleep", lambda *_: None)

    if mod is gelderland:
        monkeypatch.setattr(mod, "KANTOREN", {n: k for k, (n, _) in harness.units.items()})
        monkeypatch.setattr(mod, "_discover_invnrs", lambda kantoor, code: harness.discover(code))
        monkeypatch.setattr(
            mod,
            "_harvest_page_tokens",
            lambda kantoor, code, items, write_cache=True: harness.pages(items),
        )
    elif mod is zeeland:
        monkeypatch.setattr(
            mod,
            "_discover_kantoren",
            lambda: [{"name": n, "minr": k} for k, (n, _) in harness.units.items()],
        )
        monkeypatch.setattr(mod, "_discover_invnrs", lambda minr: harness.discover(minr))
        monkeypatch.setattr(
            mod,
            "_harvest_page_tokens",
            lambda minr, items, write_cache=True: harness.pages(items),
        )
    else:
        monkeypatch.setattr(
            mod,
            "_discover_sections",
            lambda: [
                {"period_minr": k, "kantoor": n, "period_text": "1818-1825"}
                for k, (n, _) in harness.units.items()
            ],
        )
        # Noord-Holland has no separate discovery step: the harvest is the
        # expensive per-section pass, so that is what must be skipped.
        monkeypatch.setattr(
            mod, "_harvest_page_tokens", lambda minr: harness.pages(harness.discover(minr))
        )

    return harness


def _done_file(mod, tmp_path):
    return tmp_path / paths.cache_dir(mod.ARCHIVE) / "done.txt"


# ---------------------------------------------------------------------------
# CLI threading
# ---------------------------------------------------------------------------


def _run_cli(monkeypatch, argv: list[str]) -> None:
    import sys

    from memories_crawl import cli

    monkeypatch.setattr(sys, "argv", ["memories-crawl", *argv])
    cli.main()


def test_cli_forwards_kantoor_to_the_pipeline(monkeypatch, tmp_path) -> None:
    captured: dict = {}
    monkeypatch.setattr(gelderland, "main", lambda **kw: captured.update(kw))

    _run_cli(
        monkeypatch,
        [
            "gelderland",
            "--kantoor",
            "Tiel",
            "--kantoor",
            "0026",
            "--invnr",
            "4",
            "--out-dir",
            str(tmp_path),
        ],
    )

    assert captured["kantoren"] == {"Tiel", "0026"}
    assert captured["invnrs"] == {"4"}


def test_cli_reports_kantoor_as_ignored_for_nationaalarchief(monkeypatch, tmp_path, capsys) -> None:
    from memories_crawl import nationaalarchief

    captured: dict = {}
    monkeypatch.setattr(nationaalarchief, "main", lambda **kw: captured.update(kw))

    _run_cli(monkeypatch, ["nationaalarchief", "--kantoor", "Tiel", "--out-dir", str(tmp_path)])

    assert "kantoren" not in captured
    assert "--kantoor is ignored" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Matching semantics
# ---------------------------------------------------------------------------


def test_matches_without_a_filter() -> None:
    assert filters.matches(None, "anything")


def test_matches_by_name_and_code_case_insensitively() -> None:
    wanted = filters.normalize(["borculo", "33439946"])
    assert filters.matches(wanted, "Borculo", "0022")
    assert filters.matches(wanted, "Goes", "33439946")
    assert not filters.matches(wanted, "Arnhem", "0021")


def test_matches_ignores_leading_zeros_and_whitespace() -> None:
    assert filters.matches(filters.normalize(["  22 "]), "Borculo", "0022")
    assert filters.matches(filters.normalize(["0022"]), "Borculo", "22")


def test_normalize_treats_an_empty_filter_as_absent() -> None:
    assert filters.normalize(None) is None
    assert filters.normalize([]) is None
    assert filters.normalize(["", "  "]) is None


def test_describe_names_both_filters() -> None:
    assert filters.describe({"4"}, {"Tiel"}) == "--kantoor Tiel / --invnr 4"
    assert filters.describe({"4"}, None) == "--invnr 4"


# ---------------------------------------------------------------------------
# --kantoor selects a kantoor, by either identifier
# ---------------------------------------------------------------------------


def test_kantoor_filter_by_name(h) -> None:
    h.mod.main(kantoren={h.name_of(h.first)})

    assert h.discovered == [h.first]
    assert h.downloaded_invnrs == set(h.invnrs_of(h.first))


def test_kantoor_filter_by_code_or_minr(h) -> None:
    h.mod.main(kantoren={h.second})

    assert h.discovered == [h.second]
    assert h.downloaded_invnrs == set(h.invnrs_of(h.second))


def test_kantoor_filter_is_case_insensitive(h, tmp_path) -> None:
    h.mod.main(kantoren={h.name_of(h.second).upper()})
    assert h.discovered == [h.second]

    # A completed kantoor is recorded in done.txt; drop it so the second run
    # exercises the filter rather than the resume marker.
    _done_file(h.mod, tmp_path).unlink()
    h.discovered.clear()
    h.downloads.clear()
    h.mod.main(kantoren={h.name_of(h.second).lower()})
    assert h.discovered == [h.second]


def test_unmatched_kantoor_is_skipped_before_discovery(h) -> None:
    """The point of the flag: no discovery pass for a kantoor we do not want."""
    h.mod.main(kantoren={h.name_of(h.first)})
    assert h.second not in h.discovered


def test_kantoor_matching_nothing_warns(h, capsys) -> None:
    h.mod.main(kantoren={"Nergenshuizen"})

    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "Nergenshuizen" in out
    assert h.discovered == []
    assert h.downloads == []


def test_kantoor_and_invnr_combine(h) -> None:
    unit = h.first
    wanted = h.invnrs_of(unit)[0]
    h.mod.main(kantoren={h.name_of(unit)}, invnrs={str(wanted)})

    assert h.discovered == [unit]
    assert h.downloaded_invnrs == {wanted}


# ---------------------------------------------------------------------------
# done.txt (issue #22) under --kantoor
# ---------------------------------------------------------------------------


def test_kantoor_only_run_records_the_kantoren_it_finished(h, tmp_path) -> None:
    """--kantoor is no finer than done.txt, so what it finished may be marked."""
    h.mod.main(kantoren={h.name_of(h.first)})

    done = _done_file(h.mod, tmp_path)
    assert done.exists()
    assert set(done.read_text().split()) == {h.first}

    # …and the kantoor that was never visited is still pending.
    h.downloads.clear()
    h.mod.main()
    assert h.downloaded_invnrs == set(h.invnrs_of(h.second))


def test_kantoor_with_invnr_does_not_record_done(h, tmp_path) -> None:
    """--invnr is finer than done.txt, whether or not --kantoor is also set."""
    h.mod.main(kantoren={h.name_of(h.first)}, invnrs={str(h.invnrs_of(h.first)[0])})

    done = _done_file(h.mod, tmp_path)
    assert not done.exists() or done.read_text().strip() == ""


# ---------------------------------------------------------------------------
# A warm cache rules kantoren out without --kantoor being passed
# ---------------------------------------------------------------------------


def test_warm_cache_skips_kantoren_that_cannot_hold_the_invnr(h) -> None:
    if h.name not in CACHE_SKIPPERS:
        pytest.skip(f"{h.name} has no kantoor-level inventory cache")

    h.warm_cache(h.second)  # says: this kantoor holds only the other invnrs
    h.mod.main(invnrs={str(h.invnrs_of(h.first)[0])})

    assert h.second not in h.discovered
    assert h.downloaded_invnrs == {h.invnrs_of(h.first)[0]}


def test_cold_cache_never_skips_a_kantoor(h) -> None:
    """A missing cache is not evidence of absence -- discovery must still run."""
    h.mod.main(invnrs={str(h.invnrs_of(h.first)[0])})

    assert set(h.discovered) == set(h.units)


def test_warm_cache_for_the_matching_kantoor_does_not_skip_it(h) -> None:
    if h.name not in CACHE_SKIPPERS:
        pytest.skip(f"{h.name} has no kantoor-level inventory cache")

    wanted = h.invnrs_of(h.first)[0]
    h.warm_cache(h.first)
    h.mod.main(invnrs={str(wanted)})

    assert h.first in h.discovered
    assert h.downloaded_invnrs == {wanted}


# ---------------------------------------------------------------------------
# The register-driven (Memorix) archives, where the kantoor is a column of the
# one register listing rather than a branch of a tree.
# ---------------------------------------------------------------------------


@pytest.fixture
def offline(monkeypatch, tmp_path):
    """Run a Memorix pipeline with no HTTP at all beyond a stubbed listing."""
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(None)

    def install(mod, registers):
        monkeypatch.setattr(mod, "_session", lambda: object())
        monkeypatch.setattr(download.time, "sleep", lambda *_: None)
        # Deeds, persons and assets are only ever paged for a register that
        # survived the filters, so an empty answer is enough here.
        monkeypatch.setattr(
            mod,
            "_paginate",
            lambda session, path, fq, kind: registers if path == "/register" else [],
        )
        if hasattr(mod, "_collect_registers"):
            monkeypatch.setattr(mod, "_collect_registers", lambda session: registers)

    return install


def _memorix_register(reg_id: str, invnr: str, **metadata) -> dict:
    return {"id": reg_id, "metadata": {"inventarisnummer": invnr, **metadata}}


def test_drentsarchief_filters_registers_by_gemeente(offline, capsys) -> None:
    from memories_crawl import drentsarchief

    offline(
        drentsarchief,
        [
            _memorix_register("r1", "1", gemeente="Assen", naam="Successiememorie Assen"),
            _memorix_register("r2", "31", gemeente="Coevorden", naam="Successiememorie Coevorden"),
        ],
    )
    drentsarchief.main(kantoren={"coevorden"})

    out = capsys.readouterr().out
    assert "Coevorden inv 31" in out
    assert "Assen inv" not in out


def test_drentsarchief_kantoor_matching_nothing_warns(offline, capsys) -> None:
    from memories_crawl import drentsarchief

    offline(drentsarchief, [_memorix_register("r1", "1", gemeente="Assen", naam="Assen")])
    drentsarchief.main(kantoren={"Nergenshuizen"})

    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "Nergenshuizen" in out


def test_bhic_filters_registers_by_gemeente_or_code(offline, capsys) -> None:
    from memories_crawl import bhic

    registers = [
        _memorix_register("r1", "1", gemeente="Eindhoven", naam="MvS", code="036.03.07"),
        _memorix_register("r2", "2", gemeente="Boxtel", naam="MvS", code="036.03.04"),
    ]
    offline(bhic, registers)
    bhic.main(kantoren={"036.03.04"})

    out = capsys.readouterr().out
    assert "Boxtel deel 2" in out
    assert "Eindhoven deel" not in out


def test_friesland_filters_registers_by_kantoor(offline, capsys) -> None:
    from memories_crawl import friesland

    registers = [
        _memorix_register("r1", "1", naam="Memories kantoor Sneek"),
        _memorix_register("r2", "2", naam="Memories kantoor Leeuwarden"),
    ]
    offline(friesland, registers)
    friesland.main(kantoren={"SNEEK"})

    out = capsys.readouterr().out
    assert "Sneek invnr 1" in out
    assert "Leeuwarden invnr" not in out
