"""Tests for the cached archive-level inventory listing (issue #25).

Two layers: the generic :mod:`memories_crawl.regcache` helper, and the four
API-backed pipelines wired to it.  Everything runs offline -- the network calls
and the Memorix pagination are replaced by fakes, in the style of
``test_invnr_filter.py``.

The properties that matter most are the ones where being wrong is expensive:
a cache that is stale, damaged, or written for a different query must cause a
re-collection rather than a silently short inventory.
"""

from __future__ import annotations

import json

import pytest

from memories_crawl import bhic, drentsarchief, friesland, nationaalarchief, paths, regcache

KEY = 'search_s_type_title:"memorie van successie"'


@pytest.fixture(autouse=True)
def _out_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    paths.set_out_dir(tmp_path / "scans")
    yield
    paths.set_out_dir(None)


class Counter:
    """A ``collect`` callable that records how often it ran."""

    def __init__(self, items=None, exc: Exception | None = None) -> None:
        self.items = [{"id": "a"}] if items is None else items
        self.exc = exc
        self.calls = 0

    def __call__(self) -> list:
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.items


def _load(collect, **kw):
    return regcache.load_or_collect("bhic", "registers.json", collect, key=KEY, **kw)


# ── regcache ────────────────────────────────────────────────────────────────


def test_a_fresh_cache_is_written_and_reused() -> None:
    collect = Counter()
    assert _load(collect) == [{"id": "a"}]
    assert _load(collect) == [{"id": "a"}]
    assert collect.calls == 1

    cache = paths.cache_dir("bhic") / "registers.json"
    assert cache.exists()
    # The cache lives beside the other caches, not among the images.
    assert paths.archive_dir("bhic") not in cache.parents


def test_an_expired_cache_triggers_re_collection() -> None:
    collect = Counter()
    _load(collect)

    cache = paths.cache_dir("bhic") / "registers.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["collected_at"] -= regcache.TTL_SECONDS + 60
    cache.write_text(json.dumps(payload), encoding="utf-8")

    _load(collect)
    assert collect.calls == 2


def test_a_cache_timestamped_in_the_future_is_not_trusted() -> None:
    """A moved clock re-collects rather than serving an entry that never ages."""
    collect = Counter()
    _load(collect)

    cache = paths.cache_dir("bhic") / "registers.json"
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["collected_at"] += 10 * regcache.TTL_SECONDS
    cache.write_text(json.dumps(payload), encoding="utf-8")

    _load(collect)
    assert collect.calls == 2


def test_refresh_forces_re_collection_and_rewrites() -> None:
    collect = Counter()
    _load(collect)
    assert _load(Counter([{"id": "b"}]), refresh=True) == [{"id": "b"}]
    # …and the fresh answer replaced the old one.
    assert _load(collect) == [{"id": "b"}]
    assert collect.calls == 1


def test_a_legacy_located_cache_is_honoured() -> None:
    """A cache left by an older layout must not be silently re-paid for."""
    legacy = paths.archive_dir("bhic") / "registers.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(
        json.dumps(
            {
                "format": 1,
                "key": KEY,
                "collected_at": regcache.time.time(),
                "items": [{"id": "legacy"}],
            }
        ),
        encoding="utf-8",
    )

    collect = Counter()
    assert _load(collect) == [{"id": "legacy"}]
    assert collect.calls == 0


@pytest.mark.parametrize(
    "body",
    [
        "not json at all",
        "[]",  # an old-style bare list, i.e. an unrecognised layout
        json.dumps({"format": 1, "key": KEY, "collected_at": 0, "items": [{"id": "a"}]}),
        json.dumps({"format": 99, "key": KEY, "collected_at": 1e18, "items": [{"id": "a"}]}),
        json.dumps({"format": 1, "key": KEY, "collected_at": None, "items": [{"id": "a"}]}),
        json.dumps({"format": 1, "key": KEY, "items": [{"id": "a"}]}),
    ],
)
def test_a_damaged_cache_falls_back_to_collection(body: str) -> None:
    cache = paths.cache_file("bhic", "registers.json")
    cache.write_text(body, encoding="utf-8")

    collect = Counter()
    assert _load(collect) == [{"id": "a"}]
    assert collect.calls == 1


def test_a_cache_written_for_another_query_is_ignored() -> None:
    collect = Counter()
    regcache.load_or_collect("bhic", "registers.json", collect, key="some other filter")
    assert _load(collect) == [{"id": "a"}]
    assert collect.calls == 2


def test_an_empty_cache_is_a_miss_not_an_empty_inventory() -> None:
    """Far worse than a slow run: a run that thinks the archive is empty."""
    cache = paths.cache_file("bhic", "registers.json")
    cache.write_text(
        json.dumps({"format": 1, "key": KEY, "collected_at": regcache.time.time(), "items": []}),
        encoding="utf-8",
    )

    collect = Counter()
    assert _load(collect) == [{"id": "a"}]
    assert collect.calls == 1


def test_an_empty_result_is_never_cached() -> None:
    """A transient API hiccup must not pin an empty listing for a month."""
    empty = Counter(items=[])
    assert _load(empty) == []
    assert not (paths.cache_dir("bhic") / "registers.json").exists()


def test_a_failing_collect_propagates_and_leaves_the_cache_alone() -> None:
    _load(Counter([{"id": "good"}]))
    with pytest.raises(RuntimeError):
        _load(Counter(exc=RuntimeError("boom")), refresh=True)
    assert _load(Counter([{"id": "other"}])) == [{"id": "good"}]


# ── Memorix pipelines: cache + server-side --invnr ──────────────────────────

MEMORIX = [
    (bhic, 'search_s_type_title:"memorie van successie"'),
    (friesland, 'search_s_type_title:"Memories van successie"'),
    (drentsarchief, 'search_s_brontype:"Memorie van Successie"'),
]


def _register(invnr: str) -> dict:
    return {
        "id": f"reg-{invnr}",
        "metadata": {"inventarisnummer": invnr, "gemeente": "Assen", "naam": f"deel {invnr}"},
    }


class FakeApi:
    """Stands in for ``_paginate``; records every filter query it was given."""

    def __init__(self, registers: list[dict]) -> None:
        self.registers = registers
        self.queries: list[str] = []

    def __call__(self, session, path, fq, key):
        self.queries.append(fq)
        return list(self.registers)


@pytest.fixture(params=MEMORIX, ids=[m.ARCHIVE for m, _ in MEMORIX])
def memorix(request, monkeypatch):
    mod, base_fq = request.param
    api = FakeApi([_register("1"), _register("2")])
    monkeypatch.setattr(mod, "_paginate", api)
    monkeypatch.setattr(mod, "_session", lambda: None)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
    api.module = mod
    api.base_fq = base_fq
    return api


def _listing(mod, **kw) -> list[dict]:
    collect = mod._collect_registers if mod is drentsarchief else mod._load_registers
    return collect(None, **kw)


def test_memorix_caches_the_unfiltered_listing(memorix) -> None:
    mod = memorix.module
    assert len(_listing(mod)) == 2
    assert len(_listing(mod)) == 2
    assert memorix.queries == [memorix.base_fq]
    assert (paths.cache_dir(mod.ARCHIVE) / "registers.json").exists()


def test_memorix_refresh_cache_re_collects(memorix) -> None:
    mod = memorix.module
    _listing(mod)
    _listing(mod, refresh_cache=True)
    assert memorix.queries == [memorix.base_fq, memorix.base_fq]


def test_memorix_pushes_the_invnr_filter_into_the_query(memorix) -> None:
    """One targeted request instead of the whole listing -- and no cache."""
    mod = memorix.module
    memorix.registers = [_register("2")]
    assert [r["metadata"]["inventarisnummer"] for r in _listing(mod, invnrs={"2"})] == ["2"]
    assert memorix.queries == [f'{memorix.base_fq} AND search_s_inventarisnummer:("2")']
    assert not (paths.cache_dir(mod.ARCHIVE) / "registers.json").exists()


def test_memorix_invnr_run_ignores_and_preserves_the_cache(memorix) -> None:
    mod = memorix.module
    _listing(mod)  # warm it
    memorix.registers = [_register("2")]
    _listing(mod, invnrs={"2"})
    assert memorix.queries[-1].endswith('search_s_inventarisnummer:("2")')
    # The warm cache is still the full listing, not the one-register answer.
    payload = json.loads(
        (paths.cache_dir(mod.ARCHIVE) / "registers.json").read_text(encoding="utf-8")
    )
    assert len(payload["items"]) == 2


def test_memorix_ors_several_invnrs_in_one_request(memorix) -> None:
    mod = memorix.module
    _listing(mod, invnrs={"12", "1903-1906"})
    assert memorix.queries == [
        f'{memorix.base_fq} AND search_s_inventarisnummer:("12" OR "1903-1906")'
    ]


def test_memorix_escapes_quotes_in_an_invnr(memorix) -> None:
    mod = memorix.module
    _listing(mod, invnrs={'a"b'})
    assert memorix.queries == [f'{memorix.base_fq} AND search_s_inventarisnummer:("a\\"b")']


def test_drentsarchief_applies_the_tafel_rule_to_a_warm_cache(monkeypatch) -> None:
    """The stored listing is raw, so the exclusion rule is never baked in."""
    tafel = {"id": "reg-t", "metadata": {"inventarisnummer": "9", "naam": "Tafel V-bis Assen"}}
    api = FakeApi([_register("1"), tafel])
    monkeypatch.setattr(drentsarchief, "_paginate", api)

    assert len(drentsarchief._collect_registers(None)) == 1
    assert len(drentsarchief._collect_registers(None)) == 1
    payload = json.loads(
        (paths.cache_dir("drentsarchief") / "registers.json").read_text(encoding="utf-8")
    )
    assert len(payload["items"]) == 2


# ── Nationaal Archief: cache the EAD-derived listing ────────────────────────


class FakeEad:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.calls = 0

    def __call__(self, session):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return [2276, 2277]


def test_nationaalarchief_caches_the_parsed_inventory(monkeypatch) -> None:
    ead = FakeEad()
    monkeypatch.setattr(nationaalarchief, "_collect_inventory_numbers", ead)

    assert nationaalarchief._fetch_inventory_numbers(None) == [2276, 2277]
    assert nationaalarchief._fetch_inventory_numbers(None) == [2276, 2277]
    assert ead.calls == 1

    assert nationaalarchief._fetch_inventory_numbers(None, refresh_cache=True) == [2276, 2277]
    assert ead.calls == 2


def test_nationaalarchief_never_caches_the_fallback_list(monkeypatch, capsys) -> None:
    """A transient outage must not pin the hardcoded list for 30 days."""
    ead = FakeEad(exc=RuntimeError("503"))
    monkeypatch.setattr(nationaalarchief, "_collect_inventory_numbers", ead)

    assert nationaalarchief._fetch_inventory_numbers(None) == nationaalarchief._fallback_invnrs()
    assert "using fallback list" in capsys.readouterr().out
    assert not (paths.cache_dir("nationaalarchief") / "inventory.json").exists()

    monkeypatch.setattr(nationaalarchief, "_collect_inventory_numbers", FakeEad())
    assert nationaalarchief._fetch_inventory_numbers(None) == [2276, 2277]
