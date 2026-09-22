# CLAUDE.md – Memories van Successie Pipeline

## What this project does

Downloads all surviving *Memories van Successie* (Dutch succession/inheritance registers, 1806–1927) from ten regional Dutch archives. Each scan is saved alongside a `metadata.json` sidecar.

## How to run

```bash
uv run memories-crawl friesland          # Friesland (Tresoar / AlleFriezen, Memorix API)
uv run memories-crawl nationaalarchief   # Zuid-Holland (Nationaal Archief 3.06.05)
uv run memories-crawl drentsarchief      # Drenthe (Memorix API)
uv run memories-crawl bhic               # Noord-Brabant (BHIC Memorix API)
uv run memories-crawl overijssel         # Overijssel (HCO) – requires Playwright
uv run memories-crawl utrechtsarchief    # Utrecht (Het Utrechts Archief) – requires Playwright
uv run memories-crawl limburg            # Limburg (RHCL, archieven.nl MAIS) – requires Playwright
uv run memories-crawl noordholland       # Noord-Holland (Noord-Hollands Archief) – requires Playwright
uv run memories-crawl zeeland            # Zeeland (Zeeuws Archief) – requires Playwright
uv run memories-crawl gelderland         # Gelderland (Gelders Archief) – requires Playwright
uv run memories-crawl all
```

## Output root

Everything is written below `--out-dir` (default `./scans`, overridable with
`$MEMORIES_CRAWL_OUT_DIR`), resolved by `src/memories_crawl/paths.py`:

* scans → `<out-dir>/{archive}/…`
* caches that make reruns cheap (inventory listings, Playwright token harvests,
  `done.txt` resume markers, progress CSVs) → `<out-dir>/.cache/{archive}/`

Pipelines must not hardcode `Path("scans/…")`. Use `paths.archive_dir(ARCHIVE)`
for scan directories and `paths.cache_file(ARCHIVE, name)` for caches; the latter
keeps using a cache that already exists at its pre-0.3 location
(`<out-dir>/{archive}/{name}`, or the working directory for the progress CSVs), so
an upgrade never silently repeats a token harvest.

Every pipeline's `main()` takes `out_dir: Path | None = None` and calls
`paths.set_out_dir(out_dir)` when it is given; the CLI sets it once up front.

## Concurrent image downloads

Image fetches — and only image fetches — go through the shared thread pool in
`src/memories_crawl/download.py`. Every pipeline's `main()` takes
`workers: int = download.DEFAULT_WORKERS` (4), which the CLI fills from
`--workers`; `--workers 1` restores the strictly sequential pre-0.3 path (same
thread as the caller, jobs walked in order).

A pipeline builds one `download.Downloader(_download_file, workers=workers,
rate=DOWNLOAD_RATE, session_factory=_session)` per run, then hands each batch of
`download.Job(url, dest, key=None)` to `downloader.run(jobs, on_result=…)` and
splits the returned `Counter` with `download.tally(counts)` →
`(downloaded, existing, missing)`. Rules:

* **Never share a `requests.Session` across threads** – it is not thread-safe.
  The pool builds one per worker thread from `session_factory`, so a pipeline
  needs a module-level `_session()` that sets its headers (User-Agent, Referer).
* `rate` is the requests/second ceiling shared by all workers, replacing the old
  fixed `time.sleep` between images. Pass the pace that sleep produced
  (`DOWNLOAD_RATE = 1 / 0.15` for the MAIS pipelines, `10.0` for Limburg,
  nothing for the Memorix/NA pipelines, which never slept between images).
* A 429 pauses **all** workers (`RateLimiter.penalize`, honouring `Retry-After`),
  because per-worker backoff would just let the other workers keep hammering.
* **Every pipeline's `_download_file` delegates to `download.fetch_file`** and
  does nothing else. Do not hand-roll a fetch: `fetch_file` owns the exists
  check, the retry rules, the atomic `.part` write and the status mapping, and
  takes the two things that genuinely differ per archive — `missing_statuses`
  (`(404,)`, or `(404, 202)` where a MAIS server answers 202 + an SVG
  placeholder for an untokened page) and `timeout`. It returns `failed` rather
  than raising, so one bad page never ends the register.
* Counters and the `on_result` callback run under the pool's lock, so pipeline
  bookkeeping need not be thread-safe itself.
* Keep discovery, the Playwright token harvest and metadata writes sequential.

## Run reporting (issue #19)

`src/memories_crawl/summary.py` holds the counting. Every pipeline's `main()`
returns a `RunSummary | None` (`None` only for a `--list-invnrs` pass, which
downloads nothing).

* `PageTally` folds one download outcome at a time via
  `tally.record(status, dest)`, where `status` is what `_download_file`
  returns (`downloaded` / `exists` / `missing` / `failed`). It is a plain value
  object with `+=`, so a concurrent download loop (issue #26) can keep a tally
  per thread and merge at the join — never mutate module-level counters.
  Bytes come from `dest.stat().st_size` of pages this run actually fetched;
  never multiply a page count by an assumed average (36× spread between
  archives).
* `tally.describe(n_pages)` renders the per-register progress line
  (`38 pages (38 new, 0 existing, 0 missing, 21.7 MB)`).
* `announce(pages, registers, where)` prints the up-front
  `→ about to download N pages across M registers in …` line. Call it only
  where the page list is genuinely known in advance — the MAIS pipelines know
  it after the token harvest; Limburg counts its token caches first.
* `RunSummary` carries `units` (kantoren, gemeenten or archive codes;
  `unit_name=None` for the Nationaal Archief, which has no such layer),
  `registers`, optional `records` (deeds/persons, where the archive indexes
  them), and the `pages` tally. `summary.report()` prints the end-of-run block;
  each pipeline calls it at the point where it used to print `Done (X).`.
* Fold each register's tally into the summary **as that register finishes**,
  not once per kantoor, so a crash midway keeps the partial totals.
* `RunSummary` registers itself with the collector `cli.py` installs around
  each pipeline call (`summary.collect()`), which is how the CLI can print a
  partial summary for a pipeline that raised and still include it in the
  `all` grand total (`summary.grand_total`). The CLI prints a summary only if
  the pipeline did not already report it, and prints none at all for
  `--list-invnrs`.

## Archive-level inventory listing (issue #25)

The four API-backed pipelines have to know which registers exist before they
can do anything else, and that enumeration used to be re-paid on every
invocation, including runs that download nothing. `src/memories_crawl/regcache.py`
now stores the listing under `<out-dir>/.cache/{archive}/` for **30 days**:

| Archive | Cache file | What it holds |
|---|---|---|
| `bhic` | `registers.json` | the raw `/register` documents (1,896) |
| `friesland` | `registers.json` | the raw `/register` documents (1,107) |
| `drentsarchief` | `registers.json` | the raw `/register` documents (557) |
| `nationaalarchief` | `inventory.json` | the invnrs parsed out of the EAD XML |

Rules the cache follows, because a listing that silently loses registers is
much worse than a slow one:

* keyed by the query that produced it (`REGISTER_FILTER` / `EAD_XML_URL`), so a
  changed filter re-collects instead of serving the old answer;
* anything unreadable, unrecognised, expired, future-dated or **empty** counts
  as a miss and is re-collected — never read as "this archive is empty";
* an empty result is never written, so a transient API hiccup cannot pin an
  empty inventory in place for a month;
* the Nationaal Archief fallback invnr list is never cached, for the same
  reason;
* Drenthe stores the *raw* register documents and applies `_is_tafel()` after
  loading, so the Tafel V-bis rule is never baked into a cache file.

`--refresh-cache` forces re-collection for these four pipelines. It is not
passed to the Playwright ones, which have their own inventory/token caches.

**Server-side `--invnr` is preferred over the cache.** Memorix indexes the
inventarisnummer as the exact-match field `search_s_inventarisnummer`, so a
`--invnr` run on `bhic`, `friesland` or `drentsarchief` issues one targeted
request (`… AND search_s_inventarisnummer:("84" OR "1903-1906")`) instead of
walking the listing, and neither reads nor writes the cache. Verified against
all three tenants to return exactly what the old client-side filter kept,
including non-numeric numbers (`1903-1906`, `15.2`, `6004a`) and with no
prefix bleed (`1` does not match `12`). The Nationaal Archief has no such
query — its inventory is one EAD XML download — so it keeps filtering client
side over the cached list.

## File map

| File | Purpose |
|---|---|
| `src/memories_crawl/cli.py` | CLI dispatcher |
| `src/memories_crawl/paths.py` | Output root, per-archive scan dirs and cache paths |
| `src/memories_crawl/listing.py` | `--list-invnrs` count *and* date vocabulary: the `?` unknown marker, `has_scans()`, the "nothing to download" suffix, `parse_years()`/`span()`/`YEAR_FIELDS` |
| `src/memories_crawl/download.py` | Bounded thread pool, shared rate limiter, global 429 backoff, and `fetch_file` (per-image retry + atomic write) |

| `src/memories_crawl/summary.py` | Download counters, per-archive summary, cross-archive total |

| `src/memories_crawl/regcache.py` | TTL cache for the archive-level inventory listing |
| `src/memories_crawl/filters.py` | Case-insensitive name/code matching for `--kantoor` |
| `src/memories_crawl/nationaalarchief.py` | Zuid-Holland: scrape viewer pages, download via UUID |
| `src/memories_crawl/drentsarchief.py` | Drenthe: Memorix REST API, deed→asset chain |
| `src/memories_crawl/bhic.py` | Noord-Brabant (BHIC): Memorix REST API, register→asset chain |
| `src/memories_crawl/overijssel.py` | Overijssel: Playwright-based MAIS token extraction |
| `src/memories_crawl/utrechtsarchief.py` | Utrecht: Playwright-based MAIS stk3 inline strip extraction |
| `src/memories_crawl/limburg.py` | Limburg (RHCL): Playwright on archieven.nl, strip Volgende-step |
| `src/memories_crawl/noordholland.py` | Noord-Holland: Playwright-based MAIS stk3 inline strip extraction |
| `src/memories_crawl/zeeland.py` | Zeeland: Playwright-based MAIS hybrid (inv3 discovery + inv2 strip harvest) |
| `src/memories_crawl/friesland.py` | Friesland: Tresoar / AlleFriezen Memorix REST API, register→deed→person chain |
| `src/memories_crawl/gelderland.py` | Gelderland: Playwright-based MAIS, one micode per kantoor (21 codes), strip auto-loads on inv2 minr |

## Scan availability in `--list-invnrs`

Every pipeline's listing carries a count column next to the inventarisnummer, in
both the printed table and the `--csv` output. Counts are `int` internally and
`None` when the archive would charge an extra request per register to produce
them; `src/memories_crawl/listing.py` renders `None` as `?`.

**A `?` must never become a `0`.** `listing.has_scans(count)` (`count != 0`) is the
only filter `--only-digitized` uses, so an unmeasured register is kept, not hidden.
Beware the partial token caches in `gelderland`/`zeeland`: an invnr missing from a
*complete* cache really has no pages, but one missing from a `_partial.json` was
simply never harvested — `_cached_page_counts()` returns a `complete` flag for
exactly this reason.

| Archive | column(s) | free source | `--count-scans` cost |
|---|---|---|---|
| friesland | `n_persons`, `n_with_scans` | `register["asset"]` non-empty ⇒ digitized (verified on 14 registers, 0 disagreements) | a `/person` walk joined to a `/deed` walk per register |
| bhic | `n_scans` | `register["asset"]` non-empty | 1 `/asset?rows=1` per register (`pagination.total`) |
| drentsarchief | `n_scans` | `register["asset"]` non-empty (553 of 557) | 1 `/asset?rows=1` per register — `/asset` is queryable by `register_id` even though assets hang off the deeds |
| nationaalarchief | `kantoor`, `n_scans` | `did/dao` METS link in the EAD XML: 3,237 of 3,958 items digitized (spot-checked, no-`dao` ⇒ 0 scans) | 1 viewer page fetch per invnr |
| overijssel, utrechtsarchief, noordholland | `pages` | exact — the token harvest is mandatory anyway | n/a |
| limburg, zeeland, gelderland | `pages` | warm token cache, else `?` | full Playwright token harvest |

Memorix pagination trick: `rows=1` still reports `metadata.pagination.total`, so an
exact count is one small response rather than a full paging walk. Do not page the
whole result set just to `len()` it.

Every pipeline's `main()` takes `only_digitized: bool = False` and
`count_scans: bool = False`; the CLI exposes them as `--only-digitized` and
`--count-scans`. The period columns that sit next to these counts are described
under *Period in `--list-invnrs`* below.

Download runs print one summary line per register so a zero-yield register is
visible in the log rather than inferred from the filesystem, e.g.
`Lemmer 12038: 79 persons, 0 with scans — nothing to download`. The suffix keys
off what the register *holds*, not off what the current run fetched: a fully
resumed register downloads nothing and must not be labelled empty.

The Nationaal Archief entry cache uses `EAD_XML_URL + "#entries-v2"`, so legacy
integer-list caches (and the `v1` rows written before the period columns) are
re-collected. Availability filters apply after inventory and kantoor
selection. Friesland records `partial` when `--only-digitized` skips person
sidecars within a register, allowing a later full run to finish them.

## Period in `--list-invnrs` (issue #38)

A count answers "is there anything here?"; the *period* answers "is it the
right thing?". Without it a period-limited plan can only rank inventarisnummers
against a date span, which is a guess: one such plan put **80.6% of the deaths
it fetched outside the target years** and 58 GB of scans had to be deleted.

Every pipeline's listing therefore carries `year_from` and `year_to` (the last
two entries of its `LIST_FIELDS`, via `listing.YEAR_FIELDS`), and the Memorix
and Nationaal Archief tables also print a human `period` column
(`1818`, `1837-1838`, `?`). `listing.parse_years(text, invnr)` turns a datering
into the pair; `listing.span(values)` folds years or ISO dates into one.

**An unparsed date is `?`, never a year.** `parse_years` strips the leading
inventarisnummer from a MAIS description before reading it (`"1888  1901 eerste
kwartaal"` is 1901, and `"1888"` alone is no year at all), and only accepts
1795–1935, so memorienummers and page counts cannot become periods. Three of
the 3,958 Nationaal Archief items carry a typo (`"l872 okt. - dec."`) and
report `?`; do not "fix" them by guessing.

| Archive | where the period comes from | cost |
|---|---|---|
| nationaalarchief | `<unitdate normal="1818-01/1818-03">` in the EAD, else the `unittitle` text | free — 3,955 of 3,958 items, from the XML already downloaded |
| friesland | `register["metadata"]["periode"]` (`[1837, 1838]`) | free — 1,106 of 1,107 registers |
| gelderland, zeeland, utrechtsarchief, noordholland, overijssel | the MAIS tree link text (`"12  1843 jan.-juni"`) | free — the discovery/token pass reads it anyway |
| limburg | the archive's own `datering` (`"Amby, 1818-1828"`), title as fallback | free |
| drentsarchief, bhic | `min`/`max` of the persons' `datum_overlijden` | one `/person` walk per register — **`--dates` only** |

`--dates` is passed to `cli.DATE_QUERY_PIPELINES` (`drentsarchief`, `bhic`)
only, the way `--refresh-cache` is passed to `CACHED_LISTING_PIPELINES`;
everywhere else the period is free, so it is always reported and the flag has
nothing to switch on. Those two `main()`s take `dates: bool = False`.

Where a register sits inside a dated section (noordholland's `period` column,
gelderland's period sub-sections), the register's own years are **not**
inherited from the section: the section spans years the single register does
not, and a wider bound presented as the register's period misleads exactly like
a guessed one. The section text stays in its own column.

The Overijssel token cache now stores `inv_text` per page. A cache harvested
before this change has none, so its listing reports an empty description and a
`?` period until it is re-harvested — never a date inferred from elsewhere.

## Filters: `--invnr` and `--kantoor`

`--invnr` (repeatable) narrows the download to specific inventarisnummers.
`--kantoor` (repeatable) narrows the *search* to specific kantoren, and is
applied **before** any discovery or token-harvest work — an inventarisnummer
belongs to exactly one kantoor, so without it a single-register fetch walks all
21 Gelderland kantoren to find one (issue #24).

`src/memories_crawl/filters.py` does the matching: case-insensitive, whitespace
trimmed, leading zeros ignored for numeric identifiers (`--kantoor 22` finds
Gelderland's `0022`). A value matches when it equals *any* label the pipeline
offers for that kantoor — the name from the `kantoor` column of `--list-invnrs`
plus, where the archive has one, the archief-code (gelderland `0026`, bhic
`036.03.04`, limburg `07.D03`), the micode (utrechtsarchief `337-2`), the minr
(overijssel, zeeland, noordholland) or the archiefnummer (drentsarchief).
`nationaalarchief` has no kantoor subdivision; the CLI reports the flag as
ignored rather than pretending to apply it.

Every pipeline's `main()` takes `kantoren: set[str] | None = None` (the raw
user strings — pass them to `filters.normalize()` once at the top) except
`nationaalarchief`.

**Cache-driven skipping.** Where a cache can *prove* a kantoor holds none of the
requested invnrs, the kantoor is skipped before any network work even without
`--kantoor`: gelderland and limburg consult `inventory_{code}.json`, overijssel
and zeeland the kantoor's *complete* token cache (a partial harvest is not
evidence). A missing cache must always fall through to normal discovery —
absence of evidence is never evidence of absence.

**Warnings.** A filter that matches nothing across the whole archive prints a
`WARNING` line, so `--invnr 99999` is distinguishable from a successful no-op.
A kantoor already recorded in `done.txt` counts as matched.

**`done.txt` interaction.** The markers are keyed by kantoor, which is coarser
than `--invnr` but exactly as coarse as `--kantoor`: a `--kantoor` run may
record the kantoren it fully processed, a run with `--invnr` set may not.
`tests/test_invnr_filter.py` (issue #22) and `tests/test_kantoor_filter.py`
(issue #24) are the regression tests for that rule.

## Exclusion rule

**Always exclude Tafel V-bis.** In all parsers and filters, skip any record whose SourceType contains "tafel" or "v-bis" (case-insensitive). The Nationaal Archief Tafel V-bis items are in a different inventory section (outside 2276–2357) and are excluded by range.

---

## Overijssel (HCO) – MAIS token extraction

First-time setup: `uv sync && playwright install chromium`

The HCO uses a MAIS Internet viewer. Each scan page requires unique per-page tokens
(`miahd`, `rdt`, `open`). The implementation in `python/overijssel.py`:

1. Opens the MAIS inv3 page in headless Chromium to establish the session.
2. Clicks each invnr-item stk3 link via `mi_inv3_toggle_stk(...)`.
3. Harvests `img[src*="/fonc-hco/"]` from the DOM to get per-page tokens.

**Image URL format:**
```
https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4/{invnr}/
    NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg
    ?miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
```

**Kantoor minr values** (verified April 2026):

| Kantoor    | minr    |
|------------|---------|
| Almelo     | 2227676 |
| Deventer   | 2227950 |
| Enschede   | 2228207 |
| Goor       | 2228335 |
| Kampen     | 2228502 |
| Ommen      | 2228649 |
| Raalte     | 2228752 |
| Steenwijk  | 2228889 |
| Vollenhove | 2228980 |
| Zwolle     | 2229046 |

---

## Pipeline status (verified 2026-04-24)

Each pipeline was live-tested against the real APIs and servers.

| Pipeline | API/Server | End-to-end | Notes |
|---|---|---|---|
| **friesland** | ✅ | ⚠️ not yet tested | Tresoar / AlleFriezen Memorix REST API. 1,107 registers, ~238k persons. Deed-level assets with .jp2 downloads. Person→deed join via deed_id. Output: <out-dir>/friesland/{kantoor}/{invnr}/{person}/. |
| **nationaalarchief** | ✅ | ✅ | 70 scans downloaded from invnr 2276 in 60s (174 MB). EAD XML parses correctly, drupal-settings-json extraction works, `service.archief.nl` download works. |
| **drentsarchief** | ✅ | ✅ verified | Register-driven: one `/register` request (557 registers) resolves the whole inventory, then deeds/persons are paged per register. `--list-invnrs` runs in ~1 s; `--invnr` touches only matching registers. Smoke-tested 2026-09-16: Coevorden invnr 1 → 176 deeds, ~6 MB/scan. |
| **overijssel** | ✅ | ⚠️ slow first run | Playwright + Chromium work. Almelo has 256 stk3 items → ~1825 pages of tokens; collecting tokens takes ~6 min per kantoor. Token results are cached in `<out-dir>/.cache/overijssel/tokens_minr_{minr}.json` — reruns skip Playwright entirely. |
| **utrechtsarchief** | ✅ | ⚠️ slow first run | Playwright + Chromium. Uses stk3 inline toggle (same approach as Overijssel). Amersfoort verified: 66,615 pages from 211 invnrs across 2 subsections (~12 min harvest). Token results cached per subsection — reruns skip Playwright. 11 kantoren configured. |
| **limburg** | ✅ | ✅ verified | archieven.nl MAIS (miadt=38, mivast=0). Two codes: 07.D03 (1818-1900, 111 digitized of 1,314, ~104k scans, by place) and 07.D08 (1901-1927, 42 digitized of 460, ~7k scans, by kantoor). End-to-end smoke-tested: invnr 1 (Amby) → 527 pages; invnr 491 (Gennep) → 207 pages. Inventory + tokens cached per code/invnr; reruns skip Playwright. Image format is `format=large` PNG (714×1024); see module docstring for trade-off vs. IIPSrv full-res JP2 path. |
| **noordholland** | ✅ | ⚠️ not yet tested | noord-hollandsarchief.nl MAIS (miadt=236, mivast=236, micode=178). Uses stk3 inline toggle (same approach as Overijssel/Utrecht). Kantoor sections discovered dynamically from inv2 tree. Tokens cached per section minr; reruns skip Playwright. Image server: preserve-nha.archieven.nl/mi-0/fonc-nha/178/. |
| **zeeland** | ✅ | ✅ verified | Zeeuws Archief MAIS (miadt=239, mivast=239, micode=398). Hybrid approach: inv3 tree for discovery (kantoor→sub-section→invnr with h_scan markers), inv2 minr pages for strip harvesting (auto-loads strip, force-load all chunks via mi_strip_store.populate()). Goes verified: 990 digitized invnrs of 1,109, invnr 1 → 327 pages, invnr 2 → 373 pages. Image server: preserve-zaf.archieven.nl/mi-239/fonc-zaf/398/. Downloads at `format=large` PNG (673×1024). Filenames include segment slug for uniqueness (e.g. `1-1_0001.jpg`). Tokens cached per kantoor in `tokens_minr_{minr}.json`. |
| **gelderland** | ✅ | ✅ verified | Gelders Archief MAIS (miadt=37, mivast=37). 21 kantoren, each with its own micode (0021–0037, 0092, 0221–0223). Per-kantoor pipeline: inv2 → pick "Register IV" minr (filter "Tafel VI / V-bis") → inv3 → swapinv-expand period sub-sections → collect leaf invnrs with `h_scan` markers and `^\d+\s` text. Per-invnr token harvest = navigate to inv2&minr=…, force-load strip via `mi_strip_store.populate()`, harvest `img[src*="fonc-gea"]`. Borculo (0022) verified end-to-end: 49 digitized invnrs, invnr 1 → 38 pages, full-size 1024×858 PNG (~570 KB/page). Image server: preserve2.archieven.nl/mi-37/fonc-gea/{code}/. Filenames `{invnr}-{page:04d}.jpg`. Inventory + tokens cached per code; reruns skip Playwright. |

**Setup reminder**: Chromium must be installed with `uv run playwright install chromium` (not bare `playwright install chromium`).

---

## Technical notes

### Nationaal Archief scan extraction

Scans are in a `<script data-drupal-selector="drupal-settings-json">` JSON blob. Parse `settings["viewer"]["response"]["scans"]`. Each scan has `{"id": UUID, "label": "NL-HaNA_...", "default": {"url": "https://service.archief.nl/api/file/v1/default/{UUID}"}}`. Download via `default.url`.

The invnrs parsed out of the EAD XML are cached in
`<out-dir>/.cache/nationaalarchief/inventory.json`, so the XML is downloaded at
most once a month. `_fallback_invnrs()` is deliberately never cached — see
*Archive-level inventory listing* above.

### Drents Archief API

```
Base: https://webservices.memorix.nl/genealogy
Key:  a85387a2-fdb2-44d0-8209-3635e59c537e
Register list: GET /register?q=*:*&fq=search_s_brontype:"Memorie van Successie"&rows=1000
Deeds:         GET /deed?fq=register_id:{register_id}&rows=1000&page=N
Persons:       GET /person?fq=register_id:{register_id}&rows=1000&page=N
Full image:    asset[].download  (e.g. https://images.memorix.nl/dre/download/fullsize/{uuid}.jpg)
```

557 registers, ~106,378 deeds (1 person per deed). Scans hang off the **deed**,
not the register — unlike BHIC. Register metadata is the only place that carries
`inventarisnummer` / `gemeente`: deed documents have no `register` key and no
`inventarisnummer` field, so the inventory must come from `/register`. Kantoren
are Assen, Coevorden, Emmen, Hoogeveen, Meppel across archiefnummers 0119.01,
.03, .05, .07, .08, .10; the same invnr can recur under different archiefnummers,
so `--invnr N` may select several registers.

`rows=1000` is accepted on all three endpoints, so the full register listing is a
single request. Do **not** re-introduce the old person-index walk
(`/person?q=*:*` over ~1,064 pages) — see issue #28.

The listing is cached in `<out-dir>/.cache/drentsarchief/registers.json`, and
`--invnr` is pushed into the query as `search_s_inventarisnummer` — see
*Archive-level inventory listing* above. The win is small here (the walk was
already one request), but it keeps the three Memorix pipelines identical.

### BHIC (Noord-Brabant) API

Same Memorix backend, **different tenant key**, and scans live at the **register**
level (one register = one bound book), not at the deed level.

```
Base: https://webservices.memorix.nl/genealogy
Key:  24c66d08-da4a-4d60-917f-5942681dcaa1
Register list: GET /register?q=*:*&fq=search_s_type_title:"memorie van successie"&rows=100&page=N
Assets:        GET /asset?fq=register_id:{register_id}&rows=100&page=N
Deeds:         GET /deed?fq=register_id:{register_id}&rows=100&page=N
Persons:       GET /person?fq=register_id:{register_id}&rows=100&page=N
Full image:    asset[].download  (https://images.memorix.nl/bhic/download/fullsize/{file_id}.jpg)
```

1,896 registers total. Code prefixes are `036.03.01..19` (Memories van successie,
kantoor X) plus `021.13` (Memories van successie Brabant). Tafel V-bis is not
indexed at BHIC, but `_is_tafel()` filters defensively just in case.

The `rows=100` walk is 19 requests, ~5.2 s of which most is the deliberate
`REQUEST_SLEEP`. It is cached in `<out-dir>/.cache/bhic/registers.json`, and an
`--invnr` run skips it entirely via `search_s_inventarisnummer` (~18 ms) — see
*Archive-level inventory listing* above.

### Friesland (Tresoar / AlleFriezen) – Memorix REST API

Tresoar's *Memories van Successie* are served via AlleFriezen, which runs the
same Memorix Genealogy REST API as Drenthe and BHIC.

```
Base: https://webservices.memorix.nl/genealogy
Key:  aa030ec4-12d0-4dc0-afaf-b65fd6128b39
Tenant: frl
Register list: GET /register?q=*:*&fq=search_s_type_title:"Memories van successie"&rows=100&page=N
Deeds:         GET /deed?fq=register_id:{register_id}&rows=100&page=N
Persons:       GET /person?fq=register_id:{register_id}&rows=100&page=N
Full image:    asset[].download → https://tresoar-images.memorix.nl/frl/download/fullsize/{path}.jp2
```

1,107 registers total, ~238,576 persons. Entity types: `mvs` (register),
`mvs_a` (deed/akte), `mvs_a_persoon` (person). One person per deed (the
"overledene"). Deeds embed their asset references directly (`has_assets: "deed"`,
`asset[].download`).

Tafel V-bis is not present in the Tresoar collection (0 results).

**Person metadata** includes `person_display_name`, `voornaam`, `tussenvoegsel`,
`geslachtsnaam`, `patroniem`, `datum_overlijden`, `plaats` (overlijdensplaats),
`plaats_wonen`, `geslacht`.

Deed metadata includes `nummer` (aktenummer), `plaats`, `diversen`
(free-text notes with filmnummer, estate details, family relations).

**Image format**: JPEG 2000 (`.jp2`). No format conversion is done;
convert with `magick mogrify -format jpg *.jp2` if needed.

```
Folder layout
─────────────
  <out-dir>/friesland/{kantoor}/{invnr}/{person_slug}/
      {NNNN}.jp2           – sequentially numbered scan pages
      metadata.json        – per-person info (name, date of death, …)
```

Kantoor is extracted from the register `naam` field (e.g. "Sneek" from
"Memories kantoor Sneek").

**Resume**: `<out-dir>/.cache/friesland/friesland_progress.csv` tracks completed registers. Existing
per-person directories (with `metadata.json`) are skipped on reruns.

**Inventory cache**: the 12-page register walk (~3.1 s) is cached in
`<out-dir>/.cache/friesland/registers.json`, and an `--invnr` run replaces it
with one `search_s_inventarisnummer` request (~23 ms) — see *Archive-level
inventory listing* above.

### Limburg (RHCL) – archieven.nl MAIS

Two archive codes hold all Memories van Successie at RHCL:

| Code   | Period         | Total invnrs | Digitized | Organised by |
|--------|----------------|--------------|-----------|--------------|
| 07.D03 | 1818-1900 (1905) | 1,314      | 111       | Plaats (place of death) |
| 07.D08 | 1901-1927      | 460          | 42        | Kantoor      |

07.D08 also contains a sibling section "Tafels 5bis" (minr 1014481) which is
**excluded** per the project-wide Tafel V-bis rule. The scraper drills into
07.D08's MvS-only sub-section (parent minr 1014062), so the tafel branch is
never visited.

```
inv2 root:      https://www.archieven.nl/nl/zoeken
                  ?mivast=0&mizig=210&miadt=38&micode={code}&miview=inv2
per-invnr page: …same…&minr={minr}  (strip auto-loads)
image URL:      https://preserve3.archieven.nl/mi-0/fonc-rhcl/{code}/{invnr}/
                  NL-MtHCL_{code}_{invnr}_{page:04d}.jpg
                  ?format=large&miadt=38&miahd={miahd}&mivast=0&rdt={rdt}&open={token}
```

Pagination quirks:
- The root inv2 page renders only ~100 leaf nodes at a time, with a
  ``Records N t/m M`` toggle per remaining batch driven by
  ``mi_inv3_swapinv(...)``. The scraper clicks every batch in-page until none
  remain.
- The per-invnr strip exposes only 25 thumbnails initially; the rest are
  loaded by clicking the ``.snext`` (Volgende) arrow. The scraper steps the
  arrow until the ``.snavuit`` (disabled) class appears.

Image format: ``format=large`` returns a 714×1024 PNG (~700 KB-1.2 MB per
page). The archival 2090×3000 JPEG is only available via the IIPSrv zoomify
tile server (``iipsrv12.fcgi?FIF=cache/fonc-rhcl/{hash}.jp2&CVT=jpeg``), but
the ``{invnr,page} → JP2 hash`` map is only exposed inside each scan's
embed-viewer HTML, so reaching full-res would require an extra viewer load
per scan (~110 k loads). See module docstring for details.

Caches:
- ``<out-dir>/.cache/limburg/inventory_{code}.json``  – list of digitized invnrs
- ``<out-dir>/.cache/limburg/tokens_{code}_{invnr}.json`` – per-page tokens for one register

Both caches are sufficient for the download phase; rerunning skips Playwright
entirely once they exist.

### Noord-Holland (NHA) – noord-hollandsarchief.nl MAIS

Archive 178 holds all *Memories van Successie* for the province of Noord-Holland.
The inventory is organized by kantoor (tax office), discovered dynamically from
the inv2 tree via Playwright.

**Approach**: Same stk3 inline toggle pattern as Overijssel and Utrecht.

```
inv2 root:      https://noord-hollandsarchief.nl/bronnen/archieven
                  ?mivast=236&mizig=210&miadt=236&micode=178&miview=inv2
inv3 (kantoor): …same…&miaet=1&micode=178&minr={minr}&milang=nl&miview=inv3
image URL:      https://preserve-nha.archieven.nl/mi-0/fonc-nha/178/{invnr}/
                  NL-HlmNHA_178_{invnr}_{page:04d}.jpg
                  ?miadt=236&miahd={miahd}&mivast=0&rdt={rdt}&open={token}
```

**Image format**: Remove `?format=thumb` from thumbnail URLs to get full-size.
Note that the preserve URL uses `mivast=0` (not 236), same pattern as Limburg.

**Caches**:
- ``<out-dir>/.cache/noordholland/sections.json`` – discovered kantoor sections
- ``<out-dir>/.cache/noordholland/tokens_{minr}.json`` – per-page tokens for one kantoor section
- ``<out-dir>/.cache/noordholland/tokens_{minr}_partial.json`` – incremental save (crash-resilient)

**Resume**: ``<out-dir>/.cache/noordholland/done.txt`` tracks completed kantoor sections.
Partial token caches allow resuming interrupted harvest runs.

``done.txt`` is keyed by kantoor section, which is coarser than ``--invnr`` filters at,
so a filtered run neither reads nor writes it: ``--invnr`` runs are stateless with
respect to unit completion, and per-file existence checks keep repeat runs cheap.
A filter that matches nothing anywhere prints a warning instead of exiting silently.
``--kantoor`` is exactly as coarse as the marker, so a ``--kantoor``-only run
still records the kantoren it finished; adding ``--invnr`` suppresses that again.

### Zeeland (Zeeuws Archief) – MAIS token extraction

First-time setup: ``uv sync && playwright install chromium``

The Zeeuws Archief runs its own MAIS instance on the zeeuwsarchief.nl domain. The
scraper takes a **hybrid approach**:

1. **Discovery** – Navigates to the inv3 tree view for each kantoor minr, expands
   all sub-sections via swapinv clicks, then harvests inventarisnummer minr values
   (and their texts) from stk3 onclick handlers. Digitized items are those whose
   tree node carries an `h_scan.gif` marker. Tafel V-bis filtered by text.

2. **Token harvest** – Navigates to each invnr's inv2 minr page. The strip viewer
   auto-loads on this page. All strip chunks are force-loaded via
   ``mi_strip_store.populate()``, then thumbnail ``<img>`` elements with
   ``src*="fonc-zaf"`` are harvested from the DOM.

3. **Download** – Thumbnails have ``?format=thumb``; replacing with ``?format=large``
   yields 673×1024 PNG. The preserve server is ``preserve-zaf.archieven.nl/mi-239/``.

**Image URL format:**
```
https://preserve-zaf.archieven.nl/mi-239/fonc-zaf/398/{invnr}/
    NL-MdbZA_398_{invnr}_{slug}_{page:04d}.jpg
    ?format=large&miadt=239&miahd={miahd}&mivast=239&rdt={rdt}&open={token}
```
Some images omit the ``{slug}_`` component (e.g. ``NL-MdbZA_398_1_0001.jpg``). The
slug provides uniqueness when the same trailing page number appears in multiple
scan segments within one register.

**Kantoren** (9 total, discovered dynamically):

| Kantoor     | minr      | Digitized invnrs | Total invnrs |
|-------------|-----------|------------------|--------------|
| Goes        | 33439946  | 990              | 1,109        |
| Hulst       | 33439947  | TBD              | TBD          |
| Colijnsplaat/Kortgene | 33439948 | TBD       | TBD          |
| Middelburg  | 33439949  | TBD              | TBD          |
| Oostburg    | 33439950  | TBD              | TBD          |
| Tholen      | 33439951  | TBD              | TBD          |
| Veere       | 33439952  | TBD              | TBD          |
| Vlissingen  | 33439953  | TBD              | TBD          |
| Zierikzee   | 33439954  | TBD              | TBD          |

**Caches**:
- ``<out-dir>/.cache/zeeland/kantoren.json`` – discovered kantoor entries with minr values
- ``<out-dir>/.cache/zeeland/tokens_minr_{minr}.json`` – per-page tokens for one kantoor
- ``<out-dir>/.cache/zeeland/tokens_minr_{minr}_partial.json`` – incremental save (crash-resilient)

**Resume**: ``<out-dir>/.cache/zeeland/done.txt`` tracks completed kantoren.
Partial token caches allow resuming interrupted harvest runs.

``done.txt`` is keyed by kantoor, which is coarser than ``--invnr`` filters at,
so a filtered run neither reads nor writes it: ``--invnr`` runs are stateless with
respect to unit completion, and per-file existence checks keep repeat runs cheap.
A filter that matches nothing anywhere prints a warning instead of exiting silently.
``--kantoor`` is exactly as coarse as the marker, so a ``--kantoor``-only run
still records the kantoren it finished; adding ``--invnr`` suppresses that again.
The per-kantoor token cache is suppressed the same way, since it claims to hold
every page in the kantoor; a warm cache is still narrowed to the requested invnrs.

**Smoke test** (2026-05-11): Goes invnr 1 → 327 pages, invnr 2 → 373 pages.
Downloads at ``format=large`` PNG (673×1024, ~300KB–950KB per page).

### Gelderland (Gelders Archief) – per-kantoor MAIS code

First-time setup: ``uv sync && uv run playwright install chromium``

Unlike every other MAIS instance in the project, the Gelders Archief gives
**each kantoor its own archief-code**. Twenty-one kantoren are hardcoded in
``KANTOREN`` (resolved 2026-05-11 from the kantoor permalinks listed at
``https://www.geldersarchief.nl/informatie/zoekhulp/997-memories-van-successie``):

| Kantoor     | Code  | Kantoor     | Code  | Kantoor     | Code  |
|-------------|-------|-------------|-------|-------------|-------|
| Arnhem      | 0021  | Elst        | 0028  | Tiel        | 0026  |
| Apeldoorn   | 0092  | Groenlo     | 0029  | Wageningen  | 0036  |
| Borculo     | 0022  | Harderwijk  | 0030  | Winterswijk | 0223  |
| Culemborg   | 0023  | Hattem      | 0031  | Zaltbommel  | 0037  |
| Doesburg    | 0024  | Lochem      | 0032  | Zevenaar    | 0221  |
| Druten      | 0025  | Nijkerk     | 0033  | Zutphen     | 0222  |
| Elburg      | 0027  | Nijmegen    | 0034  |             |       |
|             |       | Terborg     | 0035  |             |       |

Inside each kantoor's inv2 tree there are normally two top-level openinv items:

1. *Register IV, akten van het recht van successie en van overgang …* – the
   actual Memories van Successie.  Scraper keeps this.
2. *Tafel VI, alfabetische index … en Tafel V-bis, …* – Tafel V-bis is
   excluded per the project-wide rule, so we filter any top-level openinv
   whose text contains "tafel", "v-bis", or "5bis".

Below Register IV the records are grouped by 5-year periods ("Akten,
1818-1825.", "Akten, 1826-1830.", …).  Each period eventually contains the
leaf inventarisnummers ("1  1818", "140  1895 eerste kwartaal", …).
Digitized leaves carry an ``h_scan.gif`` icon in their tree row; leaves whose
text starts with ``^\d+\s`` and that have the marker are kept.

Scans are accessed by navigating to each leaf invnr's inv2 page; the
thumbnail strip auto-loads (25 thumbs initially) and remaining chunks are
force-loaded via ``mi_strip_store[…].populate()`` exactly as the Zeeland
scraper does.

**Image URL format:**
```
https://preserve2.archieven.nl/mi-37/fonc-gea/{code}/{invnr}/
    {invnr}-{page:04d}.jp2
    ?format=large&miadt=37&miahd={miahd}&mivast=37&rdt={rdt}&open={token}
```
Note the unusual filename convention: the file is named after the
inventarisnummer (``{invnr}-{page:04d}.jp2``), not a fixed archive
identifier.  The path itself also contains ``{invnr}`` between the code and
filename.  ``?format=large`` returns a 1024-pixel-tall PNG (~500 KB/page);
the full-resolution JP2 is only reachable via IIPSrv tile-server requests
that would require an extra viewer load per page (~tens of thousands of
extra requests project-wide), so ``format=large`` is the practical maximum
here.

**Caches**:
- ``<out-dir>/.cache/gelderland/inventory_{code}.json`` – discovered leaf invnrs for one
  kantoor: ``[{invnr, text, minr, hasScan}, …]``
- ``<out-dir>/.cache/gelderland/tokens_{code}.json`` – per-page tokens for one kantoor
- ``<out-dir>/.cache/gelderland/tokens_{code}_partial.json`` – incremental save written
  every 25 invnrs so a crash mid-harvest doesn't lose work

**Resume**: ``<out-dir>/.cache/gelderland/done.txt`` tracks completed kantoor codes.

``done.txt`` is keyed by kantoor code, which is coarser than ``--invnr`` filters at,
so a filtered run neither reads nor writes it: ``--invnr`` runs are stateless with
respect to unit completion, and per-file existence checks keep repeat runs cheap.
A filter that matches nothing anywhere prints a warning instead of exiting silently.
``--kantoor`` is exactly as coarse as the marker, so a ``--kantoor``-only run
still records the kantoren it finished; adding ``--invnr`` suppresses that again.
The per-kantoor code token cache is suppressed the same way, since it claims to hold
every page in the kantoor code; a warm cache is still narrowed to the requested invnrs.

**Smoke test** (2026-05-11): Borculo (code 0022) end-to-end – 49 digitized
invnrs discovered, invnr 1 ("1 1818 eerste halfjaar") → 38 pages, full-size
download = 1024×858 PNG (~570 KB).  Tafel-only kantoor sections are
automatically skipped at the Register-IV selection step.
