# CLAUDE.md – Memories van Successie Pipeline

## What this project does

Downloads all surviving *Memories van Successie* (Dutch succession/inheritance registers, 1806–1927) from eight regional Dutch archives. Each scan is saved alongside a `metadata.json` sidecar.

## How to run

```bash
uv run python main.py friesland          # Friesland (Tresoar / AlleFriezen, Memorix API)
uv run python main.py openarchieven      # (deprecated – all archives migrated)
uv run python main.py nationaalarchief   # Zuid-Holland (Nationaal Archief 3.06.05)
uv run python main.py drentsarchief      # Drenthe (Memorix API)
uv run python main.py bhic               # Noord-Brabant (BHIC Memorix API)
uv run python main.py overijssel         # Overijssel (HCO) – requires Playwright
uv run python main.py utrechtsarchief    # Utrecht (Het Utrechts Archief) – requires Playwright
uv run python main.py limburg            # Limburg (RHCL, archieven.nl MAIS) – requires Playwright
uv run python main.py noordholland       # Noord-Holland (Noord-Hollands Archief) – requires Playwright
uv run python main.py zeeland            # Zeeland (Zeeuws Archief) – requires Playwright
uv run python main.py gelderland         # Gelderland (Gelders Archief) – requires Playwright
uv run python main.py all
```

## File map

| File | Purpose |
|---|---|
| `main.py` | CLI dispatcher |
| `python/step1_collect_record_guids_from_search_api.py` | Open Archieven: collect record GUIDs |
| `python/step2_oai_pmh_dumps.py` | Open Archieven: parse OAI-PMH XML dumps → `scan_urls.csv` |
| `python/step3_download_steps.py` | Open Archieven: download scans from `scan_urls.csv` |
| `python/nationaalarchief.py` | Zuid-Holland: scrape viewer pages, download via UUID |
| `python/drentsarchief.py` | Drenthe: Memorix REST API, deed→asset chain |
| `python/bhic.py` | Noord-Brabant (BHIC): Memorix REST API, register→asset chain |
| `python/overijssel.py` | Overijssel: Playwright-based MAIS token extraction |
| `python/utrechtsarchief.py` | Utrecht: Playwright-based MAIS stk3 inline strip extraction |
| `python/limburg.py` | Limburg (RHCL): Playwright on archieven.nl, strip Volgende-step |
| `python/noordholland.py` | Noord-Holland: Playwright-based MAIS stk3 inline strip extraction |
| `python/zeeland.py` | Zeeland: Playwright-based MAIS hybrid (inv3 discovery + inv2 strip harvest) |
| `python/friesland.py` | Friesland: Tresoar / AlleFriezen Memorix REST API, register→deed→person chain |
| `python/gelderland.py` | Gelderland: Playwright-based MAIS, one micode per kantoor (21 codes), strip auto-loads on inv2 minr |

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
| **friesland** | ✅ | ⚠️ not yet tested | Tresoar / AlleFriezen Memorix REST API. 1,107 registers, ~238k persons. Deed-level assets with .jp2 downloads. Person→deed join via deed_id. Output: scans/friesland/{kantoor}/{invnr}/{person}/. |
| **nationaalarchief** | ✅ | ✅ | 70 scans downloaded from invnr 2276 in 60s (174 MB). EAD XML parses correctly, drupal-settings-json extraction works, `service.archief.nl` download works. |
| **drentsarchief** | ✅ | ⚠️ slow start | API returns ~106k deeds. Pipeline must paginate ~1064 pages to collect all deed IDs **before** any download begins (~5 min). Once collection finishes, downloads work (8.3 MB/scan tested). |
| **openarchieven** | ✅ | ⚠️ slow start | All 7 archive dump URLs resolve on S3. Step 1 paginates millions of records (546k for BHI alone) before step 2 can begin. Expect hours before first scan file. |
| **overijssel** | ✅ | ⚠️ slow first run | Playwright + Chromium work. Almelo has 256 stk3 items → ~1825 pages of tokens; collecting tokens takes ~6 min per kantoor. Token results are cached in `scans/overijssel/tokens_minr_{minr}.json` — reruns skip Playwright entirely. |
| **utrechtsarchief** | ✅ | ⚠️ slow first run | Playwright + Chromium. Uses stk3 inline toggle (same approach as Overijssel). Amersfoort verified: 66,615 pages from 211 invnrs across 2 subsections (~12 min harvest). Token results cached per subsection — reruns skip Playwright. 11 kantoren configured. |
| **limburg** | ✅ | ✅ verified | archieven.nl MAIS (miadt=38, mivast=0). Two codes: 07.D03 (1818-1900, 111 digitized of 1,314, ~104k scans, by place) and 07.D08 (1901-1927, 42 digitized of 460, ~7k scans, by kantoor). End-to-end smoke-tested: invnr 1 (Amby) → 527 pages; invnr 491 (Gennep) → 207 pages. Inventory + tokens cached per code/invnr; reruns skip Playwright. Image format is `format=large` PNG (714×1024); see module docstring for trade-off vs. IIPSrv full-res JP2 path. |
| **noordholland** | ✅ | ⚠️ not yet tested | noord-hollandsarchief.nl MAIS (miadt=236, mivast=236, micode=178). Uses stk3 inline toggle (same approach as Overijssel/Utrecht). Kantoor sections discovered dynamically from inv2 tree. Tokens cached per section minr; reruns skip Playwright. Image server: preserve-nha.archieven.nl/mi-0/fonc-nha/178/. |
| **zeeland** | ✅ | ✅ verified | Zeeuws Archief MAIS (miadt=239, mivast=239, micode=398). Hybrid approach: inv3 tree for discovery (kantoor→sub-section→invnr with h_scan markers), inv2 minr pages for strip harvesting (auto-loads strip, force-load all chunks via mi_strip_store.populate()). Goes verified: 990 digitized invnrs of 1,109, invnr 1 → 327 pages, invnr 2 → 373 pages. Image server: preserve-zaf.archieven.nl/mi-239/fonc-zaf/398/. Downloads at `format=large` PNG (673×1024). Filenames include segment slug for uniqueness (e.g. `1-1_0001.jpg`). Tokens cached per kantoor in `tokens_minr_{minr}.json`. |
| **gelderland** | ✅ | ✅ verified | Gelders Archief MAIS (miadt=37, mivast=37). 21 kantoren, each with its own micode (0021–0037, 0092, 0221–0223). Per-kantoor pipeline: inv2 → pick "Register IV" minr (filter "Tafel VI / V-bis") → inv3 → swapinv-expand period sub-sections → collect leaf invnrs with `h_scan` markers and `^\d+\s` text. Per-invnr token harvest = navigate to inv2&minr=…, force-load strip via `mi_strip_store.populate()`, harvest `img[src*="fonc-gea"]`. Borculo (0022) verified end-to-end: 49 digitized invnrs, invnr 1 → 38 pages, full-size 1024×858 PNG (~570 KB/page). Image server: preserve2.archieven.nl/mi-37/fonc-gea/{code}/. Filenames `{invnr}-{page:04d}.jpg`. Inventory + tokens cached per code; reruns skip Playwright. |

**Setup reminder**: Chromium must be installed with `uv run playwright install chromium` (not bare `playwright install chromium`).

**Open Archieven archive codes**: All archives have been migrated to custom
scrapers. Gelders Archief (formerly `gra`) → `python/gelderland.py`; Tresoar /
Friesland (formerly `frl`) → `python/friesland.py`.

---

## Technical notes

### Open Archieven (step2) filter logic

```python
if SOURCETYPE.lower() not in source_type.lower():
    continue
if "tafel" in source_type.lower() or "v-bis" in source_type.lower():
    continue
```

### Nationaal Archief scan extraction

Scans are in a `<script data-drupal-selector="drupal-settings-json">` JSON blob. Parse `settings["viewer"]["response"]["scans"]`. Each scan has `{"id": UUID, "label": "NL-HaNA_...", "default": {"url": "https://service.archief.nl/api/file/v1/default/{UUID}"}}`. Download via `default.url`.

### Drents Archief API

```
Base: https://webservices.memorix.nl/genealogy
Key:  a85387a2-fdb2-44d0-8209-3635e59c537e
Person search: GET /person?q=*:*&fq=search_s_deed_type_title:"Successiememories"&rows=100&page=N
Deed detail:   GET /deed/{deed_id}
Full image:    asset[].download  (e.g. https://images.memorix.nl/dre/download/fullsize/{uuid}.jpg)
```

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
  scans/friesland/{kantoor}/{invnr}/{person_slug}/
      {NNNN}.jp2           – sequentially numbered scan pages
      metadata.json        – per-person info (name, date of death, …)
```

Kantoor is extracted from the register `naam` field (e.g. "Sneek" from
"Memories kantoor Sneek").

**Resume**: `friesland_progress.csv` tracks completed registers. Existing
per-person directories (with `metadata.json`) are skipped on reruns.

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
- ``scans/limburg/inventory_{code}.json``  – list of digitized invnrs
- ``scans/limburg/tokens_{code}_{invnr}.json`` – per-page tokens for one register

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
- ``scans/noordholland/sections.json`` – discovered kantoor sections
- ``scans/noordholland/tokens_{minr}.json`` – per-page tokens for one kantoor section
- ``scans/noordholland/tokens_{minr}_partial.json`` – incremental save (crash-resilient)

**Resume**: ``scans/noordholland/done.txt`` tracks completed kantoor sections.
Partial token caches allow resuming interrupted harvest runs.

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
- ``scans/zeeland/kantoren.json`` – discovered kantoor entries with minr values
- ``scans/zeeland/tokens_minr_{minr}.json`` – per-page tokens for one kantoor
- ``scans/zeeland/tokens_minr_{minr}_partial.json`` – incremental save (crash-resilient)

**Resume**: ``scans/zeeland/done.txt`` tracks completed kantoren.
Partial token caches allow resuming interrupted harvest runs.

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
- ``scans/gelderland/inventory_{code}.json`` – discovered leaf invnrs for one
  kantoor: ``[{invnr, text, minr, hasScan}, …]``
- ``scans/gelderland/tokens_{code}.json`` – per-page tokens for one kantoor
- ``scans/gelderland/tokens_{code}_partial.json`` – incremental save written
  every 25 invnrs so a crash mid-harvest doesn't lose work

**Resume**: ``scans/gelderland/done.txt`` tracks completed kantoor codes.

**Smoke test** (2026-05-11): Borculo (code 0022) end-to-end – 49 digitized
invnrs discovered, invnr 1 ("1 1818 eerste halfjaar") → 38 pages, full-size
download = 1024×858 PNG (~570 KB).  Tafel-only kantoor sections are
automatically skipped at the Register-IV selection step.
