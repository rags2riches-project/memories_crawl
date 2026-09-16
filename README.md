# Memories van Successie – Download Pipeline

Downloads all surviving *Memories van Successie* (Dutch succession/inheritance registers, 1806–1927) from regional Dutch archives and saves the scans with structured metadata.

## What are Memories van Successie?

When someone died in the Netherlands between 1806 and 1927, their heirs were required to register the estate with the local tax office (*kantoor van successie*). These registers are a goldmine for genealogical research: they record the name of the deceased, the date and place of death, heirs and their relationships, and the value of the estate.

The registers are organised by fiscal district (*kantoor*) and contain individual entries (*akten*). **Tafel V-bis** (an appendix covering special cases) is excluded from all pipelines in this project.

---

## Archive coverage

| Province | Archive | System | Status |
|---|---|---|---|
| Friesland | Tresoar | Memorix REST API | ✅ 1,107 registers, ~238k persons |
| Gelderland | Gelders Archief | MAIS + Playwright | ✅ 21 kantoren |
| Zuid-Holland | Nationaal Archief | Custom scraper | ✅ |
| Drenthe | Drents Archief | Memorix REST API | ✅ |
| Noord-Brabant | BHIC | Memorix REST API | ✅ 1,896 registers |
| Overijssel | Historisch Centrum Overijssel | MAIS + Playwright | ✅ 10 kantoren |
| Utrecht | Het Utrechts Archief | MAIS + Playwright | ✅ 11 kantoren |
| Limburg | RHCL | MAIS + Playwright | ✅ |
| Noord-Holland | Noord-Hollands Archief | MAIS + Playwright | ✅ |
| Zeeland | Zeeuws Archief | MAIS + Playwright | ✅ |

**Playwright note**: Gelderland, Overijssel, Utrecht, Limburg, Noord-Holland, and Zeeland (MAIS) pipelines require `uv run playwright install chromium` to download the matching Chromium browser before running.

---

New to this project? [GUIDE.md](GUIDE.md) explains what these scripts do, why they're needed, and how the archives work — in plain terms, no technical background assumed.

## Install

```bash
pip install memories-crawl
```

Or for development with [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/rags2riches-project/memories_crawl.git
cd memories_crawl
uv sync
```

## Quick start

**Requirements**: Python >= 3.12.

```bash
# First-time MAIS/Playwright setup (Gelderland, Overijssel, Utrecht, Limburg, Noord-Holland, Zeeland)
uv run playwright install chromium

# Download all archives (takes several hours)
memories-crawl all

# Or run one archive at a time
memories-crawl friesland
memories-crawl nationaalarchief
memories-crawl drentsarchief
memories-crawl bhic
memories-crawl overijssel
memories-crawl utrechtsarchief
memories-crawl limburg
memories-crawl noordholland
memories-crawl zeeland
memories-crawl gelderland
```

---

## Filtering and listing inventory numbers

These flags let you scope downloads instead of pulling the entire archive:

### `--list-invnrs` — see what's available

Prints all digitized inventory numbers (with kantoor, description, date range, and page count where available) and exits without downloading anything.

```bash
# List all digitized invnrs for an archive
uv run memories-crawl limburg --list-invnrs
uv run memories-crawl gelderland --list-invnrs
uv run memories-crawl drentsarchief --list-invnrs
```

For the API-backed archives (Drenthe, BHIC, Friesland) and for archives with a cached
inventory (Limburg, Gelderland, Zeeland), this runs in about a second without launching a
browser. For the others (Overijssel, Utrecht, Noord-Holland), it needs the Playwright
token-harvest pass first — but cached tokens are reused on reruns, as long as the later
run uses the same `--out-dir`.

### `--csv` — export listing to a spreadsheet

When combined with `--list-invnrs`, writes the inventory listing to a CSV file
instead of (or in addition to) printing it to the terminal. The terminal output
is still shown.

```bash
# Default filename: {pipeline}_invnrs.csv
uv run memories-crawl zeeland --list-invnrs --csv

# Custom filename
uv run memories-crawl gelderland --list-invnrs --csv my-output.csv
```

| Archive | CSV columns |
|---|---|
| friesland | `invnr, kantoor, register_name` |
| nationaalarchief | `invnr` |
| drentsarchief | `invnr, gemeente, register_name` |
| bhic | `invnr, gemeente, register_name` |
| overijssel | `kantoor, invnr, pages` |
| utrechtsarchief | `kantoor, section, invnr, description, pages` |
| limburg | `code, invnr, place_or_kantoor, datering, title` |
| noordholland | `kantoor, period, invnr, description, pages` |
| zeeland | `kantoor, invnr, description` |
| gelderland | `kantoor, code, invnr, description` |

### `--invnr` — download a specific volume

Restricts the download to one or more inventory numbers. Repeat the flag for multiple:

```bash
# Download a single register
uv run memories-crawl limburg --invnr 1

# Download several at once
uv run memories-crawl gelderland --invnr 1 --invnr 2

# Combine with --list-invnrs to preview what would be downloaded
uv run memories-crawl zeeland --invnr 1 --invnr 42 --list-invnrs
```

The filter is applied as early as possible: for archives with cached inventory it
happens before the slow Playwright token-harvest phase; for the rest it happens after
token harvest but before downloading. Only matching invnrs are processed.

For Drenthe, BHIC and Friesland it is applied earlier still — in the API query
itself — so a `--invnr` run makes one targeted request instead of walking the
whole register listing. On BHIC that is ~18 ms rather than ~5.2 s.

### `--refresh-cache` — re-read the inventory listing

The API-backed archives (Friesland, Nationaal Archief, Drenthe, BHIC) cache their
archive-level inventory listing under `<out-dir>/.cache/{archive}/` for 30 days, so
running the CLI many times in a row — a sampler fetching one register per
invocation, say — does not re-enumerate the archive each time. These archives
re-catalogue on the order of years, so the cached listing is effectively always
current; `--refresh-cache` re-collects it anyway:

```bash
uv run memories-crawl bhic --refresh-cache --list-invnrs
```

A cache that is unreadable, expired, or written for a different query is discarded
and re-collected rather than trusted, and an empty listing is never written — a
transient API failure cannot leave you with an archive that looks empty. The flag
has no effect on the Playwright archives, which keep their own inventory and token
caches.

### `--out-dir` — choose where everything lands

By default scans and caches are written below `./scans`, relative to the directory
you run the command from. `--out-dir` makes that root explicit:

```bash
uv run memories-crawl gelderland --out-dir /mnt/data/mvs
```

`$MEMORIES_CRAWL_OUT_DIR` sets the same root if you would rather not repeat the flag.

**Use the same `--out-dir` for every invocation of an archive.** The inventory and
Playwright token caches live below the output root, so a `--list-invnrs` pass and a
download pass run with different roots cannot see each other's caches, and the token
harvest — by far the slowest part of the MAIS pipelines — silently runs twice.

---

## Pipelines in detail

### Friesland – Tresoar / AlleFriezen

`uv run memories-crawl friesland`
Source file: `src/memories_crawl/friesland.py`

Uses Tresoar's **Memorix genealogy REST API** via the AlleFriezen tenant key
(`aa030ec4-12d0-4dc0-afaf-b65fd6128b39`).

1. Enumerates all 1,107 MvS registers via `/register?fq=search_s_type_title:"Memories van successie"`.
2. For each register, paginates `/deed` (assets embedded) and `/person`.
3. Joins persons to deeds by `deed_id`, filters to *overledene* persons.
4. Downloads all `asset[].download` URLs (JPEG 2000 `.jp2`, full-size).

Tafel V-bis is not present at Tresoar (0 results for "tafel" or "v-bis").

Progress is tracked in `<out-dir>/.cache/friesland/friesland_progress.csv` (per-register). Existing per-person directories (with `metadata.json`) are skipped on reruns.
Output: `<out-dir>/friesland/{kantoor}/{invnr}/{person_slug}/`.

---

### Gelderland – Gelders Archief

`uv run memories-crawl gelderland`
Source file: `src/memories_crawl/gelderland.py`

Uses the **MAIS Internet viewer** (`miadt=37`, `mivast=37`) on the `geldersarchief.nl` domain. Unlike other MAIS instances, the Gelders Archief gives **each kantoor its own archive code** (micode). 21 kantoren are configured with codes 0021–0037, 0092, 0221–0223.

1. For each kantoor (micode), navigates to the inv2 root, picks the "Register IV" top-level minr (filtering out Tafel VI / V-bis).
2. Enumerates leaf inventarisnummers via the inv3 tree, expanding all period sub-sections and filtering for digitized (h_scan) items.
3. For each leaf invnr, navigates to the inv2 minr page (strip auto-loads), force-loads all strip chunks via `mi_strip_store.populate()`, and harvests thumbnail URLs (`fonc-gea`).
4. Converts thumbnail URLs to full-size (`?format=large`, 1024-pixel-tall PNG) and downloads.

**Image URL format:**
```
https://preserve2.archieven.nl/mi-37/fonc-gea/{code}/{invnr}/
    {invnr}-{page:04d}.jp2
    ?format=large&miadt=37&miahd={miahd}&mivast=37&rdt={rdt}&open={token}
```

The full-resolution JP2 is only reachable via IIPSrv tile-server requests; `format=large` is the practical maximum.

Inventory and token caches (`inventory_{code}.json`, `tokens_{code}.json` with partial saves every 25 invnrs) skip Playwright on reruns. Already-downloaded kantoren are tracked in `<out-dir>/.cache/gelderland/done.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

### Nationaal Archief – Zuid-Holland

`uv run memories-crawl nationaalarchief`
Source file: `src/memories_crawl/nationaalarchief.py`

Access number **3.06.05**. The pipeline:
1. Fetches the EAD XML inventory (`/download/xml`) and parses section 2.4 for Memories invnrs, excluding Tafel V-bis and Tafel VI. Falls back to a hardcoded range list if the download fails.
2. For each inventory number, loads the viewer page and extracts scan UUIDs from the embedded `drupal-settings-json` data block.
3. Downloads full-size scans from `service.archief.nl/api/file/v1/default/{UUID}`.

Progress is tracked in `<out-dir>/.cache/nationaalarchief/nationaalarchief_done.txt` so interrupted runs can be resumed.
Output: `<out-dir>/nationaalarchief/{invnr}/`.

---

### Drents Archief

`uv run memories-crawl drentsarchief`
Source file: `src/memories_crawl/drentsarchief.py`

Uses the **Memorix genealogy REST API** at `webservices.memorix.nl/genealogy`
(557 registers, ~106,000 deeds total).

1. Enumerates all Memorie van Successie registers in a single request
   (`/register?fq=search_s_brontype:"Memorie van Successie"&rows=1000`). Register
   metadata carries `inventarisnummer` and `gemeente`, so `--list-invnrs` and
   `--invnr` are resolved before any other request is made.
2. For each selected register, pages `/deed?fq=register_id:{id}` and
   `/person?fq=register_id:{id}`. Deed search results already embed
   `asset[].download`, so no per-deed detail request is needed.
3. Downloads all `asset[].download` URLs (full-size JPEGs).

Progress is tracked in `<out-dir>/.cache/drentsarchief/drentsarchief_deeds.csv`, flushed
after every deed.
Output: `<out-dir>/drentsarchief/{deed_id}/`.

---

### BHIC – Brabants Historisch Informatie Centrum (Noord-Brabant)

`uv run memories-crawl bhic`
Source file: `src/memories_crawl/bhic.py`

Uses the **same Memorix backend** as Drenthe but with a different tenant key
(`24c66d08-da4a-4d60-917f-5942681dcaa1`). Crucially, BHIC's scans live at the
**register** level (one register = one bound book of memories), not at the deed
level — so the pipeline pivots around registers, not deeds.

1. Enumerates all 1,896 registers via `/register?fq=search_s_type_title:"memorie van successie"`. Covers both `036.03.xx` (kantoor series) and `021.13` (Memories van successie Brabant).
2. For each register, paginates `/asset?fq=register_id:{id}` and downloads every `asset[].download` URL (full-size JPEG).
3. Paginates `/deed?fq=register_id:{id}` and `/person?fq=register_id:{id}` and writes them, joined, as a `deeds.json` sidecar — giving you aktenummer, plaats, naam van de overledene, datum overlijden, … alongside the scans.

Tafel V-bis is not indexed at BHIC, but a defensive filter skips any record
whose name/type still contains "tafel" or "v-bis".

Progress is tracked in `bhic_progress.csv`.
Output: `<out-dir>/bhic/{gemeente}/deel_{invnr}/`.

---

### Limburg – Regionaal Historisch Centrum Limburg (RHCL)

`uv run memories-crawl limburg`
Source file: `src/memories_crawl/limburg.py`

Uses the **MAIS Internet viewer on archieven.nl** (`miadt=38`, `mivast=0`). Covers two archive codes:

| Code   | Period           | Total invnrs | Digitized | Organised by      |
|--------|------------------|--------------|-----------|-------------------|
| 07.D03 | 1818–1900 (1905) | 1,314        | 111       | Plaats (place)    |
| 07.D08 | 1901–1927        | 460          | 42        | Kantoor           |

The pipeline uses **Playwright/Chromium** to:

1. Navigate to the inv2 root for each code, expand all "Records N t/m M" batch toggles, then harvest digitized invnr minr values (marked with `h_scan.gif`). Exclusion: 07.D08's sibling "Tafels 5bis" section is never entered.
2. For each digitized invnr: navigate to the inv2 page (strip auto-loads), click "Volgende" until all pages are loaded, harvest per-page tokens from `<img src>` attributes.
3. Download full-size PNG scans (`format=large`, 714x1024).

Inventory and token caches (`<out-dir>/.cache/limburg/inventory_{code}.json`, `<out-dir>/.cache/limburg/tokens_{code}_{invnr}.json`) skip the slow Playwright pass on reruns.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

### Overijssel – Historisch Centrum Overijssel

`uv run memories-crawl overijssel`
Source file: `src/memories_crawl/overijssel.py`

The HCO uses a MAIS Internet viewer where scan images require per-page authentication tokens (`miahd`, `rdt`, `open`) injected by the browser-side JavaScript. These cannot be retrieved with plain HTTP requests.

The pipeline uses **Playwright/Chromium** to drive a headless browser:

1. Navigates to the MAIS `inv3` inventory page for each kantoor, establishing the required session cookies automatically.
2. Calls `mi_inv3_toggle_stk()` for each invnr volume to load the stk3 thumbnail strip.
3. Harvests per-page tokens from the rendered `<img src>` attributes.
4. Downloads full-size scans using those tokens.

Token results are cached per-kantoor in `<out-dir>/.cache/overijssel/tokens_minr_{minr}.json` so the Playwright pass does not need to repeat on reruns.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

Covers all 10 kantoren: Almelo, Deventer, Enschede, Goor, Kampen, Ommen, Raalte, Steenwijk, Vollenhove, Zwolle.

---

### Utrechts Archief – Het Utrechts Archief (HUA)

`uv run memories-crawl utrechtsarchief`
Source file: `src/memories_crawl/utrechtsarchief.py`

The HUA also uses a MAIS Internet viewer (`miadt=39`, `mivast=39`). The pipeline uses **Playwright/Chromium** with the same stk3 inline toggle approach as Overijssel:

1. Navigates to the `inv2` inventory page for each kantoor's archive code, expands the tree to discover *Memories van Successie* subsection `minr` values.
2. For each subsection, navigates to the `inv3` view in a single Playwright session.
3. Calls `mi_inv3_toggle_stk()` for each inventarisnummer to expand the stk3 thumbnail strip inline.
4. Harvests per-page tokens from the rendered `<img src>` attributes.
5. Derives full-size URLs by stripping `?format=thumb` from the harvested thumbnail URLs.
6. Downloads full-size PNG scans.

Unlike Overijssel, each kantoor has a different archive code (`micode`, e.g. `337-2` for Amersfoort, `337-7` for Utrecht), and subsection minr values are discovered dynamically rather than being hardcoded.

Token results are cached per subsection in `<out-dir>/.cache/utrechtsarchief/tokens_{micode}_{minr}.json`. Partial results are saved every 25 items for crash resilience. Already-downloaded inventarisnummers are tracked in `<out-dir>/.cache/utrechtsarchief/done_{kantoor}.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

Covers all 11 kantoren: Amersfoort, Amerongen, Loenen, Maarssen, Montfoort, Rhenen, Utrecht, IJsselstein, Vianen, Woerden, Wijk bij Duurstede.

---

### Noord-Holland – Noord-Hollands Archief (NHA)

`uv run memories-crawl noordholland`
Source file: `src/memories_crawl/noordholland.py`

Uses the **MAIS Internet viewer** (`miadt=236`, `mivast=236`, archive code 178) on the `noord-hollandsarchief.nl` domain. The pipeline uses **Playwright/Chromium** with the same stk3 inline toggle approach as Overijssel and Utrecht:

1. Navigates to the inv2 page for archive 178; 15 kantoor-level entries are parsed from the initial DOM.
2. For each kantoor: expands the tree node to reveal period children, collects their minr values, and filters out Tafel V-bis items.
3. For each MvS period minr: navigates to the inv3 page, collects all stk3 child items, toggles each one to force-load the thumbnail strip, harvests per-page tokens from `<img src>` attributes.
4. Converts thumbnail URLs to full-size (removes `?format=thumb`) and downloads.

Token results are cached per period minr in `<out-dir>/.cache/noordholland/tokens_{minr}.json` with partial saves for crash resilience. Already-downloaded kantoren are tracked in `<out-dir>/.cache/noordholland/done.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

### Zeeland – Zeeuws Archief

`uv run memories-crawl zeeland`
Source file: `src/memories_crawl/zeeland.py`

Uses the **MAIS Internet viewer** (`miadt=239`, `mivast=239`) on the `zeeuwsarchief.nl` domain. The archive is identified by `micode=398` ("Ontvangers der Successierechten in Zeeland, (1795) 1806-1927"). The pipeline uses **Playwright/Chromium** with the same stk3 inline toggle approach as Overijssel, Utrecht, and Noord-Holland:

1. Navigates to the `inv2` inventory page for archive 398, discovers kantoor sections from the tree (`mi_inv3_openinv` links).
2. Expands each kantoor node to reveal inventarisnummers with stk3 inline strips.
3. Calls `mi_inv3_toggle_stk()` for each inventarisnummer to load the stk3 thumbnail strip.
4. Force-loads all strip chunks and harvests per-page tokens from `<img src>` attributes.
5. Derives full-size URLs by stripping `?format=thumb` from thumbnail URLs and downloads scans.

Token results are cached per kantoor in `<out-dir>/.cache/zeeland/tokens_minr_{minr}.json` with partial saves for crash resilience. Already-downloaded kantoren are tracked in `<out-dir>/.cache/zeeland/done.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

## Output structure

Scans go below the output root (`./scans` unless `--out-dir` says otherwise):

```
<out-dir>/
├── friesland/{kantoor}/{invnr}/{person_slug}/
│   ├── metadata.json
│   └── 0001.jp2 …
├── gelderland/{kantoor}/{invnr:04d}/
│   ├── metadata.json
│   └── {invnr}-0001.jpg …
├── nationaalarchief/{invnr}/
│   ├── metadata.json
│   └── NL-HaNA_3.06.05_{invnr}_*.jpg
├── drentsarchief/{deed_id}/
│   ├── metadata.json
│   └── 0001.jpg …
├── bhic/{gemeente}/deel_{invnr}/
│   ├── metadata.json
│   ├── deeds.json
│   └── {Gemeente}_{NNN}_NNNN.jpg …
├── limburg/{code}/{invnr}/
│   ├── metadata.json
│   └── 0001.jpg …
├── overijssel/{kantoor}/{invnr}/
│   ├── metadata.json
│   └── 0000.jpg …
├── utrechtsarchief/{kantoor}/{invnr}/
│   ├── metadata.json
│   └── 0000.jpg …
├── noordholland/{kantoor}/{invnr:04d}/
│   ├── metadata.json
│   └── 0001.jpg …
├── zeeland/{kantoor}/{invnr}/
│   ├── metadata.json
│   └── 0000.jpg …
└── .cache/{archive}/
    ├── registers.json            – archive-level register listing (30-day TTL)
    ├── inventory.json            – same, for the Nationaal Archief's EAD invnrs
    ├── inventory_{code}.json     – discovered inventarisnummers
    ├── tokens_*.json             – harvested Playwright tokens
    ├── done.txt                  – resume markers
    └── {archive}_progress.csv    – per-register progress
```

`.cache/` is deliberately kept out of the per-archive scan directories, so the images
can be moved or deleted without discarding a token harvest that took a quarter of an
hour to produce. Caches written by memories-crawl ≤ 0.2 sat mixed in with the images
(`scans/{archive}/tokens_*.json`); those are still found and updated in place, so
upgrading never forces a re-harvest.

## Metadata JSON format

Every scan folder contains a `metadata.json` with standardised fields:

```json
{
  "archief_naam": "BHIC",
  "archief_nummer": "...",
  "brontype": "Memorie van Successie",
  "gemeente": "...",
  "inventarisnummer": "...",
  "naam_overledene": "...",
  "sterfjaar": "...",
  "kantoor": "...",
  "url_origineel": "..."
}
```

Fields vary by archive depending on what metadata is available in the source system.

---

## Resuming interrupted runs

All pipelines are designed to be safely restarted:

- **Friesland**: tracks completed registers in `<out-dir>/.cache/friesland/friesland_progress.csv` (rows with `status=done` are skipped); existing per-person directories (with `metadata.json`) are skipped on reruns. The register listing is cached in `registers.json` for 30 days (`--refresh-cache` re-collects it).
- **Gelderland**: inventory and token cache files (`inventory_{code}.json`, `tokens_{code}.json` with partial saves every 25 invnrs) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed kantoren are tracked in `<out-dir>/.cache/gelderland/done.txt`.
- **Nationaal Archief**: tracks completed inventory numbers in `<out-dir>/.cache/nationaalarchief/nationaalarchief_done.txt`. The invnrs parsed from the EAD XML are cached in `inventory.json` for 30 days (`--refresh-cache` re-collects them); the hardcoded fallback list is never cached.
- **Drents Archief**: tracks completed deeds in `<out-dir>/.cache/drentsarchief/drentsarchief_deeds.csv` (rows with `status=done` are skipped), written after every deed so an interrupted run keeps its progress; scans are downloaded to a `.part` file and renamed on completion, so a truncated file is never mistaken for a finished one. The register listing is cached in `registers.json` for 30 days (`--refresh-cache` re-collects it).
- **BHIC**: tracks completed registers in `<out-dir>/.cache/bhic/bhic_progress.csv` (rows with `status=done` are skipped); already-downloaded scans are skipped by file existence check. The 19-page register listing is cached in `registers.json` for 30 days (`--refresh-cache` re-collects it).
- **Overijssel**: token cache files (`tokens_minr_*.json`) skip the slow Playwright pass; already-downloaded images are skipped by file existence check.
- **Limburg**: inventory and token cache files (`inventory_{code}.json`, `tokens_{code}_{invnr}.json`) skip the slow Playwright pass; already-downloaded images are skipped by file existence check.
- **Utrechts Archief**: token cache files (`tokens_{micode}_{minr}.json`, with partial saves every 25 items for crash resilience) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed inventarisnummers are tracked in `done_{kantoor}.txt` per kantoor.
- **Noord-Holland**: token cache files (`tokens_{minr}.json`, with partial saves for crash resilience) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed kantoren are tracked in `<out-dir>/.cache/noordholland/done.txt`.
- **Zeeland**: token cache files (`tokens_minr_{minr}.json`, with partial saves for crash resilience) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed kantoren are tracked in `<out-dir>/.cache/zeeland/done.txt`.
