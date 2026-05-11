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

## Quick start

**Requirements**: Python >= 3.14, [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync

# First-time MAIS/Playwright setup (Gelderland, Overijssel, Utrecht, Limburg, Noord-Holland, Zeeland)
uv run playwright install chromium

# Download all archives (takes several hours)
uv run python main.py all

# Or run one archive at a time
uv run python main.py friesland
uv run python main.py nationaalarchief
uv run python main.py drentsarchief
uv run python main.py bhic
uv run python main.py overijssel
uv run python main.py utrechtsarchief
uv run python main.py limburg
uv run python main.py noordholland
uv run python main.py zeeland
uv run python main.py gelderland
```

---

## Pipelines in detail

### Friesland – Tresoar / AlleFriezen

`uv run python main.py friesland`
Source file: `python/friesland.py`

Uses Tresoar's **Memorix genealogy REST API** via the AlleFriezen tenant key
(`aa030ec4-12d0-4dc0-afaf-b65fd6128b39`).

1. Enumerates all 1,107 MvS registers via `/register?fq=search_s_type_title:"Memories van successie"`.
2. For each register, paginates `/deed` (assets embedded) and `/person`.
3. Joins persons to deeds by `deed_id`, filters to *overledene* persons.
4. Downloads all `asset[].download` URLs (JPEG 2000 `.jp2`, full-size).

Tafel V-bis is not present at Tresoar (0 results for "tafel" or "v-bis").

Progress is tracked in `friesland_progress.csv` (per-register). Existing per-person directories (with `metadata.json`) are skipped on reruns.
Output: `scans/friesland/{kantoor}/{invnr}/{person_slug}/`.

---

### Gelderland – Gelders Archief

`uv run python main.py gelderland`
Source file: `python/gelderland.py`

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

Inventory and token caches (`inventory_{code}.json`, `tokens_{code}.json` with partial saves every 25 invnrs) skip Playwright on reruns. Already-downloaded kantoren are tracked in `scans/gelderland/done.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

### Nationaal Archief – Zuid-Holland

`uv run python main.py nationaalarchief`
Source file: `python/nationaalarchief.py`

Access number **3.06.05**. The pipeline:
1. Fetches the EAD XML inventory (`/download/xml`) and parses section 2.4 for Memories invnrs, excluding Tafel V-bis and Tafel VI. Falls back to a hardcoded range list if the download fails.
2. For each inventory number, loads the viewer page and extracts scan UUIDs from the embedded `drupal-settings-json` data block.
3. Downloads full-size scans from `service.archief.nl/api/file/v1/default/{UUID}`.

Progress is tracked in `nationaalarchief_done.txt` so interrupted runs can be resumed.
Output: `scans/nationaalarchief/{invnr}/`.

---

### Drents Archief

`uv run python main.py drentsarchief`
Source file: `python/drentsarchief.py`

Uses the **Memorix genealogy REST API** at `webservices.memorix.nl/genealogy` (~106,000 deeds total).

1. Searches all persons with deed type `Successiememories` (paginated, 35,000+ pages).
2. Collects unique deed IDs and fetches the deed detail for each.
3. Downloads all `asset[].download` URLs (full-size JPEGs).

Progress is tracked in `drentsarchief_deeds.csv`.
Output: `scans/drentsarchief/{deed_id}/`.

---

### BHIC – Brabants Historisch Informatie Centrum (Noord-Brabant)

`uv run python main.py bhic`
Source file: `python/bhic.py`

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
Output: `scans/bhic/{gemeente}/deel_{invnr}/`.

---

### Limburg – Regionaal Historisch Centrum Limburg (RHCL)

`uv run python main.py limburg`
Source file: `python/limburg.py`

Uses the **MAIS Internet viewer on archieven.nl** (`miadt=38`, `mivast=0`). Covers two archive codes:

| Code   | Period           | Total invnrs | Digitized | Organised by      |
|--------|------------------|--------------|-----------|-------------------|
| 07.D03 | 1818–1900 (1905) | 1,314        | 111       | Plaats (place)    |
| 07.D08 | 1901–1927        | 460          | 42        | Kantoor           |

The pipeline uses **Playwright/Chromium** to:

1. Navigate to the inv2 root for each code, expand all "Records N t/m M" batch toggles, then harvest digitized invnr minr values (marked with `h_scan.gif`). Exclusion: 07.D08's sibling "Tafels 5bis" section is never entered.
2. For each digitized invnr: navigate to the inv2 page (strip auto-loads), click "Volgende" until all pages are loaded, harvest per-page tokens from `<img src>` attributes.
3. Download full-size PNG scans (`format=large`, 714x1024).

Inventory and token caches (`scans/limburg/inventory_{code}.json`, `scans/limburg/tokens_{code}_{invnr}.json`) skip the slow Playwright pass on reruns.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

### Overijssel – Historisch Centrum Overijssel

`uv run python main.py overijssel`
Source file: `python/overijssel.py`

The HCO uses a MAIS Internet viewer where scan images require per-page authentication tokens (`miahd`, `rdt`, `open`) injected by the browser-side JavaScript. These cannot be retrieved with plain HTTP requests.

The pipeline uses **Playwright/Chromium** to drive a headless browser:

1. Navigates to the MAIS `inv3` inventory page for each kantoor, establishing the required session cookies automatically.
2. Calls `mi_inv3_toggle_stk()` for each invnr volume to load the stk3 thumbnail strip.
3. Harvests per-page tokens from the rendered `<img src>` attributes.
4. Downloads full-size scans using those tokens.

Token results are cached per-kantoor in `scans/overijssel/tokens_minr_{minr}.json` so the Playwright pass does not need to repeat on reruns.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

Covers all 10 kantoren: Almelo, Deventer, Enschede, Goor, Kampen, Ommen, Raalte, Steenwijk, Vollenhove, Zwolle.

---

### Utrechts Archief – Het Utrechts Archief (HUA)

`uv run python main.py utrechtsarchief`
Source file: `python/utrechtsarchief.py`

The HUA also uses a MAIS Internet viewer (`miadt=39`, `mivast=39`). The pipeline uses **Playwright/Chromium** with the same stk3 inline toggle approach as Overijssel:

1. Navigates to the `inv2` inventory page for each kantoor's archive code, expands the tree to discover *Memories van Successie* subsection `minr` values.
2. For each subsection, navigates to the `inv3` view in a single Playwright session.
3. Calls `mi_inv3_toggle_stk()` for each inventarisnummer to expand the stk3 thumbnail strip inline.
4. Harvests per-page tokens from the rendered `<img src>` attributes.
5. Derives full-size URLs by stripping `?format=thumb` from the harvested thumbnail URLs.
6. Downloads full-size PNG scans.

Unlike Overijssel, each kantoor has a different archive code (`micode`, e.g. `337-2` for Amersfoort, `337-7` for Utrecht), and subsection minr values are discovered dynamically rather than being hardcoded.

Token results are cached per subsection in `scans/utrechtsarchief/tokens_{micode}_{minr}.json`. Partial results are saved every 25 items for crash resilience. Already-downloaded inventarisnummers are tracked in `scans/utrechtsarchief/done_{kantoor}.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

Covers all 11 kantoren: Amersfoort, Amerongen, Loenen, Maarssen, Montfoort, Rhenen, Utrecht, IJsselstein, Vianen, Woerden, Wijk bij Duurstede.

---

### Noord-Holland – Noord-Hollands Archief (NHA)

`uv run python main.py noordholland`
Source file: `python/noordholland.py`

Uses the **MAIS Internet viewer** (`miadt=236`, `mivast=236`, archive code 178) on the `noord-hollandsarchief.nl` domain. The pipeline uses **Playwright/Chromium** with the same stk3 inline toggle approach as Overijssel and Utrecht:

1. Navigates to the inv2 page for archive 178; 15 kantoor-level entries are parsed from the initial DOM.
2. For each kantoor: expands the tree node to reveal period children, collects their minr values, and filters out Tafel V-bis items.
3. For each MvS period minr: navigates to the inv3 page, collects all stk3 child items, toggles each one to force-load the thumbnail strip, harvests per-page tokens from `<img src>` attributes.
4. Converts thumbnail URLs to full-size (removes `?format=thumb`) and downloads.

Token results are cached per period minr in `scans/noordholland/tokens_{minr}.json` with partial saves for crash resilience. Already-downloaded kantoren are tracked in `scans/noordholland/done.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

### Zeeland – Zeeuws Archief

`uv run python main.py zeeland`
Source file: `python/zeeland.py`

Uses the **MAIS Internet viewer** (`miadt=239`, `mivast=239`) on the `zeeuwsarchief.nl` domain. The archive is identified by `micode=398` ("Ontvangers der Successierechten in Zeeland, (1795) 1806-1927"). The pipeline uses **Playwright/Chromium** with the same stk3 inline toggle approach as Overijssel, Utrecht, and Noord-Holland:

1. Navigates to the `inv2` inventory page for archive 398, discovers kantoor sections from the tree (`mi_inv3_openinv` links).
2. Expands each kantoor node to reveal inventarisnummers with stk3 inline strips.
3. Calls `mi_inv3_toggle_stk()` for each inventarisnummer to load the stk3 thumbnail strip.
4. Force-loads all strip chunks and harvests per-page tokens from `<img src>` attributes.
5. Derives full-size URLs by stripping `?format=thumb` from thumbnail URLs and downloads scans.

Token results are cached per kantoor in `scans/zeeland/tokens_minr_{minr}.json` with partial saves for crash resilience. Already-downloaded kantoren are tracked in `scans/zeeland/done.txt`.

**First-time setup**: run `uv run playwright install chromium` after `uv sync`.

---

## Output structure

```
scans/
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
└── zeeland/{kantoor}/{invnr}/
    ├── metadata.json
    └── 0000.jpg …
```

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

- **Friesland**: tracks completed registers in `friesland_progress.csv` (rows with `status=done` are skipped); existing per-person directories (with `metadata.json`) are skipped on reruns.
- **Gelderland**: inventory and token cache files (`inventory_{code}.json`, `tokens_{code}.json` with partial saves every 25 invnrs) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed kantoren are tracked in `scans/gelderland/done.txt`.
- **Nationaal Archief**: tracks completed inventory numbers in `nationaalarchief_done.txt`.
- **Drents Archief**: tracks completed deeds in `drentsarchief_deeds.csv` (rows with `status=done` are skipped).
- **BHIC**: tracks completed registers in `bhic_progress.csv` (rows with `status=done` are skipped); already-downloaded scans are skipped by file existence check.
- **Overijssel**: token cache files (`tokens_minr_*.json`) skip the slow Playwright pass; already-downloaded images are skipped by file existence check.
- **Limburg**: inventory and token cache files (`inventory_{code}.json`, `tokens_{code}_{invnr}.json`) skip the slow Playwright pass; already-downloaded images are skipped by file existence check.
- **Utrechts Archief**: token cache files (`tokens_{micode}_{minr}.json`, with partial saves every 25 items for crash resilience) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed inventarisnummers are tracked in `done_{kantoor}.txt` per kantoor.
- **Noord-Holland**: token cache files (`tokens_{minr}.json`, with partial saves for crash resilience) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed kantoren are tracked in `scans/noordholland/done.txt`.
- **Zeeland**: token cache files (`tokens_minr_{minr}.json`, with partial saves for crash resilience) skip the slow Playwright pass; already-downloaded images are skipped by file existence check. Completed kantoren are tracked in `scans/zeeland/done.txt`.
