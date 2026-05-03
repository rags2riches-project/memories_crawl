# CLAUDE.md – Memories van Successie Pipeline

## What this project does

Downloads all surviving *Memories van Successie* (Dutch succession/inheritance registers, 1806–1927) from eight regional Dutch archives. Each scan is saved alongside a `metadata.json` sidecar.

## How to run

```bash
uv run python main.py openarchieven      # BHIC, Zeeuws Archief, HUA, Gelders, NHA
uv run python main.py nationaalarchief   # Zuid-Holland (Nationaal Archief 3.06.05)
uv run python main.py drentsarchief      # Drenthe (Memorix API)
uv run python main.py overijssel         # Overijssel (HCO) – requires Playwright
uv run python main.py utrechtsarchief    # Utrecht (Het Utrechts Archief) – requires Playwright
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
| `python/overijssel.py` | Overijssel: Playwright-based MAIS token extraction |
| `python/utrechtsarchief.py` | Utrecht: Playwright-based MAIS stk3 inline strip extraction |

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
| **nationaalarchief** | ✅ | ✅ | 70 scans downloaded from invnr 2276 in 60s (174 MB). EAD XML parses correctly, drupal-settings-json extraction works, `service.archief.nl` download works. |
| **drentsarchief** | ✅ | ⚠️ slow start | API returns ~106k deeds. Pipeline must paginate ~1064 pages to collect all deed IDs **before** any download begins (~5 min). Once collection finishes, downloads work (8.3 MB/scan tested). |
| **openarchieven** | ✅ | ⚠️ slow start | All 7 archive dump URLs resolve on S3. Step 1 paginates millions of records (546k for BHI alone) before step 2 can begin. Expect hours before first scan file. |
| **overijssel** | ✅ | ⚠️ slow first run | Playwright + Chromium work. Almelo has 256 stk3 items → ~1825 pages of tokens; collecting tokens takes ~6 min per kantoor. Token results are cached in `scans/overijssel/tokens_minr_{minr}.json` — reruns skip Playwright entirely. |
| **utrechtsarchief** | ✅ | ⚠️ slow first run | Playwright + Chromium. Uses stk3 inline toggle (same approach as Overijssel). Amersfoort verified: 66,615 pages from 211 invnrs across 2 subsections (~12 min harvest). Token results cached per subsection — reruns skip Playwright. 11 kantoren configured. |

**Setup reminder**: Chromium must be installed with `uv run playwright install chromium` (not bare `playwright install chromium`).

**Open Archieven archive codes**: bhi (BHIC/Noord-Brabant), zar (Zeeuws Archief), frl (Tresoar/Friesland), rhl (RHCL/Limburg), hua (Het Utrechts Archief), gra (Gelders Archief), nha (Noord-Hollands Archief).

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
