# Memories van Successie – Download Pipeline

Downloads all surviving *Memories van Successie* (Dutch succession/inheritance registers, 1806–1927) from eight regional Dutch archives and saves the scans with structured metadata.

## What are Memories van Successie?

When someone died in the Netherlands between 1806 and 1927, their heirs were required to register the estate with the local tax office (*kantoor van successie*). These registers are a goldmine for genealogical research: they record the name of the deceased, the date and place of death, heirs and their relationships, and the value of the estate.

The registers are organised by fiscal district (*kantoor*) and contain individual entries (*akten*). **Tafel V-bis** (an appendix covering special cases) is excluded from all pipelines in this project.

---

## Archive coverage

| Province | Archive | System | Status |
|---|---|---|---|
| Noord-Brabant | BHIC | Open Archieven | ✅ |
| Zeeland | Zeeuws Archief | Open Archieven | ✅ |
| Utrecht | Het Utrechts Archief | Open Archieven | ✅ |
| Gelderland | Gelders Archief | Open Archieven | ✅ |
| Noord-Holland | Noord-Hollands Archief | Open Archieven | ✅ |
| Zuid-Holland | Nationaal Archief | Custom scraper | ✅ |
| Drenthe | Drents Archief | Memorix REST API | ✅ |
| Overijssel | Historisch Centrum Overijssel | MAIS viewer + Playwright | ✅ |

---

## Quick start

**Requirements**: Python ≥ 3.14, [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync

# First-time Overijssel setup (Playwright/Chromium)
playwright install chromium

# Download all archives (takes several hours)
uv run python main.py all

# Or run one archive at a time
uv run python main.py openarchieven
uv run python main.py nationaalarchief
uv run python main.py drentsarchief
uv run python main.py overijssel
```

---

## Pipelines in detail

### Open Archieven (5 archives)

`uv run python main.py openarchieven`

Covers: BHIC (Noord-Brabant), Zeeuws Archief, Het Utrechts Archief, Gelders Archief, Noord-Hollands Archief.

**Three steps run in sequence:**

1. **Step 1** (`python/step1_collect_record_guids_from_search_api.py`)  
   Queries the Open Archieven search API (`api.openarch.nl`) for each archive and collects all record GUIDs. Output: `records.csv`.

2. **Step 2** (`python/step2_oai_pmh_dumps.py`)  
   Downloads the full OAI-PMH XML export for each archive from `openarchieven.nl/exports/xml/`, parses the A2A records, filters to *Memories van Successie* only (excluding Tafel V-bis), and extracts scan URLs and metadata. Output: `scan_urls.csv`.

3. **Step 3** (`python/step3_download_steps.py`)  
   Downloads every scan JPEG listed in `scan_urls.csv` and writes a `metadata.json` sidecar next to each group of scans. Output: `scans/openarchieven/{archive}/{record_id}/`.

---

### Nationaal Archief – Zuid-Holland

`uv run python main.py nationaalarchief`  
Source file: `python/nationaalarchief.py`

Access number **3.06.05**. The pipeline:
1. Fetches the inventory listing page for section 2.4 (inventory numbers 2276–2357).
2. For each inventory number, loads the viewer page and extracts scan UUIDs from the embedded `drupal-settings-json` data block.
3. Downloads full-size scans from `service.archief.nl/api/file/v1/default/{UUID}`.

Progress is tracked in `nationaalarchief_done.txt` so interrupted runs can be resumed.  
Output: `scans/nationaalarchief/{invnr}/`.

---

### Drents Archief

`uv run python main.py drentsarchief`  
Source file: `python/drentsarchief.py`

Uses the **Memorix genealogy REST API** at `webservices.memorix.nl/genealogy`.

1. Searches all persons with deed type `Successiememories` (paginated).
2. Collects unique deed IDs and fetches the deed detail for each.
3. Downloads all `asset[].download` URLs (full-size JPEGs).

Progress is tracked in `drentsarchief_deeds.csv`.  
Output: `scans/drentsarchief/{deed_id}/`.

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

**First-time setup**: run `playwright install chromium` after `uv sync`.

Covers all 10 kantoren: Almelo, Deventer, Enschede, Goor, Kampen, Ommen, Raalte, Steenwijk, Vollenhove, Zwolle.

---

## Output structure

```
scans/
├── openarchieven/
│   ├── bhi/{record_id}/          ← Noord-Brabant (BHIC)
│   │   ├── metadata.json
│   │   ├── 1.jpg
│   │   └── 2.jpg …
│   ├── zar/{record_id}/          ← Zeeland
│   ├── hua/{record_id}/          ← Utrecht
│   ├── gra/{record_id}/          ← Gelderland
│   └── nha/{record_id}/          ← Noord-Holland
├── nationaalarchief/{invnr}/
│   ├── metadata.json
│   └── NL-HaNA_3.06.05_{invnr}_*.jpg
├── drentsarchief/{deed_id}/
│   ├── metadata.json
│   └── 0001.jpg …
└── overijssel/{kantoor}/{invnr}/
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

- **Open Archieven step 3**: skips files that already exist and have a non-zero size.
- **Nationaal Archief**: tracks completed inventory numbers in `nationaalarchief_done.txt`.
- **Drents Archief**: tracks completed deeds in `drentsarchief_deeds.csv` (rows with `status=done` are skipped).
