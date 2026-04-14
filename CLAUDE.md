# CLAUDE.md – Memories van Successie Pipeline

## What this project does

Downloads all surviving *Memories van Successie* (Dutch succession/inheritance registers, 1806–1927) from eight regional Dutch archives. Each scan is saved alongside a `metadata.json` sidecar.

## How to run

```bash
uv run python main.py openarchieven      # BHIC, Zeeuws Archief, HUA, Gelders, NHA
uv run python main.py nationaalarchief   # Zuid-Holland (Nationaal Archief 3.06.05)
uv run python main.py drentsarchief      # Drenthe (Memorix API)
uv run python main.py overijssel         # ⚠️ INCOMPLETE – see below
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
| `python/overijssel.py` | Overijssel: **concept only**, see TODOs below |

## Exclusion rule

**Always exclude Tafel V-bis.** In all parsers and filters, skip any record whose SourceType contains "tafel" or "v-bis" (case-insensitive). The Nationaal Archief Tafel V-bis items are in a different inventory section (outside 2276–2357) and are excluded by range.

---

## ⚠️ Overijssel – TODO list

The Historisch Centrum Overijssel (HCO) uses a MAIS Internet viewer system. Scans are at:

```
https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4/
    NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg
    ?miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
```

**Without the tokens you get HTTP 202 + an SVG placeholder — not the real image.**

The tokens are injected by the MAIS JavaScript viewer (viewer3.js from srv.archieven.nl). Fetching the stk3 endpoint with plain `requests` always returns an empty `<div>`.

### TODO 1 – Find minr values for all 10 kantoren

In `python/overijssel.py`, the dict `KANTOOR_MINR` maps kantoor name → `minr` (MAIS item ID for the "Memories van Successie" sub-item of that kantoor). Only Almelo is filled in:

```python
KANTOOR_MINR = {
    "Almelo":     2227676,   # confirmed
    "Deventer":   None,      # need to discover
    "Enschede":   None,
    ...
}
```

**How to find them**: Navigate to the collectieoverijssel.nl viewer for access code `0136.4` (`miadt=141, mivast=20`), open the tree, and read the `minr=...` parameter from the link for each kantoor's "Memories van Successie" item (not the "Alfabetische Tafel/Klapper" items).

Direct viewer URL pattern:
```
https://collectieoverijssel.nl/wp-content/plugins/mais-mdws/maisi_ajax_proxy.php
  ?mivast=20&mizig=210&miadt=141&miaet=1&micode=0136.4&minr={MINR}&milang=nl&miview=viewer
```

### TODO 2 – Implement `_fetch_page_tokens_via_playwright(minr)`

This function in `python/overijssel.py` needs to:

1. Launch a browser (Playwright/Chromium) and navigate to the collectieoverijssel.nl viewer for the given `minr`.
2. Wait for the stk3 thumbnail strip to fully render (all `<img>` tags loaded).
3. Extract each `<img src="...">` and parse out `invnr`, `page`, `miahd`, `rdt`, `open` from the URL.
4. Return a list of dicts with those fields.

The stk3 URL that loads the thumbnails:
```
https://collectieoverijssel.nl/wp-content/plugins/mais-mdws/maisi_ajax_proxy.php
  ?mivast=20&mizig=210&miadt=141&miaet=1&micode=0136.4&minr={minr}&milang=nl&miview=stk3
```
This must be fetched *from within* a browser session on collectieoverijssel.nl (requires `PHPSESSID` + `mi_sessid` cookies set by the viewer page).

**Dependency to add** to `pyproject.toml`:
```toml
dependencies = [
    "requests>=2.33.1",
    "playwright>=1.40",   # add this
]
```
And run `playwright install chromium` after installing.

### TODO 3 – Verify page count per inventory item

The known example (Kantoor Almelo) has 1372 pages. Other kantoren likely have different counts. The stk3 approach in TODO 2 will naturally give the correct count because it enumerates all thumbnail images. No separate page-count API call is needed.

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
