---
name: Memories van Successie pipeline
description: Python pipeline to download Dutch succession registers from 8 regional archives; all archives implemented
type: project
---
Downloads all *Memories van Successie* (Dutch inheritance registers, 1806–1927) from 8 Dutch regional archives. Scans are saved with `metadata.json` sidecars.

**Status** (as of 2026-04-14):
- ✅ Open Archieven (5 archives: BHIC, Zeeuws Archief, HUA, Gelders, NHA) — 3-step pipeline via OAI-PMH XML dumps
- ✅ Nationaal Archief (Zuid-Holland, access 3.06.05) — Drupal viewer scrape
- ✅ Drents Archief (Drenthe) — Memorix REST API
- ✅ Overijssel (HCO) — Playwright-based MAIS token extraction, implemented April 2026

**Why:** Complete download for genealogical/historical research purposes.

**How to apply:** `python/overijssel.py` is fully implemented. Run with `uv run python main.py overijssel`. Requires `playwright install chromium` after `uv sync`.
