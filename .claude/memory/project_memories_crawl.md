---
name: Memories van Successie pipeline
description: Python pipeline to download Dutch succession registers from 8 regional archives; Overijssel incomplete
type: project
originSessionId: f010dbb7-39a9-4ee7-9487-db07934e9335
---
Downloads all *Memories van Successie* (Dutch inheritance registers, 1806–1927) from 8 Dutch regional archives. Scans are saved with `metadata.json` sidecars.

**Status** (as of 2026-04-14):
- ✅ Open Archieven (5 archives: BHIC, Zeeuws Archief, HUA, Gelders, NHA) — 3-step pipeline via OAI-PMH XML dumps
- ✅ Nationaal Archief (Zuid-Holland, access 3.06.05) — Drupal viewer scrape
- ✅ Drents Archief (Drenthe) — Memorix REST API
- ⚠️ Overijssel (HCO) — concept stub only, blocked on MAIS auth tokens

**Why:** Complete download for genealogical/historical research purposes.

**How to apply:** Check `python/overijssel.py` and `CLAUDE.md` for the Overijssel TODOs when continuing work on that module.
