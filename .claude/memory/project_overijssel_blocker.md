---
name: Overijssel MAIS token blocker
description: Why Overijssel is incomplete and exactly what needs to be implemented to finish it
type: project
originSessionId: f010dbb7-39a9-4ee7-9487-db07934e9335
---
The Historisch Centrum Overijssel (HCO) uses the MAIS Internet viewer system. Images at `preserve2.archieven.nl` require three per-page auth tokens in the query string: `miahd`, `rdt`, `open`. Without them the server returns HTTP 202 + an SVG placeholder.

These tokens are only available inside a live browser JS session — the MAIS stk3 viewer endpoint returns an empty `<div>` when called with plain `requests`.

**Two concrete TODOs** (see also `CLAUDE.md` and `python/overijssel.py`):

1. **Fill in `KANTOOR_MINR` dict** — only Almelo (`minr=2227676`) is known. The other 9 kantoren (Deventer, Enschede, Hardenberg, Kampen, Oldenzaal, Ommen, Steenwijk, Zwolle, Overige) need their `minr` values found by browsing the collectieoverijssel.nl tree view for `miadt=141, micode=0136.4`.

2. **Implement `_fetch_page_tokens_via_playwright(minr)`** — use `playwright-python` (add to `pyproject.toml`) to load the MAIS viewer, wait for thumbnails to render, then parse `miahd/rdt/open` from `<img>` src attributes in the stk3 strip.

**Why:** MAIS requires a valid `PHPSESSID` + `mi_sessid` cookie pair (set automatically when the viewer page loads in a browser) before the stk3 endpoint returns real content.
