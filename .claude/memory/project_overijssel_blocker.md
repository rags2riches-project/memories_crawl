---
name: Overijssel MAIS token extraction — SOLVED
description: How MAIS per-page auth tokens are extracted and what the complete kantoor minr mapping is
type: project
---
The Historisch Centrum Overijssel (HCO) uses the MAIS Internet viewer system. Images at `preserve2.archieven.nl` require three per-page auth tokens: `miahd`, `rdt`, `open`. These are **per-page**: each scan page has its own unique `miahd` and `open`.

**SOLVED April 2026** — `python/overijssel.py` is fully implemented.

## How token extraction works

1. Navigate (Playwright/Chromium) to the MAIS inv3 page for the kantoor minr:
   `https://collectieoverijssel.nl/collectie/archieven/?mivast=20&mizig=210&miadt=141&miaet=1&micode=0136.4&minr={minr}&milang=nl&miview=inv3`
2. The page auto-loads PHPSESSID + mi_sessid cookies (WordPress + MAIS).
3. Collect all `a[onclick*="stk3"]` links — each is one invnr volume.
4. For each: call `mi_inv3_toggle_stk(args)` via `page.evaluate()`.
5. Wait ~1.5s, harvest `img[src*="/fonc-hco/"]` from the DOM.
6. Parse `invnr`, `page`, `miahd`, `rdt`, `open` from each src URL.

## Image URL format (corrected — original stub was wrong)

```
https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4/{invnr}/
    NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg
    ?miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={open}
```

Note the `{invnr}/` subdirectory — the original stub was missing this.

## Complete KANTOOR_MINR mapping (verified April 2026)

```python
KANTOOR_MINR = {
    "Almelo":     2227676,
    "Deventer":   2227950,
    "Enschede":   2228207,
    "Goor":       2228335,   # original stub had wrong names: Hardenberg, Oldenzaal, Overige
    "Kampen":     2228502,
    "Ommen":      2228649,
    "Raalte":     2228752,
    "Steenwijk":  2228889,
    "Vollenhove": 2228980,
    "Zwolle":     2229046,
}
```
