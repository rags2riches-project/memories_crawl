"""Overijssel – Memories van Successie downloader (CONCEPT / STUB).

Archive: Historisch Centrum Overijssel (HCO)
MAIS system: miadt=141, mivast=20, archive code 0136.4
Proxy:  https://collectieoverijssel.nl/wp-content/plugins/mais-mdws/maisi_ajax_proxy.php

STATUS: INCOMPLETE – see TODOs below.

What is known
─────────────
• Images are served by:
    https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4/
        NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg
    Full-size (JPEG ~2 MB): add query params  ?miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
    Thumbnail (PNG ~43 KB):                   ?format=thumb&miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
    Without tokens → HTTP 202 + SVG placeholder (not the real image).

• MAIS tree structure:
    - 10 kantoren, each with two sub-items in access 0136.4:
        * "Memories van Successie"  → include (minr values below)
        * "Alfabetische Tafel" / "Klapper" → EXCLUDE
    - Known minr values (Kantoor Almelo example):
        MvS items 1–1577:   minr=2227676   (viewer: mizig=210, miaet=1, micode=0136.4)
        Alfabetische Tafel: minr=2227919   ← EXCLUDE

• The MAIS stk3 viewer endpoint (miview=stk3) returns the thumbnail list with per-page
  miahd / rdt / open values embedded in <img> src attributes – but only when executed
  inside the collectieoverijssel.nl browser session (requires JS + session cookie).
  Fetching it with plain requests always returns an empty <div>.

TODOs
─────
1. Find the minr values for all 10 kantoren (not just Almelo).
   Approach A: Use playwright-python to navigate the MAIS tree and scrape minr links.
   Approach B: Manually enumerate via browser devtools and hard-code the dict below.

2. Extract per-image tokens (miahd, rdt, open) for all pages.
   The stk3 endpoint returns HTML with <img> tags that carry these values.
   Approach: Use playwright-python to load the stk3 viewer for each minr, wait for
   the thumbnails to render, then read the img src attributes from the DOM.

3. Once tokens are available, downloading is straightforward (see _download_file below).

Dependency needed: pip install playwright && playwright install chromium
(add playwright to pyproject.toml before using this module)
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests

ARCHIVE_NAME = "Historisch Centrum Overijssel"
ARCHIVE_NUMBER = "0136.4"
MAIS_ADT = "141"
MAIS_VAST = "20"
PROXY_BASE = (
    "https://collectieoverijssel.nl/wp-content/plugins/mais-mdws/maisi_ajax_proxy.php"
)
IMAGE_BASE = "https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4"
OUTPUT_DIR = Path("scans/overijssel")
USER_AGENT = "memories-crawl/1.0"

# Known minr values for each kantoor's Memories van Successie sub-item.
# Values marked None need to be discovered (see TODO 1 above).
# Minr values for the "Alfabetische Tafel / Klapper" sub-items are NOT listed here
# because we only process MvS items.
KANTOOR_MINR: dict[str, int | None] = {
    "Almelo":     2227676,
    "Deventer":   None,   # TODO: discover via browser
    "Enschede":   None,
    "Hardenberg": None,
    "Kampen":     None,
    "Oldenzaal":  None,
    "Ommen":      None,
    "Steenwijk":  None,
    "Zwolle":     None,
    "Overige":    None,
}


def _image_url(invnr: int, page: int, miahd: int, rdt: str, open_token: str) -> str:
    filename = f"NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg"
    return (
        f"{IMAGE_BASE}/{filename}"
        f"?miadt={MAIS_ADT}&miahd={miahd}&mivast={MAIS_VAST}&rdt={rdt}&open={open_token}"
    )


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return "exists"
    resp = session.get(url, stream=True, timeout=120)
    if resp.status_code in (404, 202):
        return "missing"
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(65536):
            if chunk:
                f.write(chunk)
    return "downloaded"


def _write_metadata(dest_dir: Path, kantoor: str, invnr: int, n_scans: int) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": ARCHIVE_NUMBER,
        "brontype": "Memorie van Successie",
        "kantoor": kantoor,
        "inventarisnummer": str(invnr),
        "n_scans": n_scans,
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _fetch_page_tokens_via_playwright(minr: int) -> list[dict]:
    """Return list of {invnr, page, miahd, rdt, open} dicts for all pages in minr.

    Requires: pip install playwright && playwright install chromium
    TODO: implement this function.
    """
    raise NotImplementedError(
        "Token extraction via Playwright is not yet implemented.\n"
        "See the module docstring for the approach."
    )


def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for kantoor, minr in KANTOOR_MINR.items():
        if minr is None:
            print(f"  {kantoor}: minr unknown – skipped (see TODO 1 in overijssel.py)")
            continue

        print(f"  {kantoor} (minr={minr}): fetching page tokens …")
        try:
            pages = _fetch_page_tokens_via_playwright(minr)
        except NotImplementedError as exc:
            print(f"    SKIPPED – {exc}")
            continue

        print(f"    {len(pages)} pages found")
        for p in pages:
            invnr = p["invnr"]
            dest_dir = OUTPUT_DIR / kantoor / str(invnr)
            dest = dest_dir / f"{p['page']:04d}.jpg"
            url = _image_url(invnr, p["page"], p["miahd"], p["rdt"], p["open"])
            status = _download_file(session, url, dest)
            if status == "downloaded":
                _write_metadata(dest_dir, kantoor, invnr, len(pages))
            time.sleep(0.2)

    print("Done (Overijssel – partially implemented).")


if __name__ == "__main__":
    main()
