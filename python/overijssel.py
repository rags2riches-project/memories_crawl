"""Overijssel – Memories van Successie downloader.

Archive: Historisch Centrum Overijssel (HCO)
MAIS system: miadt=141, mivast=20, archive code 0136.4
WordPress proxy: https://collectieoverijssel.nl/wp-content/plugins/mais-mdws/maisi_ajax_proxy.php

How it works
────────────
Images live at:
    https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4/{invnr}/
        NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg
    Full-size: add query params  ?miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
    Thumbnail: add              ?format=thumb&miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
    Without tokens → HTTP 202 + SVG placeholder.

Tokens are per-page (miahd, open) and per-item (rdt). They are only visible in the
``<img src>`` attributes of the MAIS stk3 thumbnail strip, which is rendered by JavaScript
inside the collectieoverijssel.nl browser session.

Strategy
────────
1. Navigate to the inv3 page for each kantoor's MvS minr (Playwright / Chromium).
   This establishes the MAIS PHPSESSID + mi_sessid session cookies.
2. Collect all child-item stk3 links from the DOM
   (``a[onclick*="stk3"]``; each link corresponds to one invnr volume).
3. For each link: call ``mi_inv3_toggle_stk(...)`` via evaluate() to trigger the AJAX
   stk3 strip. Wait for thumbnails. Extract ``img[src*="/fonc-hco/"]`` hrefs.
4. Parse invnr, page, miahd, rdt, open from each src URL.
5. Download full-size images; write metadata.json sidecars.

Dependency: ``playwright`` must be installed and ``playwright install chromium`` run.
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
IMAGE_BASE = "https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4"
OUTPUT_DIR = Path("scans/overijssel")
USER_AGENT = "memories-crawl/1.0"

# minr values for each kantoor's "Memories van Successie" item in the MAIS tree.
# These were discovered by browsing the collectieoverijssel.nl inv3 tree for
# miadt=141, mivast=20, micode=0136.4 (verified April 2026).
# The "Alfabetische Tafel / Klapper" sub-items are NOT listed here.
KANTOOR_MINR: dict[str, int] = {
    "Almelo":     2227676,
    "Deventer":   2227950,
    "Enschede":   2228207,
    "Goor":       2228335,
    "Kampen":     2228502,
    "Ommen":      2228649,
    "Raalte":     2228752,
    "Steenwijk":  2228889,
    "Vollenhove": 2228980,
    "Zwolle":     2229046,
}

_INV3_URL = (
    "https://collectieoverijssel.nl/collectie/archieven/"
    "?mivast=20&mizig=210&miadt=141&miaet=1&micode=0136.4"
    "&minr={minr}&milang=nl&miview=inv3"
)

# JS to collect all stk3 toggle-call argument strings from the inv3 DOM
_JS_COLLECT_STK3 = """() => {
    return Array.from(document.querySelectorAll('a[onclick*="stk3"]')).map(a => {
        const oc = a.getAttribute('onclick');
        const m = oc.match(/mi_inv3_toggle_stk\\((.+?)\\);\\s*return/s);
        return m ? m[1] : null;
    }).filter(Boolean);
}"""

# JS to snapshot which strip IDs are currently in mi_strip_store
_JS_STRIP_IDS = "() => Object.keys(mi_strip_store || {})"

# JS to force-load all chunks for a specific strip ID, returns total page count
_JS_FORCE_LOAD_STRIP = """(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.aantal) return 0;
    const chunkSize = r.numloadScans || 25;
    const totalChunks = Math.ceil(r.aantal / chunkSize);
    for (let chunk = 0; chunk < totalChunks; chunk++) {
        if (r.loadedChunks.indexOf(chunk) === -1) {
            r.cursor = chunk * chunkSize + 1;
            r.dir = -1;
            r.populate();
        }
    }
    return r.aantal;
}"""

# JS to check whether a specific strip has all scans rendered in the DOM.
# loadedChunks is pushed immediately when fetch fires (not on response), so
# we poll loadedScans instead, which is incremented by mi_strip_populate.
_JS_STRIP_LOADED = """(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.aantal) return true;
    return (r.loadedScans || []).length >= r.aantal;
}"""

# JS to harvest fonc-hco thumbnail srcs from a single strip's slider element,
# then remove the strip from the store and DOM to free memory.
_JS_HARVEST_STRIP = """(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.sslider) return [];
    const srcs = Array.from(r.sslider.querySelectorAll('img[src*="/fonc-hco/"]')).map(i => i.src);
    if (r.strip && r.strip.parentNode) r.strip.parentNode.removeChild(r.strip);
    delete mi_strip_store[stripId];
    return srcs;
}"""

# JS to harvest all preserve2 /fonc-hco/ thumbnail srcs currently in the DOM
_JS_HARVEST_IMGS = """() => {
    return Array.from(document.querySelectorAll('img[src*="/fonc-hco/"]')).map(i => i.src);
}"""

_SRC_RE = re.compile(
    r"NL-ZlHCO_0136\.4_(\d+)_(\d+)\.jpg[^?]*\?"
    r".*?miahd=(\d+).*?rdt=([^&]+).*?open=([^&\"']+)"
)


def _parse_thumb_src(src: str) -> dict | None:
    m = _SRC_RE.search(src)
    if not m:
        return None
    return {
        "invnr": int(m.group(1)),
        "page": int(m.group(2)),
        "miahd": int(m.group(3)),
        "rdt": m.group(4),
        "open": m.group(5),
    }


def _get_token_cache_path(minr: int) -> Path:
    """Return path to the token cache file for a given minr."""
    return OUTPUT_DIR / f"tokens_minr_{minr}.json"


def _load_cached_tokens(minr: int) -> list[dict] | None:
    """Load cached page tokens if they exist and are non-empty."""
    cache_path = _get_token_cache_path(minr)
    if cache_path.exists():
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                tokens = json.load(f)
            if tokens:
                print(f"    loaded {len(tokens)} cached tokens")
                return tokens
        except (json.JSONDecodeError, IOError):
            pass
    return None


def _save_cached_tokens(minr: int, tokens: list[dict]) -> None:
    """Save page tokens to cache file."""
    cache_path = _get_token_cache_path(minr)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def _fetch_page_tokens_via_playwright(minr: int) -> list[dict]:
    """Return [{invnr, page, miahd, rdt, open}, ...] for every scan page under minr.

    Launches a headless Chromium browser, navigates to the MAIS inv3 page for the
    given kantoor minr, and iterates over all child item stk3 strips to extract
    per-page auth tokens.

    Results are cached to avoid re-fetching on subsequent runs.

    Requires: pip install playwright && playwright install chromium
    """
    # Try to load from cache first
    cached = _load_cached_tokens(minr)
    if cached is not None:
        return cached

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    # last-wins dedup: later stk3 calls give more-specific tokens than auto-load
    pages_by_key: dict[tuple[int, int], dict] = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        # Do NOT override User-Agent: the MAIS proxy returns no content for non-browser UAs.

        # Navigate to inv3 — MAIS fires an AJAX call via mi_useprox to populate items.
        # Wait for networkidle so scripts and the initial proxy AJAX both complete, then
        # confirm the stk3 links are actually in the DOM before proceeding.
        page.goto(_INV3_URL.format(minr=minr), wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick*="stk3"]', state="attached", timeout=30_000)

        # Collect all stk3 argument strings from child item links
        stk3_arg_list: list[str] = page.evaluate(_JS_COLLECT_STK3)
        print(f"    found {len(stk3_arg_list)} stk3 items")

        for idx, args in enumerate(stk3_arg_list):
            # Snapshot existing strip IDs so we can identify the new one below
            before_ids: set[str] = set(page.evaluate(_JS_STRIP_IDS))

            # Trigger the stk3 strip for this item
            page.evaluate(f"mi_inv3_toggle_stk({args})")
            page.wait_for_timeout(1_500)

            # Find the newly created strip (if any)
            after_ids: set[str] = set(page.evaluate(_JS_STRIP_IDS))
            new_ids = after_ids - before_ids

            if new_ids:
                strip_id = next(iter(new_ids))
                # Force-load all chunks beyond the initial 25, then poll until done.
                page.evaluate(_JS_FORCE_LOAD_STRIP, strip_id)
                for _ in range(120):  # up to 60 s for very large invnrs
                    if page.evaluate(_JS_STRIP_LOADED, strip_id):
                        break
                    page.wait_for_timeout(500)

                # Harvest thumbnails from this strip only, then remove from store.
                srcs: list[str] = page.evaluate(_JS_HARVEST_STRIP, strip_id)
            else:
                # No new strip — fall back to harvesting the full DOM (rare case)
                srcs = page.evaluate(_JS_HARVEST_IMGS)

            for src in srcs:
                rec = _parse_thumb_src(src)
                if rec:
                    pages_by_key[(rec["invnr"], rec["page"])] = rec

            if (idx + 1) % 25 == 0:
                print(f"    processed {idx + 1}/{len(stk3_arg_list)} items, "
                      f"{len(pages_by_key)} pages so far")

        browser.close()

    result = sorted(pages_by_key.values(), key=lambda r: (r["invnr"], r["page"]))
    print(f"    total pages collected: {len(result)}")

    # Save to cache for future runs
    _save_cached_tokens(minr, result)

    return result


def _image_url(invnr: int, page: int, miahd: int, rdt: str, open_token: str) -> str:
    filename = f"NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg"
    return (
        f"{IMAGE_BASE}/{invnr}/{filename}"
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
    # Always rewrite: n_scans may have been wrong on a prior truncated run.
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


def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for kantoor, minr in KANTOOR_MINR.items():
        print(f"\n  {kantoor} (minr={minr}): fetching page tokens via Playwright …")
        pages = _fetch_page_tokens_via_playwright(minr)

        if not pages:
            print(f"    WARNING: no pages found for {kantoor}")
            continue

        # Group by invnr to write per-invnr metadata
        invnr_pages: dict[int, list[dict]] = {}
        for p in pages:
            invnr_pages.setdefault(p["invnr"], []).append(p)

        downloaded = skipped = missing = 0
        for invnr, inv_pages in sorted(invnr_pages.items()):
            dest_dir = OUTPUT_DIR / kantoor / str(invnr)
            _write_metadata(dest_dir, kantoor, invnr, len(inv_pages))
            for p in inv_pages:
                dest = dest_dir / f"{p['page']:04d}.jpg"
                url = _image_url(invnr, p["page"], p["miahd"], p["rdt"], p["open"])
                status = _download_file(session, url, dest)
                if status == "downloaded":
                    downloaded += 1
                elif status == "exists":
                    skipped += 1
                else:
                    missing += 1
                time.sleep(0.15)

        print(f"    {kantoor}: {downloaded} downloaded, {skipped} existing, {missing} missing")

    print("\nDone (Overijssel).")


if __name__ == "__main__":
    main()
