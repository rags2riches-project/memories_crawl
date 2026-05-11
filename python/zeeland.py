"""Zeeland – Memories van Successie downloader.

Archive: Zeeuws Archief
MAIS system: miadt=239, mivast=239, archive code 398
Base URL: https://www.zeeuwsarchief.nl/onderzoek-het-zelf/archief/
Image server: preserve-zaf.archieven.nl/mi-239/fonc-zaf/398/

How it works
────────────
The Zeeuws Archief uses the MAIS Internet viewer on the zeeuwsarchief.nl domain.
The archive is identified by micode=398 ("Ontvangers der Successierechten in Zeeland,
(1795) 1806-1927").

The inventory tree has a three-level structure:
  Root (398)
  └── Kantoor (via mi_inv3_openinv, e.g. Goes minr=33439946)
      └── inv3 page with sub-sections (expand via swapinv)
          └── Inventarisnummers (each has its own minr, with h_scan markers)

Scans are accessed by navigating to each inventarisnummer's inv2 page:
  …&miview=inv2&minr={invnr_minr}
The strip viewer auto-loads on this page (25 thumbnails at a time).  We force-load
all strip chunks via the mi_strip_store's populate() method, then harvest thumbnail
URLs from <img src*="fonc-zaf"> elements.  Full-size downloads replace ?format=thumb.

Strategy
────────
1. Navigate to the inv2 page; extract kantoor minr values from the initial DOM.
2. For each kantoor: navigate to its inv3 page, expand all sub-sections via swapinv
   clicks, then harvest invnr minr values and texts from stk3 onclick handlers.
   Filter out Tafel V-bis / non-digitized (no h_scan marker) items.
3. For each digitized invnr: navigate to inv2&minr={invnr_minr}, force-load all
   strip chunks, harvest thumbnail fonc-zaf URLs, parse tokens.
4. Derive full-size URLs by stripping ?format=thumb and download.
5. Cache harvested tokens per kantoor minr so reruns skip Playwright.

Dependency: ``uv sync && uv run playwright install chromium``.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARCHIVE_NAME = "Zeeuws Archief"
ARCHIVE_NUMBER = "398"
MAIS_ADT = "239"
MAIS_VAST = "239"
OUTPUT_DIR = Path("scans/zeeland")
USER_AGENT = "memories-crawl/1.0"

# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

_INV2_URL = (
    "https://www.zeeuwsarchief.nl/onderzoek-het-zelf/archief/"
    f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
    f"&micode={ARCHIVE_NUMBER}&milang=nl&miview=inv2"
)


def _inv3_url(minr: int) -> str:
    return (
        "https://www.zeeuwsarchief.nl/onderzoek-het-zelf/archief/"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miaet=1&micode={ARCHIVE_NUMBER}&minr={minr}"
        f"&milang=nl&miview=inv3"
    )


def _inv2_minr_url(minr: int) -> str:
    """Navigate to a specific inventarisnummer via inv2 view; strip auto-loads."""
    return (
        "https://www.zeeuwsarchief.nl/onderzoek-het-zelf/archief/"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miview=inv2&milang=nl&micode={ARCHIVE_NUMBER}&minr={minr}"
    )


# ---------------------------------------------------------------------------
# JavaScript snippets for kantoor discovery
# ---------------------------------------------------------------------------

_JS_EXTRACT_KANTOREN = r"""() => {
    const links = document.querySelectorAll('.mi_tree_node a[onclick*="mi_inv3_openinv"]');
    const result = [];
    const seen = new Set();
    for (const a of links) {
        const onclick = a.getAttribute('onclick') || '';
        const text = (a.textContent || '').trim();
        const m = onclick.match(/minr=(\d+)/);
        if (!m) continue;
        const minr = parseInt(m[1], 10);
        if (seen.has(minr)) continue;
        seen.add(minr);
        if (text.match(/^\d+\.\s*Kantoor/)) {
            result.push({ name: text.substring(0, 120), minr });
        }
    }
    return result;
}"""

_JS_EXPAND_SUBSECTIONS = """() => {
    const links = document.querySelectorAll(
        'a[onclick*="swapinv"]:not([data-mvs-expanded])');
    let count = 0;
    links.forEach(a => {
        a.setAttribute('data-mvs-expanded', '1');
        a.click();
        count++;
    });
    return count;
}"""

_JS_COLLECT_INVNRS = """() => {
    return Array.from(document.querySelectorAll('a[onclick*="stk3"]')).map(a => {
        const oc = a.getAttribute('onclick') || '';
        const text = (a.textContent || '').trim().substring(0, 200);
        if (!text || text === 'Toon details van deze beschrijving') return null;
        const low = text.toLowerCase();
        if (low.includes('tafel') || low.includes('v-bis') || low.includes('5bis')) return null;
        const m = oc.match(/minr=(\\d+)/);
        if (!m) return null;
        const minr = parseInt(m[1], 10);
        const numMatch = text.match(/^(\\d+)\\s/);
        const invnr = numMatch ? parseInt(numMatch[1], 10) : 0;
        const node = a.closest('.mi_tree_node');
        const hasScan = node && node.querySelector('img[src*="h_scan"]') != null;
        return { invnr, text, minr, hasScan };
    }).filter(Boolean);
}"""

# ---------------------------------------------------------------------------
# JavaScript snippets for strip force-load and harvest (inv2 minr page)
# ---------------------------------------------------------------------------

_JS_STRIP_FORCE_LOAD = """() => {
    const keys = Object.keys(mi_strip_store || {});
    if (!keys.length) return { total: 0, chunks: 0 };
    const r = mi_strip_store[keys[0]];
    const cs = r.numloadScans || 25;
    const totalChunks = Math.ceil(r.aantal / cs);
    for (let c = 0; c < totalChunks; c++) {
        if ((r.loadedChunks || []).indexOf(c) === -1) {
            r.cursor = c * cs + 1;
            r.dir = -1;
            r.populate();
        }
    }
    return { total: r.aantal, chunks: totalChunks, key: keys[0] };
}"""

_JS_STRIP_LOADED = """() => {
    const keys = Object.keys(mi_strip_store || {});
    if (!keys.length) return true;
    const r = mi_strip_store[keys[0]];
    return (r.loadedScans || []).length >= r.aantal;
}"""

_JS_HARVEST_THUMBS = (
    "() => Array.from(document.querySelectorAll('img[src*=\"fonc-zaf\"]'))"
    "          .map(i => i.src)"
)

# ---------------------------------------------------------------------------
# Thumbnail URL parsing and full-size URL construction
# ---------------------------------------------------------------------------

_IMG_URL_RE = re.compile(
    r"/fonc-zaf/\d+/(?P<invnr>\d+)/"
    r"NL-MdbZA_\d+_\d+_(?P<slug>.+?)_(?P<page>\d+)\.jpg"
    r"\?.*?miahd=(?P<miahd>\d+)"
    r".*?rdt=(?P<rdt>[^&]+)"
    r".*?open=(?P<open>[^&]+)"
)


def _parse_thumb_url(url: str) -> dict | None:
    """Parse a fonc-zaf thumbnail URL into {invnr, page, slug, miahd, rdt, open, thumb_url}.

    ``slug`` is the filename portion between invnr and page, e.g. ``1-1`` or empty.
    It provides uniqueness when different scan segments share the same trailing page number.
    """
    m = _IMG_URL_RE.search(url)
    if not m:
        # Fallback: files like NL-MdbZA_398_1_0001.jpg (no slug)
        alt = re.search(
            r"/fonc-zaf/\d+/(?P<invnr>\d+)/"
            r"NL-MdbZA_\d+_\d+_(?P<page>\d+)\.jpg"
            r"\?.*?miahd=(?P<miahd>\d+)"
            r".*?rdt=(?P<rdt>[^&]+)"
            r".*?open=(?P<open>[^&]+)",
            url,
        )
        if not alt:
            return None
        return {
            "invnr": int(alt.group("invnr")),
            "page": int(alt.group("page")),
            "slug": "",
            "miahd": int(alt.group("miahd")),
            "rdt": alt.group("rdt"),
            "open": alt.group("open"),
            "thumb_url": url,
        }
    return {
        "invnr": int(m.group("invnr")),
        "page": int(m.group("page")),
        "slug": m.group("slug"),
        "miahd": int(m.group("miahd")),
        "rdt": m.group("rdt"),
        "open": m.group("open"),
        "thumb_url": url,
    }


def _fullsize_url(thumb_url: str) -> str:
    url = thumb_url.replace("?format=thumb&", "?")
    url = url.replace("&format=thumb", "")
    url = url.replace("?format=thumb", "")
    if "format=large" not in url:
        url = url.replace("?miadt=", "?format=large&miadt=")
    return url


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

def _token_cache_path(kantoor_minr: int) -> Path:
    return OUTPUT_DIR / f"tokens_minr_{kantoor_minr}.json"


def _partial_cache_path(kantoor_minr: int) -> Path:
    return OUTPUT_DIR / f"tokens_minr_{kantoor_minr}_partial.json"


def _load_cached_tokens(kantoor_minr: int) -> list[dict] | None:
    for path in (_token_cache_path(kantoor_minr), _partial_cache_path(kantoor_minr)):
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    tokens = json.load(f)
                if tokens:
                    label = (
                        "complete"
                        if path.name.startswith("tokens_") and "partial" not in path.name
                        else "partial"
                    )
                    print(f"    loaded {len(tokens)} cached tokens ({label})")
                    return tokens
            except (json.JSONDecodeError, OSError):
                pass
    return None


def _save_cached_tokens(kantoor_minr: int, tokens: list[dict]) -> None:
    cache_path = _token_cache_path(kantoor_minr)
    partial_path = _partial_cache_path(kantoor_minr)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)
    if partial_path.exists():
        partial_path.unlink()


def _save_partial_cache(kantoor_minr: int, tokens: list[dict]) -> None:
    partial_path = _partial_cache_path(kantoor_minr)
    partial_path.parent.mkdir(parents=True, exist_ok=True)
    with open(partial_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Phase 1: Discover kantoor minr values from inv2 tree
# ---------------------------------------------------------------------------

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def _discover_kantoren() -> list[dict]:
    """Discover kantoor entries from the inv2 page.

    Returns [{name: str, minr: int}, ...].  Caches in scans/zeeland/kantoren.json.
    """
    cache_path = OUTPUT_DIR / "kantoren.json"
    if cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                kantoren = json.load(f)
            if kantoren:
                print(f"  loaded {len(kantoren)} cached kantoren")
                return kantoren
        except (json.JSONDecodeError, OSError):
            pass

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        print("  Loading inv2 page …")
        page.goto(_INV2_URL, wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(2_000)

        discovered: list[dict] = page.evaluate(_JS_EXTRACT_KANTOREN)
        print(f"  Found {len(discovered)} kantoren in the tree")

        for k in discovered:
            print(f"    {k['name']} (minr={k['minr']})")

        browser.close()

    _save_json(cache_path, discovered)
    return discovered


# ---------------------------------------------------------------------------
# Phase 2: Discover invnrs + harvest tokens per kantoor
# ---------------------------------------------------------------------------

def _discover_invnrs(kantoor_minr: int) -> list[dict]:
    """Return [{invnr, text, minr, hasScan}, ...] for one kantoor."""
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        url = _inv3_url(kantoor_minr)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick]', state="attached", timeout=30_000)

        # Expand all sub-sections
        total_expanded = 0
        for _ in range(40):
            n = page.evaluate(_JS_EXPAND_SUBSECTIONS)
            if not n:
                break
            total_expanded += n
            page.wait_for_timeout(1_500)
        print(f"    expanded {total_expanded} sub-sections")

        items: list[dict] = page.evaluate(_JS_COLLECT_INVNRS)
        browser.close()

    digitized = [it for it in items if it["hasScan"]]
    print(f"    {len(digitized)} digitized inventarisnummers (out of {len(items)})")
    return items


def _harvest_page_tokens(kantoor_minr: int, invnrs: list[dict]) -> list[dict]:
    """Harvest tokens for all digitized invnrs: navigate to inv2 minr, force-load strip.

    Returns [{invnr, page, miahd, rdt, open, thumb_url, inv_text}, ...].
    Caches tokens per kantoor minr.
    """
    complete_path = _token_cache_path(kantoor_minr)
    if complete_path.exists():
        cached = _load_cached_tokens(kantoor_minr)
        if cached is not None:
            return cached

    pages: list[dict] = []
    seen_keys: set[tuple[int, str, int]] = set()
    completed_invnrs: set[int] = set()

    partial = _load_cached_tokens(kantoor_minr)
    if partial is not None:
        pages = partial
        for p in pages:
            seen_keys.add((p["invnr"], p.get("slug", ""), p["page"]))
        completed_invnrs = {p["invnr"] for p in pages}
        print(f"    resuming from {len(pages)} already-collected pages "
              f"({len(completed_invnrs)} invnrs)")

    # Only process digitized, not-yet-completed invnrs
    digitized = [it for it in invnrs if it["hasScan"] and it["invnr"] not in completed_invnrs]
    if not digitized:
        return pages

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        for idx, it in enumerate(digitized):
            invnr = it["invnr"]
            invnr_minr = it["minr"]
            inv_text = it["text"]
            print(f"    [{idx + 1}/{len(digitized)}] invnr {invnr} "
                  f"({inv_text[:60].strip()} …)", end=" ", flush=True)

            url = _inv2_minr_url(invnr_minr)
            page.goto(url, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(1_500)

            # Force-load all strip chunks
            fl = page.evaluate(_JS_STRIP_FORCE_LOAD)
            if not fl["total"]:
                print(f"0 pages")
                continue

            for _ in range(60):
                if page.evaluate(_JS_STRIP_LOADED):
                    break
                page.wait_for_timeout(500)

            srcs: list[str] = page.evaluate(_JS_HARVEST_THUMBS)
            new_pages = 0
            for src in srcs:
                rec = _parse_thumb_url(src)
                if rec and rec["invnr"] == invnr:
                    key = (rec["invnr"], rec.get("slug", ""), rec["page"])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        rec["inv_text"] = inv_text
                        pages.append(rec)
                        new_pages += 1
            print(f"{new_pages} new pages (total {len(pages)})")

            batch_idx = idx + 1
            if batch_idx % 25 == 0 or batch_idx == len(digitized):
                _save_partial_cache(kantoor_minr, pages)

        browser.close()

    print(f"    total pages: {len(pages)}")
    _save_cached_tokens(kantoor_minr, pages)
    return pages


# ---------------------------------------------------------------------------
# Phase 3: Download scans
# ---------------------------------------------------------------------------

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


def _write_metadata(
    dest_dir: Path, kantoor: str, invnr: int, inv_description: str, n_scans: int
) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    naam = f"Memorie van Successie {kantoor} {ARCHIVE_NUMBER} {invnr}".strip()
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": ARCHIVE_NUMBER,
        "brontype": "Memorie van Successie",
        "kantoor": kantoor,
        "inventarisnummer": str(invnr),
        "omschrijving": inv_description,
        "naam": naam,
        "n_scans": n_scans,
    }
    with open(dest_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.headers["Referer"] = "https://www.zeeuwsarchief.nl/"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Phase 1: Discover kantoren
    print("Discovering kantoren …")
    kantoren = _discover_kantoren()

    if not kantoren:
        print("ERROR: no kantoren found. The tree structure may have changed.")
        return

    print(f"\n{'='*60}")
    print(f"Processing {len(kantoren)} kantoren")
    print(f"{'='*60}")

    done_file = OUTPUT_DIR / "done.txt"
    done: set[str] = set()
    if done_file.exists():
        done = set(done_file.read_text().splitlines())

    grand_downloaded = grand_skipped = grand_missing = 0

    for k_idx, k_data in enumerate(kantoren):
        kantoor = k_data["name"]
        kantoor_minr = k_data["minr"]

        print(f"\n{'='*60}")
        print(f"  [{k_idx + 1}/{len(kantoren)}] {kantoor}")
        print(f"  kantoor_minr={kantoor_minr}")
        print(f"{'='*60}")

        if str(kantoor_minr) in done:
            print("  Already fully downloaded, skipping.")
            continue

        # Phase 2a: Discover digitized inventarisnummers
        invnrs = _discover_invnrs(kantoor_minr)
        digitized = [it for it in invnrs if it["hasScan"]]
        if not digitized:
            print("  No digitized inventarisnummers, skipping.")
            with open(done_file, "a") as f:
                f.write(f"{kantoor_minr}\n")
            continue

        # Phase 2b: Harvest tokens for all digitized invnrs
        pages = _harvest_page_tokens(kantoor_minr, invnrs)

        if not pages:
            print("  No pages found in this kantoor")
            with open(done_file, "a") as f:
                f.write(f"{kantoor_minr}\n")
            continue

        # Group pages by invnr
        invnr_pages: dict[int, list[dict]] = {}
        invnr_texts: dict[int, str] = {}
        for p in pages:
            invnr_pages.setdefault(p["invnr"], []).append(p)
            if p["invnr"] not in invnr_texts:
                invnr_texts[p["invnr"]] = p.get("inv_text", "")

        print(f"  {len(invnr_pages)} inventarisnummers with scans")

        downloaded = skipped = missing = 0
        for invnr, inv_pages in sorted(invnr_pages.items()):
            inv_text = invnr_texts.get(invnr, "")
            safe_kantoor = kantoor.replace(". ", "_").replace(" ", "_")[:60]
            dest_dir = OUTPUT_DIR / safe_kantoor / str(invnr)
            print(f"  invnr {invnr} ({inv_text[:40].strip()}) …", end=" ", flush=True)

            _write_metadata(dest_dir, kantoor, invnr, inv_text, len(inv_pages))

            inv_downloaded = inv_skipped = inv_missing = 0
            for p in sorted(inv_pages, key=lambda x: (x["page"], x.get("slug", ""))):
                url = _fullsize_url(p["thumb_url"])
                slug = p.get("slug", "")
                if slug:
                    dest = dest_dir / f"{slug}_{p['page']:04d}.jpg"
                else:
                    dest = dest_dir / f"{p['page']:04d}.jpg"
                status = _download_file(session, url, dest)
                if status == "downloaded":
                    inv_downloaded += 1
                elif status == "exists":
                    inv_skipped += 1
                else:
                    inv_missing += 1
                time.sleep(0.15)

            print(f"{len(inv_pages)} pages "
                  f"({inv_downloaded} new, {inv_skipped} existing, {inv_missing} missing)")

            downloaded += inv_downloaded
            skipped += inv_skipped
            missing += inv_missing

        print(f"  Kantoor totals: {downloaded} new, {skipped} existing, {missing} missing")

        grand_downloaded += downloaded
        grand_skipped += skipped
        grand_missing += missing

        with open(done_file, "a") as f:
            f.write(f"{kantoor_minr}\n")

    print(f"\n===== COMPLETE =====")
    print(f"Total: {grand_downloaded} downloaded, {grand_skipped} existing, {grand_missing} missing")
    print("Done (Zeeland).")


if __name__ == "__main__":
    main()
