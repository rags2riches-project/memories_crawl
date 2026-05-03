"""Het Utrechts Archief – Memories van Successie downloader.

Archive: Het Utrechts Archief (HUA)
MAIS system: miadt=39, mivast=39
Image server: img.hetutrechtsarchief.nl/mi-39/hua/archiefbank/...

How it works
────────────
The HUA uses the MAIS Internet viewer. Each kantoor's archive is identified
by a micode (e.g. 337-7 for Kantoor Utrecht). The inventory tree has
subsections (e.g. 1818-1848 and 1849-1902), each containing individual
inventarisnummers.

Each inventarisnummer's scans are accessed via stk3 inline strips on the
inv3 page. Thumbnail URLs are harvested from the stk3 strip's sslider DOM,
and full-size images are downloaded by stripping the ?format=thumb parameter.

Scans are served from:
    https://img.hetutrechtsarchief.nl/mi-39/hua/archiefbank/.../NL-UtHUA_{micode}_{invnr}_{page:04d}.jpg
    ?miadt=39&miahd={miahd}&mivast=39&rdt={rdt}&open={token}

Each page has unique miahd/open tokens that are session-bound.

Strategy
────────
1. Navigate to the inv2 inventory page for each kantoor's archive code.
2. Expand the tree to discover MvS subsection minr values.
3. For each subsection, navigate to the inv3 view in a single Playwright session.
4. Collect all stk3 child item onclick argument strings from the DOM.
5. For each stk3 item: call mi_inv3_toggle_stk() to expand inline,
   force-load all strip chunks, harvest thumbnail URLs, remove strip from DOM.
6. Convert thumbnail URLs to full-size (remove ?format=thumb) and download.
7. Cache harvested page tokens per subsection minr to skip Playwright on reruns.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests

ARCHIVE_NAME = "Het Utrechts Archief"
MAIS_ADT = "39"
MAIS_VAST = "39"
OUTPUT_DIR = Path("scans/utrechtsarchief")
USER_AGENT = "memories-crawl/1.0"

# Kantoren and their archive codes (micode).
# Subsection minr values are discovered dynamically from the inv2 tree.
KANTOREN: dict[str, str] = {
    "Amersfoort":        "337-2",
    "Amerongen":         "337-1",
    "Loenen":            "337-3",
    "Maarssen":          "337-4",
    "Montfoort":         "337-5",
    "Rhenen":            "337-6",
    "Utrecht":           "337-7",
    "IJsselstein":       "337-10",
    "Vianen":            "1279",
    "Woerden":           "1274",
    "Wijk bij Duurstede": "337-9",
}

# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

def _inv2_url(micode: str) -> str:
    return (
        "https://hetutrechtsarchief.nl/onderzoek/resultaten/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&micode={micode}&milang=nl&miview=inv2"
    )


def _inv3_url(micode: str, minr: int) -> str:
    return (
        "https://hetutrechtsarchief.nl/onderzoek/resultaten/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miaet=1&micode={micode}&minr={minr}"
        f"&milang=nl&miview=inv3"
    )


# ---------------------------------------------------------------------------
# JavaScript snippets
# ---------------------------------------------------------------------------

_JS_EXPAND_TREE = """() => {
    const links = document.querySelectorAll('a[onclick*="mi_inv3_openinv"]');
    let count = 0;
    for (const link of links) {
        const text = (link.textContent || '').trim();
        if (text && !text.startsWith('Toon')) {
            link.click();
            count++;
        }
    }
    return count;
}"""

_JS_SUBSECTIONS = """() => {
    const links = document.querySelectorAll('a[onclick*="mi_inv3_openinv"],a[onclick*="stk3"]');
    const result = [];
    const seen = new Set();
    for (const a of links) {
        const text = (a.textContent || '').trim();
        if (!text || text.startsWith('Toon') || text.startsWith('Favoriet')) continue;
        const onclick = a.getAttribute('onclick') || '';
        const minrMatch = onclick.match(/minr=(\\d+)/);
        if (!minrMatch) continue;
        const minr = parseInt(minrMatch[1], 10);
        if (seen.has(minr)) continue;
        seen.add(minr);
        if (text.toLowerCase().includes('memories') || text.match(/^\\d+-\\d+/)) {
            result.push({ text: text.substring(0, 120), minr });
        }
    }
    return result;
}"""

# Collect all stk3 child-item info from inv3 DOM.
# Skips the tree-opener ("Toon details van deze beschrijving").
_JS_COLLECT_STK3_ITEMS = """() => {
    return Array.from(document.querySelectorAll('a[onclick*="stk3"]')).map(a => {
        const oc = a.getAttribute('onclick');
        const text = (a.textContent || '').trim().substring(0, 200);
        if (!text || text === 'Toon details van deze beschrijving') return null;
        const start = oc.indexOf('(');
        const end = oc.lastIndexOf(')');
        if (start === -1 || end === -1) return null;
        const args = oc.substring(start + 1, end);
        const numMatch = text.match(/^(\\d+)\\s/);
        const invnr = numMatch ? parseInt(numMatch[1], 10) : 0;
        return { invnr, text, args };
    }).filter(Boolean);
}"""

_JS_STRIP_IDS = "() => Object.keys(mi_strip_store || {})"

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

_JS_STRIP_LOADED = """(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.aantal) return true;
    return (r.loadedScans || []).length >= r.aantal;
}"""

# Harvest thumbnail srcs from a single strip, then remove from store/DOM.
_JS_HARVEST_STRIP = """(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.sslider) return [];
    const srcs = Array.from(
        r.sslider.querySelectorAll('img[src*="img.hetutrechtsarchief.nl"]')
    ).map(i => i.src);
    if (r.strip && r.strip.parentNode) r.strip.parentNode.removeChild(r.strip);
    delete mi_strip_store[stripId];
    return srcs;
}"""

# Fallback: harvest all HUA image URLs currently in the DOM.
_JS_HARVEST_ALL = """() => {
    return Array.from(
        document.querySelectorAll('img[src*="img.hetutrechtsarchief.nl"]')
    ).map(i => i.src);
}"""

# ---------------------------------------------------------------------------
# Thumbnail URL parsing and full-size URL construction
# ---------------------------------------------------------------------------

_IMG_URL_RE = re.compile(
    r"NL-UtHUA_(?P<micode>[^_]+)_(?P<invnr>\d+)_(?P<page>\d+)\.jpg"
    r"\?.*?miahd=(?P<miahd>\d+)"
    r".*?rdt=(?P<rdt>[^&]+)"
    r".*?open=(?P<open>[^&]+)"
)


def _parse_thumb_url(url: str) -> dict | None:
    """Parse a thumbnail URL into {micode, invnr, page, miahd, rdt, open, thumb_url}."""
    m = _IMG_URL_RE.search(url)
    if not m:
        return None
    return {
        "micode": m.group("micode"),
        "invnr": int(m.group("invnr")),
        "page": int(m.group("page")),
        "miahd": int(m.group("miahd")),
        "rdt": m.group("rdt"),
        "open": m.group("open"),
        "thumb_url": url,
    }


def _fullsize_url(thumb_url: str) -> str:
    """Convert a thumbnail URL to a full-size download URL by removing format=thumb."""
    url = thumb_url.replace("?format=thumb&", "?")
    url = url.replace("&format=thumb", "")
    url = url.replace("?format=thumb", "")
    return url


# ---------------------------------------------------------------------------
# Caching (with incremental save for crash resilience)
# ---------------------------------------------------------------------------

def _get_token_cache_path(micode: str, subsection_minr: int) -> Path:
    return OUTPUT_DIR / f"tokens_{micode}_{subsection_minr}.json"


def _get_partial_cache_path(micode: str, subsection_minr: int) -> Path:
    return OUTPUT_DIR / f"tokens_{micode}_{subsection_minr}_partial.json"


def _load_cached_tokens(micode: str, subsection_minr: int) -> list[dict] | None:
    """Load cached tokens. Prefers the complete cache over the partial one."""
    cache_path = _get_token_cache_path(micode, subsection_minr)
    partial_path = _get_partial_cache_path(micode, subsection_minr)

    for path in (cache_path, partial_path):
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    tokens = json.load(f)
                if tokens:
                    label = "complete" if path == cache_path else "partial"
                    print(f"    loaded {len(tokens)} cached tokens ({label})")
                    return tokens
            except (json.JSONDecodeError, IOError):
                pass
    return None


def _save_cached_tokens(micode: str, subsection_minr: int, tokens: list[dict]) -> None:
    """Save final complete cache and remove partial."""
    cache_path = _get_token_cache_path(micode, subsection_minr)
    partial_path = _get_partial_cache_path(micode, subsection_minr)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)
    # Remove partial if it exists
    if partial_path.exists():
        partial_path.unlink()


def _save_partial_cache(micode: str, subsection_minr: int, tokens: list[dict]) -> None:
    """Save incremental partial cache during harvesting."""
    partial_path = _get_partial_cache_path(micode, subsection_minr)
    partial_path.parent.mkdir(parents=True, exist_ok=True)
    with open(partial_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Playwright-based page token harvesting (Overijssel-style stk3 approach)
# ---------------------------------------------------------------------------

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def _harvest_page_tokens(micode: str, subsection_minr: int) -> list[dict]:
    """Return [{micode, invnr, page, miahd, rdt, open, thumb_url, inv_text}, ...].

    Uses Playwright to navigate to the inv3 page, iterate over all stk3 child
    items, toggle them open, force-load all scan chunks, and harvest thumbnail
    URLs. Results are cached per subsection minr.

    Supports incremental saving: partial results are saved every 25 items.
    If interrupted, the next run will resume from the partial cache and only
    process remaining items.
    """
    # Check for complete cache first
    complete_path = _get_token_cache_path(micode, subsection_minr)
    if complete_path.exists():
        cached = _load_cached_tokens(micode, subsection_minr)
        if cached is not None:
            return cached

    # Check for partial cache (resume from interruption)
    pages: list[dict] = []
    seen_keys: set[tuple[int, int]] = set()
    start_from: int = 0
    # Track which invnr+page we've already collected so we can skip
    completed_invnrs: set[int] = set()

    partial = _load_cached_tokens(micode, subsection_minr)
    if partial is not None:
        pages = partial
        for p in pages:
            seen_keys.add((p["invnr"], p["page"]))
        # Count unique invnrs already processed
        completed_invnrs = {p["invnr"] for p in pages}
        start_from = 0  # We'll skip items whose invnrs are already in the cache
        print(f"    resuming from {len(pages)} already-collected pages "
              f"({len(completed_invnrs)} invnrs)")

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        url = _inv3_url(micode, subsection_minr)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick*="stk3"]', state="attached", timeout=30_000)

        stk3_items: list[dict] = page.evaluate(_JS_COLLECT_STK3_ITEMS)
        print(f"    found {len(stk3_items)} stk3 child items")

        # Filter out items whose invnrs are already fully collected
        items_to_process = [
            item for item in stk3_items
            if item["invnr"] not in completed_invnrs
        ]
        skipped_count = len(stk3_items) - len(items_to_process)
        if skipped_count > 0:
            print(f"    skipping {skipped_count} already-collected items")

        for idx, item in enumerate(items_to_process):
            invnr = item["invnr"]
            inv_text = item["text"]
            args_str = item["args"]

            before_ids: set[str] = set(page.evaluate(_JS_STRIP_IDS))

            page.evaluate(f"mi_inv3_toggle_stk({args_str})")
            page.wait_for_timeout(1_500)

            after_ids: set[str] = set(page.evaluate(_JS_STRIP_IDS))
            new_ids = after_ids - before_ids

            if new_ids:
                strip_id = next(iter(new_ids))
                total = page.evaluate(_JS_FORCE_LOAD_STRIP, strip_id)
                for _ in range(120):
                    if page.evaluate(_JS_STRIP_LOADED, strip_id):
                        break
                    page.wait_for_timeout(500)

                srcs: list[str] = page.evaluate(_JS_HARVEST_STRIP, strip_id)
            else:
                srcs = page.evaluate(_JS_HARVEST_ALL)

            for src in srcs:
                rec = _parse_thumb_url(src)
                if rec:
                    key = (rec["invnr"], rec["page"])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        rec["inv_text"] = inv_text
                        pages.append(rec)

            # Incremental save every 25 items (or if this is the last item)
            batch_idx = idx + 1
            is_last = batch_idx == len(items_to_process)
            if batch_idx % 25 == 0 or is_last:
                processed = len(completed_invnrs) + batch_idx
                print(f"    processed {processed}/{len(stk3_items)} items, "
                      f"{len(pages)} pages so far")
                _save_partial_cache(micode, subsection_minr, pages)

        browser.close()

    print(f"    total pages collected: {len(pages)}")
    _save_cached_tokens(micode, subsection_minr, pages)
    return pages


# ---------------------------------------------------------------------------
# Subsection discovery
# ---------------------------------------------------------------------------

def _discover_subsections(micode: str) -> list[dict]:
    """Navigate to inv2 page and discover MvS subsections.

    Returns [{text, minr}, ...] for each MvS subsection found.
    """
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    url = _inv2_url(micode)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=60_000)

        expanded = page.evaluate(_JS_EXPAND_TREE)
        page.wait_for_timeout(4_000)

        expanded2 = page.evaluate(_JS_EXPAND_TREE)
        page.wait_for_timeout(4_000)

        subsections: list[dict] = page.evaluate(_JS_SUBSECTIONS)
        browser.close()

    return subsections


# ---------------------------------------------------------------------------
# Download helpers
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
    dest_dir: Path, kantoor: str, micode: str, invnr: int, inv_description: str, n_scans: int
) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": micode,
        "brontype": "Memorie van Successie",
        "kantoor": kantoor,
        "inventarisnummer": str(invnr),
        "omschrijving": inv_description,
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
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for kantoor, micode in KANTOREN.items():
        print(f"\n{'='*60}")
        print(f"  {kantoor} (micode={micode})")
        print(f"{'='*60}")

        # Step 1: Discover subsections
        print("  Discovering subsections …")
        subsections = _discover_subsections(micode)
        print(f"  Found {len(subsections)} MvS subsections")

        if not subsections:
            print("  WARNING: no subsections found, skipping")
            continue

        done_file = OUTPUT_DIR / f"done_{kantoor}.txt"
        done: set[str] = set()
        if done_file.exists():
            done = set(done_file.read_text().splitlines())

        for section_idx, section in enumerate(subsections):
            section_minr = section["minr"]
            print(f"\n  --- Section {section_idx + 1}/{len(subsections)} "
                  f"(minr={section_minr}): {section['text'][:80]} ---")

            # Step 2: Harvest all page tokens for this subsection
            pages = _harvest_page_tokens(micode, section_minr)

            if not pages:
                print("    No pages found in this section")
                continue

            # Group pages by invnr
            invnr_pages: dict[int, list[dict]] = {}
            invnr_texts: dict[int, str] = {}
            for p in pages:
                invnr_pages.setdefault(p["invnr"], []).append(p)
                if p["invnr"] not in invnr_texts:
                    invnr_texts[p["invnr"]] = p.get("inv_text", "")

            print(f"    {len(invnr_pages)} inventarisnummers with scans")

            downloaded = skipped = missing = 0
            for invnr, inv_pages in sorted(invnr_pages.items()):
                key = str(invnr)
                if key in done:
                    skipped += len(inv_pages)
                    continue

                inv_text = invnr_texts.get(invnr, "")
                dest_dir = OUTPUT_DIR / kantoor / f"{invnr:04d}"
                print(f"    invnr {invnr} ({inv_text[:40].strip()}) …", end=" ", flush=True)

                _write_metadata(dest_dir, kantoor, micode, invnr, inv_text, len(inv_pages))

                inv_downloaded = inv_skipped = inv_missing = 0
                for p in sorted(inv_pages, key=lambda x: x["page"]):
                    url = _fullsize_url(p["thumb_url"])
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

                with open(done_file, "a") as f:
                    f.write(key + "\n")

                downloaded += inv_downloaded
                skipped += inv_skipped
                missing += inv_missing

            print(f"    Section totals: {downloaded} new, {skipped} existing, {missing} missing")

    print("\nDone (Utrechts Archief).")


if __name__ == "__main__":
    main()
