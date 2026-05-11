"""Noord-Holland – Memories van Successie downloader.

Archive: Noord-Hollands Archief (NHA)
MAIS system: miadt=236, mivast=236, archive code 178
Image server: preserve-nha.archieven.nl/mi-0/fonc-nha/178/

How it works
────────────
The NHA uses the MAIS Internet viewer on their own domain. The archive is
identified by micode=178. The inventory tree has 15 kantoor-level entries
(Alkmaar, Amsterdam, …). Each kantoor expands to reveal period children
(e.g. "1-194 Memories van aangifte van nalatenschappen, 1818-1902").

Period children are displayed on the inv3 page (miview=inv3) where each
inventarisnummer has an stk3 inline strip with per-page thumbnails.

Scans are served from:
    https://preserve-nha.archieven.nl/mi-236/fonc-nha/178/{invnr}/
        NL-HlmNHA_178_{invnr}_{page:04d}.jpg
    ?format=thumb&miadt=236&miahd={miahd}&mivast=236&rdt={rdt}&open={token}
    (remove ?format=thumb for full-size)

Strategy
────────
1. Navigate to the inv2 page; all 15 kantoor minr values are in the initial DOM.
2. For each kantoor: expand the tree node to reveal period children, collect
   their minr values, and filter out Tafel V-bis items.
3. For each MvS period minr: navigate to the inv3 page, collect all stk3 child
   items, toggle each one to force-load the strip, harvest thumbnail URLs,
   and remove strips from the DOM.
4. Convert thumbnail URLs to full-size (remove ?format=thumb) and download.
5. Cache tokens per period minr so reruns skip Playwright entirely.

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

ARCHIVE_NAME = "Noord-Hollands Archief"
ARCHIVE_NUMBER = "178"
MAIS_ADT = "236"
MAIS_VAST = "236"
OUTPUT_DIR = Path("scans/noordholland")
USER_AGENT = "memories-crawl/1.0"

# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

_INV2_URL = (
    "https://noord-hollandsarchief.nl/bronnen/archieven"
    f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
    f"&micode={ARCHIVE_NUMBER}&milang=nl&miview=inv2"
)


def _inv3_url(minr: int) -> str:
    return (
        "https://noord-hollandsarchief.nl/bronnen/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miaet=1&micode={ARCHIVE_NUMBER}&minr={minr}"
        f"&milang=nl&miview=inv3"
    )


# ---------------------------------------------------------------------------
# JavaScript snippets for kantoor discovery
# ---------------------------------------------------------------------------

# Extract all kantoor-level items from the initial inv2 DOM.
# The tree is rendered inline — no AJAX needed for kantoor discovery.
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

# Expand a kantoor tree node by clicking its toggle link (mi_inv3_swapinv).
_JS_CLICK_KANTOOR = r"""(minr) => {
    const links = document.querySelectorAll('.mi_tree_node a');
    for (const a of links) {
        const oc = a.getAttribute('onclick') || '';
        if (oc.includes('minr=' + minr) && oc.includes('swapinv')) {
            a.click();
            return true;
        }
    }
    return false;
}"""

# Extract period children from a kantoor node's children container.
# Skips Tafel V-bis / Tafel VI items.
_JS_EXTRACT_PERIODS = r"""(parentMinr) => {
    const result = [];
    const seen = new Set();
    const parents = document.querySelectorAll('.mi_tree_node');
    let parentNode = null;
    for (const node of parents) {
        const links = node.querySelectorAll('a[onclick]');
        for (const a of links) {
            if ((a.getAttribute('onclick') || '').includes('minr=' + parentMinr)) {
                parentNode = node;
                break;
            }
        }
        if (parentNode) break;
    }
    if (!parentNode) return result;
    const childrenContainer = parentNode.querySelector('[id*="mirecchildren"]');
    if (!childrenContainer) return result;
    const childLinks = childrenContainer.querySelectorAll('a[onclick*="minr="]');
    for (const a of childLinks) {
        const oc = a.getAttribute('onclick') || '';
        const text = (a.textContent || '').trim();
        const m = oc.match(/minr=(\d+)/);
        if (!m) continue;
        const minr = parseInt(m[1], 10);
        if (seen.has(minr)) continue;
        seen.add(minr);
        const low = text.toLowerCase();
        if (low.includes('tafel') || low.includes('v-bis') || low.includes('5bis')) continue;
        result.push({ text: text.substring(0, 200), minr });
    }
    return result;
}"""

# ---------------------------------------------------------------------------
# JavaScript snippets for stk3 strip interaction (inv3 page)
# ---------------------------------------------------------------------------

# Collect all stk3 child-item info from the inv3 DOM.
# Skips the "Toon details van deze beschrijving" placeholder.
_JS_COLLECT_STK3_ITEMS = r"""() => {
    return Array.from(document.querySelectorAll('a[onclick*="stk3"]')).map(a => {
        const oc = a.getAttribute('onclick');
        const text = (a.textContent || '').trim().substring(0, 200);
        if (!text || text === 'Toon details van deze beschrijving') return null;
        const start = oc.indexOf('(');
        const end = oc.lastIndexOf(')');
        if (start === -1 || end === -1) return null;
        const args = oc.substring(start + 1, end);
        const numMatch = text.match(/^(\d+)\s/);
        const invnr = numMatch ? parseInt(numMatch[1], 10) : 0;
        return { invnr, text, args };
    }).filter(Boolean);
}"""

_JS_STRIP_IDS = "() => Object.keys(mi_strip_store || {})"

_JS_FORCE_LOAD_STRIP = r"""(stripId) => {
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

_JS_STRIP_LOADED = r"""(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.aantal) return true;
    return (r.loadedScans || []).length >= r.aantal;
}"""

_JS_HARVEST_STRIP = r"""(stripId) => {
    const r = (mi_strip_store || {})[stripId];
    if (!r || !r.sslider) return [];
    const srcs = Array.from(
        r.sslider.querySelectorAll('img[src*="preserve-nha.archieven.nl"]')
    ).map(i => i.src);
    if (r.strip && r.strip.parentNode) r.strip.parentNode.removeChild(r.strip);
    delete mi_strip_store[stripId];
    return srcs;
}"""

_JS_HARVEST_ALL = """() => {
    return Array.from(
        document.querySelectorAll('img[src*="preserve-nha.archieven.nl"]')
    ).map(i => i.src);
}"""

# ---------------------------------------------------------------------------
# Thumbnail URL parsing and full-size URL construction
# ---------------------------------------------------------------------------

_IMG_URL_RE = re.compile(
    r"_(\d+)_(\d+)_(\d+)\.jpg"
    r"\?.*?miahd=(?P<miahd>\d+)"
    r".*?rdt=(?P<rdt>[^&]+)"
    r".*?open=(?P<open>[^&]+)"
)


def _parse_thumb_url(url: str) -> dict | None:
    """Parse a thumbnail URL into {invnr, page, miahd, rdt, open, thumb_url}."""
    m = _IMG_URL_RE.search(url)
    if not m:
        return None
    return {
        "invnr": int(m.group(2)),   # group 2 = invnr (group 1 = archive code 178)
        "page": int(m.group(3)),    # group 3 = page number
        "miahd": int(m.group("miahd")),
        "rdt": m.group("rdt"),
        "open": m.group("open"),
        "thumb_url": url,
    }


def _fullsize_url(thumb_url: str) -> str:
    url = thumb_url.replace("?format=thumb&", "?")
    url = url.replace("&format=thumb", "")
    url = url.replace("?format=thumb", "")
    return url


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

def _token_cache_path(period_minr: int) -> Path:
    return OUTPUT_DIR / f"tokens_{period_minr}.json"


def _partial_cache_path(period_minr: int) -> Path:
    return OUTPUT_DIR / f"tokens_{period_minr}_partial.json"


def _load_cached_tokens(period_minr: int) -> list[dict] | None:
    for path in (_token_cache_path(period_minr), _partial_cache_path(period_minr)):
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    tokens = json.load(f)
                if tokens:
                    label = "complete" if path.name.startswith("tokens_") and "partial" not in path.name else "partial"
                    print(f"    loaded {len(tokens)} cached tokens ({label})")
                    return tokens
            except (json.JSONDecodeError, OSError):
                pass
    return None


def _save_cached_tokens(period_minr: int, tokens: list[dict]) -> None:
    cache_path = _token_cache_path(period_minr)
    partial_path = _partial_cache_path(period_minr)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)
    if partial_path.exists():
        partial_path.unlink()


def _save_partial_cache(period_minr: int, tokens: list[dict]) -> None:
    partial_path = _partial_cache_path(period_minr)
    partial_path.parent.mkdir(parents=True, exist_ok=True)
    with open(partial_path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Phase 1: Discover kantoor sections and their MvS periods
# ---------------------------------------------------------------------------

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def _discover_sections() -> list[dict]:
    """Return [{kantoor: str, period_text: str, period_minr: int}, ...].

    Navigates to inv2, extracts 15 kantoor minr values from the initial DOM,
    expands each kantoor to discover MvS period children (filtering Tafel V-bis),
    and caches the result.
    """
    cache_path = OUTPUT_DIR / "sections.json"
    if cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                sections = json.load(f)
            if sections:
                print(f"  loaded {len(sections)} cached sections")
                return sections
        except (json.JSONDecodeError, OSError):
            pass

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    sections: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        print("  Loading inv2 page …")
        page.goto(_INV2_URL, wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(2_000)

        # Step 1: Extract all kantoor minr values from the initial DOM
        kantoren: list[dict] = page.evaluate(_JS_EXTRACT_KANTOREN)
        print(f"  Found {len(kantoren)} kantoren in the tree")

        for idx, k in enumerate(kantoren):
            kantoor_name = k["name"]
            kantoor_minr = k["minr"]
            print(f"\n  [{idx + 1}/{len(kantoren)}] {kantoor_name} (minr={kantoor_minr})")

            # Step 2: Expand the kantoor tree node to reveal children
            clicked = page.evaluate(_JS_CLICK_KANTOOR, kantoor_minr)
            page.wait_for_timeout(2_000)

            if not clicked:
                print(f"    WARNING: could not expand {kantoor_name}")
                continue

            # Step 3: Extract MvS period children (filtered for Tafel V-bis)
            periods: list[dict] = page.evaluate(_JS_EXTRACT_PERIODS, kantoor_minr)
            print(f"    {len(periods)} MvS periods found")

            for p in periods:
                sections.append({
                    "kantoor": kantoor_name,
                    "period_text": p["text"],
                    "period_minr": p["minr"],
                })

        browser.close()

    print(f"\n  Total: {len(sections)} MvS period sections across {len(kantoren)} kantoren")
    _save_json(cache_path, sections)
    return sections


# ---------------------------------------------------------------------------
# Phase 2: Harvest page tokens via Playwright (stk3 approach)
# ---------------------------------------------------------------------------

def _harvest_page_tokens(period_minr: int) -> list[dict]:
    """Return [{invnr, page, miahd, rdt, open, thumb_url, inv_text}, ...]."""
    complete_path = _token_cache_path(period_minr)
    if complete_path.exists():
        cached = _load_cached_tokens(period_minr)
        if cached is not None:
            return cached

    pages: list[dict] = []
    seen_keys: set[tuple[int, int]] = set()
    completed_invnrs: set[int] = set()

    partial = _load_cached_tokens(period_minr)
    if partial is not None:
        pages = partial
        for p in pages:
            seen_keys.add((p["invnr"], p["page"]))
        completed_invnrs = {p["invnr"] for p in pages}
        print(f"    resuming from {len(pages)} already-collected pages "
              f"({len(completed_invnrs)} invnrs)")

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        url = _inv3_url(period_minr)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick*="stk3"]', state="attached", timeout=30_000)

        stk3_items: list[dict] = page.evaluate(_JS_COLLECT_STK3_ITEMS)
        print(f"    {len(stk3_items)} stk3 child items")

        items_to_process = [
            item for item in stk3_items
            if item["invnr"] not in completed_invnrs
        ]
        skipped = len(stk3_items) - len(items_to_process)
        if skipped > 0:
            print(f"    skipping {skipped} already-collected items")

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
                page.evaluate(_JS_FORCE_LOAD_STRIP, strip_id)
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

            batch_idx = idx + 1
            is_last = batch_idx == len(items_to_process)
            if batch_idx % 25 == 0 or is_last:
                processed = len(completed_invnrs) + batch_idx
                print(f"    {processed}/{len(stk3_items)} items, {len(pages)} pages so far")
                _save_partial_cache(period_minr, pages)

        browser.close()

    print(f"    total pages: {len(pages)}")
    _save_cached_tokens(period_minr, pages)
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
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Phase 1: Discover kantoren and their MvS period sections
    print("Discovering kantoor sections …")
    sections = _discover_sections()

    if not sections:
        print("ERROR: no sections found. The tree structure may have changed.")
        return

    print(f"\n{'='*60}")
    print(f"Harvesting page tokens for {len(sections)} period sections")
    print(f"{'='*60}")

    done_file = OUTPUT_DIR / "done.txt"
    done: set[str] = set()
    if done_file.exists():
        done = set(done_file.read_text().splitlines())

    grand_downloaded = grand_skipped = grand_missing = 0

    for section_idx, section in enumerate(sections):
        period_minr = section["period_minr"]
        kantoor = section["kantoor"]
        period_text = section["period_text"]

        print(f"\n{'='*60}")
        print(f"  [{section_idx + 1}/{len(sections)}] {kantoor}: {period_text[:80]}")
        print(f"  period_minr={period_minr}")
        print(f"{'='*60}")

        if str(period_minr) in done:
            print("  Already fully downloaded, skipping.")
            continue

        # Phase 2: Harvest all page tokens for this period
        pages = _harvest_page_tokens(period_minr)

        if not pages:
            print("  No pages found in this period")
            with open(done_file, "a") as f:
                f.write(f"{period_minr}\n")
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
            # Clean kantoor name for folder use
            safe_kantoor = kantoor.replace(". ", "_").replace(" ", "_")[:60]
            dest_dir = OUTPUT_DIR / safe_kantoor / f"{invnr:04d}"
            print(f"  invnr {invnr} ({inv_text[:40].strip()}) …", end=" ", flush=True)

            _write_metadata(dest_dir, kantoor, invnr, inv_text, len(inv_pages))

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

            downloaded += inv_downloaded
            skipped += inv_skipped
            missing += inv_missing

        print(f"  Section totals: {downloaded} new, {skipped} existing, {missing} missing")

        grand_downloaded += downloaded
        grand_skipped += skipped
        grand_missing += missing

        with open(done_file, "a") as f:
            f.write(f"{period_minr}\n")

    print(f"\n===== COMPLETE =====")
    print(f"Total: {grand_downloaded} downloaded, {grand_skipped} existing, {grand_missing} missing")
    print("Done (Noord-Holland).")


if __name__ == "__main__":
    main()
