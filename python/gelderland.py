"""Gelderland – Memories van Successie downloader.

Archive: Gelders Archief
MAIS system: miadt=37, mivast=37
Image server: preserve2.archieven.nl/mi-37/fonc-gea/{code}/

How it works
────────────
Unlike the other MAIS instances we crawl, the Gelders Archief gives **each
kantoor its own archief-code**: the Memories van Successie of kantoor Arnhem
live under micode=0021, Apeldoorn under 0092, Borculo under 0022, etc.  Twenty-
one kantoren are configured in :data:`KANTOREN`.

Inside one kantoor's inv2 tree there are normally two top-level openinv items:

    1.  Register IV, akten van het recht van successie en van overgang …
    2.  Tafel VI, alfabetische index op namen van personen waarvan een
        testament is opgemaakt door een notaris en Tafel V-bis, …

The second one is the index-on-testaments + Tafel V-bis; it must be **excluded**
per the project-wide rule (CLAUDE.md §"Exclusion rule").  We only drill into the
"Register IV" section.

Below Register IV the records are grouped by 5-year periods ("Akten,
1818-1825.", "Akten, 1826-1830.", …).  Each period eventually contains the leaf
inventarisnummers ("1  1818", "2  1819", "140  1895 eerste kwartaal", …).
Digitized leaves carry an ``h_scan`` icon in their tree row.

Scans are accessed by navigating to each leaf invnr's inv2 page::

    https://www.geldersarchief.nl/bronnen/archieven
        ?mivast=37&mizig=210&miadt=37&miview=inv2&milang=nl
        &micode={code}&minr={invnr_minr}

The thumbnail strip auto-loads on this page (25 thumbs initially).  We force-
load every remaining chunk via ``mi_strip_store.populate()`` exactly as the
Zeeland scraper does, then harvest the ``img[src*="fonc-gea"]`` srcs.

Image URLs look like::

    https://preserve2.archieven.nl/mi-37/fonc-gea/0021/1/1-0001.jp2
        ?format=thumb&miadt=37&miahd=4431669227&mivast=37
        &rdt=20251205&open=46EC9

Replace ``?format=thumb`` with ``?format=large`` to get a 756×1024 PNG
(~500 KB/page).  The full-resolution JP2 is only reachable via the IIPSrv tile
server, whose ``{invnr,page → jp2 hash}`` map is not exposed without an extra
viewer load per page; ``format=large`` is the practical maximum.

Strategy
────────
1. **Discover**: for each kantoor (micode), navigate to inv2; pick the
   "Register IV" top-level minr (filtering out Tafel VI / V-bis).
2. **Enumerate**: navigate to inv3 of that minr; iteratively click every
   ``swapinv`` link to expand the period sub-sections inline; collect leaf
   stk3 items whose text starts with ``^\\d+\\s`` and whose row has an
   ``h_scan`` marker.
3. **Harvest**: for each digitized leaf, navigate to its inv2&minr=… page,
   force-load all strip chunks, and harvest fonc-gea thumbnail URLs.
4. **Download**: build the ``?format=large`` variant of each thumb URL and
   write it to disk as ``{invnr}-{page:04d}.jpg``.
5. **Cache**: per-kantoor inventory + tokens are cached so reruns skip
   Playwright entirely once they exist.

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

ARCHIVE_NAME = "Gelders Archief"
MAIS_ADT = "37"
MAIS_VAST = "37"
OUTPUT_DIR = Path("scans/gelderland")
USER_AGENT = "memories-crawl/1.0"

#: Kantoor → archive code mapping.  Resolved 2026-05-11 by following the
#: permalinks listed on https://www.geldersarchief.nl/informatie/zoekhulp/
#: 997-memories-van-successie.  Each kantoor is its own access.
KANTOREN: dict[str, str] = {
    "Arnhem":      "0021",
    "Apeldoorn":   "0092",
    "Borculo":     "0022",
    "Culemborg":   "0023",
    "Doesburg":    "0024",
    "Druten":      "0025",
    "Elburg":      "0027",
    "Elst":        "0028",
    "Groenlo":     "0029",
    "Harderwijk":  "0030",
    "Hattem":      "0031",
    "Lochem":      "0032",
    "Nijkerk":     "0033",
    "Nijmegen":    "0034",
    "Terborg":     "0035",
    "Tiel":        "0026",
    "Wageningen":  "0036",
    "Winterswijk": "0223",
    "Zaltbommel":  "0037",
    "Zevenaar":    "0221",
    "Zutphen":     "0222",
}

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

def _inv2_url(code: str) -> str:
    """inv2 root page for one kantoor (micode); lists Register IV + Tafel."""
    return (
        "https://www.geldersarchief.nl/bronnen/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&micode={code}&milang=nl&miview=inv2"
    )


def _inv3_url(code: str, minr: int) -> str:
    """inv3 page for a specific minr inside one kantoor's tree."""
    return (
        "https://www.geldersarchief.nl/bronnen/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miaet=1&micode={code}&minr={minr}"
        f"&milang=nl&miview=inv3"
    )


def _inv2_minr_url(code: str, minr: int) -> str:
    """inv2 page for a specific leaf invnr; the strip viewer auto-loads here."""
    return (
        "https://www.geldersarchief.nl/bronnen/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miview=inv2&milang=nl&micode={code}&minr={minr}"
    )


# ---------------------------------------------------------------------------
# JavaScript snippets
# ---------------------------------------------------------------------------

# Pick out the "Register IV" top-level section minr while skipping Tafel V-bis.
# Each kantoor normally has two top-level openinv entries; we want the
# non-Tafel one.  Returns {name, minr} or null.
_JS_FIND_REGISTER_IV = r"""() => {
    const opens = Array.from(document.querySelectorAll('a[onclick*="openinv"]'));
    for (const a of opens) {
        const oc = a.getAttribute('onclick') || '';
        const text = (a.textContent || '').trim();
        const low = text.toLowerCase();
        if (low.includes('tafel') || low.includes('v-bis') || low.includes('5bis')) continue;
        const m = oc.match(/minr=(\d+)/);
        if (!m) continue;
        return { name: text.substring(0, 200), minr: parseInt(m[1], 10) };
    }
    return null;
}"""

# Click every swapinv link that hasn't been clicked yet to expand all period
# sub-sections inline.  Returns the number of links clicked this pass.
_JS_EXPAND_SUBSECTIONS = r"""() => {
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

# Collect leaf invnrs.  A leaf stk3 link has text starting with `\d+\s` (e.g.
# "1  1818" or "140 1895 eerste kwartaal"), is NOT a Tafel item, and its row
# carries an h_scan marker when digitized.  Section headers (e.g. "Akten,
# 1818-1825.") and the "Toon details van deze beschrijving" placeholder are
# filtered out by the regex.
_JS_COLLECT_INVNRS = r"""() => {
    const out = [];
    document.querySelectorAll('a[onclick*="stk3"]').forEach(a => {
        const oc = a.getAttribute('onclick') || '';
        const text = (a.textContent || '').trim();
        if (!text || text === 'Toon details van deze beschrijving') return;
        const num = text.match(/^(\d+)\s+/);
        if (!num) return;
        const low = text.toLowerCase();
        if (low.includes('tafel') || low.includes('v-bis') || low.includes('5bis')) return;
        const m = oc.match(/minr=(\d+)/);
        if (!m) return;
        const minr = parseInt(m[1], 10);
        const invnr = parseInt(num[1], 10);
        const node = a.closest('.mi_tree_node');
        const hasScan = node && node.querySelector('img[src*="h_scan"]') != null;
        out.push({ invnr, text: text.substring(0, 200), minr, hasScan });
    });
    return out;
}"""

# Strip force-load: kick off populate() for every chunk that hasn't been
# fetched yet.  Returns {total, chunks, key} for the first strip in the store.
_JS_STRIP_FORCE_LOAD = r"""() => {
    const keys = Object.keys(mi_strip_store || {});
    if (!keys.length) return { total: 0, chunks: 0, key: null };
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

_JS_STRIP_LOADED = r"""() => {
    const keys = Object.keys(mi_strip_store || {});
    if (!keys.length) return true;
    const r = mi_strip_store[keys[0]];
    return (r.loadedScans || []).length >= r.aantal;
}"""

_JS_HARVEST_THUMBS = (
    "() => Array.from(document.querySelectorAll('img[src*=\"fonc-gea\"]'))"
    "          .map(i => i.src)"
)


# ---------------------------------------------------------------------------
# Thumbnail URL parsing
# ---------------------------------------------------------------------------

# Example URL:
#   https://preserve2.archieven.nl/mi-37/fonc-gea/0021/1/1-0001.jp2
#       ?format=thumb&miadt=37&miahd=4431669227&mivast=37
#       &rdt=20251205&open=46EC9
_IMG_URL_RE = re.compile(
    r"/fonc-gea/(?P<code>\d+)/(?P<invnr>\d+)/"
    r"(?P=invnr)-(?P<page>\d+)\.jp2"
    r"\?.*?miahd=(?P<miahd>\d+)"
    r".*?rdt=(?P<rdt>[^&]+)"
    r".*?open=(?P<open>[^&]+)"
)


def _parse_thumb_url(url: str) -> dict | None:
    m = _IMG_URL_RE.search(url)
    if not m:
        return None
    return {
        "code": m.group("code"),
        "invnr": int(m.group("invnr")),
        "page": int(m.group("page")),
        "miahd": int(m.group("miahd")),
        "rdt": m.group("rdt"),
        "open": m.group("open"),
        "thumb_url": url,
    }


def _fullsize_url(thumb_url: str) -> str:
    """Switch ?format=thumb → ?format=large so the server returns the
    1024-pixel-tall PNG instead of the 300-pixel thumbnail."""
    url = thumb_url.replace("?format=thumb&", "?")
    url = url.replace("&format=thumb", "")
    url = url.replace("?format=thumb", "")
    if "format=large" not in url:
        url = url.replace("?miadt=", "?format=large&miadt=")
    return url


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _inventory_path(code: str) -> Path:
    return OUTPUT_DIR / f"inventory_{code}.json"


def _tokens_path(code: str) -> Path:
    return OUTPUT_DIR / f"tokens_{code}.json"


def _tokens_partial_path(code: str) -> Path:
    return OUTPUT_DIR / f"tokens_{code}_partial.json"


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _load_json(path: Path) -> object | None:
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Phase 1: Discover digitized invnrs for one kantoor
# ---------------------------------------------------------------------------

def _discover_invnrs(kantoor: str, code: str) -> list[dict]:
    """Return [{invnr, text, minr, hasScan}, ...] for one kantoor, cached."""
    cached = _load_json(_inventory_path(code))
    if isinstance(cached, list) and cached:
        print(f"    loaded {len(cached)} cached inventory entries")
        return cached

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    items: list[dict] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        page = context.new_page()

        # Step 1: find the Register IV minr from the kantoor's inv2 root.
        print(f"    loading inv2 root for {kantoor} (code {code}) …")
        page.goto(_inv2_url(code), wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(2_000)
        register = page.evaluate(_JS_FIND_REGISTER_IV)
        if not register:
            print(f"    WARNING: no Register IV section found for {kantoor}")
            browser.close()
            return []
        register_minr = int(register["minr"])
        print(f"    Register IV minr={register_minr}: {register['name'][:80]}")

        # Step 2: navigate to inv3 of Register IV and expand every sub-section
        # iteratively until no new swapinv links appear.  Each pass clicks the
        # links that were not yet expanded; new periods may surface as deeper
        # children load their own swapinvs.
        page.goto(_inv3_url(code, register_minr),
                  wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick]', state="attached", timeout=30_000)

        total_expanded = 0
        for _ in range(40):
            n = page.evaluate(_JS_EXPAND_SUBSECTIONS)
            if not n:
                break
            total_expanded += n
            page.wait_for_timeout(1_500)
        print(f"    expanded {total_expanded} sub-sections")

        items = page.evaluate(_JS_COLLECT_INVNRS)
        browser.close()

    digitized = [it for it in items if it["hasScan"]]
    print(f"    discovered {len(items)} leaf invnrs ({len(digitized)} digitized)")
    _save_json(_inventory_path(code), items)
    return items


# ---------------------------------------------------------------------------
# Phase 2: Harvest tokens for every digitized invnr
# ---------------------------------------------------------------------------

def _harvest_page_tokens(kantoor: str, code: str, invnrs: list[dict]) -> list[dict]:
    """Navigate to each digitized invnr's inv2 page and harvest its thumbnails.

    Resumes from a partial cache if one exists, writing the partial every 25
    invnrs so a crash mid-kantoor doesn't lose work.
    """
    complete_path = _tokens_path(code)
    cached_complete = _load_json(complete_path)
    if isinstance(cached_complete, list) and cached_complete:
        print(f"    loaded {len(cached_complete)} cached tokens (complete)")
        return cached_complete

    pages: list[dict] = []
    seen_keys: set[tuple[int, int]] = set()
    completed_invnrs: set[int] = set()

    partial = _load_json(_tokens_partial_path(code))
    if isinstance(partial, list) and partial:
        pages = partial
        for p in pages:
            seen_keys.add((p["invnr"], p["page"]))
        completed_invnrs = {p["invnr"] for p in pages}
        print(f"    resuming from {len(pages)} already-collected pages "
              f"({len(completed_invnrs)} invnrs)")

    digitized = [
        it for it in invnrs
        if it.get("hasScan") and it["invnr"] not in completed_invnrs
    ]
    if not digitized:
        if pages:
            _save_json(complete_path, pages)
            _tokens_partial_path(code).unlink(missing_ok=True)
        return pages

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_BROWSER_UA)
        p = context.new_page()

        for idx, it in enumerate(digitized):
            invnr = it["invnr"]
            invnr_minr = it["minr"]
            inv_text = it["text"]
            print(f"    [{idx + 1}/{len(digitized)}] invnr {invnr} "
                  f"({inv_text[:60].strip()} …)", end=" ", flush=True)

            p.goto(_inv2_minr_url(code, invnr_minr),
                   wait_until="networkidle", timeout=60_000)
            p.wait_for_timeout(1_500)

            fl = p.evaluate(_JS_STRIP_FORCE_LOAD)
            if not fl["total"]:
                print("0 pages")
                continue

            for _ in range(120):  # up to 60 s for very large invnrs
                if p.evaluate(_JS_STRIP_LOADED):
                    break
                p.wait_for_timeout(500)

            srcs: list[str] = p.evaluate(_JS_HARVEST_THUMBS)
            new_pages = 0
            for src in srcs:
                rec = _parse_thumb_url(src)
                if rec and rec["invnr"] == invnr:
                    key = (rec["invnr"], rec["page"])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        rec["inv_text"] = inv_text
                        pages.append(rec)
                        new_pages += 1
            print(f"{new_pages} new pages (total {len(pages)})")

            batch_idx = idx + 1
            if batch_idx % 25 == 0 or batch_idx == len(digitized):
                _save_json(_tokens_partial_path(code), pages)

        browser.close()

    print(f"    total pages: {len(pages)}")
    _save_json(complete_path, pages)
    _tokens_partial_path(code).unlink(missing_ok=True)
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
    dest_dir: Path,
    kantoor: str,
    code: str,
    invnr: int,
    inv_description: str,
    n_scans: int,
) -> None:
    """Mirror the metadata layout in CLAUDE.md's Gelderland example."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    naam = f"Memories van Successie, Kantoor {kantoor} {code} {invnr}".strip()
    # Description has "<num> <datering>"; strip the leading invnr to recover
    # just the datering ("1895 eerste kwartaal").  Falls back to the full
    # description when no match.
    m = re.match(r"^\s*\d+\s+(.+)$", inv_description)
    datering = m.group(1).strip() if m else inv_description
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": code,
        "brontype": "Memorie van Successie",
        "kantoor": kantoor,
        "inventarisnummer": str(invnr),
        "datering": datering,
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
    session.headers["Referer"] = "https://www.geldersarchief.nl/"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    done_file = OUTPUT_DIR / "done.txt"
    done: set[str] = set()
    if done_file.exists():
        done = set(done_file.read_text().splitlines())

    grand_downloaded = grand_skipped = grand_missing = 0

    for k_idx, (kantoor, code) in enumerate(KANTOREN.items()):
        print(f"\n{'='*60}")
        print(f"  [{k_idx + 1}/{len(KANTOREN)}] Kantoor {kantoor}  (code {code})")
        print(f"{'='*60}")

        if code in done:
            print("  Already fully downloaded, skipping.")
            continue

        # Phase 1: discover digitized leaf invnrs
        invnrs = _discover_invnrs(kantoor, code)
        digitized = [it for it in invnrs if it.get("hasScan")]
        if not digitized:
            print("  No digitized inventarisnummers, skipping.")
            with open(done_file, "a") as f:
                f.write(f"{code}\n")
            continue

        # Phase 2: harvest tokens
        pages = _harvest_page_tokens(kantoor, code, invnrs)
        if not pages:
            print("  No pages harvested, skipping.")
            with open(done_file, "a") as f:
                f.write(f"{code}\n")
            continue

        # Group pages by invnr
        invnr_pages: dict[int, list[dict]] = {}
        invnr_texts: dict[int, str] = {}
        for p in pages:
            invnr_pages.setdefault(p["invnr"], []).append(p)
            if p["invnr"] not in invnr_texts:
                invnr_texts[p["invnr"]] = p.get("inv_text", "")

        print(f"  {len(invnr_pages)} inventarisnummers with scans")

        # Phase 3: download
        safe_kantoor = kantoor.replace(" ", "_")[:60]
        downloaded = skipped = missing = 0
        for invnr, inv_pages in sorted(invnr_pages.items()):
            inv_text = invnr_texts.get(invnr, "")
            dest_dir = OUTPUT_DIR / safe_kantoor / f"{invnr:04d}"

            _write_metadata(dest_dir, kantoor, code, invnr, inv_text, len(inv_pages))

            inv_downloaded = inv_skipped = inv_missing = 0
            for pg in sorted(inv_pages, key=lambda x: x["page"]):
                url = _fullsize_url(pg["thumb_url"])
                # Filename matches the on-server convention: "{invnr}-{page:04d}.jpg"
                dest = dest_dir / f"{invnr}-{pg['page']:04d}.jpg"
                status = _download_file(session, url, dest)
                if status == "downloaded":
                    inv_downloaded += 1
                elif status == "exists":
                    inv_skipped += 1
                else:
                    inv_missing += 1
                time.sleep(0.15)

            print(f"  invnr {invnr} ({inv_text[:40].strip()}) "
                  f"{len(inv_pages)} pages "
                  f"({inv_downloaded} new, {inv_skipped} existing, "
                  f"{inv_missing} missing)")
            downloaded += inv_downloaded
            skipped += inv_skipped
            missing += inv_missing

        print(f"  Kantoor totals: {downloaded} new, {skipped} existing, "
              f"{missing} missing")

        grand_downloaded += downloaded
        grand_skipped += skipped
        grand_missing += missing

        with open(done_file, "a") as f:
            f.write(f"{code}\n")

    print(f"\n===== COMPLETE =====")
    print(f"Total: {grand_downloaded} downloaded, {grand_skipped} existing, "
          f"{grand_missing} missing")
    print("Done (Gelderland).")


if __name__ == "__main__":
    main()
