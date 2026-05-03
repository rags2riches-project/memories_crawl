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

Each inventarisnummer's scans are listed on an ldt (list/detail) view page:
    https://hetutrechtsarchief.nl/onderzoek/resultaten/archieven
        ?mivast=39&mizig=236&miadt=39&miview=ldt&milang=nl
        &micode={archive_code}-{invnr}&miaet=54

Scans are served from:
    https://img.hetutrechtsarchief.nl/mi-39/hua/archiefbank/.../NL-UtHUA_{micode}_{invnr}_{page:04d}.jpg
    ?miadt=39&miahd={miahd}&mivast=39&rdt={rdt}&open={token}

Each page has unique miahd/open tokens that are session-bound.

Strategy
────────
1. Navigate to the inv2 inventory page for each kantoor's archive code.
2. Expand the tree to discover MvS subsection minr values.
3. Navigate to inv3 view for each subsection to collect inventarisnummer data.
4. For each inventarisnummer, navigate to its ldt view (Playwright/Chromium).
5. Force-load all strip chunks via mi_strip_store.populate().
6. Extract per-page image URLs (with auth tokens) from strip sliders.
7. Download full-size images (without ?format=thumb) using requests.
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

# Kantoren and their archive codes (micode), with their MvS subsection minr
# values discovered by browsing the inventory tree.
KANTOREN: dict[str, dict] = {
    "Amersfoort": {
        "micode": "337-2",
        "subsections": None,  # discovered dynamically
    },
    "Amerongen": {
        "micode": "337-1",
        "subsections": None,
    },
    "Loenen": {
        "micode": "337-3",
        "subsections": None,
    },
    "Maarssen": {
        "micode": "337-4",
        "subsections": None,
    },
    "Montfoort": {
        "micode": "337-5",
        "subsections": None,
    },
    "Rhenen": {
        "micode": "337-6",
        "subsections": None,
    },
    "Utrecht": {
        "micode": "337-7",
        "subsections": None,
    },
    "IJsselstein": {
        "micode": "337-10",
        "subsections": None,
    },
    "Vianen": {
        "micode": "1279",
        "subsections": None,
    },
    "Woerden": {
        "micode": "1274",
        "subsections": None,
    },
    "Wijk bij Duurstede": {
        "micode": "337-9",
        "subsections": None,
    },
}

# Also covers Departement Utrecht/Amstelland (micode=336)


def _subsections_inv3_url(micode: str, minr: int) -> str:
    return (
        "https://hetutrechtsarchief.nl/onderzoek/resultaten/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&miaet=1&micode={micode}&minr={minr}"
        f"&milang=nl&miview=inv3"
    )


def _ldt_url(micode: str, invnr: int) -> str:
    return (
        "https://hetutrechtsarchief.nl/onderzoek/resultaten/archieven"
        f"?mivast={MAIS_VAST}&mizig=236&miadt={MAIS_ADT}"
        f"&miview=ldt&milang=nl&micode={micode}-{invnr}&miaet=54"
    )


_JS_EXPAND_TREE = """() => {
    // Click all expandable tree links to reveal child items
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
    // After expanding tree, find subsection items that have memories text
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
        // Only keep items with "Memories" in their text
        if (text.toLowerCase().includes('memories') || text.match(/^\\d+-\\d+/)) {
            result.push({ text: text.substring(0, 120), minr });
        }
    }
    return result;
}"""

_JS_INVNR_ITEMS = """() => {
    const links = document.querySelectorAll('a[onclick*="stk3"]');
    const result = [];
    const seen = new Set();
    for (const a of links) {
        const text = (a.textContent || '').trim();
        if (!text || text === 'Toon details van deze beschrijving') continue;
        const numMatch = text.match(/^(\\d+)\\s/);
        if (!numMatch) continue;
        const invnr = parseInt(numMatch[1], 10);
        if (seen.has(invnr)) continue;
        seen.add(invnr);
        const onclick = a.getAttribute('onclick') || '';
        const minrMatch = onclick.match(/minr=(\\d+)/);
        const minr = minrMatch ? parseInt(minrMatch[1], 10) : 0;
        result.push({ invnr, minr, text: text.substring(0, 120) });
    }
    return result;
}"""

_JS_LOAD_ALL_STRIPS = """() => {
    if (typeof mi_strip_store === 'undefined') return 0;
    let triggered = 0;
    for (const v of Object.values(mi_strip_store)) {
        if (!v.aantal || typeof v.populate !== 'function') continue;
        const chunkSize = v.numloadScans || 25;
        const totalChunks = Math.ceil(v.aantal / chunkSize);
        for (let chunk = 0; chunk < totalChunks; chunk++) {
            if (!v.loadedChunks || v.loadedChunks.indexOf(chunk) === -1) {
                v.cursor = chunk * chunkSize + 1;
                v.dir = -1;
                v.populate();
                triggered++;
            }
        }
    }
    return triggered;
}"""

_JS_STRIPS_LOADED = """() => {
    if (typeof mi_strip_store === 'undefined') return true;
    for (const v of Object.values(mi_strip_store)) {
        if (!v.aantal) continue;
        const loaded = (v.loadedScans || []).length;
        if (loaded < v.aantal) return false;
    }
    return true;
}"""

_JS_EXTRACT_IMAGES = """() => {
    const urls = new Set();
    if (typeof mi_strip_store === 'undefined') return [];
    for (const v of Object.values(mi_strip_store)) {
        if (!v.sslider) continue;
        const imgs = v.sslider.querySelectorAll('img[src*="img.hetutrechtsarchief.nl"]');
        imgs.forEach(img => {
            if (img.src) urls.add(img.src);
        });
    }
    return Array.from(urls).sort();
}"""

_IMG_URL_RE = re.compile(
    r"NL-UtHUA_(?P<micode>[^_]+)_(?P<invnr>\d+)_(?P<page>\d+)\.jpg"
    r"\?.*?miahd=(?P<miahd>\d+)"
    r".*?rdt=(?P<rdt>[^&]+)"
    r".*?open=(?P<open>[^&]+)"
)


def _parse_thumb_url(url: str) -> dict | None:
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
    }


def _image_url(parsed: dict) -> str:
    """Build full-size URL (without ?format=thumb)."""
    base = (
        f"https://img.hetutrechtsarchief.nl/mi-{MAIS_VAST}/hua/archiefbank"
        f"/_Projecten2019/DTR05_2019_{parsed['micode']}_20190913_002"
        f"/{parsed['invnr']}/NL-UtHUA_{parsed['micode']}_{parsed['invnr']}_{parsed['page']:04d}.jpg"
    )
    return (
        f"{base}"
        f"?miadt={MAIS_ADT}&miahd={parsed['miahd']}"
        f"&mivast={MAIS_VAST}&rdt={parsed['rdt']}&open={parsed['open']}"
    )


_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)


def _new_page(pw) -> tuple:
    """Create a new browser and page with proper User-Agent."""
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context(user_agent=_BROWSER_UA)
    page = context.new_page()
    return browser, page


def _discover_subsections(micode: str) -> list[dict]:
    """Navigate to inv2 page and discover MvS subsections.

    Expands the inventory tree and finds subsection items
    that contain "Memories van successie" text.

    Returns [{text, minr}, ...] for each MvS subsection found.
    """
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    url = (
        "https://hetutrechtsarchief.nl/onderzoek/resultaten/archieven"
        f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
        f"&micode={micode}&milang=nl&miview=inv2"
    )

    with sync_playwright() as pw:
        browser, page = _new_page(pw)
        page.goto(url, wait_until="networkidle", timeout=60_000)

        # Expand all tree nodes by clicking openinv links
        expanded = page.evaluate(_JS_EXPAND_TREE)
        page.wait_for_timeout(4_000)

        # Expand again (might reveal second-level items)
        expanded2 = page.evaluate(_JS_EXPAND_TREE)
        page.wait_for_timeout(4_000)

        # Collect subsection entries
        subsections: list[dict] = page.evaluate(_JS_SUBSECTIONS)
        browser.close()

    return subsections


def _collect_invnr_items(micode: str, minr: int) -> list[dict]:
    """Navigate to inv3 view for a subsection and collect all inventarisnummers.

    Returns [{invnr, minr, text}, ...] sorted by invnr.
    """
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    url = _subsections_inv3_url(micode, minr)

    with sync_playwright() as pw:
        browser, page = _new_page(pw)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(2_000)

        items: list[dict] = page.evaluate(_JS_INVNR_ITEMS)
        browser.close()

    return sorted(items, key=lambda x: x["invnr"])


def _collect_page_tokens(micode: str, invnr: int) -> list[dict]:
    """Return parsed image metadata for every page of one inventarisnummer.

    Launches a headless Chromium browser, navigates to the ldt view,
    loads all strip chunks via JavaScript, and harvests thumbnail URLs.

    Returns [{micode, invnr, page, miahd, rdt, open}, ...].
    """
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    pages_by_key: dict[int, dict] = {}

    with sync_playwright() as pw:
        browser, page = _new_page(pw)

        url = _ldt_url(micode, invnr)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(2_000)

        # Trigger loading of all chunks
        triggered = page.evaluate(_JS_LOAD_ALL_STRIPS)
        if triggered == 0:
            # try clicking first image to initialize strip if needed
            first_img = page.query_selector('img[src*="img.hetutrechtsarchief.nl"]')
            if first_img:
                first_img.click()
                page.wait_for_timeout(2_000)
                triggered = page.evaluate(_JS_LOAD_ALL_STRIPS)

        if triggered > 0:
            # Poll until all strips are fully loaded
            for _ in range(120):
                if page.evaluate(_JS_STRIPS_LOADED):
                    break
                page.wait_for_timeout(500)

        # Extract image URLs from strip sliders
        img_urls: list[str] = page.evaluate(_JS_EXTRACT_IMAGES)

        for url in img_urls:
            rec = _parse_thumb_url(url)
            if rec:
                pages_by_key[rec["page"]] = rec

        browser.close()

    result = sorted(pages_by_key.values(), key=lambda r: r["page"])
    return result


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
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": micode,
        "brontype": "Memorie van Successie",
        "kantoor": kantoor,
        "inventarisnummer": str(invnr),
        "omschrijving": inv_description,
        "n_scans": n_scans,
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(dest_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for kantoor, info in KANTOREN.items():
        micode = info["micode"]
        print(f"\n{'='*60}")
        print(f"  {kantoor} (micode={micode})")
        print(f"{'='*60}")

        # Step 1: Discover subsections (items with "Memories" in text)
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
            print(f"\n  --- Section {section_idx + 1}/{len(subsections)} (minr={section_minr}) ---")

            # Step 2: Collect inventarisnummers in this subsection
            items = _collect_invnr_items(micode, section_minr)
            if not items:
                print("    No items found in this section")
                continue

            print(f"    Found {len(items)} inventarisnummers: {items[0]['invnr']}–{items[-1]['invnr']}")

            for item in items:
                invnr = item["invnr"]
                inv_text = item["text"]
                key = str(invnr)
                if key in done:
                    continue

                dest_dir = OUTPUT_DIR / kantoor / f"{invnr:04d}"
                print(f"    invnr {invnr} ({inv_text[:40]}) …", end=" ", flush=True)

                # Step 3: Collect page tokens from the ldt view
                pages = _collect_page_tokens(micode, invnr)

                if not pages:
                    print("no scans found")
                    with open(done_file, "a") as f:
                        f.write(key + "\n")
                    time.sleep(0.5)
                    continue

                _write_metadata(dest_dir, kantoor, micode, invnr, inv_text, len(pages))

                downloaded = skipped = missing = 0
                for p in pages:
                    url = _image_url(p)
                    dest = dest_dir / f"{p['page']:04d}.jpg"
                    status = _download_file(session, url, dest)
                    if status == "downloaded":
                        downloaded += 1
                    elif status == "exists":
                        skipped += 1
                    else:
                        missing += 1
                    time.sleep(0.15)

                print(f"{len(pages)} pages ({downloaded} new, {skipped} existing, {missing} missing)")
                with open(done_file, "a") as f:
                    f.write(key + "\n")

    print("\nDone (Utrechts Archief).")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
