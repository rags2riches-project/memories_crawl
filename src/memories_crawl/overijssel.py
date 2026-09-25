"""Overijssel – Memories van Successie downloader.

Archive: Historisch Centrum Overijssel (HCO)
MAIS system: miadt=141, mivast=20, archive code 0136.4
WordPress proxy: https://collectieoverijssel.nl/wp-content/plugins/mais-mdws/maisi_ajax_proxy.php

How it works
────────────
Images live at:
    https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4/{invnr}/
        NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg
    Full-size: ?format=download&miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
    Thumbnail: add  ?format=thumb&miadt=141&miahd={miahd}&mivast=20&rdt={rdt}&open={token}
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

import csv
import json
import re
from pathlib import Path

import requests

from memories_crawl import download, filters, listing, paths
from memories_crawl.summary import PageTally, RunSummary, announce

ARCHIVE_NAME = "Historisch Centrum Overijssel"
ARCHIVE_NUMBER = "0136.4"
MAIS_ADT = "141"
MAIS_VAST = "20"
IMAGE_BASE = "https://preserve2.archieven.nl/mi-20/fonc-hco/0136.4"
ARCHIVE = "overijssel"
USER_AGENT = "memories-crawl/1.0"
# Requests per second for the image fetches: the pace the old fixed
# time.sleep(0.15) between images produced, now shared across workers.
DOWNLOAD_RATE = 1 / 0.15

# minr values for each kantoor's "Memories van Successie" item in the MAIS tree.
# These were discovered by browsing the collectieoverijssel.nl inv3 tree for
# miadt=141, mivast=20, micode=0136.4 (verified April 2026).
# The "Alfabetische Tafel / Klapper" sub-items are NOT listed here.
KANTOOR_MINR: dict[str, int] = {
    "Almelo": 2227676,
    "Deventer": 2227950,
    "Enschede": 2228207,
    "Goor": 2228335,
    "Kampen": 2228502,
    "Ommen": 2228649,
    "Raalte": 2228752,
    "Steenwijk": 2228889,
    "Vollenhove": 2228980,
    "Zwolle": 2229046,
}

_INV3_URL = (
    "https://collectieoverijssel.nl/collectie/archieven/"
    "?mivast=20&mizig=210&miadt=141&miaet=1&micode=0136.4"
    "&minr={minr}&milang=nl&miview=inv3"
)

# JS to collect the register stk3 toggle calls from the inv3 DOM, with the link text.
# The text is the item's datering ("12  1843 jan.-juni"), which the archive
# renders right there and which is the only date this pipeline can ever see --
# see gelderland._JS_COLLECT_INVNRS, which has always kept it (issue #38).
_JS_COLLECT_STK3 = """() => {
    return Array.from(document.querySelectorAll('.mi_tree_node.tpDB a[onclick*="stk3"]')).map(a => {
        const oc = a.getAttribute('onclick');
        const m = oc.match(/mi_inv3_toggle_stk\\((.+?)\\);\\s*return/s);
        return m ? { args: m[1], text: (a.textContent || '').trim().substring(0, 200) } : null;
    }).filter(Boolean);
}"""

# A token cache written before issue #40 either lacks descriptions altogether
# or may have descriptions claimed by structural tree nodes.  Changing the
# schema makes those caches safely self-invalidating after an upgrade.
TOKEN_CACHE_SCHEMA_VERSION = 2

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
    return paths.cache_file(ARCHIVE, f"tokens_minr_{minr}.json")


def _read_cached_tokens(minr: int) -> list[dict] | None:
    """Read a current-schema, non-empty token cache, if one exists."""
    cache_path = _get_token_cache_path(minr)
    if cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                cache = json.load(f)
            if (
                isinstance(cache, dict)
                and cache.get("schema_version") == TOKEN_CACHE_SCHEMA_VERSION
                and isinstance(cache.get("tokens"), list)
                and cache["tokens"]
            ):
                tokens: list[dict] = cache["tokens"]
                return tokens
        except (OSError, json.JSONDecodeError):
            pass
    return None


def _load_cached_tokens(minr: int) -> list[dict] | None:
    """Load cached page tokens if they exist and are non-empty."""
    tokens = _read_cached_tokens(minr)
    if tokens is not None:
        print(f"    loaded {len(tokens)} cached tokens")
    return tokens


def _save_cached_tokens(minr: int, tokens: list[dict]) -> None:
    """Save page tokens to cache file."""
    cache_path = _get_token_cache_path(minr)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(
            {"schema_version": TOKEN_CACHE_SCHEMA_VERSION, "tokens": tokens},
            f,
            ensure_ascii=False,
            indent=2,
        )


def _cached_kantoor_holds(minr: int, invnrs: set[str]) -> bool | None:
    """Whether the kantoor's token cache covers any of ``invnrs``.

    ``None`` when there is no cache: an absent cache is not evidence that the
    kantoor lacks the invnr, so the caller must fall through to the harvest.
    """
    tokens = _read_cached_tokens(minr)
    if tokens is None:
        return None
    return any(str(t.get("invnr")) in invnrs for t in tokens)


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
    # invnr → the tree description MAIS renders for it, e.g. "12  1843 jan.-juni"
    inv_texts: dict[int, str] = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        # Do NOT override User-Agent: the MAIS proxy returns no content for non-browser UAs.

        # Navigate to inv3 — MAIS fires an AJAX call via mi_useprox to populate items.
        # Wait for networkidle so scripts and the initial proxy AJAX both complete, then
        # confirm the stk3 links are actually in the DOM before proceeding.
        page.goto(_INV3_URL.format(minr=minr), wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick*="stk3"]', state="attached", timeout=30_000)

        # Collect only register stk3 calls (argument string + description).
        stk3_items: list[dict] = page.evaluate(_JS_COLLECT_STK3)
        print(f"    found {len(stk3_items)} stk3 items")

        for idx, item in enumerate(stk3_items):
            args = item["args"]
            text = (item.get("text") or "").strip()
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
                    # The DOM fallback above can return pages of an item other
                    # than this link's, so only a register description whose
                    # leading inventarisnummer matches may be retained.
                    leading = re.match(r"\s*(\d+)\b", text)
                    if leading is not None and int(leading.group(1)) == rec["invnr"]:
                        inv_texts.setdefault(rec["invnr"], text)

            if (idx + 1) % 25 == 0:
                print(
                    f"    processed {idx + 1}/{len(stk3_items)} items, "
                    f"{len(pages_by_key)} pages so far"
                )

        browser.close()

    result = sorted(pages_by_key.values(), key=lambda r: (r["invnr"], r["page"]))
    for rec in result:
        rec["inv_text"] = inv_texts.get(rec["invnr"], "")
    print(f"    total pages collected: {len(result)}")

    # Save to cache for future runs
    _save_cached_tokens(minr, result)

    return result


def _image_url(invnr: int, page: int, miahd: int, rdt: str, open_token: str) -> str:
    filename = f"NL-ZlHCO_0136.4_{invnr}_{page:04d}.jpg"
    return (
        f"{IMAGE_BASE}/{invnr}/{filename}"
        f"?format=download&miadt={MAIS_ADT}&miahd={miahd}&mivast={MAIS_VAST}&rdt={rdt}&open={open_token}"
    )


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    """Fetch one scan; see :func:`download.fetch_file` for the retry rules."""
    return download.fetch_file(
        session,
        url,
        dest,
        missing_statuses=(404, 202),
        timeout=120,
    )


def _datering(inv_text: str) -> str:
    """The description with its leading inventarisnummer stripped.

    MAIS renders "12  1843 jan.-juni"; the sidecar wants "1843 jan.-juni", the
    same shape gelderland writes.
    """
    m = re.match(r"^\s*\d+\s+(.+)$", inv_text)
    return m.group(1).strip() if m else inv_text.strip()


def _write_metadata(
    dest_dir: Path, kantoor: str, invnr: int, n_scans: int, inv_text: str = ""
) -> None:
    sidecar = dest_dir / "metadata.json"
    # Always rewrite: n_scans may have been wrong on a prior truncated run.
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": ARCHIVE_NUMBER,
        "brontype": "Memorie van Successie",
        "kantoor": kantoor,
        "inventarisnummer": str(invnr),
        "datering": _datering(inv_text),
        "omschrijving": inv_text,
        "n_scans": n_scans,
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


LIST_FIELDS = ["kantoor", "invnr", "pages", "description", *listing.YEAR_FIELDS]


def main(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    kantoor_filter = filters.normalize(kantoren)

    if out_dir is not None:
        paths.set_out_dir(out_dir)
    output_dir = paths.archive_dir(ARCHIVE)
    output_dir.mkdir(parents=True, exist_ok=True)

    downloader = download.Downloader(
        _download_file, workers=workers, rate=DOWNLOAD_RATE, session_factory=_session
    )
    try:
        csv_rows: list[dict] = []
        summary = RunSummary(ARCHIVE, "Overijssel", unit_name="kantoren")
        matched_any = False

        for kantoor, minr in KANTOOR_MINR.items():
            # Both filters are applied before the Playwright harvest: --kantoor by
            # name or minr, --invnr through the kantoor's token cache where one
            # exists (a missing cache proves nothing, so that kantoor is visited).
            if not filters.matches(kantoor_filter, kantoor, minr):
                continue
            if invnrs is not None and _cached_kantoor_holds(minr, invnrs) is False:
                continue

            print(f"\n  {kantoor} (minr={minr}): fetching page tokens via Playwright …")
            pages = _fetch_page_tokens_via_playwright(minr)

            if not pages:
                print(f"    WARNING: no pages found for {kantoor}")
                continue

            # Group by invnr to write per-invnr metadata. The description comes
            # along with the tokens, so a warm cache from before issue #38 simply
            # reports no date rather than a wrong one.
            invnr_pages: dict[int, list[dict]] = {}
            invnr_texts: dict[int, str] = {}
            for p in pages:
                invnr_pages.setdefault(p["invnr"], []).append(p)
                if not invnr_texts.get(p["invnr"]):
                    invnr_texts[p["invnr"]] = p.get("inv_text") or ""

            # --invnr filter before download
            if invnrs is not None:
                invnr_pages = {
                    invnr: ips for invnr, ips in invnr_pages.items() if str(invnr) in invnrs
                }

            if invnr_pages:
                matched_any = True

            # --list-invnrs: print and skip download for this kantoor
            if list_invnrs:
                # Page counts are exact here: the token harvest that feeds them has
                # to run before anything can be downloaded anyway, so --count-scans
                # has nothing left to resolve.
                print(f"\n{kantoor}:")
                print(f"  {'invnr':>6}  {'pages':>6}  {'period':<11}  description")
                print(f"  {'------':>6}  {'------':>6}  {'-' * 11:<11}  -----------")
                for invnr in sorted(invnr_pages.keys()):
                    pages_here = len(invnr_pages[invnr])
                    if only_digitized and not listing.has_scans(pages_here):
                        continue
                    inv_text = invnr_texts.get(invnr, "")
                    year_from, year_to = listing.parse_years(inv_text, invnr)
                    print(
                        f"  {invnr:>6}  {pages_here:>6}"
                        f"  {listing.fmt_period(year_from, year_to):<11}  {inv_text[:60]}"
                    )
                    csv_rows.append(
                        {
                            "kantoor": kantoor,
                            "invnr": invnr,
                            "pages": pages_here,
                            "description": inv_text,
                            **listing.year_row(year_from, year_to),
                        }
                    )
                continue

            n_pages = sum(len(v) for v in invnr_pages.values())
            announce(n_pages, len(invnr_pages), kantoor, indent="    ")
            summary.units += 1
            summary.registers += len(invnr_pages)

            kantoor_tally = PageTally()
            for invnr, inv_pages in sorted(invnr_pages.items()):
                dest_dir = output_dir / kantoor / str(invnr)
                _write_metadata(
                    dest_dir, kantoor, invnr, len(inv_pages), invnr_texts.get(invnr, "")
                )
                jobs = [
                    download.Job(
                        _image_url(invnr, p["page"], p["miahd"], p["rdt"], p["open"]),
                        dest_dir / f"{p['page']:04d}.jpg",
                    )
                    for p in inv_pages
                ]
                tally = PageTally()

                def record_page(job: download.Job, status: str) -> None:
                    tally.record(status, job.dest)
                    summary.pages.record(status, job.dest)

                downloader.run(jobs, on_result=record_page)

                print(f"    invnr {invnr} {tally.describe(len(inv_pages))}", flush=True)
                # Fold in per register, not per kantoor, so a run that dies midway
                # still reports everything it downloaded.
                kantoor_tally += tally

            print(f"    {kantoor}: {kantoor_tally.describe()}")

        if (invnrs is not None or kantoor_filter is not None) and not matched_any:
            print(
                f"\nWARNING: {filters.describe(invnrs, kantoren)} matched no "
                f"inventarisnummer in any of the {len(KANTOOR_MINR)} kantoren."
            )

        if list_invnrs:
            print()
            if csv_out and csv_rows:
                with open(csv_out, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=LIST_FIELDS)
                    writer.writeheader()
                    writer.writerows(csv_rows)
                print(f"Wrote {len(csv_rows)} rows to {csv_out}\n")
            return

        summary.report()
        return summary
    finally:
        downloader.close()


if __name__ == "__main__":
    main()
