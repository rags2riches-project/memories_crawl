"""Limburg – Memories van Successie downloader.

Archive: Regionaal Historisch Centrum Limburg / Historisch Centrum Limburg (RHCL),
         te Maastricht.
MAIS system on archieven.nl: miadt=38, mivast=0
Two inventory codes hold Memories van Successie at RHCL:

    07.D03 – Memories van Successie, 1818-1900 (1905)
             1,314 inventarisnummers, ~111 digitized (≈ 104 k scans),
             organised by **place of death** (Amby, Amstenrade, …).
    07.D08 – Memories van Successie, 1901-1927
               460 inventarisnummers, ~ 42 digitized (≈   7 k scans),
             organised by **kantoor** (Gulpen, Heerlen, Maastricht, …).
             07.D08 also has a sister section "Tafels 5bis" which we
             EXCLUDE per the project-wide Tafel V-bis rule.

Image server
────────────
Scans are served from preserve3.archieven.nl behind per-page tokens:

    https://preserve3.archieven.nl/mi-0/fonc-rhcl/{code}/{invnr}/
        NL-MtHCL_{code}_{invnr}_{page:04d}.jpg
    + ?format=thumb&miadt=38&miahd={miahd}&mivast=0&rdt={rdt}&open={token}  → 209×300 PNG
    + ?format=large&miadt=38&miahd={miahd}&mivast=0&rdt={rdt}&open={token}  → 714×1024 PNG
    + ?<tokens>                       (no format param)                     → 209×300 PNG
    + ?format=download&<tokens>                         → original JPEG
    + no tokens                                        → HTTP 202 SVG placeholder

The original JPEG is returned by ``format=download`` with the same strip
tokens. No extra viewer load or IIPSrv tile stitching is needed. Omitting
``format`` returns a thumbnail; ``format=large`` is only a 1024-pixel preview.

Strategy
────────
1. (Playwright, once per archive code) Build the digitized-inventory map:
   navigate to the inv2 root, expand every "Records N t/m M" batch via the
   ``mi_inv3_swapinv`` AJAX, then read every ``<a onclick*="stk">`` node.
   Items whose row carries a ``h_scan.gif`` marker are digitized.
2. (Playwright, once per digitized invnr) Harvest per-page tokens:
   navigate to the per-invnr inv2 URL (the strip auto-loads), then click
   the strip "Volgende" arrow until the snavuit class appears, scraping
   ``img[src*="/fonc-rhcl/"]`` srcs each step. Tokens are cached so reruns
   skip Playwright entirely.
3. (requests) Download every page at ``format=download`` to
   ``{code}/{invnr}/NL-MtHCL_{code}_{invnr}_{page:04d}.jpg``, writing a
   ``metadata.json`` sidecar per inventarisnummer.  Releases up to 0.5.1
   saved pages as ``….png``; a rerun renames such a file when it already
   holds the original JPEG, and replaces (then removes) a real PNG preview.

Dependency: ``uv sync && uv run playwright install chromium``.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

import requests

from memories_crawl import download, filters, listing, paths
from memories_crawl.summary import PageTally, RunSummary, announce

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARCHIVE_NAME = "Regionaal Historisch Centrum Limburg"
MAIS_ADT = "38"
MAIS_VAST = "0"
IMAGE_BASE = "https://preserve3.archieven.nl/mi-0/fonc-rhcl"
USER_AGENT = "memories-crawl/1.0"
# Requests per second for the image fetches: the pace the old fixed
# time.sleep(0.10) after every fetched image produced, now shared across workers.
DOWNLOAD_RATE = 10.0

ARCHIVE = "limburg"

# Each archive code's behaviour in the MAIS tree.
#   parent_minr: if not None, we drill into this sub-section's children rather
#                than the archive root – this is how we skip "Tafels 5bis" in
#                07.D08 without walking it.
#   axis: human label for the per-item grouping in the title ("Plaats" or "Kantoor").
ARCHIVE_CODES: dict[str, dict] = {
    "07.D03": {
        "title": "Memories van Successie, 1818-1900 (1905)",
        "parent_minr": None,  # walk from the archive root
        "axis": "Plaats",
    },
    "07.D08": {
        "title": "Memories van Successie, 1901-1927",
        # 07.D08 root has two children: the MvS section (1014062) and "Tafels 5bis"
        # (1014481). We start at the MvS minr so the tafel branch is never visited.
        "parent_minr": 1014062,
        "axis": "Kantoor",
    },
}

# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------

_BASE_INV2 = (
    "https://www.archieven.nl/nl/zoeken"
    f"?mivast={MAIS_VAST}&mizig=210&miadt={MAIS_ADT}"
    "&miview=inv2&milang=nl&micode={code}"
)


def _inv2_root_url(code: str, minr: int | None = None) -> str:
    url = _BASE_INV2.format(code=code)
    if minr is not None:
        url += f"&minr={minr}"
    return url


# ---------------------------------------------------------------------------
# JavaScript snippets used via page.evaluate()
# ---------------------------------------------------------------------------

# Click every "Records N t/m M" toggle on the current page that has not yet
# been clicked, returning the number of new clicks. Re-invoke until 0.
_JS_EXPAND_BATCHES = """() => {
    const links = Array.from(document.querySelectorAll(
        'a[onclick*="swapinv"]:not([data-mvs-clicked])'));
    links.forEach(a => {
        a.setAttribute('data-mvs-clicked', '1');
        const oc = (a.getAttribute('onclick') || '')
            .replace('return false;', '')
            .replace('mi_no_auto_open_tree(this);', '');
        try { eval(oc); } catch (e) { /* swallow – next iter retries */ }
    });
    return links.length;
}"""

# Read every leaf inventarisnummer link visible in the tree. Filters non-
# numeric prefixes (placeholders such as "Toon details van deze beschrijving"
# attached to non-leaf section nodes).
_JS_LIST_ITEMS = """() => {
    const links = Array.from(document.querySelectorAll('a[onclick*="stk"]'));
    const out = [];
    for (const a of links) {
        const txt = (a.textContent || '').trim();
        const m = txt.match(/^(\\d+)\\s+(.+)$/);
        if (!m) continue;
        const node = a.closest('.mi_tree_node');
        const hasScan = node && node.querySelector('img[src*="h_scan"]') != null;
        const mr = (a.getAttribute('onclick') || '').match(/minr=(\\d+)/);
        out.push({
            invnr: parseInt(m[1]),
            title: m[2].trim(),
            minr:  mr ? parseInt(mr[1]) : null,
            hasScan: !!hasScan,
        });
    }
    return out;
}"""

# Get all current /fonc-rhcl/ thumbnail srcs anywhere on the page.
_JS_HARVEST_THUMBS = (
    "() => Array.from(document.querySelectorAll('img[src*=\"/fonc-rhcl/\"]'))"
    "          .map(i => i.src)"
)

# Click the strip's "Volgende" arrow if it's enabled; return whether the click
# actually happened (i.e. there were more pages to load).
_JS_STRIP_NEXT = """() => {
    const a = document.querySelector('.snext:not(.snavuit) a');
    if (!a) return false;
    a.click();
    return true;
}"""


# ---------------------------------------------------------------------------
# Thumb URL → token record
# ---------------------------------------------------------------------------

# Filename format: NL-MtHCL_<archive_code>_<invnr>_<page>.jpg
# archive_code contains a dot ("07.D03"). Capture invnr/page from the trailing
# segment, then read the query parameters by name.
_SRC_FILENAME = re.compile(r"NL-MtHCL_[\d.A-Za-z]+_(\d+)_(\d+)\.jpg")


def _parse_thumb_src(src: str) -> dict | None:
    """Return {invnr, page, miahd, rdt, open} from a fonc-rhcl thumb URL, or None."""
    fn = _SRC_FILENAME.search(src)
    if not fn:
        return None
    qs = src.split("?", 1)[1] if "?" in src else ""
    params: dict[str, str] = {}
    for kv in qs.split("&"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            params[k] = v
    if "miahd" not in params or "open" not in params:
        return None
    return {
        "invnr": int(fn.group(1)),
        "page": int(fn.group(2)),
        "miahd": params["miahd"],
        "rdt": params.get("rdt", ""),
        "open": params["open"],
    }


# ---------------------------------------------------------------------------
# Title parser – splits "Place name, 1818-1828" into (name, datering)
# ---------------------------------------------------------------------------

_DATERING_RE = re.compile(r",\s*([\d\-/]+(?:\s*[-–]\s*[\d\-/]+)?)\s*$")
# 07.D08 titles consistently start with "Kantoor "; some also have a duplicated
# "Kantoor Kantoor X" prefix in the source data – strip both copies.
_KANTOOR_PREFIX = re.compile(r"^(?:Kantoor\s+)+", re.IGNORECASE)


def _split_title(raw_title: str, axis: str) -> tuple[str, str]:
    """Return (name, datering). ``axis`` is "Plaats" or "Kantoor"."""
    datering = ""
    name = raw_title
    m = _DATERING_RE.search(raw_title)
    if m:
        datering = m.group(1).strip()
        name = raw_title[: m.start()].rstrip(", ").strip()
    if axis == "Kantoor":
        name = _KANTOOR_PREFIX.sub("", name).strip() or name
    return name, datering


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _inventory_cache_path(code: str) -> Path:
    return paths.cache_file(ARCHIVE, f"inventory_{code}.json")


def _tokens_cache_path(code: str, invnr: int) -> Path:
    return paths.cache_file(ARCHIVE, f"tokens_{code}_{invnr}.json")


def _load_json(path: Path) -> object | None:
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Phase 1: enumerate the digitized inventory for one archive code
# ---------------------------------------------------------------------------


def _keep_item(
    item: dict, code: str, invnrs: set[str] | None, kantoor_filter: set[str] | None
) -> bool:
    """Whether one inventory item survives the --invnr / --kantoor filters.

    ``name`` is the place of death for 07.D03 and the kantoor for 07.D08 --
    the ``place_or_kantoor`` column of ``--list-invnrs`` -- and the archive
    code itself is accepted as a kantoor name too.
    """
    if invnrs is not None and str(item.get("invnr")) not in invnrs:
        return False
    return filters.matches(kantoor_filter, code, item.get("name") or "")


def _cached_inventory(code: str) -> list[dict] | None:
    """The cached inventory for ``code``, or ``None`` when there is none.

    A missing cache is not evidence that the code holds nothing, so callers
    must fall through to a full :func:`_harvest_inventory` pass.
    """
    cached = _load_json(_inventory_cache_path(code))
    if not isinstance(cached, list) or not cached:
        return None
    return cached


def _harvest_inventory(code: str) -> list[dict]:
    """Return every digitized inventarisnummer for ``code``.

    Each record is ``{invnr, title, minr, hasScan, name, datering}``.
    Results are cached in ``<out-dir>/.cache/limburg/inventory_<code>.json``.
    """
    cached = _load_json(_inventory_cache_path(code))
    if isinstance(cached, list) and cached:
        print(f"    loaded {len(cached)} cached inventory items for {code}")
        return cached

    spec = ARCHIVE_CODES[code]
    axis = spec["axis"]

    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        url = _inv2_root_url(code, spec["parent_minr"])
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_selector('a[onclick*="stk"]', state="attached", timeout=30_000)
        page.wait_for_timeout(1_500)

        # Expand every "Records N t/m M" batch. Each expansion is async, so
        # loop until no new toggles are present.
        for _ in range(40):  # 13 batches for 07.D03; loop guard is generous
            n_clicked = page.evaluate(_JS_EXPAND_BATCHES)
            if not n_clicked:
                break
            page.wait_for_timeout(1_500)

        items: list[dict] = page.evaluate(_JS_LIST_ITEMS)
        browser.close()

    # Deduplicate (the placeholder "Toon details" link can occur twice when a
    # section header and child both attach an onclick=stk handler), drop
    # non-digitized items, and decorate with parsed name/datering.
    seen: set[int] = set()
    out: list[dict] = []
    for r in items:
        if r["invnr"] in seen:
            continue
        seen.add(r["invnr"])
        if not r["hasScan"]:
            continue
        title = r["title"] or ""
        # Defensive Tafel V-bis filter (the parent-minr trick already skips
        # Tafels 5bis in 07.D08, but check title too).
        low = title.lower()
        if "tafel" in low or "v-bis" in low or "5bis" in low:
            continue
        name, datering = _split_title(title, axis)
        out.append(
            {
                "invnr": r["invnr"],
                "minr": r["minr"],
                "title": title,
                "name": name,  # Plaats or Kantoor
                "datering": datering,
                "axis": axis,
            }
        )
    out.sort(key=lambda r: r["invnr"])

    _save_json(_inventory_cache_path(code), out)
    print(f"    {code}: {len(out)} digitized inventarisnummers")
    return out


# ---------------------------------------------------------------------------
# Phase 2: harvest per-page tokens for one inventarisnummer
# ---------------------------------------------------------------------------


def _harvest_tokens(page, code: str, invnr: int, minr: int) -> list[dict]:
    """Open the per-invnr page and step the strip's Volgende arrow to exhaustion."""
    url = _inv2_root_url(code) + f"&minr={minr}"
    page.goto(url, wait_until="networkidle", timeout=60_000)
    # Strip thumbs auto-load after a tick – give them a moment.
    try:
        page.wait_for_selector('img[src*="/fonc-rhcl/"]', state="attached", timeout=30_000)
    except Exception:
        return []
    page.wait_for_timeout(1_500)

    by_key: dict[int, dict] = {}

    def absorb() -> int:
        before = len(by_key)
        srcs: list[str] = page.evaluate(_JS_HARVEST_THUMBS)
        for s in srcs:
            rec = _parse_thumb_src(s)
            if rec and rec["invnr"] == invnr:
                by_key[rec["page"]] = rec
        return len(by_key) - before

    absorb()

    # Click Volgende until disabled, or until 6 consecutive clicks yield no
    # new pages (safety stop for unexpected DOM states).
    no_progress = 0
    for _ in range(2_000):  # the largest registers are ~1000 pages
        clicked = page.evaluate(_JS_STRIP_NEXT)
        if not clicked:
            break
        page.wait_for_timeout(900)
        added = absorb()
        if added == 0:
            no_progress += 1
            if no_progress >= 6:
                break
        else:
            no_progress = 0

    absorb()  # last sweep after the loop exits
    return sorted(by_key.values(), key=lambda r: r["page"])


def _ensure_tokens(page, code: str, invnr: int, minr: int) -> list[dict]:
    """Return cached tokens or harvest fresh ones, caching the result."""
    cache = _tokens_cache_path(code, invnr)
    cached = _load_json(cache)
    if isinstance(cached, list) and cached:
        return cached
    tokens = _harvest_tokens(page, code, invnr, minr)
    if tokens:
        _save_json(cache, tokens)
    return tokens


# ---------------------------------------------------------------------------
# Phase 3: download
# ---------------------------------------------------------------------------


def _image_url(code: str, tok: dict) -> str:
    filename = _page_filename(code, tok)
    return (
        f"{IMAGE_BASE}/{code}/{tok['invnr']}/{filename}"
        "?format=download"
        f"&miadt={MAIS_ADT}&miahd={tok['miahd']}"
        f"&mivast={MAIS_VAST}&rdt={tok['rdt']}&open={tok['open']}"
    )


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Referer"] = "https://www.archieven.nl/"
    return s


def _page_filename(code: str, tok: dict) -> str:
    """On-disk name of one page: the server's own filename, ``.jpg`` included.

    Up to 0.5.1 pages were saved as ``….png``.  ``download.fetch_file`` only
    checks the JPEG signature for ``.jpg`` / ``.jpeg`` names, so under ``.png``
    an old 1024-pixel PNG preview passed as "exists" and was never replaced,
    and a fresh download went unvalidated.
    """
    return f"NL-MtHCL_{code}_{tok['invnr']}_{tok['page']:04d}.jpg"


_JPEG_MAGIC = b"\xff\xd8\xff"


def _is_jpeg(path: Path) -> bool:
    try:
        with path.open("rb") as stream:
            return stream.read(3) == _JPEG_MAGIC
    except OSError:
        return False


def _download_one(session: requests.Session, url: str, dest: Path) -> str:
    """Fetch one scan with shared validation, retries and atomic replacement.

    Migrates the pre-0.5.2 ``.png`` name first.  A legacy file that already
    holds JPEG bytes (0.5.1 downloaded the original under the wrong suffix) is
    renamed to ``dest`` and reported as ``exists`` -- no request is made.  A
    legacy file that really is a PNG preview is left alone until the original
    has landed at ``dest``, then removed; a failed download keeps it.
    """
    legacy = dest.with_suffix(".png")
    if legacy.exists() and not _is_jpeg(dest) and _is_jpeg(legacy):
        legacy.replace(dest)
        return "exists"
    # Looked up at call time on purpose: wrappers patch download.fetch_file.
    status = download.fetch_file(session, url, dest, missing_statuses=(202, 404))
    if status in ("downloaded", "exists") and legacy.exists() and _is_jpeg(dest):
        legacy.unlink(missing_ok=True)
    return status


def _write_metadata(dest_dir: Path, code: str, item: dict, n_scans: int) -> None:
    naam = f"Memorie van Successie {item['name']} {code} {item['invnr']}".strip()
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": code,
        "brontype": "Memorie van Successie",
        item["axis"].lower(): item["name"],  # "plaats" or "kantoor"
        "datering": item["datering"],
        "inventarisnummer": str(item["invnr"]),
        "naam": naam,
        "n_scans": n_scans,
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(dest_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


LIST_FIELDS = [
    "code",
    "invnr",
    "place_or_kantoor",
    "datering",
    "pages",
    "title",
    *listing.YEAR_FIELDS,
]


def _cached_page_count(code: str, invnr: int) -> int | None:
    """Pages for one invnr from its token cache, or ``None`` when uncached.

    Tokens are cached per inventarisnummer here, so a cache hit is an exact
    count and a miss is genuinely unknown -- there is no partial-cache case to
    misread as a zero.
    """
    tokens = _load_json(_tokens_cache_path(code, invnr))
    return len(tokens) if isinstance(tokens, list) else None


def _harvest_all_tokens(code: str, items: list[dict]) -> None:
    """Fill the token cache for every uncached invnr of one archive code.

    One Playwright session is shared across all invnrs; individual page
    navigations reuse the same browser context.
    """
    from playwright.sync_api import sync_playwright  # noqa: PLC0415

    if all(_tokens_cache_path(code, it["invnr"]).exists() for it in items):
        print(f"\n  {code}: all token caches present, skipping Playwright.")
        return

    print(f"\n  {code}: harvesting tokens for {len(items)} registers …")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        for idx, it in enumerate(items, 1):
            if _tokens_cache_path(code, it["invnr"]).exists():
                continue
            print(
                f"    [{idx:>3}/{len(items)}] invnr {it['invnr']} "
                f"({it['name']}, {it['datering']}) …",
                flush=True,
            )
            toks = _ensure_tokens(page, code, it["invnr"], it["minr"])
            print(f"        → {len(toks)} pages")
        browser.close()


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
        _download_one, workers=workers, rate=DOWNLOAD_RATE, session_factory=_session
    )
    try:
        # Phase 1: inventory per archive code (one Playwright session per code).
        # Where the inventory is already cached the filters are resolved against it
        # first, so a code that provably holds nothing we want is never discovered.
        filtering = invnrs is not None or kantoor_filter is not None
        inventories: dict[str, list[dict]] = {}
        for code in ARCHIVE_CODES:
            items = _cached_inventory(code) if filtering else None
            if items is None:
                print(f"\n  {code}: discovering digitized inventarisnummers …")
                items = _harvest_inventory(code)
            inventories[code] = [it for it in items if _keep_item(it, code, invnrs, kantoor_filter)]

        if filtering:
            total = sum(len(v) for v in inventories.values())
            print(f"\nFiltered to {total} items matching {filters.describe(invnrs, kantoren)}.")
            if not total:
                print(
                    f"\nWARNING: {filters.describe(invnrs, kantoren)} matched no "
                    f"inventarisnummer in either of the {len(ARCHIVE_CODES)} archive codes."
                )

        # --list-invnrs (print table and exit, no Playwright or downloads)
        if list_invnrs:
            csv_rows: list[dict] = []
            for code, items in inventories.items():
                # Page counts come from the per-invnr token caches for free.
                # Harvesting the missing ones means driving Playwright over every
                # register, so that only happens under --count-scans.
                if count_scans and items:
                    _harvest_all_tokens(code, items)

                axis = ARCHIVE_CODES[code]["axis"]
                print(f"\n{code} ({ARCHIVE_CODES[code]['title']}):")
                print(f"  {'invnr':>6}  {axis:<20}  {'datering':<12}  {'pages':>6}  title")
                print(
                    f"  {'------':>6}  {'-' * min(len(axis), 20):<20}  {'------------':<12}"
                    f"  {'------':>6}  -----"
                )
                for it in items:
                    pages_here = _cached_page_count(code, it["invnr"])
                    if only_digitized and not listing.has_scans(pages_here):
                        continue
                    print(
                        f"  {it['invnr']:>6}  {it['name']:<20}"
                        f"  {it['datering']:<12}  {listing.fmt_count(pages_here):>6}"
                        f"  {it['title'][:60]}"
                    )
                    # The datering is the archive's own, so the invnr never
                    # needs stripping; the title is the fallback for the few
                    # items whose title carries no trailing ", 1818-1828".
                    year_from, year_to = listing.parse_years(it.get("datering", ""))
                    if year_from is None:
                        year_from, year_to = listing.parse_years(it.get("title", ""), it["invnr"])
                    csv_rows.append(
                        {
                            "code": code,
                            "invnr": it["invnr"],
                            "place_or_kantoor": it.get("name", ""),
                            "datering": it.get("datering", ""),
                            "pages": listing.fmt_count(pages_here),
                            "title": it.get("title", ""),
                            **listing.year_row(year_from, year_to),
                        }
                    )
            print()
            if csv_out and csv_rows:
                with open(csv_out, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=LIST_FIELDS)
                    writer.writeheader()
                    writer.writerows(csv_rows)
                print(f"Wrote {len(csv_rows)} rows to {csv_out}\n")
            return

        # Phase 2 + 3: token harvest per digitized invnr, then download.
        # One Playwright session shared across all invnrs of a given code –
        # individual page navigations reuse the same browser context.
        summary = RunSummary(ARCHIVE, "Limburg", unit_name="archive codes")

        for code, items in inventories.items():
            if not items:
                print(f"\n  {code}: no digitized items, skipping.")
                continue

            _harvest_all_tokens(code, items)

            # Download phase. The token caches already hold every page of every
            # register, so the size of what follows is known before it starts.
            print(f"\n  {code}: downloading scans …")
            n_pages = sum(
                len(_load_json(_tokens_cache_path(code, it["invnr"])) or []) for it in items
            )
            announce(n_pages, len(items), code)
            summary.units += 1

            code_tally = PageTally()
            for it in items:
                tokens = _load_json(_tokens_cache_path(code, it["invnr"])) or []
                if only_digitized and not tokens:
                    print(f"    invnr {it['invnr']}: 0 pages{listing.NOTHING_TO_DOWNLOAD}")
                    continue
                summary.registers += 1
                dest_dir = output_dir / code / str(it["invnr"])
                _write_metadata(dest_dir, code, it, len(tokens))
                jobs = [
                    download.Job(
                        _image_url(code, tok),
                        dest_dir / _page_filename(code, tok),
                    )
                    for tok in tokens
                ]
                tally = PageTally()

                def record_page(job: download.Job, status: str) -> None:
                    tally.record(status, job.dest)
                    summary.pages.record(status, job.dest)

                downloader.run(jobs, on_result=record_page)
                print(
                    f"    invnr {it['invnr']} ({it['name']}) {tally.describe(len(tokens))}",
                    flush=True,
                )
                # Fold in per register, not per code, so a run that dies midway
                # still reports everything it downloaded.
                code_tally += tally
            print(f"    {code}: {code_tally.describe()}")

        summary.report()
        return summary
    finally:
        downloader.close()


if __name__ == "__main__":
    main()
