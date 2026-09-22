"""Nationaal Archief – Memories van Successie van Zuid-Holland (access 3.06.05).

Section 2.4 of the inventory (Ontvangers van de successierechten, 1818-1927)
contains 21 kantoren whose individual invnrs range from 2276 to 7268.

Tafel V-bis (alphabetical death registers) and Tafel VI (testament registers)
are interleaved in the same numeric range but occupy different subsections in
the EAD inventory; they are excluded during invnr discovery.

For each inventory number:
  1. Fetch the viewer page to extract embedded JSON (drupal-settings-json script tag).
  2. Parse the scans array from viewer.response.
  3. Download each scan via default.url → https://service.archief.nl/api/file/v1/default/{UUID}
"""

from __future__ import annotations

import csv
import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

from memories_crawl import download, filters, listing, paths, regcache
from memories_crawl.summary import PageTally, RunSummary

ACCESS_NUMBER = "3.06.05"
EAD_XML_URL = "https://www.nationaalarchief.nl/onderzoeken/archief/3.06.05/download/xml"
INVENTORY_CACHE_NAME = "inventory.json"
# Base URL pattern for inventory viewer pages
VIEWER_URL_TPL = (
    "https://www.nationaalarchief.nl/onderzoeken/archief/3.06.05/invnr/@{invnr}"
    "/file/NL-HaNA_3.06.05_{invnr}_0000"
)
ARCHIVE = "nationaalarchief"
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Nationaal Archief"

# Complete fallback list of Memories van Successie invnrs (section 2.4),
# excluding Tafel V-bis and Tafel VI, derived from the EAD XML (July 2022 edition).
# Covers all 21 kantoren; gaps in the sequence are the excluded Tafel items.
_FALLBACK_INVNR_RANGES: list[tuple[int, int, str]] = [
    (2276, 2450, "Kantoor Alphen aan de Rijn"),  # 2.4.01
    (2469, 2534, "Kantoor Brielle"),  # 2.4.02
    (2551, 2752, "Kantoor Delft"),  # 2.4.03
    (2798, 2964, "Kantoor Dordrecht"),  # 2.4.04
    (3005, 3180, "Kantoor Gorinchem"),  # 2.4.05
    (3201, 3412, "Kantoor Gouda"),  # 2.4.06
    # 2.4.07 part 1 (excl. Tafel V-bis 3897-3943)
    (3468, 3889, "Kantoor 's-Gravenhage"),
    (3944, 3946, "Kantoor 's-Gravenhage"),  # part 2 (excl. Tafel VI 3947-4028)
    (4029, 4238, "Kantoor Leiden"),  # 2.4.09
    (4265, 4431, "Kantoor Noordwijk"),  # 2.4.10
    (4444, 4560, "Kantoor Oud-Beijerland"),  # 2.4.11
    (4585, 4723, "Kantoor Rotterdam"),  # 2.4.12 part 1 (excl. Tafel 6 / V-bis 4724-…)
    (4962, 5088, "Kantoor Schiedam"),  # 2.4.13
    # 2.4.14 + Sliedrecht/Papendrecht (2.4.15)
    (5124, 5292, "Kantoor Schoonhoven"),
    (5310, 5414, "Kantoor Sommelsdijk/Middelharnis/Dirksland"),  # 2.4.16
    (5501, 5508, "Kantoor Vlaardingen"),  # 2.4.18
    (5611, 5740, "Kantoor Woubrugge"),  # 2.4.20 + IJsselmonde (2.4.21) part 1
    (5744, 5810, "Kantoor IJsselmonde"),  # part 2 (excl. Tafel V-bis 5741-5743)
    (5815, 5816, "Kantoor IJsselmonde"),  # part 3
    # 2.4.12 part 2 + Hillegersberg (2.4.08) + others
    (5819, 7021, "Kantoor Rotterdam e.a."),
    (7106, 7139, "diverse kantoren"),  # Tafel V-bis tails excluded
    (7212, 7268, "diverse kantoren"),  # tail
]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get_children(elem: ET.Element) -> list[ET.Element]:
    """Return child component elements (c, c01-c06, …)."""
    return [
        c
        for c in elem
        if c.tag == "c" or (c.tag.startswith("c") and len(c.tag) <= 3 and c.tag[1:].isdigit())
    ]


def _get_unitid(elem: ET.Element) -> str:
    e = elem.find("did/unitid")
    return e.text.strip() if e is not None and e.text else ""


def _get_unittitle(elem: ET.Element) -> str:
    e = elem.find("did/unittitle")
    return e.text.strip() if e is not None and e.text else ""


def _leaf_description(elem: ET.Element) -> str:
    """The item's own datering, e.g. ``"1818 jan. - mrt."``.

    The text lives either directly in ``unittitle`` ("1849, memories van
    aangifte, jan. - juni") or inside a nested ``unitdate``, so the whole
    subtree is flattened rather than just ``unittitle.text``.
    """
    title = elem.find("did/unittitle")
    if title is None:
        return ""
    return " ".join("".join(title.itertext()).split())


def _leaf_years(elem: ET.Element) -> tuple[int | None, int | None]:
    """Period of one inventory item, from the EAD we already download.

    ``<unitdate normal="1818-01/1818-03">`` is machine-readable but present on
    only about half the items; the rest carry the years in the title text
    ("1849, memories van aangifte, jan. - juni"), so that is read as a
    fallback. Together they date 3,955 of the 3,958 items in section 2.4, and
    neither costs a request -- the date is free where issue #38 assumed one
    viewer fetch per invnr. The three that stay unknown are typos in the
    archive's own text ("l872 okt. - dec."), reported as ``?``.
    """
    normals = [
        part
        for unitdate in elem.findall("did/unittitle/unitdate") + elem.findall("did/unitdate")
        for part in (unitdate.get("normal") or "").split("/")
    ]
    year_from, year_to = listing.span(normals)
    if year_from is not None:
        return year_from, year_to
    return listing.parse_years(_leaf_description(elem))


def _collect_leaf_invnrs(elem: ET.Element) -> list[dict]:
    """Recursively collect leaf-level purely-numeric unitids as inventory rows.

    Each row is ``{invnr, has_scans, description, year_from, year_to}``.
    ``has_scans`` comes from the ``<dao>`` METS link the EAD attaches to every
    digitized item, so whether an inventarisnummer has scans at all is known
    from the one XML download the pipeline already makes -- no viewer page
    needed.  The same is true of the period (issue #38).
    """
    children = _get_children(elem)
    if not children:
        uid = _get_unitid(elem)
        if uid.isdigit():
            year_from, year_to = _leaf_years(elem)
            return [
                {
                    "invnr": int(uid),
                    "has_scans": bool(elem.findall("did/dao")),
                    "description": _leaf_description(elem),
                    "year_from": year_from,
                    "year_to": year_to,
                }
            ]
        return []
    results: list[dict] = []
    for child in children:
        results.extend(_collect_leaf_invnrs(child))
    return results


def _parse_ead_invnrs(xml_bytes: bytes) -> list[int]:
    """Compatibility projection of the richer EAD entries."""
    return [e["invnr"] for e in _parse_ead_entries(xml_bytes)]


def _is_excluded_subsection(title_lower: str) -> bool:
    """Return True for Tafel V-bis and Tafel VI subsections."""
    return (
        "tafel v-bis" in title_lower
        or "tafels v-bis" in title_lower
        or "v-bis" in title_lower
        or "tafel vi" in title_lower
        or "tafels vi" in title_lower
        or "tafel 6" in title_lower
    )


def _fallback_invnrs() -> list[int]:
    return [e["invnr"] for e in _fallback_entries()]


def _collect_inventory_numbers(session: requests.Session) -> list[int]:
    """Download the EAD XML and parse section 2.4 Memories invnrs."""
    resp = session.get(EAD_XML_URL, timeout=120)
    resp.raise_for_status()
    invnrs = _parse_ead_invnrs(resp.content)
    if not invnrs:
        raise ValueError("no section 2.4 inventory numbers in the EAD XML")
    return invnrs


def _fetch_inventory_numbers(session: requests.Session, refresh_cache: bool = False) -> list[int]:
    """Return the section 2.4 Memories invnrs, downloading the EAD XML once.

    The parsed listing is cached for :data:`regcache.TTL_SECONDS` so repeated
    invocations do not re-download the inventory (issue #25).  A failed fetch
    or parse falls back to the hardcoded list, which is deliberately *not*
    cached: a transient outage must not pin the fallback in place for a month.
    """
    try:
        return regcache.load_or_collect(
            ARCHIVE,
            INVENTORY_CACHE_NAME,
            lambda: _collect_inventory_numbers(session),
            key=EAD_XML_URL,
            refresh=refresh_cache,
        )
    except Exception as exc:
        print(f"  Warning: EAD XML fetch/parse failed ({exc}); using fallback list.")
    return _fallback_invnrs()


def _extract_scans_from_viewer(html: str, *, strict: bool = False) -> list[dict]:
    """Extract scan arrays; strict counting distinguishes invalid pages from zero."""
    try:
        match = re.search(
            r'<script[^>]+data-drupal-selector="drupal-settings-json"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not match:
            raise ValueError("missing viewer settings")
        settings = json.loads(match.group(1))
        response = settings["viewer"]["response"]
        if isinstance(response, str):
            response = json.loads(response)
        for key in ("scans", "files", "pages"):
            scans = response.get(key)
            if isinstance(scans, list):
                return scans
        raise ValueError("missing scan array")
    except (ValueError, KeyError, TypeError, AttributeError):
        if strict:
            raise ValueError("invalid viewer scan data") from None
        return []


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    """Fetch one scan; see :func:`download.fetch_file` for the retry rules."""
    return download.fetch_file(
        session,
        url,
        dest,
        missing_statuses=(404,),
        timeout=120,
    )


def _write_metadata(dest_dir: Path, invnr: int, html: str, scans: list[dict]) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    period_match = re.search(r"<h1[^>]*>([^<]+)</h1>", html)
    period = period_match.group(1).strip() if period_match else ""
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": ACCESS_NUMBER,
        "brontype": "Memorie van Successie",
        "inventarisnummer": str(invnr),
        "periode": period,
        "n_scans": len(scans),
        "url_origineel": VIEWER_URL_TPL.format(invnr=invnr),
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _as_year(cell: str | int | None) -> int | None:
    """Read a ``year_from`` / ``year_to`` cell back, treating ``?`` as unknown."""
    if isinstance(cell, int):
        return cell
    return int(cell) if cell and str(cell).isdigit() else None


def _list_inventory(
    entries: list[dict],
    csv_out: str | None = None,
    counts: dict[int, int | None] | None = None,
    only_digitized: bool = False,
) -> None:
    """Print one row per inventory number with its kantoor and scan count."""
    rows: list[dict] = []
    for entry in entries:
        n_scans = _entry_n_scans(entry, counts)
        if only_digitized and not listing.has_scans(n_scans):
            continue
        rows.append(
            {
                "invnr": entry["invnr"],
                "kantoor": entry.get("kantoor") or "",
                "n_scans": listing.fmt_count(n_scans),
                "description": entry.get("description") or "",
                **listing.year_row(entry.get("year_from"), entry.get("year_to")),
            }
        )

    if not rows:
        print("  (none)")
        return

    print(f"\n{len(rows)} inventory numbers:\n")
    print(f"  {'invnr':>6}  {'scans':>6}  {'period':<11}  kantoor")
    print(f"  {'------':>6}  {'------':>6}  {'-' * 11:<11}  -------")
    for row in rows:
        period = listing.fmt_period(_as_year(row["year_from"]), _as_year(row["year_to"]))
        print(f"  {row['invnr']:>6}  {row['n_scans']:>6}  {period:<11}  {row['kantoor']}")
    print()

    if csv_out:
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LIST_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to {csv_out}\n")


def _parse_ead_entries(xml_bytes: bytes) -> list[dict]:
    """Parse the EAD XML into sorted ``{invnr, kantoor, has_scans}`` rows.

    Covers section 2.4 only, excluding Tafel V-bis and Tafel VI subsections.
    """
    root = ET.fromstring(xml_bytes)
    dsc = root.find(".//dsc")
    if dsc is None:
        return []

    top_level = _get_children(dsc)
    section2 = next((s for s in top_level if _get_unitid(s) == "2"), None)
    if section2 is None:
        return []

    section24 = next((s for s in _get_children(section2) if _get_unitid(s) == "2.4"), None)
    if section24 is None:
        return []

    by_invnr: dict[int, dict] = {}

    def absorb(rows: list[dict], kantoor: str) -> None:
        for row in rows:
            by_invnr.setdefault(row["invnr"], {**row, "kantoor": kantoor})

    for kantoor_elem in _get_children(section24):
        kantoor = _get_unittitle(kantoor_elem) or _get_unitid(kantoor_elem)
        subsections = _get_children(kantoor_elem)
        if not subsections:
            absorb(_collect_leaf_invnrs(kantoor_elem), kantoor)
            continue

        for subsec in subsections:
            sub_title = _get_unittitle(subsec).lower()
            if _is_excluded_subsection(sub_title):
                continue

            sub_children = _get_children(subsec)
            if sub_children:
                for subsub in sub_children:
                    subsub_title = _get_unittitle(subsub).lower()
                    if not _is_excluded_subsection(subsub_title):
                        absorb(_collect_leaf_invnrs(subsub), kantoor)
            else:
                absorb(_collect_leaf_invnrs(subsec), kantoor)

    return [by_invnr[n] for n in sorted(by_invnr)]


def _fallback_entries() -> list[dict]:
    """Return the hardcoded fallback inventory rows.

    The ranges carry a kantoor but neither a digitization marker nor a
    datering, so ``has_scans`` and the years are ``None`` (unknown) rather than
    a guess either way.
    """
    by_invnr: dict[int, dict] = {}
    for lo, hi, kantoor in _FALLBACK_INVNR_RANGES:
        for n in range(lo, hi + 1):
            by_invnr.setdefault(
                n,
                {
                    "invnr": n,
                    "kantoor": kantoor,
                    "has_scans": None,
                    "description": "",
                    "year_from": None,
                    "year_to": None,
                },
            )
    return [by_invnr[n] for n in sorted(by_invnr)]


def _collect_inventory_entries(session: requests.Session) -> list[dict]:
    resp = session.get(EAD_XML_URL, timeout=120)
    resp.raise_for_status()
    entries = _parse_ead_entries(resp.content)
    if not entries:
        raise ValueError("no section 2.4 inventory entries in the EAD XML")
    return entries


def _fetch_inventory_entries(session: requests.Session, refresh_cache: bool = False) -> list[dict]:
    """Cache full EAD entries with a key distinct from legacy integer lists.

    The key is versioned: ``#entries-v2`` rows carry the item's period, so a
    ``v1`` cache written before issue #38 is re-collected rather than served
    without dates.  Failed collection leaves the cache untouched; fallback rows
    are never cached.
    """
    try:
        return regcache.load_or_collect(
            ARCHIVE,
            INVENTORY_CACHE_NAME,
            lambda: _collect_inventory_entries(session),
            key=EAD_XML_URL + "#entries-v2",
            refresh=refresh_cache,
        )
    except Exception as exc:
        print(f"  Warning: EAD XML fetch/parse failed ({exc}); using fallback list.")
    return _fallback_entries()


def _count_scans(session: requests.Session, invnr: int) -> int | None:
    """Exact page count for one invnr, at the cost of one viewer page fetch.

    Only reached under ``--count-scans``: the EAD says *whether* an item is
    digitized but not how many pages it has, and the page count is embedded in
    the (large) viewer HTML.
    """
    try:
        resp = session.get(VIEWER_URL_TPL.format(invnr=invnr), timeout=60)
        if resp.status_code == 404:
            return 0
        resp.raise_for_status()
    except requests.RequestException:
        return None
    try:
        return len(_extract_scans_from_viewer(resp.text, strict=True))
    except ValueError:
        return None


LIST_FIELDS = ["invnr", "kantoor", "n_scans", "description", *listing.YEAR_FIELDS]


def _entry_n_scans(entry: dict, counts: dict[int, int | None] | None) -> int | None:
    """Scan count for one inventory row.

    Without ``--count-scans`` only the EAD's digitized marker is available, so
    an undigitized item is an exact ``0`` and a digitized one an honest ``?``:
    the page count lives in the viewer page, one HTTP fetch per invnr.
    """
    if counts is not None:
        return counts.get(entry["invnr"])
    has_scans = entry.get("has_scans")
    return 0 if has_scans is False else None


def main(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    refresh_cache: bool = False,
) -> RunSummary | None:
    if out_dir is not None:
        paths.set_out_dir(out_dir)
    output_dir = paths.archive_dir(ARCHIVE)

    session = _session()
    downloader = download.Downloader(_download_file, workers=workers, session_factory=_session)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)

        print("Fetching inventory numbers from EAD XML …")
        entries = _fetch_inventory_entries(session, refresh_cache=refresh_cache)
        print(
            f"Found {len(entries)} inventory items: "
            f"{entries[0]['invnr']}–{entries[-1]['invnr']} "
            f"({sum(1 for e in entries if e.get('has_scans')) or '?'} digitized)"
        )

        if invnrs is not None:
            entries = [e for e in entries if str(e["invnr"]) in invnrs]
            print(f"Filtered to {len(entries)} inventory numbers matching --invnr.")

        if invnrs is not None and not entries:
            print(
                f"\nWARNING: {filters.describe(invnrs)} matched no "
                "inventarisnummer in access 3.06.05."
            )

        if list_invnrs:
            counts: dict[int, int | None] | None = None
            if count_scans:
                wanted = [e for e in entries if e.get("has_scans") is not False]
                print(f"Fetching viewer pages to count scans for {len(wanted)} items …")
                counts = {e["invnr"]: 0 for e in entries}
                for entry in wanted:
                    counts[entry["invnr"]] = _count_scans(session, entry["invnr"])
                    time.sleep(0.5)
            _list_inventory(entries, csv_out=csv_out, counts=counts, only_digitized=only_digitized)
            return

        if only_digitized:
            # The EAD's <dao> markers make this free: 721 of the 3,958 items have
            # no scans, and each one would otherwise cost a viewer page fetch.
            kept = [e for e in entries if e.get("has_scans") is not False]
            print(f"--only-digitized: {len(kept)} of {len(entries)} inventory numbers have scans.")
            entries = kept

        inv_numbers = [e["invnr"] for e in entries]

        # Access 3.06.05 is a flat run of inventarisnummers: no kantoor layer.
        summary = RunSummary(ARCHIVE, "Zuid-Holland (Nationaal Archief)", unit_name=None)

        done_file = paths.cache_file(
            ARCHIVE, "nationaalarchief_done.txt", legacy=Path("nationaalarchief_done.txt")
        )
        done: set[str] = set()
        if done_file.exists():
            done = set(done_file.read_text().splitlines())

        for invnr in inv_numbers:
            key = str(invnr)
            if key in done:
                continue

            dest_dir = output_dir / key
            print(f"  invnr {invnr} …", end=" ", flush=True)

            url = VIEWER_URL_TPL.format(invnr=invnr)
            resp = session.get(url, timeout=60)
            if resp.status_code == 404:
                print("404 – skipped")
                with open(done_file, "a") as f:
                    f.write(key + "\n")
                time.sleep(0.5)
                continue
            resp.raise_for_status()
            html = resp.text

            scans = _extract_scans_from_viewer(html)
            if not scans:
                print(f"0 scans{listing.NOTHING_TO_DOWNLOAD}")
                with open(done_file, "a") as f:
                    f.write(key + "\n")
                time.sleep(0.5)
                continue

            _write_metadata(dest_dir, invnr, html, scans)
            # The viewer page lists every scan, so the size of this register is
            # known before the first image is fetched.
            print(f"{len(scans)} scans …", end=" ", flush=True)
            summary.registers += 1

            jobs: list[download.Job] = []
            tally = PageTally()
            for scan in scans:
                label = scan.get("label") or f"{invnr}_{scan.get('order', 0):04d}.jpg"
                default = scan.get("default") or {}
                download_url = default.get("url") or ""
                if not download_url:
                    scan_id = scan.get("id") or scan.get("uuid") or ""
                    if scan_id:
                        download_url = f"https://service.archief.nl/api/file/v1/default/{scan_id}"
                if not download_url:
                    continue
                jobs.append(download.Job(download_url, dest_dir / label))

            def record_page(job: download.Job, status: str) -> None:
                tally.record(status, job.dest)
                summary.pages.record(status, job.dest)

            downloader.run(jobs, on_result=record_page)

            print(tally.describe(len(scans)))
            with open(done_file, "a") as f:
                f.write(key + "\n")
            time.sleep(1.0)

        summary.report()
        return summary
    finally:
        downloader.close()


if __name__ == "__main__":
    main()
