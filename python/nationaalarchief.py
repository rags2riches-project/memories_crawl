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

import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

ACCESS_NUMBER = "3.06.05"
EAD_XML_URL = "https://www.nationaalarchief.nl/onderzoeken/archief/3.06.05/download/xml"
# Base URL pattern for inventory viewer pages
VIEWER_URL_TPL = (
    "https://www.nationaalarchief.nl/onderzoeken/archief/3.06.05/invnr/@{invnr}"
    "/file/NL-HaNA_3.06.05_{invnr}_0000"
)
OUTPUT_DIR = Path("scans/nationaalarchief")
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Nationaal Archief"

# Complete fallback list of Memories van Successie invnrs (section 2.4),
# excluding Tafel V-bis and Tafel VI, derived from the EAD XML (July 2022 edition).
# Covers all 21 kantoren; gaps in the sequence are the excluded Tafel items.
_FALLBACK_INVNR_RANGES: list[tuple[int, int]] = [
    (2276, 2450),   # Kantoor Alphen aan de Rijn (2.4.01)
    (2469, 2534),   # Kantoor Brielle (2.4.02)
    (2551, 2752),   # Kantoor Delft (2.4.03)
    (2798, 2964),   # Kantoor Dordrecht (2.4.04)
    (3005, 3180),   # Kantoor Gorinchem (2.4.05)
    (3201, 3412),   # Kantoor Gouda (2.4.06)
    (3468, 3889),   # Kantoor 's-Gravenhage (2.4.07) part 1 (excl. Tafel V-bis 3897-3943)
    (3944, 3946),   # Kantoor 's-Gravenhage part 2 (excl. Tafel VI 3947-4028)
    (4029, 4238),   # Kantoor Leiden (2.4.09)
    (4265, 4431),   # Kantoor Noordwijk (2.4.10)
    (4444, 4560),   # Kantoor Oud-Beijerland (2.4.11)
    (4585, 4723),   # Kantoor Rotterdam (2.4.12) part 1 (excl. Tafel 6 / V-bis 4724-…)
    (4962, 5088),   # Kantoor Schiedam (2.4.13)
    (5124, 5292),   # Kantoor Schoonhoven (2.4.14) + Sliedrecht/Papendrecht (2.4.15)
    (5310, 5414),   # Kantoor Sommelsdijk/Middelharnis/Dirksland (2.4.16)
    (5501, 5508),   # Kantoor Vlaardingen (2.4.18)
    (5611, 5740),   # Kantoor Woubrugge (2.4.20) + IJsselmonde (2.4.21) part 1
    (5744, 5810),   # Kantoor IJsselmonde part 2 (excl. Tafel V-bis 5741-5743)
    (5815, 5816),   # Kantoor IJsselmonde part 3
    (5819, 7021),   # Kantoor Rotterdam (2.4.12) part 2 + Hillegersberg (2.4.08) + others
    (7106, 7139),   # Various kantoren (Tafel V-bis tails excluded)
    (7212, 7268),   # Various kantoren (tail)
]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get_children(elem: ET.Element) -> list[ET.Element]:
    """Return child component elements (c, c01-c06, …)."""
    return [
        c for c in elem
        if c.tag == 'c' or (c.tag.startswith('c') and len(c.tag) <= 3 and c.tag[1:].isdigit())
    ]


def _get_unitid(elem: ET.Element) -> str:
    e = elem.find('did/unitid')
    return e.text.strip() if e is not None and e.text else ''


def _get_unittitle(elem: ET.Element) -> str:
    e = elem.find('did/unittitle')
    return e.text.strip() if e is not None and e.text else ''


def _collect_leaf_invnrs(elem: ET.Element) -> list[int]:
    """Recursively collect all leaf-level purely-numeric unitid values."""
    children = _get_children(elem)
    if not children:
        uid = _get_unitid(elem)
        if uid.isdigit():
            return [int(uid)]
        return []
    results: list[int] = []
    for child in children:
        results.extend(_collect_leaf_invnrs(child))
    return results


def _parse_ead_invnrs(xml_bytes: bytes) -> list[int]:
    """Parse the EAD XML and return sorted Memories invnrs from section 2.4.

    Excludes Tafel V-bis and Tafel VI subsections.
    """
    root = ET.fromstring(xml_bytes)
    dsc = root.find('.//dsc')
    if dsc is None:
        return []

    top_level = _get_children(dsc)
    section2 = next((s for s in top_level if _get_unitid(s) == '2'), None)
    if section2 is None:
        return []

    section24 = next(
        (s for s in _get_children(section2) if _get_unitid(s) == '2.4'), None
    )
    if section24 is None:
        return []

    all_invnrs: list[int] = []

    for kantoor in _get_children(section24):
        subsections = _get_children(kantoor)
        if not subsections:
            all_invnrs.extend(_collect_leaf_invnrs(kantoor))
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
                        all_invnrs.extend(_collect_leaf_invnrs(subsub))
            else:
                all_invnrs.extend(_collect_leaf_invnrs(subsec))

    return sorted(set(all_invnrs))


def _is_excluded_subsection(title_lower: str) -> bool:
    """Return True for Tafel V-bis and Tafel VI subsections."""
    return (
        'tafel v-bis' in title_lower
        or 'tafels v-bis' in title_lower
        or 'v-bis' in title_lower
        or 'tafel vi' in title_lower
        or 'tafels vi' in title_lower
        or 'tafel 6' in title_lower
    )


def _fallback_invnrs() -> list[int]:
    """Return the hardcoded fallback list of Memories invnrs."""
    result: list[int] = []
    for lo, hi in _FALLBACK_INVNR_RANGES:
        result.extend(range(lo, hi + 1))
    return sorted(set(result))


def _fetch_inventory_numbers(session: requests.Session) -> list[int]:
    """Fetch the EAD XML and parse section 2.4 Memories invnrs.

    Falls back to the hardcoded list if the download or parse fails.
    """
    try:
        resp = session.get(EAD_XML_URL, timeout=120)
        resp.raise_for_status()
        invnrs = _parse_ead_invnrs(resp.content)
        if invnrs:
            return invnrs
    except Exception as exc:
        print(f"  Warning: EAD XML fetch/parse failed ({exc}); using fallback list.")
    return _fallback_invnrs()


def _extract_scans_from_viewer(html: str) -> list[dict]:
    """Extract the scans array from the drupal-settings-json script tag."""
    match = re.search(
        r'<script[^>]+data-drupal-selector="drupal-settings-json"[^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        return []
    try:
        settings = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    # viewer.response can be a JSON string or already a dict
    viewer = settings.get("viewer", {})
    response = viewer.get("response", {})
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            return []
    scans = response.get("scans") or response.get("files") or response.get("pages") or []
    return scans if isinstance(scans, list) else []


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return "exists"
    resp = session.get(url, stream=True, timeout=120)
    if resp.status_code == 404:
        return "missing"
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(65536):
            if chunk:
                f.write(chunk)
    return "downloaded"


def _write_metadata(dest_dir: Path, invnr: int, html: str, scans: list[dict]) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    period_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html)
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


def main() -> None:
    session = _session()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching inventory numbers from EAD XML …")
    inv_numbers = _fetch_inventory_numbers(session)
    print(f"Found {len(inv_numbers)} inventory items: {inv_numbers[0]}–{inv_numbers[-1]}")

    done_file = Path("nationaalarchief_done.txt")
    done: set[str] = set()
    if done_file.exists():
        done = set(done_file.read_text().splitlines())

    for invnr in inv_numbers:
        key = str(invnr)
        if key in done:
            continue

        dest_dir = OUTPUT_DIR / key
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
            print("no scans found")
            with open(done_file, "a") as f:
                f.write(key + "\n")
            time.sleep(0.5)
            continue

        _write_metadata(dest_dir, invnr, html, scans)

        for scan in scans:
            label = scan.get("label") or f"{invnr}_{scan.get('order', 0):04d}.jpg"
            default = scan.get("default") or {}
            download_url = default.get("url") or ""
            if not download_url:
                scan_id = scan.get("id") or scan.get("uuid") or ""
                if scan_id:
                    download_url = (
                        f"https://service.archief.nl/api/file/v1/default/{scan_id}"
                    )
            if not download_url:
                continue
            dest = dest_dir / label
            _download_file(session, download_url, dest)

        print(f"{len(scans)} scans")
        with open(done_file, "a") as f:
            f.write(key + "\n")
        time.sleep(1.0)

    print("Done.")


if __name__ == "__main__":
    main()
