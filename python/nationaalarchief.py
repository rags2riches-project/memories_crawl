"""Nationaal Archief – Memories van Successie van Zuid-Holland (access 3.06.05).

Inventory items 2276–2357 are the Memories van Successie section (inventory section 2.4).
Tafel V-bis is under a different section and is excluded by only processing 2276–2357.

For each inventory number:
  1. Fetch the viewer page to extract embedded JSON (drupal-settings-json script tag).
  2. Parse the scans array from viewer.response.
  3. Download each scan via default.url → https://service.archief.nl/api/file/v1/default/{UUID}
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests

ACCESS_NUMBER = "3.06.05"
INVENTORY_LISTING_URL = (
    "https://www.nationaalarchief.nl/onderzoeken/archief/3.06.05/invnr/2.4"
)
# Base URL pattern for inventory viewer pages
VIEWER_URL_TPL = (
    "https://www.nationaalarchief.nl/onderzoeken/archief/3.06.05/invnr/@{invnr}"
    "/file/NL-HaNA_3.06.05_{invnr}_0000"
)
OUTPUT_DIR = Path("scans/nationaalarchief")
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Nationaal Archief"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _extract_inventory_numbers(html: str) -> list[int]:
    """Parse inventory numbers from the inventory listing page HTML."""
    # Links of the form /invnr/@NNNN or unitid attributes
    numbers = sorted(
        {int(m) for m in re.findall(r"/invnr/@(\d+)", html)}
    )
    return numbers


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


def _fetch_inventory_numbers(session: requests.Session) -> list[int]:
    resp = session.get(INVENTORY_LISTING_URL, timeout=60)
    resp.raise_for_status()
    numbers = _extract_inventory_numbers(resp.text)
    if not numbers:
        # Fallback: known range from research
        numbers = list(range(2276, 2358))
    return numbers


def _fetch_scans_for_invnr(session: requests.Session, invnr: int) -> list[dict]:
    url = VIEWER_URL_TPL.format(invnr=invnr)
    resp = session.get(url, timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return _extract_scans_from_viewer(resp.text)


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
    # Try to extract date range from page title or heading
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

    print("Fetching inventory listing …")
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
            # scan["label"] is like "NL-HaNA_3.06.05_2276_0000.jpg"
            label = scan.get("label") or f"{invnr}_{scan.get('order', 0):04d}.jpg"
            default = scan.get("default") or {}
            download_url = default.get("url") or ""
            if not download_url:
                # Fallback: construct from scan id
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
