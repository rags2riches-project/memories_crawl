"""Drents Archief – Memories van Successie downloader.

Uses the Memorix genealogy REST API:
  - Person search: GET /genealogy/person?q=*:*&fq=search_s_deed_type_title:"Successiememories"
  - Deed detail:   GET /genealogy/deed/{deed_id}  → asset[].download for full-size JPEGs

One deed corresponds to one succession register entry; multiple persons can share a deed.
We collect unique deed_ids from the person search, then download per deed.
"""
from __future__ import annotations

import csv
import json
import time
from pathlib import Path

import requests

API_BASE = "https://webservices.memorix.nl/genealogy"
API_KEY = "a85387a2-fdb2-44d0-8209-3635e59c537e"
DEED_TYPE_FILTER = 'search_s_deed_type_title:"Successiememories"'
PAGE_SIZE = 100
OUTPUT_DIR = Path("scans/drentsarchief")
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Drents Archief"
ARCHIVE_NUMBER = "0119.03"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Content-Type"] = "application/json"
    return s


def _search_page(session: requests.Session, page: int) -> dict:
    resp = session.get(
        f"{API_BASE}/person",
        params={
            "q": "*:*",
            "rows": PAGE_SIZE,
            "page": page,
            "fq": DEED_TYPE_FILTER,
            "apiKey": API_KEY,
            "lang": "nl",
        },
        timeout=60,
    )
    if resp.status_code == 429:
        time.sleep(10)
        resp = session.get(resp.request.url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _collect_deed_ids(session: requests.Session) -> dict[str, dict]:
    """Return mapping deed_id → person metadata (first person encountered for that deed)."""
    deeds: dict[str, dict] = {}
    page = 1
    total_pages = None
    while True:
        data = _search_page(session, page)
        results = data.get("person", [])
        for person in results:
            deed_id = person.get("deed_id") or person.get("register_id")
            if deed_id and deed_id not in deeds:
                deeds[deed_id] = person
        if total_pages is None:
            total_pages = data.get("metadata", {}).get("pagination", {}).get("pages")
        if total_pages and page >= int(total_pages):
            break
        if not results:
            break
        page += 1
        time.sleep(0.3)
    return deeds


def _fetch_deed(session: requests.Session, deed_id: str) -> dict:
    resp = session.get(
        f"{API_BASE}/deed/{deed_id}",
        params={"apiKey": API_KEY, "lang": "nl"},
        timeout=60,
    )
    if resp.status_code == 404:
        return {}
    if resp.status_code == 429:
        time.sleep(10)
        resp = session.get(resp.request.url, timeout=60)
    resp.raise_for_status()
    return resp.json()


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


def _write_metadata(dest_dir: Path, deed_data: dict, person_data: dict) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": ARCHIVE_NUMBER,
        "brontype": "Memorie van Successie",
        "gemeente": person_data.get("register_gemeente") or person_data.get("plaats") or "",
        "inventarisnummer": (
            deed_data.get("register", {}).get("inventarisnummer")
            or deed_data.get("inventarisnummer")
            or person_data.get("register_naam", "")
        ),
        "naam_overledene": (
            f"{person_data.get('voornaam', '')} {person_data.get('geslachtsnaam', '')}".strip()
        ),
        "overlijdensdatum": person_data.get("datum_overlijden") or person_data.get("datum") or "",
        "overlijdensplaats": person_data.get("plaats") or "",
        "deed_id": deed_data.get("id") or deed_data.get("uuid") or "",
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def main() -> None:
    session = _session()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Collecting deed IDs from person search …")
    deeds = _collect_deed_ids(session)
    print(f"Found {len(deeds)} unique deeds.")

    # Write a progress CSV so the run can be resumed
    progress_csv = Path("drentsarchief_deeds.csv")
    done: set[str] = set()
    if progress_csv.exists():
        with open(progress_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "done":
                    done.add(row["deed_id"])

    with open(progress_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["deed_id", "status", "n_scans"])
        if progress_csv.stat().st_size == 0:
            writer.writeheader()

        for deed_id, person_data in deeds.items():
            if deed_id in done:
                continue
            deed_data = _fetch_deed(session, deed_id)
            assets = deed_data.get("asset") or []
            if not assets:
                writer.writerow({"deed_id": deed_id, "status": "no_assets", "n_scans": 0})
                time.sleep(0.2)
                continue

            dest_dir = OUTPUT_DIR / deed_id
            _write_metadata(dest_dir, deed_data, person_data)

            for idx, asset in enumerate(assets, start=1):
                download_url = asset.get("download") or asset.get("thumb.large") or ""
                if not download_url:
                    continue
                ext = ".jpg"
                dest = dest_dir / f"{idx:04d}{ext}"
                _download_file(session, download_url, dest)

            writer.writerow({"deed_id": deed_id, "status": "done", "n_scans": len(assets)})
            time.sleep(0.3)

    print("Done.")


if __name__ == "__main__":
    main()
