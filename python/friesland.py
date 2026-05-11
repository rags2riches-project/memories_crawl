"""Tresoar / AlleFriezen (Friesland) – Memories van Successie downloader.

Uses the Memorix genealogy REST API with Tresoar tenant key.
Deeds embed their own scan page references (asset[] with download URLs),
enabling per-person output directories.

Pipeline
────────
  1. Enumerate all 1,107 MvS registers via
        /register?fq=search_s_type_title:"Memories van successie"
  2. For each register, paginate /deed (assets embedded) and /person.
  3. Join persons → deeds by deed_id.
  4. For each person: download their deed's asset pages, write metadata.json.

Folder layout
─────────────
  scans/friesland/{kantoor}/{invnr}/{person_slug}/
      {NNNN}.jp2               – sequentially numbered scan pages
      metadata.json            – per-person info (name, date of death, …)

JPEG 2000 note
──────────────
  Tresoar serves scans in JPEG 2000 (.jp2) format.
  Convert with ImageMagick if needed:  magick mogrify -format jpg *.jp2

Tafel V-bis
───────────
  Not present at Tresoar (0 results for "tafel" or "v-bis"). No filter needed.
"""
from __future__ import annotations

import csv
import json
import re
import time
from pathlib import Path

import requests

API_BASE = "https://webservices.memorix.nl/genealogy"
API_KEY = "aa030ec4-12d0-4dc0-afaf-b65fd6128b39"
REGISTER_FILTER = 'search_s_type_title:"Memories van successie"'
PAGE_SIZE = 100
OUTPUT_DIR = Path("scans/friesland")
PROGRESS_CSV = Path("friesland_progress.csv")
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Tresoar"
ARCHIVE_NUMBER = "42"
BRONTYPE_LABEL = "Memorie van Successie"

REQUEST_SLEEP = 0.25
RATE_LIMIT_SLEEP = 10


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get_json(session: requests.Session, path: str, params: dict, retries: int = 3) -> dict:
    params = {**params, "apiKey": API_KEY, "lang": "nl"}
    delay = RATE_LIMIT_SLEEP
    for attempt in range(retries):
        resp = session.get(f"{API_BASE}{path}", params=params, timeout=60)
        if resp.status_code == 429 or resp.status_code in (502, 503, 504):
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
                continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}


def _paginate(session: requests.Session, path: str, fq: str, key: str) -> list[dict]:
    """Paginate through a Memorix search endpoint and return all items."""
    items: list[dict] = []
    page = 1
    while True:
        data = _get_json(
            session,
            path,
            {"q": "*:*", "rows": PAGE_SIZE, "page": page, "fq": fq},
        )
        rows = data.get(key, []) or []
        items.extend(rows)
        pagination = data.get("metadata", {}).get("pagination", {})
        total_pages = int(pagination.get("pages") or 0)
        if not rows or page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_SLEEP)
    return items


_SANITIZE_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _sanitize(name: str) -> str:
    cleaned = _SANITIZE_RE.sub("_", (name or "").strip())
    return cleaned.rstrip(". ") or "unknown"


def _kantoor_from_register(register: dict) -> str:
    """Extract a short kantoor name from register metadata."""
    md = register.get("metadata") or {}
    naam = md.get("naam") or ""
    # "Memories kantoor Sneek" → "Sneek"
    # "Dagregister kantoor Gorredijk" → "Gorredijk"
    m = re.match(r"(?:Memories|Dagregister)\s+kantoor\s+(.*)", naam, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return naam or "onbekend"


def _person_slug(person: dict) -> str:
    """Generate a safe directory name from person metadata."""
    pmd = person.get("metadata") or {}
    name = pmd.get("person_display_name") or pmd.get("achternaam") or "onbekend"
    pid = (person.get("id") or "")[:8]
    slug = _sanitize(name)[:40]
    return f"{slug}_{pid}"


def _write_person_metadata(
    dest_dir: Path, person: dict, deed: dict, register: dict, n_scans: int
) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    pmd = person.get("metadata") or {}
    dmd = deed.get("metadata") or {}
    rmd = register.get("metadata") or {}

    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": ARCHIVE_NUMBER,
        "brontype": BRONTYPE_LABEL,
        "kantoor": _kantoor_from_register(register),
        "inventarisnummer": rmd.get("inventarisnummer") or "",
        "aktenummer": dmd.get("nummer") or "",
        "naam_overledene": pmd.get("person_display_name") or "",
        "voornaam": pmd.get("voornaam") or "",
        "tussenvoegsel": pmd.get("tussenvoegsel") or "",
        "geslachtsnaam": pmd.get("geslachtsnaam") or "",
        "patroniem": pmd.get("patroniem") or "",
        "datum_overlijden": pmd.get("datum_overlijden") or pmd.get("datum") or "",
        "plaats_overlijden": pmd.get("plaats") or "",
        "plaats_wonen": pmd.get("plaats_wonen") or "",
        "geslacht": pmd.get("geslacht") or "",
        "diversen": dmd.get("diversen") or "",
        "register_naam": rmd.get("naam") or "",
        "n_scans": n_scans,
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _download_file(session: requests.Session, url: str, dest: Path, retries: int = 3) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return "exists"
    delay = 5
    for attempt in range(retries):
        try:
            resp = session.get(url, stream=True, timeout=180, allow_redirects=True)
        except requests.RequestException as exc:
            if attempt < retries - 1:
                print(f"      network error ({exc}); retry in {delay}s", flush=True)
                time.sleep(delay)
                delay *= 2
                continue
            return "failed"
        if resp.status_code == 404:
            return "missing"
        if resp.status_code in (429, 502, 503, 504) and attempt < retries - 1:
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(65536):
                if chunk:
                    f.write(chunk)
        tmp.rename(dest)
        return "downloaded"
    return "failed"


def _load_done() -> set[str]:
    done: set[str] = set()
    if PROGRESS_CSV.exists():
        with open(PROGRESS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "done":
                    done.add(row["register_id"])
    return done


def main() -> None:
    session = _session()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Collecting Tresoar Memorie van Successie registers …")
    registers = _paginate(session, "/register", REGISTER_FILTER, "register")
    print(f"Found {len(registers)} registers.")

    done = _load_done()
    write_header = not PROGRESS_CSV.exists() or PROGRESS_CSV.stat().st_size == 0
    with open(PROGRESS_CSV, "a", newline="", encoding="utf-8") as progress:
        writer = csv.DictWriter(
            progress,
            fieldnames=["register_id", "kantoor", "invnr", "status", "n_persons"],
        )
        if write_header:
            writer.writeheader()
            progress.flush()

        for idx, reg in enumerate(registers, start=1):
            reg_id = reg.get("id") or ""
            rmd = reg.get("metadata") or {}
            kantoor = _kantoor_from_register(reg)
            invnr = rmd.get("inventarisnummer") or "?"

            if not reg_id or reg_id in done:
                continue

            print(
                f"[{idx}/{len(registers)}] {kantoor} invnr {invnr} …",
                flush=True,
            )

            # Fetch deeds (with embedded asset[]) and persons.
            try:
                deeds = _paginate(session, "/deed", f"register_id:{reg_id}", "deed")
                persons = _paginate(session, "/person", f"register_id:{reg_id}", "person")
            except Exception as exc:
                print(f"      ERROR: fetch failed: {exc}", flush=True)
                writer.writerow({
                    "register_id": reg_id, "kantoor": kantoor, "invnr": invnr,
                    "status": "fetch_failed", "n_persons": 0,
                })
                progress.flush()
                continue

            # Index deeds by id for person→deed join.
            deed_by_id: dict[str, dict] = {d.get("id", ""): d for d in deeds}

            n_persons = 0
            for person in persons:
                deed_id = person.get("deed_id") or ""
                deed = deed_by_id.get(deed_id)
                if not deed:
                    continue

                pmd = person.get("metadata") or {}
                # Only include overledene persons (skip Vermeld etc.)
                if pmd.get("type_title", "").lower() not in ("overledene", ""):
                    continue

                slug = _person_slug(person)
                dest_dir = (
                    OUTPUT_DIR / _sanitize(kantoor) / _sanitize(invnr) / slug
                )

                # Download scan pages from the deed's embedded assets.
                assets = deed.get("asset") or []
                n_done = 0
                for asset_idx, asset in enumerate(assets, start=1):
                    url = asset.get("download") or ""
                    if not url:
                        continue
                    # Determine file extension from URL path.
                    url_path = url.split("?")[0]
                    ext = Path(url_path).suffix or ".jp2"
                    dest = dest_dir / f"{asset_idx:04d}{ext}"
                    status = _download_file(session, url, dest)
                    if status in ("downloaded", "exists"):
                        n_done += 1

                _write_person_metadata(dest_dir, person, deed, reg, n_done)
                n_persons += 1

            writer.writerow({
                "register_id": reg_id, "kantoor": kantoor, "invnr": invnr,
                "status": "done", "n_persons": n_persons,
            })
            progress.flush()
            print(f"      {n_persons} persons", flush=True)
            time.sleep(REQUEST_SLEEP)

    print("Friesland pipeline finished.")


if __name__ == "__main__":
    main()
