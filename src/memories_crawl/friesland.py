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
  <out-dir>/friesland/{kantoor}/{invnr}/{person_slug}/
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

from memories_crawl import listing, paths

API_BASE = "https://webservices.memorix.nl/genealogy"
API_KEY = "aa030ec4-12d0-4dc0-afaf-b65fd6128b39"
REGISTER_FILTER = 'search_s_type_title:"Memories van successie"'
PAGE_SIZE = 100
ARCHIVE = "friesland"
PROGRESS_CSV_NAME = "friesland_progress.csv"
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


def _count(session: requests.Session, path: str, fq: str) -> int:
    """Exact number of hits for a filter, in a single request.

    ``rows=1`` still reports ``pagination.total``, so a count costs one small
    response instead of paging the whole result set.
    """
    data = _get_json(session, path, {"q": "*:*", "rows": 1, "page": 1, "fq": fq})
    pagination = (data.get("metadata") or {}).get("pagination") or {}
    return int(pagination.get("total") or 0)


def _register_is_digitized(register: dict) -> bool:
    """Has Tresoar digitized this register?

    Register search results carry a truncated ``asset`` sample, present exactly
    when the register's deeds have scans (verified against 14 registers, no
    disagreement). It comes with the register listing we already fetch, so the
    "is there anything here at all?" question costs nothing extra.
    """
    return bool(register.get("asset"))


def _register_counts(
    session: requests.Session, register: dict, count_scans: bool
) -> tuple[int | None, int | None]:
    """Return ``(n_persons, n_with_scans)`` for one register.

    Without ``--count-scans`` this stays free: an undigitized register is an
    exact ``(?, 0)`` and everything else an honest ``(?, ?)``. With it, the
    counts are exact and cost one ``/person`` count plus a ``/deed`` walk.
    """
    if not count_scans:
        return None, (None if _register_is_digitized(register) else 0)
    reg_id = register.get("id") or ""
    if not reg_id:
        return None, None
    n_persons = _count(session, "/person", f"register_id:{reg_id}")
    deeds = _paginate(session, "/deed", f"register_id:{reg_id}", "deed")
    return n_persons, sum(1 for d in deeds if d.get("asset"))


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


def _progress_csv() -> Path:
    return paths.cache_file(ARCHIVE, PROGRESS_CSV_NAME, legacy=Path(PROGRESS_CSV_NAME))


def _load_done() -> set[str]:
    done: set[str] = set()
    progress_csv = _progress_csv()
    if progress_csv.exists():
        with open(progress_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "done":
                    done.add(row["register_id"])
    return done


LIST_FIELDS = ["invnr", "kantoor", "register_name", "n_persons", "n_with_scans"]


def _list_registers(
    registers: list[dict],
    csv_out: str | None = None,
    counts: dict[str, tuple[int | None, int | None]] | None = None,
    only_digitized: bool = False,
) -> None:
    """Print a table of available inventory numbers from register metadata.

    ``counts`` maps register id → ``(n_persons, n_with_scans)`` and is only
    supplied under ``--count-scans``; otherwise the free digitized flag on the
    register decides between an exact ``0`` and an unknown ``?``.
    """
    rows: list[dict] = []
    for reg in registers:
        rmd = reg.get("metadata") or {}
        if counts is not None:
            n_persons, n_with_scans = counts.get(reg.get("id") or "", (None, None))
        else:
            n_persons, n_with_scans = None, (None if _register_is_digitized(reg) else 0)
        if only_digitized and not listing.has_scans(n_with_scans):
            continue
        rows.append(
            {
                "invnr": rmd.get("inventarisnummer") or "",
                "kantoor": _kantoor_from_register(reg),
                "register_name": rmd.get("naam") or "",
                "n_persons": listing.fmt_count(n_persons),
                "n_with_scans": listing.fmt_count(n_with_scans),
            }
        )

    print(f"\n  {'invnr':>6}  {'kantoor':<14}  {'persons':>7}  {'w/scans':>7}  register name")
    print(f"  {'------':>6}  {'-' * 14:<14}  {'-------':>7}  {'-------':>7}  -------------")
    for row in rows:
        print(
            f"  {row['invnr'] or '?':>6}  {row['kantoor']:<14}"
            f"  {row['n_persons']:>7}  {row['n_with_scans']:>7}  {row['register_name'] or '?'}"
        )
    print()

    if csv_out:
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LIST_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to {csv_out}\n")


def main(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
) -> None:
    if out_dir is not None:
        paths.set_out_dir(out_dir)
    output_dir = paths.archive_dir(ARCHIVE)

    session = _session()
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Collecting Tresoar Memorie van Successie registers …")
    registers = _paginate(session, "/register", REGISTER_FILTER, "register")
    print(f"Found {len(registers)} registers.")

    if invnrs is not None:
        registers = [
            r for r in registers if (r.get("metadata") or {}).get("inventarisnummer", "") in invnrs
        ]
        print(f"Filtered to {len(registers)} registers matching --invnr.")

    if list_invnrs:
        counts: dict[str, tuple[int | None, int | None]] | None = None
        if count_scans:
            print(f"Counting persons and scans for {len(registers)} registers …")
            counts = {
                reg.get("id") or "": _register_counts(session, reg, True) for reg in registers
            }
        _list_registers(registers, csv_out=csv_out, counts=counts, only_digitized=only_digitized)
        return

    if only_digitized:
        # The register listing already says which registers Tresoar digitized,
        # so the empty ones cost nothing to drop -- and dropping them saves the
        # two /deed + /person round trips they would otherwise each consume.
        kept = [r for r in registers if _register_is_digitized(r)]
        print(f"--only-digitized: {len(kept)} of {len(registers)} registers have scans.")
        registers = kept

    done = _load_done()
    progress_csv = _progress_csv()
    write_header = not progress_csv.exists() or progress_csv.stat().st_size == 0
    with open(progress_csv, "a", newline="", encoding="utf-8") as progress:
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
                writer.writerow(
                    {
                        "register_id": reg_id,
                        "kantoor": kantoor,
                        "invnr": invnr,
                        "status": "fetch_failed",
                        "n_persons": 0,
                    }
                )
                progress.flush()
                continue

            # Index deeds by id for person→deed join.
            deed_by_id: dict[str, dict] = {d.get("id", ""): d for d in deeds}

            n_persons = 0
            n_with_scans = 0
            for person in persons:
                deed_id = person.get("deed_id") or ""
                deed = deed_by_id.get(deed_id)
                if not deed:
                    continue

                pmd = person.get("metadata") or {}
                # Only include overledene persons (skip Vermeld etc.)
                if pmd.get("type_title", "").lower() not in ("overledene", ""):
                    continue

                # Download scan pages from the deed's embedded assets.
                assets = deed.get("asset") or []
                if only_digitized and not assets:
                    # Otherwise this person gets a directory holding nothing but
                    # a metadata.json with "n_scans": 0.
                    n_persons += 1
                    continue

                slug = _person_slug(person)
                dest_dir = output_dir / _sanitize(kantoor) / _sanitize(invnr) / slug

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
                if n_done:
                    n_with_scans += 1

            writer.writerow(
                {
                    "register_id": reg_id,
                    "kantoor": kantoor,
                    "invnr": invnr,
                    "status": "done",
                    "n_persons": n_persons,
                }
            )
            progress.flush()
            print(
                f"      {kantoor} {invnr}: {n_persons} persons, "
                f"{n_with_scans} with scans{listing.summary_suffix(n_with_scans)}",
                flush=True,
            )
            time.sleep(REQUEST_SLEEP)

    print("Friesland pipeline finished.")


if __name__ == "__main__":
    main()
