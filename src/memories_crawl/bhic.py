"""BHIC (Noord-Brabant) – Memories van Successie downloader.

The Brabants Historisch Informatie Centrum exposes the same Memorix genealogy
REST API as Drents Archief, but with a different tenant key. Crucially, BHIC's
scans live at the **register** level (one register = one bound book of memorie
entries), not at the deed level as in Drenthe.

Pipeline
────────
  1. Enumerate all 1,896 MvS registers via
        /register?fq=search_s_type_title:"memorie van successie"
  2. For each register, paginate
        /asset?fq=register_id:{register_id}
     and download every asset[].download (full-size JPEG).
  3. Paginate
        /deed?fq=register_id:{register_id}
        /person?fq=register_id:{register_id}
     and persist as deeds.json so per-akte/per-person info stays alongside the
     scans (aktenummer, plaats van overlijden, naam overledene, …).

Folder layout
─────────────
  <out-dir>/bhic/{gemeente}/deel_{invnr}/
      {asset_name}.jpg           – e.g. BergenopZoom_044_0001.jpg
      metadata.json              – register-level info
      deeds.json                 – list of all deeds + persons in this register

Tafel V-bis
───────────
  Not present at BHIC (a register search for "tafel" returns 0). A defensive
  filter still skips any register whose naam/type contains "tafel" or "v-bis".
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
API_KEY = "24c66d08-da4a-4d60-917f-5942681dcaa1"
REGISTER_FILTER = 'search_s_type_title:"memorie van successie"'
PAGE_SIZE = 100
ARCHIVE = "bhic"
PROGRESS_CSV_NAME = "bhic_progress.csv"
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Brabants Historisch Informatie Centrum"
BRONTYPE_LABEL = "Memorie van Successie"

# Be polite – Memorix is shared infra.
REQUEST_SLEEP = 0.25
RATE_LIMIT_SLEEP = 10


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get_json(session: requests.Session, path: str, params: dict, retries: int = 3) -> dict:
    """GET an API endpoint with backoff on 429/5xx."""
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
    """Has BHIC digitized this register?

    Register search results carry a truncated ``asset`` sample -- one entry for
    a register holding hundreds of scans -- so it answers "any scans at all?"
    for free, but never "how many". That needs ``--count-scans``.
    """
    return bool(register.get("asset"))


def _count_scans(session: requests.Session, register: dict) -> int | None:
    """Exact number of scans in one register (one ``/asset`` count request)."""
    reg_id = register.get("id") or ""
    if not reg_id:
        return None
    return _count(session, "/asset", f"register_id:{reg_id}")


def _is_tafel(register: dict) -> bool:
    md = register.get("metadata") or {}
    blob = f"{md.get('naam', '')} {md.get('type_title', '')}".lower()
    return "tafel" in blob or "v-bis" in blob


_SANITIZE_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _sanitize(name: str) -> str:
    """Make a string safe for use as a path segment on common filesystems."""
    cleaned = _SANITIZE_RE.sub("_", (name or "").strip())
    # Trim trailing dots/spaces (Windows-hostile, but cheap to do everywhere).
    return cleaned.rstrip(". ") or "unknown"


def _progress_csv() -> Path:
    return paths.cache_file(ARCHIVE, PROGRESS_CSV_NAME, legacy=Path(PROGRESS_CSV_NAME))


def _register_dir(register: dict) -> Path:
    """Return <out-dir>/bhic/{gemeente}/deel_{invnr}/ for a register."""
    md = register.get("metadata") or {}
    gemeente = _sanitize(md.get("gemeente") or "onbekend")
    invnr = _sanitize(md.get("inventarisnummer") or register.get("id", "unknown"))
    return paths.archive_dir(ARCHIVE) / gemeente / f"deel_{invnr}"


def _write_register_metadata(dest_dir: Path, register: dict) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    md = register.get("metadata") or {}
    code = md.get("code") or ""
    # Code looks like "036.03.01-44" → archief prefix before the final "-".
    archief_nummer = code.rsplit("-", 1)[0] if "-" in code else code

    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": archief_nummer,
        "brontype": BRONTYPE_LABEL,
        "gemeente": md.get("gemeente") or "",
        "inventarisnummer": md.get("inventarisnummer") or "",
        "naam": md.get("naam") or "",
        "code": code,
        "register_id": register.get("id") or "",
        "url_origineel": (
            f"https://www.bhic.nl/memorix/genealogy/search/registers/{register.get('id', '')}"
        ),
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _write_deeds_sidecar(dest_dir: Path, deeds: list[dict], persons: list[dict]) -> None:
    sidecar = dest_dir / "deeds.json"
    if sidecar.exists():
        return
    # Group persons by deed_id so each deed entry carries its overledene(n).
    persons_by_deed: dict[str, list[dict]] = {}
    for p in persons:
        deed_id = p.get("deed_id") or ""
        if not deed_id:
            continue
        pmd = p.get("metadata") or {}
        persons_by_deed.setdefault(deed_id, []).append(
            {
                "person_id": p.get("id"),
                "voornaam": pmd.get("voornaam") or "",
                "tussenvoegsel": pmd.get("tussenvoegsel") or "",
                "geslachtsnaam": pmd.get("geslachtsnaam") or "",
                "naam_volledig": pmd.get("person_display_name") or "",
                "geslacht": pmd.get("geslacht") or "",
                "datum_overlijden": pmd.get("datum_overlijden") or pmd.get("datum") or "",
                "plaats_overlijden": pmd.get("plaats_overlijden") or pmd.get("plaats") or "",
                "rol": pmd.get("type_title") or "",
            }
        )

    out: list[dict] = []
    for d in deeds:
        dmd = d.get("metadata") or {}
        out.append(
            {
                "deed_id": d.get("id"),
                "aktenummer": dmd.get("nummer") or "",
                "plaats": dmd.get("plaats") or "",
                "personen": persons_by_deed.get(d.get("id") or "", []),
            }
        )
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


def _asset_filename(asset: dict) -> str:
    """Prefer the human-readable name; fall back to the file UUID."""
    md = asset.get("metadata") or {}
    name = md.get("name") or asset.get("title") or asset.get("file_id") or asset.get("id") or "scan"
    return _sanitize(name) + ".jpg"


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
    progress_csv = _progress_csv()
    if progress_csv.exists():
        with open(progress_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "done":
                    done.add(row["register_id"])
    return done


LIST_FIELDS = ["invnr", "gemeente", "register_name", "n_scans"]


def _list_registers(
    registers: list[dict],
    csv_out: str | None = None,
    counts: dict[str, int | None] | None = None,
    only_digitized: bool = False,
) -> None:
    """Print a table of available inventory numbers from register metadata.

    ``counts`` maps register id → exact scan count and is only supplied under
    ``--count-scans``; otherwise the free digitized flag on the register
    decides between an exact ``0`` and an unknown ``?``.
    """
    rows: list[dict] = []
    for reg in registers:
        md = reg.get("metadata") or {}
        if counts is not None:
            n_scans = counts.get(reg.get("id") or "")
        else:
            n_scans = None if _register_is_digitized(reg) else 0
        if only_digitized and not listing.has_scans(n_scans):
            continue
        rows.append(
            {
                "invnr": md.get("inventarisnummer") or "",
                "gemeente": md.get("gemeente") or "",
                "register_name": md.get("naam") or "",
                "n_scans": listing.fmt_count(n_scans),
            }
        )

    print(f"\n  {'invnr':>6}  {'gemeente':<20}  {'scans':>6}  register name")
    print(f"  {'------':>6}  {'-' * 20:<20}  {'------':>6}  {'-------------'}")
    for row in rows:
        print(
            f"  {row['invnr'] or '?':>6}  {row['gemeente'] or '?':<20}"
            f"  {row['n_scans']:>6}  {row['register_name'] or '?'}"
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
    paths.archive_dir(ARCHIVE).mkdir(parents=True, exist_ok=True)

    session = _session()

    print("Collecting BHIC Memorie van Successie registers …")
    registers = _paginate(session, "/register", REGISTER_FILTER, "register")
    print(f"Found {len(registers)} registers.")

    if invnrs is not None:
        registers = [
            r for r in registers if (r.get("metadata") or {}).get("inventarisnummer", "") in invnrs
        ]
        print(f"Filtered to {len(registers)} registers matching --invnr.")

    if list_invnrs:
        counts: dict[str, int | None] | None = None
        if count_scans:
            print(f"Counting scans for {len(registers)} registers …")
            counts = {reg.get("id") or "": _count_scans(session, reg) for reg in registers}
        _list_registers(registers, csv_out=csv_out, counts=counts, only_digitized=only_digitized)
        return

    if only_digitized:
        # The register listing already carries the digitized flag, so dropping
        # the empty registers here is free and saves each of them a deeds,
        # persons and asset round trip.
        kept = [r for r in registers if _register_is_digitized(r)]
        print(f"--only-digitized: {len(kept)} of {len(registers)} registers have scans.")
        registers = kept

    done = _load_done()
    progress_csv = _progress_csv()
    write_header = not progress_csv.exists() or progress_csv.stat().st_size == 0
    with open(progress_csv, "a", newline="", encoding="utf-8") as progress:
        writer = csv.DictWriter(
            progress, fieldnames=["register_id", "gemeente", "invnr", "status", "n_scans"]
        )
        if write_header:
            writer.writeheader()
            progress.flush()

        for idx, reg in enumerate(registers, start=1):
            reg_id = reg.get("id") or ""
            md = reg.get("metadata") or {}
            gemeente = md.get("gemeente") or "?"
            invnr = md.get("inventarisnummer") or "?"

            if not reg_id or reg_id in done:
                continue
            if _is_tafel(reg):
                writer.writerow(
                    {
                        "register_id": reg_id,
                        "gemeente": gemeente,
                        "invnr": invnr,
                        "status": "skipped_tafel",
                        "n_scans": 0,
                    }
                )
                progress.flush()
                continue

            dest_dir = _register_dir(reg)
            print(f"[{idx}/{len(registers)}] {gemeente} deel {invnr} → {dest_dir} …", flush=True)

            _write_register_metadata(dest_dir, reg)

            # Pull all deeds + persons for the genealogical sidecar.
            try:
                deeds = _paginate(session, "/deed", f"register_id:{reg_id}", "deed")
                persons = _paginate(session, "/person", f"register_id:{reg_id}", "person")
                _write_deeds_sidecar(dest_dir, deeds, persons)
            except Exception as exc:
                print(f"      WARN: deeds/persons fetch failed: {exc}", flush=True)

            # Page through assets and download each scan.
            try:
                assets = _paginate(session, "/asset", f"register_id:{reg_id}", "asset")
            except Exception as exc:
                print(f"      ERROR: asset listing failed: {exc}", flush=True)
                writer.writerow(
                    {
                        "register_id": reg_id,
                        "gemeente": gemeente,
                        "invnr": invnr,
                        "status": "asset_list_failed",
                        "n_scans": 0,
                    }
                )
                progress.flush()
                continue

            n_done = 0
            for asset in assets:
                # Prefer the explicit asset-search "download" URL; fall back to
                # building one from the file_id if missing.
                url = asset.get("download") or ""
                file_id = asset.get("file_id") or ""
                if not url and file_id:
                    url = f"https://images.memorix.nl/bhic/download/fullsize/{file_id}.jpg"
                if not url:
                    continue
                dest = dest_dir / _asset_filename(asset)
                status = _download_file(session, url, dest)
                if status in ("downloaded", "exists"):
                    n_done += 1

            writer.writerow(
                {
                    "register_id": reg_id,
                    "gemeente": gemeente,
                    "invnr": invnr,
                    "status": "done",
                    "n_scans": n_done,
                }
            )
            progress.flush()
            print(
                f"      ✓ {gemeente} {invnr}: {n_done} scans{listing.summary_suffix(n_done)}",
                flush=True,
            )
            time.sleep(REQUEST_SLEEP)

    print("BHIC pipeline finished.")


if __name__ == "__main__":
    main()
