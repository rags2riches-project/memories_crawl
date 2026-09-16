"""Drents Archief – Memories van Successie downloader.

Uses the Memorix genealogy REST API, the same backend as BHIC and Tresoar but
with its own tenant key (``dre``).

Pipeline
────────
  1. Enumerate all 557 MvS registers in a single request via
        /register?fq=search_s_brontype:"Memorie van Successie"&rows=1000
     Register metadata carries ``inventarisnummer`` and ``gemeente``, so the
     inventory listing and the ``--invnr`` filter cost one request in total.
  2. For each (selected) register, paginate
        /deed?fq=register_id:{register_id}
        /person?fq=register_id:{register_id}
     Deed search results already embed ``asset[].download`` (full-size JPEG),
     so no per-deed detail request is needed.
  3. Download every deed asset into scans/drentsarchief/{deed_id}/.

Scans live at the **deed** level here (one deed = one memorie entry, one
person), unlike BHIC where they hang off the register.

Why not walk the person index
─────────────────────────────
  Earlier versions derived the inventory by paging /person (~106,000 records,
  ~1,064 requests plus 0.3 s of sleeping per page) and then fetching each deed
  detail one by one. That took well over half an hour before printing anything
  — and could never work, because deed documents carry no inventarisnummer at
  all (verified against the live API: no ``register`` key, no
  ``inventarisnummer`` field). The register endpoint has both. See issue #28.

Folder layout
─────────────
  scans/drentsarchief/{deed_id}/
      0001.jpg …          – one file per deed asset
      metadata.json       – deed + person + register info
"""

from __future__ import annotations

import csv
import json
import time
from collections import Counter
from pathlib import Path

import requests

from memories_crawl import download, paths

API_BASE = "https://webservices.memorix.nl/genealogy"
API_KEY = "a85387a2-fdb2-44d0-8209-3635e59c537e"
REGISTER_FILTER = 'search_s_brontype:"Memorie van Successie"'
PAGE_SIZE = 1000
ARCHIVE = "drentsarchief"
PROGRESS_CSV_NAME = "drentsarchief_deeds.csv"
USER_AGENT = "memories-crawl/1.0"

ARCHIVE_NAME = "Drents Archief"
ARCHIVE_NUMBER = "0119.03"

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


def _is_tafel(record: dict) -> bool:
    """Project-wide rule: never download Tafel V-bis.

    Drenthe indexes no tafels today (a register search for "tafel" returns 0),
    but the filter is kept so a future addition cannot slip through.
    """
    md = record.get("metadata") or {}
    blob = f"{md.get('naam', '')} {md.get('type_title', '')}".lower()
    return "tafel" in blob or "v-bis" in blob


def _register_invnr(register: dict) -> str:
    return (register.get("metadata") or {}).get("inventarisnummer") or ""


def _register_gemeente(register: dict) -> str:
    return (register.get("metadata") or {}).get("gemeente") or ""


def _collect_registers(session: requests.Session) -> list[dict]:
    """Return every Memorie van Successie register, Tafel V-bis excluded."""
    registers = _paginate(session, "/register", REGISTER_FILTER, "register")
    return [r for r in registers if not _is_tafel(r)]


def _list_registers(registers: list[dict], csv_out: str | None = None) -> None:
    """Print a table of available inventory numbers from register metadata."""
    print(f"\n{'invnr':>6}  {'gemeente':<20}  register name")
    print(f"{'------':>6}  {'------------------':<20}  {'-------------'}")
    for reg in sorted(
        registers,
        key=lambda r: (
            _register_gemeente(r),
            int(_register_invnr(r)) if _register_invnr(r).isdigit() else 999999,
            _register_invnr(r),
        ),
    ):
        invnr = _register_invnr(reg) or "?"
        gemeente = _register_gemeente(reg) or "?"
        naam = (reg.get("metadata") or {}).get("naam") or "?"
        print(f"  {invnr:>6}  {gemeente:<20}  {naam}")
    print()

    if csv_out:
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["invnr", "gemeente", "register_name"])
            writer.writeheader()
            for reg in registers:
                writer.writerow(
                    {
                        "invnr": _register_invnr(reg),
                        "gemeente": _register_gemeente(reg),
                        "register_name": (reg.get("metadata") or {}).get("naam") or "",
                    }
                )
        print(f"Wrote {len(registers)} rows to {csv_out}\n")


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return "exists"
    resp = session.get(url, stream=True, timeout=120)
    if resp.status_code == 404:
        return "missing"
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with open(tmp, "wb") as f:
        for chunk in resp.iter_content(65536):
            if chunk:
                f.write(chunk)
    tmp.rename(dest)
    return "downloaded"


def _write_metadata(dest_dir: Path, deed: dict, person: dict, register: dict) -> None:
    sidecar = dest_dir / "metadata.json"
    if sidecar.exists():
        return
    dmd = deed.get("metadata") or {}
    pmd = person.get("metadata") or {}
    rmd = register.get("metadata") or {}
    meta = {
        "archief_naam": ARCHIVE_NAME,
        "archief_nummer": rmd.get("archiefnummer") or ARCHIVE_NUMBER,
        "brontype": "Memorie van Successie",
        "gemeente": rmd.get("gemeente") or pmd.get("register_gemeente") or "",
        "inventarisnummer": rmd.get("inventarisnummer") or "",
        "register_naam": rmd.get("naam") or dmd.get("register_naam") or "",
        "register_id": register.get("id") or "",
        "naam_overledene": (f"{pmd.get('voornaam', '')} {pmd.get('geslachtsnaam', '')}".strip()),
        "overlijdensdatum": pmd.get("datum_overlijden") or pmd.get("datum") or "",
        "overlijdensplaats": pmd.get("plaats") or dmd.get("plaats") or "",
        "aktenummer": dmd.get("nummer") or "",
        "diversen": dmd.get("diversen") or "",
        "deed_id": deed.get("id") or "",
    }
    dest_dir.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def _progress_csv() -> Path:
    return paths.cache_file(ARCHIVE, PROGRESS_CSV_NAME, legacy=Path(PROGRESS_CSV_NAME))


def _load_done() -> set[str]:
    done: set[str] = set()
    progress_csv = _progress_csv()
    if progress_csv.exists():
        with open(progress_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "done":
                    done.add(row["deed_id"])
    return done


def main(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    workers: int = download.DEFAULT_WORKERS,
) -> None:
    if out_dir is not None:
        paths.set_out_dir(out_dir)
    output_dir = paths.archive_dir(ARCHIVE)
    output_dir.mkdir(parents=True, exist_ok=True)

    session = _session()
    downloader = download.Downloader(_download_file, workers=workers, session_factory=_session)

    print("Collecting Drents Archief Memorie van Successie registers …", flush=True)
    registers = _collect_registers(session)
    print(f"Found {len(registers)} registers.")

    if invnrs is not None:
        registers = [r for r in registers if _register_invnr(r) in invnrs]
        print(f"Filtered to {len(registers)} registers matching --invnr.")

    if list_invnrs:
        _list_registers(registers, csv_out=csv_out)
        return

    done = _load_done()
    progress_csv = _progress_csv()
    write_header = not progress_csv.exists() or progress_csv.stat().st_size == 0
    with open(progress_csv, "a", newline="", encoding="utf-8") as progress:
        writer = csv.DictWriter(progress, fieldnames=["deed_id", "invnr", "status", "n_scans"])
        if write_header:
            writer.writeheader()
            progress.flush()

        for idx, reg in enumerate(registers, start=1):
            reg_id = reg.get("id") or ""
            if not reg_id:
                continue
            invnr = _register_invnr(reg)
            gemeente = _register_gemeente(reg) or "?"

            deeds = _paginate(session, "/deed", f"register_id:{reg_id}", "deed")
            persons = _paginate(session, "/person", f"register_id:{reg_id}", "person")
            persons_by_deed = {p.get("deed_id"): p for p in persons if p.get("deed_id")}
            print(
                f"[{idx}/{len(registers)}] {gemeente} inv {invnr or '?'} – {len(deeds)} deeds …",
                flush=True,
            )

            # One batch per register: a deed usually holds a single scan, so
            # queueing the whole register is what keeps the workers busy.
            jobs: list[download.Job] = []
            fetched: list[str] = []
            for deed in deeds:
                deed_id = deed.get("id") or ""
                if not deed_id or deed_id in done:
                    continue

                assets = deed.get("asset") or []
                if not assets:
                    writer.writerow(
                        {"deed_id": deed_id, "invnr": invnr, "status": "no_assets", "n_scans": 0}
                    )
                    progress.flush()
                    continue

                dest_dir = output_dir / deed_id
                _write_metadata(dest_dir, deed, persons_by_deed.get(deed_id, {}), reg)
                fetched.append(deed_id)

                for asset_idx, asset in enumerate(assets, start=1):
                    download_url = asset.get("download") or asset.get("thumb.large") or ""
                    if not download_url:
                        continue
                    dest = dest_dir / f"{asset_idx:04d}.jpg"
                    jobs.append(download.Job(download_url, dest, key=deed_id))

            per_deed: Counter[str] = Counter()

            def _tally(job: download.Job, status: str, per_deed: Counter[str] = per_deed) -> None:
                if status in ("downloaded", "exists"):
                    per_deed[job.key] += 1

            downloader.run(jobs, on_result=_tally)

            n_scans = 0
            for deed_id in fetched:
                n_done = per_deed[deed_id]
                writer.writerow(
                    {"deed_id": deed_id, "invnr": invnr, "status": "done", "n_scans": n_done}
                )
                progress.flush()
                n_scans += n_done

            print(f"      ✓ {n_scans} scans", flush=True)
            time.sleep(REQUEST_SLEEP)

    downloader.close()
    print("Drents Archief pipeline finished.")


if __name__ == "__main__":
    main()
