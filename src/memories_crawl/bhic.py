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

from memories_crawl import download, filters, listing, paths, regcache
from memories_crawl.summary import PageTally, RunSummary

API_BASE = "https://webservices.memorix.nl/genealogy"
API_KEY = "24c66d08-da4a-4d60-917f-5942681dcaa1"
REGISTER_FILTER = 'search_s_type_title:"memorie van successie"'
#: Memorix indexes the inventarisnummer as an exact-match string field, so the
#: ``--invnr`` filter can be pushed into the query instead of walking the whole
#: listing and discarding 1,890 of 1,896 registers (issue #25).
INVNR_FIELD = "search_s_inventarisnummer"
PAGE_SIZE = 100
ARCHIVE = "bhic"
PROGRESS_CSV_NAME = "bhic_progress.csv"
REGISTER_CACHE_NAME = "registers.json"
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


def _escape_fq(value: str) -> str:
    """Quote-escape a user-supplied value for use inside an ``fq`` phrase."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _register_fq(invnrs: set[str]) -> str:
    """Return the register filter query narrowed to ``invnrs`` server-side.

    Verified against the live API to select exactly the registers the old
    client-side filter kept, including non-numeric numbers such as
    ``1903-1906``, and never a numeric prefix of another (``1`` does not match
    ``12``).
    """
    values = " OR ".join(f'"{_escape_fq(v)}"' for v in sorted(invnrs))
    return f"{REGISTER_FILTER} AND {INVNR_FIELD}:({values})"


def _load_registers(
    session: requests.Session,
    invnrs: set[str] | None = None,
    refresh_cache: bool = False,
) -> list[dict]:
    """Return the MvS registers, narrowed to ``invnrs`` when one is given.

    A ``--invnr`` run asks Memorix for just those registers, which is a single
    request rather than the nineteen-page walk.  An unfiltered run pays for the
    walk once and caches it (see :mod:`memories_crawl.regcache`).
    """
    if invnrs is not None:
        if not invnrs:
            return []
        return _paginate(session, "/register", _register_fq(invnrs), "register")
    return regcache.load_or_collect(
        ARCHIVE,
        REGISTER_CACHE_NAME,
        lambda: _paginate(session, "/register", REGISTER_FILTER, "register"),
        key=REGISTER_FILTER,
        refresh=refresh_cache,
    )


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


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    """Fetch one scan; see :func:`download.fetch_file` for the retry rules."""
    return download.fetch_file(
        session,
        url,
        dest,
        missing_statuses=(404,),
        timeout=180,
        allow_redirects=True,
    )


def _load_done() -> set[str]:
    done: set[str] = set()
    progress_csv = _progress_csv()
    if progress_csv.exists():
        with open(progress_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "done":
                    done.add(row["register_id"])
    return done


def _list_registers(
    registers: list[dict],
    csv_out: str | None = None,
    counts: dict[str, int | None] | None = None,
    only_digitized: bool = False,
    years: dict[str, tuple[int | None, int | None]] | None = None,
) -> None:
    """Print a table of available inventory numbers from register metadata.

    ``counts`` maps register id → exact scan count and is only supplied under
    ``--count-scans``; otherwise the free digitized flag on the register
    decides between an exact ``0`` and an unknown ``?``.

    ``years`` maps register id → ``(year_from, year_to)`` and is only supplied
    under ``--dates``, which spends one request per register on it; without it
    the period is an honest ``?`` rather than a span guessed from the
    inventarisnummer.
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
        year_from, year_to = (years or {}).get(reg.get("id") or "", (None, None))
        rows.append(
            {
                "invnr": md.get("inventarisnummer") or "",
                "gemeente": md.get("gemeente") or "",
                "register_name": md.get("naam") or "",
                "n_scans": listing.fmt_count(n_scans),
                "period": listing.fmt_period(year_from, year_to),
                **listing.year_row(year_from, year_to),
            }
        )

    print(f"\n  {'invnr':>6}  {'gemeente':<20}  {'period':<11}  {'scans':>6}  register name")
    print(f"  {'------':>6}  {'-' * 20:<20}  {'-' * 11:<11}  {'------':>6}  {'-------------'}")
    for row in rows:
        print(
            f"  {row['invnr'] or '?':>6}  {row['gemeente'] or '?':<20}  {row['period']:<11}"
            f"  {row['n_scans']:>6}  {row['register_name'] or '?'}"
        )
    print()

    if csv_out:
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LIST_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to {csv_out}\n")


def _register_years(session: requests.Session, register: dict) -> tuple[int | None, int | None]:
    """Period of one register, from the death dates of the people in it.

    A BHIC register is one bound book, described as 'Memories van successie
    Eindhoven deel 84' -- an identifier, never a datering (issue #38).
    Each person carries ``datum_overlijden``; ``rows=100`` paging walks the
    register in one or two requests, so this costs about what ``--count-scans``
    does and is only paid under ``--dates``.
    """
    reg_id = register.get("id") or ""
    if not reg_id:
        return None, None
    try:
        persons = _paginate(session, "/person", f"register_id:{reg_id}", "person")
    except requests.RequestException:
        return None, None
    return listing.span(
        (p.get("metadata") or {}).get("datum_overlijden") or (p.get("metadata") or {}).get("datum")
        for p in persons
    )


def _count(session: requests.Session, path: str, fq: str) -> int | None:
    """Exact number of hits for a filter, in a single request.

    ``rows=1`` still reports ``pagination.total``, so a count costs one small
    response instead of paging the whole result set.
    """
    try:
        data = _get_json(session, path, {"q": "*:*", "rows": 1, "page": 1, "fq": fq})
        total = ((data.get("metadata") or {}).get("pagination") or {}).get("total")
        return int(total) if total is not None and int(total) >= 0 else None
    except (requests.RequestException, ValueError, TypeError):
        return None


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


LIST_FIELDS = [
    "invnr",
    "gemeente",
    "register_name",
    "n_scans",
    "period",
    *listing.YEAR_FIELDS,
]


def main(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
    refresh_cache: bool = False,
    dates: bool = False,
) -> RunSummary | None:
    kantoor_filter = filters.normalize(kantoren)

    if out_dir is not None:
        paths.set_out_dir(out_dir)
    paths.archive_dir(ARCHIVE).mkdir(parents=True, exist_ok=True)

    session = _session()
    downloader = download.Downloader(_download_file, workers=workers, session_factory=_session)
    try:
        print("Collecting BHIC Memorie van Successie registers …")
        registers = _load_registers(session, invnrs=invnrs, refresh_cache=refresh_cache)
        if kantoor_filter is not None:
            registers = [
                r
                for r in registers
                if filters.matches(
                    kantoor_filter,
                    (r.get("metadata") or {}).get("gemeente") or "",
                    (r.get("metadata") or {}).get("code") or "",
                )
            ]
        if invnrs is not None or kantoor_filter is not None:
            print(
                f"Filtered to {len(registers)} registers matching "
                f"{filters.describe(invnrs, kantoren)}."
            )
            if not registers:
                print(
                    f"\nWARNING: {filters.describe(invnrs, kantoren)} matched no "
                    "register in the BHIC collection."
                )
        else:
            print(f"Found {len(registers)} registers.")

        if list_invnrs:
            counts: dict[str, int | None] | None = None
            if count_scans:
                print(f"Counting scans for {len(registers)} registers …")
                counts = {reg.get("id") or "": _count_scans(session, reg) for reg in registers}
            years: dict[str, tuple[int | None, int | None]] | None = None
            if dates:
                # One /person walk per register: the only way to date a register
                # here, so it stays behind --dates instead of slowing every listing.
                print(f"Resolving the period of {len(registers)} registers …")
                years = {reg.get("id") or "": _register_years(session, reg) for reg in registers}
            _list_registers(
                registers,
                csv_out=csv_out,
                counts=counts,
                only_digitized=only_digitized,
                years=years,
            )
            return

        if only_digitized:
            # The register listing already carries the digitized flag, so dropping
            # the empty registers here is free and saves each of them a deeds,
            # persons and asset round trip.
            kept = [r for r in registers if _register_is_digitized(r)]
            print(f"--only-digitized: {len(kept)} of {len(registers)} registers have scans.")
            registers = kept

        # Registers are grouped by gemeente here; the archive has no kantoor layer.
        summary = RunSummary(ARCHIVE, "Noord-Brabant (BHIC)", unit_name="gemeenten")
        gemeenten_seen: set[str] = set()

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
                print(
                    f"[{idx}/{len(registers)}] {gemeente} deel {invnr} → {dest_dir} …", flush=True
                )

                _write_register_metadata(dest_dir, reg)

                # Pull all deeds + persons for the genealogical sidecar.
                deeds: list[dict] = []
                try:
                    deeds = _paginate(session, "/deed", f"register_id:{reg_id}", "deed")
                    persons = _paginate(session, "/person", f"register_id:{reg_id}", "person")
                    _write_deeds_sidecar(dest_dir, deeds, persons)
                    summary.records += len(deeds)
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

                jobs: list[download.Job] = []
                print(
                    f"      {len(deeds)} memories, {len(assets)} scans"
                    f"{listing.summary_suffix(len(assets))}",
                    flush=True,
                )

                tally = PageTally()
                for asset in assets:
                    # Prefer the explicit asset-search "download" URL; fall back to
                    # building one from the file_id if missing.
                    url = asset.get("download") or ""
                    file_id = asset.get("file_id") or ""
                    if not url and file_id:
                        url = f"https://images.memorix.nl/bhic/download/fullsize/{file_id}.jpg"
                    if not url:
                        continue
                    jobs.append(download.Job(url, dest_dir / _asset_filename(asset)))

                def record_page(job: download.Job, status: str) -> None:
                    tally.record(status, job.dest)
                    summary.pages.record(status, job.dest)

                downloader.run(jobs, on_result=record_page)

                n_done = tally.downloaded + tally.skipped
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
                print(f"      ✓ {tally.describe(len(assets))}", flush=True)

                gemeenten_seen.add(gemeente)
                summary.units = len(gemeenten_seen)
                summary.registers += 1
                time.sleep(REQUEST_SLEEP)

        summary.report()
        return summary
    finally:
        downloader.close()


if __name__ == "__main__":
    main()
