"""Entry point for the Memories van Successie download pipeline.

Usage
─────
    uv run python main.py friesland          # Friesland (Tresoar / AlleFriezen, Memorix API)
    uv run python main.py nationaalarchief   # Zuid-Holland (Nationaal Archief)
    uv run python main.py drentsarchief      # Drenthe (Memorix API)
    uv run python main.py bhic               # Noord-Brabant (BHIC Memorix API)
    uv run python main.py overijssel         # Overijssel (HCO, MAIS + Playwright)
    uv run python main.py utrechtsarchief    # Utrecht (Het Utrechts Archief)
    uv run python main.py limburg            # Limburg (RHCL, archieven.nl MAIS)
    uv run python main.py noordholland       # Noord-Holland (Noord-Hollands Archief)
    uv run python main.py zeeland            # Zeeland (Zeeuws Archief)
    uv run python main.py gelderland         # Gelderland (Gelders Archief)
    uv run python main.py all                # Run all pipelines

Scans and caches are written below ``--out-dir`` (default ``./scans``); the
caches that make reruns cheap live in ``<out-dir>/.cache/<archive>/``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from memories_crawl import download, paths
from memories_crawl.summary import RunSummary, collect, grand_total


def _run_friesland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
    refresh_cache: bool = False,
) -> RunSummary | None:
    print("=== Friesland pipeline (Tresoar / AlleFriezen, Memorix API) ===")
    from memories_crawl.friesland import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
        refresh_cache=refresh_cache,
    )


def _run_nationaalarchief(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
    refresh_cache: bool = False,
) -> RunSummary | None:
    print("=== Nationaal Archief pipeline (Zuid-Holland, access 3.06.05) ===")
    from memories_crawl.nationaalarchief import main as run

    # 3.06.05 is a single flat inventory range, not a per-kantoor tree, so
    # there is nothing for --kantoor to select; say so instead of pretending
    # the filter was applied.
    if kantoren:
        print("  NOTE: nationaalarchief has no kantoor subdivision; --kantoor is ignored.")
    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        refresh_cache=refresh_cache,
    )


def _run_drentsarchief(
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
    print("=== Drents Archief pipeline (Memorix API) ===")
    from memories_crawl.drentsarchief import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
        refresh_cache=refresh_cache,
        dates=dates,
    )


def _run_bhic(
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
    print("=== BHIC pipeline (Noord-Brabant, Memorix API) ===")
    from memories_crawl.bhic import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
        refresh_cache=refresh_cache,
        dates=dates,
    )


def _run_overijssel(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    print("=== Overijssel pipeline (INCOMPLETE – see python/overijssel.py) ===")
    from memories_crawl.overijssel import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
    )


def _run_utrechtsarchief(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    print("=== Utrechts Archief pipeline ===")
    from memories_crawl.utrechtsarchief import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
    )


def _run_limburg(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    print("=== Limburg pipeline (RHCL, archieven.nl MAIS) ===")
    from memories_crawl.limburg import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
    )


def _run_noordholland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    print("=== Noord-Holland pipeline (Noord-Hollands Archief) ===")
    from memories_crawl.noordholland import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
    )


def _run_zeeland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    print("=== Zeeland pipeline (Zeeuws Archief) ===")
    from memories_crawl.zeeland import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
    )


def _run_gelderland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    only_digitized: bool = False,
    count_scans: bool = False,
    workers: int = download.DEFAULT_WORKERS,
    kantoren: set[str] | None = None,
) -> RunSummary | None:
    print("=== Gelderland pipeline (Gelders Archief) ===")
    from memories_crawl.gelderland import main as run

    return run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        only_digitized=only_digitized,
        count_scans=count_scans,
        workers=workers,
        kantoren=kantoren,
    )


PIPELINES = {
    "friesland": _run_friesland,
    "nationaalarchief": _run_nationaalarchief,
    "drentsarchief": _run_drentsarchief,
    "bhic": _run_bhic,
    "overijssel": _run_overijssel,
    "utrechtsarchief": _run_utrechtsarchief,
    "limburg": _run_limburg,
    "noordholland": _run_noordholland,
    "zeeland": _run_zeeland,
    "gelderland": _run_gelderland,
}

#: Pipelines whose archive-level inventory listing is cached on disk, and which
#: therefore accept ``--refresh-cache``.  The Playwright-driven ones keep their
#: own inventory/token caches and are not covered by that flag.
CACHED_LISTING_PIPELINES = frozenset({"friesland", "nationaalarchief", "drentsarchief", "bhic"})

#: Pipelines that can only date a register by querying for it, one request per
#: register, and therefore accept ``--dates``.  Everywhere else the period
#: comes with the inventory the pipeline already fetches, so it is always
#: reported and the flag has nothing to switch on (issue #38).
DATE_QUERY_PIPELINES = frozenset({"drentsarchief", "bhic"})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Memories van Successie scans from Dutch archives."
    )
    parser.add_argument(
        "pipeline",
        choices=list(PIPELINES) + ["all"],
        help="Which archive pipeline to run.",
    )
    parser.add_argument(
        "--invnr",
        dest="invnrs",
        action="append",
        default=None,
        help="Only scrape a specific inventarisnummer. Repeatable.",
    )
    parser.add_argument(
        "--kantoor",
        dest="kantoren",
        action="append",
        default=None,
        help="Restrict to one or more kantoren. Repeatable. Names match the "
        "kantoor column of --list-invnrs; where an archive also exposes a code "
        "or minr for the kantoor, that works too. Matching is case-insensitive.",
    )
    parser.add_argument(
        "--list-invnrs",
        action="store_true",
        default=False,
        help="List available inventory numbers and exit (no download).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help=(
            "Root directory for scans and caches "
            f"(default: ./{paths.DEFAULT_OUT_DIR}, or ${paths.ENV_VAR} if set). "
            "Caches live in <out-dir>/.cache/, so a listing pass and a download "
            "pass sharing an --out-dir also share the expensive token harvest."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=download.DEFAULT_WORKERS,
        help=(
            "Concurrent image downloads per archive "
            f"(default: {download.DEFAULT_WORKERS}). "
            "--workers 1 restores the strictly sequential behaviour of earlier "
            "releases. Raising it is at your own risk: these are small public "
            "archives, so stay well below what the server can take."
        ),
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        default=False,
        help=(
            "Re-collect the archive-level inventory listing instead of reusing "
            "the cached one (friesland, nationaalarchief, drentsarchief, bhic; "
            "the cache expires after 30 days by itself)."
        ),
    )
    parser.add_argument(
        "--csv",
        dest="csv_out",
        nargs="?",
        const="__default__",
        default=None,
        help="Write --list-invnrs output to a CSV file. "
        "Optional filename (default: {pipeline}_invnrs.csv).",
    )
    parser.add_argument(
        "--only-digitized",
        action="store_true",
        default=False,
        help=(
            "Skip inventarisnummers that are known to have no scans. Counts the "
            "pipeline could not obtain are never treated as zero, so nothing is "
            "hidden on a guess."
        ),
    )
    parser.add_argument(
        "--dates",
        action="store_true",
        default=False,
        help=(
            "Resolve the period each register covers in --list-invnrs where "
            "that costs extra requests (drentsarchief, bhic: one /person walk "
            "per register). Every other pipeline reports year_from/year_to for "
            "free and always does."
        ),
    )
    parser.add_argument(
        "--count-scans",
        action="store_true",
        default=False,
        help=(
            "Request exact counts in --list-invnrs. Costs extra metadata "
            "requests per inventarisnummer (a Playwright token harvest for "
            "the MAIS archives), so it is opt-in."
        ),
    )
    args = parser.parse_args()

    out_dir = paths.set_out_dir(args.out_dir)
    print(f"Output root: {out_dir.resolve()}")

    invnr_filter: set[str] | None = set(args.invnrs) if args.invnrs else None
    kantoor_filter: set[str] | None = set(args.kantoren) if args.kantoren else None
    targets = list(PIPELINES) if args.pipeline == "all" else [args.pipeline]
    summaries: list[RunSummary] = []
    for name in targets:
        csv_path: str | None = None
        if args.csv_out is not None:
            csv_path = f"{name}_invnrs.csv" if args.csv_out == "__default__" else args.csv_out
        kwargs: dict = {
            "invnrs": invnr_filter,
            "list_invnrs": args.list_invnrs,
            "csv_out": csv_path,
            "out_dir": out_dir,
        }
        kwargs["only_digitized"] = args.only_digitized
        kwargs["count_scans"] = args.count_scans
        kwargs["workers"] = args.workers
        kwargs["kantoren"] = kantoor_filter
        if name in CACHED_LISTING_PIPELINES:
            kwargs["refresh_cache"] = args.refresh_cache
        if name in DATE_QUERY_PIPELINES:
            kwargs["dates"] = args.dates
        failure: Exception | None = None
        # collect() sees the pipeline's summary even if the call never returns,
        # so a run that dies halfway still reports what it managed to download.
        with collect() as collected:
            try:
                PIPELINES[name](**kwargs)
            except Exception as exc:
                failure = exc
                print(f"ERROR in {name}: {exc}", file=sys.stderr)
                for run in collected:
                    run.error = str(exc)

        if failure is not None and not collected:
            # The pipeline died before it got as far as counting anything; the
            # archive still belongs in the table, as a row that failed.
            collected.append(RunSummary(name, error=str(failure)))

        if not args.list_invnrs:
            summaries.extend(collected)
            for run in collected:
                if not run.reported:  # a pipeline that finished reported itself
                    run.report()
        if failure is not None and args.pipeline != "all":
            raise failure

    if args.pipeline == "all" and summaries:
        print(f"\n{grand_total(summaries)}")


if __name__ == "__main__":
    main()
