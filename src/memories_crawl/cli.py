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

from memories_crawl import paths


def _run_friesland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    refresh_cache: bool = False,
) -> None:
    print("=== Friesland pipeline (Tresoar / AlleFriezen, Memorix API) ===")
    from memories_crawl.friesland import main as run

    run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        refresh_cache=refresh_cache,
    )


def _run_nationaalarchief(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    refresh_cache: bool = False,
) -> None:
    print("=== Nationaal Archief pipeline (Zuid-Holland, access 3.06.05) ===")
    from memories_crawl.nationaalarchief import main as run

    run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        refresh_cache=refresh_cache,
    )


def _run_drentsarchief(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    refresh_cache: bool = False,
) -> None:
    print("=== Drents Archief pipeline (Memorix API) ===")
    from memories_crawl.drentsarchief import main as run

    run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        refresh_cache=refresh_cache,
    )


def _run_bhic(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
    refresh_cache: bool = False,
) -> None:
    print("=== BHIC pipeline (Noord-Brabant, Memorix API) ===")
    from memories_crawl.bhic import main as run

    run(
        invnrs=invnrs,
        list_invnrs=list_invnrs,
        csv_out=csv_out,
        out_dir=out_dir,
        refresh_cache=refresh_cache,
    )


def _run_overijssel(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
) -> None:
    print("=== Overijssel pipeline (INCOMPLETE – see python/overijssel.py) ===")
    from memories_crawl.overijssel import main as run

    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out, out_dir=out_dir)


def _run_utrechtsarchief(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
) -> None:
    print("=== Utrechts Archief pipeline ===")
    from memories_crawl.utrechtsarchief import main as run

    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out, out_dir=out_dir)


def _run_limburg(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
) -> None:
    print("=== Limburg pipeline (RHCL, archieven.nl MAIS) ===")
    from memories_crawl.limburg import main as run

    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out, out_dir=out_dir)


def _run_noordholland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
) -> None:
    print("=== Noord-Holland pipeline (Noord-Hollands Archief) ===")
    from memories_crawl.noordholland import main as run

    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out, out_dir=out_dir)


def _run_zeeland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
) -> None:
    print("=== Zeeland pipeline (Zeeuws Archief) ===")
    from memories_crawl.zeeland import main as run

    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out, out_dir=out_dir)


def _run_gelderland(
    invnrs: set[str] | None = None,
    list_invnrs: bool = False,
    csv_out: str | None = None,
    out_dir: Path | None = None,
) -> None:
    print("=== Gelderland pipeline (Gelders Archief) ===")
    from memories_crawl.gelderland import main as run

    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out, out_dir=out_dir)


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
    args = parser.parse_args()

    out_dir = paths.set_out_dir(args.out_dir)
    print(f"Output root: {out_dir.resolve()}")

    invnr_filter: set[str] | None = set(args.invnrs) if args.invnrs else None
    targets = list(PIPELINES) if args.pipeline == "all" else [args.pipeline]
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
        if name in CACHED_LISTING_PIPELINES:
            kwargs["refresh_cache"] = args.refresh_cache
        try:
            PIPELINES[name](**kwargs)
        except Exception as exc:
            print(f"ERROR in {name}: {exc}", file=sys.stderr)
            if args.pipeline != "all":
                raise


if __name__ == "__main__":
    main()
