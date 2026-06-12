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
"""
from __future__ import annotations

import argparse
import sys


def _run_friesland(invnrs: set[str] | None = None, list_invnrs: bool = False,
                   csv_out: str | None = None) -> None:
    print("=== Friesland pipeline (Tresoar / AlleFriezen, Memorix API) ===")
    from python.friesland import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_nationaalarchief(invnrs: set[str] | None = None, list_invnrs: bool = False,
                          csv_out: str | None = None) -> None:
    print("=== Nationaal Archief pipeline (Zuid-Holland, access 3.06.05) ===")
    from python.nationaalarchief import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_drentsarchief(invnrs: set[str] | None = None, list_invnrs: bool = False,
                       csv_out: str | None = None) -> None:
    print("=== Drents Archief pipeline (Memorix API) ===")
    from python.drentsarchief import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_bhic(invnrs: set[str] | None = None, list_invnrs: bool = False,
              csv_out: str | None = None) -> None:
    print("=== BHIC pipeline (Noord-Brabant, Memorix API) ===")
    from python.bhic import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_overijssel(invnrs: set[str] | None = None, list_invnrs: bool = False,
                    csv_out: str | None = None) -> None:
    print("=== Overijssel pipeline (INCOMPLETE – see python/overijssel.py) ===")
    from python.overijssel import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_utrechtsarchief(invnrs: set[str] | None = None, list_invnrs: bool = False,
                         csv_out: str | None = None) -> None:
    print("=== Utrechts Archief pipeline ===")
    from python.utrechtsarchief import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_limburg(invnrs: set[str] | None = None, list_invnrs: bool = False,
                 csv_out: str | None = None) -> None:
    print("=== Limburg pipeline (RHCL, archieven.nl MAIS) ===")
    from python.limburg import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_noordholland(invnrs: set[str] | None = None, list_invnrs: bool = False,
                      csv_out: str | None = None) -> None:
    print("=== Noord-Holland pipeline (Noord-Hollands Archief) ===")
    from python.noordholland import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_zeeland(invnrs: set[str] | None = None, list_invnrs: bool = False,
                 csv_out: str | None = None) -> None:
    print("=== Zeeland pipeline (Zeeuws Archief) ===")
    from python.zeeland import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


def _run_gelderland(invnrs: set[str] | None = None, list_invnrs: bool = False,
                    csv_out: str | None = None) -> None:
    print("=== Gelderland pipeline (Gelders Archief) ===")
    from python.gelderland import main as run
    run(invnrs=invnrs, list_invnrs=list_invnrs, csv_out=csv_out)


PIPELINES = {
    "friesland":        _run_friesland,
    "nationaalarchief": _run_nationaalarchief,
    "drentsarchief":    _run_drentsarchief,
    "bhic":             _run_bhic,
    "overijssel":       _run_overijssel,
    "utrechtsarchief":  _run_utrechtsarchief,
    "limburg":          _run_limburg,
    "noordholland":     _run_noordholland,
    "zeeland":          _run_zeeland,
    "gelderland":       _run_gelderland,
}


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
        "--csv",
        dest="csv_out",
        nargs="?",
        const="__default__",
        default=None,
        help="Write --list-invnrs output to a CSV file. "
             "Optional filename (default: {pipeline}_invnrs.csv).",
    )
    args = parser.parse_args()

    invnr_filter: set[str] | None = set(args.invnrs) if args.invnrs else None
    targets = list(PIPELINES) if args.pipeline == "all" else [args.pipeline]
    for name in targets:
        csv_path: str | None = None
        if args.csv_out is not None:
            csv_path = f"{name}_invnrs.csv" if args.csv_out == "__default__" else args.csv_out
        try:
            PIPELINES[name](invnrs=invnr_filter, list_invnrs=args.list_invnrs,
                            csv_out=csv_path)
        except Exception as exc:
            print(f"ERROR in {name}: {exc}", file=sys.stderr)
            if args.pipeline != "all":
                raise


if __name__ == "__main__":
    main()
