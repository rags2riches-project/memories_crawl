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
    uv run python main.py openarchieven      # (deprecated – all archives migrated)
    uv run python main.py all                # Run all pipelines
"""
from __future__ import annotations

import argparse
import sys


def _run_friesland() -> None:
    print("=== Friesland pipeline (Tresoar / AlleFriezen, Memorix API) ===")
    from python.friesland import main as run
    run()


def _run_openarchieven() -> None:
    print("=== Open Archieven pipeline (deprecated – all migrated) ===")

    print("--- Step 1: collecting record GUIDs ---")
    from python.step1_collect_record_guids_from_search_api import main as step1
    step1()

    print("--- Step 2: parsing OAI-PMH XML dumps ---")
    from python.step2_oai_pmh_dumps import main as step2
    step2()

    print("--- Step 3: downloading scans ---")
    from python.step3_download_steps import main as step3
    step3()


def _run_nationaalarchief() -> None:
    print("=== Nationaal Archief pipeline (Zuid-Holland, access 3.06.05) ===")
    from python.nationaalarchief import main as run
    run()


def _run_drentsarchief() -> None:
    print("=== Drents Archief pipeline (Memorix API) ===")
    from python.drentsarchief import main as run
    run()


def _run_bhic() -> None:
    print("=== BHIC pipeline (Noord-Brabant, Memorix API) ===")
    from python.bhic import main as run
    run()


def _run_overijssel() -> None:
    print("=== Overijssel pipeline (INCOMPLETE – see python/overijssel.py) ===")
    from python.overijssel import main as run
    run()


def _run_utrechtsarchief() -> None:
    print("=== Utrechts Archief pipeline ===")
    from python.utrechtsarchief import main as run
    run()


def _run_limburg() -> None:
    print("=== Limburg pipeline (RHCL, archieven.nl MAIS) ===")
    from python.limburg import main as run
    run()


def _run_noordholland() -> None:
    print("=== Noord-Holland pipeline (Noord-Hollands Archief) ===")
    from python.noordholland import main as run
    run()


def _run_zeeland() -> None:
    print("=== Zeeland pipeline (Zeeuws Archief) ===")
    from python.zeeland import main as run
    run()


def _run_gelderland() -> None:
    print("=== Gelderland pipeline (Gelders Archief) ===")
    from python.gelderland import main as run
    run()


PIPELINES = {
    "friesland":        _run_friesland,
    "openarchieven":    _run_openarchieven,
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
    args = parser.parse_args()

    targets = list(PIPELINES) if args.pipeline == "all" else [args.pipeline]
    for name in targets:
        try:
            PIPELINES[name]()
        except Exception as exc:
            print(f"ERROR in {name}: {exc}", file=sys.stderr)
            if args.pipeline != "all":
                raise


if __name__ == "__main__":
    main()
