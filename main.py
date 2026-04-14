from __future__ import annotations

import argparse

from python.step1_collect_record_guids_from_search_api import main as step1_main
from python.step2_oai_pmh_dumps import main as step2_main
from python.step3_download_steps import main as step3_main


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=["step1", "step2", "step3"])
    args, _ = parser.parse_known_args()

    if args.step == "step1":
        step1_main()
    elif args.step == "step2":
        step2_main()
    else:
        step3_main()


if __name__ == "__main__":
    main()
