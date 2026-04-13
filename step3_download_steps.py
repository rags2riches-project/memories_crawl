from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
from typing import Iterable

import requests

DEFAULT_INPUT_FILE = "scan_urls.csv"
DEFAULT_OUTPUT_DIR = "scans"
USER_AGENT = os.getenv("OPENARCH_USER_AGENT", "memories-crawl/1.0")


def _download_file(session: requests.Session, url: str, dest: Path) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return "exists"
    with session.get(url, stream=True, timeout=120) as response:
        if response.status_code == 404:
            return "missing"
        response.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in response.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)
    return "downloaded"


def _iter_rows(path: str) -> Iterable[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    output_dir = Path(args.output_dir)
    for row in _iter_rows(args.input):
        archive = row.get("archive", "").strip()
        record_id = row.get("record_id", "").strip()
        page_seq = row.get("page_seq", "").strip() or "1"
        scan_uri = row.get("scan_uri", "").strip()
        if not archive or not record_id or not scan_uri:
            continue
        suffix = Path(scan_uri.split("?", 1)[0]).suffix or ".jpg"
        dest = output_dir / archive / record_id / f"{page_seq}{suffix}"
        _download_file(session, scan_uri, dest)


if __name__ == "__main__":
    main()
