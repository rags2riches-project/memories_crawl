from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Iterable

import requests

DEFAULT_INPUT_FILE = "scan_urls.csv"
DEFAULT_OUTPUT_DIR = "scans/openarchieven"
USER_AGENT = os.getenv("OPENARCH_USER_AGENT", "memories-crawl/1.0")

METADATA_COLS = [
    "brontype", "gemeente", "archief_naam", "inventarisnummer",
    "deel", "jaar", "kantoor", "naam_overledene", "sterfjaar", "sterfplaats",
]


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


def _write_metadata_sidecar(dest_dir: Path, row: dict[str, str]) -> None:
    meta: dict[str, str] = {
        "archive": row.get("archive", ""),
        "record_id": row.get("record_id", ""),
    }
    for col in METADATA_COLS:
        value = row.get(col, "")
        if value:
            meta[col] = value
    sidecar = dest_dir / "metadata.json"
    if not sidecar.exists():
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    output_dir = Path(args.output_dir)
    written_sidecars: set[Path] = set()

    for row in _iter_rows(args.input):
        archive = row.get("archive", "").strip()
        record_id = row.get("record_id", "").strip()
        page_seq = row.get("page_seq", "").strip() or "1"
        scan_uri = row.get("scan_uri", "").strip()
        if not archive or not record_id or not scan_uri:
            continue
        suffix = Path(scan_uri.split("?", 1)[0]).suffix or ".jpg"
        record_dir = output_dir / archive / record_id
        dest = record_dir / f"{page_seq}{suffix}"
        _download_file(session, scan_uri, dest)
        if record_dir not in written_sidecars:
            _write_metadata_sidecar(record_dir, row)
            written_sidecars.add(record_dir)


if __name__ == "__main__":
    main()
