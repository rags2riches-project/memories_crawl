from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path
from typing import Iterable

import requests

DEFAULT_INPUT_FILE = "scan_urls.csv"
DEFAULT_OUTPUT_DIR = "scans/openarchieven"
CHECKPOINT_FILE = "step3_checkpoint.json"
USER_AGENT = os.getenv("OPENARCH_USER_AGENT", "memories-crawl/1.0")

METADATA_COLS = [
    "brontype", "gemeente", "archief_naam", "inventarisnummer",
    "deel", "jaar", "kantoor", "naam_overledene", "sterfjaar", "sterfplaats",
]


def _download_file(session: requests.Session, url: str, dest: Path, retries: int = 3) -> str:
    if dest.exists() and dest.stat().st_size > 0:
        return "exists"
    delay = 2
    for attempt in range(retries):
        try:
            with session.get(url, stream=True, timeout=120) as response:
                if response.status_code == 404:
                    return "missing"
                if response.status_code in (502, 503, 504):
                    if attempt < retries - 1:
                        print(f"    {response.status_code} on attempt {attempt + 1}, retrying in {delay}s …")
                        time.sleep(delay)
                        delay *= 2
                        continue
                    return "error"
                response.raise_for_status()
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in response.iter_content(1024 * 64):
                        if chunk:
                            f.write(chunk)
                return "downloaded"
        except Exception as exc:
            if attempt < retries - 1:
                print(f"    error on attempt {attempt + 1}: {exc}, retrying in {delay}s …")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"    failed after {retries} attempts: {exc}")
                return "error"
    return "error"


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


def _load_checkpoint() -> dict | None:
    """Load checkpoint if it exists."""
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def _save_checkpoint(data: dict) -> None:
    """Save checkpoint to disk for resumability."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args([])

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    output_dir = Path(args.output_dir)
    written_sidecars: set[Path] = set()

    # Count total rows for progress tracking
    with open(args.input, newline="", encoding="utf-8") as f:
        total_rows = sum(1 for _ in csv.DictReader(f))

    start_time = time.time()
    download_counts = {"exists": 0, "downloaded": 0, "missing": 0, "error": 0}
    current_row = 0

    # Try to load checkpoint
    checkpoint = _load_checkpoint()
    if checkpoint:
        current_row = checkpoint.get("last_row", 0)
        print(f"  Resuming from checkpoint: row {current_row}")

    for row in _iter_rows(args.input):
        current_row += 1
        if current_row <= checkpoint.get("last_row", 0) if checkpoint else 0:
            continue

        archive = row.get("archive", "").strip()
        record_id = row.get("record_id", "").strip()
        page_seq = row.get("page_seq", "").strip() or "1"
        scan_uri = row.get("scan_uri", "").strip()
        if not archive or not record_id or not scan_uri:
            continue
        suffix = Path(scan_uri.split("?", 1)[0]).suffix or ".jpg"
        record_dir = output_dir / archive / record_id
        dest = record_dir / f"{page_seq}{suffix}"
        status = _download_file(session, scan_uri, dest)
        download_counts[status] = download_counts.get(status, 0) + 1

        if record_dir not in written_sidecars:
            _write_metadata_sidecar(record_dir, row)
            written_sidecars.add(record_dir)

        # Progress logging every 50 downloads
        if (download_counts["downloaded"] + download_counts["exists"] + download_counts["missing"]) % 50 == 0:
            elapsed = time.time() - start_time
            rate = current_row / elapsed if elapsed > 0 else 0
            downloaded = download_counts["downloaded"]
            exists = download_counts["exists"]
            missing = download_counts["missing"]
            errors = download_counts["error"]
            print(
                f"  progress: {current_row}/{total_rows} ({100*current_row/total_rows:.1f}%) "
                f"| downloaded={downloaded} exists={exists} missing={missing} errors={errors} "
                f"| {rate:.1f} rows/s",
                flush=True,
            )
            _save_checkpoint({"last_row": current_row})

    elapsed = time.time() - start_time
    print(f"  Step 3 complete: downloaded={download_counts['downloaded']} exists={download_counts['exists']} missing={download_counts['missing']} errors={download_counts.get('error', 0)} in {elapsed:.1f}s")

    # Clear checkpoint on success
    if Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()


if __name__ == "__main__":
    main()
