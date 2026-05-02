from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any

import requests

ARCHIVES = ["bhi", "zar", "frl", "rhl", "hua", "gra", "nha"]
BASE_URL = "https://api.openarch.nl/1.1/records/search.php"
PAGE_SIZE = 100
OUTPUT_FILE = "records.csv"
CHECKPOINT_FILE = "step1_checkpoint.json"
SOURCETYPE = "Memories van Successie"
USER_AGENT = os.getenv("OPENARCH_USER_AGENT", "memories-crawl/1.0")
PROGRESS_LOG_INTERVAL = 100  # Log every N records


def _docs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    response = payload.get("response")
    if isinstance(response, dict):
        docs = response.get("docs")
        if isinstance(docs, list):
            return [x for x in docs if isinstance(x, dict)]
    for key in ("results", "records", "persons", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _get_total(payload: dict[str, Any]) -> int | None:
    response = payload.get("response")
    if isinstance(response, dict):
        value = response.get("number_found")
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    for key in ("total", "count", "hits"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _record_id(record: dict[str, Any]) -> str:
    for key in ("identifier", "id", "guid", "record_id"):
        value = record.get(key)
        if value:
            return str(value)
    source = record.get("Source")
    if isinstance(source, dict):
        for key in ("Identifier", "id", "GUID"):
            value = source.get(key)
            if value:
                return str(value)
    return ""


def _record_url(archive: str, record_id: str) -> str:
    return f"https://www.openarchieven.nl/{archive}:{record_id}" if record_id else ""


def fetch_page(session: requests.Session, archive: str, offset: int, retries: int = 3) -> dict[str, Any]:
    params = {
        "archive": archive,
        "name": "*",
        "sourcetype": SOURCETYPE,
        "number": PAGE_SIZE,
        "offset": offset,
        "format": "json",
    }
    delay = 5
    for attempt in range(retries):
        try:
            response = session.get(BASE_URL, params=params, timeout=60)
        except requests.exceptions.ConnectionError as exc:
            if attempt < retries - 1:
                print(f"    connection error on attempt {attempt + 1}, retrying in {delay}s … ({exc})", flush=True)
                time.sleep(delay)
                delay *= 2
                continue
            raise
        if response.status_code == 429:
            time.sleep(delay)
            delay *= 2
            continue
        if response.status_code in (502, 503, 504) and attempt < retries - 1:
            print(f"    {response.status_code} on attempt {attempt + 1}, retrying in {delay}s …", flush=True)
            time.sleep(delay)
            delay *= 2
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"fetch_page failed after {retries} attempts for archive={archive} offset={offset}")


def _load_checkpoint() -> dict[str, Any] | None:
    """Load checkpoint if it exists and matches current configuration."""
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def _save_checkpoint(data: dict[str, Any]) -> None:
    """Save checkpoint to disk for resumability."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main(output_file: str = OUTPUT_FILE) -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    # Try to load checkpoint
    checkpoint = _load_checkpoint()
    start_archive_idx = 0
    start_offset = 0
    seen: set[tuple[str, str]] = set()

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archive", "record_id", "url"])

        start_time = time.time()
        total_records = 0

        if checkpoint:
            print(f"  Resuming from checkpoint: archive={checkpoint.get('last_archive')}, offset={checkpoint.get('last_offset')}")
            # Find starting archive index
            try:
                start_archive_idx = ARCHIVES.index(checkpoint.get("last_archive", ""))
            except ValueError:
                start_archive_idx = 0
            start_offset = checkpoint.get("last_offset", 0)
            seen = set((tuple(x) for x in checkpoint.get("seen", [])))

        for archive_idx, archive in enumerate(ARCHIVES[start_archive_idx:], start=start_archive_idx):
            offset = start_offset if archive_idx == start_archive_idx else 0

            while True:
                payload = fetch_page(session, archive, offset)
                docs = _docs(payload)
                if not docs:
                    break

                for record in docs:
                    record_id = _record_id(record)
                    if not record_id:
                        continue
                    key = (archive, record_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow([archive, record_id, _record_url(archive, record_id)])
                    total_records += 1

                # Progress logging
                total = _get_total(payload)
                offset += len(docs)

                elapsed = time.time() - start_time
                rate = offset / elapsed if elapsed > 0 else 0  # Use offset for API rate
                progress_pct = (offset / total * 100) if total else 0
                print(
                    f"  {archive}: offset={offset}/{total or '?'} ({progress_pct:.1f}%) "
                    f"| unique_records={total_records} api_calls={offset // PAGE_SIZE} "
                    f"| {rate:.1f} pages/s",
                    flush=True,
                )

                # Save checkpoint periodically
                if total_records % PROGRESS_LOG_INTERVAL == 0:
                    _save_checkpoint({
                        "last_archive": archive,
                        "last_offset": offset,
                        "seen": [list(k) for k in seen],
                        "total_records": total_records,
                        "start_time": start_time,
                    })

                if total is not None and offset >= total:
                    break
                time.sleep(0.5)
            time.sleep(1.5)

    # Clear checkpoint on success
    if Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()
    print(f"  Step 1 complete: {total_records} records collected from {len(ARCHIVES)} archives")


if __name__ == "__main__":
    main()