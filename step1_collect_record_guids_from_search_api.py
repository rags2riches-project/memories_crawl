from __future__ import annotations

import csv
import os
import time
from typing import Any

import requests

ARCHIVES = ["bhi", "zar", "frl", "rhl", "hua", "gra", "nha"]
BASE_URL = "https://api.openarch.nl/1.1/records/search.php"
PAGE_SIZE = 100
OUTPUT_FILE = "records.csv"
SOURCETYPE = "Memories van Successie"
USER_AGENT = os.getenv("OPENARCH_USER_AGENT", "memories-crawl/1.0")


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


def fetch_page(session: requests.Session, archive: str, offset: int) -> dict[str, Any]:
    params = {
        "archive": archive,
        "name": "*",
        "sourcetype": SOURCETYPE,
        "number": PAGE_SIZE,
        "offset": offset,
        "format": "json",
    }
    response = session.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    seen: set[tuple[str, str]] = set()
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archive", "record_id", "url"])

        for archive in ARCHIVES:
            offset = 0
            total = None
            while True:
                payload = fetch_page(session, archive, offset)
                if total is None:
                    total = _get_total(payload)
                docs = _docs(payload)
                if not docs:
                    break
                for record in docs[:2]:
                    record_id = _record_id(record)
                    if not record_id:
                        continue
                    key = (archive, record_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow([archive, record_id, _record_url(archive, record_id)])
                break
            time.sleep(0.26)


if __name__ == "__main__":
    main()
