from __future__ import annotations

import argparse
import csv
import gzip
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests

ARCHIVES = ["bhi", "zar", "frl", "rhl", "hua", "gra", "nha"]
BASE_EXPORT_URL = "https://www.openarchieven.nl/exports/"
DEFAULT_OUTPUT_FILE = "scan_urls.csv"
TEST_OUTPUT_FILE = "test_results/step2/scan_urls_test.csv"
SOURCETYPE = "Memories van Successie"
USER_AGENT = os.getenv("OPENARCH_USER_AGENT", "memories-crawl/1.0")
NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "a2a": "http://Mindbus.nl/A2A",
}


def _text(node: ET.Element | None) -> str:
    return "" if node is None or node.text is None else node.text.strip()


def _find_first_text(root: ET.Element, paths: Iterable[str]) -> str:
    for path in paths:
        value = _text(root.find(path, NS))
        if value:
            return value
    return ""


def _extract_scan_uris(record_root: ET.Element) -> list[str]:
    uris: list[str] = []
    for elem in record_root.findall(".//{*}SourceAvailableScans//{*}Uri"):
        value = _text(elem)
        if value:
            uris.append(value)
    if not uris:
        for elem in record_root.findall(".//{*}Scan//{*}Uri"):
            value = _text(elem)
            if value:
                uris.append(value)
    seen: set[str] = set()
    out: list[str] = []
    for uri in uris:
        if uri not in seen:
            seen.add(uri)
            out.append(uri)
    return out


def _iter_records(xml_root: ET.Element) -> Iterable[ET.Element]:
    yield from xml_root.findall(".//{*}Record")


def _parse_xml_bytes(xml_bytes: bytes) -> ET.Element:
    return ET.fromstring(xml_bytes)


def _find_records_from_xml(root_xml: ET.Element, archive: str) -> list[tuple[str, list[str]]]:
    records: list[tuple[str, list[str]]] = []
    for rec in _iter_records(root_xml):
        source_type = _find_first_text(rec, [".//{*}SourceType", ".//{*}sourceType"])
        if SOURCETYPE.lower() not in source_type.lower():
            continue
        identifier = _find_first_text(rec, [".//{*}Identifier", ".//{*}identifier", ".//{*}GUID", ".//{*}guid", ".//{*}RecordId", ".//{*}id"])
        uris = _extract_scan_uris(rec)
        if identifier and uris:
            records.append((identifier, uris))
    return records


def _load_dump_xml(path: Path) -> ET.Element:
    with gzip.open(path, "rb") as f:
        return _parse_xml_bytes(f.read())
def _download(url: str, dest: Path, session: requests.Session) -> None:
    with session.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)


def _archive_dump_url(session: requests.Session, archive: str) -> str:
    page = session.get(f"{BASE_EXPORT_URL}xml/", timeout=60)
    page.raise_for_status()
    match = re.search(rf'href="([^"]*{re.escape(archive)}[^"<>]*\.xml\.gz)"', page.text, re.I)
    if not match:
        raise FileNotFoundError(f"No dump URL found for archive {archive}")
    return urljoin(BASE_EXPORT_URL, match.group(1))



def main(output_file: str = DEFAULT_OUTPUT_FILE) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=output_file)
    parser.add_argument("--dumps-dir", default="dumps")
    parser.add_argument("--limit-per-archive", type=int, default=0)
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    dumps_dir = Path(args.dumps_dir)
    dumps_dir.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archive", "record_id", "page_seq", "scan_uri"])
        for archive in ARCHIVES:
            dump_path = dumps_dir / f"{archive}.xml.gz"
            if not dump_path.exists():
                _download(_archive_dump_url(session, archive), dump_path, session)
            root_xml = _load_dump_xml(dump_path)
            count = 0
            for record_id, uris in _find_records_from_xml(root_xml, archive):
                for idx, uri in enumerate(uris, start=1):
                    writer.writerow([archive, record_id, idx, uri])
                count += 1
                if args.limit_per_archive and count >= args.limit_per_archive:
                    break



if __name__ == "__main__":
    main()
