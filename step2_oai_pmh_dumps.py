from __future__ import annotations

import argparse
import csv
import os
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests

ARCHIVES = ["bhi", "zar", "frl", "rhl", "hua", "gra", "nha"]
BASE_EXPORT_URL = "https://www.openarchieven.nl/exports/"
DEFAULT_OUTPUT_FILE = "scan_urls.csv"
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


def _iter_xml_files_from_tar(tar_path: Path) -> Iterable[tuple[str, bytes]]:
    with tarfile.open(tar_path, "r:*") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.lower().endswith((".xml", ".xml.gz", ".xml.zip")):
                extracted = tar.extractfile(member)
                if extracted is not None:
                    yield member.name, extracted.read()


def _download(url: str, dest: Path, session: requests.Session) -> None:
    with session.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)


def _archive_dump_url(session: requests.Session, archive: str) -> str:
    page = session.get(BASE_EXPORT_URL, timeout=60)
    page.raise_for_status()
    return urljoin(BASE_EXPORT_URL, f"{archive}.tar.gz")



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--dumps-dir", default="dumps")
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    dumps_dir = Path(args.dumps_dir)
    dumps_dir.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archive", "record_id", "page_seq", "scan_uri"])
        for archive in ARCHIVES:
            dump_path = dumps_dir / f"{archive}.tar.gz"
            if not dump_path.exists():
                _download(_archive_dump_url(session, archive), dump_path, session)
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                with tarfile.open(dump_path, "r:*") as tar:
                    tar.extractall(tmp_path)
                for xml_file in sorted(tmp_path.rglob("*.xml")):
                    try:
                        root = ET.parse(xml_file).getroot()
                    except ET.ParseError:
                        continue
                    for rec in root.findall(".//{*}Record"):
                        source_type = _find_first_text(rec, [".//{*}SourceType", ".//{*}sourceType"])
                        if SOURCETYPE.lower() not in source_type.lower():
                            continue
                        identifier = _find_first_text(rec, [".//{*}Identifier", ".//{*}identifier", ".//{*}GUID", ".//{*}guid", ".//{*}RecordId", ".//{*}id"])
                        uris = _extract_scan_uris(rec)
                        for idx, uri in enumerate(uris, start=1):
                            writer.writerow([archive, identifier, idx, uri])


if __name__ == "__main__":
    main()
