from __future__ import annotations

import argparse
import codecs
import csv
import gzip
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable, Iterator
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


def _extract_identifier(record_root: ET.Element) -> str:
    return _find_first_text(record_root, [
        ".//{*}RecordGUID",
        ".//{*}Identifier",
        ".//{*}identifier",
        ".//{*}GUID",
        ".//{*}guid",
        ".//{*}RecordId",
        ".//{*}id",
    ])


def _extract_metadata(record_root: ET.Element) -> dict[str, str]:
    """Extract rich metadata fields from an A2A record element."""
    meta: dict[str, str] = {}

    meta["brontype"] = _find_first_text(record_root, [".//{*}SourceType", ".//{*}sourceType"])
    meta["gemeente"] = _find_first_text(record_root, [".//{*}SourcePlace", ".//{*}Place"])
    meta["archief_naam"] = _find_first_text(record_root, [
        ".//{*}SourceReference/{*}Archive",
        ".//{*}Archive",
    ])
    meta["inventarisnummer"] = _find_first_text(record_root, [
        ".//{*}SourceReference/{*}RegistrationNumber",
        ".//{*}RegistrationNumber",
    ])
    meta["deel"] = _find_first_text(record_root, [
        ".//{*}SourceReference/{*}Volume",
        ".//{*}Volume",
    ])
    meta["jaar"] = _find_first_text(record_root, [
        ".//{*}SourceDate/{*}Year",
        ".//{*}SourceDate/{*}From/{*}Year",
    ])
    meta["kantoor"] = _find_first_text(record_root, [
        ".//{*}SourceReference/{*}Place",
        ".//{*}SourceReference/{*}InstitutionName",
    ])

    # Find the first deceased person's last name
    for person_elem in record_root.findall(".//{*}Person"):
        role = _find_first_text(person_elem, [".//{*}RelationEP"])
        if not role or "overlede" in role.lower() or "deceased" in role.lower() or role == "":
            last_name = _find_first_text(person_elem, [".//{*}PersonNameLastName"])
            first_name = _find_first_text(person_elem, [".//{*}PersonNameFirstName"])
            if last_name or first_name:
                meta["naam_overledene"] = f"{first_name} {last_name}".strip()
                break

    # Death event date
    for event_elem in record_root.findall(".//{*}Event"):
        event_type = _find_first_text(event_elem, [".//{*}EventType"])
        if "overlijden" in event_type.lower() or "death" in event_type.lower() or not event_type:
            meta["sterfjaar"] = _find_first_text(event_elem, [".//{*}EventDate/{*}Year"])
            meta["sterfplaats"] = _find_first_text(event_elem, [".//{*}EventPlace"])
            if meta.get("sterfjaar"):
                break

    return meta


def _sanitize_xml_text(text: str) -> str:
    return "".join(
        ch for ch in text
        if ch in "\t\n\r" or ord(ch) >= 0x20
    )


def _iter_sanitized_xml_chunks(path: Path, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    with gzip.open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            text = decoder.decode(chunk)
            text = _sanitize_xml_text(text).replace("\ufffd", "")
            if text:
                yield text.encode("utf-8")
        tail = decoder.decode(b"", final=True)
        tail = _sanitize_xml_text(tail).replace("\ufffd", "")
        if tail:
            yield tail.encode("utf-8")


def _iter_a2a_records(path: Path) -> Iterable[ET.Element]:
    parser = ET.XMLPullParser(events=["end"])
    for chunk in _iter_sanitized_xml_chunks(path):
        parser.feed(chunk)
        for event, elem in parser.read_events():
            if event == "end" and elem.tag.endswith("A2A"):
                yield elem
                elem.clear()


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
    match = re.search(rf'href="([^"]*{re.escape(archive)}[^"]*\.xml(?:\.gz)?)"', page.text, re.I)
    if not match:
        raise FileNotFoundError(f"No dump URL found for archive {archive}")
    return urljoin(BASE_EXPORT_URL, match.group(1))


METADATA_COLS = [
    "brontype", "gemeente", "archief_naam", "inventarisnummer",
    "deel", "jaar", "kantoor", "naam_overledene", "sterfjaar", "sterfplaats",
]


def main(output_file: str = DEFAULT_OUTPUT_FILE) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=output_file)
    parser.add_argument("--dumps-dir", default="dumps")
    parser.add_argument("--limit-per-archive", type=int, default=0)
    parser.add_argument("--archives", nargs="+", choices=ARCHIVES)
    args = parser.parse_args([])

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    dumps_dir = Path(args.dumps_dir)
    dumps_dir.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archive", "record_id", "page_seq", "scan_uri"] + METADATA_COLS)
        archives = args.archives or ARCHIVES
        for archive in archives:
            dump_path = dumps_dir / f"{archive}.xml.gz"
            if not dump_path.exists():
                _download(_archive_dump_url(session, archive), dump_path, session)
            count = 0
            for record in _iter_a2a_records(dump_path):
                source_type = _find_first_text(record, [".//{*}SourceType", ".//{*}sourceType"])
                if SOURCETYPE.lower() not in source_type.lower():
                    continue
                # Exclude Tafel V-bis
                if "tafel" in source_type.lower() or "v-bis" in source_type.lower():
                    continue
                identifier = _extract_identifier(record)
                uris = _extract_scan_uris(record)
                if not uris:
                    continue
                meta = _extract_metadata(record)
                meta_values = [meta.get(col, "") for col in METADATA_COLS]
                for idx, uri in enumerate(uris, start=1):
                    writer.writerow([archive, identifier, idx, uri] + meta_values)
                count += 1
                if args.limit_per_archive and count >= args.limit_per_archive:
                    break


if __name__ == "__main__":
    main()
