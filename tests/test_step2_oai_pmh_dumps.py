from __future__ import annotations

import csv
import gzip

import step2_oai_pmh_dumps as step2


def test_step2_sanitizes_and_extracts_half_scan_urls(tmp_path):
    xml = b'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<A2ACollection xmlns="http://Mindbus.nl/RecordCollectionA2A" xmlns:a2a="http://Mindbus.nl/A2A">
  <a2a:A2A>
    <a2a:SourceType>Memories van Successie</a2a:SourceType>
    <a2a:Person><a2a:BirthPlace><a2a:Place>Boechout (Belgi\xeb)</a2a:Place></a2a:BirthPlace></a2a:Person>
    <a2a:RecordGUID>rec-1</a2a:RecordGUID>
    <a2a:SourceAvailableScans>
      <a2a:Scan><a2a:Uri>https://example.test/1.jpg</a2a:Uri></a2a:Scan>
      <a2a:Scan><a2a:Uri>https://example.test/2.jpg</a2a:Uri></a2a:Scan>
      <a2a:Scan><a2a:Uri>https://example.test/3.jpg</a2a:Uri></a2a:Scan>
      <a2a:Scan><a2a:Uri>https://example.test/4.jpg</a2a:Uri></a2a:Scan>
    </a2a:SourceAvailableScans>
  </a2a:A2A>
  <a2a:A2A>
    <a2a:SourceType>Other source</a2a:SourceType>
    <a2a:RecordGUID>rec-ignored</a2a:RecordGUID>
    <a2a:SourceAvailableScans>
      <a2a:Scan><a2a:Uri>https://example.test/ignored.jpg</a2a:Uri></a2a:Scan>
    </a2a:SourceAvailableScans>
  </a2a:A2A>
</A2ACollection>'''
    dump_path = tmp_path / "bhi.xml.gz"
    with gzip.open(dump_path, "wb") as f:
        f.write(xml)

    records = list(step2._iter_a2a_records(dump_path))
    assert len(records) == 2

    output = tmp_path / "scan_urls.csv"
    dumps_dir = tmp_path
    argv = [
        "step2_oai_pmh_dumps.py",
        "--output",
        str(output),
        "--dumps-dir",
        str(dumps_dir),
        "--archives",
        "bhi",
        "--limit-per-archive",
        "1",
    ]

    import sys

    old_argv = sys.argv
    try:
        sys.argv = argv
        step2.main()
    finally:
        sys.argv = old_argv

    with output.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert rows == [
        {
            "archive": "bhi",
            "record_id": "rec-1",
            "page_seq": "1",
            "scan_uri": "https://example.test/1.jpg",
        },
        {
            "archive": "bhi",
            "record_id": "rec-1",
            "page_seq": "2",
            "scan_uri": "https://example.test/2.jpg",
        },
    ]
