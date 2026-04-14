from __future__ import annotations

import csv

import step3_download_steps as step3


class DummyResponse:
    def __init__(self, body: bytes, status_code: int = 200):
        self.body = body
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code != 404:
            raise RuntimeError(f"unexpected status {self.status_code}")

    def iter_content(self, chunk_size: int):
        for idx in range(0, len(self.body), chunk_size):
            yield self.body[idx : idx + chunk_size]


class DummySession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def get(self, url, stream=None, timeout=None):
        self.calls.append((url, stream, timeout))
        return DummyResponse(b"image-bytes")


def test_step3_downloads_files_from_csv(monkeypatch, tmp_path):
    input_path = tmp_path / "scan_urls.csv"
    with input_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["archive", "record_id", "page_seq", "scan_uri"])
        writer.writerow(["bhi", "rec-1", "1", "https://example.test/scan.jpg?x=1"])

    session = DummySession()
    monkeypatch.setattr(step3.requests, "Session", lambda: session)

    output_dir = tmp_path / "downloads"

    import sys

    old_argv = sys.argv
    try:
        sys.argv = [
            "step3_download_steps.py",
            "--input",
            str(input_path),
            "--output-dir",
            str(output_dir),
        ]
        step3.main()
    finally:
        sys.argv = old_argv

    downloaded = output_dir / "bhi" / "rec-1" / "1.jpg"
    assert downloaded.read_bytes() == b"image-bytes"
    assert session.calls == [("https://example.test/scan.jpg?x=1", True, 120)]
