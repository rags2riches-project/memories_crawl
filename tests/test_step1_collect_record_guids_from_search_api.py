from __future__ import annotations

import csv

from python import step1_collect_record_guids_from_search_api as step1


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, payloads):
        self.payloads = payloads
        self.headers = {}
        self.calls = []

    def get(self, url, params=None, timeout=None):
        archive = params["archive"]
        offset = params["offset"]
        self.calls.append((url, archive, offset, timeout))
        return DummyResponse(self.payloads[(archive, offset)])


def test_step1_writes_two_records_per_archive(monkeypatch, tmp_path):
    payloads = {}
    for archive in step1.ARCHIVES:
        payloads[(archive, 0)] = {
            "response": {
                "docs": [
                    {"identifier": f"{archive}-1"},
                    {"Source": {"Identifier": f"{archive}-2"}},
                    {"identifier": f"{archive}-3"},
                ]
            }
        }

    session = DummySession(payloads)
    monkeypatch.setattr(step1.requests, "Session", lambda: session)
    monkeypatch.setattr(step1.time, "sleep", lambda _seconds: None)

    output = tmp_path / "records.csv"
    step1.main(str(output))

    with output.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == len(step1.ARCHIVES) * 2
    assert rows[0] == {
        "archive": step1.ARCHIVES[0],
        "record_id": f"{step1.ARCHIVES[0]}-1",
        "url": f"https://www.openarchieven.nl/{step1.ARCHIVES[0]}:{step1.ARCHIVES[0]}-1",
    }
    assert rows[1]["record_id"] == f"{step1.ARCHIVES[0]}-2"
    assert all(call[0] == step1.BASE_URL for call in session.calls)
