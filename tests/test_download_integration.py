"""Concurrent downloads preserve filter selection, byte totals and resume files."""

import csv
import json
import threading

import pytest

from memories_crawl import download, drentsarchief, friesland, paths


@pytest.mark.parametrize("mod", [drentsarchief, friesland])
@pytest.mark.parametrize("workers", [1, 4])
def test_register_batch_summary_and_resume(mod, workers, monkeypatch, tmp_path):
    paths.set_out_dir(tmp_path)
    reg = {
        "id": "r1",
        "metadata": {
            "inventarisnummer": "7",
            "gemeente": "Assen",
            "naam": "Memories kantoor Assen",
        },
    }
    deeds = [
        {"id": "d1", "asset": [{"download": "https://example.invalid/new.jpg"}]},
        {"id": "d2", "asset": [{"download": "https://example.invalid/old.jpg"}]},
        {"id": "d3", "asset": [{"download": "https://example.invalid/missing.jpg"}]},
    ]
    persons = [
        {"id": f"p{i}", "deed_id": f"d{i}", "metadata": {"person_display_name": f"P{i}"}}
        for i in range(1, 4)
    ]

    def paginate(session, path, fq, key):
        return {"/register": [reg], "/deed": deeds, "/person": persons}[path]

    calls = []

    def fetch(session, url, dest):
        calls.append(url)
        if "missing" in url:
            return "missing"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"abc")
        return "exists" if "old" in url else "downloaded"

    monkeypatch.setattr(mod, "_paginate", paginate)
    monkeypatch.setattr(mod, "_download_file", fetch)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)
    run = mod.main(invnrs={"7"}, kantoren={"assen"}, workers=workers)
    assert (run.pages.downloaded, run.pages.skipped, run.pages.missing) == (1, 1, 1)
    assert run.pages.bytes_written == 3
    assert run.units == run.registers == 1
    with mod._progress_csv().open() as stream:
        rows = list(csv.DictReader(stream))
    assert all(row["status"] == "done" for row in rows)
    if mod is drentsarchief:
        assert {r["deed_id"]: r["n_scans"] for r in rows} == {
            "d1": "1",
            "d2": "1",
            "d3": "0",
        }
    else:
        assert rows[0]["n_persons"] == "3"
        sidecars = list((tmp_path / "friesland").rglob("metadata.json"))
        assert sorted(json.loads(p.read_text())["n_scans"] for p in sidecars) == [0, 1, 1]
    calls.clear()
    resumed = mod.main(invnrs={"7"}, kantoren={"assen"}, workers=workers)
    assert not calls
    assert resumed.pages.total == 0


def test_failed_batch_waits_for_and_counts_inflight_success(tmp_path):
    started = threading.Event()
    finished = threading.Event()

    def fetch(session, url, dest):
        if url == "fail":
            assert started.wait(5)
            raise RuntimeError("failed")
        started.set()
        dest.write_bytes(b"image")
        finished.set()
        return "downloaded"

    recorded = []
    with download.Downloader(fetch, workers=2) as dl:
        with pytest.raises(RuntimeError, match="failed"):
            dl.run(
                [download.Job("fail", tmp_path / "a"), download.Job("ok", tmp_path / "b")],
                on_result=lambda job, status: recorded.append((job.url, status)),
            )
        assert finished.is_set()
        assert recorded == [("ok", "downloaded")]
