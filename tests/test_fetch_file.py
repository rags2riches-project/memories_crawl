"""Tests for the shared image fetch (:func:`download.fetch_file`).

No network: every response is a stub.  These pin down the three defects the
nine hand-rolled ``_download_file`` helpers shared -- a transient error ending
the whole run, a body that broke mid-stream never being retried, and a
part-written image being mistaken for a complete one on the next resume.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from memories_crawl import download


class Body:
    """A response body that may break part-way through, like a reset transfer."""

    def __init__(self, chunks: list[bytes], fail_after: int | None = None) -> None:
        self.chunks = chunks
        self.fail_after = fail_after

    def __call__(self, size: int):
        for i, chunk in enumerate(self.chunks):
            if self.fail_after is not None and i >= self.fail_after:
                raise requests.exceptions.ChunkedEncodingError("connection broken")
            yield chunk


class Resp:
    def __init__(self, status_code: int = 200, body: Body | None = None) -> None:
        self.status_code = status_code
        self._body = body or Body([b"jpeg"])
        self.headers: dict[str, str] = {}

    def iter_content(self, size: int):
        return self._body(size)


class Session:
    """Replays a scripted list of responses/exceptions, one per ``get``."""

    def __init__(self, *script: object) -> None:
        self.script = list(script)
        self.calls: list[dict] = []

    def get(self, url: str, **kwargs: object):
        self.calls.append({"url": url, **kwargs})
        item = self.script.pop(0) if self.script else Resp()
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture
def sleeps() -> list[float]:
    return []


def _fetch(session: Session, dest: Path, sleeps: list[float], **kw) -> str:
    return download.fetch_file(
        session, "https://example.invalid/x.jpg", dest, sleep=sleeps.append, **kw
    )


def test_writes_the_file_and_reports_downloaded(tmp_path, sleeps):
    dest = tmp_path / "0001.jpg"
    assert _fetch(Session(Resp()), dest, sleeps) == "downloaded"
    assert dest.read_bytes() == b"jpeg"
    assert sleeps == []


def test_existing_file_is_not_refetched(tmp_path, sleeps):
    dest = tmp_path / "0001.jpg"
    dest.write_bytes(b"already here")
    session = Session(Resp())
    assert _fetch(session, dest, sleeps) == "exists"
    assert session.calls == []


def test_missing_status_is_not_retried(tmp_path, sleeps):
    """404 means the archive has no such page; asking again is pointless."""
    session = Session(Resp(404))
    assert _fetch(session, tmp_path / "x.jpg", sleeps) == "missing"
    assert len(session.calls) == 1


def test_202_counts_as_missing_where_the_pipeline_says_so(tmp_path, sleeps):
    """The MAIS archives answer 202 + an SVG placeholder for an untokened page."""
    session = Session(Resp(202))
    status = _fetch(session, tmp_path / "x.jpg", sleeps, missing_statuses=(404, 202))
    assert status == "missing"
    assert len(session.calls) == 1


@pytest.mark.parametrize("exc", [
    requests.exceptions.ConnectionError("Failed to resolve host"),
    requests.exceptions.ConnectTimeout("timed out"),
    requests.exceptions.ChunkedEncodingError("Connection broken: ConnectionResetError(104)"),
])
def test_transient_errors_are_retried_then_succeed(tmp_path, sleeps, exc):
    """A DNS failure, a timeout or a reset must not end the run (issue: #22 kin)."""
    dest = tmp_path / "0001.jpg"
    session = Session(exc, Resp())
    assert _fetch(session, dest, sleeps) == "downloaded"
    assert dest.read_bytes() == b"jpeg"
    assert sleeps == [download.DEFAULT_BACKOFF]


def test_transient_error_exhausts_retries_into_failed_not_an_exception(tmp_path, sleeps):
    """Exhausted retries are a failed *page*, not an exception that kills the run."""
    session = Session(
        requests.exceptions.ConnectionError("down"),
        requests.exceptions.ConnectionError("down"),
        requests.exceptions.ConnectionError("down"),
    )
    assert _fetch(session, tmp_path / "x.jpg", sleeps) == "failed"
    assert len(session.calls) == 3
    assert sleeps == [5.0, 10.0]  # doubling


def test_body_breaking_mid_stream_is_retried(tmp_path, sleeps):
    """The old helpers wrapped only session.get, so a broken body escaped."""
    dest = tmp_path / "0001.jpg"
    broken = Resp(body=Body([b"aa", b"bb", b"cc"], fail_after=1))
    session = Session(broken, Resp(body=Body([b"whole"])))
    assert _fetch(session, dest, sleeps) == "downloaded"
    assert dest.read_bytes() == b"whole"


def test_a_broken_transfer_leaves_no_file_behind(tmp_path, sleeps):
    """A truncated dest would be skipped as "exists" by every later resume."""
    dest = tmp_path / "0001.jpg"
    session = Session(
        Resp(body=Body([b"aa", b"bb"], fail_after=1)),
        Resp(body=Body([b"aa", b"bb"], fail_after=1)),
        Resp(body=Body([b"aa", b"bb"], fail_after=1)),
    )
    assert _fetch(session, dest, sleeps) == "failed"
    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []


def test_retry_statuses_back_off_and_then_succeed(tmp_path, sleeps):
    dest = tmp_path / "0001.jpg"
    session = Session(Resp(503), Resp(429), Resp())
    assert _fetch(session, dest, sleeps) == "downloaded"
    assert sleeps == [5.0, 10.0]


def test_permanent_refusal_fails_without_retrying(tmp_path, sleeps):
    """An expired MAIS token gives 403; repeating it would only be rude."""
    session = Session(Resp(403))
    assert _fetch(session, tmp_path / "x.jpg", sleeps) == "failed"
    assert len(session.calls) == 1
    assert sleeps == []


def test_timeout_and_redirect_options_reach_the_session(tmp_path, sleeps):
    session = Session(Resp())
    _fetch(session, tmp_path / "x.jpg", sleeps, timeout=180, allow_redirects=True)
    assert session.calls[0]["timeout"] == 180
    assert session.calls[0]["allow_redirects"] is True
    assert session.calls[0]["stream"] is True


def test_redirects_are_left_to_requests_when_unset(tmp_path, sleeps):
    session = Session(Resp())
    _fetch(session, tmp_path / "x.jpg", sleeps)
    assert "allow_redirects" not in session.calls[0]


# -- the pipelines are actually wired to it ------------------------------------

PIPELINES = [
    "bhic", "drentsarchief", "friesland", "gelderland", "nationaalarchief",
    "noordholland", "overijssel", "utrechtsarchief", "zeeland",
]


def _download_file_of(name: str):
    import importlib

    return importlib.import_module(f"memories_crawl.{name}")._download_file


@pytest.fixture
def no_backoff(monkeypatch):
    """fetch_file looks time.sleep up late, so patching it here actually works."""
    monkeypatch.setattr(download.time, "sleep", lambda _s: None)


@pytest.mark.parametrize("name", PIPELINES)
def test_pipeline_survives_a_dns_failure(tmp_path, no_backoff, name):
    """The exact failure that ended an Overijssel run: one page lost, not the register.

    Before this, five pipelines let the exception out of Downloader.run and
    every remaining page of the register was forfeited with it.
    """
    session = Session(*[
        requests.exceptions.ConnectionError("Failed to resolve 'preserve2.archieven.nl'")
    ] * 3)
    got = _download_file_of(name)(session, "https://x.invalid/a.jpg", tmp_path / "a.jpg")
    assert got == "failed"


@pytest.mark.parametrize("name", PIPELINES)
def test_pipeline_recovers_from_one_reset(tmp_path, no_backoff, name):
    dest = tmp_path / "a.jpg"
    session = Session(
        requests.exceptions.ChunkedEncodingError("Connection broken: ConnectionResetError(104)"),
        Resp(body=Body([b"scan"])),
    )
    assert _download_file_of(name)(session, "https://x.invalid/a.jpg", dest) == "downloaded"
    assert dest.read_bytes() == b"scan"
