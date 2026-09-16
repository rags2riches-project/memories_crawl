"""Tests for the shared download pool (issue #26).

No network: every fetch is a stub.  The points worth pinning down are the ones
concurrency makes fragile — one session per worker, a rate ceiling the workers
share, a 429 that stops *everyone*, counters that survive parallel updates, and
``--workers 1`` still walking the jobs in order through one session.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import pytest

from memories_crawl import download


class FakeClock:
    """Monotonic clock whose only movement is the sleeps we hand it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _limiter(
    rate: float | None, burst: float | None = None
) -> tuple[download.RateLimiter, FakeClock]:
    clock = FakeClock()
    return download.RateLimiter(rate, burst, clock=clock.time, sleep=clock.sleep), clock


class FakeResponse:
    def __init__(self, status_code: int = 200, headers: dict | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}


class FakeSession:
    """Stands in for ``requests.Session`` — records what it was asked for."""

    def __init__(self, response: FakeResponse | None = None) -> None:
        self.response = response or FakeResponse()
        self.urls: list[str] = []
        self.closed = False

    def get(self, url: str, **kwargs: object) -> FakeResponse:
        self.urls.append(url)
        return self.response

    def close(self) -> None:
        self.closed = True


def _jobs(n: int, tmp_path: Path) -> list[download.Job]:
    return [
        download.Job(f"https://example.invalid/{i}", tmp_path / f"{i:04d}.jpg") for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


def test_every_worker_gets_its_own_session(tmp_path):
    """requests.Session is not thread-safe: no session may be seen by two threads."""
    seen: list[tuple[int, int]] = []
    barrier = threading.Barrier(4)
    lock = threading.Lock()

    def fetch(session, url, dest):
        # Hold every worker here so all four are in flight at once.
        barrier.wait(timeout=10)
        with lock:
            seen.append((threading.get_ident(), id(session)))
        return "downloaded"

    with download.Downloader(fetch, workers=4, session_factory=FakeSession) as dl:
        dl.run(_jobs(16, tmp_path))

    by_session: dict[int, set[int]] = {}
    for thread_id, session_id in seen:
        by_session.setdefault(session_id, set()).add(thread_id)
    assert all(len(threads) == 1 for threads in by_session.values()), by_session
    assert len(by_session) > 1, "the pool never actually ran on more than one thread"


def test_one_worker_uses_a_single_session(tmp_path):
    sessions: list[int] = []

    def fetch(session, url, dest):
        sessions.append(id(session))
        return "downloaded"

    with download.Downloader(fetch, workers=1, session_factory=FakeSession) as dl:
        dl.run(_jobs(5, tmp_path))
        dl.run(_jobs(5, tmp_path))

    assert len(set(sessions)) == 1


def test_close_closes_the_sessions(tmp_path):
    dl = download.Downloader(lambda s, u, d: "downloaded", workers=1, session_factory=FakeSession)
    dl.run(_jobs(1, tmp_path))
    raw = dl.sessions[0]
    dl.close()
    assert raw.closed


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


def test_rate_limiter_bounds_the_request_rate():
    """At 10 req/s, ten requests may not start in under 0.9 s of clock time."""
    limiter, clock = _limiter(10.0)
    for _ in range(10):
        limiter.acquire()
    assert clock.now == pytest.approx(0.9, abs=1e-6)  # the first token is free


def test_rate_limiter_burst_allows_a_head_start():
    limiter, clock = _limiter(10.0, burst=5)
    for _ in range(5):
        limiter.acquire()
    assert clock.now == 0.0
    limiter.acquire()
    assert clock.now == pytest.approx(0.1, abs=1e-6)


def test_rate_limiter_without_a_rate_never_waits():
    limiter, clock = _limiter(None)
    for _ in range(100):
        limiter.acquire()
    assert clock.sleeps == []


def test_rate_limiter_is_shared_across_threads():
    """Two threads draw from one bucket, so the ceiling is global, not per worker."""
    limiter, clock = _limiter(10.0)
    lock = threading.Lock()

    def pull() -> None:
        for _ in range(5):
            limiter.acquire()
            with lock:
                pass

    threads = [threading.Thread(target=pull) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    # Ten requests through one bucket: nine of them had to wait 0.1 s each.
    assert clock.now == pytest.approx(0.9, abs=1e-6)


# ---------------------------------------------------------------------------
# 429 backoff
# ---------------------------------------------------------------------------


def test_429_backs_off_every_worker_not_just_the_one_that_saw_it():
    limiter, clock = _limiter(None)
    unlucky = download._ThrottledSession(
        FakeSession(FakeResponse(429, {"Retry-After": "12"})), limiter
    )
    bystander = download._ThrottledSession(FakeSession(), limiter)

    unlucky.get("https://example.invalid/a")
    assert clock.sleeps == []  # the 429 itself does not sleep, it arms the pause

    bystander.get("https://example.invalid/b")
    assert clock.sleeps == [pytest.approx(12.0)]


def test_429_without_retry_after_uses_the_default_pause():
    limiter, clock = _limiter(None)
    sess = download._ThrottledSession(FakeSession(FakeResponse(429)), limiter)
    sess.get("https://example.invalid/a")
    download._ThrottledSession(FakeSession(), limiter).get("https://example.invalid/b")
    assert clock.sleeps == [pytest.approx(download.DEFAULT_RETRY_AFTER)]


def test_a_normal_response_arms_no_backoff():
    limiter, clock = _limiter(None)
    sess = download._ThrottledSession(FakeSession(FakeResponse(200)), limiter)
    sess.get("https://example.invalid/a")
    sess.get("https://example.invalid/b")
    assert clock.sleeps == []


def test_throttled_session_delegates_everything_else():
    limiter, _clock = _limiter(None)
    raw = FakeSession()
    sess = download._ThrottledSession(raw, limiter)
    sess.close()
    assert raw.closed


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------


def test_counters_aggregate_across_workers(tmp_path):
    statuses = ["downloaded", "exists", "missing", "failed"]

    def fetch(session, url, dest):
        return statuses[int(dest.stem) % len(statuses)]

    with download.Downloader(fetch, workers=8, session_factory=FakeSession) as dl:
        counts = dl.run(_jobs(400, tmp_path))

    assert counts == Counter({s: 100 for s in statuses})
    assert download.tally(counts) == (100, 100, 200)


def test_on_result_sees_every_job_and_may_touch_shared_state(tmp_path):
    """on_result runs under the pool's lock, so plain dict updates are safe."""
    per_key: Counter[str] = Counter()

    def fetch(session, url, dest):
        return "downloaded"

    jobs = [
        download.Job(f"https://example.invalid/{i}", tmp_path / f"{i}.jpg", key=f"k{i % 5}")
        for i in range(250)
    ]
    with download.Downloader(fetch, workers=8, session_factory=FakeSession) as dl:
        dl.run(jobs, on_result=lambda job, status: per_key.update([job.key]))

    assert per_key == Counter({f"k{i}": 50 for i in range(5)})


def test_empty_batch_is_a_no_op(tmp_path):
    def fetch(session, url, dest):  # pragma: no cover - must never run
        raise AssertionError("fetch called for an empty batch")

    with download.Downloader(fetch, workers=4, session_factory=FakeSession) as dl:
        assert dl.run([]) == Counter()


def test_a_failing_fetch_still_propagates(tmp_path):
    def fetch(session, url, dest):
        raise RuntimeError("boom")

    with download.Downloader(fetch, workers=4, session_factory=FakeSession) as dl:
        with pytest.raises(RuntimeError, match="boom"):
            dl.run(_jobs(8, tmp_path))


# ---------------------------------------------------------------------------
# --workers 1 == the pre-#26 sequential path
# ---------------------------------------------------------------------------


def test_one_worker_is_the_old_sequential_path(tmp_path):
    """Same thread as the caller, jobs walked in order, one request at a time."""
    order: list[str] = []
    threads: set[int] = set()

    def fetch(session, url, dest):
        order.append(url)
        threads.add(threading.get_ident())
        return "downloaded"

    jobs = _jobs(20, tmp_path)
    with download.Downloader(fetch, workers=1, session_factory=FakeSession) as dl:
        dl.run(jobs)

    assert order == [j.url for j in jobs]
    assert threads == {threading.get_ident()}


def test_one_worker_keeps_the_old_pace(tmp_path):
    """The token bucket replaces the old fixed sleep at the same rate."""
    limiter, clock = _limiter(1 / 0.15)

    def fetch(session, url, dest):
        session.get(url, stream=True)
        return "downloaded"

    with download.Downloader(fetch, workers=1, session_factory=FakeSession, limiter=limiter) as dl:
        dl.run(_jobs(6, tmp_path))

    # Six images at the old time.sleep(0.15) pace: five gaps, the first is free.
    assert clock.now == pytest.approx(5 * 0.15, abs=1e-6)


def test_results_are_recorded_in_submission_order(tmp_path):
    """Workers interleave, but bookkeeping stays deterministic."""
    seen: list[str] = []

    def fetch(session, url, dest):
        # Make the later jobs finish first.
        time.sleep(0.02 if url.endswith("/0") else 0.0)
        return "downloaded"

    jobs = _jobs(8, tmp_path)
    with download.Downloader(fetch, workers=4, session_factory=FakeSession) as dl:
        dl.run(jobs, on_result=lambda job, status: seen.append(job.url))

    assert seen == [j.url for j in jobs]


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def test_every_pipeline_main_accepts_workers():
    import importlib
    import inspect

    for archive in [
        "friesland",
        "nationaalarchief",
        "drentsarchief",
        "bhic",
        "overijssel",
        "utrechtsarchief",
        "limburg",
        "noordholland",
        "zeeland",
        "gelderland",
    ]:
        mod = importlib.import_module(f"memories_crawl.{archive}")
        params = inspect.signature(mod.main).parameters
        assert "workers" in params, archive
        assert params["workers"].default == download.DEFAULT_WORKERS, archive


def test_cli_exposes_workers():
    result = subprocess.run(
        [sys.executable, "-m", "memories_crawl", "--help"],
        capture_output=True,
        text=True,
    )
    assert "--workers" in result.stdout
