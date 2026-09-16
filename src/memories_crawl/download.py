"""Bounded, polite concurrency for the image-fetch step (issue #26).

Every pipeline used to fetch scans in a sequential ``for`` loop through one
``requests.Session`` with a fixed ``time.sleep`` per image, so throughput was
bounded by round-trip latency rather than by bandwidth.  This module supplies a
small thread pool that the download loops hand their jobs to, and nothing else:
discovery, the Playwright token harvest and metadata writes stay sequential,
where the correctness risk of concurrency would be much higher.

Four things the pool has to get right:

* **One session per worker.**  ``requests.Session`` is not thread-safe, so a
  ``threading.local()`` hands every worker thread its own, built by the
  pipeline's own session factory (User-Agent, Referer, …).
* **A rate limit shared by all workers.**  A :class:`RateLimiter` token bucket
  sits in front of every request, so "N concurrent requests" and "at most R
  requests per second" are independent knobs.  It replaces the old fixed sleep;
  passing the pace the sleep produced keeps ``--workers 1`` as polite as before.
* **Global 429 backoff.**  A rebuff seen by one worker pauses *all* of them
  (:meth:`RateLimiter.penalize`), otherwise concurrency amplifies exactly the
  problem the server is complaining about.
* **Thread-safe aggregation.**  Status counters are summed under a lock, and
  the optional ``on_result`` callback is invoked under that same lock, so
  pipeline bookkeeping need not be thread-safe itself.

``workers=1`` bypasses the pool entirely and runs the jobs in the calling
thread, in order: the strictly sequential path the pipelines had before.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

#: Conservative default: these are small public archives.
DEFAULT_WORKERS = 4

#: How long everyone backs off on a 429 that carries no usable ``Retry-After``.
DEFAULT_RETRY_AFTER = 30.0

# Token accounting is floating-point, so "a whole token" needs a little slack:
# without it a rounding crumb can ask for a sleep too small to move the clock.
_EPSILON = 1e-9


@dataclass(frozen=True)
class Job:
    """One image to fetch.

    ``key`` is opaque to this module; pipelines use it to attribute a result to
    the register, person or invnr the job came from.
    """

    url: str
    dest: Path
    key: Any = None


class RateLimiter:
    """Token bucket shared by every worker of one :class:`Downloader`.

    ``rate`` is the sustained ceiling in requests per second; ``burst`` is how
    many requests may go out back-to-back after an idle stretch (default: one,
    i.e. the same steady pace the old ``time.sleep`` between images produced).
    A ``rate`` of ``None`` or ``<= 0`` means "no limit", which is what the
    pipelines that never slept between images pass.
    """

    def __init__(
        self,
        rate: float | None,
        burst: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.rate = float(rate) if rate and rate > 0 else 0.0
        self.capacity = float(burst) if burst else 1.0
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._tokens = self.capacity
        self._updated = clock()
        self._blocked_until = 0.0

    def _do_sleep(self, seconds: float) -> None:
        # Looked up late so a test (or a pipeline) that patches time.sleep wins.
        (self._sleep or time.sleep)(seconds)

    def acquire(self) -> None:
        """Block until this thread may send one request."""
        while True:
            with self._lock:
                now = self._clock()
                if now >= self._blocked_until:
                    if self.rate <= 0:
                        return
                    self._tokens = min(
                        self.capacity, self._tokens + (now - self._updated) * self.rate
                    )
                    self._updated = now
                    if self._tokens + _EPSILON >= 1.0:
                        self._tokens = max(0.0, self._tokens - 1.0)
                        return
                    wait = (1.0 - self._tokens) / self.rate
                else:
                    wait = self._blocked_until - now
            self._do_sleep(wait)

    def penalize(self, seconds: float = DEFAULT_RETRY_AFTER) -> None:
        """Pause *every* worker for ``seconds`` (called on HTTP 429)."""
        with self._lock:
            self._blocked_until = max(self._blocked_until, self._clock() + seconds)
            self._tokens = 0.0
            self._updated = self._clock()


def tally(counts: Counter[str]) -> tuple[int, int, int]:
    """Split a status counter into ``(downloaded, existing, missing)``.

    Anything that is neither ``downloaded`` nor ``exists`` counts as missing,
    which is how the sequential loops classified statuses before.
    """
    downloaded = counts["downloaded"]
    existing = counts["exists"]
    return downloaded, existing, sum(counts.values()) - downloaded - existing


def _retry_after(resp: requests.Response) -> float:
    """Seconds to wait from a 429's ``Retry-After``, if it carries a usable one."""
    try:
        value = float(resp.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        return DEFAULT_RETRY_AFTER
    return value if value > 0 else DEFAULT_RETRY_AFTER


class _ThrottledSession:
    """One worker's ``requests.Session``, fronted by the shared rate limiter.

    ``get`` waits for a token before every request and reports a 429 back to
    the limiter, so the backoff is global rather than per worker.  Everything
    else is delegated to the wrapped session, which lets the pipelines' own
    ``_download_file`` helpers keep working unchanged.
    """

    def __init__(self, session: requests.Session, limiter: RateLimiter) -> None:
        self._session = session
        self._limiter = limiter

    def get(self, *args: Any, **kwargs: Any) -> requests.Response:
        self._limiter.acquire()
        resp = self._session.get(*args, **kwargs)
        if resp.status_code == 429:
            self._limiter.penalize(_retry_after(resp))
        return resp

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


class Downloader:
    """Runs a pipeline's ``_download_file`` over batches of jobs.

    ``fetch`` keeps each pipeline's own retry/backoff behaviour: it is called
    as ``fetch(session, url, dest)`` and returns the usual status string
    (``downloaded`` / ``exists`` / ``missing`` / ``failed``).  Sessions come
    from ``session_factory`` — once per worker thread, never shared.
    """

    def __init__(
        self,
        fetch: Callable[..., str],
        workers: int = DEFAULT_WORKERS,
        rate: float | None = None,
        session_factory: Callable[[], requests.Session] = requests.Session,
        limiter: RateLimiter | None = None,
    ) -> None:
        self.fetch = fetch
        self.workers = max(1, int(workers))
        self.limiter = limiter if limiter is not None else RateLimiter(rate)
        self._session_factory = session_factory
        self._local = threading.local()
        self._lock = threading.Lock()
        self._pool: ThreadPoolExecutor | None = None
        #: Every session handed out, for tests and for closing them at the end.
        self.sessions: list[requests.Session] = []

    # -- sessions ---------------------------------------------------------

    def session(self) -> _ThrottledSession:
        """This thread's session, created on first use."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            raw = self._session_factory()
            with self._lock:
                self.sessions.append(raw)
            sess = _ThrottledSession(raw, self.limiter)
            self._local.session = sess
        return sess

    # -- running ----------------------------------------------------------

    def run(
        self,
        jobs: Iterable[Job],
        on_result: Callable[[Job, str], None] | None = None,
    ) -> Counter[str]:
        """Fetch ``jobs``, blocking until the batch is done.

        Returns a counter of status strings.  ``on_result`` (if given) is
        called once per job under a lock, so it may mutate shared state.
        """
        jobs = list(jobs)
        counts: Counter[str] = Counter()
        if not jobs:
            return counts

        def record(job: Job, status: str) -> None:
            with self._lock:
                counts[status] += 1
                if on_result is not None:
                    on_result(job, status)

        if self.workers == 1:
            # Strictly sequential, in order: exactly the pre-#26 behaviour.
            for job in jobs:
                record(job, self.fetch(self.session(), job.url, job.dest))
            return counts

        pool = self._ensure_pool()
        futures: dict[Future[str], Job] = {}
        for job in jobs:
            futures[pool.submit(self._fetch_one, job)] = job
        # Iterated in submission order, so results are recorded deterministically
        # however the workers interleave.
        error: Exception | None = None
        for fut in futures:
            if error is not None and fut.cancel():
                continue
            try:
                record(futures[fut], fut.result())
            except Exception as exc:
                # Cancel queued work, but drain and count work already running.
                # The summary must include every file actually written before
                # the first error is propagated to the pipeline.
                if error is None:
                    error = exc
        if error is not None:
            raise error
        return counts

    def _fetch_one(self, job: Job) -> str:
        return self.fetch(self.session(), job.url, job.dest)

    def _ensure_pool(self) -> ThreadPoolExecutor:
        if self._pool is None:
            self._pool = ThreadPoolExecutor(
                max_workers=self.workers, thread_name_prefix="mc-download"
            )
        return self._pool

    # -- teardown ---------------------------------------------------------

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True)
            self._pool = None
        for sess in self.sessions:
            try:
                sess.close()
            except Exception:  # pragma: no cover - a close() failure is noise
                pass
        self.sessions = []

    def __enter__(self) -> Downloader:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
