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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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


#: HTTP statuses that earn another attempt rather than a failure.  A 429 has
#: usually already paused every worker via :meth:`RateLimiter.penalize` by the
#: time it gets here; the extra wait below is small and on the polite side.
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

#: Attempts per image, and the first pause between them (doubled after each).
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 5.0


def mais_original_url(thumb_url: str) -> str:
    """Request the original JPEG using the cached MAIS strip tokens.

    Omitting ``format`` returns a thumbnail. ``format=download`` is the
    viewer's download route, including for preserve URLs ending in ``.jp2``.
    """
    parts = urlsplit(thumb_url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "format"]
    return urlunsplit(parts._replace(query=urlencode([("format", "download"), *query])))


def _valid_image(path: Path, *, jpeg: bool) -> bool:
    """Check the JPEG signature, or nonempty content for other image formats."""
    with path.open("rb") as stream:
        prefix = stream.read(3)
    return prefix == b"\xff\xd8\xff" if jpeg else bool(prefix)


def _discard(path: Path) -> None:
    """Remove a part-file, ignoring the case where it was never created."""
    try:
        path.unlink(missing_ok=True)
    except OSError:  # pragma: no cover - a failed cleanup is not worth raising
        pass


def fetch_file(
    session: requests.Session,
    url: str,
    dest: Path,
    *,
    missing_statuses: Iterable[int] = (404,),
    retries: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    timeout: float = 120,
    allow_redirects: bool | None = None,
    sleep: Callable[[float], None] | None = None,
) -> str:
    """Fetch one image to ``dest``, retrying what is worth retrying.

    Returns the usual status string: ``exists`` when the file is already there,
    ``missing`` for a status in ``missing_statuses`` (the archive has no such
    page), ``downloaded`` on success, ``failed`` when the attempts ran out or
    the server refused permanently.

    This replaces nine near-identical ``_download_file`` helpers that between
    them had three separate defects:

    * **Transient errors killed the whole register.**  Five pipelines had no
      retry at all, so ``raise_for_status`` or a ``ConnectionError`` propagated
      out of :meth:`Downloader.run` and ended the run -- one momentary DNS
      failure or connection reset forfeited every remaining page.  Here an
      exhausted retry is a ``failed`` page, counted in the summary, and the run
      carries on.
    * **A broken body escaped the retry.**  Even the three pipelines that did
      retry only wrapped ``session.get``; the streaming read that follows sat
      outside the ``try``, so a ``ChunkedEncodingError`` mid-image -- by far the
      most common way one of these transfers dies -- was never retried.  The
      read is inside the loop here.
    * **A part-written image looked complete.**  Six pipelines streamed
      straight into ``dest``, so an interrupted transfer left a truncated JPEG
      that the ``dest.exists()`` check at the top then skipped forever: the
      corruption survived every later resume.  Writing to ``.part`` and
      renaming means a file at ``dest`` is always whole.

    A permanently refused status (anything >= 400 that is neither "missing" nor
    retryable -- an expired MAIS token, say) fails immediately; repeating it
    would only be rude.
    """
    # Old MAIS downloads were PNG previews saved as .jpg. Revisit those on
    # resume and validate replacements before atomically overwriting them.
    jpeg = dest.suffix.lower() in {".jpg", ".jpeg"}
    if dest.exists() and _valid_image(dest, jpeg=jpeg):
        return "exists"

    # Looked up late, like RateLimiter._do_sleep: binding time.sleep as a
    # default would capture it at import and leave a test that patches
    # time.sleep waiting out the real backoff.
    pause = sleep if sleep is not None else (lambda seconds: time.sleep(seconds))
    missing = frozenset(missing_statuses)
    tmp = dest.with_suffix(dest.suffix + ".part")
    kwargs: dict[str, Any] = {"stream": True, "timeout": timeout}
    if allow_redirects is not None:
        kwargs["allow_redirects"] = allow_redirects

    attempts = max(1, retries)
    delay = backoff
    for attempt in range(attempts):
        last = attempt >= attempts - 1
        try:
            resp = session.get(url, **kwargs)
            if resp.status_code in missing:
                return "missing"
            if resp.status_code in RETRY_STATUSES:
                if last:
                    print(f"      {resp.status_code} after {attempt + 1} attempts", flush=True)
                    return "failed"
                pause(delay)
                delay *= 2
                continue
            if resp.status_code >= 400:
                print(f"      {resp.status_code} for {url}", flush=True)
                return "failed"
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(65536):
                    if chunk:
                        fh.write(chunk)
            if not _valid_image(tmp, jpeg=jpeg):
                _discard(tmp)
                print(f"      invalid image for {dest}; page failed", flush=True)
                return "failed"
            tmp.rename(dest)
            return "downloaded"
        except requests.RequestException as exc:
            # Covers the connection resets, DNS failures, timeouts and broken
            # chunked bodies that all used to end the run.
            _discard(tmp)
            if last:
                print(f"      network error ({exc}); giving up on this page", flush=True)
                return "failed"
            print(f"      network error ({exc}); retry in {delay:g}s", flush=True)
            pause(delay)
            delay *= 2
    return "failed"
