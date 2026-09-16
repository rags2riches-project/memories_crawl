"""On-disk cache for the archive-level inventory listing.

The API-backed pipelines have to know which registers (or, at the Nationaal
Archief, which inventarisnummers) exist before they can do anything else, and
that enumeration is a fixed cost paid on *every* invocation -- including runs
that download nothing.  For BHIC it is a nineteen-page walk over the Memorix
search API, most of whose ~5.6 s is the deliberate ``REQUEST_SLEEP`` between
pages.  Paid once that is a rounding error; paid seventy times by a script that
fetches one register per invocation it is six and a half minutes of pure
re-enumeration.  See issue #25.

So the listing is written to ``<out-dir>/.cache/<archive>/`` and reused for
:data:`TTL_SECONDS`.  Three things matter more than the speedup, because a
listing that silently loses registers is far worse than a slow one:

* the cache is keyed by the query that produced it, so changing the filter
  (or the archive re-cataloguing under a new one) re-collects rather than
  serving the old answer;
* anything unreadable, unrecognised, expired or *empty* is treated as a miss
  and re-collected, never as "this archive has no registers";
* an empty result is never written, so a transient API hiccup cannot pin an
  empty inventory in place for a month.

Where the API can filter server-side -- Memorix indexes the inventarisnummer as
an exact-match field -- a ``--invnr`` run skips this module entirely and asks
for just the registers it wants, which is one request rather than nineteen.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from memories_crawl import paths

#: How long a listing stays fresh.  These archives re-catalogue on the order of
#: years, so a month is comfortably inside the noise, while still short enough
#: that newly digitised material turns up without anyone having to remember
#: ``--refresh-cache``.
TTL_SECONDS = 30 * 24 * 60 * 60

#: Bumped whenever the payload layout changes; an older file is a cache miss.
_FORMAT = 1


def _read(path: Path, key: str, ttl: float) -> list[Any] | None:
    """Return the cached items, or ``None`` when the file cannot be trusted."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("format") != _FORMAT:
        return None
    if payload.get("key") != key:
        return None
    collected_at = payload.get("collected_at")
    if not isinstance(collected_at, (int, float)) or isinstance(collected_at, bool):
        return None
    age = time.time() - collected_at
    # A timestamp in the future means a clock moved; re-collecting is cheap and
    # is the safe direction to be wrong in.
    if age < 0 or age > ttl:
        return None
    items = payload.get("items")
    # An empty listing is never written, so an empty one here means damage.
    if not isinstance(items, list) or not items:
        return None
    return items


def _write(path: Path, key: str, items: list[Any]) -> None:
    """Write the listing atomically; a failure to cache is not a failure."""
    payload = {"format": _FORMAT, "key": key, "collected_at": time.time(), "items": items}
    tmp = path.with_suffix(path.suffix + ".part")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except (OSError, TypeError, ValueError) as exc:
        print(f"  Warning: could not cache the listing in {path} ({exc}); continuing.")
        tmp.unlink(missing_ok=True)


def load_or_collect(
    archive: str,
    name: str,
    collect: Callable[[], list[Any]],
    *,
    key: str,
    refresh: bool = False,
    legacy: Path | None = None,
    ttl: float = TTL_SECONDS,
) -> list[Any]:
    """Return ``archive``'s inventory listing, collecting it only when needed.

    ``collect`` is called when there is no usable cache (or ``refresh`` is set,
    as ``--refresh-cache`` does) and its result is cached under ``name`` in
    ``<out-dir>/.cache/<archive>/``.  ``key`` identifies the query the listing
    answers: a cache written for a different one is ignored.  Exceptions from
    ``collect`` propagate and leave any existing cache untouched, so a caller
    with a fallback of its own (Nationaal Archief) never caches the fallback.
    """
    path = paths.cache_file(archive, name, legacy=legacy)
    if not refresh:
        cached = _read(path, key, ttl)
        if cached is not None:
            print(f"Reusing the cached listing in {path} ({len(cached)} items).")
            return cached
    items = collect()
    if items:
        _write(path, key, items)
    return items
