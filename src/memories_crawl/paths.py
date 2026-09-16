"""Resolution of the output root, per-archive scan directories and caches.

Every pipeline writes two very different kinds of file below a single root
(``--out-dir``, default ``./scans``):

* the downloaded scans, under ``<out-dir>/<archive>/…``;
* the caches that make reruns cheap -- inventory listings, Playwright token
  harvests, ``done.txt`` resume markers, progress CSVs -- under
  ``<out-dir>/.cache/<archive>/``.

Before this module existed both lived under ``scans/<archive>/``, resolved
relative to the current working directory, so a ``--list-invnrs`` pass and a
download pass started from different directories could not see each other's
caches and the (by far slowest) token harvest silently ran twice.

Keeping the caches in their own subtree also means the images can be moved or
deleted without throwing away a token harvest that took a quarter of an hour.

Caches written by earlier versions sit mixed in with the images.
:func:`cache_file` keeps using such a file when one is already there, so
upgrading never discards work that has already been paid for.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_OUT_DIR = Path("scans")

#: Environment variable consulted when no ``--out-dir`` is given.
ENV_VAR = "MEMORIES_CRAWL_OUT_DIR"

_out_dir: Path = Path(os.environ.get(ENV_VAR) or DEFAULT_OUT_DIR)


def set_out_dir(path: str | os.PathLike[str] | None) -> Path:
    """Point every subsequent path lookup at ``path``.

    ``None`` restores the default (``$MEMORIES_CRAWL_OUT_DIR`` or ``./scans``).
    Returns the root now in effect.
    """
    global _out_dir
    if path is None:
        _out_dir = Path(os.environ.get(ENV_VAR) or DEFAULT_OUT_DIR)
    else:
        _out_dir = Path(path)
    return _out_dir


def out_dir() -> Path:
    """The output root currently in effect."""
    return _out_dir


def archive_dir(archive: str) -> Path:
    """Directory holding the downloaded scans of one archive."""
    return _out_dir / archive


def cache_dir(archive: str) -> Path:
    """Directory holding one archive's inventory/token/resume caches."""
    return _out_dir / ".cache" / archive


def cache_file(archive: str, name: str, legacy: Path | None = None) -> Path:
    """Path to cache file ``name`` for ``archive``.

    Normally ``<out-dir>/.cache/<archive>/<name>``, with the parent directory
    created on demand.  If the file does not exist there but does exist at its
    pre-0.3 location, that path is returned instead so existing caches keep
    being read *and* updated in place.  ``legacy`` overrides that older
    location (used for the progress CSVs, which used to sit in the working
    directory rather than under ``scans/``).
    """
    current = cache_dir(archive) / name
    if not current.exists():
        old = legacy if legacy is not None else archive_dir(archive) / name
        if old.exists():
            return old
    current.parent.mkdir(parents=True, exist_ok=True)
    return current
