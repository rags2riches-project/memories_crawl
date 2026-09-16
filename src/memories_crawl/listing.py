"""Shared vocabulary for the ``--list-invnrs`` pre-flight listing (issue #27).

Knowing *which* inventarisnummers exist is not enough to plan a download: at
Tresoar most registers are indexed but not digitized, and bytes-per-page varies
36x across archives. Every pipeline therefore reports a count alongside each
inventarisnummer -- persons with a scan, scans, or pages, depending on what the
archive counts in.

A count is an ``int`` when the archive tells us exactly, and ``None`` when
finding out would cost the per-register request that ``--list-invnrs`` exists
to avoid. ``None`` renders as ``?``; it is never rendered or treated as ``0``,
because a wrong zero would make a caller skip real data. ``--only-digitized``
drops the known zeroes and keeps the unknowns.
"""

from __future__ import annotations

# Rendered in place of a count the archive did not hand us cheaply.
UNKNOWN = "?"

# Appended to a per-register log line whose register yielded no images, so a
# zero-yield register is visible in the log instead of inferred afterwards.
NOTHING_TO_DOWNLOAD = " — nothing to download"


def fmt_count(count: int | None) -> str:
    """Render a scan/page count for a listing table or CSV cell."""
    return UNKNOWN if count is None else str(count)


def has_scans(count: int | None) -> bool:
    """Is this register worth downloading?

    Only a *known* zero answers no. An unknown count keeps the register, so
    ``--only-digitized`` can never hide data we merely failed to measure.
    """
    return count != 0


def summary_suffix(count: int | None) -> str:
    """``NOTHING_TO_DOWNLOAD`` for a known-empty register, else nothing."""
    return "" if has_scans(count) else NOTHING_TO_DOWNLOAD
