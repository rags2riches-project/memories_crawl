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

The same discipline applies to the period each register covers (issue #38):
``parse_years`` and friends turn whatever datering an archive publishes into a
``year_from`` / ``year_to`` pair, and an archive that publishes none reports
``?`` rather than letting the caller infer years from inventarisnummers.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

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


# ---------------------------------------------------------------------------
# Dates (issue #38)
# ---------------------------------------------------------------------------
#
# A count answers "is there anything here?"; a date answers "is it the *right*
# thing?". Planning a period-limited download without one means ranking
# inventarisnummers against a date span, which is a guess: one such plan put
# 80% of the fetched deaths outside the target years. So every listing carries
# ``year_from`` / ``year_to`` next to its count, with the same discipline --
# an honest ``?`` when the archive does not say, never an invented year.

#: The Memories van Successie run 1806-1927. A wider window is accepted so a
#: binding labelled with an adjacent year still parses, while four-digit
#: inventarisnummers (2276, 7268) and page counts stay out of the way.
MIN_YEAR = 1795
MAX_YEAR = 1935

#: CSV columns every pipeline appends to its listing schema.
YEAR_FIELDS = ["year_from", "year_to"]

_YEAR_RE = re.compile(r"(?<!\d)(1[789]\d{2})(?!\d)")


def _strip_leading_invnr(text: str, invnr: int | str | None) -> str:
    """Drop a leading inventarisnummer from a MAIS description.

    MAIS renders leaves as ``"140  1895 eerste kwartaal"``. Most invnrs are too
    small to be mistaken for a year, but the four-digit ones are not, so the
    number is removed before any year is read out of the text.
    """
    if invnr is None:
        return text
    return re.sub(rf"^\s*{re.escape(str(invnr))}\b[.,]?\s*", "", text)


def parse_years(text: str | None, invnr: int | str | None = None) -> tuple[int | None, int | None]:
    """Return ``(year_from, year_to)`` read out of a free-text datering.

    Archives describe a register as ``"1818 jan. - mrt."``, ``"1826-1830"`` or
    ``"1895 eerste kwartaal"``; the span is the lowest and highest plausible
    year in the text. ``(None, None)`` when the text carries no year at all --
    an unknown period, which a caller can see and act on, unlike a guessed one.
    """
    if not text:
        return None, None
    years = [int(y) for y in _YEAR_RE.findall(_strip_leading_invnr(text, invnr))]
    return span(years)


def span(years: Iterable[int | str | None]) -> tuple[int | None, int | None]:
    """Fold a collection of years (ints, ``"1883-12-12"`` dates, ``None``) into a span."""
    found: list[int] = []
    for value in years:
        if value is None:
            continue
        if isinstance(value, int):
            candidate: int | None = value
        else:
            match = _YEAR_RE.search(str(value))
            candidate = int(match.group(1)) if match else None
        if candidate is not None and MIN_YEAR <= candidate <= MAX_YEAR:
            found.append(candidate)
    if not found:
        return None, None
    return min(found), max(found)


def fmt_year(year: int | None) -> str:
    """Render one end of a span for a table or CSV cell."""
    return UNKNOWN if year is None else str(year)


def fmt_period(year_from: int | None, year_to: int | None) -> str:
    """Render a span for the printed table: ``1818-1820``, ``1883`` or ``?``."""
    if year_from is None and year_to is None:
        return UNKNOWN
    if year_from == year_to:
        return fmt_year(year_from)
    return f"{fmt_year(year_from)}-{fmt_year(year_to)}"


def year_row(year_from: int | None, year_to: int | None) -> dict[str, str]:
    """The two CSV cells for a span, ready to merge into a listing row."""
    return {"year_from": fmt_year(year_from), "year_to": fmt_year(year_to)}
