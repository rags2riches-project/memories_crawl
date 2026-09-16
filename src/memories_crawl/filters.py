"""Case-insensitive matching for the ``--kantoor`` filter.

Every archive names its kantoren slightly differently, and most expose a second
identifier next to the name -- an archief-code (Gelderland's ``0022``), a MAIS
``minr`` (Zeeland's ``33439946``) or a micode (Utrecht's ``337-2``).
``--list-invnrs`` prints whichever of the two the archive uses, so a caller
feeding that listing back in may hold either.  A filter value therefore matches
when it equals *any* of the labels a pipeline offers for one kantoor, compared
case-insensitively and ignoring surrounding whitespace.  Purely numeric labels
also match with their leading zeros stripped, so ``--kantoor 22`` finds
Gelderland's ``0022``.
"""

from __future__ import annotations

from collections.abc import Iterable


def _variants(value: object) -> set[str]:
    """Every spelling of ``value`` that a filter may legitimately use."""
    folded = str(value).strip().casefold()
    if not folded:
        return set()
    variants = {folded}
    if folded.isdigit():
        variants.add(folded.lstrip("0") or "0")
    return variants


def normalize(values: Iterable[str] | None) -> set[str] | None:
    """Fold a ``--kantoor`` filter into a lookup set, ``None`` when unset."""
    if not values:
        return None
    wanted: set[str] = set()
    for value in values:
        wanted |= _variants(value)
    return wanted or None


def matches(wanted: set[str] | None, *labels: object) -> bool:
    """True when no filter is active, or one of ``labels`` is in it."""
    if wanted is None:
        return True
    return any(_variants(label) & wanted for label in labels)


def describe(invnrs: Iterable[str] | None = None, kantoren: Iterable[str] | None = None) -> str:
    """Render the active filters for a "matched nothing" warning."""
    parts: list[str] = []
    if kantoren:
        parts.append("--kantoor " + ", ".join(sorted(kantoren)))
    if invnrs:
        parts.append("--invnr " + ", ".join(sorted(invnrs)))
    return " / ".join(parts) or "the active filter"
