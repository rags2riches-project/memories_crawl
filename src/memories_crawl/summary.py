"""How much a run is downloading, while it runs and once it is done.

A full crawl of ten archives takes hours and writes hundreds of gigabytes, and
before this module the only feedback was a per-register line scrolling past.
Issue #19 asked for the scale to be visible: how many kantoren, how many
registers, how many pages -- and, because bytes-per-page varies 36x between
archives (Overijssel ~93 KB, Nationaal Archief ~3.4 MB), how many bytes.

Three things are reported:

* a per-register progress line, via :meth:`PageTally.describe`;
* an up-front ``about to download N pages across M registers`` line wherever a
  pipeline already knows the page list before it starts downloading (the MAIS
  scrapers harvest it during the token phase), via :func:`announce`;
* a per-archive block at the end of the run, via :meth:`RunSummary.report`,
  which the CLI aggregates into a :func:`grand_total` table for ``all``.

Counting
────────
  Downloads fold into a :class:`PageTally`, which is a plain value object: a
  download loop can keep its own tally and merge it into the register's with
  ``+=``, so making the loops concurrent (issue #26) needs no locking, only a
  merge at the join.

Partial runs
────────────
  ``cli.py``'s ``all`` target catches per-pipeline exceptions and carries on, so
  a pipeline that dies halfway must still be able to report what it did. Each
  :class:`RunSummary` registers itself with the collector the CLI installs
  around the call (see :func:`collect`) at construction time, so the CLI holds
  the object -- and its partial totals -- even when the call never returns.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

#: Collector installed by :func:`collect` around one pipeline call.
_COLLECTOR: ContextVar[list[RunSummary] | None] = ContextVar("summary_collector", default=None)


def human_bytes(n: int) -> str:
    """Format a byte count in decimal units, as archives quote their sizes."""
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(value) < 1000 or unit == "TB":
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{value:.1f} TB"  # pragma: no cover - unreachable


def human_count(n: int) -> str:
    """Thousands-separated count, so six-figure page totals stay readable."""
    return f"{n:,}"


def _size_of(dest: Path | None) -> int:
    """Bytes on disk at ``dest``, or 0 if it is gone or unreadable."""
    if dest is None:
        return 0
    try:
        return dest.stat().st_size
    except OSError:
        return 0


@dataclass
class PageTally:
    """Outcome of a batch of page downloads.

    ``skipped`` counts pages a resumed run found already on disk; ``missing``
    counts pages the server refused to hand over (404/202, or a download that
    failed every retry). Only pages actually fetched add to ``bytes_written``,
    so the figure is what this run cost, not what the directory holds.
    """

    downloaded: int = 0
    skipped: int = 0
    missing: int = 0
    bytes_written: int = 0

    @property
    def total(self) -> int:
        return self.downloaded + self.skipped + self.missing

    def record(self, status: str, dest: Path | None = None) -> str:
        """Fold one finished download into the tally and return ``status``.

        ``status`` is what the pipelines' ``_download_file`` returns:
        ``downloaded``, ``exists`` for a page that was already there, anything
        else (``missing``, ``failed``) for a page we did not get.
        """
        if status == "downloaded":
            self.downloaded += 1
            self.bytes_written += _size_of(dest)
        elif status == "exists":
            self.skipped += 1
        else:
            self.missing += 1
        return status

    def __iadd__(self, other: PageTally) -> PageTally:
        self.downloaded += other.downloaded
        self.skipped += other.skipped
        self.missing += other.missing
        self.bytes_written += other.bytes_written
        return self

    def describe(self, n_pages: int | None = None) -> str:
        """One-line progress text: ``38 pages (38 new, 0 existing, 0 missing, 21.1 MB)``.

        ``n_pages`` overrides the page count when the caller knows how many
        pages the register has (a download loop that skips URL-less pages
        records fewer outcomes than there are pages).
        """
        total = self.total if n_pages is None else n_pages
        text = (
            f"{human_count(total)} pages ({human_count(self.downloaded)} new, "
            f"{human_count(self.skipped)} existing, {human_count(self.missing)} missing"
        )
        if self.bytes_written:
            text += f", {human_bytes(self.bytes_written)}"
        return text + ")"


@dataclass
class RunSummary:
    """What one archive's pipeline processed during one run.

    ``units`` counts the kantoren (or archive codes) the run touched,
    ``registers`` the inventarisnummers, ``records`` the individual memories
    where the archive indexes them per deed/person, and ``pages`` the scans.
    """

    archive: str
    label: str = ""
    #: What this archive calls its top-level grouping, or ``None`` when it has
    #: none (the Nationaal Archief inventory is a flat list of invnrs).
    unit_name: str | None = "kantoren"
    record_name: str = "memories"
    units: int = 0
    registers: int = 0
    records: int = 0
    pages: PageTally = field(default_factory=PageTally)
    #: Set by the CLI when the pipeline stopped on an exception, so the block
    #: below cannot be mistaken for a complete run.
    error: str | None = None
    reported: bool = False

    def __post_init__(self) -> None:
        collector = _COLLECTOR.get()
        if collector is not None:
            collector.append(self)

    @property
    def title(self) -> str:
        return self.label or self.archive

    def record(self, status: str, dest: Path | None = None) -> str:
        """Fold a single download straight into the run (see :meth:`PageTally.record`)."""
        return self.pages.record(status, dest)

    def render(self) -> str:
        """The end-of-run block for this archive."""
        rows: list[tuple[str, str]] = []
        if self.unit_name:
            rows.append((f"{self.unit_name} processed", human_count(self.units)))
        rows.append(("registers processed", human_count(self.registers)))
        if self.records:
            rows.append((f"{self.record_name} processed", human_count(self.records)))
        rows.append(("pages downloaded", human_count(self.pages.downloaded)))
        rows.append(("pages already present", human_count(self.pages.skipped)))
        rows.append(("pages missing", human_count(self.pages.missing)))
        rows.append(("bytes written", human_bytes(self.pages.bytes_written)))

        lines = [f"===== {self.title}: run summary ====="]
        lines += [f"  {label:<22}{value:>13}" for label, value in rows]
        if self.error:
            lines.append(f"  INCOMPLETE - stopped by error: {self.error}")
        return "\n".join(lines)

    def report(self) -> None:
        """Print :meth:`render`, once."""
        print(f"\n{self.render()}", flush=True)
        self.reported = True


def announce(n_pages: int, n_registers: int, where: str = "", indent: str = "  ") -> None:
    """Print the up-front scale of what is about to be fetched.

    Only worth calling where the page list is already known -- the MAIS
    scrapers harvest every page token before downloading anything, so they can
    say what the next phase costs before it starts.
    """
    suffix = f" in {where}" if where else ""
    print(
        f"{indent}→ about to download {human_count(n_pages)} pages "
        f"across {human_count(n_registers)} registers{suffix}",
        flush=True,
    )


@contextmanager
def collect() -> Iterator[list[RunSummary]]:
    """Capture every :class:`RunSummary` created inside the block.

    The list is filled as the summaries are constructed, not when the pipeline
    returns, so it survives a pipeline that raises midway.
    """
    box: list[RunSummary] = []
    token = _COLLECTOR.set(box)
    try:
        yield box
    finally:
        _COLLECTOR.reset(token)


def grand_total(summaries: list[RunSummary]) -> str:
    """One table across archives, for ``memories-crawl all``."""
    if not summaries:
        return "===== ALL ARCHIVES: nothing ran ====="

    header = f"  {'archive':<32}{'kantoren':>10}{'registers':>11}{'pages':>10}{'bytes':>12}"
    rule = f"  {'-' * 75}"
    lines = ["===== ALL ARCHIVES: grand total =====", header, rule]

    pages = PageTally()
    units = registers = 0
    incomplete: list[str] = []
    for run in summaries:
        unit_cell = human_count(run.units) if run.unit_name else "-"
        lines.append(
            f"  {run.title[:32]:<32}{unit_cell:>10}{human_count(run.registers):>11}"
            f"{human_count(run.pages.downloaded):>10}"
            f"{human_bytes(run.pages.bytes_written):>12}"
        )
        units += run.units
        registers += run.registers
        pages += run.pages
        if run.error:
            incomplete.append(run.title)

    lines.append(rule)
    lines.append(
        f"  {f'{len(summaries)} archives':<32}{human_count(units):>10}"
        f"{human_count(registers):>11}{human_count(pages.downloaded):>10}"
        f"{human_bytes(pages.bytes_written):>12}"
    )
    lines.append(
        f"  {human_count(pages.skipped)} pages already present, "
        f"{human_count(pages.missing)} missing."
    )
    if incomplete:
        lines.append(f"  INCOMPLETE (stopped by an error): {', '.join(incomplete)}")
    return "\n".join(lines)
