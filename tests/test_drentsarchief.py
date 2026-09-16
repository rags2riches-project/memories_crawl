"""Unit tests for the Drents Archief register-driven inventory (issue #28).

These run offline: they exercise the pure helpers that turn Memorix register
documents into inventory rows.
"""

from __future__ import annotations

import csv

from memories_crawl import drentsarchief as dre


def _register(invnr: str, gemeente: str, naam: str, type_title: str = "Belastingen") -> dict:
    return {
        "id": f"reg-{invnr}",
        "metadata": {
            "inventarisnummer": invnr,
            "gemeente": gemeente,
            "naam": naam,
            "type_title": type_title,
            "archiefnummer": "0119.03",
        },
    }


def test_invnr_and_gemeente_come_from_register_metadata() -> None:
    reg = _register("31", "Coevorden", "Successiememorie Coevorden 0119.07 31")
    assert dre._register_invnr(reg) == "31"
    assert dre._register_gemeente(reg) == "Coevorden"


def test_missing_metadata_does_not_raise() -> None:
    assert dre._register_invnr({}) == ""
    assert dre._register_gemeente({"metadata": None}) == ""


def test_tafel_registers_are_excluded() -> None:
    """Project-wide rule: Tafel V-bis is never downloaded."""
    assert dre._is_tafel(_register("9", "Assen", "Tafel V-bis Assen"))
    assert dre._is_tafel(_register("9", "Assen", "Index", type_title="Tafel VI"))
    assert not dre._is_tafel(_register("9", "Assen", "Successiememorie Assen 0119.03 9"))


def test_list_registers_writes_csv_columns(tmp_path, capsys) -> None:
    registers = [
        _register("2", "Meppel", "Successiememorie Meppel 0119.05 2"),
        _register("10", "Assen", "Successiememorie Assen 0119.03 10"),
    ]
    out = tmp_path / "invnrs.csv"
    dre._list_registers(registers, csv_out=str(out))

    with open(out, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [r["invnr"] for r in rows] == ["2", "10"]
    assert rows[0]["gemeente"] == "Meppel"
    assert rows[1]["register_name"] == "Successiememorie Assen 0119.03 10"

    # Printed table is sorted by gemeente, then numerically by invnr.
    printed = capsys.readouterr().out
    assert printed.index("Assen") < printed.index("Meppel")


def test_list_registers_sorts_invnrs_numerically(capsys) -> None:
    registers = [
        _register("10", "Assen", "tien"),
        _register("2", "Assen", "twee"),
        _register("68.2", "Assen", "niet-numeriek"),
    ]
    dre._list_registers(registers)
    printed = capsys.readouterr().out
    assert printed.index("twee") < printed.index("tien") < printed.index("niet-numeriek")
