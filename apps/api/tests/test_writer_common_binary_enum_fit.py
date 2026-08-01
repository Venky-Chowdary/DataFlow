"""BINARY width + ENUM/SET domain write quarantine + fail-closed bind."""

from __future__ import annotations

import base64
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import coerce_binary_wire  # noqa: E402
from connectors.writer_common import (  # noqa: E402
    fits_binary,
    quarantine_unfit_binaries,
    quarantine_unfit_enum_set,
)


def test_fits_binary_width():
    payload = b"\x00" * 16
    wire = base64.b64encode(payload).decode("ascii")
    assert fits_binary(wire, 16) is True
    assert fits_binary(wire, 15) is False
    assert fits_binary(payload, 16) is True


def test_quarantine_holds_out_oversized_binary():
    big = base64.b64encode(b"x" * 32).decode("ascii")
    ok = base64.b64encode(b"y" * 8).decode("ascii")
    details: list[dict] = []
    out = quarantine_unfit_binaries(
        [(big,), (ok,)],
        ["blob"],
        ["VARBINARY(16)"],
        details,
        policy="quarantine",
    )
    assert out == [(ok,)]
    assert details and "exceeds" in details[0]["reason"]


def test_quarantine_holds_out_invalid_base64():
    details: list[dict] = []
    out = quarantine_unfit_binaries(
        [("not-base64!!!",)],
        ["blob"],
        ["VARBINARY(64)"],
        details,
        policy="quarantine",
    )
    assert out == []
    assert details and "base64" in details[0]["reason"].lower()


def test_coerce_binary_wire_refuses_silent_utf8():
    with pytest.raises(ValueError, match="base64"):
        coerce_binary_wire("not-valid-base64!!!")


def test_enum_domain_quarantine():
    details: list[dict] = []
    out = quarantine_unfit_enum_set(
        [("a",), ("z",)],
        ["status"],
        ["ENUM('a','b')"],
        details,
        policy="quarantine",
    )
    assert out == [("a",)]
    assert details and "ENUM" in details[0]["reason"]


def test_set_domain_quarantine():
    details: list[dict] = []
    out = quarantine_unfit_enum_set(
        [("x,y",), ("x,z",)],
        ["flags"],
        ["SET('x','y')"],
        details,
        policy="quarantine",
    )
    assert out == [("x,y",)]
    assert details
