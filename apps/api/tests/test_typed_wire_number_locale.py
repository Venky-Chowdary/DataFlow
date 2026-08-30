"""Typed carriers read canonically; human-authored text still fails closed.

A PostgreSQL ``NUMERIC(12,3)`` reaches the write path as the text ``10.129``
because readers render cells to strings. Auto must refuse that shape in a CSV
(US 10.129 vs EU 10129 is a real coin flip) but must not refuse it out of a
typed column, where no human chose a grouping convention. WIRE is that
distinction, and Validate and Execute have to pick it the same way or a route
Validate cleared is one Run quarantines.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.transform_engine import (  # noqa: E402
    NUMBER_LOCALE_WIRE,
    decimal_wire_value,
    reset_active_number_locale,
    set_active_number_locale,
)
from src.transfer.connector_capabilities import (  # noqa: E402
    typed_wire_number_locale,
)
from src.transfer.engine import _run_number_locale  # noqa: E402


@pytest.fixture
def wire():
    token = set_active_number_locale(NUMBER_LOCALE_WIRE)
    yield
    reset_active_number_locale(token)


@pytest.fixture
def auto():
    token = set_active_number_locale("")
    yield
    reset_active_number_locale(token)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("10.129", Decimal("10.129")),
        ("20.500", Decimal("20.500")),
        ("0.016666668", Decimal("0.016666668")),
        ("-0.001", Decimal("-0.001")),
        ("1.5e3", Decimal("1500")),
        ("$5.00", Decimal("5.00")),
    ],
)
def test_wire_reads_a_typed_carrier_as_the_number_it_holds(wire, text, expected):
    assert decimal_wire_value(text) == expected


@pytest.mark.parametrize("text", ["1,234", "12,345", "abc"])
def test_wire_never_resolves_what_auto_refuses(wire, text):
    """Text a reader could not have emitted keeps the Auto contract.

    WIRE only settles the one form Auto cannot — a lone dot group out of a
    typed carrier. It must not become a back door that reads a grouped token
    Auto refused, or a typed route would silently rewrite ``1,234``.
    """
    assert decimal_wire_value(text) is None


@pytest.mark.parametrize("text", ["10.129", "1,234", "1.005", "1.000"])
def test_auto_still_refuses_human_authored_lone_groups(auto, text):
    assert decimal_wire_value(text) is None


@pytest.mark.parametrize(
    "text,expected",
    [
        ("1,234.50", Decimal("1234.50")),
        ("1.5", Decimal("1.5")),
        ("1e5", Decimal("100000")),
        ("0.025", Decimal("0.025")),
    ],
)
def test_auto_keeps_reading_what_was_never_ambiguous(auto, text, expected):
    assert decimal_wire_value(text) == expected


def test_explicit_locales_are_unchanged_by_the_wire_contract():
    for locale, expected in (("US", Decimal("1234")), ("EU", Decimal("1.234"))):
        token = set_active_number_locale(locale)
        try:
            assert decimal_wire_value("1,234") == expected
        finally:
            reset_active_number_locale(token)


@pytest.mark.parametrize(
    "kind,fmt,expected",
    [
        ("database", "postgresql", NUMBER_LOCALE_WIRE),
        ("database", "mysql", NUMBER_LOCALE_WIRE),
        ("database", "snowflake", NUMBER_LOCALE_WIRE),
        ("file", "csv", ""),
        ("file", "excel", ""),
        ("object_store", "s3", ""),
        ("", "", ""),
    ],
)
def test_only_a_typed_source_renders_wire_values(kind, fmt, expected):
    assert typed_wire_number_locale(kind, fmt) == expected


def _request(kind: str, fmt: str, declared: str = ""):
    return SimpleNamespace(
        number_locale=declared,
        source=SimpleNamespace(kind=kind, format=fmt),
    )


def test_execute_reads_a_typed_source_the_way_validate_did():
    assert _run_number_locale(_request("database", "postgresql")) == NUMBER_LOCALE_WIRE
    assert _run_number_locale(_request("file", "csv")) == ""


def test_an_operator_declaration_always_wins():
    assert _run_number_locale(_request("database", "postgresql", "EU")) == "EU"
    assert _run_number_locale(_request("file", "csv", "US")) == "US"
