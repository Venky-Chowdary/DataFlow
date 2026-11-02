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
    NUMBER_LOCALE_DEFAULT,
    NUMBER_LOCALE_WIRE,
    ambiguous_number_columns,
    assumed_number_locale,
    decimal_wire_value,
    infer_number_locale,
    reset_active_number_locale,
    set_active_number_locale,
)
from src.transfer.connector_capabilities import (  # noqa: E402
    typed_wire_number_locale,
)
from src.transfer.engine import (  # noqa: E402
    _run_number_locale,
    _settle_locales,
)


@pytest.fixture
def wire():
    token = set_active_number_locale(NUMBER_LOCALE_WIRE)
    yield
    reset_active_number_locale(token)


@pytest.fixture
def auto():
    """Auto for the test, and Auto again after — a pin must never leak."""
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
        number_locale_assumed=False,
        date_locale="",
        source=SimpleNamespace(kind=kind, format=fmt),
    )


def test_execute_reads_a_typed_source_the_way_validate_did():
    assert _run_number_locale(_request("database", "postgresql")) == NUMBER_LOCALE_WIRE
    assert _run_number_locale(_request("file", "csv")) == ""


def test_an_operator_declaration_always_wins():
    assert _run_number_locale(_request("database", "postgresql", "EU")) == "EU"
    assert _run_number_locale(_request("file", "csv", "US")) == "US"


# --- The US assumption: last resort, per column, never silent -----------------


def test_evidence_beats_the_assumption_in_both_directions():
    """A column that shows its convention is read, not assumed."""
    assert assumed_number_locale([{"amt": "1.234,56"}, {"amt": "9,50"}], ["amt"]) == ""
    assert infer_number_locale([{"amt": "1.234,56"}], ["amt"]) == "EU"
    assert infer_number_locale([{"amt": "1,234.56"}], ["amt"]) == "US"


def test_only_a_genuinely_ambiguous_sample_is_assumed():
    rows = [{"amt": "1,234"}, {"amt": "10.129"}]
    assert assumed_number_locale(rows, ["amt"]) == "US"
    named = [f["column"] for f in ambiguous_number_columns(rows, ["amt"])]
    assert named == ["amt"]


@pytest.mark.parametrize(
    "rows",
    [
        [{"amt": "$1,000.00"}],
        [{"amt": "1e5"}],
        [{"amt": "0.5"}],
        [{"amt": "not a number"}],
        [],
    ],
)
def test_nothing_ambiguous_means_nothing_assumed(rows):
    assert assumed_number_locale(rows, ["amt"]) == ""


@pytest.mark.parametrize("declared", ["US", "EU", NUMBER_LOCALE_WIRE])
def test_a_settled_contract_is_never_overridden_by_the_assumption(declared):
    token = set_active_number_locale(declared)
    try:
        assert assumed_number_locale([{"amt": "1,234"}], ["amt"]) == ""
    finally:
        reset_active_number_locale(token)


def test_a_typed_source_reads_wire_rather_than_the_us_assumption(auto):
    """WIRE is a carrier fact; US is a guess. A typed route never needs the guess."""
    request = _request("database", "postgresql")
    _settle_locales(request, [{"amt": "10.129"}], ["amt"])
    assert request.number_locale == ""
    assert _run_number_locale(request) == NUMBER_LOCALE_WIRE


def test_a_file_route_settles_on_us_and_records_it_on_the_request(auto):
    request = _request("file", "csv")
    _settle_locales(request, [{"amt": "10.129"}], ["amt"])
    assert request.number_locale == "US"


def test_the_assumption_reads_grouped_and_scientific_text_as_us(auto):
    token = set_active_number_locale(NUMBER_LOCALE_DEFAULT)
    try:
        assert decimal_wire_value("1,234") == Decimal("1234")
        assert decimal_wire_value("10.129") == Decimal("10.129")
        assert decimal_wire_value("$1,234.50") == Decimal("1234.50")
        assert decimal_wire_value("0.016666668") == Decimal("0.016666668")
        assert decimal_wire_value("1.5e3") == Decimal("1500")
    finally:
        reset_active_number_locale(token)


def test_proof_keeps_the_assumption_apart_from_a_declaration(auto):
    """``US`` chosen for the operator is not ``US`` chosen by the operator."""
    declared = _request("file", "csv", "US")
    _settle_locales(declared, [{"amt": "1,234"}], ["amt"])
    assert declared.number_locale_assumed is False

    guessed = _request("file", "csv")
    _settle_locales(guessed, [{"amt": "1,234"}], ["amt"])
    assert guessed.number_locale == "US"
    assert guessed.number_locale_assumed is True


def test_evidence_settles_without_claiming_an_assumption(auto):
    request = _request("file", "csv")
    _settle_locales(request, [{"amt": "1.234,56"}], ["amt"])
    assert request.number_locale == "EU"
    assert request.number_locale_assumed is False


def test_type_inference_reads_the_column_the_way_the_writer_will(auto):
    """A column the run lands as DECIMAL must not be typed VARCHAR first.

    Inference under Auto refused ``10.129``, typed the CSV column VARCHAR, and
    Map then blocked the route as a VARCHAR→DECIMAL fidelity collapse for a
    value preflight and the writer both read as US.
    """
    from services.schema_inference import infer_schema_map

    schema, _intel = infer_schema_map({"dep_time": ["10.129", "30.987"]})
    assert schema["dep_time"].startswith("DECIMAL")


def test_type_inference_still_follows_the_evidence_not_the_fallback(auto):
    from services.schema_inference import infer_schema_map

    schema, _intel = infer_schema_map({"amt": ["1.234,56", "2.000,00"]})
    assert schema["amt"] == "DECIMAL(6,2)"

    token = set_active_number_locale("EU")
    try:
        assert decimal_wire_value("1.234,56") == Decimal("1234.56")
    finally:
        reset_active_number_locale(token)


def test_profiling_and_inference_agree_on_the_ambiguous_column(auto):
    from services.data_profiler import profile_dataset

    profile = profile_dataset(["dep_time"], [{"dep_time": "10.129"}, {"dep_time": "30.987"}])
    assert profile["schema"]["dep_time"] == "DECIMAL(5,3)"
