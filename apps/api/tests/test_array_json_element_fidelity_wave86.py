"""Wave 86: ARRAY element + JSON document fail-closed write gate.

Every scalar carrier already had a write quarantine gate (DECIMAL, INTEGER,
BOOLEAN, temporal, VARCHAR, BINARY, ENUM/SET). ARRAY and JSON columns had
none, so malformed payloads and unfit elements reached PG ``int[]``, BigQuery
``ARRAY<INT64>``, ClickHouse ``Array(T)``, and lakehouse list columns silently.

Research anchors
----------------
- BigQuery arrays: result ARRAYs may not contain NULL elements and arrays of
  arrays are unsupported (an ARRAY of STRUCT is required).
  https://docs.cloud.google.com/bigquery/docs/arrays
- ClickHouse ``Array(T)`` only accepts NULL elements when declared
  ``Array(Nullable(T))``; ``Nullable(Array(T))`` is rejected outright.
  https://clickhouse.com/docs/sql-reference/data-types/nullable
- PostgreSQL arrays permit both NULL arrays and NULL elements; an unquoted
  ``NULL`` in a ``{...}`` literal is a real NULL, a quoted ``"NULL"`` is text.
  https://www.postgresql.org/docs/18/arrays.html

Ambiguity is never quarantined: a bare scalar (e.g. SET joiner text ``a,b``)
must still pass, so this gate cannot produce false holdouts.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import (  # noqa: E402
    apply_write_quarantine_matrix,
    array_element_unfit_reason,
    parse_array_wire_elements,
    quarantine_unfit_arrays,
)


def _probe(typ: str, value, label: str = "destination", policy: str = "quarantine"):
    details: list[dict] = []
    kept = apply_write_quarantine_matrix(
        [(value,)], ["col"], [typ], details, policy=policy, dialect_label=label
    )
    return kept, details


def test_array_wire_parses_json_and_pg_literals():
    assert parse_array_wire_elements(["a", "b"]) == (["a", "b"], None)
    assert parse_array_wire_elements('["a","b"]') == (["a", "b"], None)
    assert parse_array_wire_elements("[1,2,3]") == ([1, 2, 3], None)
    assert parse_array_wire_elements("[]") == ([], None)

    # Postgres literal: unquoted NULL is a real NULL, quoted stays text.
    elements, err = parse_array_wire_elements("{a,b,NULL}")
    assert err is None
    assert elements == ["a", "b", None]
    elements, err = parse_array_wire_elements('{"NULL",b}')
    assert err is None
    assert elements == ["NULL", "b"]
    # Quoted delimiter must not split the element.
    elements, err = parse_array_wire_elements('{"quoted,comma",b}')
    assert err is None
    assert elements == ["quoted,comma", "b"]


def test_array_wire_refuses_object_and_malformed_payloads():
    _, err = parse_array_wire_elements({"a": 1})
    assert err and "object" in err.lower()
    _, err = parse_array_wire_elements(b"\x00\x01")
    assert err and "binary" in err.lower()
    _, err = parse_array_wire_elements('["unterminated"')
    assert err is None or "malformed" in err.lower()
    _, err = parse_array_wire_elements('[1,2,]')
    assert err and "malformed" in err.lower()
    _, err = parse_array_wire_elements('{"not":"array"}')
    assert err and "object" in err.lower()


def test_ambiguous_scalars_are_never_quarantined():
    """SET joiner text / engine-native scalars must not produce false holdouts."""
    elements, err = parse_array_wire_elements("a,b")
    assert elements is None and err is None
    elements, err = parse_array_wire_elements("plain scalar")
    assert elements is None and err is None

    kept, details = _probe("TEXT[]", "a,b", "PostgreSQL")
    assert len(kept) == 1
    assert details == []


def test_element_unfit_reason_reuses_scalar_fit_ssot():
    assert array_element_unfit_reason(5, "BIGINT") is None
    assert array_element_unfit_reason("not-a-number", "BIGINT")
    assert array_element_unfit_reason(2**70, "BIGINT")
    assert array_element_unfit_reason("maybe", "BOOLEAN")
    assert array_element_unfit_reason("true", "BOOLEAN") is None
    assert array_element_unfit_reason("1.0000000000001", "DECIMAL(22,9)")
    assert array_element_unfit_reason("10.50", "DECIMAL(22,9)") is None
    assert array_element_unfit_reason("toolong", "VARCHAR(3)")
    assert array_element_unfit_reason("ok", "VARCHAR(3)") is None
    assert array_element_unfit_reason("not-a-date", "TIMESTAMPTZ")
    assert array_element_unfit_reason("2024-01-01T00:00:00Z", "TIMESTAMPTZ") is None
    assert array_element_unfit_reason("2024-01-15", "DATE") is None
    assert array_element_unfit_reason("2024-13-45", "DATE")
    # Untyped element carrier (Snowflake semi-structured ARRAY) never invents unfit.
    assert array_element_unfit_reason("anything", "") is None


def test_unfit_array_elements_quarantine_not_silent():
    for typ, value in [
        ("ARRAY<BIGINT>", '["not-a-number"]'),
        ("ARRAY<BIGINT>", "[99999999999999999999999]"),
        ("ARRAY<BOOLEAN>", '["maybe"]'),
        ("ARRAY<DECIMAL(22,9)>", '["1.0000000000001"]'),
        ("ARRAY<TIMESTAMPTZ>", '["not-a-date"]'),
        ("ARRAY<JSON>", '{"not":"an array"}'),
        ("INTEGER[]", "{1,notanint}"),
    ]:
        kept, details = _probe(typ, value, "Shopify")
        assert kept == [], f"{typ} {value} silently reached the destination"
        assert details, f"{typ} {value} held out without a quarantine reason"

    for typ, value in [
        ("ARRAY<BIGINT>", "[1,2,3]"),
        ("ARRAY<TEXT>", '["a","b"]'),
        ("ARRAY<TIMESTAMPTZ>", '["2024-01-01T00:00:00Z"]'),
        ("ARRAY<DECIMAL(22,9)>", '["10.00","20.50"]'),
        ("ARRAY<JSON>", '[{"amount":"1.00","currency_code":"USD"}]'),
        ("ARRAY<TEXT>", "[]"),
        ("INTEGER[]", "{1,2,3}"),
    ]:
        kept, details = _probe(typ, value, "Shopify")
        assert len(kept) == 1, f"{typ} {value} falsely quarantined: {details}"
        assert details == []


def test_bigquery_forbids_null_elements_postgres_allows_them():
    """BigQuery raises on NULL array elements; Postgres documents them as legal."""
    kept, details = _probe("ARRAY<INT64>", "[1,null,3]", "BigQuery")
    assert kept == []
    assert "may not be NULL" in details[0]["reason"]

    kept, details = _probe("INTEGER[]", "{1,NULL,3}", "PostgreSQL")
    assert len(kept) == 1
    assert details == []


def test_clickhouse_array_requires_nullable_element_declaration():
    kept, details = _probe("Array(String)", '["a",null]', "ClickHouse")
    assert kept == []
    assert "may not be NULL" in details[0]["reason"]


def test_bigquery_rejects_nested_arrays():
    kept, details = _probe("ARRAY<INT64>", "[[1,2],[3]]", "BigQuery")
    assert kept == []
    assert "arrays of arrays" in details[0]["reason"]


def test_malformed_json_document_held_but_plain_scalar_allowed():
    """A broken document would silently become a JSON string — fail closed."""
    kept, details = _probe("JSON", '{"a": }', "Shopify")
    assert kept == []
    assert "malformed document" in details[0]["reason"]

    # coerce_json_wire losslessly wraps bare scalars, so these are not losses.
    for value in ["plain text scalar", '{"a":1}', '[1,2,3]', '"quoted"']:
        kept, details = _probe("JSON", value, "Shopify")
        assert len(kept) == 1, f"{value!r} falsely quarantined: {details}"
        assert details == []


def test_coerce_null_policy_nulls_cell_instead_of_dropping_row():
    details: list[dict] = []
    kept = apply_write_quarantine_matrix(
        [('["not-a-number"]', "keep-me")],
        ["arr", "other"],
        ["ARRAY<BIGINT>", "TEXT"],
        details,
        policy="coerce_null",
        dialect_label="Shopify",
    )
    assert kept == [(None, "keep-me")]
    assert details


def test_fail_policy_stamps_and_holds_out_unfit_arrays():
    """Strict/fail must stamp unfit ARRAY cells before bind — never soft-driver hope."""
    details: list[dict] = []
    rows = [('["not-a-number"]',)]
    kept = quarantine_unfit_arrays(
        rows, ["arr"], ["ARRAY<BIGINT>"], details, "fail", dialect_label="Shopify"
    )
    assert kept == []
    assert details
    assert details[0]["policy"] == "write_quarantine"


def test_quarantine_detail_stamps_row_column_and_replay_values():
    details: list[dict] = []
    apply_write_quarantine_matrix(
        [("ok", '["bad"]')],
        ["name", "counts"],
        ["TEXT", "ARRAY<BIGINT>"],
        details,
        policy="quarantine",
        dialect_label="BigQuery",
    )
    assert len(details) == 1
    detail = details[0]
    assert detail["row"] == 1
    assert detail["column"] == "counts"
    assert detail["policy"] == "write_quarantine"
    # Replay needs the full destination-shaped row image.
    assert detail["values"]["name"] == "ok"
