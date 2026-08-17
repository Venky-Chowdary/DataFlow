"""Wave 88 — BigQuery JSON wire fidelity + array/JSON gate wiring.

Three defects proved here, all found by writing to a live BigQuery endpoint
rather than trusting string assertions:

1. ``Decimal`` / ``UUID`` / ``bytes`` cells aborted the whole transfer with
   ``Object of type Decimal is not JSON serializable``. BigQuery's JSON wire has
   no NUMERIC notion, so exact decimals must travel as text; a JSON number is
   read as FLOAT64 and silently loses digits.
2. Integers beyond ±(2^53−1) were sent as JSON numbers. BigQuery's loading docs
   are explicit: "pass it as a string to avoid data corruption" (RFC 7159 §6).
3. ``ARRAY<T>`` columns received a JSON *string*. A REPEATED field rejects text
   outright — the live endpoint answers ``invalid value type string for ARRAY
   column`` — so every row carrying a typed array failed to land.

The array/JSON element gates added in wave 86 also never ran for the five
writers that call the scalar gates individually instead of going through
``apply_write_quarantine_matrix``, so BigQuery/Snowflake array rules were dead
code at the destinations that have real ARRAY types.
"""

from __future__ import annotations

import ast
import json
import socket
import sys
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from connectors.warehouse_temporal import (  # noqa: E402
    bigquery_json_cell,
    bigquery_repeated_element,
    records_for_bigquery,
)

CONNECTORS = Path(__file__).resolve().parents[1] / "connectors"


# --------------------------------------------------------------------------
# 1. JSON wire fidelity — exact digits or nothing
# --------------------------------------------------------------------------


def test_decimal_reaches_bigquery_as_exact_text_not_float():
    """The reported abort: Decimal cells must serialize, and keep every digit."""
    value = Decimal("12345678901234567890.123456789")
    cell = bigquery_json_cell(value)

    assert cell == "12345678901234567890.123456789"
    assert json.dumps({"amt": cell})
    # A float round-trip would have silently truncated to 17 significant digits.
    assert cell != repr(float(value))
    assert Decimal(cell) == value


def test_bignumeric_scale_38_survives_the_wire():
    value = Decimal("1.00000000000000000000000000000000000001")
    assert Decimal(bigquery_json_cell(value)) == value


def test_integers_past_2_53_travel_as_string_smaller_stay_numbers():
    """BigQuery docs: ints outside the exact JSON range corrupt unless stringified."""
    assert bigquery_json_cell(2**53 - 1) == 2**53 - 1
    assert bigquery_json_cell(9223372036854775807) == "9223372036854775807"
    assert bigquery_json_cell(-9223372036854775808) == "-9223372036854775808"
    # Ordinary ids must not become strings — that would change the JSON type.
    assert bigquery_json_cell(42) == 42
    assert isinstance(bigquery_json_cell(42), int)


def test_binary_uuid_and_float_carriers_are_json_safe():
    assert bigquery_json_cell(b"\x00\x01\xff") == "AAH/"
    assert bigquery_json_cell(memoryview(b"ab")) == "YWI="
    assert bigquery_json_cell(uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")) == (
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    )
    # FLOAT64 semantics stay exactly as the source produced them.
    assert bigquery_json_cell(1.5) == 1.5
    assert isinstance(bigquery_json_cell(1.5), float)


def test_whole_typed_record_serializes_with_json_dumps():
    from datetime import timezone

    cols = ["id", "amt", "big", "blob", "guid", "ts"]
    types = ["INT64", "BIGNUMERIC", "INT64", "BYTES", "STRING", "TIMESTAMP"]
    # TIMESTAMP requires offset/Z — naive wall-clock is refuse (no UTC invent).
    row = (
        1,
        Decimal("10.5000"),
        9223372036854775807,
        b"\x00\xff",
        uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
        datetime(2024, 3, 10, 12, 34, 56, 123456, tzinfo=timezone.utc),
    )
    rec = records_for_bigquery([row], cols, types)[0]

    # allow_nan=False mirrors what the transport actually accepts.
    assert json.dumps(rec, allow_nan=False)
    assert rec["amt"] == "10.5000", "trailing scale carries the declared precision"
    assert isinstance(rec["ts"], str) and rec["ts"].endswith("Z")


# --------------------------------------------------------------------------
# 2. REPEATED vs JSON columns are different wire shapes
# --------------------------------------------------------------------------


def test_only_parameterized_arrays_are_repeated_fields():
    assert bigquery_repeated_element("ARRAY<STRING>") == "STRING"
    assert bigquery_repeated_element("array<numeric>") == "numeric"
    assert bigquery_repeated_element("ARRAY<STRUCT<a INT64>>") == "STRUCT<a INT64>"
    # Bare array/json map to a BigQuery JSON column, where text is correct.
    assert bigquery_repeated_element("JSON") is None
    assert bigquery_repeated_element("array") is None
    assert bigquery_repeated_element("STRING") is None
    assert bigquery_repeated_element("") is None


@pytest.mark.parametrize(
    "wire",
    [
        ["a", "b"],                # driver-native list (psycopg text[])
        '{a,b}',                   # Postgres array literal
        '["a","b"]',               # JSON text from an upstream serializer
    ],
    ids=["pylist", "pg_literal", "json_text"],
)
def test_every_array_wire_form_becomes_a_real_json_array(wire):
    """A REPEATED column rejects text, so all three wire forms must widen to a list."""
    rec = records_for_bigquery([("1", wire)], ["id", "tags"], ["STRING", "ARRAY<STRING>"])[0]
    assert rec["tags"] == ["a", "b"], f"{wire!r} did not widen to a JSON array"
    assert isinstance(rec["tags"], list)


def test_quoted_postgres_element_keeps_its_embedded_comma():
    rec = records_for_bigquery(
        [("1", '{a,"b,c"}')], ["id", "tags"], ["STRING", "ARRAY<STRING>"]
    )[0]
    assert rec["tags"] == ["a", "b,c"], "quoted element was split on its own comma"


def test_array_elements_keep_exact_decimal_digits():
    rec = records_for_bigquery(
        [("1", [Decimal("0.1"), Decimal("12345678901234567890.123456789")])],
        ["id", "nums"],
        ["STRING", "ARRAY<BIGNUMERIC>"],
    )[0]
    assert rec["nums"] == ["0.1", "12345678901234567890.123456789"]


def test_temporal_array_elements_are_normalized_per_element():
    rec = records_for_bigquery(
        [("1", [date(2024, 3, 10), date(2025, 1, 1)])],
        ["id", "days"],
        ["STRING", "ARRAY<DATE>"],
    )[0]
    assert rec["days"] == ["2024-03-10", "2025-01-01"]


def test_json_column_still_receives_document_text_not_a_list():
    rec = records_for_bigquery(
        [("1", '{"k":1}', '["x"]')], ["id", "meta", "raw"], ["STRING", "JSON", "JSON"]
    )[0]
    assert rec["meta"] == '{"k":1}'
    assert rec["raw"] == '["x"]', "a JSON column must not be widened into an array"


def test_unparseable_array_payload_is_not_invented_into_an_array():
    """Ambiguity must reach the destination as-is, never become a fabricated array."""
    rec = records_for_bigquery(
        [("1", "not-an-array")], ["id", "tags"], ["STRING", "ARRAY<STRING>"]
    )[0]
    assert rec["tags"] == "not-an-array"


# --------------------------------------------------------------------------
# 3. Gate wiring — the wave 86 gates must actually run
# --------------------------------------------------------------------------


def _called_functions(module_name: str) -> set[str]:
    tree = ast.parse((CONNECTORS / f"{module_name}.py").read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            names.add(node.func.id)
    return names


@pytest.mark.parametrize("writer", ["bigquery_writer", "snowflake_writer"])
def test_writers_with_native_array_types_run_the_array_gate(writer):
    """These map ``array<T>`` onto a real ARRAY column, so the gate is load-bearing."""
    called = _called_functions(writer)
    assert "quarantine_unfit_arrays" in called, (
        f"{writer} gates scalars but not ARRAY elements — NULL elements and "
        "arrays of arrays would reach the destination unchecked"
    )
    assert "quarantine_unfit_json" in called


@pytest.mark.parametrize("writer", ["bigquery_writer", "snowflake_writer"])
def test_api_writers_that_gate_scalars_also_gate_json_documents(writer):
    """These build request payloads directly, so each runs the gate itself."""
    called = _called_functions(writer)
    assert "quarantine_unfit_strings" in called, "precondition: scalar gates present"
    assert "quarantine_unfit_json" in called, (
        f"{writer} would let a malformed document degrade into JSON text"
    )


@pytest.mark.parametrize("engine", ["postgresql", "mysql"])
def test_sql_bind_quarantines_a_truncated_json_document(engine):
    """SQL writers inherit the document gate from the shared bind path.

    A truncated payload binds without raising — ``coerce_json_wire`` wraps it as
    a JSON string — so JSONB would hold text, and row count and checksum would
    both still agree. It must be held out, and a bare scalar must not be.
    """
    from connectors.writer_common import bind_rows_keeping_numbers

    rejected: list[dict] = []
    bound, kept = bind_rows_keeping_numbers(
        [("1", '{"k": 1'), ("2", '{"k": 1}'), ("3", "plain text")],
        ["id", "doc"],
        ["VARCHAR(10)", "JSON" if engine == "mysql" else "JSONB"],
        rejected,
        "quarantine",
        engine=engine,
        dialect_label=engine,
        row_numbers=[11, 12, 13],
    )

    assert kept == [12, 13], bound
    assert len(rejected) == 1
    assert rejected[0]["row"] == 11
    assert rejected[0]["column"] == "doc"


def test_bigquery_null_array_element_is_quarantined_not_written():
    """Real BigQuery: "Array cannot have a null element". Emulators are lenient,
    so this must be caught by our gate rather than by the destination."""
    from connectors.writer_common import quarantine_unfit_arrays

    rejected: list[dict] = []
    kept = quarantine_unfit_arrays(
        [("1", ["a", None])],
        ["id", "tags"],
        ["STRING", "ARRAY<STRING>"],
        rejected,
        "quarantine",
        dialect_label="BigQuery",
    )
    assert kept == [], "row with a NULL array element must not reach BigQuery"
    assert rejected and "null" in rejected[0]["reason"].lower()


def test_postgres_native_array_column_still_permits_null_elements():
    """Same gate, different dialect: PG int[]/text[] legitimately hold NULLs."""
    from connectors.writer_common import quarantine_unfit_arrays

    rejected: list[dict] = []
    kept = quarantine_unfit_arrays(
        [("1", ["a", None])],
        ["id", "tags"],
        ["STRING", "text[]"],
        rejected,
        "quarantine",
        dialect_label="PostgreSQL",
    )
    assert kept == [("1", ["a", None])]
    assert rejected == []


# --------------------------------------------------------------------------
# 4. Live proof against a real BigQuery endpoint
# --------------------------------------------------------------------------

BQ_HOST, BQ_PORT = "localhost", 9050
BQ_BASE = f"http://{BQ_HOST}:{BQ_PORT}/bigquery/v2/projects/dataflow-test"


def _bq_post(path: str, payload: dict) -> dict:
    req = urllib.request.Request(
        f"{BQ_BASE}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read() or b"{}")


def _require_bigquery() -> None:
    try:
        with socket.create_connection((BQ_HOST, BQ_PORT), timeout=3):
            pass
    except OSError:
        pytest.skip(f"BigQuery endpoint {BQ_HOST}:{BQ_PORT} not reachable")


def test_repeated_and_numeric_columns_accept_our_records_live():
    """End-to-end: build records, land them, read back exact digits."""
    _require_bigquery()
    table = f"wave88_arr_{uuid.uuid4().hex[:10]}"
    created = _bq_post(
        "/datasets/dataflow/tables",
        {
            "tableReference": {
                "projectId": "dataflow-test",
                "datasetId": "dataflow",
                "tableId": table,
            },
            "schema": {
                "fields": [
                    {"name": "id", "type": "STRING"},
                    {"name": "tags", "type": "STRING", "mode": "REPEATED"},
                    {"name": "nums", "type": "BIGNUMERIC", "mode": "REPEATED"},
                    {"name": "amt", "type": "BIGNUMERIC"},
                    {"name": "big", "type": "INT64"},
                    {"name": "meta", "type": "JSON"},
                ]
            },
        },
    )
    if "error" in created:
        pytest.skip(f"cannot create BigQuery table: {created['error']}")

    exact = Decimal("12345678901234567890.123456789")
    records = records_for_bigquery(
        [("r1", '{a,"b,c"}', [Decimal("0.1"), exact], exact, 9223372036854775807, '{"k":1}')],
        ["id", "tags", "nums", "amt", "big", "meta"],
        ["STRING", "ARRAY<STRING>", "ARRAY<BIGNUMERIC>", "BIGNUMERIC", "INT64", "JSON"],
    )
    insert = _bq_post(
        f"/datasets/dataflow/tables/{table}/insertAll",
        {"rows": [{"json": r} for r in records]},
    )
    assert "error" not in insert, insert.get("error")
    assert not insert.get("insertErrors"), insert.get("insertErrors")

    result = _bq_post(
        "/queries",
        {
            "query": (
                "SELECT id, ARRAY_LENGTH(tags) AS n_tags, "
                "CAST(amt AS STRING) AS amt_text, CAST(big AS STRING) AS big_text, "
                "CAST(nums[OFFSET(1)] AS STRING) AS num1 "
                f"FROM `dataflow-test.dataflow.{table}`"
            ),
            "useLegacySql": False,
        },
    )
    assert "error" not in result, result.get("error")
    row = [cell["v"] for cell in result["rows"][0]["f"]]
    assert row[0] == "r1"
    assert int(row[1]) == 2, "quoted Postgres element must stay a single element"
    assert Decimal(row[2]) == exact, f"BIGNUMERIC lost digits: {row[2]}"
    assert row[3] == "9223372036854775807", f"INT64 corrupted: {row[3]}"
    assert Decimal(row[4]) == exact, f"array element lost digits: {row[4]}"


def test_json_string_into_repeated_column_is_what_bigquery_rejects():
    """Pins the root cause so nobody reintroduces the JSON-text shortcut."""
    _require_bigquery()
    table = f"wave88_rej_{uuid.uuid4().hex[:10]}"
    created = _bq_post(
        "/datasets/dataflow/tables",
        {
            "tableReference": {
                "projectId": "dataflow-test",
                "datasetId": "dataflow",
                "tableId": table,
            },
            "schema": {
                "fields": [
                    {"name": "id", "type": "STRING"},
                    {"name": "tags", "type": "STRING", "mode": "REPEATED"},
                ]
            },
        },
    )
    if "error" in created:
        pytest.skip(f"cannot create BigQuery table: {created['error']}")

    bad = _bq_post(
        f"/datasets/dataflow/tables/{table}/insertAll",
        {"rows": [{"json": {"id": "1", "tags": '["a","b"]'}}]},
    )
    combined = json.dumps(bad).lower()
    assert "error" in bad or bad.get("insertErrors"), (
        "expected BigQuery to refuse text in a REPEATED column"
    )
    assert "array" in combined

    good = _bq_post(
        f"/datasets/dataflow/tables/{table}/insertAll",
        {"rows": [{"json": {"id": "2", "tags": ["a", "b"]}}]},
    )
    assert "error" not in good and not good.get("insertErrors"), good
