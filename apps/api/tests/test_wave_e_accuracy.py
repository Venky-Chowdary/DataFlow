"""Wave E accuracy: precision-collapse, TZ polarity, binary inference, ES identity, Oracle redo."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import sqlalchemy as sa

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_precision_collapse_helper():
    from services.type_system import is_precision_collapse_coercion

    assert is_precision_collapse_coercion("FLOAT", "DECIMAL(12,4)") is True
    assert is_precision_collapse_coercion("float", "integer") is True
    assert is_precision_collapse_coercion("DECIMAL", "INTEGER") is True
    assert is_precision_collapse_coercion("TIMESTAMPTZ", "DATE") is True
    assert is_precision_collapse_coercion("VARCHAR", "INTEGER") is False
    assert is_precision_collapse_coercion("INTEGER", "DECIMAL") is False


def test_schema_drift_float_to_decimal_not_soft_passed():
    from services.schema_drift import detect_schema_drift

    report = detect_schema_drift(
        source_columns=["amt"],
        source_schema={"amt": "FLOAT"},
        target_columns=["amt"],
        target_schema={"amt": "DECIMAL(12,4)"},
        mappings=[{"source": "amt", "target": "amt", "confidence": 0.99}],
        destination_db_type="postgresql",
        sample_rows=[{"amt": "1.5"}, {"amt": "2.25"}],
    )
    assert report["severity"] == "breaking"
    assert any(m.get("reason") == "precision_collapse" for m in report["type_mismatches"])


def test_generic_sql_datetime_preserves_timezone_polarity():
    from connectors.generic_sql import _logical_type_from_sa, _sa_type_for_logical

    assert _logical_type_from_sa(sa.DateTime(timezone=True)) == "timestamptz"
    assert _logical_type_from_sa(sa.DateTime(timezone=False)) == "timestamp_ntz"
    tz = _sa_type_for_logical("timestamptz", "postgresql", "postgresql")
    ntz = _sa_type_for_logical("timestamp_ntz", "postgresql", "postgresql")
    assert getattr(tz, "timezone", None) is True
    assert getattr(ntz, "timezone", None) in (False, None)


def test_binary_inference_requires_name_or_strong_payload():
    from services.schema_inference import infer_type

    assert infer_type(["SGVsbG8gV29ybGQ="]) == "VARCHAR"
    assert infer_type(["SGVsbG8gV29ybGQ="], field_name="payload_b64") == "BINARY"
    # Longer padded base64 without a binary-ish name still qualifies.
    long_b64 = "U29tZVNlbnNpYmxlQmluYXJ5UGF5bG9hZERhdGFGb3JUZXN0cw=="
    assert infer_type([long_b64]) == "BINARY"


def test_es_upsert_derives_id_from_conflict_columns():
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True
    captured: list[dict] = []

    def fake_bulk(_client, actions, raise_on_error=False):
        items = list(actions)
        captured.extend(items)
        return (len(items), [])

    with patch("connectors.elasticsearch_writer._client", return_value=client):
        with patch("elasticsearch.helpers.bulk", side_effect=fake_bulk):
            result = write_mapped_rows(
                host="localhost",
                port=9200,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="events",
                headers=["order_id", "amt"],
                data_rows=[["o-1", "10"]],
                mappings=[
                    {"source": "order_id", "target": "order_id", "transform": "direct"},
                    {"source": "amt", "target": "amt", "transform": "direct"},
                ],
                column_types={"order_id": "TEXT", "amt": "TEXT"},
                create_table=False,
                write_mode="upsert",
                conflict_columns=["order_id"],
                error_policy="fail",
            )

    assert result.ok is True
    assert captured and captured[0].get("_id") == "o-1"


def test_es_upsert_fails_closed_without_identity():
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True

    with patch("connectors.elasticsearch_writer._client", return_value=client):
        with patch("elasticsearch.helpers.bulk", return_value=(0, [])):
            result = write_mapped_rows(
                host="localhost",
                port=9200,
                database="",
                username="",
                password="",
                schema="",
                connection_string="",
                ssl=False,
                table_name="events",
                headers=["note"],
                data_rows=[["hello"]],
                mappings=[{"source": "note", "target": "note", "transform": "direct"}],
                column_types={"note": "TEXT"},
                create_table=False,
                write_mode="upsert",
                conflict_columns=[],
                error_policy="fail",
            )

    assert result.ok is False
    assert "identity" in (result.error or "").lower()


def test_reconciliation_refuses_unproven_identity():
    from services.reconciliation import build_reconciliation_proof

    proof = build_reconciliation_proof(
        [{"name": "a"}, {"name": "b"}],
        [{"name": "a"}, {"name": "b"}],
        [{"source": "name", "target": "name"}],
        primary_key=None,
    )
    assert proof["passed"] is False
    assert proof["verification_mode"] == "unproven_identity"
    assert proof["row_fidelity_score"] == 0.0


def test_reconciliation_refuses_duplicate_keys():
    from services.reconciliation import build_reconciliation_proof

    proof = build_reconciliation_proof(
        [{"id": "1", "v": "a"}, {"id": "1", "v": "b"}],
        [{"id": "1", "v": "a"}],
        [{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
        primary_key="id",
    )
    assert proof["passed"] is False
    assert proof["verification_mode"] == "positional_only"


def test_oracle_logminer_parses_quoted_comma_and_to_date():
    from connectors.oracle_logminer import _parse_sql_redo

    sql = (
        'INSERT INTO "APP"."ORDERS"("ID","NOTE","TS") '
        "VALUES('1','hello, world',TO_DATE('2024-01-15 10:00:00','YYYY-MM-DD HH24:MI:SS'))"
    )
    row = _parse_sql_redo(sql, op="insert")
    assert row.get("ID") == "1"
    assert row.get("NOTE") == "hello, world"
    assert "TO_DATE" in (row.get("TS") or "")
    assert "_df_unparsed_sql_redo" not in row


def test_oracle_logminer_mismatch_refuses_corruption():
    from connectors.oracle_logminer import _parse_sql_redo

    sql = 'INSERT INTO T("A","B") VALUES(\'only-one\')'
    row = _parse_sql_redo(sql, op="insert")
    assert row.get("_df_unparsed_sql_redo") == "1"
