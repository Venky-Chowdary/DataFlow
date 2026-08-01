"""BigQuery/Redshift advisory keys + SQLite UNIQUE + VSS + BQ STRING(n)."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import (  # noqa: E402
    _bigquery_fetch_primary_key,
    _bq_to_logical,
    _mark_unique_keys_advisory,
    _sqlite_fetch_unique_keys,
)
from services.type_system import (  # noqa: E402
    ddl_type,
    is_variation_insensitive_collation,
    parse_binary_carrier_width,
    parse_string_carrier_width,
    unique_equality_key,
)


def test_bigquery_pk_always_unenforced():
    tbl = SimpleNamespace(
        table_constraints=SimpleNamespace(
            primary_key=SimpleNamespace(columns=["id", "tenant_id"])
        )
    )
    meta = _bigquery_fetch_primary_key(tbl)
    assert meta["primary_key_columns"] == ["id", "tenant_id"]
    assert meta["unique_keys"][0]["enforced"] is False
    assert meta["unique_keys"][0]["primary"] is True


def test_bq_string_and_bytes_width_carriers():
    assert _bq_to_logical("STRING", max_length=32) == "STRING(32)"
    assert _bq_to_logical("BYTES", max_length=16) == "BINARY(16)"
    assert parse_string_carrier_width("STRING(32)") == 32
    assert parse_binary_carrier_width("BYTES(16)") == 16
    assert ddl_type("bigquery", "STRING(64)") == "STRING(64)"
    assert ddl_type("bigquery", "BINARY(16)") == "BYTES(16)"
    assert ddl_type("bigquery", "VARBINARY(32)") == "BYTES(32)"
    assert ddl_type("mysql", "BINARY(16)") == "BINARY(16)"
    assert ddl_type("mysql", "VARBINARY(32)") == "VARBINARY(32)"
    assert ddl_type("sqlserver", "VARBINARY(64)") == "VARBINARY(64)"
    assert ddl_type("snowflake", "BINARY(16)") == "BINARY(16)"
    assert ddl_type("redshift", "VARBINARY(128)") == "VARBYTE(128)"
    assert ddl_type("snowflake", "VARCHAR(100)") == "VARCHAR(100)"


def test_redshift_advisory_marks_unenforced():
    meta = _mark_unique_keys_advisory(
        {
            "primary_key_columns": ["id"],
            "unique_keys": [
                {"name": "pk", "columns": ["id"], "primary": True},
                {"name": "uq", "columns": ["email"], "primary": False},
            ],
        }
    )
    assert all(u["enforced"] is False for u in meta["unique_keys"])


def test_integrity_skips_bigquery_advisory_pk_on_append():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "id", "target": "id"}],
        [{"id": "1"}, {"id": "1"}],
        "strict",
        dest_kind="bigquery",
        primary_key="id",
        sync_mode="append",
        destination_pk_columns=["id"],
        destination_unique_keys=[
            {
                "name": "PRIMARY",
                "columns": ["id"],
                "primary": True,
                "enforced": False,
            }
        ],
        target_types={"id": "STRING"},
    )
    assert result["passed"] is True
    warnings = " ".join(result.get("warnings") or [])
    assert "NOT ENFORCED" in warnings
    assert "duplicate" in warnings.lower()


def test_integrity_blocks_sqlite_enforced_unique():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [{"email": "a"}, {"email": "a"}],
        "strict",
        dest_kind="sqlite",
        primary_key="id",
        sync_mode="append",
        destination_unique_keys=[
            {
                "name": "uq_email",
                "columns": ["email"],
                "enforced": True,
            }
        ],
        target_types={"email": "TEXT", "id": "INTEGER"},
    )
    assert result["passed"] is False


def test_sqlite_fetch_unique_keys_from_pragma():
    cur = MagicMock()
    # index_list then index_info
    cur.fetchall.side_effect = [
        [(0, "uq_email", 1, "u", 0)],
        [(0, 0, "email")],
    ]
    info_rows = [
        (0, "id", "INTEGER", 1, None, 1),
        (1, "email", "TEXT", 0, None, 0),
    ]
    meta = _sqlite_fetch_unique_keys(cur, '"users"', info_rows)
    assert meta["primary_key_columns"] == ["id"]
    names = {u["name"]: u for u in meta["unique_keys"]}
    assert names["PRIMARY"]["enforced"] is True
    assert names["uq_email"]["columns"] == ["email"]


def test_sqlite_refuses_utf8_invent_on_invalid_base64():
    import pytest
    from connectors.sqlite_writer import _to_sqlite_value

    with pytest.raises(ValueError, match="refuse silent UTF-8 encode"):
        _to_sqlite_value("not-valid-base64!!!", "BINARY")


def test_sqlite_quarantine_holds_invalid_binary_before_convert():
    from connectors.writer_common import quarantine_unfit_binaries

    details: list[dict] = []
    out = quarantine_unfit_binaries(
        [("not-valid-base64!!!",), ("AQID",)],
        ["blob"],
        ["BINARY(16)"],
        details,
        policy="quarantine",
        dialect_label="SQLite BLOB",
    )
    assert len(out) == 1
    assert details and "refuse silent UTF-8 encode" in details[0]["reason"]


def test_vss_insensitive_strips_variation_selectors():
    ddl = "NVARCHAR(20) COLLATE Japanese_Bushu_Kakusu_140_CI_AS"
    assert is_variation_insensitive_collation(ddl)
    assert not is_variation_insensitive_collation(
        "NVARCHAR(20) COLLATE Japanese_Bushu_Kakusu_140_CI_AS_VSS"
    )
    base = "葛"
    vs = base + "\ufe00"  # VS1
    assert unique_equality_key(base, ddl) == unique_equality_key(vs, ddl)
    assert unique_equality_key(
        base, "NVARCHAR(20) COLLATE Japanese_Bushu_Kakusu_140_CI_AS_VSS"
    ) != unique_equality_key(
        vs, "NVARCHAR(20) COLLATE Japanese_Bushu_Kakusu_140_CI_AS_VSS"
    )
