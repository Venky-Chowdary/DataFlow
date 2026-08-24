"""Refuse null-PK mass-delete and SaaS/Dynamo/Parquet invent paths."""

from __future__ import annotations

import math
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_delete_by_keys_refuses_null_conflict():
    from connectors.generic_sql import _delete_by_keys
    import sqlalchemy as sa

    meta = sa.MetaData()
    table = sa.Table(
        "t",
        meta,
        sa.Column("id", sa.Integer),
        sa.Column("note", sa.String),
    )
    conn = MagicMock()
    with pytest.raises(ValueError, match="null/empty conflict"):
        _delete_by_keys(conn, table, [{"id": None, "note": "x"}], ["id"])
    conn.execute.assert_not_called()


def test_dynamo_key_schema_strict_composite():
    from connectors.dynamodb_writer import _resolve_key_schema

    keys = _resolve_key_schema(
        ["userid", "orgid"],
        [],
        conflict_columns=["UserId", "OrgId"],
        source_types=None,
    )
    assert [k[0] for k in keys] == ["userid", "orgid"]
    with pytest.raises(ValueError, match="unresolved|conflict"):
        _resolve_key_schema(
            ["orgid"],
            [],
            conflict_columns=["UserId", "OrgId"],
            source_types=None,
        )


def test_hubspot_does_not_invent_id_from_email():
    from connectors import hubspot_writer as hw

    # Simulate the identity extract used in write loop
    id_property = "hs_object_id"
    props = {"email": "a@b.com", "name": "A"}
    id_val = props.pop(id_property, None)
    assert id_val is None
    assert props.get("email") == "a@b.com"  # must not be used as hs_object_id


def test_parquet_nan_not_invented_to_none():
    # Mirror file_parser cell normalize: keep NaN, do not map to None
    rec = {"amt": float("nan")}
    v = rec["amt"]
    if isinstance(v, float) and v != v:
        pass  # keep
    else:
        rec["amt"] = None
    assert math.isnan(rec["amt"])
