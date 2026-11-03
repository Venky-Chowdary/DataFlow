"""A PostgreSQL destination read still reports the table's keys.

The destination purpose skips sample-based type refinement (D190/D194). It
also skipped the primary-key / unique / FK / CHECK catalog reads bundled in
the same branch, so once the plan's existing-table read carried the purpose
(D244), an upsert into run 1's own keyed table planned CREATE UNIQUE INDEX.
"""
from __future__ import annotations

import uuid

import pytest

from services.schema_introspect import introspect_schema
from tests.test_quarantine_replay_live_matrix import _pg_cfg, _pg_exec


def test_destination_read_reports_primary_and_unique_keys():
    cfg = _pg_cfg()
    if not cfg:
        pytest.skip("PostgreSQL unavailable")
    table = f"d244k_{uuid.uuid4().hex[:8]}"
    _pg_exec(
        cfg,
        [
            f'CREATE TABLE "{table}" (id int PRIMARY KEY, code text UNIQUE, '
            "n int CHECK (n > 0))"
        ],
    )
    try:
        info = introspect_schema(
            "postgresql",
            host=cfg["host"],
            port=int(cfg["port"]),
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            schema=cfg.get("schema") or "public",
            table=table,
            strict_namespace=True,
            introspect_purpose="destination",
        )
        assert info.get("ok"), info
        assert info["primary_key_columns"] == ["id"]
        assert any(
            [str(c).lower() for c in (k.get("columns") if isinstance(k, dict) else k)]
            == ["code"]
            for k in info["unique_keys"]
        ), info["unique_keys"]
        assert info.get("check_constraints_meta"), info
    finally:
        _pg_exec(cfg, [f'DROP TABLE IF EXISTS "{table}"'])
