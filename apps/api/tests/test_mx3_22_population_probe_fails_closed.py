"""MX3-22 — a source probe that ERRORS must fail closed with a named reason.

QA: when the table population walk raised (host unresolvable during tunnel
rotation), Validate logged "Validate will use the preview" and g3f reported
"No unfit value in 10 scanned row(s)…" as a warn — the run was approved even
though the overflowing row was never read. The duplicate-key probe path
(session A, ``data_integrity``) is not touched here.
"""

from __future__ import annotations

import socket
import uuid
from unittest.mock import patch

import pytest


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")


@pytest.fixture
def narrow_source():
    import psycopg2

    table = f"mx322_{uuid.uuid4().hex[:6]}"
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, name TEXT)")
        # Preview rows fit VARCHAR(5); row 99 beyond the preview does not.
        cur.executemany(
            f"INSERT INTO {table} VALUES (%s,%s)",
            [(i, "ab") for i in range(1, 50)] + [(99, "much-too-long-value")],
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _preflight(table: str):
    from services.preflight_service import run_file_preflight
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    mappings = _sign_required_risk_contracts(
        [
            {"source": "id", "target": "id", "confidence": 0.99, "transform": None,
             "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "name", "target": "name", "confidence": 0.99, "transform": None,
             "source_type": "TEXT", "target_type": "VARCHAR(5)"},
        ],
        {"execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "accepted"},
        table=table,
    )
    return run_file_preflight(
        columns=["id", "name"], column_types={"id": "INTEGER", "name": "TEXT"}, row_count=50,
        mappings=mappings, destination_connected=True, destination_can_create=True,
        source_connected=True, source_kind="database", source_format="postgresql",
        sync_mode="full_refresh_append",
        sample_rows=[{"id": i, "name": "ab"} for i in range(1, 11)],
        destination_db_type="postgresql",
        source_config={
            "type": "postgresql", "host": "localhost", "port": 5432, "database": "dataflow",
            "username": "dataflow", "password": "dataflow", "schema": "public",
        },
        source_table=table, destination_table=f"{table}_d", destination_table_exists=True,
        destination_column_types={"id": "INTEGER", "name": "VARCHAR(5)"},
        validation_mode="strict",
    )


def _gate(result, gate_id="g3f_population_fit"):
    return next(g for g in result["gates"] if g.get("id") == gate_id)


def test_healthy_walk_forecasts_the_held_out_row(narrow_source):
    result = _preflight(narrow_source)
    gate = _gate(result)
    assert gate["status"] == "warn" and "1 row(s) will be held out" in gate["message"], gate


def test_walk_error_blocks_with_a_named_probe_failure(narrow_source):
    import services.preflight_service as ps

    def _raise(**_kw):
        raise RuntimeError('could not translate host name "tunnel-host" to address')

    with patch.object(ps, "_iter_table_population_for_preflight", _raise):
        result = _preflight(narrow_source)
    gate = _gate(result)
    assert gate["status"] == "block", gate
    assert gate["message"].startswith("Population probe failed"), gate["message"]
    assert "tunnel-host" in gate["message"]
    assert gate["details"]["probe_failed"] is True
    assert result["passed"] is False
    decision = result["proof_bundle"]["transfer_decision"]["decision"]
    assert decision != "approve", decision
    # A probe failure is not a data finding: never reported as duplicate keys.
    assert not any("duplicate" in str(b.get("message", "")).lower() for b in result["blockers"])


def test_no_walker_keeps_the_sample_only_warning(narrow_source):
    """No walk available (not an error) stays the existing honest warn."""
    import services.preflight_service as ps

    with patch.object(ps, "_iter_table_population_for_preflight", lambda **_kw: None):
        result = _preflight(narrow_source)
    gate = _gate(result)
    assert gate["status"] == "warn" and "not population-proven" in gate["message"], gate
