"""D3 — a keyed incremental Snowflake→Snowflake run earns an independent
bounded-window source re-read (M3 machinery is engine-agnostic; Snowflake is
in SNAPSHOT_SCAN_SOURCES and its keyed modes already decline the COPY fast
path, so the row path's window machinery applies). Before this fix the
verification ladder had no Snowflake population read — every Snowflake
destination declined with ``destination_engine_not_in_ladder`` and the run
could only report write-pass readback.

A clean run verifies with the destination digest scoped to the batch keys;
a divergence inside the ladder's population read is caught by the same
five-layer check every other engine runs.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fakesnow", reason="requires the optional Snowflake test dependency")

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import src.transfer.engine as engine_mod  # noqa: E402
from connectors.snowflake_conn import get_connection  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _conn():
    return get_connection(
        account="localhost",
        username="test",
        password="test",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )


def _exec(sql: str, params=None) -> None:
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params) if params else cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _seed() -> tuple[str, str]:
    src = f"d3_src_{uuid.uuid4().hex[:8]}"
    dst = f"d3_dst_{uuid.uuid4().hex[:8]}"
    _exec(f'DROP TABLE IF EXISTS "{src}"')
    _exec(f'DROP TABLE IF EXISTS "{dst}"')
    # VARCHAR keys: the composite keyset page sends the bookmark as text
    # params, and real Snowflake implicit-casts them to the column type while
    # fakesnow's duckdb backend refuses a BIGINT-vs-VARCHAR tuple compare.
    # Text columns keep the keyed-incremental contract under test identical.
    _exec(
        f'CREATE TABLE "{src}" (id VARCHAR PRIMARY KEY, seq VARCHAR NOT NULL, v VARCHAR)'
    )
    _exec(
        f'INSERT INTO "{src}" (id, seq, v) VALUES '
        "('1', '1', 'a'), ('2', '2', 'b'), ('3', '3', 'c')"
    )
    _exec(
        f'CREATE TABLE "{dst}" (id VARCHAR PRIMARY KEY, seq VARCHAR NOT NULL, v VARCHAR)'
    )
    return src, dst


def _request(src: str, dst: str) -> TransferRequest:
    endpoint = {
        "kind": "database",
        "format": "snowflake",
        "host": "localhost",
        "port": 443,
        "database": "dataflow",
        "username": "test",
        "password": "test",
        "schema": "public",
    }
    return TransferRequest(
        source=EndpointConfig(**endpoint, table=src),
        destination=EndpointConfig(**endpoint, table=dst),
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "seq", "target": "seq", "confidence": 0.99},
            {"source": "v", "target": "v", "confidence": 0.99},
        ],
        sync_mode="incremental_deduped",
        stream_contracts=[
            {
                "name": "t",
                "sync_mode": "incremental_deduped",
                "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": "id",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="balanced",
    )


def _run(fake, src: str, dst: str):
    job_id = "d3" + uuid.uuid4().hex[:20]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)


def test_snowflake_keyed_incremental_clean_run_is_independently_reread(
    tmp_path: Path, isolated
) -> None:
    src, dst = _seed()
    first = _run(isolated, src, dst)
    assert first.success, first.error

    _exec(f'UPDATE "{src}" SET seq=\'6\', v=\'b2\' WHERE id=\'2\'')
    _exec(
        f'INSERT INTO "{src}" (id, seq, v) VALUES '
        "('4', '4', 'd'), ('5', '5', 'e')"
    )

    second = _run(isolated, src, dst)
    assert second.success, second.error
    recon = second.reconciliation or {}
    assert recon.get("passed") is True, recon
    assert recon.get("checksum_match") is True, recon
    assert recon.get("verification_verdict") == "verified", recon
    assert recon.get("source_independently_reread") is True, recon
    assert recon.get("checksum_scope") == "written_batch_keys", recon
    # The window is the written batch {2,4,5}; the destination holds all 5.
    assert recon.get("source_rows") == 3, recon
    assert recon.get("target_rows") == 5, recon


def test_snowflake_ladder_population_read_catches_window_divergence() -> None:
    """The Snowflake population read feeds the same five-layer ladder as every
    other engine: a tampered/missing row inside the read-back population must
    fail the digest. (A mid-run dest tamper cannot be simulated without a
    trigger — Snowflake upsert self-heals covered rows by rewriting them, and
    fakesnow exposes no post-write hook — so the divergence is exercised at
    the layer that owns the verdict.)"""
    from services.verification_ladder import (
        read_snowflake_rows,
        run_five_layer_verification,
    )

    src, dst = _seed()
    _exec(
        f'INSERT INTO "{dst}" (id, seq, v) VALUES '
        "('1', '1', 'a'), ('2', '2', 'b'), ('3', '3', 'c')"
    )
    columns = ["id", "seq", "v"]
    source = read_snowflake_rows(
        host="localhost",
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=src,
        columns=columns,
    )
    target = read_snowflake_rows(
        host="localhost",
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=dst,
        columns=columns,
    )
    assert len(source) == len(target) == 3

    clean = run_five_layer_verification(
        source_rows=source, target_rows=target, columns=columns, pk_column="id"
    )
    assert clean["passed"] is True, clean

    # Same divergence the re-read would surface: one cell changed, one row
    # dropped — both must flip the verdict.
    tampered = [dict(r) for r in target]
    tampered[1]["v"] = "TAMPERED"
    result = run_five_layer_verification(
        source_rows=source, target_rows=tampered, columns=columns, pk_column="id"
    )
    assert result["passed"] is False, result
    dropped = target[:1] + target[2:]
    result = run_five_layer_verification(
        source_rows=source, target_rows=dropped, columns=columns, pk_column="id"
    )
    assert result["passed"] is False, result


def test_snowflake_population_read_honours_ladder_budget() -> None:
    from services.verification_ladder import (
        PopulationTooLarge,
        read_snowflake_rows,
    )

    src, _dst = _seed()
    with pytest.raises(PopulationTooLarge):
        read_snowflake_rows(
            host="localhost",
            database="dataflow",
            username="test",
            password="test",
            schema="public",
            table=src,
            columns=["id", "seq", "v"],
            max_rows=2,
        )


def test_culprit_census_conn_supports_snowflake() -> None:
    """The failure-path key census advertised snowflake in its own
    'has no census driver' message but _culprit_census_conn had no
    snowflake branch — a failed Snowflake run could never name culprit
    keys. Fails on c6c16128 (returns None)."""
    from src.transfer.reconcile_step import _culprit_census_conn

    info = _culprit_census_conn(
        "snowflake",
        {
            "host": "localhost",
            "username": "test",
            "password": "test",
            "database": "dataflow",
            "schema": "public",
            "warehouse": "",
        },
    )
    assert info is not None
    eng, conn = info
    assert eng == "snowflake"
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() is not None
    conn.close()
