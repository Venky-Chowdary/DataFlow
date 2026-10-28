"""Gate-8 — an engine-digest pair must not be re-scoped to written keys.

PostgreSQL -> PostgreSQL ``incremental_append`` run 2 that also adds a column:
both populations were digested in their engines (and matched), then the keyed
read-back replaced only the *target* digest with a Python fingerprint of the
written rows. The two algorithms can never agree, so a correct load failed
"Checksum mismatch (strict)" after every row had landed.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG_PASSWORD = os.environ.get("PGPASSWORD") or "dataflow"


def test_append_that_adds_a_column_passes_gate8_on_matching_tables(tmp_path: Path, monkeypatch):
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError:
        pytest.skip("PostgreSQL not reachable on :5432")
    import psycopg2

    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    try:
        conn = psycopg2.connect(
            host="127.0.0.1", user="dataflow", password=PG_PASSWORD, dbname="dataflow"
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL credentials rejected: {exc}")
    conn.autocommit = True
    cur = conn.cursor()
    suffix = uuid.uuid4().hex[:8]
    src_t, dst_t = f"g8s_{suffix}", f"g8d_{suffix}"

    def endpoint(table):
        return EndpointConfig(
            kind="database", format="postgresql", host="127.0.0.1", port=5432,
            database="dataflow", username="dataflow", password=PG_PASSWORD,
            schema="public", table=table,
        )

    def run(**extra):
        request = TransferRequest(
            source=endpoint(src_t),
            destination=endpoint(dst_t),
            sync_mode="incremental_append",
            stream_contracts=[
                {
                    "name": src_t,
                    "cursor_field": "id",
                    "cursor_semantics": "monotonic_sequence",
                    "sync_mode": "incremental_append",
                    "selected": True,
                }
            ],
            validation_mode="strict",
            **extra,
        )
        job_id = "g8" + uuid.uuid4().hex[:16]
        fake.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(request, job_id)

    try:
        cur.execute(f"CREATE TABLE {src_t} (id INT PRIMARY KEY, name TEXT, qty INT)")
        cur.executemany(f"INSERT INTO {src_t} VALUES (%s,%s,%s)", [(i, f"n{i}", i) for i in range(1, 6)])
        assert run().success

        cur.execute(f"ALTER TABLE {src_t} ADD COLUMN extra TEXT")
        cur.executemany(
            f"INSERT INTO {src_t} VALUES (%s,%s,%s,%s)", [(i, f"n{i}", i, f"x{i}") for i in range(6, 9)]
        )
        result = run(schema_policy="propagate_columns")

        assert result.success, result.error
        cur.execute(f"SELECT count(*), count(extra) FROM {dst_t}")
        assert cur.fetchone() == (8, 3)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        cur.execute(f"DROP TABLE IF EXISTS {dst_t}")
        conn.close()
