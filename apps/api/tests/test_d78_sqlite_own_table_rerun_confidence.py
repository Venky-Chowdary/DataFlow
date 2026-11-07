"""D78 — run 2 into the SQLite table DataFlow created is blocked at G4.

Run 1 (PostgreSQL ``BIT(8)``, ``BIT(1)``, ``BOOLEAN`` -> new SQLite table)
creates ``VARCHAR(8)`` / ``INTEGER`` / ``INTEGER`` and verifies. Run 2 maps the
same columns onto those carriers and was refused "below the Map confidence
floor":

* the mapping pipeline reported the source as ``ddl_carrier_type``'s bare
  ``BINARY`` instead of ``BIT(8)``, so ``BINARY -> VARCHAR(8)`` scored as a
  lossy pair (and Map said CREATE ``BLOB`` while the writer created
  ``VARCHAR(8)``);
* the BOOLEAN -> INTEGER cast onto SQLite's own boolean carrier was graded a
  "custom transform · sparse sample" (0.70) on a small table.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

from services.mapping_pipeline import run_mapping_pipeline


def _run2(sources, targets):
    out = run_mapping_pipeline(
        [s["name"] for s in sources],
        [t["name"] for t in targets],
        source_schemas=sources,
        target_schemas=targets,
        destination_db_type="sqlite",
        destination_table_exists=True,
        source_db_type="postgresql",
        use_llm=False,
        source_types_authoritative=True,
    )
    return {m["source"]: m for m in out["mappings"]}


def test_bitstring_keeps_its_own_carrier_onto_the_created_varchar():
    by = _run2(
        [{"name": "bits", "inferred_type": "BIT(8)", "samples": ["10101010", "00001111"]}],
        [{"name": "bits", "inferred_type": "VARCHAR(8)"}],
    )
    row = by["bits"]
    assert row["source_type"] == "BIT(8)", row
    assert row["target"] == "bits"
    assert "lossy" not in row["reasoning"], row
    assert float(row["confidence"]) >= 0.85, row


def test_bitstring_create_new_projects_the_carrier_the_writer_creates():
    out = run_mapping_pipeline(
        ["bits"],
        [],
        source_schemas=[{"name": "bits", "inferred_type": "BIT(8)", "samples": ["10101010"]}],
        destination_db_type="sqlite",
        destination_table_exists=False,
        source_db_type="postgresql",
        use_llm=False,
        source_types_authoritative=True,
    )
    assert out["mappings"][0]["target_type"] == "VARCHAR(8)", out["mappings"][0]


def test_boolean_onto_sqlite_integer_carrier_is_not_a_sparse_custom_transform():
    by = _run2(
        [
            {"name": "flag", "inferred_type": "BIT(1)", "samples": ["1", "0"]},
            {"name": "b", "inferred_type": "BOOLEAN", "samples": ["true", "false"]},
        ],
        [
            {"name": "flag", "inferred_type": "INTEGER"},
            {"name": "b", "inferred_type": "INTEGER"},
        ],
    )
    for col in ("flag", "b"):
        row = by[col]
        assert row.get("confidence_class") != "custom_transform_sparse", row
        assert not row["requires_review"], row
        assert float(row["confidence"]) >= 0.85, row


# --------------------------------------------------------------------------
# Live: PG -> new SQLite table, incremental_upsert run 1 then run 2.
# --------------------------------------------------------------------------

_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


@pytest.mark.skipif(
    not (_PG["host"] and _reachable(_PG["host"], _PG["port"])),
    reason="live PostgreSQL: set PG* (reachable)",
)
def test_pg_bits_and_booleans_rerun_into_own_sqlite_table(tmp_path, isolated):
    import psycopg2

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    table = "d78_" + uuid.uuid4().hex[:10]
    dest = tmp_path / "d78.sqlite"
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()

    def run():
        job_id = "d78" + uuid.uuid4().hex[:12]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
                destination=EndpointConfig(
                    kind="database", format="sqlite", database=str(dest), table=table
                ),
                sync_mode="incremental_upsert",
                stream_contracts=[
                    {
                        "name": table,
                        "cursor_field": "updated_at",
                        "cursor_semantics": "monotonic_sequence",
                        "sync_mode": "incremental_upsert",
                        "primary_key": ["id"],
                        "selected": True,
                    }
                ],
                validation_mode="strict",
            ),
            job_id,
        )

    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, bits BIT(8), "  # nosec B608
            "flag BIT(1), b BOOLEAN, updated_at BIGINT)"
        )
        cur.execute(
            f"INSERT INTO {table} VALUES (1, B'10101010', B'1', true, 1), "  # nosec B608
            "(2, B'00001111', B'0', false, 2), (3, NULL, NULL, NULL, 3)"
        )
        pg.commit()
        first = run()
        assert first.success, first.error

        cur.execute(f"UPDATE {table} SET bits = B'11111111', updated_at = 10 WHERE id = 1")  # nosec B608
        pg.commit()
        second = run()
        assert second.success, second.error

        conn = sqlite3.connect(str(dest))
        try:
            rows = conn.execute(
                f"SELECT id, bits, flag, b, updated_at FROM {table} ORDER BY id"  # nosec B608
            ).fetchall()
        finally:
            conn.close()
        assert rows == [
            (1, "11111111", 1, 1, 10),
            (2, "00001111", 0, 0, 2),
            (3, None, None, None, 3),
        ]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.commit()
        pg.close()
