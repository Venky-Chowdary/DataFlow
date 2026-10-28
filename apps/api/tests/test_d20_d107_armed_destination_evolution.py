"""D20 / D107 — approved destination evolution reaches the existing table.

Run 1 creates the destination. The source then gains a column (D20) or widens
INT -> BIGINT (D107), and run 2 goes through the full engine with preflight ON.

* Unapproved (no acknowledgement, no backfill, manual review): still blocked,
  and the destination is untouched.
* Approved by an acknowledged drift review, ``backfill_new_fields`` or a
  propagate policy: the evolution is applied to the plan once, so every gate
  grades the source against the destination the run leaves behind, and the
  rows land.

Before the fix a mapped new column stayed blocked under acknowledgement and
backfill (the review policy never saw the backfill, and an unsaved contract
cannot be acknowledged), and a widen was refused by four separate gates
(schema contract, mapping confidence, data integrity, population fit).
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG_PASSWORD = os.environ.get("PGPASSWORD") or "dataflow"


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d20" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _request(source: EndpointConfig, destination: EndpointConfig, stream: str, **extra):
    return TransferRequest(
        source=source,
        destination=destination,
        sync_mode="incremental_append",
        stream_contracts=[
            {
                "name": stream,
                "cursor_field": "id",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
        validation_mode="strict",
        **extra,
    )


def _maps(*names: str) -> list[dict]:
    return [{"source": n, "target": n, "confidence": 1.0} for n in names]


ARMED = {
    "ack": {"schema_drift_acknowledged": True},
    "backfill": {"backfill_new_fields": True},
    "propagate": {"schema_policy": "propagate_columns"},
}


# ── unit: the one decision ────────────────────────────────────────────────


def test_backfill_request_is_a_propagate_policy_for_every_gate():
    from services.schema_drift import effective_schema_policy

    assert effective_schema_policy("manual_review", backfill_new_fields=True) == "propagate_columns"
    assert effective_schema_policy("manual_review") == "manual_review"
    # Policies that forbid a silent ADD COLUMN keep their policy.
    for policy in ("type_locked", "pause_on_change", "block_all"):
        assert effective_schema_policy(policy, backfill_new_fields=True) == policy


def test_destination_evolution_approval_is_policy_plus_acknowledgement():
    from services.schema_drift import destination_evolution_approved as approved

    assert approved("propagate_columns") and approved("propagate_all")
    assert not approved("manual_review")
    assert approved("manual_review", acknowledged=True)
    for policy in ("type_locked", "pause_on_change", "block_all"):
        assert not approved(policy, acknowledged=True)


def test_plan_widens_the_destination_type_and_adds_the_mapped_column():
    from services.schema_drift import plan_destination_evolution

    mappings, dest_types, applied = plan_destination_evolution(
        [
            {"source": "qty", "target": "qty", "review_kind": "lossy", "requires_review": True},
            {"source": "extra", "target": "extra"},
        ],
        source_columns=["qty", "extra"],
        source_schema={"qty": "BIGINT", "extra": "TEXT"},
        dest_types={"qty": "INTEGER"},
        dest_db="postgresql",
    )
    assert dest_types == {"qty": "BIGINT", "extra": "TEXT"}
    qty = next(m for m in mappings if m["source"] == "qty")
    assert qty["dest_widen"] is True and qty["target_type"] == "BIGINT"
    assert qty["requires_review"] is False and "review_kind" not in qty
    assert {a["propagate_reason"] for a in applied} == {"widen_type", "add_column"}


# ── SQLite e2e: D20 new column ────────────────────────────────────────────


def _sqlite_pair(tmp_path: Path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?,?,?)", [(i, f"n{i}", i) for i in range(1, 6)])
    conn.commit()
    return conn, src, dst


def _sqlite_endpoints(src: Path, dst: Path):
    return (
        EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
    )


def _sqlite_dest(dst: Path):
    conn = sqlite3.connect(dst)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(t)")]
        rows = conn.execute("SELECT * FROM t ORDER BY id").fetchall()
    finally:
        conn.close()
    return cols, rows


def _add_source_column(conn):
    conn.execute("ALTER TABLE t ADD COLUMN extra TEXT")
    conn.executemany("INSERT INTO t VALUES (?,?,?,?)", [(i, f"n{i}", i, f"x{i}") for i in range(6, 9)])
    conn.commit()


def test_mapped_new_column_stays_blocked_without_approval(tmp_path, isolated):
    conn, src, dst = _sqlite_pair(tmp_path)
    s, d = _sqlite_endpoints(src, dst)
    assert _run(isolated, _request(s, d, "t")).success
    _add_source_column(conn)

    result = _run(isolated, _request(s, d, "t", mappings=_maps("id", "name", "qty", "extra")))

    assert not result.success
    assert _sqlite_dest(dst)[0] == ["id", "name", "qty"]


@pytest.mark.parametrize("armed", ["ack", "backfill"])
def test_mapped_new_column_lands_when_approved(tmp_path, isolated, armed):
    conn, src, dst = _sqlite_pair(tmp_path)
    s, d = _sqlite_endpoints(src, dst)
    assert _run(isolated, _request(s, d, "t")).success
    _add_source_column(conn)

    result = _run(
        isolated,
        _request(s, d, "t", mappings=_maps("id", "name", "qty", "extra"), **ARMED[armed]),
    )

    assert result.success, result.error
    cols, rows = _sqlite_dest(dst)
    assert cols == ["id", "name", "qty", "extra"]
    assert [r[3] for r in rows] == [None] * 5 + ["x6", "x7", "x8"]


def test_backfill_lands_an_unmapped_new_column(tmp_path, isolated):
    conn, src, dst = _sqlite_pair(tmp_path)
    s, d = _sqlite_endpoints(src, dst)
    assert _run(isolated, _request(s, d, "t")).success
    _add_source_column(conn)

    result = _run(isolated, _request(s, d, "t", backfill_new_fields=True))

    assert result.success, result.error
    assert _sqlite_dest(dst)[0] == ["id", "name", "qty", "extra"]


# ── PostgreSQL e2e: D107 widen, D20 new column ────────────────────────────


@pytest.fixture()
def pg():
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError:
        pytest.skip("PostgreSQL not reachable on :5432")
    import psycopg2

    try:
        conn = psycopg2.connect(
            host="127.0.0.1", user="dataflow", password=PG_PASSWORD, dbname="dataflow"
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL credentials rejected: {exc}")
    conn.autocommit = True
    suffix = uuid.uuid4().hex[:8]
    names = (f"d20s_{suffix}", f"d20d_{suffix}")
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {names[0]} (id INT PRIMARY KEY, name TEXT, qty INT)")
    # The destination is declared INT. DataFlow now creates BIGINT for an INT
    # key/measure on its own, so a table it created would never need the widen.
    cur.execute(f"CREATE TABLE {names[1]} (id INT PRIMARY KEY, name TEXT, qty INT)")
    cur.executemany(f"INSERT INTO {names[0]} VALUES (%s,%s,%s)", [(i, f"n{i}", i) for i in range(1, 6)])
    try:
        yield cur, names
    finally:
        for table in names:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def _pg_endpoints(names):
    def ep(table):
        return EndpointConfig(
            kind="database", format="postgresql", host="127.0.0.1", port=5432,
            database="dataflow", username="dataflow", password=PG_PASSWORD,
            schema="public", table=table,
        )

    return ep(names[0]), ep(names[1])


def _pg_dest(cur, table):
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name='{table}' ORDER BY ordinal_position"
    )
    cols = cur.fetchall()
    cur.execute(f"SELECT * FROM {table} ORDER BY id")
    return cols, cur.fetchall()


def _widen_source(cur, table):
    cur.execute(f"ALTER TABLE {table} ALTER COLUMN qty TYPE BIGINT")
    cur.executemany(
        f"INSERT INTO {table} VALUES (%s,%s,%s)",
        [(i, f"n{i}", 5_000_000_000 + i) for i in range(6, 9)],
    )


def test_int_to_bigint_widen_stays_blocked_without_approval(pg, isolated):
    cur, names = pg
    s, d = _pg_endpoints(names)
    assert _run(isolated, _request(s, d, names[0])).success
    _widen_source(cur, names[0])

    result = _run(isolated, _request(s, d, names[0]))

    assert not result.success
    assert _pg_dest(cur, names[1])[0][2] == ("qty", "integer")


@pytest.mark.parametrize("armed", ["ack", "backfill", "propagate"])
def test_int_to_bigint_widen_lands_when_approved(pg, isolated, armed):
    cur, names = pg
    s, d = _pg_endpoints(names)
    assert _run(isolated, _request(s, d, names[0])).success
    _widen_source(cur, names[0])

    result = _run(isolated, _request(s, d, names[0], **ARMED[armed]))

    assert result.success, result.error
    cols, rows = _pg_dest(cur, names[1])
    assert cols[2] == ("qty", "bigint")
    assert [r[2] for r in rows][-3:] == [5_000_000_006, 5_000_000_007, 5_000_000_008]


@pytest.mark.parametrize("armed", ["ack", "backfill"])
def test_pg_mapped_new_column_lands_when_approved(pg, isolated, armed):
    cur, names = pg
    s, d = _pg_endpoints(names)
    assert _run(isolated, _request(s, d, names[0])).success
    cur.execute(f"ALTER TABLE {names[0]} ADD COLUMN extra TEXT")
    cur.executemany(
        f"INSERT INTO {names[0]} VALUES (%s,%s,%s,%s)", [(i, f"n{i}", i, f"x{i}") for i in range(6, 9)]
    )

    result = _run(
        isolated,
        _request(s, d, names[0], mappings=_maps("id", "name", "qty", "extra"), **ARMED[armed]),
    )

    assert result.success, result.error
    cols, rows = _pg_dest(cur, names[1])
    assert [c[0] for c in cols] == ["id", "name", "qty", "extra"]
    assert [r[3] for r in rows] == [None] * 5 + ["x6", "x7", "x8"]
