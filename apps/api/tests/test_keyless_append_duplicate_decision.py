"""Story 2: append reruns into an existing table, decided by sync mode and key.

Real SQLite -> SQLite runs through the engine. Full refresh · Append means
"insert the full snapshot again" (Airbyte full_refresh_append semantics): on a
table that enforces no key, run 2 stores a second copy, and Validate says so.
A table that enforces the key refuses that insert unless the route chose a
duplicate policy (``dedupe_skip`` / ``upsert``); Validate and Execute must
agree on that choice. Execute's own Validate re-derives lossy casts.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_MAPPINGS = [
    {"source": c, "target": c, "approved": True, "user_override": True, "confidence": 1.0}
    for c in ("id", "name")
]


def _seed(path: Path, rows: list[tuple[int, str]]) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.close()


def _rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT id, name FROM t ORDER BY id, name").fetchall()
    finally:
        conn.close()


@pytest.fixture()
def fake(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mongo)
    return mongo


def _run(fake, src: Path, dst: Path, *, request_policy: str = "", dest_extra: dict | None = None):
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t",
            extra=dict(dest_extra or {}),
        ),
        sync_mode="full_refresh_append",
        validation_mode="strict",
        mappings=[dict(m) for m in _MAPPINGS],
        duplicate_policy=request_policy,
    )
    job_id = "s2dup" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


def _seed_keyed(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.close()


def test_keyless_full_append_rerun_stores_the_second_copy_it_names(
    tmp_path: Path, fake
) -> None:
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, [(1, "alpha"), (2, "beta")])
    _seed(dst, [])
    assert _run(fake, src, dst).success
    second = _run(fake, src, dst)
    assert second.success, second.error
    assert _rows(dst) == [(1, "alpha"), (1, "alpha"), (2, "beta"), (2, "beta")]


@pytest.mark.parametrize(
    ("request_policy", "dest_extra", "expected"),
    [
        ("dedupe_skip", None, [(1, "alpha"), (2, "beta"), (3, "gamma")]),
        ("", {"duplicate_policy": "dedupe_skip"}, [(1, "alpha"), (2, "beta"), (3, "gamma")]),
        ("upsert", None, [(1, "alpha"), (2, "BETA"), (3, "gamma")]),
        ("", {"duplicate_policy": "merge"}, [(1, "alpha"), (2, "BETA"), (3, "gamma")]),
    ],
    ids=["skip_request_field", "skip_destination_option", "upsert_request_field", "merge_alias"],
)
def test_enforced_key_rerun_follows_the_chosen_duplicate_policy(
    tmp_path: Path, fake, request_policy: str, dest_extra: dict | None, expected: list
) -> None:
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, [(1, "alpha"), (2, "beta")])
    _seed_keyed(dst)
    assert _run(fake, src, dst).success
    refused = _run(fake, src, dst)
    assert not refused.success, "an enforced key cannot take a second copy"
    assert _rows(dst) == [(1, "alpha"), (2, "beta")]
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("UPDATE t SET name = 'BETA' WHERE id = 2")
        conn.execute("INSERT INTO t VALUES (3, 'gamma')")
    conn.close()
    chosen = _run(fake, src, dst, request_policy=request_policy, dest_extra=dest_extra)
    assert chosen.success, chosen.error
    # skip keeps the stored image of key 2; upsert replaces it. Both insert key 3.
    assert _rows(dst) == expected


@pytest.mark.parametrize("policy", ["dedupe_skip", "upsert"])
def test_enforced_key_rerun_with_every_key_stored_completes(
    tmp_path: Path, fake, policy: str
) -> None:
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, [(1, "alpha"), (2, "beta")])
    _seed_keyed(dst)
    assert _run(fake, src, dst, request_policy=policy).success
    again = _run(fake, src, dst, request_policy=policy)
    assert again.success, again.error
    assert _rows(dst) == [(1, "alpha"), (2, "beta")]
    summary = dict(getattr(again, "destination_summary", None) or {})
    assert int(summary.get("rejected_rows") or 0) == 0, summary


def test_execute_rederives_a_lossy_cast_the_client_did_not_flag(tmp_path: Path, fake) -> None:
    """Execute's own Validate re-derives fidelity from the real types.

    The mapping arrives approved with no ``fidelity`` / ``type_narrowing``; the
    REAL -> INTEGER narrowing must still be refused with zero rows written.
    """
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER, name REAL)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, 1.5), (2, 2.25)])
    conn.close()
    conn = sqlite3.connect(str(dst))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name INTEGER NOT NULL)")
    conn.close()

    result = _run(fake, src, dst, request_policy="append")
    assert not result.success
    assert _rows(dst) == [], "a refused lossy cast wrote rows"


# --- Live PostgreSQL: a declared key the destination does not enforce. -------
# PostgreSQL resolves ON CONFLICT only on a unique index, so the key gate must
# disclose the index (approval required) before any write, and an approved run
# must resolve stored keys exactly as chosen with Gate-8 grading only this run's
# write (dedupe_skip leaves the stored image of key 2, which is not a loss).

_PG = dict(host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow")


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(connect_timeout=3, **_PG)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable on localhost:5432: {exc}")
    conn.autocommit = True
    return conn


def _pg_rows(table: str) -> list[tuple]:
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT id, name FROM public."{table}" ORDER BY id, name')
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _pg_indexes(table: str) -> list[str]:
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT indexdef FROM pg_indexes WHERE tablename = %s", (table,))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _run_pg(fake, src: Path, table: str, policy: str, *, ack: bool = False, declare_key: bool = True):
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(
            kind="database", format="postgresql", host="localhost", port=5432,
            database="dataflow", username="dataflow", password="dataflow",
            schema="public", table=table, extra={"duplicate_policy": policy},
        ),
        sync_mode="full_refresh_append",
        validation_mode="strict",
        mappings=[dict(m) for m in _MAPPINGS],
        stream_contracts=[{"primary_key": ["id"]}] if declare_key else [],
        ddl_changes_acknowledged=ack,
    )
    job_id = "s2pg" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


@pytest.fixture()
def pg_table(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    created: list[str] = []

    def make(pk: bool) -> str:
        name = "s2_" + uuid.uuid4().hex[:10]
        conn = _pg()
        try:
            key = " PRIMARY KEY" if pk else ""
            conn.cursor().execute(f'CREATE TABLE public."{name}" (id BIGINT{key}, name TEXT)')
        finally:
            conn.close()
        created.append(name)
        return name

    yield make
    conn = _pg()
    try:
        for name in created:
            conn.cursor().execute(f'DROP TABLE IF EXISTS public."{name}"')
    finally:
        conn.close()


def _change_source(src: Path) -> None:
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("UPDATE t SET name = 'BETA' WHERE id = 2")
        conn.execute("INSERT INTO t VALUES (3, 'gamma')")
    conn.close()


@pytest.mark.parametrize("policy", ["dedupe_skip", "upsert"])
def test_pg_heap_keyed_policy_is_refused_until_the_disclosed_index_is_approved(
    tmp_path: Path, fake, pg_table, policy: str
) -> None:
    table = pg_table(pk=False)
    src = tmp_path / "src.db"
    _seed(src, [(1, "alpha"), (2, "beta")])
    refused = _run_pg(fake, src, table, policy)
    assert not refused.success
    assert "CREATE UNIQUE INDEX" in (refused.error or ""), refused.error
    assert _pg_rows(table) == [], "Validate refused before any write"
    assert _pg_indexes(table) == [], "no DDL without approval"


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        ("dedupe_skip", [(1, "alpha"), (2, "beta"), (3, "gamma")]),
        ("upsert", [(1, "alpha"), (2, "BETA"), (3, "gamma")]),
    ],
)
@pytest.mark.parametrize("pk", [True, False], ids=["enforced_pk", "heap_approved_index"])
def test_pg_keyed_policy_rerun_resolves_stored_keys_and_reconciles(
    tmp_path: Path, fake, pg_table, policy: str, expected: list, pk: bool
) -> None:
    table = pg_table(pk=pk)
    src = tmp_path / "src.db"
    _seed(src, [(1, "alpha"), (2, "beta")])
    first = _run_pg(fake, src, table, policy, ack=not pk)
    assert first.success, first.error
    if not pk:
        # The approved index lands on the first, empty-table load too.
        assert any("UNIQUE INDEX" in d for d in _pg_indexes(table)), _pg_indexes(table)
    _change_source(src)
    second = _run_pg(fake, src, table, policy, ack=not pk)
    assert second.success, second.error
    assert _pg_rows(table) == expected
    ledger = dict((getattr(second, "destination_summary", None) or {}).get("row_accounting") or {})
    assert ledger.get("conservation_kind") == "keyed", ledger
    assert ledger.get("balanced") is True, ledger
    assert ledger.get("inserts") == 1, ledger
    # Stored keys 1 and 2: skip keeps both untouched, upsert replaces both.
    assert ledger.get("updates") == (0 if policy == "dedupe_skip" else 2), ledger
    if not pk:
        assert any("UNIQUE INDEX" in d for d in _pg_indexes(table))


def test_pg_keyed_policy_with_no_declared_key_is_refused_before_write(
    tmp_path: Path, fake, pg_table
) -> None:
    table = pg_table(pk=False)
    src = tmp_path / "src.db"
    _seed(src, [(1, "alpha")])
    refused = _run_pg(fake, src, table, "dedupe_skip", ack=True, declare_key=False)
    assert not refused.success
    assert "no key is declared" in (refused.error or ""), refused.error
    assert _pg_rows(table) == []


@pytest.mark.parametrize("policy", ["dedupe_skip", "upsert"])
@pytest.mark.parametrize("pk", [True, False], ids=["enforced_pk", "heap_approved_index"])
def test_pg_keyed_policy_rerun_with_every_key_stored_is_not_quarantine(
    tmp_path: Path, fake, pg_table, policy: str, pk: bool
) -> None:
    # Rerunning an unchanged snapshot: every key is already stored. Skip keeps
    # all of them, which is a decided outcome — not "every row quarantined".
    table = pg_table(pk=pk)
    src = tmp_path / "src.db"
    _seed(src, [(1, "alpha"), (2, "beta"), (3, "gamma")])
    first = _run_pg(fake, src, table, policy, ack=not pk)
    assert first.success, first.error
    second = _run_pg(fake, src, table, policy, ack=not pk)
    assert second.success, second.error
    assert _pg_rows(table) == [(1, "alpha"), (2, "beta"), (3, "gamma")]
    summary = dict(getattr(second, "destination_summary", None) or {})
    assert int(summary.get("rejected_rows") or 0) == 0, summary
    ledger = dict(summary.get("row_accounting") or {})
    assert ledger.get("balanced") is True, str(ledger)
    assert ledger.get("inserts") == 0, ledger
    assert ledger.get("updates") == (0 if policy == "dedupe_skip" else 3), ledger
