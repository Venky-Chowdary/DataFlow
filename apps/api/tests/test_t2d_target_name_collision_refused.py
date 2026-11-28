"""Two source columns whose targets the destination names alike are refused.

``"a b"`` and ``"a_b"`` both land as ``a_b`` on a SQL destination (the
writer's identifier rule). The writer kept the first and silently dropped the
other column's values, and Gate-8 — which reads back by the writer's names —
could not see it. Validate (plan / start_transfer) and Execute now ask one
check (``services.target_name_collision``) and refuse, naming both source
columns; nothing is written.

Live PostgreSQL → PostgreSQL: the plan route QA uses (``start_transfer``), and
the engine with ``skip_preflight=True`` (Execute's own refusal).
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests import value_fidelity_route as r  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


@pytest.fixture()
def source_table():
    pg = r.pg_conn()
    table = "t2d_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    cur.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY, "a b" TEXT, "a_b" TEXT)')
    for i in range(1, 6):
        cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (i, f"space {i}", f"under {i}"))  # nosec B608
    try:
        yield table
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        cur.execute(f"DROP TABLE IF EXISTS {table}_out")  # nosec B608
        pg.close()


def _dest_exists(table: str) -> bool:
    return bool(r.query("postgresql", f"SELECT to_regclass('public.\"{table}\"') IS NOT NULL")[0][0])


def test_plan_refuses_naming_both_source_columns(client, source_table):
    token = uuid.uuid4().hex[:8]
    src_id, dst_id = r.connectors(token, "postgresql")
    staged, _plan, blockers = r.stage(
        source_connector_id=src_id, source_table=source_table, dest_connector_id=dst_id,
        dest_table=source_table + "_out", sync_mode="full_refresh_overwrite", primary_key="id",
    )
    text = " ".join(blockers) + " " + str(staged.error or "")
    assert "collision" in text and "'a b'" in text and "'a_b'" in text, text


def test_execute_refuses_without_validate(source_table, tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    c = r.PG

    def _ep(table: str) -> EndpointConfig:
        return EndpointConfig(kind="database", format="postgresql", table=table, host=c["host"],
                              port=c["port"], database=c["database"], username=c["username"],
                              password=c["password"], schema="public")

    request = TransferRequest(
        source=_ep(source_table), destination=_ep(source_table + "_out"),
        mappings=[{"source": "id", "target": "id"}, {"source": "a b", "target": "a b"},
                  {"source": "a_b", "target": "a_b"}],
        sync_mode="full_refresh_overwrite", skip_preflight=True, validation_mode="strict",
    )
    job_id = "t2d" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert not result.success
    assert "'a b'" in str(result.error) and "'a_b'" in str(result.error), result.error
    assert not _dest_exists(source_table + "_out"), "nothing may be written"


def test_distinct_landed_names_are_not_a_collision():
    from services.target_name_collision import target_name_collisions

    assert target_name_collisions(
        [{"source": "a b", "target": "a b"}, {"source": "ab", "target": "ab"},
         {"source": "x", "target": "x"}, {"source": "x", "target": "x"},  # one source, twice
         {"source": "a_b", "target": "a_b", "transform": "omit"}],
        dest_type="postgresql",
    ) == []


@pytest.fixture()
def case_source():
    pg = r.pg_conn()
    table = "t2dc_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    cur.execute(f'CREATE TABLE {table} (id BIGINT PRIMARY KEY, "Name" TEXT, "name" TEXT)')
    for i in range(1, 6):
        cur.execute(f"INSERT INTO {table} VALUES (%s, %s, %s)", (i, f"Upper {i}", f"lower {i}"))  # nosec B608
    try:
        yield table
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()


def test_case_only_collision_into_an_existing_sqlite_table_is_refused(case_source, tmp_path, monkeypatch):
    """Re-review: ``Name`` and ``name`` are one SQLite column. Appended into a table
    holding data under skip_preflight, one column's values were lost; now refused
    naming both sources, and the table is left exactly as it was."""
    import sqlite3

    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    db = tmp_path / "d.sqlite"
    conn = sqlite3.connect(str(db))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(100, "kept 100"), (101, "kept 101")])
    conn.close()
    c = r.PG
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=case_source, host=c["host"],
                              port=c["port"], database=c["database"], username=c["username"],
                              password=c["password"], schema="public"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(db), table="t"),
        mappings=[{"source": "id", "target": "id"}, {"source": "Name", "target": "Name"},
                  {"source": "name", "target": "name"}],
        sync_mode="full_refresh_append", skip_preflight=True, validation_mode="strict",
    )
    job_id = "t2dc" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert not result.success
    assert "'Name'" in str(result.error) and "'name'" in str(result.error), result.error
    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("SELECT id, name FROM t ORDER BY id").fetchall() == [
            (100, "kept 100"), (101, "kept 101")
        ]
    finally:
        conn.close()


_CASE_PAIR = [{"source": "Name", "target": "Name"}, {"source": "name", "target": "name"}]


@pytest.mark.parametrize("dest", ["sqlite", "postgresql", "mysql", "sqlserver", "bigquery", ""])
@pytest.mark.parametrize("preserve_case", [True, False])
def test_validate_and_the_writer_decide_a_case_pair_alike(dest, preserve_case):
    from connectors.writer_common import resolve_target_columns
    from services.ddl_compatibility import evaluate_ddl_compatibility

    _ok, issues = evaluate_ddl_compatibility(
        mappings=_CASE_PAIR, dest_db_type=dest or "postgresql", table_exists=False, allow_create=True,
    )
    assert any("'Name' and 'name'" in i for i in issues), issues
    with pytest.raises(ValueError, match="'Name' and 'name'"):
        resolve_target_columns(_CASE_PAIR, {}, preserve_case=preserve_case, dest_db=dest)


@pytest.mark.parametrize("dest", ["mongodb", "kafka", "elasticsearch", "csv"])
def test_case_exact_destinations_keep_two_columns(dest):
    from connectors.writer_common import resolve_target_columns
    from services.target_name_collision import target_name_collisions

    assert target_name_collisions(_CASE_PAIR, dest_type=dest) == []
    targets, _types = resolve_target_columns(_CASE_PAIR, {}, preserve_case=True, dest_db=dest)
    assert targets == ["Name", "name"]


def test_a_pending_mapping_the_writer_skips_is_not_a_collision():
    """Re-review: an unproven-existence mapping is not written, so it collides with nothing."""
    from connectors.writer_common import resolve_target_columns
    from services.ddl_compatibility import evaluate_ddl_compatibility

    maps = [{"source": "id", "target": "id"}, {"source": "a b", "target": "a b"},
            {"source": "a_b", "target": "a_b", "assignment_strategy": "pending_dest_schema"}]
    targets, _ = resolve_target_columns(maps, {}, preserve_case=True, dest_db="postgresql",
                                        table_exists=True)
    assert targets == ["id", "a_b"]
    _ok, issues = evaluate_ddl_compatibility(mappings=maps, dest_db_type="postgresql", table_exists=True,
                                             dest_connected=True, target_schema={"id": "BIGINT", "a_b": "TEXT"})
    assert not any("collision" in i for i in issues), issues
    # Confirmed create-new: the writer creates it, so the same pair is refused.
    with pytest.raises(ValueError, match="'a b' and 'a_b'"):
        resolve_target_columns(maps, {}, preserve_case=True, dest_db="postgresql", table_exists=False)
