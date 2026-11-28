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
