"""QA r12 Snowflake D4: Snowflake -> PG incremental_append refused on the second run.

QA (build 8f7d246d): the second run of a Snowflake -> PostgreSQL
incremental_append was refused with "Destination already stores these keys",
on a destination DataFlow created without a primary key.

Root cause: the contract names its cursor and key as typed (``updated_at``,
``id``) while Snowflake's catalog, and every row it hands back, spells them
``UPDATED_AT`` / ``ID``. Each owner that looks the cursor up by name missed:

* the pre-write key probe's watermark bound found no ``updated_at`` in the
  rows and fell back to the whole table, so every key already loaded read as a
  collision — the refusal;
* the boundary tie-set read asked the destination for a ``updated_at`` column
  it does not have, failed open, and re-appended the boundary row every run.

The page-edge seek also bound the bookmark as a text row value,
``(c, pk) > (%s, %s)``, which an engine that will not cast a text tuple onto
``(TIMESTAMP, NUMBER)`` refuses; it is now the expanded predicate.

Live: fakesnow (DuckDB) source -> live PostgreSQL destination the first run
creates. First load, two no-op runs, a row that ties the max cursor plus a
newer one, a last no-op. Exact counts, no duplicate keys, NULL / '' / unicode.
"""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fakesnow", reason="requires the optional Snowflake test dependency")

from tests import r4_route_harness as h  # noqa: E402

pytestmark = pytest.mark.timeout(400)

_SF = {
    "kind": "database", "format": "snowflake", "host": "localhost", "port": 443,
    "database": "dataflow", "username": "test", "password": "test", "schema": "public",
}
_BASE = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
_NAMES = {1: "Ada", 2: None, 3: "", 4: "   ", 5: "Zoë ✓", 6: "日本語", 7: "max"}


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


def _sf(sql: str, params=None) -> None:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(account="localhost", username="test", password="test",
                          database="dataflow", schema="public", warehouse="",
                          connection_string="")
    try:
        cur = conn.cursor()
        cur.execute(sql, params) if params else cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _insert(table: str, rows) -> None:
    for rid, name, hours in rows:
        _sf(f'INSERT INTO "{table}" VALUES (%s, %s, %s)',  # nosec B608
            (rid, name, (_BASE + dt.timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")))


def _run(fake, src: str, dst: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(**_SF, table=src),
        destination=EndpointConfig(kind="database", format="postgresql", table=dst,
                                   **{k: v for k, v in h.PG.items()}),
        sync_mode="incremental_append",
        source_timezone="UTC",
        # Typed the way an operator (or the REST schedule body) writes them.
        stream_contracts=[{
            "name": src, "cursor_field": "updated_at",
            "cursor_semantics": "monotonic_sequence",
            "sync_mode": "incremental_append", "primary_key": ["id"], "selected": True,
        }],
        validation_mode="strict",
    )
    job_id = "r12sf" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _dest_rows(dst: str) -> dict:
    cols = [r[0] for r in h.query(
        "postgresql",
        "SELECT column_name FROM information_schema.columns "  # nosec B608
        f"WHERE table_schema = 'public' AND table_name = '{dst}'",
    )]
    id_col = next(c for c in cols if c.lower() == "id")
    name_col = next(c for c in cols if c.lower() == "name")
    rows = h.query("postgresql", f'SELECT "{id_col}", "{name_col}" FROM public."{dst}"')  # nosec B608
    ids = [int(r[0]) for r in rows]
    assert len(ids) == len(set(ids)), ("duplicate keys", sorted(ids))
    return {int(r[0]): r[1] for r in rows}


def test_snowflake_to_pg_incremental_append_second_run_appends_only_new_rows(isolated):
    h.pg_conn().close()
    token = uuid.uuid4().hex[:8]
    src, dst = f"r12sf_src_{token}", f"r12sf_dst_{token}"
    try:
        _sf(f'DROP TABLE IF EXISTS "{src}"')
        _sf(f'CREATE TABLE "{src}" (id NUMBER(38,0), name VARCHAR, updated_at TIMESTAMP_NTZ)')
        # Rows 6 and 7 share the max cursor value already on the first load.
        _insert(src, [(i, _NAMES[i], 7 if i >= 6 else i) for i in _NAMES])
        expected = dict(_NAMES)

        first = _run(isolated, src, dst)
        assert first.success, first.error
        assert _dest_rows(dst) == expected

        for n in (1, 2):
            again = _run(isolated, src, dst)
            assert again.success, (n, again.error)
            assert int(again.records_transferred or 0) == 0, (n, again.records_transferred)
            assert _dest_rows(dst) == expected, n

        # Arrives later on the max cursor value: still loads, exactly once.
        _insert(src, [(8, "tie ü", 7), (9, None, 9)])
        expected.update({8: "tie ü", 9: None})
        delta = _run(isolated, src, dst)
        assert delta.success, delta.error
        assert int(delta.records_transferred or 0) == 2, delta.records_transferred
        assert _dest_rows(dst) == expected

        last = _run(isolated, src, dst)
        assert last.success, last.error
        assert _dest_rows(dst) == expected
    finally:
        _sf(f'DROP TABLE IF EXISTS "{src}"')
        h.drop_table("postgresql", dst)
