"""Datawrap brand: the objects the product leaves in a customer's database.

The owner's report: "the old 'dataflow' name leaks into customer databases".
Existing customers already hold tables and columns with the old names, and the
next run must keep working against them, so the rule is:

* an artifact that already exists under the old name stays the one in use
  (it is recognised as the product's own and is never renamed);
* only a NEW artifact may get the new name, and only when the off-by-default
  flag ``DATAWRAP_BRAND_OBJECT_NAMES=1`` is set (with an audit entry);
* an artifact created under the new name stays in use if the flag is later
  turned off.

Everything here runs against real engines: MySQL (binlog CDC source) and
PostgreSQL through ``UniversalTransferEngine().execute_tracked``, plus the
MySQL / PostgreSQL writers directly for the ownership marker. Each CDC test
gets its own PostgreSQL database so the watermark table it finds is the one it
made.
"""

from __future__ import annotations

import socket
import uuid

import pytest

MYSQL = {
    "host": "127.0.0.1", "port": 3306, "database": "dataflow",
    "username": "dataflow", "password": "dataflow",
}
PG = {
    "host": "127.0.0.1", "port": 5432, "database": "dataflow",
    "username": "dataflow", "password": "dataflow",
}

LEGACY_WM = "_df_cdc_eos_watermarks"
NEW_WM = "_dw_cdc_eos_watermarks"


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def _mysql(**kw):
    import pymysql

    return pymysql.connect(
        host=MYSQL["host"], port=MYSQL["port"], user=MYSQL["username"],
        password=MYSQL["password"], database=MYSQL["database"], autocommit=True,
        connect_timeout=3, **kw,
    )


def _pg(dbname: str = "dataflow"):
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=dbname, user=PG["username"],
        password=PG["password"], connect_timeout=3,
    )
    conn.autocommit = True
    return conn


def _binlog_ready() -> bool:
    if not (_up(3306) and _up(5432)):
        return False
    try:
        import pymysqlreplication  # noqa: F401

        conn = _mysql()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                return bool(row and str(row[1]).upper() == "ROW")
        finally:
            conn.close()
    except Exception:
        return False


needs_cdc = pytest.mark.skipif(not _binlog_ready(), reason="MySQL ROW binlog / PostgreSQL down")
needs_pg = pytest.mark.skipif(not _up(5432), reason="PostgreSQL down")
needs_mysql = pytest.mark.skipif(not _up(3306), reason="MySQL down")


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.delenv("DATAFLOW_ENGINE_DIGEST", raising=False)
    monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)
    monkeypatch.delenv("DATAFLOW_BRAND_OBJECT_NAMES", raising=False)
    try:
        from services import brand

        brand.reset_resolution_cache()
    except Exception:  # pre-fix tree: no brand module yet
        pass


@pytest.fixture()
def audit_events(monkeypatch):
    import services.audit_log as audit_log

    seen: list[dict] = []

    def _capture(**kw):
        seen.append(kw)
        return {}

    monkeypatch.setattr(audit_log, "append_audit_event", _capture)
    return seen


def _flag(monkeypatch, on: bool) -> None:
    if on:
        monkeypatch.setenv("DATAWRAP_BRAND_OBJECT_NAMES", "1")
    else:
        monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)


# ---------------------------------------------------------------------------
# CDC: the exactly-once watermark table on the destination
# ---------------------------------------------------------------------------


@pytest.fixture()
def fresh_pg_db():
    """A throwaway PostgreSQL database: its watermark table is ours alone."""
    name = "brand_" + uuid.uuid4().hex[:10]
    admin = _pg()
    try:
        admin.cursor().execute(f'CREATE DATABASE "{name}"')
    except Exception as exc:  # no CREATEDB on this server
        admin.close()
        pytest.skip(f"cannot create a scratch database: {exc}")
    try:
        yield name
    finally:
        from services.engine_pool import dispose_all

        try:
            dispose_all()
        except Exception:
            pass
        with admin.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
        admin.close()


@pytest.fixture()
def cdc_source():
    table = "brandcdc_" + uuid.uuid4().hex[:8]
    conn = _mysql()
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"CREATE TABLE `{table}` (id INT PRIMARY KEY, note VARCHAR(40)) "
            "CHARACTER SET utf8mb4"
        )
        cur.executemany(
            f"INSERT INTO `{table}` VALUES (%s, %s)",
            [(1, "plain"), (2, None), (3, ""), (4, "  "), (5, "é中\U0001f600"),
             (6, "__DF_SQL_NULL__")],
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


def _mutate(table: str) -> None:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE `{table}` SET note = NULL WHERE id = 1")
            cur.execute(f"INSERT INTO `{table}` VALUES (7, '')")
            cur.execute(f"DELETE FROM `{table}` WHERE id = 2")
    finally:
        conn.close()


@pytest.fixture()
def fake_store(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _run_cdc(fake, table: str, dbname: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", table=table, **MYSQL),
        destination=EndpointConfig(
            kind="database", format="postgresql", schema="public", table=table + "_d",
            **{**PG, "database": dbname},
        ),
        sync_mode="cdc_incremental",
        validation_mode="strict",
        source_timezone="UTC",
        delivery_guarantee="auto",
        stream_contracts=[{
            "name": table, "selected": True, "primary_key": ["id"],
            "cursor_semantics": "cdc_position", "sync_mode": "cdc",
            "snapshot_mode": "initial",
        }],
    )
    job_id = "brand" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _tables(dbname: str) -> set[str]:
    conn = _pg(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def _wm_streams(dbname: str, table: str) -> list[str]:
    conn = _pg(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT stream_key FROM public."{table}"')
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _dest_rows(dbname: str, table: str) -> dict:
    conn = _pg(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, note FROM public."{table}_d" ORDER BY id')
            return dict(cur.fetchall())
    finally:
        conn.close()


def _dest_columns(dbname: str, table: str) -> set[str]:
    conn = _pg(dbname)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table + "_d",),
            )
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


SNAPSHOT = {1: "plain", 2: None, 3: "", 4: "  ", 5: "é中\U0001f600", 6: "__DF_SQL_NULL__"}
AFTER = {1: None, 3: "", 4: "  ", 5: "é中\U0001f600", 6: "__DF_SQL_NULL__", 7: ""}


def _assert_streamed(result) -> None:
    assert result.success, result.error
    streamed = (result.destination_summary or {}).get("cdc") or {}
    # The binlog leg, not a second snapshot: run 2 resumed from the watermark.
    assert (streamed.get("inserts"), streamed.get("updates"), streamed.get("deletes")) == (
        1, 1, 1,
    ), streamed


@needs_cdc
def test_run2_with_flag_on_keeps_using_the_legacy_watermark_table(
    fake_store, cdc_source, fresh_pg_db, monkeypatch, audit_events
):
    """A customer upgraded with the flag on: run 2 resumes from the old table."""
    _flag(monkeypatch, False)
    first = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    assert first.success, first.error
    assert first.destination_summary.get("cdc_delivery") == "exactly_once"
    assert LEGACY_WM in _tables(fresh_pg_db)
    assert _dest_rows(fresh_pg_db, cdc_source) == SNAPSHOT

    _flag(monkeypatch, True)
    _mutate(cdc_source)
    second = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    _assert_streamed(second)
    assert _dest_rows(fresh_pg_db, cdc_source) == AFTER
    tables = _tables(fresh_pg_db)
    assert NEW_WM not in tables, "the flag must never start a second watermark table"
    assert len(_wm_streams(fresh_pg_db, LEGACY_WM)) == 1
    assert "_df_lsn" in _dest_columns(fresh_pg_db, cdc_source)
    assert not [e for e in audit_events if e.get("action") == "brand.object_name.created"]


@needs_cdc
def test_flag_on_new_destination_gets_the_new_watermark_table(
    fake_store, cdc_source, fresh_pg_db, monkeypatch, audit_events
):
    _flag(monkeypatch, True)
    first = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    assert first.success, first.error
    assert first.destination_summary.get("cdc_delivery") == "exactly_once"
    tables = _tables(fresh_pg_db)
    assert NEW_WM in tables and LEGACY_WM not in tables, sorted(tables)
    assert len(_wm_streams(fresh_pg_db, NEW_WM)) == 1
    assert _dest_rows(fresh_pg_db, cdc_source) == SNAPSHOT
    created = [e for e in audit_events if e.get("action") == "brand.object_name.created"]
    assert created, audit_events
    assert created[0]["details"]["name"] == NEW_WM
    assert created[0]["details"]["legacy_name"] == LEGACY_WM

    _mutate(cdc_source)
    second = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    _assert_streamed(second)
    assert _dest_rows(fresh_pg_db, cdc_source) == AFTER
    assert LEGACY_WM not in _tables(fresh_pg_db)


@needs_cdc
def test_turning_the_flag_off_keeps_using_the_new_watermark_table(
    fake_store, cdc_source, fresh_pg_db, monkeypatch
):
    _flag(monkeypatch, True)
    first = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    assert first.success, first.error
    assert NEW_WM in _tables(fresh_pg_db)

    _flag(monkeypatch, False)
    _mutate(cdc_source)
    second = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    _assert_streamed(second)
    assert _dest_rows(fresh_pg_db, cdc_source) == AFTER
    assert LEGACY_WM not in _tables(fresh_pg_db), "flag off must not fork the CDC state"


@needs_cdc
def test_dry_run_lists_legacy_artifacts_and_renames_nothing(
    fake_store, cdc_source, fresh_pg_db, monkeypatch
):
    from services.brand_artifacts import scan_destination

    first = _run_cdc(fake_store, cdc_source, fresh_pg_db)
    assert first.success, first.error
    before = _tables(fresh_pg_db), _dest_columns(fresh_pg_db, cdc_source)

    report = scan_destination("postgresql", {**PG, "database": fresh_pg_db, "schema": "public"})

    assert report["dry_run"] is True and report["renamed"] == 0
    found = {(a["kind"], a["object"]) for a in report["artifacts"]}
    assert ("cdc_eos_watermark_table", LEGACY_WM) in found, report
    assert ("cdc_lsn_column", f"{cdc_source}_d._df_lsn") in found, report
    for art in report["artifacts"]:
        assert art["action"] == "keep"
        assert art["new_name"] and art["new_name"] != art["legacy_name"]
    assert (_tables(fresh_pg_db), _dest_columns(fresh_pg_db, cdc_source)) == before


# ---------------------------------------------------------------------------
# The ownership marker on tables the product creates (D101 gate)
# ---------------------------------------------------------------------------


def _pg_comment(table: str) -> str | None:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT obj_description(to_regclass(%s), 'pg_class')", (f'public."{table}"',))
            return cur.fetchone()[0]
    finally:
        conn.close()


def _my_comment(table: str) -> str:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
                (table,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _pg_write(table: str, rows, mode: str = "upsert"):
    from connectors.postgresql_writer import write_mapped_rows

    return write_mapped_rows(
        **PG, schema="public", connection_string="", ssl=False, table_name=table,
        headers=["k", "v"], data_rows=rows,
        mappings=[{"source": "k", "target": "k"}, {"source": "v", "target": "v"}],
        column_types={"k": "VARCHAR(40)", "v": "TEXT"},
        write_mode=mode, conflict_columns=["k"],
    )


def _my_write(table: str, rows, mode: str = "upsert"):
    from connectors.mysql_writer import write_mapped_rows

    return write_mapped_rows(
        **MYSQL, schema="", connection_string="", ssl=False, table_name=table,
        headers=["k", "v"], data_rows=rows,
        mappings=[{"source": "k", "target": "k"}, {"source": "v", "target": "v"}],
        column_types={"k": "VARCHAR(40)", "v": "TEXT"},
        write_mode=mode, conflict_columns=["k"],
    )


ROWS_1 = [["a", "x"], ["b", None], ["c", ""], ["é中", " "]]
ROWS_2 = [["a", "y"], ["d", "__DF_SQL_NULL__"]]


@pytest.fixture()
def pg_table():
    name = "brandpg_" + uuid.uuid4().hex[:8]
    yield name
    conn = _pg()
    conn.cursor().execute(f'DROP TABLE IF EXISTS public."{name}"')
    conn.close()


@pytest.fixture()
def my_table():
    name = "brandmy_" + uuid.uuid4().hex[:8]
    yield name
    conn = _mysql()
    conn.cursor().execute(f"DROP TABLE IF EXISTS `{name}`")
    conn.close()


@needs_pg
@pytest.mark.parametrize("flag", [False, True])
def test_pg_table_created_by_a_run_carries_the_flag_marker_and_run2_upserts(
    pg_table, monkeypatch, flag, audit_events
):
    _flag(monkeypatch, flag)
    res = _pg_write(pg_table, ROWS_1)
    assert res.ok, res.error
    assert _pg_comment(pg_table) == ("datawrap:managed" if flag else "dataflow:managed")
    res = _pg_write(pg_table, ROWS_2)
    assert res.ok, res.error
    created = [e for e in audit_events if e.get("action") == "brand.object_name.created"]
    assert bool(created) is flag


@needs_mysql
@pytest.mark.parametrize("flag", [False, True])
def test_mysql_table_created_by_a_run_carries_the_flag_marker_and_run2_upserts(
    my_table, monkeypatch, flag
):
    _flag(monkeypatch, flag)
    res = _my_write(my_table, ROWS_1)
    assert res.ok, res.error
    assert _my_comment(my_table) == ("datawrap:managed" if flag else "dataflow:managed")
    res = _my_write(my_table, ROWS_2)
    assert res.ok, res.error


@needs_pg
@pytest.mark.parametrize("marker", ["dataflow:managed", "datawrap:managed"])
@pytest.mark.parametrize("flag", [False, True])
def test_pg_marked_table_without_a_key_index_is_recognised_as_ours(
    pg_table, monkeypatch, marker, flag
):
    """Either marker means "this product made the table": the upsert may add
    the unique index without the operator-table DDL gate, whatever the flag."""
    _flag(monkeypatch, flag)
    conn = _pg()
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{pg_table}" (k VARCHAR(40), v TEXT)')
        cur.execute(f"COMMENT ON TABLE public.\"{pg_table}\" IS '{marker}'")
        cur.execute(f"INSERT INTO public.\"{pg_table}\" VALUES ('a', 'old')")
    conn.close()
    res = _pg_write(pg_table, ROWS_2)
    assert res.ok, res.error
    assert _pg_comment(pg_table) == marker, "the marker is never rewritten"


@needs_mysql
@pytest.mark.parametrize("marker", ["dataflow:managed", "datawrap:managed"])
@pytest.mark.parametrize("flag", [False, True])
def test_mysql_marked_table_without_a_key_index_is_recognised_as_ours(
    my_table, monkeypatch, marker, flag
):
    _flag(monkeypatch, flag)
    conn = _mysql()
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE `{my_table}` (k VARCHAR(40), v TEXT) "
            f"CHARACTER SET utf8mb4 COMMENT='{marker}'"
        )
        cur.execute(f"INSERT INTO `{my_table}` VALUES ('a', 'old')")
    conn.close()
    res = _my_write(my_table, ROWS_2)
    assert res.ok, res.error
    assert _my_comment(my_table) == marker


@needs_pg
def test_unmarked_operator_table_still_needs_approval(pg_table, monkeypatch):
    """Recognising the new marker must not widen the gate to operator tables."""
    _flag(monkeypatch, True)
    conn = _pg()
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{pg_table}" (k VARCHAR(40), v TEXT)')
        cur.execute(f"COMMENT ON TABLE public.\"{pg_table}\" IS 'datawrap:managedX'")
    conn.close()
    res = _pg_write(pg_table, ROWS_2)
    assert not res.ok
    assert "did not create" in (res.error or ""), res.error


# ---------------------------------------------------------------------------
# The session name on PostgreSQL connections
# ---------------------------------------------------------------------------


@needs_pg
@pytest.mark.parametrize("flag", [False, True])
def test_pg_session_application_name_follows_the_flag(monkeypatch, flag):
    from connectors.write_resilience import apply_postgres_session_guards

    _flag(monkeypatch, flag)
    conn = _pg()
    try:
        apply_postgres_session_guards(conn)
        with conn.cursor() as cur:
            cur.execute("SHOW application_name")
            assert cur.fetchone()[0] == ("datawrap" if flag else "dataflow")
    finally:
        conn.close()
