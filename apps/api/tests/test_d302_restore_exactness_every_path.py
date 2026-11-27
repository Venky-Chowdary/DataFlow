"""Every automatic restore of an overwrite's pre-run rows must be exact.

D302 review: the MySQL row copy was made exact (triggers refuse, AUTO_INCREMENT
put back). The same rule on the other paths:

* generic-SQL row copy (``sql_row_copy``: SQL Server, Oracle, SQLite, the
  PostgreSQL family through the generic driver, ...): a table with triggers,
  identity / auto-increment columns, or an unreadable catalog keeps its
  backup and is not restored automatically; generated columns are left out
  of the copy (they recompute). Trigger detection per engine: PostgreSQL
  ``pg_trigger`` (non-internal), MySQL ``information_schema.TRIGGERS``, SQL
  Server ``sys.triggers``, Oracle ``all_triggers``, SQLite ``sqlite_master``.
* warehouse set-aside (Snowflake, BigQuery, Elasticsearch, S3): the restore
  puts the original object back whole (rename / table copy / index clone),
  no row is re-inserted, and these engines have no triggers or identity
  counters a copy could get wrong: exact by construction.

Fakes where no local engine exists; live PostgreSQL for the generic path.
"""

from __future__ import annotations

import contextvars
import time
import uuid

import pytest

from services.error_handling import TransferCancelled


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    """SQLAlchemy-like connection answering the catalog queries by keyword."""

    def __init__(self, answers: dict[str, list], fail: bool = False) -> None:
        self.answers = answers
        self.fail = fail
        self.sql: list[str] = []

    def execute(self, clause, params=None):
        text = str(clause)
        self.sql.append(text)
        if self.fail:
            raise RuntimeError("permission denied for catalog")
        for key, rows in self.answers.items():
            if key in text:
                return _Result(rows)
        return _Result([])


@pytest.mark.parametrize(
    "dialect,answers,expect",
    [
        ("sqlserver", {"sys.triggers": [("trg_audit",)], "sys.columns": [("id", 0, 0)]}, "triggers (trg_audit)"),
        ("mssql", {"sys.triggers": [], "sys.columns": [("id", 1, 0), ("a", 0, 0)]}, "identity"),
        ("oracle", {"all_triggers": [("TRG",)], "all_tab_cols": [("ID", 0, 0)]}, "triggers (TRG)"),
        ("oracle", {"all_triggers": [], "all_tab_cols": [("ID", 1, 0)]}, "identity"),
        ("sqlite", {"sqlite_master": [("t_ai",)], "pragma_table_xinfo": [("id", 0, 0)]}, "triggers (t_ai)"),
        ("postgresql", {"pg_trigger": [("trg",)], "pg_attribute": [("id", False, False)]}, "triggers (trg)"),
        ("mariadb", {"TRIGGERS": [], "COLUMNS": [("id", 1, 0)]}, "identity"),
    ],
)
def test_triggers_and_identity_refuse_the_automatic_restore(dialect, answers, expect):
    from connectors.overwrite_restore_steps import generic_catalog, generic_restore_refusal

    catalog = generic_catalog(_FakeConn(answers), dialect, "s", "t")
    assert expect in generic_restore_refusal(dialect, catalog)


def test_generated_columns_alone_do_not_refuse():
    from connectors.overwrite_restore_steps import generic_catalog, generic_restore_refusal

    catalog = generic_catalog(
        _FakeConn({"sys.triggers": [], "sys.columns": [("id", 0, 0), ("total", 0, 1)]}),
        "sqlserver", "dbo", "t",
    )
    assert catalog["generated"] == ["total"]
    assert generic_restore_refusal("sqlserver", catalog) == ""


def test_an_unreadable_catalog_refuses_rather_than_guessing():
    from connectors.overwrite_restore_steps import generic_catalog, generic_restore_refusal

    catalog = generic_catalog(_FakeConn({}, fail=True), "oracle", "S", "T")
    assert "could not be read" in generic_restore_refusal("oracle", catalog)


@pytest.mark.parametrize("engine", ["snowflake", "bigquery", "databricks", "clickhouse"])
def test_engines_without_triggers_are_exact_and_say_so(engine):
    from connectors.overwrite_restore_steps import generic_catalog, generic_restore_refusal

    conn = _FakeConn({})
    catalog = generic_catalog(conn, engine, "s", "t")
    assert catalog.get("none_by_engine") is True and conn.sql == []
    assert generic_restore_refusal(engine, catalog) == ""


def test_sqlserver_steps_quote_suspend_triggers_and_identity_insert():
    from connectors.overwrite_restore_steps import generic_manual_steps

    steps = generic_manual_steps(
        "sqlserver", "dbo", "Order Lines", "Order Lines__df_bak",
        ["id", "qty", "total"],
        {"triggers": ["trg"], "identity": ["id"], "generated": ["total"]},
    )
    body = steps[0]
    assert body.startswith("DISABLE TRIGGER ALL ON [dbo].[Order Lines]"), body
    assert "SET IDENTITY_INSERT [dbo].[Order Lines] ON" in body
    assert "INSERT INTO [dbo].[Order Lines] ([id], [qty]) SELECT [id], [qty] FROM [dbo].[Order Lines__df_bak]" in body
    assert "[total]" not in body and body.endswith("ENABLE TRIGGER ALL ON [dbo].[Order Lines]")
    assert "DBCC CHECKIDENT" in steps[1] and steps[-1] == "DROP TABLE [dbo].[Order Lines__df_bak]"


def test_oracle_steps_open_and_close_an_always_identity_and_triggers():
    from connectors.overwrite_restore_steps import generic_manual_steps

    steps = generic_manual_steps(
        "oracle", "APP", "ORDERS", "ORDERS__DF_BAK", ["ID", "V", "VIRT"],
        {"triggers": ["T1"], "identity": ["ID"], "generated": ["VIRT"]},
    )
    joined = " | ".join(steps)
    assert steps[0] == 'ALTER TABLE "APP"."ORDERS" DISABLE ALL TRIGGERS'
    assert 'MODIFY ("ID" GENERATED BY DEFAULT AS IDENTITY)' in joined
    assert 'INSERT INTO "APP"."ORDERS" ("ID", "V") SELECT "ID", "V" FROM "APP"."ORDERS__DF_BAK"' in joined
    assert 'GENERATED ALWAYS AS IDENTITY (START WITH LIMIT VALUE)' in joined
    assert steps[-1] == 'DROP TABLE "APP"."ORDERS__DF_BAK" PURGE'


def test_a_refused_generic_restore_keeps_the_backup_untouched(monkeypatch):
    import connectors.generic_sql as gs
    import connectors.table_manager as tm
    import services.engine_pool as pool

    conn = _FakeConn({"sys.triggers": [("trg",)], "sys.columns": [("id", 0, 0)]})

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *a):
            return False

    class _Engine:
        dialect = type("D", (), {"name": "mssql"})()

        def connect(self):
            return _Ctx()

        def raw_connection(self):
            raise AssertionError("the restore ran although it was refused")

    monkeypatch.setattr(gs, "get_sqlalchemy_engine", lambda cfg: _Engine())
    monkeypatch.setattr(pool, "release_engine", lambda e: None)
    with pytest.raises(tm.TableRestoreRefused):
        tm.restore_generic_sql_overwrite("sqlserver", {"schema": "dbo"}, "orders", "orders__df_bak")
    assert not any("DELETE" in s or "DROP" in s for s in conn.sql), conn.sql


def test_a_set_aside_backup_is_put_back_whole_on_a_cancel(monkeypatch):
    """Warehouse set-aside: exact by construction (the original object comes
    back; no rows are re-inserted), so the cancel restores it."""
    import connectors.overwrite_set_aside as osa
    import src.transfer.cancel_restore as cr
    from src.transfer.engine_finish import _settle_backup_after_error
    from src.transfer.models import EndpointConfig

    calls: list[tuple] = []
    monkeypatch.setattr(osa, "restore_set_aside", lambda *a, **k: calls.append(("restore",) + a[:4]))
    monkeypatch.setattr(osa, "discard_set_aside", lambda *a, **k: calls.append(("discard",) + a[:4]))
    monkeypatch.setattr(cr, "_count", lambda d, t: 7)
    dest = EndpointConfig(
        kind="database", format="snowflake", host="acct", database="DB", username="u",
        password="p", table="ORDERS",
        extra={"overwrite_backup": "ORDERS__df_bak", "overwrite_backup_engine": "snowflake"},
    )
    req = type("R", (), {"sync_mode": "full_refresh_overwrite", "stream_contracts": None})()
    exc = TransferCancelled("Transfer cancelled by user")
    assert _settle_backup_after_error(dest, exc, request=req) is True
    assert calls and calls[0][0] == "restore" and calls[0][1] == "snowflake", calls


class _Store:
    """Job store double with the real store's compare-and-swap semantics."""

    def __init__(self):
        self.jobs = {"job1": {"status": "running"}}
        self.on_read = None

    def get_job(self, job_id):
        snapshot = dict(self.jobs.get(job_id) or {})
        if self.on_read is not None:
            self.on_read(self.jobs[job_id])
        return snapshot

    def update_job_fields(self, job_id, fields, *, expected=None, touch_updated_at=True):
        from services.mongodb_service import expected_fields_hold

        if expected and not expected_fields_hold(self.jobs[job_id], expected):
            return False
        self.jobs[job_id].update(fields)
        return True


def _bound(store, fn):
    from services import cancel_check

    def run():
        cancel_check.set_progress_meter(cancel_check.ProgressMeter(store, "job1"))
        return fn()

    return contextvars.copy_context().run(run)


def test_a_cancel_after_the_load_is_visible_at_once():
    """The load committed, post-write verification is still running: the job
    says right away that the cancel came after the load."""
    from src.transfer.cancel_restore import LATE_CANCEL_MESSAGE, mark_write_complete
    from src.transfer.models import EndpointConfig

    store = _Store()
    dest = EndpointConfig(kind="database", format="mysql", table="orders")
    _bound(store, lambda: mark_write_complete(dest, "orders"))
    time.sleep(0.3)
    assert "cancel_note" not in store.jobs["job1"]
    store.jobs["job1"].update(status="cancelled", cancel_requested=True)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and "cancel_note" not in store.jobs["job1"]:
        time.sleep(0.05)
    assert store.jobs["job1"]["message"] == LATE_CANCEL_MESSAGE


def _pg():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    import psycopg2

    p = pg_creds()
    conn = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    conn.autocommit = True
    cfg = {
        "host": p["host"], "port": int(p["port"]), "database": p["database"],
        "username": p["username"], "password": p["password"], "schema": "public",
    }
    return conn, cfg


@pytest.mark.parametrize("shape", ["plain", "trigger", "identity"])
def test_generic_row_copy_restore_on_live_postgresql(shape):
    import connectors.table_manager as tm

    conn, cfg = _pg()
    t = f"d302g_{shape}_{uuid.uuid4().hex[:6]}"
    bak = f"{t}__df_bak"
    idcol = "id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY" if shape == "identity" else "id INT PRIMARY KEY"
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {t} ({idcol}, a TEXT, n INT, n2 INT GENERATED ALWAYS AS (n * 2) STORED)")
            if shape == "identity":
                cur.execute(f"INSERT INTO {t} (a, n) VALUES ('x', 1), ('y', 2)")
            else:
                cur.execute(f"INSERT INTO {t} (id, a, n) VALUES (1, 'x', 1), (2, 'y', 2)")
            cur.execute(f"CREATE TABLE {t}_audit (id INT)")
            if shape == "trigger":
                cur.execute(
                    f"CREATE FUNCTION {t}_f() RETURNS trigger LANGUAGE plpgsql AS "
                    f"$$ BEGIN INSERT INTO {t}_audit VALUES (NEW.id); RETURN NEW; END $$"
                )
                cur.execute(f"CREATE TRIGGER {t}_ai AFTER INSERT ON {t} FOR EACH ROW EXECUTE FUNCTION {t}_f()")
            cur.execute(f"SELECT id, a, n, n2 FROM {t} ORDER BY id")
            pre = cur.fetchall()
            cur.execute(f"CREATE TABLE {bak} (LIKE {t})")
            cur.execute(f"INSERT INTO {bak} (id, a, n) SELECT id, a, n FROM {t}")
            cur.execute(f"DELETE FROM {t}")
            cur.execute(f"DELETE FROM {t}_audit")
        if shape == "plain":
            tm.restore_generic_sql_overwrite("postgresql", cfg, t, bak, "public")
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, a, n, n2 FROM {t} ORDER BY id")
                assert cur.fetchall() == pre
                cur.execute("SELECT to_regclass(%s)", (bak,))
                assert cur.fetchone()[0] is None, "backup not dropped after an exact restore"
        else:
            with pytest.raises(tm.TableRestoreRefused) as err:
                tm.restore_generic_sql_overwrite("postgresql", cfg, t, bak, "public")
            assert ("triggers" if shape == "trigger" else "identity") in str(err.value)
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {bak}")
                assert cur.fetchone()[0] == 2, "the refused restore touched the backup"
                cur.execute(f"SELECT COUNT(*) FROM {t}_audit")
                assert cur.fetchone()[0] == 0, "the refused restore fired the trigger"
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {bak}")
            cur.execute(f"DROP TABLE IF EXISTS {t}")
            cur.execute(f"DROP TABLE IF EXISTS {t}_audit")
            cur.execute(f"DROP FUNCTION IF EXISTS {t}_f()")
        conn.close()


def _wait_watcher_done(job_id: str = "job1", budget: float = 5.0) -> bool:
    from src.transfer import cancel_restore

    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        with cancel_restore._LATE_LOCK:
            if job_id not in cancel_restore._LATE_WATCHED:
                return True
        time.sleep(0.05)
    return False


def test_the_late_cancel_note_never_overwrites_the_final_cancel_record(monkeypatch):
    """The run writes its final cancelled record (cancel_outcome) between the
    watcher's read and its write: the note must not replace it."""
    import src.transfer.cancel_restore as cr
    from src.transfer.models import EndpointConfig

    store = _Store()
    final = "Cancelled after 3,000 row(s) were already committed to the destination"

    def engine_finishes_now(rec):
        if rec.get("cancel_requested") and not rec.get("cancel_outcome"):
            rec.update(cancel_outcome={"rows_committed": 3000}, message=final)

    store.jobs["job1"].update(status="cancelled", cancel_requested=True)
    store.on_read = engine_finishes_now
    dest = EndpointConfig(kind="database", format="mysql", table="orders")
    _bound(store, lambda: cr.mark_write_complete(dest, "orders"))
    assert _wait_watcher_done()
    assert store.jobs["job1"]["message"] == final
    assert "cancel_note" not in store.jobs["job1"]


@pytest.mark.parametrize(
    "status", ["completed", "completed_with_quarantine", "completed_with_warnings", "failed"]
)
def test_the_late_cancel_watcher_stops_on_every_terminal_status(status):
    """A finished run's watcher must not linger and label a later cancel or
    resume of the same job id."""
    import src.transfer.cancel_restore as cr
    from src.transfer.models import EndpointConfig

    store = _Store()
    dest = EndpointConfig(kind="database", format="mysql", table="orders")
    store.jobs["job1"]["status"] = status
    _bound(store, lambda: cr.mark_write_complete(dest, "orders"))
    assert _wait_watcher_done(), f"watcher still polling after {status}"
    store.jobs["job1"].update(status="cancelled", cancel_requested=True)
    time.sleep(0.3)
    assert "cancel_note" not in store.jobs["job1"]
