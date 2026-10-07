"""CDC catch-up must not go green while a change is still unread.

Covers three QA defects: a Postgres update still sitting in the replication
slot, create-new columns ignoring the planned BIGINT/TIMESTAMP types, and a
finished one-shot job leaving its slot behind.
"""

from __future__ import annotations

from connectors.cdc_eos_sa import _col_sql_type, _ensure_dest_table
from services.cdc_catchup import release_finished_cdc_slot
from services.cdc_engine import ChangeBatch
from src.transfer.cdc_transfer import _drain_log_reader, _raise_if_stream_behind


class _QuietThenUpdate:
    """Empty polls while the slot still has the update, then one update."""

    def __init__(self) -> None:
        self.polls = 0

    def poll(self):
        self.polls += 1
        if self.polls == 3:
            yield ChangeBatch(updates=[{"id": "1", "qty": "9"}], resume_token="lsn-9")
            return
        yield ChangeBatch(resume_token="lsn-1")

    def capture_has_pending(self) -> bool:
        return self.polls < 3


def test_drain_waits_for_the_unread_update() -> None:
    reader = _QuietThenUpdate()
    applied: list[dict] = []

    def apply(change: ChangeBatch) -> bool:
        if change.updates:
            applied.extend(change.updates)
            return True
        return False

    outcome = _drain_log_reader(
        reader,
        apply,
        max_idle=2,
        max_rounds=6,
        sleep_sec=0,
    )
    assert outcome == "caught_up"
    assert applied == [{"id": "1", "qty": "9"}]
    assert reader.polls >= 3


class _AlwaysBehind:
    def poll(self):
        yield ChangeBatch(resume_token="lsn-1")

    def capture_has_pending(self) -> bool:
        return True

    slot_name = "df_orders_slot"

    def replication_lag_bytes(self) -> int:
        return 4096


def test_drain_refuses_completion_while_the_slot_is_behind() -> None:
    reader = _AlwaysBehind()
    outcome = _drain_log_reader(
        reader,
        lambda _change: False,
        max_idle=3,
        max_rounds=2,
        sleep_sec=0,
    )
    assert outcome == "behind"
    try:
        _raise_if_stream_behind(reader, outcome)
    except Exception as exc:
        message = str(exc)
    else:
        raise AssertionError("behind stream must not complete")
    assert "df_orders_slot" in message
    assert "row-count" in message
    assert "kept" in message


class _NoProof:
    def poll(self):
        yield ChangeBatch(resume_token="q")


def test_reader_without_a_pending_proof_keeps_the_idle_stop() -> None:
    outcome = _drain_log_reader(
        _NoProof(),
        lambda _change: False,
        max_idle=1,
        max_rounds=3,
        sleep_sec=0,
    )
    assert outcome == "unknown"
    _raise_if_stream_behind(_NoProof(), outcome)


class _Conn:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, stmt, _params=None):
        self.sql.append(str(getattr(stmt, "text", stmt)))


def test_mysql_and_postgres_create_use_the_planned_types(monkeypatch) -> None:
    monkeypatch.setattr(
        "connectors.cdc_eos_sa._existing_columns",
        lambda _conn, _table: {"id", "updated_at", "_df_lsn"},
    )
    logical = {"id": "BIGINT", "updated_at": "TIMESTAMP", "_df_lsn": "string"}
    columns = ["id", "updated_at", "_df_lsn"]

    mysql = _Conn()
    _ensure_dest_table(mysql, "mysql", "orders", columns, ["id"], logical)
    mysql_sql = mysql.sql[0].upper()
    assert "BIGINT" in mysql_sql
    assert "DATETIME(6)" in mysql_sql
    assert "VARCHAR(512)" not in mysql_sql
    assert "LONGTEXT" not in mysql_sql

    pg = _Conn()
    _ensure_dest_table(pg, "postgresql", "orders", columns, ["id"], logical)
    pg_sql = pg.sql[0].upper()
    assert "BIGINT" in pg_sql
    assert "TIMESTAMP" in pg_sql
    assert "VARCHAR(512)" not in pg_sql


def test_existing_dest_column_is_not_retyped(monkeypatch) -> None:
    monkeypatch.setattr(
        "connectors.cdc_eos_sa._existing_columns",
        lambda _conn, _table: {"id", "updated_at"},
    )
    conn = _Conn()
    _ensure_dest_table(
        conn,
        "mysql",
        "orders",
        ["id", "updated_at", "note"],
        ["id"],
        {"id": "BIGINT", "updated_at": "TIMESTAMP", "note": "BIGINT"},
    )
    alters = [sql for sql in conn.sql if "ADD" in sql.upper()]
    assert len(alters) == 1
    assert "NOTE" in alters[0].upper()
    assert "BIGINT" in alters[0].upper()
    assert all("MODIFY" not in sql.upper() for sql in conn.sql)


def test_unbounded_mysql_primary_key_stays_indexable() -> None:
    assert _col_sql_type("mysql", "id", ["id"], "string") == "VARCHAR(512)"
    assert _col_sql_type("postgresql", "id", ["id"], "string") == "TEXT"
    assert _col_sql_type("mysql", "_df_lsn", ["id"], "string") == "TEXT"


def test_one_shot_completion_drops_the_slot_and_clears_the_watermark(monkeypatch) -> None:
    dropped: list[tuple] = []

    def _drop(cfg, *, slot_name, publication_name):
        dropped.append((slot_name, publication_name, cfg.get("database")))
        return {"slot": "dropped", "publication": "absent", "slot_name": slot_name}

    cleared: list[str] = []
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture", _drop
    )
    monkeypatch.setattr(
        "services.sync_cursor.clear_watermark",
        lambda key: cleared.append(key) or {"cleared": True},
    )
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *a, **k: False
    )
    out = release_finished_cdc_slot(
        {
            "cdc_slot_name": "df_orders_slot",
            "cdc_publication_name": "df_pub_orders",
            "cursor_key": "pg:qa:orders→mysql:qa:orders:stream",
        },
        reason="completed",
        source_cfg={"type": "postgresql", "database": "qa_dataflow"},
        job_id="job-4604",
    )
    assert out["released"] is True
    assert dropped == [("df_orders_slot", "df_pub_orders", "qa_dataflow")]
    assert cleared == ["pg:qa:orders→mysql:qa:orders:stream"]


def test_schedule_and_failure_keep_the_slot(monkeypatch) -> None:
    def _boom(*_a, **_k):
        raise AssertionError("slot must stay")

    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture", _boom
    )
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *a, **k: True
    )
    owned = release_finished_cdc_slot(
        {"cdc_slot_name": "df_orders_slot"},
        reason="cancelled",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-7721",
    )
    assert owned == {
        "released": False,
        "reason": "schedule_owns_slot",
        "job_id": "job-7721",
    }

    failed = release_finished_cdc_slot(
        {"cdc_slot_name": "df_orders_slot"},
        reason="failed",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-x",
    )
    assert failed["released"] is False
    assert failed["reason"] == "not_terminal"


def test_active_slot_is_not_cleared(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.cdc_catchup._schedule_owns_slot", lambda *a, **k: False
    )
    monkeypatch.setattr(
        "connectors.postgresql_change_stream.release_pg_capture",
        lambda *_a, **_k: {"slot": "active", "slot_name": "df_orders_slot"},
    )

    def _no_clear(_key):
        raise AssertionError("watermark stays while the slot is still attached")

    monkeypatch.setattr("services.sync_cursor.clear_watermark", _no_clear)
    out = release_finished_cdc_slot(
        {"cdc_slot_name": "df_orders_slot", "cursor_key": "cursor-1"},
        reason="cancelled",
        source_cfg={"type": "postgresql", "database": "qa"},
        job_id="job-7721",
    )
    assert out["released"] is False
    assert out["reason"] == "active"
