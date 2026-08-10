"""Wave 40: ClickHouse ReplacingMergeTree upsert + DB2 MERGE (NULL-safe ON)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_clickhouse_replacing_upsert_inserts_only_never_deletes():
    from connectors.generic_sql import _clickhouse_replacing_upsert

    executed: list[str] = []

    class _Result:
        rowcount = 2

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(stmt))
            return _Result()

    class _Table:
        def insert(self):
            return "INSERT_INTO_CH"

    n = _clickhouse_replacing_upsert(
        _Conn(),
        _Table(),
        [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
        ["id"],
        ["id", "v"],
        ["v"],
    )
    assert n == 2
    assert executed == ["INSERT_INTO_CH"]
    assert not any("DELETE" in e.upper() for e in executed)


def test_upsert_batch_clickhouse_skips_delete_insert_fallback():
    from connectors.generic_sql import _upsert_batch

    calls: list[str] = []

    class _Result:
        rowcount = 1

    class _Col:
        def __eq__(self, other):  # noqa: ANN001
            return ("eq", other)

    class _Table:
        name = "t"
        schema = None
        c = {"id": _Col(), "name": _Col()}

        def insert(self):
            calls.append("insert")
            return "INSERT"

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            if "DELETE" in text.upper() or "delete" in str(stmt).lower():
                calls.append("delete")
            elif stmt == "INSERT" or "INSERT" in text.upper():
                calls.append("insert_exec")
            else:
                calls.append(text[:40])
            return _Result()

        def rollback(self):
            calls.append("rollback")

    # Force native path to fail so we exercise the ClickHouse-safe fallback.
    import connectors.generic_sql as gs

    original = gs._clickhouse_replacing_upsert

    def _boom(*_a, **_k):  # noqa: ANN001
        import sqlalchemy as sa

        raise sa.exc.SQLAlchemyError("boom")

    gs._clickhouse_replacing_upsert = _boom  # type: ignore[assignment]
    try:
        n = _upsert_batch(
            _Conn(),
            _Table(),
            [{"id": 1, "name": "x"}],
            ["id"],
            ["id", "name"],
            "clickhouse",
        )
    finally:
        gs._clickhouse_replacing_upsert = original  # type: ignore[assignment]

    assert n == 1
    assert "insert" in calls or "insert_exec" in calls
    assert "delete" not in calls


def test_db2_merge_sql_null_safe_and_session_stage():
    from connectors.generic_sql import _db2_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "ORDERS"
        schema = "APP"

    import sqlalchemy as sa

    table = sa.table(
        "ORDERS",
        sa.column("id"),
        sa.column("amt"),
        schema="APP",
    )
    # Use a lightweight stand-in with .name/.schema for our helper.
    table_obj = MagicMock()
    table_obj.name = "ORDERS"
    table_obj.schema = "APP"

    n = _db2_merge_upsert(
        _Conn(),
        table_obj,
        [{"id": 1, "amt": 10}, {"id": 2, "amt": None}],
        ["id"],
        ["id", "amt"],
        ["amt"],
    )
    assert n == 2
    joined = "\n".join(executed)
    assert "DECLARE GLOBAL TEMPORARY TABLE SESSION.DF_MRG_" in joined
    assert "MERGE INTO" in joined
    assert "WHEN MATCHED THEN UPDATE" in joined
    assert "WHEN NOT MATCHED THEN INSERT" in joined
    # NULL-safe ON: (t.id = s.id OR (t.id IS NULL AND s.id IS NULL))
    assert "IS NULL" in joined


def test_build_table_clickhouse_uses_replacing_when_conflict():
    pytest.importorskip("clickhouse_sqlalchemy")
    import sqlalchemy as sa

    from connectors.generic_sql import _build_table_for_write

    engine = MagicMock()
    engine.dialect.name = "clickhouse"
    table = _build_table_for_write(
        engine,
        "events",
        None,
        ["id", "payload", "_df_lsn"],
        {"id": "integer", "payload": "string", "_df_lsn": "string"},
        db_type="clickhouse",
        conflict_columns=["id"],
    )
    # Compile with the real dialect: the MagicMock engine has no DDL compiler,
    # so compiling against it returned a mock repr that matched nothing.
    from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect

    # Engine kwargs are attached as table.args / dialect_options depending on version.
    ddl = str(sa.schema.CreateTable(table).compile(dialect=ClickHouseDialect()))
    assert "ReplacingMergeTree" in ddl or "replacingmergetree" in ddl.lower()


def test_null_safe_merge_on_shared_helper():
    from connectors.writer_common import null_safe_merge_on

    on = null_safe_merge_on(
        ["id", "tenant"],
        left_alias="t",
        right_alias="s",
        quote_column=lambda c: f'"{c}"',
    )
    assert "IS NULL" in on
    assert '"id"' in on
    assert '"tenant"' in on


def test_email_writer_quarantine_and_gate8_meta():
    """SMTP has no independent verify — stamp sample + quarantine typed carriers."""
    from connectors.email import write_mapped_rows

    with patch("connectors.email.smtplib.SMTP") as smtp_cls:
        server = MagicMock()
        smtp_cls.return_value.__enter__ = lambda s: server
        smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        result = write_mapped_rows(
            host="smtp.example",
            port=587,
            username="u",
            password="p",
            database="ops@example.com",
            table_name="report",
            headers=["id", "blob"],
            data_rows=[["1", "!!!bad!!!"], ["2", "YWI="]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "TEXT"},
                {"source": "blob", "target": "blob", "target_type": "BYTEA"},
            ],
            column_types={"id": "TEXT", "blob": "BYTEA"},
            error_policy="quarantine",
            connection_string="smtp://u:p@smtp.example:587/?to=ops@example.com&format=jsonl",
        )
    assert result.ok is True
    assert result.rejected_rows >= 1
    assert result.meta.get("reconcile_sample") is not None
    # Valid base64 row should survive; invalid quarantined.
    assert result.rows_written == 1
