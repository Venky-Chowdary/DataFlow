"""Wave 45: Informix + Firebird MERGE; Gate-8 generic_sql read-back honesty."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_informix_merge_temp_with_no_log_and_null_safe_on():
    from connectors.generic_sql import _informix_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    class _Table:
        name = "sale"
        schema = "informix"

    n = _informix_merge_upsert(
        _Conn(),
        _Table(),
        [{"cust_id": 1, "salecount": 3}],
        ["cust_id"],
        ["cust_id", "salecount"],
        ["salecount"],
    )
    assert n == 1
    blob = "\n".join(executed)
    assert "CREATE TEMP TABLE" in blob
    assert "WITH NO LOG" in blob
    assert "MERGE INTO" in blob
    assert "IS NULL" in blob
    assert "WHEN MATCHED THEN UPDATE SET" in blob
    assert any("DROP TABLE" in e for e in executed)


def test_firebird_merge_uses_rdb_database_stage():
    from connectors.generic_sql import _firebird_merge_upsert

    executed: list[tuple[str, dict | None]] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append((str(getattr(stmt, "text", stmt)).upper(), params))

    class _Table:
        name = "BOOKS"
        schema = None

    n = _firebird_merge_upsert(
        _Conn(),
        _Table(),
        [{"isbn": "1", "price": 9.5}, {"isbn": "2", "price": 11.0}],
        ["isbn"],
        ["isbn", "price"],
        ["price"],
    )
    assert n == 2
    assert len(executed) == 2
    sql, params = executed[0]
    assert "MERGE INTO" in sql
    assert "FROM RDB$DATABASE" in sql
    assert "IS NULL" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert params is not None
    assert params.get("isbn") == "1"


def test_upsert_batch_routes_informix_and_firebird():
    from connectors import generic_sql as gs

    calls: list[str] = []

    def _ifx(*_a, **_k):  # noqa: ANN001
        calls.append("informix")
        return 1

    def _fb(*_a, **_k):  # noqa: ANN001
        calls.append("firebird")
        return 2

    class _Col:
        def __eq__(self, other):  # noqa: ANN001
            return ("eq", other)

    class _Table:
        name = "t"
        schema = None
        c = {"id": _Col(), "v": _Col()}

        def insert(self):
            return "INSERT"

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            return MagicMock(rowcount=1)

        def rollback(self):
            pass

    orig_i = gs._informix_merge_upsert
    orig_f = gs._firebird_merge_upsert
    gs._informix_merge_upsert = _ifx  # type: ignore[assignment]
    gs._firebird_merge_upsert = _fb  # type: ignore[assignment]
    try:
        n1 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "informix"
        )
        n2 = gs._upsert_batch(
            _Conn(), _Table(), [{"id": 1, "v": "x"}], ["id"], ["id", "v"], "firebird"
        )
    finally:
        gs._informix_merge_upsert = orig_i  # type: ignore[assignment]
        gs._firebird_merge_upsert = orig_f  # type: ignore[assignment]

    assert n1 == 1 and n2 == 2
    assert calls == ["informix", "firebird"]


def test_verify_target_generic_sql_else_uses_sqlalchemy_readback():
    from services import reconciliation as recon

    with patch.object(
        recon, "verify_generic_sql_table", return_value=(7, "abc")
    ) as mock_v:
        count, chk = recon.verify_target(
            "generic_sql",
            {"type": "teradata", "host": "td.example", "database": "dw"},
            schema="dw",
            table_name="orders",
            fallback_rows=0,
            fallback_checksum="",
        )
    assert count == 7 and chk == "abc"
    mock_v.assert_called_once()
    assert mock_v.call_args.kwargs.get("engine_hint") == "teradata"


def test_verify_target_catalog_teradata_routes_generic_sql():
    from services import reconciliation as recon

    with patch.object(
        recon, "verify_generic_sql_table", return_value=(3, "xyz")
    ) as mock_v:
        count, chk = recon.verify_target(
            "teradata",
            {"type": "teradata", "host": "td.example"},
            schema="",
            table_name="t",
            fallback_rows=-1,
            fallback_checksum="",
        )
    assert count == 3 and chk == "xyz"
    mock_v.assert_called_once()


def test_verify_generic_sql_table_fail_closed_on_connect_error():
    from services.reconciliation import verify_generic_sql_table

    with patch(
        "connectors.generic_sql.get_sqlalchemy_engine",
        side_effect=RuntimeError("no driver"),
    ):
        count, chk = verify_generic_sql_table(
            dest={"type": "hana", "host": "x"},
            table_name="t",
            engine_hint="hana",
        )
    assert count == -1 and chk == ""
