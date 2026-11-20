"""R10: a borrowed MySQL session ends its transaction when it is handed back.

Inside a tool-call scope (D308) introspect, probe, reconcile and DDL share one
login. A released session idled with its read transaction open, so the
metadata lock it took on the table blocked the writer's own ALTER (a widen)
until lock_wait_timeout ("Could not widen updated_at ... Lock wait timeout",
R6 D211 Mongo -> MySQL run 2).
"""

from __future__ import annotations

from connectors.mysql_conn import _MysqlHandback


class _Conn:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def rollback(self) -> None:
        self.calls.append("rollback")


def test_release_rolls_back_before_the_session_idles():
    conn = _Conn()
    released: list[str] = []
    handle = _MysqlHandback(conn, lambda: released.append("released") or conn.calls.append("released"))
    handle.close()
    handle.close()  # idempotent
    assert conn.calls == ["rollback", "released"]
    assert released == ["released"]


def test_live_reader_does_not_block_a_writer_alter():
    import uuid

    from tests import r11_pg_writer_route as r

    from services.endpoint_session import endpoint_session_scope

    from connectors.mysql_conn import get_connection

    table = f"r10_mdl_{uuid.uuid4().hex[:8]}"
    cfg = r.mysql_family("mysql")
    r.mysql_execute("mysql", f"CREATE TABLE `{table}` (id INT PRIMARY KEY, v VARCHAR(10))")
    try:
        with endpoint_session_scope(["mysql"]):
            reader = get_connection(**_conn_kwargs(cfg), purpose="introspect")
            with reader.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")
                cur.fetchall()
            reader.close()  # handed back to the scope, not closed
            writer = get_connection(**_conn_kwargs(cfg), purpose="write")
            try:
                with writer.cursor() as cur:
                    cur.execute("SET SESSION lock_wait_timeout = 3")
                    cur.execute(f"ALTER TABLE `{table}` MODIFY v VARCHAR(40)")
            finally:
                writer.close()
    finally:
        r.mysql_execute("mysql", f"DROP TABLE IF EXISTS `{table}`")


def _conn_kwargs(cfg: dict) -> dict:
    import inspect

    from connectors.mysql_conn import get_connection

    names = set(inspect.signature(get_connection).parameters)
    out = {k: v for k, v in cfg.items() if k in names}
    return {"connection_string": "", "ssl": False, **out}
