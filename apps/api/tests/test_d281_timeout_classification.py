"""D366, D309, D308, D358: timeout copy and one session per preflight.

SQL Server "unavailable or does not exist" is a dead host. A MongoDB
connection string's port is the one the stall message names. MySQL
introspect inside a tool-call scope opens one session. A replica-set seed
that answers and then points at unreachable members says so.
"""

from __future__ import annotations

from types import SimpleNamespace

from connectors.generic_sql import _is_not_found_error
from connectors.mongodb_common import replica_set_members_unreachable
from services.endpoint_session import endpoint_session_scope, scope_stats
from services.error_taxonomy import classify
from src.ai.copilot.tool_deadline import describe_target
from src.ai.copilot.transfer_tools import _source_read_error


def test_sqlserver_unavailable_is_not_a_missing_table() -> None:
    exc = RuntimeError(
        "(20009, b'DB-Lib error message 20009, severity 9:\\n"
        "Unable to connect: Adaptive Server is unavailable or does not exist (qar)\\n')"
    )
    assert _is_not_found_error(exc) is False
    message = _source_read_error(
        {"name": "QAR41-MS", "host": "qar.example", "port": 1433, "type": "sqlserver"},
        "r43_ms",
        str(exc),
    )
    assert message.startswith("source unreachable/slow")
    assert "list tables" not in message
    classified = classify(exc, engine="sqlserver", side="source", phase="connect")
    assert classified is not None
    assert classified.code.startswith("network.")
    assert classified.code != "object.table_not_found"


def test_a_real_missing_table_is_still_not_found() -> None:
    exc = RuntimeError('Invalid object name \'dbo.r43_ms\'.')
    assert _is_not_found_error(exc) is True


def test_stall_label_uses_the_connection_string_port() -> None:
    label = describe_target({
        "type": "mongodb",
        "host": "tunnel.example",
        "port": 27017,
        "connection_string": (
            "mongodb://reader:secret@tunnel.example:38403/app"
            "?directConnection=true"
        ),
    })
    assert label == "tunnel.example:38403"
    assert "secret" not in label


def test_mysql_introspect_opens_one_session_per_preflight(monkeypatch) -> None:
    import connectors.mysql_conn as mysql_conn

    opened = {"n": 0}

    class _Cur:
        def __init__(self, conn):
            self.conn = conn
            self.sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def execute(self, sql, *_a):
            self.sql = str(sql)

        def fetchone(self):
            if "net_read_timeout" in self.sql:
                seconds = int(getattr(self.conn, "_read_timeout", None) or 40)
                return (seconds, seconds, "STRICT_TRANS_TABLES")
            return ("STRICT_TRANS_TABLES",)

    class _Conn:
        def ping(self, reconnect=False):  # noqa: ARG002
            return None

        def rollback(self):
            return None

        def close(self):
            return None

        def cursor(self):
            return _Cur(self)

    def _dial(*_args, **_kwargs):
        opened["n"] += 1
        return _Conn()

    monkeypatch.setattr(mysql_conn, "dial_pymysql", _dial)
    monkeypatch.setattr(
        "services.egress_guard.assert_connector_dial_allowed", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        "connectors.write_resilience.apply_mysql_session_guards", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        mysql_conn, "resolve_sql_endpoint",
        lambda **_kw: {
            "host": "db.example", "port": 3306, "username": "u",
            "password": "p", "database": "app",
        },
    )
    with endpoint_session_scope():
        first = mysql_conn.get_connection(
            host="db.example", port=3306, database="app", username="u",
            password="p", connection_string="", ssl=False, purpose="introspect",
        )
        first.close()
        second = mysql_conn.get_connection(
            host="db.example", port=3306, database="app", username="u",
            password="p", connection_string="", ssl=False, purpose="introspect",
        )
        second.close()
        stats = scope_stats()
    assert opened["n"] == 1
    assert stats["mysql"]["opened"] == 1
    assert stats["mysql"]["reused"] == 1


def test_replica_set_seed_names_the_unreachable_members() -> None:
    from pymongo.errors import ServerSelectionTimeoutError

    seed = ("tunnel.example", 38403)
    member = ("mongo-1.internal", 27017)

    class _Server:
        def __init__(self, known: bool, error: str = "") -> None:
            self.is_server_type_known = known
            self.error = error

    client = SimpleNamespace(
        topology_description=SimpleNamespace(
            replica_set_name="rs0",
            server_descriptions=lambda: {
                seed: _Server(True),
                member: _Server(False, "Connection refused"),
            },
        )
    )
    text = replica_set_members_unreachable(
        client,
        "mongodb://tunnel.example:38403/app",
        ServerSelectionTimeoutError("timed out"),
    )
    assert "replica set 'rs0'" in text
    assert "mongo-1.internal:27017" in text
    assert "directConnection=true" in text
    assert text.startswith("MongoDB replica set")
