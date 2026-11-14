"""Follow-up review 2, H3 + H4: a MySQL URL cannot hand PyMySQL a host file, socket or LOCAL INFILE.

SQLAlchemy's PyMySQL dialect passes every URL query key to ``pymysql.connect``
(``opts.update(url.query)``). Two of them reach the API host:

* H3: ``read_default_file`` / ``read_default_group`` make PyMySQL read a
  ``.cnf`` file on the API host and take ``socket`` / ``user`` / ``password``
  / ``host`` from it, so a URL that names a public host dials a Unix socket on
  this machine with the host's own MySQL credentials. The egress guard judged
  only the URL's host. A file that is not a ``.cnf`` raised a parser error that
  echoes its first line.
* H4: ``local_infile=1`` (or ``client_flag`` with ``CLIENT.LOCAL_FILES``)
  advertises LOAD DATA LOCAL to the server, and the per-connection allowlist
  that stops a rogue server from asking for any file was installed lazily, only
  by a write / bulk ``get_connection``: a pre-ping or a session query on the
  generic SQL engine sent whatever file the tenant's server named.

Now only the PyMySQL query keys the product uses are accepted in a
MySQL-family URL (TLS keys, charset / collation, timeouts, the host keys the
egress guard judges); anything else is refused at save, at the pre-dial egress
check and in ``dial_pymysql`` (the one MySQL dial), naming the key and never
the value. A connector's ``extra`` / top-level / ``connect_args`` forms of the
dangerous keys are refused at save. ``dial_pymysql`` accepts ``local_infile`` (or
the LOCAL_FILES flag) only from the product's own bulk-write path, and the
LOAD DATA LOCAL allowlist is installed when the MySQL modules are imported, so
no connection can send a host file the writer did not stage.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from services import egress_guard as eg

_API_ROOT = Path(__file__).resolve().parents[1]
_SECRET_LINE = "SENTINEL-FIRST-LINE-OF-A-HOST-FILE"
_DANGEROUS = [
    "read_default_file", "read_default_group", "local_infile", "init_command",
    "defer_connect", "auth_plugin_map", "server_public_key", "bind_address",
    "client_flag", "conv", "cursorclass", "named_pipe",
]
_SCHEMES = ["mysql+pymysql", "mysql", "mariadb", "mariadb+pymysql"]
_REAL_GETADDRINFO = socket.getaddrinfo


def _fake_getaddrinfo(host, *args, **kwargs):
    if str(host).lower() == "localhost":
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0))]
    if host == "db.example.com":
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
    return _REAL_GETADDRINFO(host, None, flags=socket.AI_NUMERICHOST)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo)
    monkeypatch.setattr(eg, "_workspace_egress_entries", lambda _ws: [])

    def _no_dial(*_a, **_k):
        raise AssertionError("a test dialled")

    monkeypatch.setattr(socket.socket, "connect", _no_dial)


@pytest.fixture
def dials(monkeypatch):
    """What PyMySQL would dial: recorded after it read any option file, before any socket."""
    import pymysql.connections as pc

    seen: list[dict] = []

    def record(self, sock=None):
        seen.append({"unix_socket": self.unix_socket, "host": self.host, "user": self.user,
                     "local_files": bool(self.client_flag & 0x80)})
        raise RuntimeError("intercepted before the dial")

    monkeypatch.setattr(pc.Connection, "connect", record)
    return seen


@pytest.fixture
def host_cnf(tmp_path):
    path = tmp_path / "host.cnf"
    path.write_text("[client]\nsocket=/var/run/mysqld/mysqld.sock\nuser=debian-sys-maint\npassword=hostsecret\n")
    return str(path)


@pytest.fixture
def host_file(tmp_path):
    path = tmp_path / "not-a-cnf.txt"
    path.write_text(_SECRET_LINE + "\n")
    return str(path)


def _cfg(url: str, db_type: str = "mysql") -> dict:
    return {"type": db_type, "host": "db.example.com", "connection_string": url, "workspace_id": "ws-secfu24"}


def _value(key: str, cnf: str) -> str:
    return {
        "read_default_file": cnf, "read_default_group": "client", "local_infile": "1",
        "init_command": "SET @x=1", "defer_connect": "1", "auth_plugin_map": "x",
        "server_public_key": "k", "bind_address": "93.184.216.34", "client_flag": "128",
        "conv": "x", "cursorclass": "x", "named_pipe": "1",
    }[key]


def _connect(cfg: dict) -> str:
    """Build the generic SQL engine and connect; the refusal text, or what happened."""
    from connectors.generic_sql import _build_engine

    try:
        engine = _build_engine(cfg)
    except (eg.EgressRefused, ValueError) as exc:
        return f"REFUSED: {exc}"
    try:
        engine.connect()
    except (eg.EgressRefused, ValueError) as exc:
        return f"REFUSED: {exc}"
    except Exception as exc:  # noqa: BLE001 - the probe reports what happened
        if isinstance(getattr(exc, "orig", None), (eg.EgressRefused, ValueError)) and "intercepted" not in str(exc):
            return f"REFUSED: {exc.orig}"
        return f"{type(exc).__name__}: {exc}"
    return "connected"


def _names_the_key(text: str, key: str, *values: str) -> None:
    assert text.startswith("REFUSED"), text
    assert key in text, text
    for value in values:
        assert value not in text, text
    assert "hostsecret" not in text and _SECRET_LINE not in text and "p@" not in text


# --- H3: option files ------------------------------------------------------------


@pytest.mark.parametrize("scheme", _SCHEMES)
@pytest.mark.parametrize("query", ["read_default_file={cnf}", "read_default_file={cnf}&read_default_group=client"])
def test_an_option_file_in_the_url_never_reaches_pymysql(dials, host_cnf, scheme, query):
    url = f"{scheme}://u:p@db.example.com/db?{query.format(cnf=host_cnf)}"
    outcome = _connect(_cfg(url, "mariadb" if scheme.startswith("mariadb") else "mysql"))
    _names_the_key(outcome, "read_default_file", host_cnf)
    assert dials == [], "PyMySQL read the host's option file and would dial its socket"


def test_a_file_that_is_not_a_cnf_is_never_echoed(dials, host_file):
    outcome = _connect(_cfg(f"mysql+pymysql://u:p@db.example.com/db?read_default_file={host_file}"))
    _names_the_key(outcome, "read_default_file", host_file)


@pytest.mark.parametrize(
    "query",
    [
        "read%5Fdefault%5Ffile={cnf}", "READ_DEFAULT_FILE={cnf}", "read-default-file={cnf}",
        "charset=utf8mb4&read_default_file={cnf}", "read_default_file=&read_default_file={cnf}",
        "read_default_group=client",
    ],
)
def test_other_spellings_are_refused(dials, host_cnf, query):
    url = f"mysql+pymysql://u:p@db.example.com/db?{query.format(cnf=host_cnf)}"
    outcome = _connect(_cfg(url))
    assert outcome.startswith("REFUSED"), outcome
    assert dials == []


# --- H4: LOAD DATA LOCAL -------------------------------------------------------


@pytest.mark.parametrize("query", ["local_infile=1", "local_infile=true", "client_flag=128", "client_flag=2176"])
def test_local_infile_in_the_url_never_reaches_pymysql(dials, query):
    outcome = _connect(_cfg(f"mysql+pymysql://u:p@db.example.com/db?{query}"))
    assert outcome.startswith("REFUSED"), outcome
    assert dials == []


@pytest.mark.parametrize("key", _DANGEROUS)
@pytest.mark.parametrize("db_type", ["mysql", "mariadb", "tidb", "singlestore"])
def test_every_dangerous_key_is_refused_before_the_driver(dials, host_cnf, key, db_type):
    url = f"mysql+pymysql://u:p@db.example.com/db?{key}={_value(key, host_cnf)}"
    outcome = _connect(_cfg(url, db_type))
    _names_the_key(outcome, key, host_cnf)
    assert dials == []


@pytest.mark.parametrize("key", _DANGEROUS)
def test_the_pre_dial_egress_check_refuses_it(key, host_cnf):
    url = f"mysql+pymysql://u:p@db.example.com/db?{key}={_value(key, host_cnf)}"
    with pytest.raises(eg.EgressRefused) as caught:
        eg.assert_connector_dial_allowed("db.example.com", url)
    assert key in str(caught.value) and host_cnf not in str(caught.value)


@pytest.mark.parametrize("key", _DANGEROUS)
@pytest.mark.parametrize("where", ["url", "top", "extra", "connect_args"])
@pytest.mark.parametrize("db_type", ["mysql", "mariadb"])
def test_save_refuses_it_in_every_form(key, host_cnf, where, db_type):
    base = {"name": "c", "type": db_type, "host": "db.example.com", "workspace_id": "ws-secfu24"}
    value = _value(key, host_cnf)
    form = {
        "url": {**base, "connection_string": f"mysql+pymysql://u:p@db.example.com/db?{key}={value}"},
        "top": {**base, key: value},
        "extra": {**base, "extra": {key: value}},
        "connect_args": {**base, "extra": {"connect_args": {key: value}}},
    }[where]
    refusal = eg.connector_save_refusal(form)
    assert key in refusal and host_cnf not in refusal, refusal


@pytest.mark.parametrize("value", [0, False, [], {}, None, ""])
def test_save_ignores_an_empty_dangerous_setting(value):
    form = {"name": "c", "type": "mysql", "host": "db.example.com", "workspace_id": "ws-secfu24",
            "extra": {"local_infile": value, "read_default_file": value}}
    assert eg.connector_save_refusal(form) == ""


@pytest.mark.parametrize("value", [1, True, ["x"], {"a": 1}, "1"])
def test_save_refuses_a_non_empty_dangerous_setting_of_any_type(value):
    form = {"name": "c", "type": "mysql", "host": "db.example.com", "workspace_id": "ws-secfu24",
            "extra": {"local_infile": value}}
    assert "local_infile" in eg.connector_save_refusal(form)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"local_infile": True}, {"client_flag": 0x80}, {"client_flag": 0x2 | 0x80},
        {"read_default_file": "/etc/mysql/debian.cnf"}, {"read_default_group": "client"},
        {"init_command": "SET @x=1"}, {"bind_address": "127.0.0.1"},
    ],
)
def test_dial_pymysql_refuses_it_from_anything_but_the_bulk_writer(monkeypatch, kwargs):
    import pymysql

    from connectors.mysql_tls import TLS_PREFERRED, TlsPlan, dial_pymysql

    called: list[dict] = []
    monkeypatch.setattr(pymysql, "connect", lambda **kw: called.append(kw) or SimpleNamespace())
    with pytest.raises(ValueError) as caught:
        dial_pymysql({"host": "db.example.com", "user": "u", "password": "p", **kwargs},
                     plan=TlsPlan(mode=TLS_PREFERRED))
    assert next(iter(kwargs)) in str(caught.value) and "/etc/mysql" not in str(caught.value)
    assert called == []


# --- what is unchanged ----------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "charset=utf8mb4", "charset=utf8mb4&collation=utf8mb4_bin", "connect_timeout=5&read_timeout=30",
        "ssl-mode=REQUIRED", "ssl_disabled=true", "sql_mode=ANSI_QUOTES", "autocommit=true", "",
    ],
)
def test_the_query_keys_the_product_uses_still_dial(dials, query):
    url = "mysql+pymysql://u:p@db.example.com/db" + (f"?{query}" if query else "")
    outcome = _connect(_cfg(url))
    assert "intercepted" in outcome, outcome
    assert dials and dials[0]["host"] == "db.example.com" and dials[0]["local_files"] is False
    assert eg.connector_save_refusal({"name": "c", **_cfg(url)}) == ""


def test_the_bulk_writer_still_asks_for_local_infile(monkeypatch):
    import pymysql

    from connectors import mysql_conn

    captured: dict = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        raise RuntimeError("intercepted")

    monkeypatch.setattr(pymysql, "connect", fake_connect)
    monkeypatch.setattr(eg, "assert_connector_dial_allowed", lambda *a, **k: None)
    for purpose, expected in (("write", True), ("bulk", True), ("introspect", False)):
        captured.clear()
        with pytest.raises(RuntimeError, match="intercepted"):
            mysql_conn.get_connection(host="db.example.com", port=3306, database="db", username="u",
                                      password="p", connection_string="", ssl=False, purpose=purpose)
        assert captured["local_infile"] is expected, purpose


@pytest.mark.parametrize(
    "load",
    [
        "import connectors.mysql_tls",
        "import connectors.mysql_conn",
        "import connectors.mysql_change_stream",
        # The generic SQL engine imports the MySQL dial before it builds a MySQL engine.
        "from connectors.generic_sql import _build_engine; "
        "_build_engine({'type': 'mysql', 'host': 'db.example.com', 'connection_string': "
        "'mysql+pymysql://u:p@db.example.com/db'})",
    ],
    ids=["mysql_tls", "mysql_conn", "mysql_change_stream", "generic_sql_engine"],
)
def test_the_local_infile_guard_is_installed_when_the_mysql_modules_load(load):
    """A fresh process: loading the MySQL dial is enough, no bulk write first."""
    code = textwrap.dedent(
        f"""
        import pymysql.connections as pc
        before = pc._send_local_file.__name__
        {load}
        print(before, pc._send_local_file.__name__)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=_API_ROOT, capture_output=True, text=True, timeout=180,
        env={**os.environ, "PYTHONPATH": str(_API_ROOT)},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert out.stdout.split()[-2:] == ["_send_local_file", "_guarded_send_local_file"], out.stdout


def test_a_rogue_server_gets_no_host_file_from_a_connection_that_never_staged_one(tmp_path):
    """A fresh process, a connection with LOCAL_FILES that no bulk writer made: the file stays put."""
    secret = tmp_path / "host-secret.txt"
    secret.write_text("SENTINEL-TENANT-SECRET")
    code = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(_API_ROOT / 'tests')!r})
        import connectors.mysql_tls  # noqa: F401  (the MySQL dial every MySQL path loads)
        import pymysql
        from test_mysql_local_infile_guard import RogueMySQLServer, _TRIGGER
        server = RogueMySQLServer({str(secret)!r})
        conn = pymysql.connect(host="127.0.0.1", port=server.port, user="u", password="p",
                               local_infile=True, connect_timeout=5)
        try:
            conn.cursor().execute(_TRIGGER)
            outcome = "sent"
        except Exception as exc:
            outcome = type(exc).__name__
        conn.close()
        server.close()
        print("OUTCOME", outcome, bytes(server.received).decode() or "-")
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", code], cwd=_API_ROOT, capture_output=True, text=True, timeout=180,
        env={**os.environ, "PYTHONPATH": str(_API_ROOT)},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    line = [ln for ln in out.stdout.splitlines() if ln.startswith("OUTCOME")][-1]
    assert "SENTINEL-TENANT-SECRET" not in line, line
    assert line.split()[1] == "OperationalError", line
