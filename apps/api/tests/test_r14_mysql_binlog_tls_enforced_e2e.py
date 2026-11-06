"""MySQL D10 (third review): the binlog reader's own connections are checked for TLS.

PyMySQL and SQLAlchemy dials refuse a REQUIRED / VERIFY_* session that came
up in plaintext (``mysql_tls.dial_pymysql``). The binlog reader
(pymysqlreplication) dials by itself from ``resolved_kwargs_for_external_dial``,
so nothing checked what it got: a driver or proxy that drops the TLS keywords
left the binlog stream (row data) in plaintext with no error.

``_binlog_kwargs`` now hands the reader a connect function that reads
``Ssl_cipher`` from the server on every connection it opens (control and
stream) and refuses a plaintext session the plan said must be encrypted.
Live MySQL 8 (:3306, read-only).
"""

from __future__ import annotations

import pytest

from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg


def _plaintext_driver(monkeypatch):
    """A driver that ignores the TLS keywords (PyMySQL 1.1.x without CLIENT.SSL)."""
    import connectors.mysql_tls as mt

    real = mt.connect_kwargs_for
    monkeypatch.setattr(mt, "connect_kwargs_for",
                        lambda base, plan: {**real(base, mt.TlsPlan(mode=mt.TLS_DISABLED,
                                                                    kwargs={"ssl_disabled": True}))})


def _control_connection(cfg: dict):
    """Open the reader's control connection exactly as BinLogStreamReader does."""
    from pymysqlreplication import BinLogStreamReader

    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    kwargs = MySqlChangeStreamCdc(cfg, "t", "id")._binlog_kwargs(blocking=False, only_events=[])
    reader = BinLogStreamReader(**kwargs)
    try:
        reader._BinLogStreamReader__connect_to_ctl()
        conn = reader._ctl_connection
        with conn.cursor() as cur:
            cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            row = cur.fetchone()
        return row.get("Value") if isinstance(row, dict) else row[1]
    finally:
        reader.close()


def test_required_binlog_control_connection_refuses_plaintext(monkeypatch):
    from connectors.mysql_tls import TlsModeError

    cfg = {**_cfg("mysql"), "type": "mysql", "ssl": True}
    _plaintext_driver(monkeypatch)
    with pytest.raises(TlsModeError, match="not encrypted"):
        _control_connection(cfg)


def test_required_binlog_control_connection_is_encrypted():
    cfg = {**_cfg("mysql"), "type": "mysql", "ssl": True}
    assert _control_connection(cfg)  # a cipher name


def test_disabled_binlog_connection_is_allowed_in_plaintext():
    cfg = {**_cfg("mysql"), "type": "mysql", "ssl": False,
           "connection_string": "?ssl-mode=DISABLED"}
    assert _control_connection(cfg) == ""
