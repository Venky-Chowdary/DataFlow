"""MySQL connection URL parsing / field merge."""

from __future__ import annotations

from connectors.mysql_conn import _parse_mysql_url


def test_parse_mysql_railway_public_url():
    url = "mysql://root:secret@tokaido.proxy.rlwy.net:32253/railway"
    parsed = _parse_mysql_url(url)
    assert parsed["host"] == "tokaido.proxy.rlwy.net"
    assert parsed["port"] == 32253
    assert parsed["username"] == "root"
    assert parsed["password"] == "secret"
    assert parsed["database"] == "railway"


def test_parse_mysql_url_with_encoded_password():
    url = "mysql://root:p%40ss%2Fword@db.example.com:3307/app"
    parsed = _parse_mysql_url(url)
    assert parsed["password"] == "p@ss/word"
    assert parsed["port"] == 3307
    assert parsed["database"] == "app"


def test_parse_ignores_non_mysql_schemes():
    assert _parse_mysql_url("postgres://u:p@h/db") == {}
    assert _parse_mysql_url("") == {}
