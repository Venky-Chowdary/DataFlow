"""Follow-up review 2 (independent re-review of H3 + H4): a MySQL URL's query is read where SQLAlchemy reads it.

``mysql_options.url_option_refusal`` (and the egress guard's query-host reader,
``residency_addressing.url_query_pairs``) took everything after the *first*
``?`` of the whole URL as the query. SQLAlchemy allows ``?``, ``#`` and ``@`` in
the password and starts the query after the database, so in
``mysql+pymysql://u:p?charset=a@h.example/db?zzz_probe=1`` they read the key
``charset`` (with the rest of the URL as its value) while SQLAlchemy hands
PyMySQL ``zzz_probe``. An option file, LOCAL INFILE or a Unix socket on the
API host hid there from the save and pre-dial checks (``dial_pymysql`` still
refused the option keywords, so H3 + H4 were not end to end; the socket rule
had no such second check), and a legitimate password with ``?k=v`` in it was
refused for a key that is not in the query at all.

Now both read the keys from ``sqlalchemy.engine.make_url(text).query`` (and
the egress reader judges the text after the first ``?`` as well, so it only
gains hosts), falling back to the plain split for a URL SQLAlchemy cannot parse.
"""

from __future__ import annotations

import pytest

_HIDDEN = [
    # The password's "?charset=..." is an allowed key, so the old split found nothing to refuse.
    pytest.param("mysql+pymysql://u:p?charset=a@h.example/db?zzz_probe=1", "zzz_probe", id="question-unknown"),
    pytest.param("mysql+pymysql://u:p?sql_mode=x@h.example/db?read_default_file=/etc/mysql/debian.cnf",
                 "read_default_file", id="question-option-file"),
    pytest.param("mysql+pymysql://u:p?connect_timeout=5@h.example/db?local_infile=1", "local_infile",
                 id="question-local-infile"),
    pytest.param("mysql+pymysql://u:p?charset=a#b@h.example/db?read_default_group=client", "read_default_group",
                 id="question-hash"),
    pytest.param("mysql+pymysql://u:p?charset=1@x@h.example/db?init_command=SET%20x%3D1", "init_command",
                 id="question-at"),
    pytest.param("mariadb+pymysql://u:p?collation=a@h.example/db?auth_plugin_map=x", "auth_plugin_map",
                 id="mariadb"),
    pytest.param("mysql+pymysql://u:p#f=1@h.example/db?local_infile=1", "local_infile", id="hash-control"),
]


@pytest.mark.parametrize("url, key", _HIDDEN)
def test_the_option_check_reads_the_query_sqlalchemy_passes(url, key):
    from connectors.mysql_options import url_option_refusal
    from sqlalchemy.engine import make_url

    assert key in make_url(url).query  # what PyMySQL would receive
    refusal = url_option_refusal(url)
    assert key in refusal, refusal
    assert "p?" not in refusal and "@h.example" not in refusal


@pytest.mark.parametrize("url, key", _HIDDEN)
def test_save_refuses_an_option_behind_a_password_question_mark(url, key):
    from services.egress_guard import connector_save_refusal

    refusal = connector_save_refusal({"type": "mysql", "name": "m", "connection_string": url})
    assert key in refusal, refusal


@pytest.mark.parametrize("url, key", _HIDDEN)
def test_the_pre_dial_check_refuses_an_option_behind_a_password_question_mark(url, key):
    from services.egress_guard import EgressRefused, assert_connector_dial_allowed

    with pytest.raises(EgressRefused) as caught:
        assert_connector_dial_allowed("h.example", url, db_type="mysql")
    assert key in str(caught.value)


def test_a_unix_socket_behind_a_password_question_mark_is_judged(monkeypatch):
    """The socket rule (follow-up review 1) read the same split: the socket hid and PyMySQL dialled it."""
    from services.egress_guard import EgressRefused, assert_connector_dial_allowed, connector_save_refusal

    import services.egress_guard as eg

    url = "mysql+pymysql://u:p?charset=x@h.example/db?unix_socket=/var/run/mysqld/mysqld.sock"
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.setattr(eg, "_workspace_egress_entries", lambda _ws: [])
    monkeypatch.setenv("DATAFLOW_EGRESS_ENFORCE_NEW", "1")
    with pytest.raises(EgressRefused):
        assert_connector_dial_allowed("h.example", url, db_type="mysql")
    assert connector_save_refusal({"type": "mysql", "name": "m", "connection_string": url})


@pytest.mark.parametrize("url", [
    "mysql+pymysql://u:p?zzz=1@h.example/db?charset=utf8mb4",
    "mysql+pymysql://u:p#read_default_file=x@h.example/db?ssl_mode=REQUIRED",
    "mysql+pymysql://u:p%3Flocal_infile%3D1@h.example/db",
    "mysql+pymysql://u:p@h.example/db?charset=utf8mb4&connect_timeout=5",
])
def test_a_password_with_question_marks_and_allowed_keys_is_accepted(url):
    from connectors.mysql_options import url_option_refusal

    assert url_option_refusal(url) == ""


@pytest.mark.parametrize("url, key", [
    ("mysql://h.example:notaport/db?read_default_file=/etc/mysql/debian.cnf", "read_default_file"),
    ("mysql+pymysql://u:p@h.example:3306abc/db?local_infile=1", "local_infile"),
])
def test_a_url_sqlalchemy_cannot_parse_falls_back_to_the_plain_split(url, key):
    from connectors.mysql_options import url_option_refusal

    assert key in url_option_refusal(url)
