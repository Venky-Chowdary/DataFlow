"""Security review round 3 (H7): the DuckDB playground cannot read host files.

Three gaps:

- ``_duckdb_reads_external`` only looked after ``FROM`` / ``JOIN``, so
  ``DESCRIBE '<path>'``, ``SHOW '<path>'``, ``SUMMARIZE '<path>'`` and
  ``PIVOT`` / ``UNPIVOT '<path>'`` passed (DuckDB reads the file's header, or
  its values as column names). They are now refused with a string or a
  quoted-path table, like ``FROM '<path>'``.
- The connect-time backstop (``enable_external_access = false`` in
  ``connect_args``) could be overridden by the connector URL:
  ``duckdb-sqlalchemy`` merges URL query keys into the DuckDB config after
  ``connect_args``. DuckDB configuration keys (``enable_external_access``,
  ``allowed_*``, ``*extension*``, ``allow_*``, ``autoinstall*``,
  ``autoload*``, ``lock_configuration``) are now refused in every connector
  URL and database value, and stripped from the playground engine's URL.
- ``md:`` / ``motherduck:`` databases skipped the backstop; the playground now
  refuses them (MotherDuck runs DuckDB locally too, and needs external access).

``duckdb-sqlalchemy`` is not installed here, so the URL-config override is
proved on the code path this repository controls: the URL and ``connect_args``
handed to ``create_engine``, merged the way that dialect merges them.
"""

from __future__ import annotations

import pytest

from src.routers.query_router import _duckdb_reads_external, _is_safe_sql

V = "/srv/exports/_workspaces/a/victim.csv"


@pytest.mark.parametrize("sql", [
    f"describe '{V}'",
    f"DESCRIBE TABLE '{V}'",
    f"show '{V}'",
    f"show table '{V}'",
    f"summarize '{V}'",
    f"SUMMARIZE TABLE '{V}'",
    f"pivot '{V}' on secret using count(*)",
    f"select * from (pivot '{V}' on secret using count(*))",
    f"unpivot '{V}' on secret into name k value val",
    f"select * from (unpivot '{V}' on secret into name k value val)",
    f'describe "{V}"',
    'describe "victim.parquet"',
    "describe '/etc/passwd'",
    f"pivot_wider '{V}' on secret using count(*)",
    f"pivot_longer '{V}' on secret into name k value v",
])
def test_reader_statements_with_a_path_are_refused(sql):
    assert _duckdb_reads_external(sql) is True


@pytest.mark.parametrize("sql", [
    "describe orders",
    "describe select 1",
    "show tables",
    "show all tables",
    "summarize orders",
    "pivot orders on region using sum(amount)",
    "select * from (unpivot orders on q1, q2 into name quarter value amount)",
    "select 'describe' as word, 'show' from orders",
    "select describe from orders",
])
def test_ordinary_metadata_and_pivots_still_pass(sql):
    assert _duckdb_reads_external(sql) is False


def test_the_reviewers_spellings_fail_against_real_duckdb_with_external_access_on(tmp_path):
    """Each refused spelling does read the file when nothing stops it."""
    duckdb = pytest.importorskip("duckdb")
    victim = tmp_path / "victim.csv"
    victim.write_text("secret\nA-EXPORT-ROW\n")
    for sql in (f"describe '{victim}'", f"show '{victim}'",
                f"select * from (pivot '{victim}' on secret using count(*))"):
        con = duckdb.connect(config={"enable_external_access": True})
        try:
            rows = con.execute(sql).fetchall()
        finally:
            con.close()
        assert rows, sql  # DuckDB answered from the file
        assert _duckdb_reads_external(sql) is True


CONFIG_KEYS = [
    "enable_external_access", "Enable_External_Access", "allowed_directories", "allowed_paths",
    "allow_unsigned_extensions", "allow_community_extensions", "autoinstall_known_extensions",
    "autoload_known_extensions", "extension_directory", "custom_extension_repository",
    "autoinstall_extension_repository", "allow_persistent_secrets", "lock_configuration",
]


@pytest.mark.parametrize("key", CONFIG_KEYS)
def test_duckdb_config_keys_are_refused_in_a_connector_url(key):
    from connectors import generic_sql

    for cfg in (
        {"type": "duckdb", "connection_string": f"duckdb:///:memory:?{key}=true"},
        {"type": "duckdb", "database": f":memory:?{key}=true"},
        {"type": "duckdb", "database": f"local.duckdb?{key}=true"},
        {"type": "generic_sql", "connection_string": f"duckdb:///local.duckdb?threads=2&{key}=1"},
    ):
        with pytest.raises(ValueError, match="(?i)configuration"):
            generic_sql._build_url(dict(cfg))


def test_ordinary_duckdb_url_options_still_build():
    from connectors import generic_sql

    url = generic_sql._build_url({"type": "duckdb", "connection_string": "duckdb:///:memory:?threads=2&access_mode=read_only"})
    assert "threads=2" in str(url)


def _fake_engine(url_text):
    import sqlalchemy as sa

    class _Engine:
        url = sa.engine.make_url(url_text)

    return _Engine()


def test_the_playground_engine_url_cannot_override_the_backstop(monkeypatch):
    """The merge duckdb-sqlalchemy does (``config.update(url_config)``) keeps access off."""
    import sqlalchemy

    from src.routers import query_router

    captured = {}

    class _Guarded:
        class dialect:  # noqa: N801 - a dialect stand-in
            supports_native_decimal = False

    def fake_create_engine(url, **kwargs):
        captured["url"], captured["kwargs"] = url, kwargs
        return _Guarded()

    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
    query_router._duckdb_playground_engine(
        _fake_engine("duckdb:///:memory:?enable_external_access=true&allowed_paths=/&threads=2")
    )
    url = captured["url"]
    url_config = dict(url.query)
    assert "enable_external_access" not in url_config and "allowed_paths" not in url_config
    assert url_config.get("threads") == "2"
    config = dict(captured["kwargs"]["connect_args"]["config"])
    config.update(url_config)  # duckdb-sqlalchemy 1.5.x _prepare_connection_params
    assert config["enable_external_access"] is False


@pytest.mark.parametrize("database", ["md:mydb", "MD:mydb", "motherduck:mydb"])
def test_the_playground_refuses_motherduck(database):
    from fastapi import HTTPException

    from src.routers import query_router

    with pytest.raises(HTTPException) as caught:
        query_router._duckdb_playground_engine(_fake_engine(f"duckdb:///{database}"))
    assert caught.value.status_code == 400
    assert "motherduck" in caught.value.detail.lower()


def test_safe_sql_still_accepts_plain_describe():
    assert _is_safe_sql("describe orders")
