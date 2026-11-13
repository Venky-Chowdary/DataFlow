"""Gate-8's NULL-marker probe opens each destination the way its writer does.

PR 427 blocker: the probe re-typed the destination config with the catalog's
*driver* type, so a DuckDB destination (driver ``generic_sql``) was opened as
SQLAlchemy dialect ``generic_sql``: ``NoSuchModuleError``, the check was
"not measured", and with strict fail-closed every DuckDB run failed and its
undo threw a correct transfer away. Now:

* the probe opens the writer's own resolved config (its ``type`` untouched)
  through the same pooled engine the writer used;
* a destination the probe has no route for (no SQLAlchemy dialect, an
  in-memory database a second connection cannot see, a non-SQL store) is
  ``not_applicable`` with the reason, which never fails a run;
* a probe that applies but cannot complete (timeout, cap, query error, or no
  connection back through the writer's engine after the write) is
  ``not_measured``, which fails strict.
"""

from __future__ import annotations

import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL
from src.transfer.models import EndpointConfig
from tests.test_cr_d125_file_quoted_empty_fidelity import _isolated  # noqa: F401 — isolated engine stores

MAPS = [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}]
RECORDS = [{"id": 1, "note": None}, {"id": 2, "note": "fine"}]


def _gate(endpoint, *, mode="strict", summary=None):
    from services.null_marker_guard import apply_null_marker_gate

    return apply_null_marker_gate(
        {"passed": True, "message": "Checksum matches."},
        endpoint=endpoint, mappings=MAPS, records=RECORDS, rows_written=2,
        dest_summary={"table": endpoint.table, "target_rows_before": 0, "sync_mode": "append",
                      **(summary or {})},
        validation_mode=mode,
    )


# --------------------------------------------------------------------------
# Every destination the probe runs against: a leaked marker is found
# --------------------------------------------------------------------------


def _sqlalchemy_seed(cfg: dict, table: str, schema: str | None = None) -> None:
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine

    engine = get_sqlalchemy_engine(cfg)
    meta = sa.MetaData()
    tbl = sa.Table(table, meta, sa.Column("id", sa.Integer, primary_key=True),
                   sa.Column("note", sa.String(40)), schema=schema)
    with engine.begin() as conn:
        meta.create_all(conn)
        conn.execute(tbl.insert(), [{"id": 1, "note": SQL_NULL_SENTINEL}, {"id": 2, "note": "fine"}])


def _sqlalchemy_drop(cfg: dict, table: str, schema: str | None = None) -> None:
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine

    with get_sqlalchemy_engine(cfg).begin() as conn:
        sa.Table(table, sa.MetaData(), schema=schema).drop(conn, checkfirst=True)


def _endpoint(kind: str, tmp_path, table: str):
    from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

    if kind in ("postgresql", "generic_sql"):
        if not pg_up():
            pytest.skip("live PostgreSQL not reachable")
        c = pg_creds()
        common = dict(host=c["host"], port=int(c["port"]), database=c["database"],
                      username=c["username"], password=c["password"], schema="public", table=table)
        if kind == "generic_sql":
            url = f"postgresql+psycopg2://{c['username']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"
            return EndpointConfig(kind="database", format="generic_sql", connection_string=url, **common)
        return EndpointConfig(kind="database", format="postgresql", **common)
    if kind == "mysql":
        if not mysql_up():
            pytest.skip("live MySQL not reachable")
        c = mysql_creds()
        return EndpointConfig(kind="database", format="mysql", host=c["host"], port=int(c["port"]),
                              database=c["database"], username=c["username"], password=c["password"],
                              table=table)
    if kind in ("sqlite", "duckdb"):
        return EndpointConfig(kind="database", format=kind, database=str(tmp_path / f"d.{kind}"), table=table)
    if kind in ("sqlserver", "oracle"):
        return _optional_server(kind, table)
    raise AssertionError(kind)


def _optional_server(kind: str, table: str):
    """SQL Server / Oracle through the C3 live modules' own login (CI-only here)."""
    import importlib

    live = importlib.import_module(f"tests.test_c3_lossy_pairs_{kind}_live")
    if not live._ready():
        pytest.skip(f"{kind} not reachable here (see tests/test_c3_lossy_pairs_{kind}_live.py)")
    cfg = live._cfg()
    extra = {k: cfg[k] for k in ("trust_server_certificate", "service_name") if k in cfg}
    return EndpointConfig(
        kind="database", format=kind, host=cfg["host"], port=int(cfg["port"]),
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema=cfg.get("schema") or ("dbo" if kind == "sqlserver" else ""), table=table, extra=extra,
    )


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlite", "duckdb", "generic_sql", "sqlserver", "oracle"])
def test_the_probe_reads_every_destination_through_its_writers_engine(kind, tmp_path):
    from src.transfer.adapters import resolve_connector_config

    table = f"mpd_{uuid.uuid4().hex[:8]}"
    endpoint = _endpoint(kind, tmp_path, table)
    cfg = resolve_connector_config(endpoint)
    schema = endpoint.schema if kind in ("postgresql", "generic_sql", "sqlserver", "oracle") else None
    _sqlalchemy_seed(cfg, table, schema)
    try:
        recon = _gate(endpoint)
        check = recon["null_marker_check"]
        # One marker row the run's records do not hold: found, on every engine.
        assert check["status"] == "leaked" and check["destination_raw_rows"] == 1, check
        assert recon["passed"] is False
    finally:
        _sqlalchemy_drop(cfg, table, schema)


def test_a_clean_duckdb_table_passes_in_strict(tmp_path):
    from src.transfer.adapters import resolve_connector_config

    endpoint = _endpoint("duckdb", tmp_path, "clean")
    cfg = resolve_connector_config(endpoint)
    _sqlalchemy_seed(cfg, "clean")
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine

    with get_sqlalchemy_engine(cfg).begin() as conn:
        conn.execute(sa.text("UPDATE clean SET note = NULL WHERE id = 1"))
    recon = _gate(endpoint)
    assert recon["passed"] is True, recon
    assert recon["null_marker_check"]["status"] == "passed", recon["null_marker_check"]


# --------------------------------------------------------------------------
# Not applicable: Datawrap's own lack of a route never fails a run
# --------------------------------------------------------------------------


def test_no_dialect_for_the_destination_is_not_applicable_and_strict_passes(monkeypatch, tmp_path):
    import sqlalchemy.exc as sa_exc

    import connectors.generic_sql as generic_sql

    def no_plugin(_cfg):
        raise sa_exc.NoSuchModuleError("Can't load plugin: sqlalchemy.dialects:nosuchdb")

    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", no_plugin)
    recon = _gate(_endpoint("duckdb", tmp_path, "t"))
    check = recon["null_marker_check"]
    assert check["status"] == "not_applicable" and "plugin" in check["reason"], check
    assert recon["passed"] is True and not recon.get("unproven"), recon


@pytest.mark.parametrize("database", [":memory:", "md:analytics"])
def test_a_database_a_second_connection_cannot_see_is_not_applicable(database):
    endpoint = EndpointConfig(kind="database", format="duckdb", database=database, table="t")
    recon = _gate(endpoint)
    assert recon["null_marker_check"]["status"] == "not_applicable", recon["null_marker_check"]
    assert recon["passed"] is True


def test_a_destination_with_no_sql_probe_is_not_applicable():
    endpoint = EndpointConfig(kind="database", format="mongodb", host="127.0.0.1", database="d", table="t")
    recon = _gate(endpoint)
    assert recon["null_marker_check"]["status"] == "not_applicable", recon.get("null_marker_check")
    assert recon["passed"] is True


# --------------------------------------------------------------------------
# Applies but cannot complete: strict fails
# --------------------------------------------------------------------------


def test_no_connection_back_through_the_writers_engine_fails_strict():
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    endpoint = EndpointConfig(kind="database", format="postgresql", host="127.0.0.1", port=1,
                              database=c["database"], username=c["username"], password=c["password"],
                              schema="public", table="t")
    strict = _gate(endpoint)
    check = strict["null_marker_check"]
    assert check["status"] == "not_measured" and "could not connect" in check["reason"], check
    assert strict["passed"] is False
    assert _gate(endpoint, mode="balanced")["passed"] is True


def test_a_query_error_on_a_supported_engine_fails_strict(tmp_path):
    # The DuckDB file exists, the table the writer reported does not.
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from src.transfer.adapters import resolve_connector_config

    endpoint = _endpoint("duckdb", tmp_path, "missing_table")
    with get_sqlalchemy_engine(resolve_connector_config(endpoint)).begin() as conn:
        conn.execute(sa.text("CREATE TABLE other (id INTEGER)"))
    recon = _gate(endpoint)
    assert recon["null_marker_check"]["status"] == "not_measured", recon["null_marker_check"]
    assert recon["passed"] is False


# --------------------------------------------------------------------------
# Guard: no destination type the writers support makes the probe guess a dialect
# --------------------------------------------------------------------------


def _writable_probe_types() -> dict[str, str]:
    from connectors.generic_sql import _DRIVERNAME_MAP
    from services.connector_capability_registry import CAPABILITY_REGISTRY
    from services.null_marker_guard import _SQL_PROBE_TYPES
    from src.transfer.connector_capabilities import _DRIVER_CAPS, CATALOG_ID_ALIASES, resolve_driver_type
    from src.transfer.connector_dispatch import has_writer
    from src.transfer.connector_registry import CONNECTOR_MODULES

    ids = set(CAPABILITY_REGISTRY) | set(CATALOG_ID_ALIASES) | set(_DRIVERNAME_MAP) | set(CONNECTOR_MODULES) \
        | set(_DRIVER_CAPS)
    out = {}
    for catalog_id in sorted(ids):
        driver = resolve_driver_type(catalog_id)
        writes = (_DRIVER_CAPS.get(driver) or {}).get("write") or has_writer(driver) or driver == "generic_sql"
        if writes and driver in _SQL_PROBE_TYPES:
            out[catalog_id] = driver
    return out


def test_the_writer_registry_is_enumerated():
    types = _writable_probe_types()
    assert {"duckdb", "sqlite", "postgresql", "mysql", "sqlserver", "oracle", "clickhouse"} <= set(types)
    assert len(types) >= 50, types


@pytest.mark.parametrize("catalog_id", sorted(_writable_probe_types()))
def test_no_writable_destination_type_makes_the_probe_guess_a_dialect(catalog_id, tmp_path):
    """The probe builds the writer's engine or says not_applicable; never a dialect it guessed."""
    from connectors.generic_sql import _build_url
    from services.null_marker_probe import ProbeNotApplicable, probe_engine
    from src.transfer.adapters import resolve_connector_config

    endpoint = EndpointConfig(kind="database", format=catalog_id, host="127.0.0.1", port=1,
                              database=str(tmp_path / "g.db"), username="u", password="p", table="t")
    if catalog_id in ("generic_sql", "genericsql"):
        # The generic writer needs a URL; it writes nowhere without one.
        endpoint.connection_string = f"sqlite:///{tmp_path / 'g.db'}"
    cfg = resolve_connector_config(endpoint)
    try:
        engine = probe_engine(cfg)
    except ProbeNotApplicable as exc:
        assert "generic_sql" not in str(exc).split("dialects:")[-1], exc
        return
    writer_url = _build_url(dict(cfg))
    writer_url = writer_url.render_as_string(hide_password=False) if hasattr(writer_url, "render_as_string") \
        else str(writer_url)
    assert engine.url.render_as_string(hide_password=False) == writer_url
    assert engine.dialect.name != "generic_sql"


def test_engine_run_into_duckdb_is_probed_and_succeeds_in_strict(tmp_path, _isolated):  # noqa: F811
    """The real engine path (SQLite -> DuckDB, strict overwrite): the check runs and passes."""
    import sqlite3

    from tests.test_cr_d125_file_quoted_empty_fidelity import run_transfer
    src = tmp_path / "s.sqlite"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, None), (2, "fine"), (3, SQL_NULL_SENTINEL)])
    dest = EndpointConfig(kind="database", format="duckdb", database=str(tmp_path / "d.duckdb"), table="t")
    result = run_transfer(EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"), dest)
    check = ((result.destination_summary or {}).get("reconciliation") or {}).get("null_marker_check") \
        or (getattr(result, "reconciliation", None) or {}).get("null_marker_check")
    assert result.success is True, result.error
    # The source really holds one marker-spelling literal; the destination holds the same one.
    assert check and check["status"] == "passed" and check["scope"] == "table_written_by_run", check
    assert check["destination_raw_rows"] == 1 and check["source_raw_rows"] == 1, check
