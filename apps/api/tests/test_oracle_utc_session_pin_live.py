"""Accuracy review finding 8, live: instants land in Oracle zone columns unchanged.

The session's default zone is forced to a non-UTC region with DST
(``ORA_SDTZ``, which python-oracledb applies at connect), so a writer that sent
clock digits into an unpinned session would move every instant. Through the
real engine (Validate on, strict, no Risk Contract):

* the product's Oracle session reads back ``+00:00`` while a bare connection
  reads the forced zone;
* PostgreSQL ``timestamptz`` values at offsets -12:00 .. +14:00, half- and
  quarter-hour zones included, and on both sides of DST transitions land in an
  existing ``TIMESTAMP WITH TIME ZONE``, an existing ``TIMESTAMP WITH LOCAL
  TIME ZONE`` and a created column as the same UTC instants;
* a cross-host style Oracle to Oracle copy (the server-side ``INSERT ...
  SELECT`` switched off, so the row writer runs) keeps every instant.

Runs in the ``oracle-live`` CI job (``tests/test_*oracle*.py``: Oracle Free plus
a PostgreSQL peer). Skips without ``DATAFLOW_ORACLE_ENABLE=1`` and a login.
"""

from __future__ import annotations

import socket
from datetime import datetime, timezone

import pytest

from services.brand_env import getenv_brand
from tests.c3_live_helpers import pg_endpoint, pg_sql, run_transfer, sqlalchemy_sql, token
from tests.test_commit_safe_review_round2 import isolated  # noqa: F401 — fixture

FORCED_ZONE = "America/St_Johns"  # -03:30 / -02:30 with DST: neither UTC nor a whole hour

# Offsets -12..+14 incl. half/quarter hours, plus instants either side of DST
# changes (US 2024-03-10 / 2024-11-03, EU 2024-03-31 / 2024-10-27, St John's).
VALUES = [
    "2024-01-15 10:00:00.000001-12:00",
    "2024-01-15 10:00:00-09:30",
    "2024-01-15 10:00:00-03:30",
    "2024-01-15 10:00:00+00:00",
    "2024-01-15 10:00:00+05:30",
    "2024-01-15 10:00:00+05:45",
    "2024-01-15 10:00:00+08:45",
    "2024-01-15 10:00:00+12:45",
    "2024-01-15 10:00:00+13:00",
    "2024-01-15 10:00:00.999999+14:00",
    "2024-03-10 01:59:59.999999-05:00",
    "2024-03-10 03:00:00-04:00",
    "2024-11-03 01:59:59-04:00",
    "2024-11-03 01:00:00-05:00",
    "2024-03-31 00:59:59+00:00",
    "2024-03-31 03:00:00+02:00",
    "2024-10-27 02:30:00+02:00",
    "2024-10-27 02:30:00+01:00",
    "2024-03-10 01:59:59-03:30",
    "2024-03-10 03:00:00-02:30",
]


def _utc(text: str) -> str:
    return datetime.fromisoformat(text).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")


EXPECTED = [(i, _utc(v)) for i, v in enumerate(VALUES, 1)]


def _cfg() -> dict:
    service = (getenv_brand("ORACLE_SERVICE") or getenv_brand("ORACLE_DATABASE") or "FREEPDB1").strip()
    user = getenv_brand("ORACLE_USER", "").strip()
    return {
        "type": "oracle",
        "host": getenv_brand("ORACLE_HOST", "localhost").strip(),
        "port": int(getenv_brand("ORACLE_PORT", "1521") or 1521),
        "database": service,
        "service_name": service,
        "username": user,
        "password": getenv_brand("ORACLE_PASSWORD", "").strip(),
        "schema": (getenv_brand("ORACLE_SCHEMA") or user).strip().upper(),
    }


def _ready() -> bool:
    if getenv_brand("ORACLE_ENABLE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    cfg = _cfg()
    if not (cfg["username"] and cfg["password"]):
        return False
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=3):
            pass
        from tests.helpers.live_env import pg_up

        return pg_up() and sqlalchemy_sql(cfg, "SELECT 1 FROM DUAL", fetch=True) == [(1,)]
    except Exception:  # noqa: BLE001 — not reachable: the module skips
        return False


pytestmark = pytest.mark.skipif(
    not _ready(),
    reason="Oracle live IT disabled — set DATAFLOW_ORACLE_ENABLE=1 plus DATAFLOW_ORACLE_USER/PASSWORD and a PostgreSQL peer",
)


@pytest.fixture()
def non_utc_session(monkeypatch):
    """New Oracle sessions default to FORCED_ZONE; cached pools are dropped around the test."""
    from services.engine_pool import dispose_all

    monkeypatch.setenv("ORA_SDTZ", FORCED_ZONE)
    dispose_all()
    yield
    dispose_all()


def _bare_zone() -> str:
    import oracledb

    cfg = _cfg()
    conn = oracledb.connect(user=cfg["username"], password=cfg["password"],
                            dsn=f"{cfg['host']}:{cfg['port']}/{cfg['service_name']}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT SESSIONTIMEZONE FROM DUAL")
            return str(cur.fetchone()[0])
    finally:
        conn.close()


def _endpoint(table: str):
    from src.transfer.models import EndpointConfig

    cfg = _cfg()
    return EndpointConfig(
        kind="database", format="oracle", host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema=cfg["schema"], table=table, extra={"service_name": cfg["service_name"]},
    )


def _drop_oracle(table: str) -> None:
    for name in {table, table.upper()}:
        sqlalchemy_sql(_cfg(), "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"" + name + "\" PURGE'; "
                       "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;")


def _literal(text: str) -> str:
    """Oracle's TIMESTAMP literal wants a space before the offset."""
    return f"{text[:-6]} {text[-6:]}"


def _stored_utc(table: str) -> list[tuple[int, str]]:
    """``(id, UTC instant)`` per row, whatever case the product named things in."""
    cfg = _cfg()
    cols = sqlalchemy_sql(
        cfg,
        "SELECT table_name, column_name FROM all_tab_columns WHERE owner = '"
        + cfg["schema"] + "' AND LOWER(table_name) = '" + table.lower() + "'",
        fetch=True,
    )
    assert cols, f"{table} not found"
    name = cols[0][0]
    by = {str(c).lower(): str(c) for _, c in cols}
    rows = sqlalchemy_sql(
        cfg,
        f"SELECT \"{by['id']}\", TO_CHAR(SYS_EXTRACT_UTC(\"{by['at']}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6') "
        f"FROM \"{name}\" ORDER BY 1",
        fetch=True,
    )
    return [(int(i), str(t)) for i, t in rows]


def _pg_source() -> str:
    table = f"ora_pin_{token()}"
    pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, at TIMESTAMPTZ(6))')
    for i, v in enumerate(VALUES, 1):
        pg_sql(f'INSERT INTO public."{table}" VALUES (%s, %s)', (i, v))
    return table


def test_live_product_session_reads_back_utc_while_the_default_is_not(non_utc_session):
    from services.oracle_session_scope import probe_oracle_utc_pin

    assert _bare_zone() == FORCED_ZONE  # the environment really is non-UTC
    assert probe_oracle_utc_pin(_cfg()) == (True, "session time zone read back as +00:00")


@pytest.mark.parametrize("existing", [
    "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6) WITH LOCAL TIME ZONE", None,
])
def test_live_pg_instants_land_unchanged_in_oracle_zone_columns(isolated, non_utc_session, existing):  # noqa: F811
    src = _pg_source()
    dest = src.upper()  # Oracle's own (unquoted) spelling
    try:
        if existing:
            sqlalchemy_sql(_cfg(), f'CREATE TABLE "{dest}" ("ID" NUMBER(10) PRIMARY KEY, "AT" {existing})')
        result = run_transfer(isolated, pg_endpoint(src), _endpoint(dest))
        assert result.success, result.error
        assert _stored_utc(dest) == EXPECTED
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{src}"')
        _drop_oracle(dest)


def test_live_oracle_to_oracle_row_writer_keeps_every_instant(isolated, non_utc_session, monkeypatch):  # noqa: F811
    monkeypatch.setenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", "0")  # the row writer, as across hosts
    src, dest = f"ORA_PIN_S_{token()}".upper(), f"ORA_PIN_D_{token()}".upper()
    cfg = _cfg()
    try:
        sqlalchemy_sql(cfg, f'CREATE TABLE "{src}" ("ID" NUMBER(10) PRIMARY KEY, "AT" TIMESTAMP(6) WITH TIME ZONE)')
        for i, v in enumerate(VALUES, 1):
            sqlalchemy_sql(cfg, f"INSERT INTO \"{src}\" VALUES ({i}, TIMESTAMP '{_literal(v)}')")
        result = run_transfer(isolated, _endpoint(src), _endpoint(dest))
        assert result.success, result.error
        assert _stored_utc(dest) == EXPECTED
    finally:
        _drop_oracle(src)
        _drop_oracle(dest)
