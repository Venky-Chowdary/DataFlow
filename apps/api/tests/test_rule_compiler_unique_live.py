"""Live post-load unique-rule checks against real database engines.

Skipped unless ``DATAFLOW_BR_LIVE=1`` and the per-engine host/port env vars
point at running engines (``DATAFLOW_BR_{PG,MYSQL,MSSQL}_{HOST,PORT}``).
Tables and low-privilege logins are created by the test with each engine's own
driver — the transfer code path never writes the fixture.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATAFLOW_BR_LIVE") != "1",
    reason="DATAFLOW_BR_LIVE=1 plus DATAFLOW_BR_{PG,MYSQL,MSSQL}_{HOST,PORT} not set",
)

TABLE = "customers_br"
NOPRIV = "br_nopriv"
NOPRIV_PW = "Br_NoPriv_2026!"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, "") or default


DESTS: dict[str, dict[str, Any]] = {
    "postgresql": {
        "endpoint": {
            "format": "postgresql",
            "host": _env("DATAFLOW_BR_PG_HOST", "localhost"),
            "port": int(_env("DATAFLOW_BR_PG_PORT", "55432")),
            "database": _env("DATAFLOW_BR_PG_DB", "dataflow"),
            "username": _env("DATAFLOW_BR_PG_USER", "dataflow"),
            "password": _env("DATAFLOW_BR_PG_PASSWORD", "dataflow"),
            "schema": "public",
            "table": TABLE,
        },
    },
    "mysql": {
        "endpoint": {
            "format": "mysql",
            "host": _env("DATAFLOW_BR_MYSQL_HOST", "localhost"),
            "port": int(_env("DATAFLOW_BR_MYSQL_PORT", "33306")),
            "database": _env("DATAFLOW_BR_MYSQL_DB", "dataflow"),
            "username": _env("DATAFLOW_BR_MYSQL_USER", "dataflow"),
            "password": _env("DATAFLOW_BR_MYSQL_PASSWORD", "dataflow"),
            "table": TABLE,
        },
    },
    "sqlserver": {
        "endpoint": {
            "format": "sqlserver",
            "host": _env("DATAFLOW_BR_MSSQL_HOST", "localhost"),
            "port": int(_env("DATAFLOW_BR_MSSQL_PORT", "21433")),
            "database": _env("DATAFLOW_BR_MSSQL_DB", "master"),
            "username": _env("DATAFLOW_BR_MSSQL_USER", "sa"),
            "password": _env("DATAFLOW_BR_MSSQL_PASSWORD", os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")),
            "table": TABLE,
        },
    },
}

ROWS_CLEAN = [(1, "a@x.com"), (2, "b@x.com"), (3, "c@x.com")]
ROWS_DUP = [(1, "a@x.com"), (2, "a@x.com"), (3, "c@x.com")]


def _connect(engine: str, *, user: str | None = None, password: str | None = None) -> Any:
    cfg = DESTS[engine]["endpoint"]
    user = user if user is not None else cfg["username"]
    password = password if password is not None else cfg["password"]
    if engine == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=user,
            password=password,
        )
        conn.autocommit = True
        return conn
    if engine == "mysql":
        import pymysql

        return pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=user,
            password=password,
            autocommit=True,
        )
    import pymssql

    return pymssql.connect(
        host=cfg["host"],
        port=str(cfg["port"]),
        database=cfg["database"],
        user=user,
        password=password,
    )


def _reset_table(
    engine: str, rows: list[tuple[int, str]], *, drop_email: bool = False
) -> None:
    coltype = {"postgresql": "TEXT", "mysql": "VARCHAR(255)", "sqlserver": "NVARCHAR(255)"}[engine]
    # MySQL seeds as root — the dataflow account has no CREATE USER privilege.
    conn = _connect(
        engine,
        user=("root" if engine == "mysql" else None),
        password=(_env("DATAFLOW_BR_MYSQL_ROOT_PASSWORD", "rootpw") if engine == "mysql" else None),
    )
    cur = conn.cursor()
    drop = {
        "postgresql": f"DROP TABLE IF EXISTS {TABLE}",
        "mysql": f"DROP TABLE IF EXISTS {TABLE}",
        "sqlserver": f"IF OBJECT_ID('{TABLE}','U') IS NOT NULL DROP TABLE {TABLE}",
    }[engine]
    cur.execute(drop)
    # For the count-error case the email column is absent: the dest COUNT(*)
    # read-back still works while the unique DISTINCT count hits a real
    # engine error (column does not exist). A permission-isolated login
    # cannot narrow to the column on MSSQL — a single column DENY blocks
    # COUNT(*) too, so every engine uses the drop-column fallback.
    ddl = f"CREATE TABLE {TABLE} (id INT" + (
        ")" if drop_email else f", email {coltype})"
    )
    cur.execute(ddl)
    for row in rows:
        if drop_email:
            cur.execute(f"INSERT INTO {TABLE} (id) VALUES (%s)", (row[0],))
        else:
            cur.execute(f"INSERT INTO {TABLE} (id, email) VALUES (%s, %s)", row)
    conn.commit()
    cur.close()
    conn.close()


def _endpoint(engine: str, *, user: str | None = None, password: str | None = None):
    from src.transfer.models import EndpointConfig

    cfg = dict(DESTS[engine]["endpoint"])
    if user is not None:
        cfg["username"] = user
        cfg["password"] = password or ""
    return EndpointConfig(kind="database", **cfg)


def _recon(engine: str, *, validation_mode: str, user=None, password=None):
    from src.transfer.reconcile_step import run_reconciliation

    return run_reconciliation(
        endpoint=_endpoint(engine, user=user, password=password),
        records=[],
        columns=[],
        rows_written=0,
        writer_checksum="",
        dest_summary={
            "source_row_count": 0,
            "sync_mode": "full_refresh_append",
            "target_rows_before": 3,
        },
        mappings=[
            {
                "source": "email",
                "target": "email",
                "rule_contracts": [{"rule_id": "uniq-email", "type": "unique"}],
            }
        ],
        validation_mode=validation_mode,
    )


@pytest.mark.parametrize("engine", sorted(DESTS))
def test_live_unique_clean_population(engine: str):
    _reset_table(engine, ROWS_CLEAN)
    recon = _recon(engine, validation_mode="balanced")
    checks = [c for c in recon.get("rule_checks") or []]
    assert checks and all(c.get("status") == "passed" for c in checks), recon
    assert not [c for c in checks if c.get("status") == "failed"], recon
    assert recon.get("unproven") is not True, recon


@pytest.mark.parametrize("engine", sorted(DESTS))
def test_live_unique_duplicate_fails(engine: str):
    _reset_table(engine, ROWS_DUP)
    recon = _recon(engine, validation_mode="balanced")
    assert recon.get("passed") is False, recon
    failed = [c for c in recon.get("rule_checks") or [] if c.get("status") == "failed"]
    assert [c["rule_id"] for c in failed] == ["uniq-email"], recon
    assert failed[0]["duplicates"] == 1


@pytest.mark.parametrize("engine", sorted(DESTS))
def test_live_unique_count_error_strict_fails(engine: str):
    """Dropping the email column forces a real count_error while the dest
    read-back COUNT(*) still succeeds (login-based isolation cannot narrow
    to one column on MSSQL)."""
    _reset_table(engine, ROWS_CLEAN, drop_email=True)
    recon = _recon(engine, validation_mode="strict")
    assert recon.get("passed") is False, recon
    assert (recon.get("details") or {}).get("reason") == "business_rule_unverified"
    unverified = recon.get("unverified_rules") or []
    assert [u["rule_id"] for u in unverified] == ["uniq-email"], recon
    assert unverified[0]["reason"] == "count_error"


@pytest.mark.parametrize("engine", sorted(DESTS))
def test_live_unique_count_error_balanced_unproven(engine: str, caplog):
    _reset_table(engine, ROWS_CLEAN, drop_email=True)
    with caplog.at_level(logging.WARNING, logger="src.transfer.reconcile_step"):
        recon = _recon(engine, validation_mode="balanced")
    if engine == "mysql":
        # MySQL's read-back SELECTs the mapped column list, so dropping email
        # breaks the read-back too — the recon is unproven writer-ack and
        # cannot pass. PG/MSSQL read back fine and keep passed=True.
        assert recon.get("passed") is False, recon
    else:
        assert recon.get("passed") is True, recon
    assert recon.get("unproven") is True, recon
    assert [u["rule_id"] for u in recon.get("unverified_rules") or []] == [
        "uniq-email"
    ]
    log = "\n".join(
        r.getMessage() for r in caplog.records if "unique rule check" in r.getMessage()
    )
    assert "error_type=" in log, caplog.records
    for value in ("a@x.com", "b@x.com", "c@x.com", NOPRIV_PW):
        assert value not in log, caplog.records
