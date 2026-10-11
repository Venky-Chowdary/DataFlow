"""Live Oracle LogMiner LOB capture into PostgreSQL."""

from __future__ import annotations

import hashlib
import os
import subprocess
import uuid
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_ORACLE_ENABLE", "1")
os.environ.setdefault("DATAFLOW_ORACLE_SERVICE", "FREEPDB1")
os.environ.setdefault("DATAFLOW_ORACLE_CDB_SERVICE", "FREE")

from services.brand_env import getenv_brand
from test_cdc_oracle_logminer_transfer_e2e import (
    _oracle_cfg,
    _oracle_logminer_ready,
)
from test_cdc_oracle_logminer_txn_live import _pg_cfg


pytestmark = pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason=(
        "Oracle LogMiner endpoint/authentication unavailable — configure "
        "DATAFLOW_ORACLE_* for a CDC-ready :1521"
    ),
)


def _pg_runtime_cfg() -> dict[str, Any]:
    cfg = _pg_cfg()
    try:
        values = subprocess.check_output(
            [
                "docker",
                "exec",
                "df-pg",
                "sh",
                "-c",
                "printf '%s\\n' \"${POSTGRES_DB:-}\" \"${POSTGRES_USER:-}\" "
                "\"${POSTGRES_PASSWORD:-}\"",
            ],
            text=True,
        ).splitlines()
    except (OSError, subprocess.CalledProcessError):
        values = []
    values.extend([""] * (3 - len(values)))
    database, username, password = values[:3]
    cfg.update(
        {
            "host": getenv_brand("PG_HOST", "localhost").strip(),
            "port": int(getenv_brand("PG_PORT", "5432") or 5432),
            "database": getenv_brand("PG_DB", database or str(cfg["database"]))
            or cfg["database"],
            "username": getenv_brand("PG_USER", username or str(cfg["username"]))
            or cfg["username"],
            "password": getenv_brand(
                "PG_PASSWORD", password or str(cfg["password"])
            )
            or cfg["password"],
        }
    )
    return cfg


def _read_lob(value: Any) -> Any:
    return value.read() if hasattr(value, "read") and callable(value.read) else value


def _signature(value: Any) -> tuple[int | None, str | None]:
    value = _read_lob(value)
    if value is None:
        return None, None
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, bytes):
        return len(value), hashlib.md5(value).hexdigest()
    text = str(value)
    return len(text), hashlib.md5(text.encode("utf-8")).hexdigest()


def _source_rows(cfg: dict[str, Any], table: str) -> dict[str, dict[str, Any]]:
    from connectors.generic_sql import get_connection

    with get_connection(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        connection_string=cfg.get("connection_string") or "",
        ssl=bool(cfg.get("ssl")),
        db_type="oracle",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT ID, C, B, V FROM "{cfg["schema"]}"."{table}" ORDER BY ID')
            rows = cur.fetchall() or []
    return {
        str(row[0]): {
            "C": _read_lob(row[1]),
            "B": bytes(_read_lob(row[2])) if row[2] is not None else None,
            "V": row[3],
        }
        for row in rows
    }


def _destination_rows(
    cfg: dict[str, Any], table: str
) -> dict[str, dict[str, Any]]:
    from connectors.postgresql_conn import get_connection

    with get_connection(**cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id, c, b, v FROM "public"."{table}" ORDER BY id'
            )
            rows = cur.fetchall() or []
    return {
        str(row[0]): {
            "C": row[1],
            "B": bytes(row[2]) if row[2] is not None else None,
            "V": row[3],
        }
        for row in rows
    }


def _assert_source_matches_destination(
    oracle: dict[str, Any], postgres: dict[str, Any], source_table: str, dest_table: str
) -> None:
    source = _source_rows(oracle, source_table)
    destination = _destination_rows(postgres, dest_table)
    assert set(destination) == set(source)
    for key in source:
        for column in ("C", "B", "V"):
            assert _signature(destination[key][column]) == _signature(
                source[key][column]
            ), (key, column, _signature(source[key][column]), _signature(destination[key][column]))


def _write_step(conn: Any, schema: str, table: str, step: int) -> int | None:
    ref = f'"{schema}"."{table}"'
    statement_bytes = None
    with conn.cursor() as cur:
        if step == 1:
            cur.execute(
                f"INSERT INTO {ref} (ID, C, B, V) VALUES (1, TO_CLOB(:c), NULL, :v)",
                {"c": "a" * 100, "v": "step-1"},
            )
        elif step == 2:
            cur.execute(
                f"""
                INSERT INTO {ref} (ID, C, B, V)
                VALUES (
                    2,
                    TO_CLOB(RPAD('x', 4000, 'x'))
                    || TO_CLOB(RPAD('x', 4000, 'x'))
                    || TO_CLOB(RPAD('x', 4000, 'x'))
                    || TO_CLOB(RPAD('x', 4000, 'x'))
                    || TO_CLOB(RPAD('x', 4000, 'x')),
                    EMPTY_BLOB(),
                    'step-2'
                )
                """
            )
            cur.execute(
                f"""
                DECLARE
                    l_blob BLOB;
                    l_buffer RAW(32767);
                BEGIN
                    SELECT B INTO l_blob FROM {ref} WHERE ID = 2 FOR UPDATE;
                    l_buffer := UTL_RAW.CAST_TO_RAW(RPAD('b', 10000, 'b'));
                    DBMS_LOB.WRITEAPPEND(l_blob, 10000, l_buffer);
                END;
                """
            )
        elif step == 3:
            cur.execute(
                f"""
                UPDATE {ref}
                SET C =
                    TO_CLOB(RPAD('y', 4000, 'y'))
                    || TO_CLOB(RPAD('y', 4000, 'y'))
                    || TO_CLOB(RPAD('y', 4000, 'y'))
                    || TO_CLOB(RPAD('y', 4000, 'y'))
                    || TO_CLOB(RPAD('y', 4000, 'y'))
                WHERE ID = 1
                """
            )
        elif step == 4:
            cur.execute(f"UPDATE {ref} SET V = 'step-4' WHERE ID = 2")
        elif step == 5:
            cur.execute(
                f"""
                DECLARE
                    l_clob CLOB;
                BEGIN
                    SELECT C INTO l_clob FROM {ref} WHERE ID = 2 FOR UPDATE;
                    DBMS_LOB.WRITEAPPEND(l_clob, 100, RPAD('z', 100, 'z'));
                END;
                """
            )
        elif step == 6:
            long_value = "v" * 4000
            statement = (
                f"INSERT INTO {ref} (ID, C, B, V) "
                f"VALUES (3, NULL, NULL, '{long_value}')"
            )
            statement_bytes = len(statement.encode("utf-8"))
            assert statement_bytes > 4000
            cur.execute(statement)
        elif step == 7:
            cur.execute(f"DELETE FROM {ref} WHERE ID = 2")
        else:
            raise ValueError(f"unknown Oracle fixture step {step}")
    return statement_bytes


def _exercise_oracle_lob_capture(
    monkeypatch: pytest.MonkeyPatch,
    txn_buffer: bool,
) -> None:
    from connectors.generic_sql import get_connection as oracle_connection
    from connectors.postgresql_conn import get_connection as pg_connection
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    oracle = _oracle_cfg()
    postgres = _pg_runtime_cfg()
    try:
        with pg_connection(**postgres):
            pass
    except Exception as exc:
        pytest.skip(
            "PostgreSQL endpoint/authentication unavailable "
            f"({type(exc).__name__}); verify PG_* settings and df-pg credentials"
        )

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("DATAFLOW_ORACLE_CDB_SERVICE", "FREE")
    monkeypatch.setenv("DATAFLOW_CDC_ORACLE_TXN_BUFFER", "1" if txn_buffer else "0")
    schema = str(oracle["schema"])
    source_table = "M4_LOB_" + uuid.uuid4().hex[:8].upper()
    destination_table = source_table.lower() + "_dest"
    job_id = "m4-oracle-lob-" + uuid.uuid4().hex[:10]
    source_ref = f'"{schema}"."{source_table}"'

    source = EndpointConfig(
        kind="database",
        format="oracle",
        table=source_table,
        schema=schema,
        extra={
            "cdb_service": getenv_brand("ORACLE_CDB_SERVICE", "FREE") or "FREE",
            "logminer_username": getenv_brand("ORACLE_LOGMINER_USER", "C##DATAFLOW")
            or "C##DATAFLOW",
        },
        **{
            key: oracle[key]
            for key in (
                "host",
                "port",
                "database",
                "username",
                "password",
                "connection_string",
                "ssl",
            )
        },
    )
    destination = EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=destination_table,
        **{
            key: postgres[key]
            for key in (
                "host",
                "port",
                "database",
                "username",
                "password",
                "connection_string",
                "ssl",
            )
        },
    )
    mappings = [
        {"source": "ID", "target": "id"},
        {"source": "C", "target": "c"},
        {"source": "B", "target": "b"},
        {"source": "V", "target": "v"},
    ]
    column_types = {"ID": "NUMERIC", "C": "TEXT", "B": "BYTEA", "V": "TEXT"}
    stream = [
        {
            "name": source_table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "ID",
            "sync_mode": "cdc",
        }
    ]

    def run_transfer() -> None:
        run_cdc_database_transfer(
            source,
            destination,
            mappings,
            column_types,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=100,
            delivery_guarantee="exactly_once",
            delivery_pinned=True,
        )

    try:
        with oracle_connection(
            host=oracle["host"],
            port=oracle["port"],
            database=oracle["database"],
            username=oracle["username"],
            password=oracle["password"],
            connection_string=oracle.get("connection_string") or "",
            ssl=bool(oracle.get("ssl")),
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {source_ref} "
                    "(ID NUMBER PRIMARY KEY, C CLOB, B BLOB, V VARCHAR2(4000))"
                )
                cur.execute(
                    f"ALTER TABLE {source_ref} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                )
            conn.commit()

        run_transfer()
        _assert_source_matches_destination(
            oracle, postgres, source_table, destination_table
        )

        with oracle_connection(
            host=oracle["host"],
            port=oracle["port"],
            database=oracle["database"],
            username=oracle["username"],
            password=oracle["password"],
            connection_string=oracle.get("connection_string") or "",
            ssl=bool(oracle.get("ssl")),
            db_type="oracle",
        ) as conn:
            for step in range(1, 8):
                statement_bytes = _write_step(conn, schema, source_table, step)
                conn.commit()
                if step == 6:
                    assert statement_bytes and statement_bytes > 4000
                run_transfer()
                _assert_source_matches_destination(
                    oracle, postgres, source_table, destination_table
                )
                if step == 3:
                    run_transfer()
                    _assert_source_matches_destination(
                        oracle, postgres, source_table, destination_table
                    )
    finally:
        try:
            with pg_connection(**postgres) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'DROP TABLE IF EXISTS "public"."{destination_table}"'
                    )
                conn.commit()
        except Exception:
            pass
        try:
            with oracle_connection(
                host=oracle["host"],
                port=oracle["port"],
                database=oracle["database"],
                username=oracle["username"],
                password=oracle["password"],
                connection_string=oracle.get("connection_string") or "",
                ssl=bool(oracle.get("ssl")),
                db_type="oracle",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE {source_ref} PURGE")
                conn.commit()
        except Exception:
            pass


@pytest.mark.parametrize("txn_buffer", [False, True], ids=["buffer-off", "buffer-on"])
def test_oracle_lob_capture_buffer_modes(
    monkeypatch: pytest.MonkeyPatch, txn_buffer: bool
) -> None:
    _exercise_oracle_lob_capture(monkeypatch, txn_buffer)
