"""Live Oracle LogMiner → sqlite CDC transfer when :1521 is up.

Requires DATAFLOW_ORACLE_ENABLE=1 plus a LogMiner-ready database.
Delivery remains at-least-once upsert. Not leftover MERGE.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest
from services.brand_env import getenv_brand

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _oracle_cfg() -> dict:
    host = getenv_brand("ORACLE_HOST", "localhost").strip()
    port = int(getenv_brand("ORACLE_PORT", "1521") or 1521)
    service = (
        getenv_brand("ORACLE_SERVICE")
        or getenv_brand("ORACLE_DATABASE")
        or "XEPDB1"
    ).strip()
    user = getenv_brand("ORACLE_USER", "dataflow").strip()
    password = getenv_brand("ORACLE_PASSWORD", "Datawrap_CDC_2022!").strip()
    schema = (getenv_brand("ORACLE_SCHEMA") or user).strip().upper()
    return {
        "host": host,
        "port": port,
        "database": service,
        "service_name": service,
        "username": user,
        "password": password,
        "schema": schema,
        "connection_string": "",
        "ssl": False,
    }


def _oracle_enabled() -> bool:
    return getenv_brand("ORACLE_ENABLE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _oracle_logminer_ready() -> bool:
    if not _oracle_enabled():
        return False
    cfg = _oracle_cfg()
    if not cfg.get("username") or not cfg.get("password"):
        return False
    try:
        import oracledb  # noqa: F401
    except ImportError:
        return False
    try:
        with socket.create_connection((cfg["host"], int(cfg["port"])), timeout=2):
            pass
    except OSError:
        return False
    try:
        from connectors.oracle_logminer import OracleLogMinerCdc

        cdc = OracleLogMinerCdc(
            cfg, table="DUAL", primary_key="DUMMY", schema=cfg["schema"]
        )
        return bool(cdc.is_available())
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _oracle_logminer_ready(),
    reason="Oracle LogMiner not reachable — set DATAFLOW_ORACLE_ENABLE=1 on a CDC-ready :1521",
)


def _dest_rows(path: Path, table: str) -> list[tuple]:
    con = sqlite3.connect(str(path))
    try:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
        if "_df_lsn" in cols:
            return list(
                con.execute(f'SELECT id, amount, "_df_lsn" FROM "{table}" ORDER BY id')
            )
        return list(con.execute(f'SELECT id, amount FROM "{table}" ORDER BY id'))
    finally:
        con.close()


def test_oracle_logminer_transfer_snapshot_resume_delete(tmp_path: Path) -> None:
    from connectors.generic_sql import get_connection
    from connectors.oracle_logminer import OracleLogMinerCdc
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    cfg = _oracle_cfg()
    schema = cfg["schema"]
    table = "CDC_XF_" + uuid.uuid4().hex[:8].upper()
    dest_path = tmp_path / "oracle_logminer_dest.db"
    job_id = "oracle-logminer-e2e-" + uuid.uuid4().hex[:8]

    with get_connection(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        connection_string="",
        ssl=False,
        db_type="oracle",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{schema}"."{table}" '
                f"(ID NUMBER PRIMARY KEY, AMOUNT NUMBER(10,2))"
            )
            cur.execute(
                f'INSERT INTO "{schema}"."{table}" (ID, AMOUNT) VALUES (1, 10)'
            )
            cur.execute(
                f'INSERT INTO "{schema}"."{table}" (ID, AMOUNT) VALUES (2, 20)'
            )
            cur.execute(
                f'ALTER TABLE "{schema}"."{table}" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS'
            )
        conn.commit()

    src = EndpointConfig(
        kind="database",
        format="oracle",
        table=table,
        schema=schema,
        extra={
            "cdb_service": getenv_brand("ORACLE_CDB_SERVICE", "XE") or "XE",
            "logminer_username": getenv_brand("ORACLE_LOGMINER_USER", "C##DATAFLOW")
            or "C##DATAFLOW",
        },
        **{k: cfg[k] for k in ("host", "port", "database", "username", "password", "connection_string", "ssl")},
    )
    dst = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table=table,
    )
    mappings = [
        {"source": "ID", "target": "id"},
        {"source": "AMOUNT", "target": "amount"},
    ]
    schema_types = {"ID": "NUMBER", "AMOUNT": "NUMBER(10,2)"}
    stream = [
        {
            "name": table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "ID",
            "sync_mode": "cdc",
        }
    ]

    try:
        probe = OracleLogMinerCdc(
            cfg, table=table, primary_key="ID", schema=schema, cursor_key=f"e2e:{table}"
        )
        assert probe.is_available(), "expected LogMiner, not flashback/query fallback"
        probe.close()

        rows1, ddl1, summary1, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema_types,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=2,
        )
        assert any("CDC(logminer)" in line for line in ddl1), ddl1
        assert not any("downgraded" in line.lower() for line in ddl1), ddl1
        assert rows1 == 2, (rows1, summary1)

        with get_connection(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg["username"],
            password=cfg["password"],
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'INSERT INTO "{schema}"."{table}" (ID, AMOUNT) VALUES (3, 30)'
                )
                cur.execute(
                    f'UPDATE "{schema}"."{table}" SET AMOUNT = 99 WHERE ID = 1'
                )
                cur.execute(f'DELETE FROM "{schema}"."{table}" WHERE ID = 2')
            conn.commit()

        rows2, ddl2, summary2, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema_types,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
        )
        assert any("CDC(logminer)" in line for line in ddl2), ddl2
        dest2 = _dest_rows(dest_path, table)
        ids = [int(r[0]) for r in dest2]
        amounts = {int(r[0]): float(r[1]) for r in dest2}
        assert 2 not in ids, dest2
        assert 1 in ids and 3 in ids, dest2
        assert amounts[1] == 99.0, dest2
        assert amounts[3] == 30.0, dest2
        artifact = {
            "test": "test_oracle_logminer_transfer_snapshot_resume_delete",
            "delivery": "at-least-once upsert",
            "capture": "CDC(logminer)",
            "snapshot_rows": rows1,
            "resume_rows": rows2,
            "dest_ids": ids,
            "watermark": (summary2.get("cdc") or {}).get("watermark"),
            "note": "Not leftover MERGE. Not dest-owned exactly-once.",
        }
        out_dir = Path(
            os.environ.get("DATAFLOW_PROOF_DIR", "/opt/cursor/artifacts/cdc-merge-live")
        )
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "oracle-logminer-transfer.json").write_text(
                json.dumps(artifact, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError:
            pass
    finally:
        try:
            with get_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE "{schema}"."{table}"')
                conn.commit()
        except Exception:
            pass
