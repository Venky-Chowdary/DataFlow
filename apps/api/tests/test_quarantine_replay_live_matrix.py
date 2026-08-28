"""Enterprise matrix: quarantine replay on live PostgreSQL and MySQL.

The SQLite golden path already proves edit → Promote → dest DLQ close.
This fixture answers the operator question the SQLite test cannot:
does the same ledger close when the destination is a real server?

``100%`` means every row on this named fixture — not every engine on earth.
CDC default remains at-least-once upsert. Closed ≠ migration_proven.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pytest
from fastapi.testclient import TestClient

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

_CSV = b"id,age\n1,30\n2,not-a-number\n3,40\n4,also-bad\n5,50\n"
_ARTIFACT = Path("/opt/cursor/artifacts/quarantine_replay_live_results.json")


def _sid(prefix: str = "df_qr") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _pg_cfg() -> dict[str, Any] | None:
    from tests.helpers.live_env import pg_creds, pg_up

    if pg_up():
        return pg_creds()
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return {
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
        }
    except Exception:
        return None


def _mysql_cfg() -> dict[str, Any] | None:
    from tests.helpers.live_env import mysql_creds, mysql_up

    if mysql_up():
        return mysql_creds()
    try:
        import pymysql

        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
        }
    except Exception:
        return None


def _pg_exec(cfg: dict[str, Any], statements: list[str]) -> list[Any]:
    import psycopg2

    conn = psycopg2.connect(
        host=cfg["host"],
        port=int(cfg.get("port") or 5432),
        dbname=cfg["database"],
        user=cfg["username"],
        password=cfg["password"],
        connect_timeout=5,
    )
    try:
        conn.autocommit = True
        out: list[Any] = []
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
                try:
                    out.append(cur.fetchall())
                except Exception:
                    out.append(None)
        return out
    finally:
        conn.close()


def _mysql_exec(cfg: dict[str, Any], statements: list[str]) -> list[Any]:
    import pymysql

    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg.get("port") or 3306),
        user=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        autocommit=True,
        connect_timeout=5,
    )
    try:
        out: list[Any] = []
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
                try:
                    out.append(cur.fetchall())
                except Exception:
                    out.append(None)
        return out
    finally:
        conn.close()


def _live_engines() -> list[tuple[str, dict[str, Any], Callable, Callable]]:
    out: list[tuple[str, dict[str, Any], Callable, Callable]] = []
    pg = _pg_cfg()
    if pg:
        out.append(("postgresql", pg, _pg_exec, lambda ident: f'"{ident}"'))
    mysql = _mysql_cfg()
    if mysql:
        out.append(("mysql", mysql, _mysql_exec, lambda ident: f"`{ident}`"))
    return out


def _client() -> TestClient:
    from src.main import app

    return TestClient(app)


def _dest(dialect: str, cfg: dict[str, Any], table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format=dialect,
        host=str(cfg.get("host") or "127.0.0.1"),
        port=int(cfg.get("port") or 0),
        database=str(cfg.get("database") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        schema=str(cfg.get("schema") or ""),
        table=table,
    )


def _edit_age(details: list[dict[str, Any]], bad: str, good: str) -> list[dict[str, Any]]:
    edited: list[dict[str, Any]] = []
    for detail in details:
        d = dict(detail)
        if str(d.get("value")) == bad:
            d["value"] = good
            values = dict(d.get("values") or {})
            values["age"] = good
            d["values"] = values
        edited.append(d)
    return edited


def test_live_engines_close_quarantine_ledger_after_replay():
    engines = _live_engines()
    if not engines:
        pytest.skip(
            "Neither Postgres nor MySQL authenticated — "
            "quarantine replay live matrix skipped"
        )

    client = _client()
    results: list[dict[str, Any]] = []
    for dialect, cfg, exec_sql, q in engines:
        table = _sid("t")
        dlq = f"{table}_df_quarantine"
        drop = [
            f"DROP TABLE IF EXISTS {q(dlq)}",
            f"DROP TABLE IF EXISTS {q(table)}",
        ]
        row: dict[str, Any] = {"dialect": dialect, "table": table, "status": "fail"}
        try:
            exec_sql(cfg, drop)
            request = TransferRequest(
                source=EndpointConfig(kind="file", format="csv"),
                destination=_dest(dialect, cfg, table),
                source_filename="ages.csv",
                source_content=_CSV,
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
                validation_mode="balanced",
                stream_contracts=[{"primary_key": "id"}],
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.95},
                    {
                        "source": "age",
                        "target": "age",
                        "confidence": 0.95,
                        "target_type": "integer",
                    },
                ],
                column_types={"id": "string", "age": "string"},
            )
            result = UniversalTransferEngine().execute(request)
            job_id = result.job_id
            rejected = int(result.destination_summary.get("rejected_rows") or 0)
            written = int(
                result.destination_summary.get("rows_written")
                or result.records_processed
                or 0
            )
            assert result.success is True, result.destination_summary
            assert rejected >= 2, result.destination_summary

            dest_n = exec_sql(cfg, [f"SELECT COUNT(*) FROM {q(table)}"])[0][0][0]
            assert int(dest_n) == 3, dest_n

            q1 = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
            assert q1.status_code == 200, q1.text
            body1 = q1.json()
            quarantine = list(body1.get("quarantine") or [])
            closure1 = body1.get("quarantine_closure") or {}
            dest_dlq = body1.get("dest_dlq") or {}
            assert quarantine
            assert closure1.get("verdict") == "open", closure1
            assert int(closure1.get("open_count") or 0) >= 2
            assert closure1.get("migration_proven") is False
            assert int(dest_dlq.get("open_rows") or 0) >= 2

            first = _edit_age(quarantine, "not-a-number", "25")
            only_fixed = [
                d
                for d in first
                if str((d.get("values") or {}).get("age") or d.get("value")) == "25"
            ]
            replay1 = client.post(
                f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
                json={"rows": only_fixed},
            )
            assert replay1.status_code == 200, replay1.text
            r1 = replay1.json()
            assert r1["success"] is True
            assert r1["parent_job_id"] == job_id
            assert int(r1.get("rows_written") or 0) >= 1
            assert int(r1.get("rejected") or 0) == 0

            age2 = exec_sql(
                cfg, [f"SELECT age FROM {q(table)} WHERE id = 2"]
            )[0][0][0]
            assert int(age2) == 25, age2
            dest_after_one = exec_sql(cfg, [f"SELECT COUNT(*) FROM {q(table)}"])[0][0][0]
            assert int(dest_after_one) == 4, dest_after_one

            q2 = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
            assert q2.status_code == 200, q2.text
            closure2 = (q2.json().get("quarantine_closure") or {})
            assert closure2.get("verdict") == "in_progress", closure2
            assert int(closure2.get("open_count") or 0) >= 1
            assert closure2.get("migration_proven") is False

            remaining = _edit_age(
                list(q2.json().get("quarantine") or quarantine),
                "also-bad",
                "41",
            )
            only_second = [
                d
                for d in remaining
                if str((d.get("values") or {}).get("age") or d.get("value")) == "41"
            ]
            replay2 = client.post(
                f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
                json={"rows": only_second},
            )
            assert replay2.status_code == 200, replay2.text
            r2 = replay2.json()
            assert r2["success"] is True
            assert int(r2.get("rejected") or 0) == 0

            age4 = exec_sql(
                cfg, [f"SELECT age FROM {q(table)} WHERE id = 4"]
            )[0][0][0]
            assert int(age4) == 41, age4
            dest_final = exec_sql(cfg, [f"SELECT COUNT(*) FROM {q(table)}"])[0][0][0]
            assert int(dest_final) == 5, dest_final

            q3 = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
            assert q3.status_code == 200, q3.text
            closure3 = q3.json().get("quarantine_closure") or {}
            dest_dlq3 = q3.json().get("dest_dlq") or {}
            assert closure3.get("verdict") == "closed", closure3
            assert int(closure3.get("open_count") or 0) == 0
            assert closure3.get("migration_proven") is False
            assert int(dest_dlq3.get("open_rows") or 0) == 0

            again = client.post(
                f"/api/v1/connectors/jobs/{job_id}/quarantine/replay",
                json={"rows": only_second},
            )
            assert again.status_code == 400, again.text
            assert "closed" in again.json()["detail"].lower()

            row.update(
                {
                    "status": "pass",
                    "job_id": job_id,
                    "parent_written": written,
                    "parent_rejected": rejected,
                    "dest_after_transfer": int(dest_n),
                    "dest_final": int(dest_final),
                    "replayed_age_id2": int(age2),
                    "replayed_age_id4": int(age4),
                    "closure": [
                        closure1.get("verdict"),
                        closure2.get("verdict"),
                        closure3.get("verdict"),
                    ],
                    "closed_not_migration_proven": True,
                }
            )
            results.append(row)
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
            results.append(row)
        finally:
            try:
                exec_sql(cfg, drop)
            except Exception:
                pass

    assert results, "live matrix produced no engine rows"
    failed = [r for r in results if r.get("status") != "pass"]
    assert not failed, failed
    if _ARTIFACT.parent.is_dir():
        _ARTIFACT.write_text(
            json.dumps(
                {
                    "fixture": "apps/api/tests/test_quarantine_replay_live_matrix.py",
                    "algorithm": "services.quarantine_dlq.evaluate_replay_closure",
                    "measured_at": datetime.now(timezone.utc).isoformat(),
                    "honesty": {
                        "closed_is_not_migration_proven": True,
                        "cdc_default": "at-least-once upsert",
                        "catalog_tiles_are_not_transfer_live": True,
                    },
                    "engines": results,
                    "pass": sum(1 for r in results if r.get("status") == "pass"),
                    "fail": sum(1 for r in results if r.get("status") != "pass"),
                    "skip": 0,
                },
                indent=2,
            )
            + "\n"
        )
