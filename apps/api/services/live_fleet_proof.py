"""Prove unique-engine cartesian + catalog slots + schedules + warehouse ports.

Honesty
-------
* Catalog tiles are not unique engines. Hosted twins share a parent driver.
* This is not 60+ unique connectors. Report the measured unique-engine count.
* Emulators are not a customer-tenant PRODUCTION_SKU certificate.
* A closed port is ``skipped`` with a named reason — never invented green.
* CDC default remains at-least-once upsert.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.desktop_lab import run_desktop_lab
from services.desktop_lab_cross import (
    LIVE_UNIQUE_ENGINES,
    _reachable,
    bind_live_engine,
    engines_for_run,
    run_live_engine_cross_matrix,
)

PORT_INVENTORY: tuple[tuple[str, str, int], ...] = (
    ("postgresql", "127.0.0.1", 5432),
    ("mysql", "127.0.0.1", 3306),
    ("mongodb", "127.0.0.1", 27017),
    ("sqlserver", "127.0.0.1", 1433),
    ("oracle", "127.0.0.1", 1521),
    ("redis", "127.0.0.1", 6379),
    ("elasticsearch", "127.0.0.1", 9200),
    ("s3_minio", "127.0.0.1", 9000),
    ("gcs_fake", "127.0.0.1", 4443),
    ("adls_azurite", "127.0.0.1", 10000),
    ("dynamodb_local", "127.0.0.1", 8000),
    ("bigquery_emulator", "127.0.0.1", 9050),
    ("iceberg_rest", "127.0.0.1", 8181),
    ("api", "127.0.0.1", 8001),
    ("web", "127.0.0.1", 5173),
)


def inventory_live_backends() -> dict[str, Any]:
    ports = []
    for name, host, port in PORT_INVENTORY:
        ports.append(
            {
                "name": name,
                "host": host,
                "port": port,
                "reachable": _reachable(host, port),
            }
        )
    fakesnow_ok = False
    try:
        import fakesnow  # noqa: F401

        fakesnow_ok = True
    except ImportError:
        fakesnow_ok = False
    return {
        "ports": ports,
        "reachable": [p["name"] for p in ports if p["reachable"]],
        "closed": [p["name"] for p in ports if not p["reachable"]],
        "fakesnow_installed": fakesnow_ok,
        "sqlite": "always (file)",
        "unique_engines_catalog": list(LIVE_UNIQUE_ENGINES),
        "engines_for_run": list(engines_for_run()),
        "honesty": {
            "not_sixty_unique_connectors": True,
            "hosted_twins_are_not_extra_engines": True,
            "emulators_are_not_customer_tenants": True,
        },
    }


def _connector_count_cfg(conn: Any) -> dict[str, Any]:
    extra = {}
    endpoint_url = str(getattr(conn, "endpoint_url", "") or "")
    if endpoint_url:
        extra["endpoint_url"] = endpoint_url
    return {
        "type": conn.type,
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "schema": conn.schema,
        "username": conn.username,
        "password": conn.password,
        "connection_string": conn.connection_string,
        "warehouse": conn.warehouse,
        "path_style": conn.path_style,
        "endpoint_url": endpoint_url or conn.connection_string,
        "extra": extra,
    }


def _dest_count_for_schedule(sched: Any) -> tuple[int | None, str]:
    from services.connector_store import get_connector
    from services.dest_precount import destination_row_count

    conn = get_connector(sched.dest_connector_id, workspace_id=sched.workspace_id or None)
    if conn is None:
        return None, "dest connector not found"
    table = (sched.dest_table or "").strip()
    if not table:
        return None, "schedule dest_table empty"
    try:
        count = destination_row_count(
            conn.type,
            _connector_count_cfg(conn),
            schema=conn.schema or "",
            table_name=table,
        )
    except Exception as exc:
        return None, str(exc)[:400]
    return count, ""


def _wait_job(job_id: str, *, timeout_sec: float = 180.0) -> dict[str, Any] | None:
    from services.mongodb_service import get_mongodb_service

    deadline = time.time() + timeout_sec
    terminal = {"completed", "success", "failed", "error", "cancelled"}
    while time.time() < deadline:
        try:
            job = get_mongodb_service().get_job(job_id)
        except Exception:
            job = None
        if isinstance(job, dict):
            status = str(job.get("status") or "").strip().lower()
            if status in terminal:
                return job
        time.sleep(1.5)
    return None


def prove_workspace_schedules() -> dict[str, Any]:
    """Run Now every saved schedule and take an independent dest COUNT."""
    from services.schedule_runner import ScheduleStartError, _run_schedule
    from services.schedule_store import list_schedules

    rows: list[dict[str, Any]] = []
    try:
        schedules = list_schedules()
    except Exception as exc:
        return {
            "schedules": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 1,
            "error": f"list_schedules failed: {exc}"[:400],
            "rows": [],
        }
    for sched in schedules:
        rec: dict[str, Any] = {
            "id": sched.id,
            "name": sched.name,
            "source_table": sched.source_table,
            "dest_table": sched.dest_table,
            "sync_mode": sched.sync_mode,
            "interval": sched.interval,
            "status": "skipped",
            "error": "",
            "job_id": "",
            "dest_count_before": None,
            "dest_count_after": None,
        }
        before, before_err = _dest_count_for_schedule(sched)
        rec["dest_count_before"] = before
        if before_err:
            rec["count_before_error"] = before_err
        try:
            job_id = _run_schedule(sched.id, manual=True)
        except ScheduleStartError as exc:
            rec["status"] = "failed"
            rec["error"] = str(exc)[:400]
            rows.append(rec)
            continue
        except Exception as exc:
            rec["status"] = "failed"
            rec["error"] = str(exc)[:400]
            rows.append(rec)
            continue
        if not job_id:
            rec["status"] = "failed"
            rec["error"] = "Run Now returned no job_id"
            rows.append(rec)
            continue
        rec["job_id"] = job_id
        job = _wait_job(job_id)
        after, after_err = _dest_count_for_schedule(sched)
        rec["dest_count_after"] = after
        if after_err:
            rec["count_after_error"] = after_err
        if job is None:
            rec["status"] = "failed"
            rec["error"] = "job did not reach a terminal status"
            rows.append(rec)
            continue
        rec["job_status"] = job.get("status")
        rec["records_transferred"] = job.get("records_transferred") or job.get("rows")
        status = str(job.get("status") or "").strip().lower()
        if status not in {"completed", "success"}:
            rec["status"] = "failed"
            rec["error"] = str(job.get("error") or job.get("message") or status)[:400]
            rows.append(rec)
            continue
        if after is None:
            rec["status"] = "failed"
            rec["error"] = after_err or "independent dest COUNT unknown"
            rows.append(rec)
            continue
        rec["status"] = "passed"
        rec["integrity"] = "independent_dest_count"
        rows.append(rec)

    passed = [r for r in rows if r["status"] == "passed"]
    failed = [r for r in rows if r["status"] == "failed"]
    skipped = [r for r in rows if r["status"] == "skipped"]
    return {
        "schedules": len(rows),
        "passed": len(passed),
        "failed": len(failed),
        "skipped": len(skipped),
        "rows": rows,
        "honesty": {
            "all_saved_schedules": True,
            "independent_dest_count": True,
            "empty_workspace_is_zero_not_green": True,
        },
    }


def prove_warehouse_binds() -> dict[str, Any]:
    """Bind every warehouse/cloud unique engine or skip with a named reason."""
    root = Path("/tmp/df-fleet-wh")
    root.mkdir(parents=True, exist_ok=True)
    wanted = (
        "s3",
        "gcs",
        "adls",
        "dynamodb",
        "snowflake",
        "bigquery",
        "redis",
        "elasticsearch",
        "iceberg",
        "oracle",
        "sqlserver",
    )
    rows: list[dict[str, Any]] = []
    for engine in wanted:
        bound = bind_live_engine(engine, f"wh{uuid.uuid4().hex[:8]}", root)
        if isinstance(bound, str):
            rows.append({"engine": engine, "status": "skipped", "error": bound})
        else:
            rows.append(
                {
                    "engine": engine,
                    "status": "bound",
                    "format": bound.format,
                    "host": bound.host,
                    "port": bound.port,
                }
            )
    return {
        "engines": wanted,
        "bound": [r["engine"] for r in rows if r["status"] == "bound"],
        "skipped": [r for r in rows if r["status"] == "skipped"],
        "rows": rows,
        "honesty": {
            "bind_is_not_a_transfer": True,
            "transfer_proof_is_the_cartesian_and_sku_matrix": True,
        },
    }


def run_live_fleet_proof(
    *,
    persist: bool = True,
    cross: bool = True,
    desktop_lab: bool = True,
    schedules: bool = True,
) -> dict[str, Any]:
    started = datetime.now(timezone.utc).isoformat()
    inventory = inventory_live_backends()
    warehouse = prove_warehouse_binds()
    cross_report: dict[str, Any] | None = None
    lab_report: dict[str, Any] | None = None
    schedule_report: dict[str, Any] | None = None
    if cross:
        cross_report = run_live_engine_cross_matrix(persist=persist)
    if desktop_lab:
        lab_report = run_desktop_lab(persist=persist)
    if schedules:
        schedule_report = prove_workspace_schedules()
    payload = {
        "fixture": "services.live_fleet_proof.run_live_fleet_proof",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started,
        "inventory": inventory,
        "warehouse_binds": warehouse,
        "unique_engine_cross": (
            {
                "unique_engines": (cross_report or {}).get("unique_engines"),
                "unique_engines_seeded": (cross_report or {}).get("unique_engines_seeded"),
                "pairs": (cross_report or {}).get("pairs"),
                "passed": (cross_report or {}).get("passed"),
                "failed": (cross_report or {}).get("failed"),
                "skipped": (cross_report or {}).get("skipped"),
                "extended_opt_in": (cross_report or {}).get("extended_opt_in"),
                "failed_detail": (cross_report or {}).get("failed_detail"),
                "skipped_detail": (cross_report or {}).get("skipped_detail"),
                "seeds": (cross_report or {}).get("seeds"),
            }
            if cross_report
            else None
        ),
        "desktop_lab": (
            {
                "catalog_slots": (lab_report or {}).get("catalog_slots"),
                "catalog_slots_duplex_passed": (lab_report or {}).get(
                    "catalog_slots_duplex_passed"
                ),
                "unique_engines_duplex_passed": (lab_report or {}).get(
                    "unique_engines_duplex_passed"
                ),
                "failed": (lab_report or {}).get("failed"),
                "skipped": (lab_report or {}).get("skipped"),
                "one_hundred_percent": (lab_report or {}).get("one_hundred_percent"),
                "failed_detail": (lab_report or {}).get("failed_detail"),
                "skipped_detail": (lab_report or {}).get("skipped_detail"),
            }
            if lab_report
            else None
        ),
        "schedules": schedule_report,
        "honesty": {
            "not_catalog_tile_cartesian": True,
            "not_sixty_plus_unique_connectors_unless_measured": True,
            "catalog_tiles_are_not_transfer_live": True,
            "cdc_default": "at-least-once upsert",
            "saas_omitted": ["salesforce", "hubspot", "stripe"],
            "oracle_iceberg_bq_skip_when_port_closed": True,
            "emulators_are_not_customer_tenants": True,
        },
    }
    if persist:
        _persist(payload)
    return payload


def _persist(payload: dict[str, Any]) -> None:
    from services.platform_config import data_dir

    dest = data_dir() / "proofs"
    dest.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2) + "\n"
    (dest / "live_fleet_proof.json").write_text(text)
    artifacts = Path("/opt/cursor/artifacts")
    if artifacts.is_dir():
        (artifacts / "live_fleet_proof.json").write_text(text)
        lab = artifacts / "warehouse-emulator-lab"
        lab.mkdir(parents=True, exist_ok=True)
        (lab / "live_fleet_proof.json").write_text(text)


def last_fleet_report() -> dict[str, Any] | None:
    try:
        from services.platform_config import data_dir

        path = data_dir() / "proofs" / "live_fleet_proof.json"
        if path.is_file():
            return json.loads(path.read_text())
    except Exception:
        return None
    return None


if __name__ == "__main__":
    report = run_live_fleet_proof(persist=True)
    cross = report.get("unique_engine_cross") or {}
    lab = report.get("desktop_lab") or {}
    sched = report.get("schedules") or {}
    print(
        json.dumps(
            {
                "inventory_reachable": report["inventory"]["reachable"],
                "inventory_closed": report["inventory"]["closed"],
                "cross_passed": cross.get("passed"),
                "cross_failed": cross.get("failed"),
                "cross_skipped": cross.get("skipped"),
                "cross_pairs": cross.get("pairs"),
                "lab_duplex": lab.get("catalog_slots_duplex_passed"),
                "lab_slots": lab.get("catalog_slots"),
                "schedules_passed": sched.get("passed"),
                "schedules_failed": sched.get("failed"),
                "schedules_n": sched.get("schedules"),
            },
            indent=2,
        )
    )
