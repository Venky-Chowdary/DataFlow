"""Unique-engine source × dest cartesian on live desktop backends.

Honesty
-------
* This is **not** 80×80 catalog aliases and not 650+ live tiles.
* Hosted twins (Neon/RDS/CNPG) share a parent driver — they live in
  ``desktop_lab.DESKTOP_LAB_CONNECTORS``, not here.
* Salesforce / HubSpot / Stripe / Kafka / Elasticsearch are omitted:
  no live backend on this desktop. Skip, never invent green.
* CDC default remains at-least-once upsert.
* Emulators (MinIO, fake-gcs, Azurite, goccy BQ, fakesnow, DynamoDB Local)
  are not a customer-tenant PRODUCTION_SKU certificate.
* Map SSOT stays ``services.semantic_mapper.map_columns``.
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.transfer.models import EndpointConfig, TransferRequest

from services.desktop_lab import (
    CSV_BYTES,
    EXPECTED_PAYLOAD,
    FIXTURE_ROWS,
    MAPPINGS,
    SHAPE_RECIPE,
    _approved_shape_hash,
    _read_payload,
    _run,
    _silent_loss,
)

AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)

# Unique engines we can bind on a desktop lab. Catalog twins are excluded.
LIVE_UNIQUE_ENGINES: tuple[str, ...] = (
    "postgresql",
    "mysql",
    "mongodb",
    "sqlserver",
    "oracle",
    "sqlite",
    "s3",
    "gcs",
    "adls",
    "dynamodb",
    "snowflake",
    "bigquery",
    "redis",
    "iceberg",
)


def _reachable(host: str, port: int, timeout: float = 0.8) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _oracle_password() -> str:
    env = (os.environ.get("DATAFLOW_ORACLE_PASSWORD") or "").strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return ""


def _sid(prefix: str = "x") -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def bind_live_engine(engine: str, table: str, root: Path) -> EndpointConfig | str:
    """Bind one unique engine to a live desktop backend, or a skip reason."""
    engine = (engine or "").strip().lower()
    if engine == "postgresql":
        if not _reachable("127.0.0.1", 5432):
            return "PostgreSQL not reachable on 127.0.0.1:5432"
        return EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=table,
        )
    if engine == "mysql":
        if not _reachable("127.0.0.1", 3306):
            return "MySQL not reachable on 127.0.0.1:3306"
        return EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            table=table,
        )
    if engine == "mongodb":
        if not _reachable("127.0.0.1", 27017):
            return "MongoDB not reachable on 127.0.0.1:27017"
        return EndpointConfig(
            kind="database",
            format="mongodb",
            host="127.0.0.1",
            port=27017,
            database="dataflow",
            table=table,
            collection=table,
            connection_string="mongodb://127.0.0.1:27017/dataflow",
        )
    if engine == "sqlserver":
        if not _reachable("127.0.0.1", 1433):
            return "SQL Server not reachable on 127.0.0.1:1433"
        return EndpointConfig(
            kind="database",
            format="sqlserver",
            host="127.0.0.1",
            port=1433,
            database="dataflow",
            username="sa",
            password="Datawrap_CDC_2022!",
            schema="dbo",
            table=table,
        )
    if engine == "oracle":
        if not _reachable("127.0.0.1", 1521):
            return "Oracle not reachable on 127.0.0.1:1521"
        password = _oracle_password()
        if not password:
            return "Oracle password unset (DATAFLOW_ORACLE_PASSWORD)"
        return EndpointConfig(
            kind="database",
            format="oracle",
            host="127.0.0.1",
            port=1521,
            database="XEPDB1",
            username="dataflow",
            password=password,
            schema="DATAFLOW",
            table=table,
        )
    if engine == "sqlite":
        path = root / f"{table}.db"
        return EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(path),
            connection_string=f"sqlite:///{path}",
            table=table,
        )
    if engine == "s3":
        if not _reachable("127.0.0.1", 9000):
            return "MinIO not reachable on 127.0.0.1:9000"
        return EndpointConfig(
            kind="database",
            format="s3",
            host="127.0.0.1",
            port=9000,
            database="dataflow",
            username="dataflow",
            password="dataflowsecret",
            connection_string="http://127.0.0.1:9000",
            endpoint_url="http://127.0.0.1:9000",
            path_style=True,
            table=f"xmat/{table}.json",
            extra={"endpoint_url": "http://127.0.0.1:9000"},
        )
    if engine == "gcs":
        if not _reachable("127.0.0.1", 4443):
            return "fake-gcs not reachable on 127.0.0.1:4443"
        return EndpointConfig(
            kind="database",
            format="gcs",
            host="localhost",
            port=4443,
            database="dataflow-test",
            connection_string="http://localhost:4443",
            table=f"xmat/{table}.json",
        )
    if engine == "adls":
        if not _reachable("127.0.0.1", 10000):
            return "Azurite not reachable on 127.0.0.1:10000"
        return EndpointConfig(
            kind="database",
            format="adls",
            host="127.0.0.1",
            port=10000,
            database="test",
            username="devstoreaccount1",
            password=AZURITE_KEY,
            table=f"xmat/{table}.json",
        )
    if engine == "dynamodb":
        if not _reachable("127.0.0.1", 8000):
            return "DynamoDB Local not reachable on 127.0.0.1:8000"
        return EndpointConfig(
            kind="database",
            format="dynamodb",
            host="127.0.0.1",
            port=8000,
            database="us-east-1",
            username="test",
            password="test",
            connection_string="http://127.0.0.1:8000",
            table=table,
            extra={"endpoint_url": "http://127.0.0.1:8000"},
        )
    if engine == "snowflake":
        try:
            import fakesnow  # noqa: F401
        except ImportError:
            return "fakesnow package not installed"
        return EndpointConfig(
            kind="database",
            format="snowflake",
            host="localhost",
            port=443,
            database="dataflow",
            username="test",
            password="test",
            schema="public",
            table=table,
        )
    if engine == "bigquery":
        if not _reachable("127.0.0.1", 9050):
            return "BigQuery emulator not reachable on 127.0.0.1:9050"
        return EndpointConfig(
            kind="database",
            format="bigquery",
            host="127.0.0.1",
            port=9050,
            database="dataflow-test",
            schema="dataflow",
            connection_string="http://127.0.0.1:9050",
            table=table,
        )
    if engine == "redis":
        if not _reachable("127.0.0.1", 6379):
            return "Redis not reachable on 127.0.0.1:6379"
        return EndpointConfig(
            kind="database",
            format="redis",
            host="127.0.0.1",
            port=6379,
            database="0",
            table=table,
        )
    if engine == "iceberg":
        if not _reachable("127.0.0.1", 8181):
            return "Iceberg REST not reachable on 127.0.0.1:8181"
        return EndpointConfig(
            kind="database",
            format="iceberg",
            host="127.0.0.1",
            port=8181,
            database="default",
            schema="default",
            connection_string="http://127.0.0.1:8181",
            warehouse="file:///tmp/iceberg-rest-wh",
            table=table,
        )
    return f"no live bind for unique engine {engine}"


def _cfg(ep: EndpointConfig) -> dict[str, Any]:
    return {
        "type": ep.format,
        "host": ep.host,
        "port": ep.port,
        "database": ep.database,
        "schema": ep.schema,
        "username": ep.username,
        "password": ep.password,
        "connection_string": ep.connection_string,
        "warehouse": ep.warehouse,
        "path_style": ep.path_style,
        "endpoint_url": ep.endpoint_url or ep.connection_string,
        "extra": dict(ep.extra or {}),
    }


def _dest_count(ep: EndpointConfig) -> int | None:
    from services.dest_precount import destination_row_count

    try:
        return destination_row_count(
            ep.format, _cfg(ep), schema=ep.schema or "", table_name=ep.table
        )
    except Exception:
        return None


def _seed(engine: str, root: Path) -> tuple[EndpointConfig | None, dict[str, Any]]:
    table = _sid("s")
    bound = bind_live_engine(engine, table, root)
    row: dict[str, Any] = {"engine": engine, "status": "skipped", "error": "", "table": table}
    if isinstance(bound, str):
        row["error"] = bound
        return None, row
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=bound,
        source_filename="lab.csv",
        source_content=CSV_BYTES,
        mappings=list(MAPPINGS),
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
        shape_recipe=dict(SHAPE_RECIPE),
        approved_shape_recipe_hash=_approved_shape_hash(),
    )
    try:
        res = _run(req)
    except Exception as exc:
        row["status"] = "failed"
        row["error"] = str(exc)[:400]
        return None, row
    lost, rejected, coerced = _silent_loss(res.destination_summary or {})
    count = _dest_count(bound)
    row.update(
        {
            "rejected": rejected,
            "coerced": coerced,
            "records": int(res.records_transferred or 0) if res.success else 0,
            "dest_count": count,
        }
    )
    if not res.success or lost or int(res.records_transferred or 0) != FIXTURE_ROWS:
        row["status"] = "failed"
        row["error"] = (res.error or f"seed transferred {res.records_transferred}")[:400]
        return None, row
    row["status"] = "passed"
    return bound, row


def _payload_ok(source: EndpointConfig, root: Path) -> tuple[bool, str]:
    sqlite_path = root / f"rb_{uuid.uuid4().hex[:8]}.db"
    table = "payload"
    req = TransferRequest(
        source=source,
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(sqlite_path),
            connection_string=f"sqlite:///{sqlite_path}",
            table=table,
        ),
        mappings=list(MAPPINGS),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    try:
        res = _run(req)
    except Exception as exc:
        return False, str(exc)[:400]
    if not res.success:
        return False, (res.error or "payload read-back failed")[:400]
    payload = _read_payload(sqlite_path, table)
    if isinstance(payload, str):
        return False, payload
    if tuple(payload) != EXPECTED_PAYLOAD:
        return False, f"corruption: got {payload} expected {list(EXPECTED_PAYLOAD)}"
    return True, ""


def _transfer_pair(src: EndpointConfig, dst: EndpointConfig) -> dict[str, Any]:
    req = TransferRequest(
        source=src,
        destination=dst,
        mappings=list(MAPPINGS),
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
    )
    try:
        res = _run(req)
    except Exception as exc:
        return {"status": "failed", "error": str(exc)[:400], "records": 0, "dest_count": None}
    lost, rejected, coerced = _silent_loss(res.destination_summary or {})
    count = _dest_count(dst)
    ok = (
        bool(res.success)
        and not lost
        and int(res.records_transferred or 0) == FIXTURE_ROWS
        and (count is None or count == FIXTURE_ROWS)
    )
    return {
        "status": "passed" if ok else "failed",
        "error": "" if ok else (res.error or f"records={res.records_transferred} dest_count={count}")[:400],
        "records": int(res.records_transferred or 0) if res.success else 0,
        "dest_count": count,
        "rejected": rejected,
        "coerced": coerced,
        "silent_loss": lost,
    }


def run_live_engine_cross_matrix(*, persist: bool = True) -> dict[str, Any]:
    """Every live unique engine as source × every live unique engine as dest."""
    root = Path(tempfile.mkdtemp(prefix="df_xmat_"))
    seeds: dict[str, EndpointConfig] = {}
    seed_rows: list[dict[str, Any]] = []
    for engine in LIVE_UNIQUE_ENGINES:
        bound, row = _seed(engine, root)
        seed_rows.append(row)
        if bound is not None:
            seeds[engine] = bound

    routes: list[dict[str, Any]] = []
    for src_id in LIVE_UNIQUE_ENGINES:
        src = seeds.get(src_id)
        for dst_id in LIVE_UNIQUE_ENGINES:
            rec: dict[str, Any] = {
                "source": src_id,
                "destination": dst_id,
                "status": "skipped",
                "error": "",
            }
            if src is None:
                rec["error"] = f"source {src_id} was not seeded"
                routes.append(rec)
                continue
            dst_table = _sid("d")
            dst = bind_live_engine(dst_id, dst_table, root)
            if isinstance(dst, str):
                rec["error"] = dst
                routes.append(rec)
                continue
            outcome = _transfer_pair(src, dst)
            rec.update(outcome)
            if rec["status"] == "passed":
                ok, err = _payload_ok(dst, root)
                if not ok:
                    rec["status"] = "failed"
                    rec["error"] = err
                    rec["integrity"] = "failed"
                else:
                    rec["integrity"] = "passed"
            routes.append(rec)

    passed = [r for r in routes if r["status"] == "passed"]
    failed = [r for r in routes if r["status"] == "failed"]
    skipped = [r for r in routes if r["status"] == "skipped"]
    payload = {
        "fixture": "services.desktop_lab_cross.run_live_engine_cross_matrix",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "unique_engines": list(LIVE_UNIQUE_ENGINES),
        "unique_engines_seeded": sorted(seeds),
        "unique_engines_seed_failed": [r for r in seed_rows if r["status"] == "failed"],
        "unique_engines_seed_skipped": [r for r in seed_rows if r["status"] == "skipped"],
        "pairs": len(routes),
        "passed": len(passed),
        "failed": len(failed),
        "skipped": len(skipped),
        "seeds": seed_rows,
        "routes": routes,
        "failed_detail": failed,
        "skipped_detail": skipped,
        "honesty": {
            "not_catalog_alias_cartesian": True,
            "not_eighty_unique_engines": True,
            "not_customer_tenant_sku": True,
            "catalog_tiles_are_not_transfer_live": True,
            "cdc_default": "at-least-once upsert",
            "saas_omitted": ["salesforce", "hubspot", "stripe"],
            "map_ssot": "services.semantic_mapper.map_columns",
            "one_hundred_percent": (
                "this named unique-engine fixture only — 2 shaped rows "
                "(1/1000.00/USD, 2/2000.50/EUR) on each live src×dst pair"
            ),
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
    (dest / "desktop_lab_cross.json").write_text(text)
    artifacts = Path("/opt/cursor/artifacts")
    if artifacts.is_dir():
        (artifacts / "desktop_lab_cross.json").write_text(text)
        lab = artifacts / "warehouse-emulator-lab"
        lab.mkdir(parents=True, exist_ok=True)
        (lab / "desktop_lab_cross.json").write_text(text)


def last_cross_report() -> dict[str, Any] | None:
    try:
        from services.platform_config import data_dir

        path = data_dir() / "proofs" / "desktop_lab_cross.json"
        if path.is_file():
            return json.loads(path.read_text())
    except Exception:
        return None
    return None
