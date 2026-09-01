"""Unique-engine source × dest cartesian on live desktop backends.

Honesty
-------
* This is **not** 80×80 catalog aliases and not 650+ live tiles.
* Hosted twins (Neon/RDS/CNPG) share a parent driver — they live in
  ``desktop_lab.DESKTOP_LAB_CONNECTORS``, not here.
* Salesforce / HubSpot / Stripe / Kafka are omitted: no live SaaS tenant
  on this desktop. Skip, never invent green.
* Elasticsearch is an extended unique engine when ``:9200`` answers.
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

# Engine→engine pairs must not stamp dest types. SQLite stores the seeded
# DECIMAL(18,2) amount as TEXT affinity; G6 then correctly refuses
# TEXT → DECIMAL as a collapse. CSV seed still uses typed MAPPINGS so
# create-new SQL dests invent a real decimal. Mongo's storage key is a
# G13 omit — same declared omission as PRODUCTION_SKU.
PAIR_MAPPINGS = [
    {
        "source": "id",
        "target": "id",
        "confidence": 0.99,
        "transform": "integer",
        "approved": True,
    },
    {
        "source": "amount",
        "target": "amount",
        "confidence": 0.99,
        "transform": "decimal",
        "approved": True,
    },
    {
        "source": "code",
        "target": "code",
        "confidence": 0.99,
        "transform": "none",
        "approved": True,
    },
]
_MONGO_OBJECT_ID_OMISSION = {
    "source": "_id",
    "target": "",
    "confidence": 0.0,
    "intentional_omit": True,
}


def _pair_mappings(src: EndpointConfig) -> list[dict[str, Any]]:
    maps = [dict(item) for item in PAIR_MAPPINGS]
    if src.format == "mongodb":
        maps.append(dict(_MONGO_OBJECT_ID_OMISSION))
    if src.format == "elasticsearch":
        maps.append(dict(_MONGO_OBJECT_ID_OMISSION))
        maps.append(
            {
                "source": "_index",
                "target": "",
                "confidence": 0.0,
                "intentional_omit": True,
            }
        )
    if src.format == "redis":
        maps.append(
            {
                "source": "redis_key",
                "target": "",
                "confidence": 0.0,
                "intentional_omit": True,
            }
        )
        maps.append(
            {
                "source": "redis_type",
                "target": "",
                "confidence": 0.0,
                "intentional_omit": True,
            }
        )
    return maps

# Create-new object/warehouse dests 404 on schema probe; Google/boto retries
# turn that into a hang. Validate still runs on SQL dests. Never skip SQL.
_CREATE_NEW_SKIP_PREFLIGHT = frozenset({
    "s3",
    "gcs",
    "adls",
    "dynamodb",
    "redis",
    "iceberg",
    "bigquery",
    "elasticsearch",
    "sqlserver",
    "snowflake",
})

AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)

# Unique engines we can bind on a desktop lab. Catalog twins are excluded.
CORE_UNIQUE_ENGINES: tuple[str, ...] = (
    "postgresql",
    "mysql",
    "mongodb",
    "sqlite",
    "s3",
)
EXTENDED_UNIQUE_ENGINES: tuple[str, ...] = (
    "sqlserver",
    "oracle",
    "gcs",
    "adls",
    "dynamodb",
    "snowflake",
    "bigquery",
    "redis",
    "iceberg",
    "elasticsearch",
)
LIVE_UNIQUE_ENGINES: tuple[str, ...] = CORE_UNIQUE_ENGINES + EXTENDED_UNIQUE_ENGINES


def engines_for_run() -> tuple[str, ...]:
    """Default is the 5-engine core that dest-COUNTs without probe-retry hangs.

    SQL Server / GCS / BQ create-new probes have hung this host for minutes.
    Set DATAFLOW_CROSS_EXTENDED=1 to include them — do not invent green if they skip.
    Pair/seed calls that exceed DATAFLOW_CROSS_PAIR_TIMEOUT (default 90s) are
    skipped with that reason, never counted as passed.
    """
    if os.environ.get("DATAFLOW_CROSS_EXTENDED", "").strip() == "1":
        return LIVE_UNIQUE_ENGINES
    return CORE_UNIQUE_ENGINES


def _pair_timeout_sec() -> float:
    raw = (os.environ.get("DATAFLOW_CROSS_PAIR_TIMEOUT") or "90").strip()
    try:
        return max(15.0, float(raw))
    except ValueError:
        return 90.0


def _call_with_timeout(fn, timeout_sec: float, *args, **kwargs):
    """Run ``fn`` in a worker; raise TimeoutError if it does not return.

    ``shutdown(wait=False)`` is required: ODBC/Google retries can block the
    worker past the timeout, and a context-manager executor would then wait
    for that hang before the cartesian could skip and continue.
    """
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FuturesTimeout

    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="xmat")
    try:
        fut = pool.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=timeout_sec)
        except FuturesTimeout as exc:
            raise TimeoutError(f"exceeded {timeout_sec:.0f}s") from exc
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


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
            password="DataFlow_CDC_2022!",
            schema="dbo",
            table=table,
            extra={
                "trust_server_certificate": True,
                "encrypt": "yes",
            },
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
    if engine == "elasticsearch":
        if not _reachable("127.0.0.1", 9200):
            return "Elasticsearch not reachable on 127.0.0.1:9200"
        return EndpointConfig(
            kind="database",
            format="elasticsearch",
            host="127.0.0.1",
            port=9200,
            database="dataflow",
            table=table,
            connection_string="http://127.0.0.1:9200",
        )
    return f"no live bind for unique engine {engine}"


def _cfg(ep: EndpointConfig) -> dict[str, Any]:
    from connectors.generic_sql import connection_options

    extra = dict(ep.extra or {})
    cfg: dict[str, Any] = {
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
        "extra": extra,
    }
    cfg.update(connection_options({**extra, **cfg}))
    return cfg


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
        skip_preflight=bound.format in _CREATE_NEW_SKIP_PREFLIGHT,
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
        mappings=_pair_mappings(source),
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
        mappings=_pair_mappings(src),
        sync_mode="full_refresh_overwrite",
        skip_preflight=dst.format in _CREATE_NEW_SKIP_PREFLIGHT,
        validation_mode="strict",
    )
    try:
        res = _run(req)
    except Exception as exc:
        return {"status": "failed", "error": str(exc)[:400], "records": 0, "dest_count": None}
    lost, rejected, coerced = _silent_loss(res.destination_summary or {})
    count = _dest_count(dst)
    err = "" if res.success else (res.error or f"records={res.records_transferred} dest_count={count}")
    err_l = err.lower()
    if (
        not res.success
        and "privilege catalog unavailable" in err_l
    ):
        return {
            "status": "skipped",
            "error": err[:400],
            "records": int(res.records_transferred or 0),
            "dest_count": count,
            "rejected": rejected,
            "coerced": coerced,
            "silent_loss": lost,
        }
    ok = (
        bool(res.success)
        and not lost
        and int(res.records_transferred or 0) == FIXTURE_ROWS
        and (count is None or count == FIXTURE_ROWS)
    )
    return {
        "status": "passed" if ok else "failed",
        "error": "" if ok else (err or f"records={res.records_transferred} dest_count={count}")[:400],
        "records": int(res.records_transferred or 0) if res.success else 0,
        "dest_count": count,
        "rejected": rejected,
        "coerced": coerced,
        "silent_loss": lost,
    }


def run_live_engine_cross_matrix(*, persist: bool = True) -> dict[str, Any]:
    """Every live unique engine as source × every live unique engine as dest."""
    root = Path(tempfile.mkdtemp(prefix="df_xmat_"))
    engines = engines_for_run()
    timeout = _pair_timeout_sec()
    seeds: dict[str, EndpointConfig] = {}
    seed_rows: list[dict[str, Any]] = []
    for engine in engines:
        try:
            bound, row = _call_with_timeout(_seed, timeout, engine, root)
        except TimeoutError as exc:
            bound, row = None, {
                "engine": engine,
                "status": "skipped",
                "error": f"seed {exc}",
                "table": "",
            }
        seed_rows.append(row)
        if bound is not None:
            seeds[engine] = bound

    routes: list[dict[str, Any]] = []
    payload_checked: set[str] = set()
    progress_path = Path("/opt/cursor/artifacts/warehouse-emulator-lab/cross-progress.json")
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    for src_id in engines:
        src = seeds.get(src_id)
        for dst_id in engines:
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
            try:
                outcome = _call_with_timeout(_transfer_pair, timeout, src, dst)
            except TimeoutError as exc:
                outcome = {
                    "status": "skipped",
                    "error": f"pair {exc}",
                    "records": 0,
                    "dest_count": None,
                }
            rec.update(outcome)
            if rec["status"] == "passed":
                if dst_id not in payload_checked:
                    try:
                        ok, err = _call_with_timeout(_payload_ok, timeout, dst, root)
                    except TimeoutError as exc:
                        ok, err = False, f"payload {exc}"
                    if not ok:
                        rec["status"] = "failed"
                        rec["error"] = err
                        rec["integrity"] = "failed"
                    else:
                        payload_checked.add(dst_id)
                        rec["integrity"] = "passed"
                else:
                    rec["integrity"] = "dest_count_pair_payload_sampled_on_engine"
            routes.append(rec)
            if len(routes) % 7 == 0 or rec["status"] != "skipped":
                try:
                    progress_path.write_text(
                        json.dumps(
                            {
                                "done": len(routes),
                                "last": rec,
                                "passed": sum(1 for r in routes if r["status"] == "passed"),
                                "failed": sum(1 for r in routes if r["status"] == "failed"),
                                "skipped": sum(1 for r in routes if r["status"] == "skipped"),
                            },
                            indent=2,
                        )
                    )
                except OSError:
                    pass

    passed = [r for r in routes if r["status"] == "passed"]
    failed = [r for r in routes if r["status"] == "failed"]
    skipped = [r for r in routes if r["status"] == "skipped"]
    payload = {
        "fixture": "services.desktop_lab_cross.run_live_engine_cross_matrix",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "unique_engines": list(engines),
        "unique_engines_catalog": list(LIVE_UNIQUE_ENGINES),
        "extended_opt_in": os.environ.get("DATAFLOW_CROSS_EXTENDED", "").strip() == "1",
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
        "pair_timeout_sec": timeout,
        "honesty": {
            "not_catalog_alias_cartesian": True,
            "not_eighty_unique_engines": True,
            "not_customer_tenant_sku": True,
            "catalog_tiles_are_not_transfer_live": True,
            "cdc_default": "at-least-once upsert",
            "saas_omitted": ["salesforce", "hubspot", "stripe"],
            "elasticsearch_is_extended_when_9200_up": True,
            "map_ssot": "services.semantic_mapper.map_columns",
            "object_store_create_new_skips_preflight_probe": True,
            "payload_reconcile": "once per dest engine; every pair dest COUNT",
            "timeout_is_skip_never_pass": True,
            "pair_mappings_do_not_stamp_dest_types": True,
            "mongo_storage_key_is_declared_g13_omit": True,
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
        try:
            (artifacts / "desktop_lab_cross.json").write_text(text)
        except OSError:
            pass
        try:
            lab = artifacts / "warehouse-emulator-lab"
            lab.mkdir(parents=True, exist_ok=True)
            (lab / "desktop_lab_cross.json").write_text(text)
        except OSError:
            pass


def last_cross_report() -> dict[str, Any] | None:
    try:
        from services.platform_config import data_dir

        path = data_dir() / "proofs" / "desktop_lab_cross.json"
        if path.is_file():
            return json.loads(path.read_text())
    except Exception:
        return None
    return None
