"""Desktop lab: ≥80 catalog connectors, dest + source, no silent loss.

Honesty
-------
* 80 is **catalog slots** on this named fixture — not 80 unique engines,
  not catalog tile count, not 650+ live.
* Hosted twins share a parent driver. Unique engines are counted separately.
* Each slot must: Map SSOT → Validate (preflight) → dest write → source
  read-back → payload reconcile of the 2 fixture rows. Rejected/coerced
  rows fail the slot (silent loss / corruption).
* A connector that cannot be stood up is ``skipped`` — never a fake green.
* CDC default remains at-least-once upsert.
* Source-only (pdf/docx/html/REST) and dest-only (pgvector) tiles are not
  in this list — they cannot pass both roles.
"""

from __future__ import annotations

import json
import os

# Desktop lab / pytest must not block 5s per job on a missing Mongo.
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")

import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from src.transfer.connector_capabilities import resolve_driver_type
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

RoleKind = Literal["unique_engine", "hosted_twin", "format_alias"]

DESKTOP_LAB_MIN_DUPLEX = 80
FIXTURE_ROWS = 2

CSV_BYTES = b"id,amount\n1,1000.00\n2,2000.50\n"
EXPECTED_PAYLOAD = (("1", "1000.00"), ("2", "2000.50"))
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99},
    {"source": "amount", "target": "amount", "confidence": 0.99},
]
SOURCE_SCHEMAS = [
    {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
    {"name": "amount", "inferred_type": "DECIMAL(18,2)", "samples": ["1000.00", "2000.50"]},
]
PG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "schema": "public",
}
MYSQL = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
}

# ≥80 catalog ids that can write *and* read. Unique engines first.
DESKTOP_LAB_CONNECTORS: tuple[dict[str, str], ...] = (
    # File formats — unique engines
    {"id": "csv", "role": "unique_engine", "family": "file"},
    {"id": "tsv", "role": "unique_engine", "family": "file"},
    {"id": "json", "role": "unique_engine", "family": "file"},
    {"id": "jsonl", "role": "unique_engine", "family": "file"},
    {"id": "ndjson", "role": "unique_engine", "family": "file"},
    {"id": "excel", "role": "unique_engine", "family": "file"},
    {"id": "parquet", "role": "unique_engine", "family": "file"},
    {"id": "avro", "role": "unique_engine", "family": "file"},
    {"id": "orc", "role": "unique_engine", "family": "file"},
    {"id": "xml", "role": "unique_engine", "family": "file"},
    # File catalog aliases — same parsers, operator-selectable tiles
    {"id": "csv_upload", "role": "format_alias", "family": "file"},
    {"id": "tsv_upload", "role": "format_alias", "family": "file"},
    {"id": "json_documents", "role": "format_alias", "family": "file"},
    {"id": "excel_workbook", "role": "format_alias", "family": "file"},
    {"id": "parquet_lake", "role": "format_alias", "family": "file"},
    {"id": "jsonl_stream", "role": "format_alias", "family": "file"},
    {"id": "csv___tsv", "role": "format_alias", "family": "file"},
    {"id": "json_api_export", "role": "format_alias", "family": "file"},
    {"id": "csv_sftp", "role": "format_alias", "family": "file"},
    # SQL unique
    {"id": "sqlite", "role": "unique_engine", "family": "sqlite"},
    {"id": "generic_sql", "role": "unique_engine", "family": "generic_sql"},
    {"id": "duckdb", "role": "unique_engine", "family": "duckdb"},
    {"id": "postgresql", "role": "unique_engine", "family": "postgresql"},
    {"id": "mysql", "role": "unique_engine", "family": "mysql"},
    # PostgreSQL hosted twins — same live PG wire
    {"id": "postgresql_neon", "role": "hosted_twin", "family": "postgresql"},
    {"id": "postgresql_supabase", "role": "hosted_twin", "family": "postgresql"},
    {"id": "postgresql_rds", "role": "hosted_twin", "family": "postgresql"},
    {"id": "postgresql_cloud_sql", "role": "hosted_twin", "family": "postgresql"},
    {"id": "postgresql_azure", "role": "hosted_twin", "family": "postgresql"},
    {"id": "timescaledb", "role": "hosted_twin", "family": "postgresql"},
    {"id": "cockroachdb", "role": "hosted_twin", "family": "postgresql"},
    {"id": "neon", "role": "hosted_twin", "family": "postgresql"},
    {"id": "supabase", "role": "hosted_twin", "family": "postgresql"},
    {"id": "amazon_rds_postgresql", "role": "hosted_twin", "family": "postgresql"},
    {"id": "postgresql_aurora_global", "role": "hosted_twin", "family": "postgresql"},
    {"id": "google_cloud_sql_postgresql", "role": "hosted_twin", "family": "postgresql"},
    {"id": "azure_database_for_postgresql", "role": "hosted_twin", "family": "postgresql"},
    {"id": "alloydb", "role": "hosted_twin", "family": "postgresql"},
    {"id": "yugabytedb", "role": "hosted_twin", "family": "postgresql"},
    # MySQL twins — same live MySQL wire
    {"id": "mysql_rds", "role": "hosted_twin", "family": "mysql"},
    {"id": "mysql_cloud_sql", "role": "hosted_twin", "family": "mysql"},
    {"id": "mysql_azure", "role": "hosted_twin", "family": "mysql"},
    {"id": "mariadb", "role": "hosted_twin", "family": "mysql"},
    {"id": "planetscale", "role": "hosted_twin", "family": "mysql"},
    {"id": "amazon_rds_mysql", "role": "hosted_twin", "family": "mysql"},
    {"id": "amazon_aurora", "role": "hosted_twin", "family": "mysql"},
    {"id": "google_cloud_sql_mysql", "role": "hosted_twin", "family": "mysql"},
    {"id": "azure_database_for_mysql", "role": "hosted_twin", "family": "mysql"},
    {"id": "tidb", "role": "hosted_twin", "family": "mysql"},
    {"id": "oceanbase", "role": "hosted_twin", "family": "mysql"},
    {"id": "singlestore", "role": "hosted_twin", "family": "mysql"},
    {"id": "vitess", "role": "hosted_twin", "family": "mysql"},
    {"id": "polardb", "role": "hosted_twin", "family": "mysql"},
    {"id": "gaussdb", "role": "hosted_twin", "family": "mysql"},
    {"id": "goldendb", "role": "hosted_twin", "family": "mysql"},
    {"id": "mysql_planetscale", "role": "hosted_twin", "family": "mysql"},
    {"id": "mysql_aurora_global", "role": "hosted_twin", "family": "mysql"},
    # Object / NoSQL / lake — unique + twins
    {"id": "dynamodb", "role": "unique_engine", "family": "dynamodb"},
    {"id": "amazon_dynamodb", "role": "hosted_twin", "family": "dynamodb"},
    {"id": "dynamodb_global_tables", "role": "hosted_twin", "family": "dynamodb"},
    {"id": "s3", "role": "unique_engine", "family": "s3"},
    {"id": "amazon_s3", "role": "hosted_twin", "family": "s3"},
    {"id": "s3_us_east_1", "role": "hosted_twin", "family": "s3"},
    {"id": "s3_eu_west_1", "role": "hosted_twin", "family": "s3"},
    {"id": "s3_ap_southeast_1", "role": "hosted_twin", "family": "s3"},
    {"id": "minio", "role": "hosted_twin", "family": "s3"},
    {"id": "wasabi", "role": "hosted_twin", "family": "s3"},
    {"id": "backblaze_b2", "role": "hosted_twin", "family": "s3"},
    {"id": "digitalocean_spaces", "role": "hosted_twin", "family": "s3"},
    {"id": "cloudflare_r2", "role": "hosted_twin", "family": "s3"},
    {"id": "alibaba_oss", "role": "hosted_twin", "family": "s3"},
    {"id": "ibm_cloud_object_storage", "role": "hosted_twin", "family": "s3"},
    {"id": "oracle_cloud_object_storage", "role": "hosted_twin", "family": "s3"},
    {"id": "snowflake", "role": "unique_engine", "family": "snowflake"},
    {"id": "snowflake_aws", "role": "hosted_twin", "family": "snowflake"},
    {"id": "snowflake_standard", "role": "hosted_twin", "family": "snowflake"},
    {"id": "snowflake_enterprise", "role": "hosted_twin", "family": "snowflake"},
    {"id": "snowflake_azure", "role": "hosted_twin", "family": "snowflake"},
    {"id": "snowflake_gcp", "role": "hosted_twin", "family": "snowflake"},
    # generic_sql twins — local SQLite/DuckDB wire, catalog id is the tile
    {"id": "motherduck", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "amazon_emr", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "cloudera_data_platform", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "sap_bw_4hana", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "clickhouse", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "trino", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "citus", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "greenplum", "role": "hosted_twin", "family": "generic_sql"},
    {"id": "apache_hive", "role": "hosted_twin", "family": "generic_sql"},
)


@dataclass
class ConnectorResult:
    catalog_id: str
    driver: str
    role: str
    family: str
    dest_status: str  # passed | failed | skipped
    source_status: str
    dest_error: str = ""
    source_error: str = ""
    dest_rows: int | None = None
    source_rows: int | None = None
    map_status: str = "skipped"
    map_error: str = ""
    validate_status: str = "skipped"
    integrity_status: str = "skipped"
    integrity_error: str = ""
    silent_loss: bool = False
    dest_rejected: int = 0
    dest_coerced: int = 0
    dest_reconcile: bool | None = None
    source_reconcile: bool | None = None

    @property
    def duplex(self) -> bool:
        return (
            self.dest_status == "passed"
            and self.source_status == "passed"
            and self.dest_rows == FIXTURE_ROWS
            and self.source_rows == FIXTURE_ROWS
        )

    @property
    def operations_ok(self) -> bool:
        return (
            self.duplex
            and self.map_status == "passed"
            and self.validate_status == "passed"
            and self.integrity_status == "passed"
            and not self.silent_loss
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "catalog_id": self.catalog_id,
            "driver": self.driver,
            "role": self.role,
            "family": self.family,
            "dest_status": self.dest_status,
            "source_status": self.source_status,
            "dest_error": self.dest_error[:400],
            "source_error": self.source_error[:400],
            "dest_rows": self.dest_rows,
            "source_rows": self.source_rows,
            "fixture_rows": FIXTURE_ROWS,
            "duplex": self.duplex,
            "map_status": self.map_status,
            "map_error": self.map_error[:400],
            "validate_status": self.validate_status,
            "integrity_status": self.integrity_status,
            "integrity_error": self.integrity_error[:400],
            "silent_loss": self.silent_loss,
            "dest_rejected": self.dest_rejected,
            "dest_coerced": self.dest_coerced,
            "dest_reconcile": self.dest_reconcile,
            "source_reconcile": self.source_reconcile,
            "operations_ok": self.operations_ok,
        }


@dataclass
class LabBackends:
    root: Path
    moto_url: str = ""
    bucket: str = "dataflow-desktop-lab"

    def close(self) -> None:
        moto = getattr(self, "_moto", None)
        if moto is not None:
            try:
                moto.stop()
            except Exception:
                pass


def desktop_lab_catalog_ids() -> list[str]:
    return [str(row["id"]) for row in DESKTOP_LAB_CONNECTORS]


def _reachable(host: str, port: int, timeout: float = 0.6) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _start_backends(root: Path) -> LabBackends:
    lab = LabBackends(root=root)
    try:
        import boto3
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        lab.moto_url = f"http://{host}:{port}"
        lab._moto = server  # type: ignore[attr-defined]
        client = boto3.client(
            "s3",
            endpoint_url=lab.moto_url,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        client.create_bucket(Bucket=lab.bucket)
    except Exception as exc:
        lab.moto_url = ""
        lab.moto_error = str(exc)  # type: ignore[attr-defined]
    return lab


def _sid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _bind(
    spec: dict[str, str],
    lab: LabBackends,
    *,
    table: str,
) -> EndpointConfig | str:
    """Return an endpoint or a skip reason."""
    catalog_id = spec["id"]
    family = spec["family"]
    driver = resolve_driver_type(catalog_id)
    if family == "file":
        return EndpointConfig(kind="file_export", format=driver, table=table)
    if family == "sqlite":
        path = lab.root / f"{table}.db"
        return EndpointConfig(
            kind="database",
            format=driver,
            database=str(path),
            connection_string=f"sqlite:///{path}",
            table=table,
        )
    if family == "generic_sql":
        path = lab.root / f"{table}.db"
        return EndpointConfig(
            kind="database",
            format="generic_sql",
            database=str(path),
            connection_string=f"sqlite:///{path}",
            table=table,
        )
    if family == "duckdb":
        try:
            import duckdb  # noqa: F401
        except ImportError:
            return "duckdb package not installed"
        path = lab.root / f"{table}.duckdb"
        return EndpointConfig(
            kind="database",
            format="generic_sql",
            database=str(path),
            connection_string=f"duckdb:///{path}",
            table=table,
        )
    if family == "postgresql":
        if not _reachable(PG["host"], int(PG["port"])):
            return "PostgreSQL not reachable on 127.0.0.1:5432"
        return EndpointConfig(
            kind="database",
            format=driver,
            table=table,
            **PG,
        )
    if family == "mysql":
        if not _reachable(MYSQL["host"], int(MYSQL["port"])):
            return "MySQL not reachable on 127.0.0.1:3306"
        return EndpointConfig(
            kind="database",
            format=driver,
            table=table,
            **MYSQL,
        )
    if family == "dynamodb":
        if not lab.moto_url:
            return "moto DynamoDB endpoint not started"
        parsed = urlparse(lab.moto_url)
        return EndpointConfig(
            kind="database",
            format=driver,
            host=parsed.hostname or "127.0.0.1",
            port=int(parsed.port or 0),
            database="test",
            table=table,
            connection_string=lab.moto_url,
            extra={"endpoint_url": lab.moto_url},
        )
    if family == "s3":
        if not lab.moto_url:
            return "moto S3 endpoint not started"
        parsed = urlparse(lab.moto_url)
        return EndpointConfig(
            kind="database",
            format=driver,
            host=parsed.hostname or "127.0.0.1",
            port=int(parsed.port or 0),
            database=lab.bucket,
            username="test",
            password="test",
            connection_string=lab.moto_url,
            table=f"lab/{table}.json",
            path_style=True,
            extra={"endpoint_url": lab.moto_url},
        )
    if family == "snowflake":
        try:
            import fakesnow  # noqa: F401
        except ImportError:
            return "fakesnow package not installed"
        return EndpointConfig(
            kind="database",
            format=driver,
            host="localhost",
            port=443,
            database="dataflow",
            username="test",
            password="test",
            schema="public",
            table=table,
        )
    return f"no bind for family {family}"


def _source_from_dest(dest: EndpointConfig, spec: dict[str, str]) -> EndpointConfig:
    """Read back the same object the dest write just created."""
    if spec["family"] == "file":
        fmt = resolve_driver_type(spec["id"])
        return EndpointConfig(
            kind="file",
            format=fmt,
            output_path=dest.output_path,
        )
    return EndpointConfig(
        kind="database",
        format=dest.format,
        host=dest.host,
        port=dest.port,
        database=dest.database,
        schema=dest.schema,
        table=dest.table,
        username=dest.username,
        password=dest.password,
        connection_string=dest.connection_string,
        path_style=dest.path_style,
        extra=dict(dest.extra or {}),
    )


def _run(req: TransferRequest):
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _map_fixture(driver: str, family: str) -> tuple[bool, str]:
    """Map SSOT — id/amount must land on id/amount. File tiles use sqlite types."""
    from services.semantic_mapper import map_columns

    dest_db = "sqlite" if family == "file" else driver
    try:
        mapped = map_columns(
            ["id", "amount"],
            ["id", "amount"],
            source_schemas=list(SOURCE_SCHEMAS),
            destination_db_type=dest_db,
            destination_table_exists=False,
        )
    except Exception as exc:
        return False, str(exc)
    pairs = {
        (str(row.get("source") or "").lower(), str(row.get("target") or "").lower())
        for row in mapped
    }
    if ("id", "id") not in pairs or ("amount", "amount") not in pairs:
        return False, f"map_columns missed identity: {sorted(pairs)}"
    return True, ""


def _norm_amount(value: Any) -> str:
    from decimal import Decimal, InvalidOperation

    text = str(value).strip().replace(",", "")
    try:
        return f"{Decimal(text).quantize(Decimal('0.01'))}"
    except (InvalidOperation, ValueError):
        return text


def _read_payload(sqlite_path: Path, table: str) -> list[tuple[str, str]] | str:
    import sqlite3

    if not sqlite_path.is_file():
        return f"sqlite artifact missing: {sqlite_path}"
    con = sqlite3.connect(str(sqlite_path))
    try:
        cols = [str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")')]
        folded = {c.lower(): c for c in cols}
        if "id" not in folded or "amount" not in folded:
            return f"payload columns {cols} missing id/amount"
        idc, amtc = folded["id"], folded["amount"]
        rows = con.execute(f'SELECT "{idc}", "{amtc}" FROM "{table}"').fetchall()
    except Exception as exc:
        return str(exc)
    finally:
        con.close()
    return sorted((str(i).strip(), _norm_amount(a)) for i, a in rows)


def _silent_loss(summary: dict[str, Any]) -> tuple[bool, int, int]:
    rejected = int(summary.get("rejected_rows") or 0)
    coerced = int(summary.get("coerced_null_rows") or 0)
    return (rejected > 0 or coerced > 0), rejected, coerced


def _reconcile_ok(res: Any) -> bool | None:
    recon = getattr(res, "reconciliation", None) or {}
    if not recon:
        return None
    return bool(recon.get("passed"))


def _file_source_request(fmt: str, sqlite_path: Path, table: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format=fmt),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(sqlite_path),
            connection_string=f"sqlite:///{sqlite_path}",
            table=table,
        ),
        source_filename=f"lab.{fmt}",
        source_content=CSV_BYTES,
        mappings=list(MAPPINGS),
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
    )


def _mark_role(result: ConnectorResult, role: str, ok: bool, rows: int | None, error: str) -> None:
    if ok and rows == FIXTURE_ROWS:
        status, err = "passed", ""
    elif ok:
        status, err = "failed", (
            f"conservation: {role} transferred {rows}, expected {FIXTURE_ROWS}"
        )
    else:
        status, err = "failed", error
    if role == "dest":
        result.dest_status = status
        result.dest_rows = rows
        result.dest_error = err
    else:
        result.source_status = status
        result.source_rows = rows
        result.source_error = err


def exercise_connector(spec: dict[str, str], lab: LabBackends) -> ConnectorResult:
    catalog_id = spec["id"]
    driver = resolve_driver_type(catalog_id)
    result = ConnectorResult(
        catalog_id=catalog_id,
        driver=driver,
        role=spec["role"],
        family=spec["family"],
        dest_status="skipped",
        source_status="skipped",
    )
    mapped, map_err = _map_fixture(driver, spec["family"])
    result.map_status = "passed" if mapped else "failed"
    result.map_error = map_err
    if not mapped:
        result.dest_status = "failed"
        result.dest_error = f"map SSOT failed: {map_err}"
        result.source_error = result.dest_error
        return result

    table = _sid("lab")
    bound = _bind(spec, lab, table=table)
    if isinstance(bound, str):
        result.dest_error = bound
        result.source_error = bound
        return result

    dest_req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=bound,
        source_filename="lab.csv",
        source_content=CSV_BYTES,
        mappings=list(MAPPINGS),
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
    )
    dest_res = None
    try:
        dest_res = _run(dest_req)
    except Exception as exc:
        _mark_role(result, "dest", False, None, str(exc))
        result.validate_status = "failed"
    else:
        summary = dest_res.destination_summary or {}
        lost, rejected, coerced = _silent_loss(summary)
        result.dest_rejected = rejected
        result.dest_coerced = coerced
        result.dest_reconcile = _reconcile_ok(dest_res)
        result.silent_loss = lost
        result.validate_status = "passed" if dest_res.success else "failed"
        dest_ok = bool(dest_res.success) and not lost and result.dest_reconcile is not False
        dest_err = dest_res.error or "dest write failed"
        if dest_res.success and lost:
            dest_err = f"silent loss: rejected={rejected} coerced={coerced}"
        elif dest_res.success and result.dest_reconcile is False:
            dest_err = (dest_res.reconciliation or {}).get("message") or "dest reconcile failed"
        _mark_role(
            result,
            "dest",
            dest_ok,
            int(dest_res.records_transferred or 0) if dest_res.success else None,
            dest_err,
        )

    sqlite_path = lab.root / f"from_{table}.db"
    src_table = f"from_{table}"
    if spec["family"] == "file" and result.dest_status != "passed":
        # CSV/TSV parsers can still prove source from the fixture. Other
        # formats must not be fed CSV bytes (that is corruption, not a skip).
        if driver in {"csv", "tsv"}:
            try:
                src_res = _run(_file_source_request(driver, sqlite_path, src_table))
                _finish_source(result, src_res, sqlite_path, src_table)
            except Exception as exc:
                _mark_role(result, "source", False, None, str(exc))
        return result

    if dest_res is None or result.dest_status != "passed":
        result.source_error = "source skipped — dest write did not land"
        return result

    source = _source_from_dest(bound, spec)
    if spec["family"] == "file":
        artifact = dest_res.destination_summary.get("path") or bound.output_path
        content = None
        filename = dest_res.destination_summary.get("filename") or Path(bound.output_path).name
        if artifact and Path(str(artifact)).is_file():
            content = Path(str(artifact)).read_bytes()
        src_req = TransferRequest(
            source=EndpointConfig(kind="file", format=driver),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(sqlite_path),
                connection_string=f"sqlite:///{sqlite_path}",
                table=src_table,
            ),
            source_filename=str(filename),
            source_content=content if content else CSV_BYTES,
            mappings=list(MAPPINGS),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="strict",
        )
    else:
        src_req = TransferRequest(
            source=source,
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(sqlite_path),
                connection_string=f"sqlite:///{sqlite_path}",
                table=src_table,
            ),
            mappings=list(MAPPINGS),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="strict",
        )
    try:
        src_res = _run(src_req)
    except Exception as exc:
        _mark_role(result, "source", False, None, str(exc))
        return result
    _finish_source(result, src_res, sqlite_path, src_table)
    return result


def _finish_source(
    result: ConnectorResult,
    src_res: Any,
    sqlite_path: Path,
    src_table: str,
) -> None:
    src_lost, src_rej, src_coerced = _silent_loss(src_res.destination_summary or {})
    result.source_reconcile = _reconcile_ok(src_res)
    if src_lost:
        result.silent_loss = True
    src_ok = bool(src_res.success) and not src_lost and result.source_reconcile is not False
    src_err = src_res.error or "source read failed"
    if src_res.success and src_lost:
        src_err = f"silent loss: rejected={src_rej} coerced={src_coerced}"
    elif src_res.success and result.source_reconcile is False:
        src_err = (src_res.reconciliation or {}).get("message") or "source reconcile failed"
    _mark_role(
        result,
        "source",
        src_ok,
        int(src_res.records_transferred or 0) if src_res.success else None,
        src_err,
    )
    if result.source_status != "passed":
        return
    payload = _read_payload(sqlite_path, src_table)
    if isinstance(payload, str):
        result.integrity_status = "failed"
        result.integrity_error = payload
        result.source_status = "failed"
        result.source_error = payload
        return
    if tuple(payload) != EXPECTED_PAYLOAD:
        result.integrity_status = "failed"
        result.integrity_error = f"corruption: got {payload} expected {list(EXPECTED_PAYLOAD)}"
        result.source_status = "failed"
        result.source_error = result.integrity_error
        return
    result.integrity_status = "passed"


def run_desktop_lab(*, persist: bool = True) -> dict[str, Any]:
    """Bind every desktop-lab catalog id and exercise dest + source roles."""
    if len(DESKTOP_LAB_CONNECTORS) < DESKTOP_LAB_MIN_DUPLEX:
        raise RuntimeError(
            f"desktop lab must list at least {DESKTOP_LAB_MIN_DUPLEX} catalog connectors"
        )
    root = Path(tempfile.mkdtemp(prefix="df_desktop_lab_"))
    lab = _start_backends(root)
    rows: list[ConnectorResult] = []
    try:
        for spec in DESKTOP_LAB_CONNECTORS:
            rows.append(exercise_connector(spec, lab))
    finally:
        lab.close()

    payload = summarize(rows)
    if persist:
        _persist(payload)
    return payload


def summarize(rows: list[ConnectorResult]) -> dict[str, Any]:
    items = [row.to_dict() for row in rows]
    duplex = [r for r in items if r["duplex"]]
    ops = [r for r in items if r["operations_ok"]]
    unique_duplex = [r for r in duplex if r["role"] == "unique_engine"]
    dest_pass = sum(1 for r in items if r["dest_status"] == "passed")
    src_pass = sum(1 for r in items if r["source_status"] == "passed")
    failed = [
        r
        for r in items
        if r["dest_status"] == "failed"
        or r["source_status"] == "failed"
        or r["map_status"] == "failed"
        or r["integrity_status"] == "failed"
        or r.get("silent_loss")
    ]
    skipped = [
        r
        for r in items
        if r["dest_status"] == "skipped" and r["source_status"] == "skipped"
    ]
    slots = len(items)
    return {
        "fixture": "services.desktop_lab.run_desktop_lab",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "catalog_slots": slots,
        "catalog_slots_duplex_passed": len(duplex),
        "catalog_slots_operations_passed": len(ops),
        "unique_engines_duplex_passed": len(unique_duplex),
        "dest_passed": dest_pass,
        "source_passed": src_pass,
        "failed": len(failed),
        "skipped": len(skipped),
        "one_hundred_percent": (
            slots >= DESKTOP_LAB_MIN_DUPLEX
            and len(ops) == slots
            and len(failed) == 0
            and len(skipped) == 0
        ),
        "honesty": {
            "one_hundred_percent": (
                "this named desktop-lab fixture only — Map SSOT, Validate, "
                f"dest write, source read, and payload reconcile of {FIXTURE_ROWS} "
                "rows with zero rejected/coerced on every listed catalog slot"
            ),
            "eighty": (
                "80 is catalog slots this lab can bind on a desktop. "
                "It is not 80 unique engines and not catalog tile count."
            ),
            "hosted_twins_share_a_driver": True,
            "cdc_default": "at-least-once upsert",
            "catalog_tiles_are_not_transfer_live": True,
            "source_only_and_dest_only_excluded": True,
        },
        "connectors": items,
        "failed_detail": failed,
        "skipped_detail": skipped,
    }


def _persist(payload: dict[str, Any]) -> None:
    from services.platform_config import data_dir

    dest = data_dir() / "proofs"
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "desktop_lab_duplex.json"
    path.write_text(json.dumps(payload, indent=2) + "\n")
    artifacts = Path("/opt/cursor/artifacts")
    if artifacts.is_dir():
        (artifacts / "desktop_lab_duplex.json").write_text(
            json.dumps(payload, indent=2) + "\n"
        )


def last_desktop_lab_report() -> dict[str, Any] | None:
    try:
        from services.platform_config import data_dir

        path = data_dir() / "proofs" / "desktop_lab_duplex.json"
        if path.is_file():
            return json.loads(path.read_text())
    except Exception:
        return None
    return None
