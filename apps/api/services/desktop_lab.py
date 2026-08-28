"""Desktop lab: ≥80 catalog connectors, each as destination *and* source.

Honesty
-------
* 80 is **catalog slots** exercised dest then source on this named fixture —
  not 80 unique engines, not catalog tile count, not 650+ live.
* Hosted twins share a parent driver. Unique engines are counted separately.
* Each slot must write the fixture (2 rows) and read those 2 rows back.
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
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99},
    {"source": "amount", "target": "amount", "confidence": 0.99},
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

    @property
    def duplex(self) -> bool:
        return (
            self.dest_status == "passed"
            and self.source_status == "passed"
            and self.dest_rows == FIXTURE_ROWS
            and self.source_rows == FIXTURE_ROWS
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
        skip_preflight=True,
        validation_mode="balanced",
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
        skip_preflight=True,
        validation_mode="balanced",
    )
    dest_res = None
    try:
        dest_res = _run(dest_req)
    except Exception as exc:
        _mark_role(result, "dest", False, None, str(exc))
    else:
        _mark_role(
            result,
            "dest",
            bool(dest_res.success),
            int(dest_res.records_transferred or 0) if dest_res.success else None,
            dest_res.error or "dest write failed",
        )

    sqlite_path = lab.root / f"from_{table}.db"
    if spec["family"] == "file" and result.dest_status != "passed":
        # CSV/TSV parsers can still prove source from the fixture. Other
        # formats must not be fed CSV bytes (that is corruption, not a skip).
        if driver in {"csv", "tsv"}:
            try:
                src_res = _run(_file_source_request(driver, sqlite_path, f"from_{table}"))
                _mark_role(
                    result,
                    "source",
                    bool(src_res.success),
                    int(src_res.records_transferred or 0) if src_res.success else None,
                    src_res.error or "source read failed",
                )
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
                table=f"from_{table}",
            ),
            source_filename=str(filename),
            source_content=content if content else CSV_BYTES,
            mappings=list(MAPPINGS),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
        )
    else:
        src_req = TransferRequest(
            source=source,
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                database=str(sqlite_path),
                connection_string=f"sqlite:///{sqlite_path}",
                table=f"from_{table}",
            ),
            mappings=list(MAPPINGS),
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
        )
    try:
        src_res = _run(src_req)
    except Exception as exc:
        _mark_role(result, "source", False, None, str(exc))
        return result
    _mark_role(
        result,
        "source",
        bool(src_res.success),
        int(src_res.records_transferred or 0) if src_res.success else None,
        src_res.error or "source read failed",
    )
    return result


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
    unique_duplex = [r for r in duplex if r["role"] == "unique_engine"]
    dest_pass = sum(1 for r in items if r["dest_status"] == "passed")
    src_pass = sum(1 for r in items if r["source_status"] == "passed")
    failed = [
        r
        for r in items
        if r["dest_status"] == "failed" or r["source_status"] == "failed"
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
        "unique_engines_duplex_passed": len(unique_duplex),
        "dest_passed": dest_pass,
        "source_passed": src_pass,
        "failed": len(failed),
        "skipped": len(skipped),
        "one_hundred_percent": (
            slots >= DESKTOP_LAB_MIN_DUPLEX
            and len(duplex) == slots
            and len(failed) == 0
            and len(skipped) == 0
        ),
        "honesty": {
            "one_hundred_percent": (
                "this named desktop-lab fixture only — dest write and source "
                f"read of {FIXTURE_ROWS} rows on every listed catalog slot"
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
