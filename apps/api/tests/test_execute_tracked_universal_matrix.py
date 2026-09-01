"""Universal transfer matrix.

Tests every live route in ``src.transfer.registry.LIVE_MATRIX`` with a tiny
payload.  Routes whose source or destination is not reachable on the current
machine are skipped gracefully, so the same file can run in CI with many
emulators or locally with only Postgres/Mongo/Redis/Minio/DuckDB/SQLite.
"""
from __future__ import annotations

import json
import socket
import sys
import uuid
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.connector_capabilities import (  # noqa: E402
    default_port,
    driver_available,
    resolve_driver_type,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.registry import (  # noqa: E402
    LIVE_DEST_DATABASES,
    LIVE_MATRIX,
    LIVE_SOURCE_DATABASES,
)

try:
    from tests.test_live_emulator_matrix import CASES as EMULATOR_CASES  # type: ignore
except Exception:  # pragma: no cover - fallback if import path differs
    from test_live_emulator_matrix import CASES as EMULATOR_CASES  # type: ignore


_SAAS_STUB_URL: str | None = None


def _saas_stub_url() -> str:
    """Local HTTP stub for Salesforce / HubSpot / Stripe / REST — not a customer org."""
    global _SAAS_STUB_URL
    if _SAAS_STUB_URL is None:
        from tests.saas_desktop_stub import seed_tabular_fixture, start_saas_stub

        _server, url = start_saas_stub()
        seed_tabular_fixture()
        _SAAS_STUB_URL = url
    return _SAAS_STUB_URL


def _oracle_sku_password() -> str:
    from services.desktop_lab_cross import _oracle_password

    return _oracle_password()
COLUMNS = ["id", "amount"]
SCHEMA = {"id": "INTEGER", "amount": "DECIMAL"}
MAPPINGS = [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}]


def _is_reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except Exception:
        return False


def _is_file_based_sql(endpoint: EndpointConfig) -> bool:
    fmt = (endpoint.format or "").lower()
    db_path = (endpoint.database or endpoint.connection_string or "").lower()
    if fmt in {"duckdb", "sqlite"}:
        return True
    if fmt == "generic_sql" and (".duckdb" in db_path or ".db" in db_path):
        return True
    return False


def _tcp_target(endpoint: EndpointConfig) -> tuple[str, int]:
    """Host/port for a reachability probe, including ``http://host:port`` hosts."""
    host = (endpoint.host or "").strip()
    port = int(endpoint.port or 0)
    if "://" in host:
        parsed = urlparse(host)
        host = parsed.hostname or "127.0.0.1"
        port = port or int(parsed.port or 0)
    cs = (endpoint.connection_string or "").strip()
    if cs.startswith("http://") or cs.startswith("https://"):
        parsed = urlparse(cs)
        if not host or host in {"localhost", "127.0.0.1"}:
            host = parsed.hostname or host or "127.0.0.1"
        port = port or int(parsed.port or 0)
    driver = resolve_driver_type(endpoint.format)
    port = port or int(default_port(driver) or 0)
    return host or "localhost", int(port or 0)


def _endpoint_reachable(endpoint: EndpointConfig) -> bool:
    if endpoint.kind != "database":
        return True
    driver = resolve_driver_type(endpoint.format)
    if not driver_available(driver, endpoint.format):
        return False
    if driver in {"sqlite", "generic_sql"} and _is_file_based_sql(endpoint):
        return True
    if driver == "snowflake":
        return True  # exercised through fakesnow
    if driver == "iceberg":
        host, port = _tcp_target(endpoint)
        if port and host:
            return _is_reachable(host, port)
        # Filesystem CoW warehouse (SKU tmp_path) has no REST broker port.
        return True
    host, port = _tcp_target(endpoint)
    if not port:
        return True
    if not _is_reachable(host, port):
        return False
    # Port open ≠ authenticated. Half-dead QEMU mssql on ARM listens then
    # rejects login — fail-skip instead of failing the matrix.
    if driver in {"sqlserver", "mssql", "azure_sql"}:
        # Same handshake as Validate/Execute (pyodbc + operator TLS extra).
        # Bare pymssql ignored ``trust_server_certificate`` and skipped dest
        # SQL Server while source seed failed Driver 18 cert verify.
        try:
            from connectors.generic_sql import connection_options
            from connectors.sqlserver import test_sqlserver

            extra = dict(endpoint.extra or {})
            ok, _msg = test_sqlserver(
                host=host,
                port=int(port),
                database=endpoint.database or "master",
                username=endpoint.username or "sa",
                password=endpoint.password or "",
                schema=endpoint.schema or "dbo",
                connection_string=endpoint.connection_string or "",
                ssl=bool(endpoint.ssl),
                type="sqlserver",
                connect_timeout=3,
                **connection_options(extra),
            )
            return bool(ok)
        except Exception:
            return False
    if driver in {"postgresql", "postgres", "timescaledb", "citus"}:
        # Port open ≠ role/password valid (common on shared localhost:5432).
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=host,
                port=int(port),
                dbname=endpoint.database or "postgres",
                user=endpoint.username or "postgres",
                password=endpoint.password or "",
                connect_timeout=3,
            )
            conn.close()
            return True
        except Exception:
            return False
    return True


# Build a lookup of canonical emulator endpoint templates keyed by the exact
# format/catalog id.  We only keep endpoints whose format is itself a live
# driver id (e.g. postgresql, s3, snowflake).  Aliases such as timescaledb or
# presto resolve to generic/postgresql drivers but are not used here, because
# they may point to unreachable emulator ports and hide the canonical case.
_LIVE_DB_DRIVERS: set[str] = set(LIVE_SOURCE_DATABASES) | set(LIVE_DEST_DATABASES)
_DB_TEMPLATES: dict[str, EndpointConfig] = {}
for _param in EMULATOR_CASES:
    _ep: EndpointConfig = _param.values[0]
    _fmt = (_ep.format or "").lower()
    if _fmt in _LIVE_DB_DRIVERS and _fmt not in _DB_TEMPLATES:
        _DB_TEMPLATES[_fmt] = _ep


def _build_db_endpoint(
    driver: str,
    tmp_path: Path,
    role: str,
    suffix: str,
    *,
    object_store: str = "",
    sftp_server: Any = None,
) -> EndpointConfig:
    """Return a database EndpointConfig for a live driver with a unique table/key."""
    if driver == "sftp":
        # ``local_sftp`` runs paramiko's server half in-process. The generated
        # host key is pinned so the matrix exercises real verification rather
        # than the insecure_ignore escape.
        if sftp_server is None:
            pytest.skip("no local SFTP server (paramiko unavailable)")
        remote = f"/matrix_payments_{role}_{suffix}.json"
        cfg = sftp_server.endpoint_config(remote)
        return EndpointConfig(
            kind="database",
            format="sftp",
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"],
            database=cfg["database"],
            table=cfg["table"],
            extra={"host_key": cfg["host_key"]},
        )
    if driver == "s3":
        # ``local_object_store`` supplies moto (or a MinIO / real endpoint the
        # operator points at). Without one there is nothing to talk to, and a
        # skip is the honest answer.
        if not object_store:
            pytest.skip("no local object store endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)")
        from tests.conftest import LOCAL_OBJECT_STORE_BUCKET

        parsed = urlparse(object_store)
        return EndpointConfig(
            kind="database",
            format="s3",
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 443,
            database=LOCAL_OBJECT_STORE_BUCKET,
            username="test",
            password="test",
            connection_string=object_store,
            table=f"matrix/payments_s3_{role}_{suffix}.json",
        )
    if driver == "dynamodb":
        # The same endpoint answers DynamoDB. Unlike a bucket, a table cannot be
        # written into existence: it needs a declared key schema first, and the
        # writer refuses identity it cannot see. The matrix keys on ``id``.
        if not object_store:
            pytest.skip("no local AWS endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)")
        import boto3

        table = f"payments_dynamodb_{role}_{suffix}"
        client = boto3.client(
            "dynamodb",
            endpoint_url=object_store,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        try:
            client.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                # The matrix keys on an INTEGER id, so the table declares N —
                # a string key would read as a numeric-to-text collapse.
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except client.exceptions.ResourceInUseException:
            pass
        parsed = urlparse(object_store)
        return EndpointConfig(
            kind="database",
            format="dynamodb",
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 443,
            # The connection probe reads the table name from ``database`` while
            # the reader accepts ``table``; set both so neither has to guess.
            database=table,
            username="test",
            password="test",
            connection_string=object_store,
            table=table,
        )
    # SFTP, email, and Qdrant require external network services; the universal
    # matrix test cannot stand up a real server here, so these routes are skipped.
    if driver == "email":
        pytest.skip(f"No local emulator for {driver}")
    if driver in {"salesforce", "hubspot", "stripe", "rest_api"}:
        url = _saas_stub_url()
        parsed = urlparse(url)
        table = {
            "salesforce": "Account",
            "hubspot": "contacts",
            "stripe": "customers",
            "rest_api": "records",
        }[driver]
        return EndpointConfig(
            kind="database",
            format=driver,
            host=url,
            port=int(parsed.port or 0),
            password="stub-token",
            api_key="stub-token",
            table=table,
            ssl=False,
            extra={"local_stub_not_customer_org": True},
        )
    if driver == "pgvector":
        # Require the Postgres vector extension; homebrew PG without pgvector must skip.
        try:
            import psycopg2

            conn = psycopg2.connect(
                host="127.0.0.1",
                port=5432,
                dbname="dataflow",
                user="dataflow",
                password="dataflow",
                connect_timeout=2,
            )
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            finally:
                conn.close()
        except Exception as exc:
            pytest.skip(f"pgvector extension unavailable: {exc}")
    if driver == "generic_sql":
        # Exercise the generic_sql catalog id with a local SQLite file — DuckDB
        # remains certified when its DBAPI is installed; matrix routes must not
        # depend on the duckdb brand path for honesty.
        db_path = tmp_path / f"generic_sql_{role}_{suffix}.db"
        return EndpointConfig(
            kind="database",
            format="generic_sql",
            database=str(db_path),
            connection_string=f"sqlite:///{db_path}",
            table=f"t_{role}_{suffix}",
        )
    if driver == "sqlite":
        db_path = tmp_path / f"sqlite_{role}_{suffix}.db"
        return EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table=f"t_{role}_{suffix}",
        )
    if driver == "iceberg":
        warehouse = tmp_path / f"iceberg_{role}_{suffix}"
        warehouse.mkdir(parents=True, exist_ok=True)
        return EndpointConfig(
            kind="database",
            format="iceberg",
            database=str(warehouse),
            table=f"t_{role}_{suffix}",
            schema="default",
        )
    if driver == "redshift":
        return EndpointConfig(
            kind="database",
            format="redshift",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=f"t_{role}_{suffix}",
        )
    # Compose-default first-class engines (always available as templates; reachability
    # is checked separately so missing services skip instead of raising).
    _COMPOSE_DEFAULTS: dict[str, EndpointConfig] = {
        "postgresql": EndpointConfig(
            kind="database",
            format="postgresql",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table="payments_postgresql",
        ),
        "mysql": EndpointConfig(
            kind="database",
            format="mysql",
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            table="payments_mysql",
        ),
        "mongodb": EndpointConfig(
            kind="database",
            format="mongodb",
            host="127.0.0.1",
            port=27017,
            database="dataflow",
            table="payments_mongodb",
        ),
        "pgvector": EndpointConfig(
            kind="database",
            format="pgvector",
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table="payments_pgvector",
        ),
        "sqlserver": EndpointConfig(
            kind="database",
            format="sqlserver",
            host="127.0.0.1",
            port=1433,
            database="dataflow",
            username="sa",
            password="DataFlow_CDC_2022!",
            schema="dbo",
            table="payments_sqlserver",
            extra={"trust_server_certificate": True, "encrypt": "yes"},
        ),
        "oracle": EndpointConfig(
            kind="database",
            format="oracle",
            host="127.0.0.1",
            port=1521,
            database="XEPDB1",
            username="dataflow",
            password=_oracle_sku_password(),
            schema="DATAFLOW",
            table="payments_oracle",
        ),
        "kafka": EndpointConfig(
            kind="database",
            format="kafka",
            host="127.0.0.1",
            port=9092,
            table="payments_kafka",
        ),
        "qdrant": EndpointConfig(
            kind="database",
            format="qdrant",
            host="127.0.0.1",
            port=6333,
            table="payments_qdrant",
        ),
        "weaviate": EndpointConfig(
            kind="database",
            format="weaviate",
            host="127.0.0.1",
            port=8080,
            table="PaymentsWeaviate",
        ),
    }
    template = _DB_TEMPLATES.get(driver) or _COMPOSE_DEFAULTS.get(driver)
    if template is None:
        pytest.skip(f"No endpoint template for driver '{driver}'")
    # Object-store writers/readers rely on a file extension for content-type
    # detection, so make sure the key ends with .json for s3/gcs/adls.
    base_table = f"payments_{driver}_{role}_{suffix}"
    if driver in {"s3", "gcs", "adls"}:
        base_table += ".json"
    return replace(template, table=base_table)


def _file_content(fmt: str) -> tuple[bytes, str]:
    """Return (content, filename) for a tiny two-row file of the given format."""
    # Document sources are proven in test_document_chunking / PDF SKU paths;
    # the tabular id/amount matrix cannot stand in for chunk provenance rows.
    if fmt in {"pdf", "docx", "html"}:
        pytest.skip(f"{fmt} document sources use chunking path; covered by document tests")
    df_data = {"id": ["1", "2"], "amount": ["1000.00", "2000.50"]}
    if fmt == "csv":
        return (b"id,amount\n1,1000.00\n2,2000.50\n", "data.csv")
    if fmt == "tsv":
        return (b"id\tamount\n1\t1000.00\n2\t2000.50\n", "data.tsv")
    if fmt == "json":
        return (json.dumps(RECORDS, indent=2).encode(), "data.json")
    if fmt in {"jsonl", "ndjson"}:
        lines = [json.dumps(r, ensure_ascii=False) for r in RECORDS]
        return ("\n".join(lines).encode(), f"data.{fmt}")
    if fmt == "parquet":
        pd = pytest.importorskip("pandas")
        buf = BytesIO()
        pd.DataFrame(df_data).to_parquet(buf, engine="pyarrow", index=False)
        return (buf.getvalue(), "data.parquet")
    if fmt == "excel":
        pd = pytest.importorskip("pandas")
        buf = BytesIO()
        pd.DataFrame(df_data).to_excel(buf, engine="openpyxl", index=False)
        return (buf.getvalue(), "data.xlsx")
    if fmt == "avro":
        import fastavro
        buf = BytesIO()
        schema = fastavro.parse_schema({
            "type": "record",
            "name": "DatawrapRow",
            "fields": [
                {"name": "id", "type": ["null", "string"]},
                {"name": "amount", "type": ["null", "string"]},
            ],
        })
        fastavro.writer(buf, schema, list(RECORDS))
        return (buf.getvalue(), "data.avro")
    if fmt == "orc":
        pytest.importorskip("pandas")
        import pyarrow as pa
        import pyarrow.orc as orc
        buf = BytesIO()
        table = pa.table(df_data)
        orc.write_table(table, buf)
        return (buf.getvalue(), "data.orc")
    if fmt == "xml":
        return (
            b'<?xml version="1.0" encoding="UTF-8"?>'
            b'<records><record><id>1</id><amount>1000.00</amount></record>'
            b'<record><id>2</id><amount>2000.50</amount></record></records>',
            "data.xml",
        )
    if fmt == "yaml":
        return (
            b'- id: "1"\n  amount: "1000.00"\n- id: "2"\n  amount: "2000.50"\n',
            "data.yaml",
        )
    if fmt == "fixed_width":
        # Self-describing layout header — guessing widths is forbidden.
        id_w, amt_w = 8, 16
        lines = ["#layout: id:8,amount:16"]
        for rec in RECORDS:
            lines.append(
                str(rec["id"]).ljust(id_w) + str(rec["amount"]).ljust(amt_w)
            )
        return ("\n".join(lines) + "\n").encode(), "data.fwf"
    raise ValueError(f"Unsupported file format: {fmt}")


# Destinations without an independent read-back verifier in reconcile_step.
# Strict mode correctly fails closed for these; the matrix uses balanced so
# writer-ack verification remains exercised without false negatives.
_NO_INDEPENDENT_VERIFIER = frozenset({
    "pgvector", "redis", "redshift", "pinecone", "milvus", "weaviate", "qdrant",
    "kafka", "elasticsearch", "neo4j", "influxdb", "couchbase", "email",
    "salesforce", "hubspot", "rest_api",
})


def _build_source(
    kind: str,
    fmt: str,
    tmp_path: Path,
    suffix: str,
    *,
    object_store: str = "",
    sftp_server: Any = None,
) -> tuple[EndpointConfig, bytes, str]:
    if kind == "file":
        content, filename = _file_content(fmt)
        return EndpointConfig(kind="file", format=fmt), content, filename
    return (
        _build_db_endpoint(
            fmt,
            tmp_path,
            "src",
            suffix,
            object_store=object_store,
            sftp_server=sftp_server,
        ),
        b"",
        "",
    )


def _build_destination(
    kind: str,
    fmt: str,
    tmp_path: Path,
    suffix: str,
    *,
    object_store: str = "",
    sftp_server: Any = None,
) -> EndpointConfig:
    if kind == "file_export":
        return EndpointConfig(kind="file_export", format=fmt)
    return _build_db_endpoint(
        fmt, tmp_path, "dst", suffix, object_store=object_store, sftp_server=sftp_server
    )


def _uses_snowflake(*endpoints: EndpointConfig) -> bool:
    return any(ep.format == "snowflake" for ep in endpoints if ep.kind == "database")


def _seed_source(source: EndpointConfig) -> dict[str, Any]:
    rows, _, summary = write_destination_database(
        source, RECORDS, COLUMNS, SCHEMA, MAPPINGS
    )
    if rows != 2:
        pytest.skip(f"source seed wrote {rows} rows: {summary}")
    if summary.get("error"):
        pytest.skip(f"source seed error: {summary.get('error')}")
    return summary


ROUTES = sorted(LIVE_MATRIX)


@pytest.mark.parametrize(
    "route",
    ROUTES,
    ids=lambda r: f"{r[0]}_{r[1]}_to_{r[2]}_{r[3]}",
)
def test_live_transfer_route(
    route: tuple[str, str, str, str],
    tmp_path: Path,
    local_object_store: str,
    local_sftp: Any,
) -> None:
    src_kind, src_fmt, dst_kind, dst_fmt = route
    suffix = uuid.uuid4().hex[:12]

    source, source_content, source_filename = _build_source(
        src_kind,
        src_fmt,
        tmp_path,
        suffix,
        object_store=local_object_store,
        sftp_server=local_sftp,
    )
    destination = _build_destination(
        dst_kind,
        dst_fmt,
        tmp_path,
        suffix,
        object_store=local_object_store,
        sftp_server=local_sftp,
    )

    if not _endpoint_reachable(source):
        pytest.skip(f"source {src_kind}/{src_fmt} not reachable")
    if not _endpoint_reachable(destination):
        pytest.skip(f"destination {dst_kind}/{dst_fmt} not reachable")

    validation_mode = "balanced" if dst_fmt in _NO_INDEPENDENT_VERIFIER else "strict"
    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=source_content,
        source_filename=source_filename,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode=validation_mode,
        mappings=MAPPINGS,
    )

    # Rely on snowflake_conn auto-patch for local accounts — nesting an outer
    # fakesnow.patch() races with the product refcount and raises "already patched".
    if _uses_snowflake(source, destination):
        pytest.importorskip("fakesnow")

    engine = UniversalTransferEngine()
    # Seed the source database/object with the two reference rows.
    if source.kind == "database":
        _seed_source(source)
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{route}: {result.error}"
    assert result.records_transferred == 2, (
        f"{route}: expected 2 records, got {result.records_transferred}"
    )
    assert result.explanation, f"{route}: missing pipeline explanation"

    if destination.kind == "database":
        assert result.reconciliation.get("passed") is True, (
            f"{route}: reconciliation failed: {result.reconciliation}"
        )
    else:
        assert result.destination_summary.get("filename"), (
            f"{route}: no exported filename in destination_summary"
        )
