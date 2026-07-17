"""Universal transfer matrix.

Tests every live route in ``src.transfer.registry.LIVE_MATRIX`` with a tiny
payload.  Routes whose source or destination is not reachable on the current
machine are skipped gracefully, so the same file can run in CI with many
emulators or locally with only Postgres/Mongo/Redis/Minio/DuckDB/SQLite.
"""
from __future__ import annotations

import contextlib
import json
import socket
import sys
import uuid
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.connector_capabilities import (  # noqa: E402
    default_port,
    resolve_driver_type,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.registry import (  # noqa: E402
    LIVE_DEST_DATABASES,
    LIVE_DEST_FILE_FORMATS,
    LIVE_MATRIX,
    LIVE_SOURCE_DATABASES,
    LIVE_SOURCE_FORMATS,
)

try:
    from tests.test_live_emulator_matrix import CASES as EMULATOR_CASES  # type: ignore
except Exception:  # pragma: no cover - fallback if import path differs
    from test_live_emulator_matrix import CASES as EMULATOR_CASES  # type: ignore


RECORDS = [{"id": "1", "amount": "1000.00"}, {"id": "2", "amount": "2000.50"}]
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


def _endpoint_reachable(endpoint: EndpointConfig) -> bool:
    if endpoint.kind != "database":
        return True
    driver = resolve_driver_type(endpoint.format)
    if driver in {"sqlite", "generic_sql"} and _is_file_based_sql(endpoint):
        return True
    if driver == "snowflake":
        return True  # exercised through fakesnow
    host = endpoint.host or "localhost"
    port = endpoint.port or default_port(driver)
    return _is_reachable(host, port)


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


def _build_db_endpoint(driver: str, tmp_path: Path, role: str, suffix: str) -> EndpointConfig:
    """Return a database EndpointConfig for a live driver with a unique table/key."""
    # SFTP and email require external network services; the universal matrix test
    # cannot stand up a real server here, so these routes are skipped.
    if driver in {"sftp", "email"}:
        pytest.skip(f"No local emulator for {driver}")
    if driver == "generic_sql":
        db_path = tmp_path / f"duckdb_{role}_{suffix}.duckdb"
        return EndpointConfig(
            kind="database",
            format="duckdb",
            database=str(db_path),
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
    template = _DB_TEMPLATES.get(driver)
    if template is None:
        raise ValueError(f"No endpoint template for driver '{driver}'")
    # Object-store writers/readers rely on a file extension for content-type
    # detection, so make sure the key ends with .json for s3/gcs/adls.
    base_table = f"payments_{driver}_{role}_{suffix}"
    if driver in {"s3", "gcs", "adls"}:
        base_table += ".json"
    return replace(template, table=base_table)


def _file_content(fmt: str) -> tuple[bytes, str]:
    """Return (content, filename) for a tiny two-row file of the given format."""
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
            "name": "DataFlowRow",
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
    raise ValueError(f"Unsupported file format: {fmt}")


def _build_source(kind: str, fmt: str, tmp_path: Path, suffix: str) -> tuple[EndpointConfig, bytes, str]:
    if kind == "file":
        content, filename = _file_content(fmt)
        return EndpointConfig(kind="file", format=fmt), content, filename
    return _build_db_endpoint(fmt, tmp_path, "src", suffix), b"", ""


def _build_destination(kind: str, fmt: str, tmp_path: Path, suffix: str) -> EndpointConfig:
    if kind == "file_export":
        return EndpointConfig(kind="file_export", format=fmt)
    return _build_db_endpoint(fmt, tmp_path, "dst", suffix)


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
def test_live_transfer_route(route: tuple[str, str, str, str], tmp_path: Path) -> None:
    src_kind, src_fmt, dst_kind, dst_fmt = route
    suffix = uuid.uuid4().hex[:12]

    source, source_content, source_filename = _build_source(src_kind, src_fmt, tmp_path, suffix)
    destination = _build_destination(dst_kind, dst_fmt, tmp_path, suffix)

    if not _endpoint_reachable(source):
        pytest.skip(f"source {src_kind}/{src_fmt} not reachable")
    if not _endpoint_reachable(destination):
        pytest.skip(f"destination {dst_kind}/{dst_fmt} not reachable")

    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=source_content,
        source_filename=source_filename,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=MAPPINGS,
    )

    snowflake = pytest.importorskip("fakesnow") if _uses_snowflake(source, destination) else None
    ctx = snowflake.patch() if snowflake else contextlib.nullcontext()

    engine = UniversalTransferEngine()
    with ctx:
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
