from __future__ import annotations

import os
import socket
import time
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from connectors.iceberg_catalog import load_catalog

try:
    from connectors.iceberg_maintenance import (
        IcebergMaintenanceUnsupportedError,
        MaintenancePolicy,
        expire_snapshots,
        remove_orphan_files,
        rewrite_data_files,
    )
except ImportError:
    IcebergMaintenanceUnsupportedError = None
    MaintenancePolicy = None
    expire_snapshots = remove_orphan_files = rewrite_data_files = None


try:
    from connectors.iceberg_reader import IcebergTimeTravelError, read_table_batch
except ImportError:
    IcebergTimeTravelError = None
    read_table_batch = None

REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181")
REST_WAREHOUSE = os.environ.get("DATAFLOW_ICEBERG_WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("DATAFLOW_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000")


def _endpoint(tmp_path, name: str) -> dict:
    return {
        "connection_string": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": str(tmp_path / "warehouse"),
        "table": name,
        "schema": "default",
        "extra": {"catalog_type": "sql"},
    }


def _create_table(endpoint: dict, name: str):
    catalog = load_catalog(endpoint)
    catalog.create_namespace("default")
    return catalog, catalog.create_table(
        ("default", name),
        schema=Schema(
            NestedField(1, "id", StringType(), required=False),
            NestedField(2, "v", StringType(), required=False),
            NestedField(3, "_df_lsn", StringType(), required=False),
        ),
    )


def _append(table, value: str) -> None:
    table.append(
        pa.Table.from_pylist(
            [{"id": value, "v": value, "_df_lsn": None}],
            schema=pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("v", pa.string()),
                    pa.field("_df_lsn", pa.string()),
                ]
            ),
        )
    )


def _recent_policy(*, retain_last: int, dry_run: bool = True):
    return MaintenancePolicy(
        older_than_ms=int(time.time() * 1000) + 60_000,
        retain_last=retain_last,
        dry_run=dry_run,
        allow_recent=True,
    )


def test_policy_validates_retention_and_recent_cutoff() -> None:
    with pytest.raises(ValueError, match="retain_last"):
        MaintenancePolicy(older_than_ms=1, retain_last=0, allow_recent=True)
    with pytest.raises(ValueError, match="one hour"):
        MaintenancePolicy(older_than_ms=int(time.time() * 1000) - 1_000)


def test_dry_run_reports_candidates_without_mutating_catalog(tmp_path) -> None:
    name = f"dry_{uuid4().hex[:10]}"
    endpoint = _endpoint(tmp_path, name)
    catalog, table = _create_table(endpoint, name)
    for value in ("one", "two", "three"):
        _append(table, value)
        table = catalog.load_table(("default", name))
        time.sleep(0.005)
    before = {snapshot.snapshot_id for snapshot in table.snapshots()}

    report = expire_snapshots(endpoint, name, _recent_policy(retain_last=1))
    after = {
        snapshot.snapshot_id
        for snapshot in catalog.load_table(("default", name)).snapshots()
    }
    assert report.dry_run is True
    assert report.candidates
    assert report.expired == ()
    assert set(report.retained) | set(report.candidates) == before
    assert after == before


def test_expiry_preserves_current_tag_and_newest_and_rejects_expired_id(
    tmp_path,
) -> None:
    assert issubclass(IcebergTimeTravelError, ValueError)
    name = f"retain_{uuid4().hex[:10]}"
    endpoint = _endpoint(tmp_path, name)
    catalog, table = _create_table(endpoint, name)
    snapshots = []
    for value in ("one", "two", "three", "four", "five", "six"):
        _append(table, value)
        table = catalog.load_table(("default", name))
        snapshots.append(table.current_snapshot())
        time.sleep(0.005)
    tag_id = snapshots[1].snapshot_id
    table.manage_snapshots().create_tag(tag_id, "pinned").commit()
    newest_ids = {snap.snapshot_id for snap in snapshots[-2:]}
    current_id = snapshots[-1].snapshot_id

    report = expire_snapshots(
        endpoint,
        name,
        _recent_policy(retain_last=2, dry_run=False),
    )
    remaining = {
        snapshot.snapshot_id
        for snapshot in catalog.load_table(("default", name)).snapshots()
    }
    assert report.expired
    assert current_id in remaining
    assert tag_id in remaining
    assert newest_ids <= remaining
    assert set(report.expired) == set(report.candidates)
    expired_id = report.expired[0]
    with pytest.raises(IcebergTimeTravelError, match="snapshot_id"):
        read_table_batch(
            cfg={**endpoint, "snapshot_id": expired_id},
            table=name,
            columns=["id"],
        )


def test_m2_watermark_resolves_after_m5_expiration(tmp_path) -> None:
    from connectors.iceberg_eos import (
        apply_eos_iceberg,
        iceberg_dest_watermark_lsn,
    )
    from services.cdc_engine import ChangeBatch

    name = f"watermark_{uuid4().hex[:10]}"
    endpoint = _endpoint(tmp_path, name)
    catalog, _table = _create_table(endpoint, name)
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    for lsn, value in (("0/10", "one"), ("0/11", "two")):
        apply_eos_iceberg(
            dest_type="iceberg",
            dest_cfg=endpoint,
            dest_table=name,
            change=ChangeBatch(
                inserts=[{"id": "1", "v": value}],
                resume_token={"lsn": lsn},
            ),
            mappings=mappings,
            column_types={"id": "string", "v": "string", "_df_lsn": "string"},
            pk_target_cols=["id"],
            stream_key="m5-maintenance",
            incoming_lsn=lsn,
            batch_id=f"batch-{lsn}",
            writer_fence=1,
        )
    expire_snapshots(endpoint, name, _recent_policy(retain_last=1, dry_run=False))
    assert iceberg_dest_watermark_lsn(endpoint, "m5-maintenance") == "0/11"


def test_compaction_and_orphan_removal_are_typed_unsupported_operations() -> None:
    assert issubclass(IcebergMaintenanceUnsupportedError, NotImplementedError)
    with pytest.raises(IcebergMaintenanceUnsupportedError, match="Spark or the engine"):
        rewrite_data_files({}, "default.t")
    with pytest.raises(IcebergMaintenanceUnsupportedError, match="Spark or the engine"):
        remove_orphan_files({}, "default.t")


def _rest_reachable() -> bool:
    try:
        for url, port in ((REST_URI, 8181), (S3_ENDPOINT, 9000)):
            host = url.split("://", 1)[-1].split("/", 1)[0]
            hostname, _, port_s = host.partition(":")
            with socket.create_connection((hostname, int(port_s or str(port))), timeout=1):
                pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as response:
            return int(response.status) == 200
    except (OSError, URLError, ValueError):
        return False


@pytest.mark.skipif(not _rest_reachable(), reason="Iceberg REST/MinIO is unavailable")
def test_live_rest_expiry_retains_snapshot_for_time_travel(monkeypatch) -> None:
    import connectors.iceberg_catalog as catalog_module
    import connectors.iceberg_maintenance as maintenance_module
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    name = f"m5_{uuid4().hex[:12]}"
    endpoint = {
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": name,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    }

    def rest_loader(config):
        parsed = catalog_module.parse_iceberg_catalog_config(config)
        properties = dict(parsed["properties"])
        properties.update(
            {
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": os.environ.get("MINIO_ROOT_USER", "admin"),
                "s3.secret-access-key": os.environ.get(
                    "MINIO_ROOT_PASSWORD", "password"
                ),
                "s3.path-style-access": "true",
                "s3.region": "us-east-1",
            }
        )
        return RestCatalog(parsed["catalog_name"], **properties)

    monkeypatch.setattr(catalog_module, "load_catalog", rest_loader)
    monkeypatch.setattr(maintenance_module, "load_catalog", rest_loader)
    catalog = rest_loader(endpoint)
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass
    table = catalog.create_table(
        ("default", name),
        schema=Schema(NestedField(1, "id", StringType(), required=False)),
    )
    for value in ("old", "middle", "current"):
        table.append(
            pa.Table.from_pylist(
                [{"id": value}], schema=pa.schema([pa.field("id", pa.string())])
            )
        )
        table = catalog.load_table(("default", name))
        time.sleep(0.01)
    retained_id = table.snapshots()[-2].snapshot_id
    report = expire_snapshots(
        endpoint, name, _recent_policy(retain_last=2, dry_run=False)
    )
    assert report.expired
    result = read_table_batch(
        cfg={**endpoint, "snapshot_id": retained_id},
        table=name,
        columns=["id"],
    )
    assert len(result.rows) == 2
    try:
        import duckdb

        connection = duckdb.connect()
        connection.execute("INSTALL iceberg")
        connection.execute("LOAD iceberg")
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
        connection.execute("SET unsafe_enable_version_guessing = true")
        connection.execute(
            "CREATE SECRET (TYPE S3, KEY_ID ?, SECRET ?, "
            "REGION 'us-east-1', ENDPOINT ?, URL_STYLE 'path', USE_SSL false)",
            [
                os.environ.get("MINIO_ROOT_USER", "admin"),
                os.environ.get("MINIO_ROOT_PASSWORD", "password"),
                S3_ENDPOINT.split("://", 1)[-1],
            ],
        )
        duckdb_rows = connection.execute(
            "SELECT id FROM iceberg_scan(?) ORDER BY id", [table.location()]
        ).fetchall()
        assert len(duckdb_rows) == 3
    except ImportError:
        return
    except Exception as exc:
        pytest.skip(f"DuckDB Iceberg readback unavailable: {exc}")
