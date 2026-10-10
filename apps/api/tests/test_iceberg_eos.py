from __future__ import annotations

import hashlib
import os
import socket
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from services.cdc_engine import ChangeBatch
from services.cdc_exactly_once import EosCrash, ExactlyOnceRouteError

try:
    from connectors import iceberg_eos
except ImportError:
    iceberg_eos = None


REST_URI = os.environ.get(
    "DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181"
)
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_WAREHOUSE", "s3://warehouse/"
)
S3_ENDPOINT = os.environ.get(
    "DATAFLOW_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000"
)
MAPPINGS = [
    {"source": "id", "target": "id", "transform": "direct"},
    {"source": "v", "target": "v", "transform": "direct"},
]
COLUMN_TYPES = {"id": "string", "v": "string", "_df_lsn": "string"}


def _rest_reachable() -> bool:
    try:
        for url, default_port in ((REST_URI, 8181), (S3_ENDPOINT, 9000)):
            host = url.split("://", 1)[-1].split("/", 1)[0]
            hostname, _, port_s = host.partition(":")
            with socket.create_connection(
                (hostname, int(port_s or str(default_port))), timeout=1.5
            ):
                pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as response:
            return int(getattr(response, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(),
    reason=f"Iceberg REST or S3 endpoint is not reachable: {REST_URI}, {S3_ENDPOINT}",
)


def _adapter() -> Any:
    if iceberg_eos is None:
        pytest.fail("connectors.iceberg_eos is not available")
    return iceberg_eos


def _install_rest_loader(monkeypatch: pytest.MonkeyPatch) -> Any:
    adapter = _adapter()
    import connectors.iceberg_catalog as catalog_module

    from connectors.iceberg_catalog import parse_iceberg_catalog_config
    from pyiceberg.catalog.rest import RestCatalog

    def load_catalog(endpoint: Any) -> Any:
        config = parse_iceberg_catalog_config(endpoint)
        properties = dict(config["properties"])
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
        return RestCatalog(config["catalog_name"], **properties)

    monkeypatch.setattr(adapter, "load_catalog", load_catalog)
    monkeypatch.setattr(catalog_module, "load_catalog", load_catalog)
    return load_catalog


@pytest.fixture
def rest_dest(monkeypatch: pytest.MonkeyPatch) -> Any:
    load_catalog = _install_rest_loader(monkeypatch)
    from pyiceberg.exceptions import NamespaceAlreadyExistsError
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType

    table_name = f"eos_{uuid.uuid4().hex[:12]}"
    config = {
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": table_name,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    }
    catalog = load_catalog(config)
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass
    identifier = ("default", table_name)
    catalog.create_table(
        identifier,
        schema=Schema(
            NestedField(
                field_id=1, name="id", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=2, name="v", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3,
                name="_df_lsn",
                field_type=StringType(),
                required=False,
            ),
        ),
    )
    yield config
    catalog.drop_table(identifier)


@pytest.fixture
def local_sql_dest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    adapter = _adapter()
    from connectors.iceberg_catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType

    monkeypatch.setattr(adapter, "load_catalog", load_catalog)
    config = {
        "connection_string": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": str(tmp_path / "warehouse"),
        "table": "eos_expiration",
        "schema": "default",
        "extra": {"catalog_type": "sql"},
    }
    catalog = load_catalog(config)
    catalog.create_namespace("default")
    catalog.create_table(
        ("default", "eos_expiration"),
        schema=Schema(
            NestedField(
                field_id=1, name="id", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=2, name="v", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3,
                name="_df_lsn",
                field_type=StringType(),
                required=False,
            ),
        ),
    )
    return config


def _load_table(config: dict[str, Any]) -> Any:
    from connectors.iceberg_catalog import load_catalog

    catalog = load_catalog(config)
    identifier = (config.get("schema") or "default", config["table"])
    return catalog.load_table(identifier)


def _batch(
    lsn: str,
    *,
    inserts: list[dict[str, Any]] | None = None,
    updates: list[dict[str, Any]] | None = None,
    deletes: list[str] | None = None,
    resume_token: Any = None,
) -> ChangeBatch:
    token = {"lsn": lsn} if resume_token is None else dict(resume_token)
    token.setdefault("lsn", lsn)
    return ChangeBatch(
        inserts=list(inserts or []),
        updates=list(updates or []),
        deletes=list(deletes or []),
        resume_token=token,
    )


def _apply(
    config: dict[str, Any],
    *,
    lsn: str,
    stream_key: str = "test-stream",
    inserts: list[dict[str, Any]] | None = None,
    updates: list[dict[str, Any]] | None = None,
    deletes: list[str] | None = None,
    writer_fence: int = 1,
    crash_after: str | None = None,
    resume_token: Any = None,
) -> Any:
    return _adapter().apply_eos_iceberg(
        dest_type="iceberg",
        dest_cfg=config,
        dest_table=config["table"],
        change=_batch(
            lsn,
            inserts=inserts,
            updates=updates,
            deletes=deletes,
            resume_token=resume_token,
        ),
        mappings=MAPPINGS,
        column_types=COLUMN_TYPES,
        pk_target_cols=["id"],
        stream_key=stream_key,
        incoming_lsn=lsn,
        batch_id=f"batch-{uuid.uuid4().hex[:8]}",
        writer_fence=writer_fence,
        crash_after=crash_after,
    )


def _prefix(stream_key: str) -> str:
    digest = hashlib.sha256(stream_key.encode("utf-8")).hexdigest()[:16]
    return f"dataflow.eos.{digest}."


def _summary(snapshot: Any) -> dict[str, str]:
    summary = getattr(snapshot, "summary", None)
    return summary.model_dump() if summary is not None else {}


@requires_rest
def test_apply_mirrors_watermark_properties_and_snapshot_summary(
    rest_dest: dict[str, Any],
) -> None:
    result = _apply(
        rest_dest,
        lsn="0/10",
        inserts=[{"id": "1", "v": "a"}],
    )
    assert result.status == "applied"
    table = _load_table(rest_dest)
    prefix = _prefix("test-stream")
    required = {
        "stream_key",
        "committed_lsn",
        "lsn_family",
        "epoch",
        "fence_epoch",
        "apply_seq",
        "phase",
        "apply_checksum",
        "resume_blob",
        "window_id",
        "snapshot_signal_id",
        "window_hi_pk",
        "batch_id",
        "dest_object",
        "algorithm",
    }
    properties = dict(table.metadata.properties)
    assert {name for name in required if prefix + name in properties} == required
    latest = _summary(table.metadata.snapshots[-1])
    for name in required:
        assert latest[prefix + name] == properties[prefix + name]
    assert latest["dataflow.commit-id"]
    assert latest["dataflow.operation"] == "eos_apply"
    assert "commit.retry.num-retries" not in properties


@requires_rest
def test_identical_lsn_redelivery_skips_snapshot_and_mismatch_is_refused(
    rest_dest: dict[str, Any],
) -> None:
    _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "a"}])
    before = len(_load_table(rest_dest).metadata.snapshots)
    replay = _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "a"}])
    assert replay.status == "already_committed"
    assert len(_load_table(rest_dest).metadata.snapshots) == before
    with pytest.raises(ExactlyOnceRouteError, match="checksum"):
        _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "different"}])


@requires_rest
def test_older_lsn_is_skipped_without_snapshot(rest_dest: dict[str, Any]) -> None:
    _apply(rest_dest, lsn="0/20", inserts=[{"id": "1", "v": "new"}])
    before = len(_load_table(rest_dest).metadata.snapshots)
    result = _apply(rest_dest, lsn="0/10", inserts=[{"id": "2", "v": "old"}])
    assert result.status in {"stream_wins_skip", "already_committed"}
    assert len(_load_table(rest_dest).metadata.snapshots) == before


@requires_rest
def test_cross_family_lsn_is_refused(rest_dest: dict[str, Any]) -> None:
    _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "a"}])
    with pytest.raises(ExactlyOnceRouteError):
        _apply(
            rest_dest,
            lsn="mysql-bin.000001:4",
            inserts=[{"id": "2", "v": "b"}],
        )


@requires_rest
def test_stale_writer_fence_is_refused(rest_dest: dict[str, Any]) -> None:
    _apply(
        rest_dest,
        lsn="0/10",
        inserts=[{"id": "1", "v": "a"}],
        writer_fence=3,
    )
    with pytest.raises(ExactlyOnceRouteError, match="fence"):
        _apply(
            rest_dest,
            lsn="0/11",
            inserts=[{"id": "2", "v": "b"}],
            writer_fence=2,
        )


@requires_rest
def test_open_raises_fence_with_snapshot_and_resume_from_properties(
    rest_dest: dict[str, Any],
) -> None:
    token = {"lsn": "0/10", "slot": "slot-a", "phase": "streaming"}
    _apply(
        rest_dest,
        lsn="0/10",
        inserts=[{"id": "1", "v": "a"}],
        resume_token=token,
    )
    before = len(_load_table(rest_dest).metadata.snapshots)
    opened = _adapter().open_iceberg_eos_session(
        dest_type="iceberg",
        dest_cfg=rest_dest,
        stream_key="test-stream",
        incoming_fence=2,
        job_resume=None,
    )
    table = _load_table(rest_dest)
    assert opened.resume["lsn"] == "0/10"
    assert opened.fence_epoch == 2
    assert len(table.metadata.snapshots) == before + 1
    assert table.metadata.properties[_prefix("test-stream") + "fence_epoch"] == "2"
    assert _summary(table.metadata.snapshots[-1])["dataflow.operation"] == "eos_open"


@requires_rest
def test_zombie_writer_redecides_after_fence_steal(
    rest_dest: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = _adapter()
    original_scan = adapter._scan_dest_rows
    fired = False

    def steal_fence(tbl: Any, pk_cols: list[str], keys: set[str]) -> Any:
        nonlocal fired
        if not fired:
            fired = True
            _apply(
                rest_dest,
                lsn="0/20",
                inserts=[{"id": "2", "v": "new-owner"}],
                writer_fence=2,
            )
        return original_scan(tbl, pk_cols, keys)

    monkeypatch.setattr(adapter, "_scan_dest_rows", steal_fence)
    with pytest.raises(ExactlyOnceRouteError, match="fence"):
        _apply(
            rest_dest,
            lsn="0/10",
            inserts=[{"id": "1", "v": "zombie"}],
            writer_fence=1,
        )
    rows = _load_table(rest_dest).scan().to_arrow().to_pylist()
    assert [(row["id"], row["v"]) for row in rows] == [("2", "new-owner")]


@requires_rest
def test_concurrent_same_stream_apply_keeps_one_row(
    rest_dest: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    adapter = _adapter()
    original_decide = adapter.decide_from_view
    barrier = threading.Barrier(2)
    calls = 0
    lock = threading.Lock()
    outcomes: list[Any] = []
    errors: list[BaseException] = []

    def synchronize_first_decisions(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        result = original_decide(*args, **kwargs)
        with lock:
            calls += 1
            should_wait = calls <= 2
        if should_wait:
            barrier.wait(timeout=20)
        return result

    monkeypatch.setattr(adapter, "decide_from_view", synchronize_first_decisions)

    def apply() -> None:
        try:
            outcomes.append(
                _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "same"}])
            )
        except BaseException as exc:
            errors.append(exc)

    workers = [threading.Thread(target=apply) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=30)
    assert not any(worker.is_alive() for worker in workers)
    assert not errors
    assert len(outcomes) == 2
    rows = _load_table(rest_dest).scan().to_arrow().to_pylist()
    assert [(row["id"], row["v"]) for row in rows] == [("1", "same")]
    assert sum(outcome.status == "applied" for outcome in outcomes) == 1


@requires_rest
@pytest.mark.parametrize(
    "crash_after",
    ["after_apply_before_watermark", "after_watermark_before_commit"],
)
def test_crash_hooks_leave_table_unchanged(
    rest_dest: dict[str, Any], crash_after: str
) -> None:
    table = _load_table(rest_dest)
    before_snapshots = len(table.metadata.snapshots)
    with pytest.raises(EosCrash):
        _apply(
            rest_dest,
            lsn="0/10",
            inserts=[{"id": "1", "v": "uncommitted"}],
            crash_after=crash_after,
        )
    table = _load_table(rest_dest)
    assert len(table.metadata.snapshots) == before_snapshots
    assert not table.metadata.properties.get(_prefix("test-stream") + "stream_key")
    assert table.scan().to_arrow().num_rows == 0


@requires_rest
def test_lsn_guarded_delete_keeps_newer_row(rest_dest: dict[str, Any]) -> None:
    _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "newer"}])
    result = _apply(
        rest_dest,
        stream_key="independent-delete-stream",
        lsn="0/9",
        deletes=["1"],
    )
    assert result.status == "empty"
    rows = _load_table(rest_dest).scan().to_arrow().to_pylist()
    assert [(row["id"], row["v"], row["_df_lsn"]) for row in rows] == [
        ("1", "newer", "0/10")
    ]


@requires_rest
def test_corrupt_watermark_properties_raise_typed_error(
    rest_dest: dict[str, Any],
) -> None:
    error_type = getattr(_adapter(), "IcebergEosWatermarkError")
    table = _load_table(rest_dest)
    txn = table.transaction()
    txn.set_properties({_prefix("test-stream") + "stream_key": "test-stream"})
    txn.commit_transaction()
    with pytest.raises(error_type):
        _adapter().iceberg_dest_watermark_view(rest_dest, "test-stream")


@requires_rest
def test_blank_resume_never_changes_fence(rest_dest: dict[str, Any]) -> None:
    _apply(rest_dest, lsn="0/10", inserts=[{"id": "1", "v": "a"}], writer_fence=4)
    adapter = _adapter()
    assert adapter.iceberg_blank_eos_resume(
        rest_dest, "test-stream", lsn="0/10"
    )
    view = adapter.iceberg_dest_watermark_view(rest_dest, "test-stream")
    assert view.committed_lsn is None
    assert view.resume_blob == ""
    assert view.fence_epoch == 4


def test_watermark_survives_snapshot_expiration(
    local_sql_dest: dict[str, Any],
) -> None:
    _apply(local_sql_dest, lsn="0/10", inserts=[{"id": "1", "v": "a"}])
    _apply(local_sql_dest, lsn="0/11", updates=[{"id": "1", "v": "b"}])
    table = _load_table(local_sql_dest)
    latest = table.current_snapshot()
    assert latest is not None
    table.maintenance.expire_snapshots().older_than(
        datetime.now(timezone.utc)
    ).commit()
    view = _adapter().iceberg_dest_watermark_view(
        local_sql_dest, "test-stream"
    )
    assert view.committed_lsn == "0/11"
    assert view.fence_epoch == 1


def test_filesystem_iceberg_is_refused(tmp_path: Path) -> None:
    config = {"warehouse": str(tmp_path), "table": "default.orders"}
    with pytest.raises(ExactlyOnceRouteError, match="catalog-backed"):
        _adapter().iceberg_dest_watermark_view(config, "test-stream")


@requires_rest
def test_exactly_once_dispatch_applies_to_catalog_iceberg(
    rest_dest: dict[str, Any],
) -> None:
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once

    _rows, _checksum, summary, _deleted = apply_change_batch_exactly_once(
        dest_type="iceberg",
        dest_cfg=rest_dest,
        dest_table=rest_dest["table"],
        change=_batch("0/10", inserts=[{"id": "1", "v": "a"}]),
        mappings=MAPPINGS,
        column_types=COLUMN_TYPES,
        headers=["id", "v"],
        pk_target_cols=["id"],
        cursor_key="dispatch-test",
    )
    assert summary["eos_status"] == "applied"
    assert _load_table(rest_dest).scan().to_arrow().to_pylist()[0]["v"] == "a"


@pytest.mark.parametrize(
    "dest_type", ["iceberg", "apache_iceberg", "iceberg_rest", "nessie"]
)
def test_direct_dispatch_aliases_refuse_filesystem_catalog(
    tmp_path: Path, dest_type: str
) -> None:
    from connectors.cdc_eos_sql import (
        apply_change_batch_exactly_once,
        blank_route_dest_resume,
        open_eos_session,
        read_route_dest_lsn,
        read_route_dest_resume,
    )

    config = {"warehouse": str(tmp_path), "table": "default.orders"}
    change = _batch("0/10", inserts=[{"id": "1", "v": "a"}])
    with pytest.raises(ExactlyOnceRouteError, match="catalog-backed"):
        apply_change_batch_exactly_once(
            dest_type=dest_type,
            dest_cfg=config,
            dest_table="default.orders",
            change=change,
            mappings=MAPPINGS,
            column_types=COLUMN_TYPES,
            headers=["id", "v"],
            pk_target_cols=["id"],
        )
    with pytest.raises(ExactlyOnceRouteError, match="catalog-backed"):
        open_eos_session(
            dest_type=dest_type,
            dest_cfg=config,
            stream_key="test-stream",
        )
    with pytest.raises(ExactlyOnceRouteError, match="catalog-backed"):
        read_route_dest_lsn(dest_type, config, "test-stream")
    with pytest.raises(ExactlyOnceRouteError, match="catalog-backed"):
        read_route_dest_resume(dest_type, config, "test-stream")
    assert not blank_route_dest_resume(
        dest_type, config, "test-stream", lsn="0/10"
    )


def test_iceberg_bundle_dispatch_refuses_multi_table_commit() -> None:
    from connectors.cdc_eos_sql import apply_eos_bundle

    with pytest.raises(ExactlyOnceRouteError, match="multi-table bundle"):
        apply_eos_bundle(dest_type="iceberg", dest_cfg={}, streams=[])


def test_registry_describes_filesystem_and_catalog_delete_paths() -> None:
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    iceberg = CAPABILITY_REGISTRY["iceberg"]
    assert iceberg["supports_cdc"] is False
    assert "filesystem" in iceberg["common_issues"][1].lower()
    assert "catalog writes use copy-on-write" in iceberg["common_issues"][1]
    assert "filesystem-equality-delete" in iceberg["write_strategy"]
    assert "catalog-copy-on-write" in iceberg["write_strategy"]
