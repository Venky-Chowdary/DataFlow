"""Route certification data cases — live SQL routes, independent read-back.

Each route × sync-mode cell runs the real engine with preflight on, then reads
the destination with its own driver (``route_cert_readback``) and compares it
with the rows the source holds. Evidence is recorded only for what the run
actually proved; a skip names the unreachable endpoint.

Data cases: nulls, numeric boundaries, timestamps and Unicode on overwrite;
composite and duplicate keys on upsert; schema drift on an overwrite rerun
that adds a source column. Kafka destinations refuse overwrite and upsert;
those cells prove the refusal names its reason and writes nothing.
"""

from __future__ import annotations

import copy
import dataclasses
import re
import uuid
from pathlib import Path
from typing import Any

import pytest

from services.route_certification import RouteEvidence
from src.transfer.adapters import write_destination_database
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from src.transfer.registry import PRODUCTION_SKU

from tests.route_cert_readback import canonical_rows, read_back
from tests.test_execute_tracked_universal_matrix import (
    _build_destination,
    _build_source,
    _endpoint_reachable,
)

SQL_FORMATS = ("sqlite", "postgresql", "mysql", "sqlserver", "oracle")
SQL_ROUTES = sorted(
    {
        r
        for r in PRODUCTION_SKU
        if r[0] == "database"
        and r[2] == "database"
        and r[1] in SQL_FORMATS
        and r[3] in SQL_FORMATS
    }
)
KAFKA_REFUSAL_ROUTES = sorted(
    {r for r in PRODUCTION_SKU if r[2] == "database" and r[3] == "kafka"}
)

TYPED_COLUMNS = ["id", "amount", "big", "note", "created_at"]
TYPED_SCHEMA = {
    "id": "INTEGER",
    "amount": "DECIMAL(18,6)",
    "big": "BIGINT",
    "note": "VARCHAR(200)",
    "created_at": "TIMESTAMP",
}
TYPED_RECORDS = [
    {
        "id": "1",
        "amount": "99999999999.999999",
        "big": "9223372036854775807",
        "note": "plain ascii",
        "created_at": "2024-02-29 23:59:59.123456",
    },
    {
        "id": "2",
        "amount": "-99999999999.999999",
        "big": "-9223372036854775808",
        "note": "Grüße — 東京 — Ωμέγα — 🚀",
        "created_at": "1970-01-01 00:00:00",
    },
    {
        "id": "3",
        "amount": "0.000001",
        "big": "0",
        "note": None,
        "created_at": None,
    },
    {
        "id": "4",
        "amount": None,
        "big": None,
        "note": "",
        "created_at": "2038-01-19 03:14:08.000001",
    },
]

KEY_COLUMNS = ["region", "id", "qty"]
KEY_SCHEMA = {"region": "VARCHAR(16)", "id": "INTEGER", "qty": "INTEGER"}
KEY_RUN1 = [
    {"region": "eu", "id": "1", "qty": "10"},
    {"region": "us", "id": "1", "qty": "20"},
    {"region": "eu", "id": "2", "qty": "30"},
]
KEY_RUN2 = [
    {"region": "eu", "id": "1", "qty": "11"},
    {"region": "us", "id": "1", "qty": "20"},
    {"region": "eu", "id": "2", "qty": "30"},
    {"region": "us", "id": "2", "qty": "40"},
]


def _route_id(route: tuple[str, str, str, str]) -> str:
    return f"{route[1]}_to_{route[3]}"


def _mappings(columns: list[str]) -> list[dict[str, Any]]:
    return [{"source": c, "target": c, "confidence": 0.99} for c in columns]


def _endpoints(route: tuple[str, str, str, str], tmp_path: Path, local_object_store: str, local_sftp: Any):
    suffix = uuid.uuid4().hex[:10]
    source, _, _ = _build_source(
        route[0], route[1], tmp_path, suffix, object_store=local_object_store, sftp_server=local_sftp
    )
    destination = _build_destination(
        route[2], route[3], tmp_path, suffix, object_store=local_object_store, sftp_server=local_sftp
    )
    if not _endpoint_reachable(source):
        pytest.skip(f"source {route[1]} not reachable")
    if not _endpoint_reachable(destination):
        pytest.skip(f"destination {route[3]} not reachable")
    return source, destination


def _fresh_table(source: Any, tag: str) -> Any:
    """Same source connector, new table: each run reads exactly one fixture."""
    fresh = copy.copy(source)
    fresh.table = f"{source.table}_{tag}"
    return fresh


def _seed(
    source: Any,
    records: list[dict],
    columns: list[str],
    schema: dict[str, str],
    *,
    expect: list[dict] | None = None,
) -> None:
    rows, _, summary = write_destination_database(
        source, records, columns, schema, _mappings(columns)
    )
    if rows != len(records):
        pytest.fail(f"source fixture wrote {rows}/{len(records)} rows: {summary}")
    at_source = read_back(source, columns)
    want = canonical_rows(expect if expect is not None else records, columns)
    if at_source is not None and canonical_rows(at_source, columns) != want:
        pytest.fail(f"source {source.format} did not store the fixture verbatim: {at_source}")


def _oracle_text(records: list[dict], *ends: Any) -> tuple[list[dict], bool]:
    """Oracle stores '' as NULL. Model that instead of crediting the nulls case."""
    if not any(getattr(e, "format", "") == "oracle" for e in ends):
        return records, True
    return [{k: (None if v == "" else v) for k, v in r.items()} for r in records], False


def _run(request: TransferRequest):
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _at_rest(destination: Any, columns: list[str]) -> list[tuple]:
    rows = read_back(destination, columns)
    assert rows is not None, f"{destination.format}: no independent read-back"
    return canonical_rows(rows, columns)


@pytest.mark.parametrize(
    "route",
    [
        pytest.param(r, marks=pytest.mark.route_cert(route=r, sync_mode="full_refresh_overwrite"), id=_route_id(r))
        for r in SQL_ROUTES
    ],
)
def test_overwrite_typed_cases(route, tmp_path, local_object_store, local_sftp, record_property):
    evidence = RouteEvidence(record_property, route, "full_refresh_overwrite")
    source, destination = _endpoints(route, tmp_path, local_object_store, local_sftp)
    typed, nulls_distinct = _oracle_text(TYPED_RECORDS, source, destination)
    at_source, _ = _oracle_text(TYPED_RECORDS, source)
    _seed(source, TYPED_RECORDS, TYPED_COLUMNS, TYPED_SCHEMA, expect=at_source)
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
        mappings=_mappings(TYPED_COLUMNS),
    )
    result = _run(request)
    assert result.success, f"{route}: {result.error}"
    assert result.records_transferred == len(TYPED_RECORDS)
    evidence.prove("preflight", "write")

    expected = canonical_rows(typed, TYPED_COLUMNS)
    assert _at_rest(destination, TYPED_COLUMNS) == expected
    cases = ("numeric_boundaries", "timestamps", "unicode")
    evidence.prove("independent_readback", cases=(("nulls",) if nulls_distinct else ()) + cases)

    rerun = _run(request)
    assert rerun.success, f"{route} rerun: {rerun.error}"
    assert _at_rest(destination, TYPED_COLUMNS) == expected
    evidence.prove("rerun")

    # Schema drift: the source gains a column; the acknowledged overwrite
    # rerun must carry it to the destination without losing the other rows.
    drift_columns = [*TYPED_COLUMNS, "added"]
    drift_schema = {**TYPED_SCHEMA, "added": "VARCHAR(20)"}
    drift_records = [{**r, "added": f"v{r['id']}"} for r in typed]
    drift_source = _fresh_table(source, "d")
    _seed(drift_source, drift_records, drift_columns, drift_schema)
    drifted = _run(
        TransferRequest(
            source=drift_source,
            destination=destination,
            sync_mode="full_refresh_overwrite",
            skip_preflight=False,
            validation_mode="strict",
            mappings=_mappings(drift_columns),
            schema_policy="propagate_columns",
            schema_drift_acknowledged=True,
        )
    )
    if destination.format == "sqlite":
        # The SQLite overwrite stage refuses a column the live table lacks.
        # Fail-closed with the prior rows untouched; schema_drift stays unproven.
        assert not drifted.success
        assert "missing mapped field(s) 'added'" in (drifted.error or "")
        assert drifted.records_transferred == 0
        assert _at_rest(destination, TYPED_COLUMNS) == expected
        return
    assert drifted.success, f"{route} schema drift: {drifted.error}"
    assert _at_rest(destination, drift_columns) == canonical_rows(drift_records, drift_columns)
    evidence.prove(cases=("schema_drift",))


@pytest.mark.parametrize(
    "route",
    [
        pytest.param(r, marks=pytest.mark.route_cert(route=r, sync_mode="upsert"), id=_route_id(r))
        for r in SQL_ROUTES
    ],
)
def test_upsert_composite_and_duplicate_keys(route, tmp_path, local_object_store, local_sftp, record_property):
    evidence = RouteEvidence(record_property, route, "upsert")
    source, destination = _endpoints(route, tmp_path, local_object_store, local_sftp)

    def request(source: Any) -> TransferRequest:
        return TransferRequest(
            source=source,
            destination=destination,
            sync_mode="upsert",
            skip_preflight=False,
            validation_mode="strict",
            mappings=_mappings(KEY_COLUMNS),
            stream_contracts=[
                {
                    "name": str(source.table),
                    "primary_key": ["region", "id"],
                    "sync_mode": "upsert",
                    "selected": True,
                }
            ],
            ddl_changes_acknowledged=True,
        )

    _seed(source, KEY_RUN1, KEY_COLUMNS, KEY_SCHEMA)
    first = _run(request(source))
    assert first.success, f"{route}: {first.error}"
    evidence.prove("preflight", "write")
    assert _at_rest(destination, KEY_COLUMNS) == canonical_rows(KEY_RUN1, KEY_COLUMNS)
    evidence.prove("independent_readback", cases=("composite_keys",))

    # Rerun: one key changes, one is new, the rest repeat. Repeated keys must
    # update in place, never land twice.
    second_source = _fresh_table(source, "r2")
    _seed(second_source, KEY_RUN2, KEY_COLUMNS, KEY_SCHEMA)
    second = _run(request(second_source))
    assert second.success, f"{route} rerun: {second.error}"
    assert _at_rest(destination, KEY_COLUMNS) == canonical_rows(KEY_RUN2, KEY_COLUMNS)
    evidence.prove("rerun", cases=("duplicate_keys",))


@pytest.mark.parametrize(
    "route",
    [
        pytest.param(r, marks=pytest.mark.route_cert(route=r, sync_mode="full_refresh_append"), id=_route_id(r))
        for r in SQL_ROUTES
    ],
)
def test_append_rerun_adds_only_new_rows(route, tmp_path, local_object_store, local_sftp, record_property):
    evidence = RouteEvidence(record_property, route, "full_refresh_append")
    source, destination = _endpoints(route, tmp_path, local_object_store, local_sftp)
    columns = ["id", "qty"]
    schema = {"id": "INTEGER", "qty": "INTEGER"}
    batch1 = [{"id": "1", "qty": "1"}, {"id": "2", "qty": None}]
    batch2 = [{"id": "3", "qty": "3"}]

    def request(source: Any) -> TransferRequest:
        return TransferRequest(
            source=source,
            destination=destination,
            sync_mode="full_refresh_append",
            skip_preflight=False,
            validation_mode="strict",
            mappings=_mappings(columns),
        )

    _seed(source, batch1, columns, schema)
    first = _run(request(source))
    assert first.success, f"{route}: {first.error}"
    evidence.prove("preflight", "write")
    assert _at_rest(destination, columns) == canonical_rows(batch1, columns)
    evidence.prove("independent_readback")

    second_source = _fresh_table(source, "r2")
    _seed(second_source, batch2, columns, schema)
    second = _run(request(second_source))
    assert second.success, f"{route} rerun: {second.error}"
    assert _at_rest(destination, columns) == canonical_rows(batch1 + batch2, columns)
    evidence.prove("rerun")


def _kafka_message_count(destination: Any) -> int:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=f"{destination.host}:{destination.port}",
        consumer_timeout_ms=2000,
    )
    try:
        partitions = consumer.partitions_for_topic(str(destination.table)) or set()
        tps = [TopicPartition(str(destination.table), p) for p in partitions]
        if not tps:
            return 0
        ends = consumer.end_offsets(tps)
        starts = consumer.beginning_offsets(tps)
        return sum(ends[tp] - starts[tp] for tp in tps)
    finally:
        consumer.close()


@pytest.mark.parametrize(
    ("route", "mode"),
    [
        pytest.param(r, m, marks=pytest.mark.route_cert(route=r, sync_mode=m), id=f"{_route_id(r)}_{m}")
        for r in KAFKA_REFUSAL_ROUTES
        for m in ("full_refresh_overwrite", "upsert")
    ],
)
def test_kafka_refuses_overwrite_and_upsert_with_zero_writes(
    route, mode, tmp_path, local_object_store, local_sftp, record_property
):
    evidence = RouteEvidence(record_property, route, mode)
    suffix = uuid.uuid4().hex[:10]
    source, content, filename = _build_source(
        route[0], route[1], tmp_path, suffix, object_store=local_object_store, sftp_server=local_sftp
    )
    destination = _build_destination(
        route[2], route[3], tmp_path, suffix, object_store=local_object_store, sftp_server=local_sftp
    )
    if not _endpoint_reachable(destination):
        pytest.skip(f"destination {route[3]} not reachable")
    columns = ["id", "amount"]
    result = _run(
        TransferRequest(
            source=source,
            destination=destination,
            source_content=content,
            source_filename=filename,
            sync_mode=mode,
            skip_preflight=False,
            validation_mode="strict",
            mappings=_mappings(columns),
            stream_contracts=[
                {"name": filename, "primary_key": ["id"], "sync_mode": mode, "selected": True}
            ],
        )
    )
    assert not result.success, f"{route} {mode}: Kafka accepted a non-append write"
    reason = str(result.error or "")
    assert "append" in reason.lower(), f"{route} {mode}: refusal names no reason: {reason!r}"
    assert result.records_transferred == 0
    written = _kafka_message_count(destination)
    assert written == 0, f"{route} {mode}: refused run still published {written} message(s)"
    evidence.refused(reason.splitlines()[0][:200], zero_writes=True)


CANCEL_COLUMNS = ["id", "qty"]
CANCEL_SCHEMA = {"id": "INTEGER", "qty": "INTEGER"}
CANCEL_RECORDS = [{"id": str(i), "qty": str(i * 10)} for i in range(1, 31)]
CANCEL_MODES = ("full_refresh_overwrite", "full_refresh_append", "upsert")


_ABSENT_TABLE_MARKERS = ("does not exist", "no such table", "doesn't exist", "ORA-00942", "Invalid object name")


def _rows_or_none_if_absent(destination: Any, columns: list[str]) -> list[dict]:
    """Rows at rest; a table the cancel removed (rolled back with it) holds none."""
    try:
        rows = read_back(destination, columns)
    except Exception as exc:  # noqa: BLE001 - only a missing table is tolerated
        if any(marker in str(exc) for marker in _ABSENT_TABLE_MARKERS):
            return []
        raise
    assert rows is not None, f"{destination.format}: no independent read-back"
    return rows


@pytest.fixture()
def cancellable_store(monkeypatch):
    """Durable job store for the resume checkpoint; small pages so a cancel lands mid-run."""
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 5)
    return fake


@pytest.mark.parametrize(
    ("route", "mode"),
    [
        pytest.param(r, m, marks=pytest.mark.route_cert(route=r, sync_mode=m), id=f"{_route_id(r)}-{m}")
        for r in SQL_ROUTES
        for m in CANCEL_MODES
    ],
)
def test_cancel_then_resume(
    route, mode, tmp_path, local_object_store, local_sftp, record_property, cancellable_store, monkeypatch
):
    import services.cancel_check as cancel_check

    evidence = RouteEvidence(record_property, route, mode)
    source, destination = _endpoints(route, tmp_path, local_object_store, local_sftp)
    _seed(source, CANCEL_RECORDS, CANCEL_COLUMNS, CANCEL_SCHEMA)
    # Oracle folds unquoted identifiers: the filter names the column as stored.
    key = "ID" if source.format == "oracle" else "id"
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode=mode,
        skip_preflight=False,
        validation_mode="strict",
        mappings=_mappings(CANCEL_COLUMNS),
        stream_contracts=[
            {"name": str(source.table), "primary_key": ["id"], "sync_mode": mode, "selected": True}
        ],
        # Pass-through filter keeps the batched row path, which has cancel points.
        source_filter={"column": key, "op": "gt", "value": 0},
    )
    job_id = uuid.uuid4().hex[:24]
    cancellable_store.update_job_status(job_id, "pending", transfer_request={})
    probes = {"n": 0}

    def cancel_after_a_few_batches(_mongo, _job_id):
        probes["n"] += 1
        return probes["n"] >= 4

    with monkeypatch.context() as m:
        m.setattr(cancel_check, "job_cancel_requested", cancel_after_a_few_batches)
        cancelled = UniversalTransferEngine().execute_tracked(request, job_id)
    assert not cancelled.success, f"{route} {mode}: cancel never landed"
    assert "cancel" in str(cancelled.error).lower(), cancelled.error
    evidence.prove("preflight")

    want = canonical_rows(CANCEL_RECORDS, CANCEL_COLUMNS)
    kept = _rows_or_none_if_absent(destination, CANCEL_COLUMNS)
    kept_rows = canonical_rows(kept, CANCEL_COLUMNS)
    # A cancel keeps a committed prefix or nothing: never a duplicate, never a
    # row the source lacks, and the job's count matches the destination.
    assert len(kept_rows) == len(set(kept_rows)), kept_rows
    assert set(kept_rows) <= set(want), kept_rows
    assert len(kept_rows) < len(want), f"{route} {mode}: cancel wrote every row"
    job = cancellable_store.jobs[job_id]
    assert job.get("status") == "cancelled", job
    assert int(job.get("records_processed") or 0) == len(kept_rows), (job, len(kept_rows))
    message = str(job.get("message") or "")
    stated = re.search(r"after (?:at least )?(\d+) row\(s\)", message)
    if stated is None:
        assert not kept_rows and "No row of this run was kept" in message, message
    else:
        assert int(stated.group(1)) == len(kept_rows), message
    evidence.prove(cases=("cancellation",))

    cancellable_store.update_job_status(job_id, "pending", message="resume", allow_terminal_exit=True)
    resumed = UniversalTransferEngine().execute_tracked(request, job_id, resume=True)
    assert resumed.success, (
        f"{route} {mode} resume: {resumed.error}; at rest: "
        f"{_rows_or_none_if_absent(destination, CANCEL_COLUMNS)}"
    )
    evidence.prove("write")
    assert _at_rest(destination, CANCEL_COLUMNS) == want
    evidence.prove("independent_readback", cases=("resume",))

    # Rerun: an append adds only rows it has not stored; the others converge.
    tail = [{"id": "31", "qty": "310"}] if mode == "full_refresh_append" else CANCEL_RECORDS
    rerun_source = _fresh_table(source, "r2")
    _seed(rerun_source, tail, CANCEL_COLUMNS, CANCEL_SCHEMA)
    rerun = _run(dataclasses.replace(request, source=rerun_source))
    assert rerun.success, f"{route} {mode} rerun: {rerun.error}"
    expected = CANCEL_RECORDS + (tail if mode == "full_refresh_append" else [])
    assert _at_rest(destination, CANCEL_COLUMNS) == canonical_rows(expected, CANCEL_COLUMNS)
    evidence.prove("rerun")
