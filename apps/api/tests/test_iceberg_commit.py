"""Iceberg commit layer: optimistic retries and fail-closed unknown outcomes.

pyiceberg retries a failed commit internally by re-applying the *same* staged
updates. An upsert or an LSN-guarded delete decided against stale metadata is
then replayed against a table another writer already changed, so the retry can
resurrect a deleted key or drop a concurrent update. These proofs pin the
application-level layer that re-decides every attempt from fresh metadata.
"""

from __future__ import annotations

import logging
import os
import socket
import sys
import threading
import uuid
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")

from pyiceberg.exceptions import (  # noqa: E402
    CommitFailedException,
    CommitStateUnknownException,
)

from connectors.iceberg_commit import (  # noqa: E402
    COMMIT_ID_PROPERTY,
    COMMIT_OPERATION_PROPERTY,
    CommitRetryPolicy,
    IcebergCommitConflictError,
    IcebergCommitStateUnknownError,
    commit_with_retry,
)
from connectors.iceberg_writer import (  # noqa: E402
    delete_by_primary_keys,
    write_mapped_rows,
)

MAPPINGS = [
    {"source": "id", "target": "id", "transform": "direct"},
    {"source": "v", "target": "v", "transform": "direct"},
]


def _sql_catalog(tmp_path: Path) -> tuple[str, str]:
    """Return (warehouse, sqlite catalog uri) — same shape as test_iceberg_upsert."""
    return str(tmp_path / "wh"), f"sqlite:///{tmp_path / 'catalog.db'}"


def _seed_table(tmp_path: Path, rows: list[list[str]]) -> tuple[str, str]:
    warehouse, uri = _sql_catalog(tmp_path)
    written = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name="default.orders",
        headers=["id", "v"],
        data_rows=rows,
        mappings=MAPPINGS,
        write_mode="append",
        create_table=True,
    )
    assert written.ok, written.error
    return warehouse, uri


def _load_orders(warehouse: str, uri: str) -> Any:
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("dataflow", uri=uri, warehouse=Path(warehouse).resolve().as_uri())
    return catalog, catalog.load_table(("default", "orders"))


def _summary(snapshot: Any) -> dict[str, str]:
    props = dict(getattr(snapshot.summary, "additional_properties", None) or {})
    operation = getattr(snapshot.summary, "operation", None)
    if operation is not None:
        props.setdefault("operation", str(operation))
    return props


def _rows_by_pk(warehouse: str, uri: str) -> dict[str, str]:
    _catalog, table = _load_orders(warehouse, uri)
    arrow = table.scan().to_arrow()
    ids = arrow.column("id").to_pylist()
    values = arrow.column("v").to_pylist()
    assert len(ids) == len(set(ids)), f"duplicate primary keys: {ids}"
    return dict(zip(ids, values))


class _FakeTransaction:
    """Stand-in transaction whose commit outcome the test scripts."""

    def __init__(self, outcomes: list[Any], committed_table: Any) -> None:
        self._outcomes = outcomes
        self._committed_table = committed_table
        self.built_with: list[Any] = []

    def __call__(self, table: Any, autocommit: bool = False) -> "_FakeTransaction":
        self.built_with.append(table)
        return self

    def commit_transaction(self) -> Any:
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return self._committed_table


@pytest.fixture
def scripted_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Real pyiceberg table + a transaction whose commit result is scripted."""
    warehouse, uri = _seed_table(tmp_path, [["1", "a"]])

    def _install(outcomes: list[Any]) -> _FakeTransaction:
        _catalog, table = _load_orders(warehouse, uri)
        fake = _FakeTransaction(outcomes, table)
        monkeypatch.setattr("pyiceberg.table.Transaction", fake)
        return fake

    return warehouse, uri, _install


def test_conflict_retries_reload_the_table_and_back_off(scripted_commit) -> None:
    warehouse, uri, install = scripted_commit
    install([CommitFailedException("conflict"), CommitFailedException("conflict"), None])

    loaded: list[Any] = []
    staged: list[Any] = []
    slept: list[float] = []

    def load_table() -> Any:
        _catalog, table = _load_orders(warehouse, uri)
        loaded.append(table)
        return table

    def stage(table: Any, _txn: Any, props: dict[str, str]) -> str:
        staged.append(table)
        assert props[COMMIT_OPERATION_PROPERTY] == "upsert"
        return "staged"

    outcome = commit_with_retry(
        load_table, stage, operation="upsert", sleep=slept.append
    )

    assert outcome.attempts == 3
    assert outcome.value == "staged"
    assert outcome.recovered_unknown is False
    assert len(staged) == 3
    assert staged == loaded
    assert len({id(t) for t in staged}) == 3, "each attempt must re-load metadata"
    assert len(slept) == 2


def test_exhausted_retries_raise_commit_conflict(scripted_commit) -> None:
    warehouse, uri, install = scripted_commit
    install([CommitFailedException("conflict") for _ in range(4)])

    staged: list[int] = []

    def stage(_table: Any, _txn: Any, _props: dict[str, str]) -> int:
        staged.append(1)
        return len(staged)

    with pytest.raises(IcebergCommitConflictError) as excinfo:
        commit_with_retry(
            lambda: _load_orders(warehouse, uri)[1],
            stage,
            operation="append",
            policy=CommitRetryPolicy(max_attempts=4),
            sleep=lambda _s: None,
        )

    assert len(staged) == 4
    assert isinstance(excinfo.value.__cause__, CommitFailedException)


def test_unknown_commit_is_recovered_by_commit_id(scripted_commit) -> None:
    warehouse, uri, install = scripted_commit
    cid = uuid.uuid4().hex
    import pyarrow as pa

    catalog, table = _load_orders(warehouse, uri)
    table.append(
        pa.table({"id": ["2"], "v": ["b"]}, schema=table.schema().as_arrow()),
        snapshot_properties={
            COMMIT_ID_PROPERTY: cid,
            COMMIT_OPERATION_PROPERTY: "append",
        },
    )
    install([CommitStateUnknownException("lost response")])
    staged: list[int] = []

    def load_table() -> Any:
        _catalog, table = _load_orders(warehouse, uri)
        return table

    def stage(_table: Any, _txn: Any, _props: dict[str, str]) -> str:
        staged.append(1)
        return "staged"

    outcome = commit_with_retry(
        load_table, stage, operation="append", commit_id=cid, sleep=lambda _s: None
    )

    assert outcome.recovered_unknown is True
    assert outcome.commit_id == cid
    assert outcome.snapshot_id is not None
    assert len(staged) == 1, "an unknown outcome must never be blindly re-staged"


def test_unknown_commit_without_our_snapshot_fails_closed(scripted_commit) -> None:
    warehouse, uri, install = scripted_commit
    install([CommitStateUnknownException("lost response")])
    staged: list[int] = []

    def stage(_table: Any, _txn: Any, _props: dict[str, str]) -> str:
        staged.append(1)
        return "staged"

    with pytest.raises(IcebergCommitStateUnknownError) as excinfo:
        commit_with_retry(
            lambda: _load_orders(warehouse, uri)[1],
            stage,
            operation="append",
            commit_id="deadbeef",
            sleep=lambda _s: None,
        )

    assert excinfo.value.commit_id == "deadbeef"
    assert "dataflow.commit-id=deadbeef" in str(excinfo.value)
    assert len(staged) == 1


def test_commit_retry_policy_reads_endpoint_and_validates() -> None:
    assert CommitRetryPolicy.from_endpoint({}) == CommitRetryPolicy(
        max_attempts=5, total_timeout_s=120.0
    )
    top_level = CommitRetryPolicy.from_endpoint(
        {"iceberg_commit_max_attempts": 7, "iceberg_commit_timeout_s": 42}
    )
    assert (top_level.max_attempts, top_level.total_timeout_s) == (7, 42)
    from_extra = CommitRetryPolicy.from_endpoint(
        {"extra": {"iceberg_commit_max_attempts": 2, "iceberg_commit_timeout_s": 90.5}}
    )
    assert (from_extra.max_attempts, from_extra.total_timeout_s) == (2, 90.5)

    for bad, key in (
        ({"iceberg_commit_max_attempts": 0}, "iceberg_commit_max_attempts"),
        ({"iceberg_commit_max_attempts": 21}, "iceberg_commit_max_attempts"),
        ({"iceberg_commit_max_attempts": "5"}, "iceberg_commit_max_attempts"),
        ({"extra": {"iceberg_commit_timeout_s": 0}}, "iceberg_commit_timeout_s"),
        ({"extra": {"iceberg_commit_timeout_s": 3601}}, "iceberg_commit_timeout_s"),
        ({"iceberg_commit_timeout_s": "fast"}, "iceberg_commit_timeout_s"),
    ):
        with pytest.raises(ValueError, match=key):
            CommitRetryPolicy.from_endpoint(bad)


def test_builtin_pyiceberg_retry_is_disabled(tmp_path: Path, monkeypatch) -> None:
    """pyiceberg retries 4× per commit by default, re-applying stale updates."""
    warehouse, uri = _seed_table(tmp_path, [["1", "a"]])
    from pyiceberg.table import Table

    original_do_commit = Table._do_commit
    calls: list[int] = []

    def counting_do_commit(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(1)
        updates = kwargs.get("updates", args[0] if args else ())
        requirements = kwargs.get("requirements", args[1] if len(args) > 1 else ())
        submitted.append((updates, requirements))
        if len(calls) == 1:
            raise CommitFailedException("concurrent update")
        return original_do_commit(self, *args, **kwargs)

    monkeypatch.setattr(Table, "_do_commit", counting_do_commit)
    submitted: list[tuple[Any, Any]] = []

    import pyarrow as pa

    _catalog, table = _load_orders(warehouse, uri)
    arrow = pa.table({"id": ["2"], "v": ["b"]}, schema=table.schema().as_arrow())

    def stage(fresh: Any, txn: Any, props: dict[str, str]) -> int:
        txn.append(arrow, snapshot_properties=props)
        return arrow.num_rows

    outcome = commit_with_retry(
        lambda: _load_orders(warehouse, uri)[1],
        stage,
        operation="append",
        sleep=lambda _s: None,
    )

    assert outcome.attempts == 2
    assert len(calls) == outcome.attempts, (
        "pyiceberg must not retry internally: one _do_commit per our attempt"
    )
    assert all(
        "commit.retry.num-retries" not in repr(updates) + repr(requirements)
        for updates, requirements in submitted
    )
    assert all(
        type(update).__name__ != "SetPropertiesUpdate"
        for updates, _requirements in submitted
        for update in updates
    )
    _catalog2, committed = _load_orders(warehouse, uri)
    assert "commit.retry.num-retries" not in committed.metadata.properties


def test_every_snapshot_carries_the_commit_id(tmp_path: Path) -> None:
    warehouse, uri = _seed_table(tmp_path, [["1", "a"], ["2", "b"]])
    common = {
        "connection_string": uri,
        "warehouse": warehouse,
        "table_name": "default.orders",
        "headers": ["id", "v"],
        "mappings": MAPPINGS,
    }
    assert write_mapped_rows(
        **common, data_rows=[["3", "c"]], write_mode="append"
    ).ok
    assert write_mapped_rows(
        **common,
        data_rows=[["1", "A"], ["4", "d"]],
        write_mode="upsert",
        conflict_columns=["id"],
    ).ok
    assert write_mapped_rows(
        **common, data_rows=[["1", "z"], ["2", "y"]], write_mode="overwrite"
    ).ok
    assert (
        delete_by_primary_keys(
            {"connection_string": uri, "warehouse": warehouse, "schema": "default"},
            "orders",
            "id",
            ["2"],
        )
        == 1
    )

    _catalog, table = _load_orders(warehouse, uri)
    summaries = [_summary(s) for s in table.metadata.snapshots]
    # The seeding append predates the commit layer assertions below only in
    # order, not in behaviour: every snapshot this module writes is tagged.
    assert len(summaries) >= 5
    for summary in summaries:
        assert summary.get(COMMIT_ID_PROPERTY), summary
        assert summary.get(COMMIT_OPERATION_PROPERTY), summary
    operations = {s[COMMIT_OPERATION_PROPERTY] for s in summaries}
    assert {"append", "upsert", "overwrite", "delete"} <= operations


def test_concurrent_upsert_is_re_decided_against_fresh_metadata(
    tmp_path: Path, monkeypatch
) -> None:
    """A conflicting writer must not have its row replayed away by a retry."""
    warehouse, uri = _seed_table(tmp_path, [["1", "a"], ["2", "b"]])
    from pyiceberg.table import Transaction

    original_commit = Transaction.commit_transaction
    interference: list[int] = []

    def interfering_commit(self):  # type: ignore[no-untyped-def]
        if not interference:
            interference.append(1)
            import pyarrow as pa

            _catalog, other = _load_orders(warehouse, uri)
            other.upsert(
                pa.table(
                    {"id": ["1", "9"], "v": ["other", "nine"]},
                    schema=other.schema().as_arrow(),
                ),
                join_cols=["id"],
            )
        return original_commit(self)

    monkeypatch.setattr(Transaction, "commit_transaction", interfering_commit)

    result = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name="default.orders",
        headers=["id", "v"],
        data_rows=[["1", "mine"], ["3", "three"]],
        mappings=MAPPINGS,
        write_mode="upsert",
        conflict_columns=["id"],
    )

    assert interference, "the concurrent writer never ran"
    assert result.ok, result.error
    monkeypatch.undo()
    rows = _rows_by_pk(warehouse, uri)
    assert rows["1"] == "mine"
    assert rows["3"] == "three"
    assert rows["9"] == "nine", "the concurrent writer's row was replayed away"


def test_delete_conflict_retries_instead_of_string_in_fallback(
    tmp_path: Path, monkeypatch
) -> None:
    warehouse, uri = _seed_table(tmp_path, [["1", "a"], ["2", "b"]])
    from pyiceberg.table import Transaction

    original_delete = Transaction.delete
    filters: list[Any] = []

    def recording_delete(self, delete_filter, *args, **kwargs):  # type: ignore[no-untyped-def]
        filters.append(delete_filter)
        if len(filters) == 1:
            raise CommitFailedException("concurrent update")
        return original_delete(self, delete_filter, *args, **kwargs)

    monkeypatch.setattr(Transaction, "delete", recording_delete)

    deleted = delete_by_primary_keys(
        {"connection_string": uri, "warehouse": warehouse, "schema": "default"},
        "orders",
        "id",
        ["2"],
    )

    assert deleted == 1
    assert len(filters) == 2, "the conflict must be retried, not re-planned"
    assert not any(isinstance(f, str) for f in filters), (
        "a commit conflict must never fall through to the string-IN predicate"
    )
    monkeypatch.undo()
    assert set(_rows_by_pk(warehouse, uri)) == {"1"}


def test_lsn_delete_with_no_surviving_keys_skips_commit(
    tmp_path: Path, monkeypatch
) -> None:
    warehouse, uri = _sql_catalog(tmp_path)
    lsn_col = "_df_lsn"
    mappings = [
        *MAPPINGS,
        {"source": lsn_col, "target": lsn_col, "transform": "direct"},
    ]
    seeded = write_mapped_rows(
        connection_string=uri,
        warehouse=warehouse,
        table_name="default.orders",
        headers=["id", "v", lsn_col],
        data_rows=[["1", "newer", "0/200"]],
        mappings=mappings,
        write_mode="append",
        create_table=True,
    )
    assert seeded.ok, seeded.error

    from pyiceberg.table import Transaction

    original_commit = Transaction.commit_transaction
    commit_calls: list[int] = []

    def counting_commit(self):  # type: ignore[no-untyped-def]
        commit_calls.append(1)
        return original_commit(self)

    monkeypatch.setattr(Transaction, "commit_transaction", counting_commit)
    deleted = delete_by_primary_keys(
        {"connection_string": uri, "warehouse": warehouse, "schema": "default"},
        "orders",
        "id",
        ["1"],
        incoming_lsn="0/100",
        lsn_column=lsn_col,
    )

    assert deleted == 0
    assert commit_calls == []


def test_writer_returns_clear_result_for_unknown_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    warehouse, uri = _sql_catalog(tmp_path)

    def raise_unknown(*_args: Any, **_kwargs: Any) -> Any:
        raise IcebergCommitStateUnknownError("inspect before replay", commit_id="abc123")

    monkeypatch.setattr("connectors.iceberg_writer.commit_with_retry", raise_unknown)
    with caplog.at_level(logging.ERROR, logger="connectors.iceberg_writer"):
        result = write_mapped_rows(
            connection_string=uri,
            warehouse=warehouse,
            table_name="default.orders",
            headers=["id", "v"],
            data_rows=[["1", "a"]],
            mappings=MAPPINGS,
            write_mode="append",
            create_table=True,
        )

    assert result.ok is False
    assert "commit outcome unknown" in result.error
    assert "commit-id abc123" in result.error
    assert "default.orders" in caplog.text


REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181").rstrip("/")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_REST_WAREHOUSE", "s3://warehouse/"
)
S3_ENDPOINT = os.environ.get("DATAFLOW_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000")


def _rest_reachable() -> bool:
    try:
        for url, default_port in ((REST_URI, 8181), (S3_ENDPOINT, 9000)):
            host = url.split("://", 1)[-1].split("/", 1)[0]
            hostname, _, port_s = host.partition(":")
            port = int(port_s or str(default_port))
            with socket.create_connection((hostname, port), timeout=1.5):
                pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as resp:
            return int(getattr(resp, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(),
    reason=f"Iceberg REST catalog or S3 endpoint not reachable: {REST_URI}, {S3_ENDPOINT}",
)


def _rest_write(table: str, rows: list[list[str]], **kwargs: Any) -> Any:
    return write_mapped_rows(
        connection_string=REST_URI,
        warehouse=REST_WAREHOUSE,
        table_name=f"default.{table}",
        headers=["id", "v"],
        data_rows=rows,
        mappings=MAPPINGS,
        extra={"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
        **kwargs,
    )


def _install_rest_client_config(monkeypatch: pytest.MonkeyPatch) -> None:
    import connectors.iceberg_catalog as catalog_module
    from pyiceberg.catalog.rest import RestCatalog

    def load_rest_catalog(endpoint: Any) -> Any:
        config = catalog_module.parse_iceberg_catalog_config(endpoint)
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

    monkeypatch.setattr(catalog_module, "load_catalog", load_rest_catalog)


def _rest_table(table: str) -> Any:
    from connectors.iceberg_catalog import load_catalog

    catalog = load_catalog(
        {
            "connection_string": REST_URI,
            "warehouse": REST_WAREHOUSE,
            "table": table,
            "schema": "default",
            "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
        }
    )
    return catalog.load_table(("default", table))


@requires_rest
def test_live_rest_concurrent_upserts_keep_one_row_per_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_rest_client_config(monkeypatch)
    table = "commit_l1_" + uuid.uuid4().hex[:8]
    seeded = _rest_write(
        table, [["1", "seed"]], write_mode="append", create_table=True
    )
    assert seeded.ok, seeded.error

    results: dict[str, Any] = {}

    def writer(name: str, rows: list[list[str]]) -> None:
        results[name] = _rest_write(
            table, rows, write_mode="upsert", conflict_columns=["id"]
        )

    threads = [
        threading.Thread(
            target=writer, args=("a", [["1", "a"], ["2", "a"], ["3", "a"]])
        ),
        threading.Thread(
            target=writer, args=("b", [["1", "b"], ["2", "b"], ["4", "b"]])
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results["a"].ok, results["a"].error
    assert results["b"].ok, results["b"].error

    arrow = _rest_table(table).scan().to_arrow()
    ids = arrow.column("id").to_pylist()
    values = dict(zip(ids, arrow.column("v").to_pylist()))
    assert len(ids) == len(set(ids)), f"concurrent upserts duplicated keys: {ids}"
    assert set(ids) == {"1", "2", "3", "4"}
    for key in ("1", "2"):
        assert values[key] in {"a", "b"}
    assert values["3"] == "a"
    assert values["4"] == "b"


@requires_rest
def test_live_rest_snapshots_are_tagged_and_retry_is_not_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_rest_client_config(monkeypatch)
    table = "commit_l2_" + uuid.uuid4().hex[:8]
    seeded = _rest_write(table, [["1", "a"]], write_mode="append", create_table=True)
    assert seeded.ok, seeded.error
    updated = _rest_write(
        table, [["1", "A"], ["2", "b"]], write_mode="upsert", conflict_columns=["id"]
    )
    assert updated.ok, updated.error

    loaded = _rest_table(table)
    summaries = [_summary(s) for s in loaded.metadata.snapshots]
    assert summaries
    for summary in summaries:
        assert summary.get(COMMIT_ID_PROPERTY), summary
        assert summary.get(COMMIT_OPERATION_PROPERTY), summary
    assert "commit.retry.num-retries" not in loaded.metadata.properties
