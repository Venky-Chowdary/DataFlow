"""Application-level commit retries for PyIceberg catalog writes."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from numbers import Real
from typing import Any, Generic, TypeVar

from connectors.write_resilience import reconnect_backoff_seconds

logger = logging.getLogger(__name__)

_T = TypeVar("_T")
_MISSING = object()
COMMIT_ID_PROPERTY = "dataflow.commit-id"
COMMIT_OPERATION_PROPERTY = "dataflow.operation"
_COMMIT_RETRY_PROPERTY = "commit.retry.num-retries"


class IcebergCommitError(RuntimeError):
    """Base class for application-level Iceberg commit failures."""


class IcebergCommitConflictError(IcebergCommitError):
    """A commit could not be applied before the retry budget was exhausted."""


class IcebergCommitStateUnknownError(IcebergCommitError):
    """The catalog may have applied a commit whose outcome could not be checked."""

    def __init__(self, message: str, *, commit_id: str) -> None:
        super().__init__(message)
        self.commit_id = commit_id


@dataclass(frozen=True)
class CommitRetryPolicy:
    """Bounds for application-level optimistic commit retries."""

    max_attempts: int = 5
    total_timeout_s: float = 120.0

    def __post_init__(self) -> None:
        _validate_max_attempts(self.max_attempts)
        _validate_timeout(self.total_timeout_s)

    @classmethod
    def from_endpoint(cls, endpoint: Any) -> "CommitRetryPolicy":
        max_attempts = _endpoint_value(endpoint, "iceberg_commit_max_attempts")
        timeout_s = _endpoint_value(endpoint, "iceberg_commit_timeout_s")
        return cls(
            max_attempts=5 if max_attempts is _MISSING else max_attempts,
            total_timeout_s=120.0 if timeout_s is _MISSING else timeout_s,
        )


@dataclass(frozen=True)
class CommitOutcome(Generic[_T]):
    """The value produced by a committed stage and its commit evidence."""

    value: _T
    attempts: int
    commit_id: str
    snapshot_id: int | None
    recovered_unknown: bool


def commit_with_retry(
    load_table: Callable[[], Any],
    stage: Callable[[Any, Any, dict[str, str]], _T],
    *,
    operation: str,
    commit_id: str | None = None,
    policy: CommitRetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CommitOutcome[_T]:
    """Stage and commit a fresh-table transaction with fail-closed recovery."""

    from pyiceberg.exceptions import CommitFailedException, ValidationException
    from pyiceberg.table import Table, Transaction

    try:
        from pyiceberg.exceptions import CommitStateUnknownException
    except ImportError:  # pragma: no cover - compatibility with older PyIceberg
        class CommitStateUnknownException(Exception):
            pass

    try:
        from requests.exceptions import ConnectionError as RequestsConnectionError
        from requests.exceptions import Timeout as RequestsTimeout
    except ImportError:  # pragma: no cover - requests is a PyIceberg dependency
        RequestsConnectionError = ConnectionError
        RequestsTimeout = TimeoutError

    cid = uuid.uuid4().hex if commit_id is None else commit_id
    retry_policy = policy or CommitRetryPolicy()
    started_at = time.monotonic()
    conflict_types = (CommitFailedException, ValidationException)
    unknown_types = (CommitStateUnknownException, RequestsConnectionError, RequestsTimeout)

    for attempt in range(1, retry_policy.max_attempts + 1):
        tbl = load_table()
        table_identifier = _table_identifier(tbl)
        transaction_table = _disable_builtin_retries(tbl, Table)
        txn = Transaction(transaction_table)
        props = {
            COMMIT_ID_PROPERTY: cid,
            COMMIT_OPERATION_PROPERTY: operation,
        }

        try:
            value = stage(tbl, txn, props)
        except conflict_types as exc:
            _retry_or_raise_conflict(
                exc,
                table_identifier=table_identifier,
                attempt=attempt,
                policy=retry_policy,
                started_at=started_at,
                sleep=sleep,
            )
            continue

        try:
            committed_table = txn.commit_transaction()
        except conflict_types as exc:
            _retry_or_raise_conflict(
                exc,
                table_identifier=table_identifier,
                attempt=attempt,
                policy=retry_policy,
                started_at=started_at,
                sleep=sleep,
            )
            continue
        except unknown_types as exc:
            try:
                recovered_table = load_table()
                recovered_snapshot_id = _find_commit_snapshot(recovered_table, cid)
            except Exception as reload_exc:
                logger.error(
                    "Iceberg commit state unknown table=%s commit_id=%s "
                    "reload_error=%s",
                    table_identifier,
                    cid,
                    type(reload_exc).__name__,
                )
                raise _unknown_error(cid) from exc
            if recovered_snapshot_id is not None:
                logger.info(
                    "Recovered Iceberg commit table=%s commit_id=%s snapshot_id=%s",
                    table_identifier,
                    cid,
                    recovered_snapshot_id,
                )
                return CommitOutcome(
                    value=value,
                    attempts=attempt,
                    commit_id=cid,
                    snapshot_id=recovered_snapshot_id,
                    recovered_unknown=True,
                )
            logger.error(
                "Iceberg commit state unknown table=%s commit_id=%s "
                "snapshot_not_found=True",
                table_identifier,
                cid,
            )
            raise _unknown_error(cid) from exc

        snapshot_id = _current_snapshot_id(committed_table)
        return CommitOutcome(
            value=value,
            attempts=attempt,
            commit_id=cid,
            snapshot_id=snapshot_id,
            recovered_unknown=False,
        )

    raise AssertionError("commit retry loop exhausted without returning or raising")


def _endpoint_value(endpoint: Any, key: str) -> Any:
    extra: Any = None
    if isinstance(endpoint, Mapping):
        value = endpoint.get(key, _MISSING)
        extra = endpoint.get("extra")
    else:
        value = getattr(endpoint, key, _MISSING)
        extra = getattr(endpoint, "extra", None)
    if value is not _MISSING:
        return value
    if isinstance(extra, Mapping):
        return extra.get(key, _MISSING)
    return _MISSING


def _validate_max_attempts(value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 20:
        raise ValueError(
            "iceberg_commit_max_attempts must be an integer between 1 and 20"
        )


def _validate_timeout(value: Any) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, Real)
        or not 1 <= float(value) <= 3600
    ):
        raise ValueError(
            "iceberg_commit_timeout_s must be a number between 1 and 3600"
        )


def _disable_builtin_retries(table: Any, table_type: Any) -> Any:
    metadata = table.metadata
    properties = dict(metadata.properties or {})
    properties[_COMMIT_RETRY_PROPERTY] = "0"
    cloned_metadata = metadata.model_copy(update={"properties": properties})
    return table_type(
        table.name(),
        cloned_metadata,
        table.metadata_location,
        table.io,
        table.catalog,
        table.config,
    )


def _table_identifier(table: Any) -> str:
    try:
        return ".".join(str(part) for part in table.name())
    except Exception:
        return str(getattr(table, "name", table))


def _current_snapshot_id(table: Any) -> int | None:
    return getattr(table.metadata, "current_snapshot_id", None)


def _find_commit_snapshot(table: Any, commit_id: str) -> int | None:
    for snapshot in table.metadata.snapshots or ():
        summary = getattr(snapshot, "summary", None)
        if _summary_value(summary, COMMIT_ID_PROPERTY) == commit_id:
            return int(snapshot.snapshot_id)
    return None


def _summary_value(summary: Any, key: str) -> Any:
    if isinstance(summary, Mapping):
        return summary.get(key)
    properties = getattr(summary, "additional_properties", None) or {}
    if isinstance(properties, Mapping) and key in properties:
        return properties[key]
    try:
        return summary[key]
    except (KeyError, IndexError, TypeError):
        return None


def _retry_or_raise_conflict(
    exc: BaseException,
    *,
    table_identifier: str,
    attempt: int,
    policy: CommitRetryPolicy,
    started_at: float,
    sleep: Callable[[float], None],
) -> None:
    logger.warning(
        "Iceberg commit conflict table=%s attempt=%s/%s error=%s",
        table_identifier,
        attempt,
        policy.max_attempts,
        type(exc).__name__,
    )
    elapsed = time.monotonic() - started_at
    remaining = policy.total_timeout_s - elapsed
    if attempt >= policy.max_attempts or remaining <= 0:
        raise IcebergCommitConflictError(
            f"Iceberg commit conflict for {table_identifier} after "
            f"{attempt} attempt(s): {type(exc).__name__}: {exc}"
        ) from exc
    delay = min(reconnect_backoff_seconds(attempt), remaining)
    sleep(delay)
    if time.monotonic() - started_at >= policy.total_timeout_s:
        raise IcebergCommitConflictError(
            f"Iceberg commit conflict for {table_identifier} after "
            f"{attempt} attempt(s): retry timeout exceeded"
        ) from exc


def _unknown_error(commit_id: str) -> IcebergCommitStateUnknownError:
    return IcebergCommitStateUnknownError(
        "Iceberg commit outcome is unknown; inspect snapshot summary "
        f"dataflow.commit-id={commit_id} before replaying",
        commit_id=commit_id,
    )
