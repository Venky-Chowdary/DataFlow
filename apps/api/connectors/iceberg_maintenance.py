from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Mapping

from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

logger = logging.getLogger(__name__)
_MIN_SNAPSHOT_AGE_MS = 60 * 60 * 1000


class IcebergMaintenanceUnsupportedError(NotImplementedError):
    pass


@dataclass(frozen=True)
class MaintenancePolicy:
    older_than_ms: int
    retain_last: int = 5
    dry_run: bool = True
    allow_recent: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.older_than_ms, bool) or not isinstance(
            self.older_than_ms, int
        ):
            raise ValueError("older_than_ms must be an integer epoch timestamp")
        if isinstance(self.retain_last, bool) or not isinstance(
            self.retain_last, int
        ) or self.retain_last < 1:
            raise ValueError("retain_last must be an integer greater than or equal to 1")
        if not isinstance(self.dry_run, bool):
            raise ValueError("dry_run must be a boolean")
        if not isinstance(self.allow_recent, bool):
            raise ValueError("allow_recent must be a boolean")
        if (
            not self.allow_recent
            and self.older_than_ms > int(time.time() * 1000) - _MIN_SNAPSHOT_AGE_MS
        ):
            raise ValueError(
                "older_than_ms must be at least one hour in the past; "
                "set allow_recent=True to override"
            )


@dataclass(frozen=True)
class MaintenanceReport:
    candidates: tuple[int, ...]
    expired: tuple[int, ...]
    retained: tuple[int, ...]
    dry_run: bool


def _identifier(endpoint: Mapping[str, Any], table: str) -> tuple[str, ...]:
    config = parse_iceberg_catalog_config(dict(endpoint))
    table_parts = tuple(part for part in str(table or config["table_name"]).split(".") if part)
    if len(table_parts) > 1:
        return table_parts
    return tuple(config["namespace"]) + table_parts


def expire_snapshots(
    endpoint: Mapping[str, Any],
    table: str,
    policy: MaintenancePolicy,
) -> MaintenanceReport:
    if not isinstance(policy, MaintenancePolicy):
        raise TypeError("policy must be a MaintenancePolicy")
    catalog = load_catalog(dict(endpoint))
    identifier = _identifier(endpoint, table)
    tbl = catalog.load_table(identifier)
    snapshots = list(tbl.snapshots())
    snapshot_ids = {snapshot.snapshot_id for snapshot in snapshots}
    current = tbl.current_snapshot()
    protected = {current.snapshot_id} if current is not None else set()
    protected.update(ref.snapshot_id for ref in tbl.refs().values())
    newest = sorted(
        snapshots, key=lambda snapshot: (snapshot.timestamp_ms, snapshot.snapshot_id)
    )[-policy.retain_last :]
    protected.update(snapshot.snapshot_id for snapshot in newest)

    candidates = tuple(
        sorted(
            snapshot.snapshot_id
            for snapshot in snapshots
            if snapshot.timestamp_ms < policy.older_than_ms
            and snapshot.snapshot_id not in protected
        )
    )
    if policy.dry_run or not candidates:
        report = MaintenanceReport(
            candidates=candidates,
            expired=(),
            retained=tuple(sorted(snapshot_ids - set(candidates))),
            dry_run=policy.dry_run,
        )
    else:
        tbl.maintenance.expire_snapshots().by_ids(list(candidates)).commit()
        refreshed = catalog.load_table(identifier)
        remaining = {snapshot.snapshot_id for snapshot in refreshed.snapshots()}
        report = MaintenanceReport(
            candidates=candidates,
            expired=tuple(sorted(set(candidates) - remaining)),
            retained=tuple(sorted(remaining)),
            dry_run=False,
        )

    logger.info(
        "Iceberg snapshot maintenance candidate_count=%d expired_count=%d retained_count=%d",
        len(report.candidates),
        len(report.expired),
        len(report.retained),
    )
    return report


def rewrite_data_files(endpoint: Mapping[str, Any], table: str) -> None:
    raise IcebergMaintenanceUnsupportedError(
        "PyIceberg 0.12 does not support data-file compaction; run it through Spark or the engine"
    )


def remove_orphan_files(endpoint: Mapping[str, Any], table: str) -> None:
    raise IcebergMaintenanceUnsupportedError(
        "PyIceberg 0.12 does not support orphan-file removal; run it through Spark or the engine"
    )
