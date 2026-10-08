"""Validate asks the run's own CDC reader whether the source emits a change log.

MariaDB and SQL Server CDC passed Validate with no binlog / no capture
instance, then the run polled the primary key: a snapshot plus new inserts,
with every update and delete lost, reported completed (DEF-B2-006). The
reader's ``is_available()`` is the same read-only check the run makes, so
Validate and Execute cannot disagree about it.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

from services.cdc_capability import (
    CAUSE_SERVER_NOT_CONFIGURED,
    LogCaptureRefusal,
    classify_log_capture_failure,
    query_cdc_change_refusal,
)

_logger = logging.getLogger(__name__)

_MYSQL = {"mysql", "mariadb", "amazon_rds_mysql", "amazon_aurora_mysql", "azure_mysql"}
_SQLSERVER = {
    "sqlserver",
    "mssql",
    "sql_server",
    "azure_sql_database",
    "microsoft_sql_server",
    "amazon_rds_sql_server",
}
_ORACLE = {"oracle"}


@dataclass(frozen=True)
class LogCaptureProbe:
    dialect: str
    available: bool | None
    reader: str = ""
    cause: str = ""
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _dialect(source_type: str) -> str:
    kind = str(source_type or "").strip().lower().replace("-", "_")
    if kind in _MYSQL:
        return "mariadb" if kind == "mariadb" else "mysql"
    if kind in _SQLSERVER:
        return "sqlserver"
    if kind in _ORACLE:
        return "oracle"
    return ""


def _try_reader(cls: Any, cfg: dict[str, Any], **kwargs: Any) -> tuple[bool | None, Any]:
    reader = None
    try:
        reader = cls({**cfg, "job_id": "", "lease_holder_id": ""}, **kwargs)
        return bool(reader.is_available()), reader
    except Exception as exc:  # noqa: BLE001
        _logger.debug("CDC log-capture probe %s failed: %s", cls.__name__, exc)
        return None, exc
    finally:
        close = getattr(reader, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # noqa: BLE001
                _logger.debug("CDC probe reader close: %s", exc)


def probe_log_capture(
    source_type: str,
    cfg: dict[str, Any] | None,
    *,
    table: str,
    schema: str = "",
    primary_key: str,
) -> LogCaptureProbe:
    """Whether the reader the run will open can attach to the source log.

    ``available=None`` means the probe could not decide (unreachable source,
    missing driver on this process). Connectivity has its own gate, so an
    undecided probe never blocks.
    """
    dialect = _dialect(source_type)
    if not dialect or not cfg or not table or not primary_key:
        return LogCaptureProbe(dialect=dialect, available=None)
    common = {"table": table, "primary_key": primary_key, "batch_size": 1}
    if dialect in {"mysql", "mariadb"}:
        from connectors.mysql_change_stream import MySqlChangeStreamCdc

        ok, reader = _try_reader(MySqlChangeStreamCdc, cfg, **common)
        if ok is False:
            reason = getattr(reader, "unavailable_reason", None)
            if isinstance(reason, LogCaptureRefusal):
                return LogCaptureProbe(dialect, False, "binlog", reason.cause, reason.detail)
            return LogCaptureProbe(dialect, False, "binlog", CAUSE_SERVER_NOT_CONFIGURED, "")
        return LogCaptureProbe(dialect, ok, "binlog" if ok else "")
    if dialect == "sqlserver":
        from connectors.sqlserver_cdc_native import SqlServerNativeCdc
        from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc

        ss_schema = schema or "dbo"
        native, _ = _try_reader(SqlServerNativeCdc, cfg, schema=ss_schema, **common)
        if native:
            return LogCaptureProbe(dialect, True, "sqlserver_native")
        tracking, _ = _try_reader(SqlServerChangeTrackingCdc, cfg, schema=ss_schema, **common)
        if tracking:
            return LogCaptureProbe(dialect, True, "change_tracking")
        if native is None and tracking is None:
            return LogCaptureProbe(dialect, None)
        refusal = classify_log_capture_failure(
            "sqlserver",
            f"change data capture is not enabled for {ss_schema}.{table} and Change Tracking is off",
        )
        return LogCaptureProbe(dialect, False, "", refusal.cause, refusal.detail)
    from connectors.oracle_change_stream import OracleFlashbackCdc
    from connectors.oracle_logminer import OracleLogMinerCdc

    logminer, _ = _try_reader(OracleLogMinerCdc, cfg, schema=schema, **common)
    if logminer:
        return LogCaptureProbe(dialect, True, "logminer")
    flashback, _ = _try_reader(OracleFlashbackCdc, cfg, schema=schema, **common)
    if flashback:
        return LogCaptureProbe(dialect, True, "flashback")
    if logminer is None and flashback is None:
        return LogCaptureProbe(dialect, None)
    refusal = classify_log_capture_failure(
        "oracle", f"supplemental logging is not enabled for {table}"
    )
    return LogCaptureProbe(dialect, False, "", refusal.cause, refusal.detail)


def build_log_capture_gate(
    probe: LogCaptureProbe,
    *,
    cursor_field: str,
    cursor_semantics: str,
    primary_key_columns: list[str],
    pass_status: str,
    block_status: str,
) -> dict[str, Any] | None:
    """g9c: block CDC that would run as a primary-key poll; warn on delete loss."""
    if probe.available is None or not probe.dialect:
        return None
    if probe.available:
        return {
            "id": "g9c_cdc_log_capture",
            "status": pass_status,
            "message": f"{probe.dialect} change log is readable ({probe.reader})",
            "duration_ms": 0,
            "details": probe.to_dict(),
        }
    if probe.cause and probe.cause != CAUSE_SERVER_NOT_CONFIGURED:
        refusal = classify_log_capture_failure(probe.dialect, probe.detail)
        return {
            "id": "g9c_cdc_log_capture",
            "status": block_status,
            "message": refusal.message(probe.dialect),
            "duration_ms": 0,
            "details": probe.to_dict(),
        }
    cursor = cursor_field or (primary_key_columns[0] if primary_key_columns else "")
    refused = query_cdc_change_refusal(
        dialect=probe.dialect,
        cursor_field=cursor,
        cursor_semantics=cursor_semantics,
        primary_key_columns=primary_key_columns,
        downgrade_cause=probe.cause or CAUSE_SERVER_NOT_CONFIGURED,
    )
    if refused:
        return {
            "id": "g9c_cdc_log_capture",
            "status": block_status,
            "message": refused,
            "duration_ms": 0,
            "details": probe.to_dict(),
        }
    return {
        "id": "g9c_cdc_log_capture",
        "status": pass_status,
        "severity": "warn",
        "message": (
            f"{probe.dialect} does not emit a change log; CDC runs as a cursor poll on "
            f"'{cursor}'. Inserts and updates are carried; deletes are not."
        ),
        "duration_ms": 0,
        "details": {**probe.to_dict(), "cdc_delete_capture": False},
    }
