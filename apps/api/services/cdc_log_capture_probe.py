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
_POSTGRES = {
    "postgresql",
    "postgres",
    "citus",
    "cockroach",
    "cockroachdb",
    "greenplum",
    "timescaledb",
    "neon",
}
_MONGO = {"mongodb", "mongo", "documentdb", "amazon_documentdb"}


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


def _probe_postgres_wal_level(cfg: dict[str, Any] | None) -> LogCaptureProbe:
    """Read-only ``SHOW wal_level``; ``logical`` stays undecided for the slot probe."""
    if not cfg or not (cfg.get("host") or cfg.get("connection_string")):
        return LogCaptureProbe(dialect="postgresql", available=None)
    from connectors.postgresql_conn import get_connection
    from services.cdc_host_prereq import _show

    try:
        conn = get_connection(
            host=str(cfg.get("host") or "localhost"),
            port=int(cfg.get("port") or 5432),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or cfg.get("user") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl")),
        )
    except Exception as exc:  # noqa: BLE001 — connectivity has its own gate
        _logger.info("CDC wal_level probe could not connect to PostgreSQL: %s", exc)
        return LogCaptureProbe(dialect="postgresql", available=None)
    try:
        with conn.cursor() as cur:
            level = _show(cur, "wal_level").lower()
    except Exception as exc:  # noqa: BLE001 — e.g. a Postgres-wire engine without wal_level
        _logger.warning("CDC wal_level probe could not read wal_level: %s", exc)
        return LogCaptureProbe(dialect="postgresql", available=None)
    finally:
        conn.close()
    if level == "logical":
        return LogCaptureProbe(dialect="postgresql", available=None)
    _logger.warning(
        "PostgreSQL source %s:%s has wal_level=%s; CDC preflight blocks before Confirm",
        cfg.get("host"), cfg.get("port"), level or "unknown",
    )
    return LogCaptureProbe(
        "postgresql",
        False,
        "pgoutput",
        CAUSE_SERVER_NOT_CONFIGURED,
        f"wal_level={level or 'unknown'} — logical decoding is off on this server",
    )


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
    kind = str(source_type or "").strip().lower().replace("-", "_")
    # Slot attach/retention stays with the slot probe; this gate only refuses
    # a server that cannot decode WAL at all. The slot probe needs a stored
    # LSN, so a first CDC run on wal_level=replica passed Validate (MX3-05).
    if kind in _POSTGRES:
        return _probe_postgres_wal_level(cfg)
    if kind in _MONGO:
        if not cfg or not table:
            return LogCaptureProbe(dialect="mongodb", available=None)
        from connectors.mongodb_change_stream import MongodbChangeStreamCdc

        ok, _reader = _try_reader(
            MongodbChangeStreamCdc,
            cfg,
            collection=table,
            primary_key=primary_key or "_id",
            batch_size=1,
        )
        if ok:
            return LogCaptureProbe("mongodb", True, "change_stream")
        if ok is False:
            return LogCaptureProbe(
                "mongodb",
                False,
                "change_stream",
                CAUSE_SERVER_NOT_CONFIGURED,
                "change streams need a replica set",
            )
        return LogCaptureProbe(dialect="mongodb", available=None)
    dialect = _dialect(source_type)
    if not dialect:
        # A database this probe does not know has no change log. Omitting the
        # gate let CDC preflight pass on sources that cannot do CDC.
        if not kind:
            return LogCaptureProbe(dialect="", available=None)
        return LogCaptureProbe(
            dialect=kind,
            available=False,
            cause=CAUSE_SERVER_NOT_CONFIGURED,
            detail="this source does not emit a change log",
        )
    if not cfg or not table or not primary_key:
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

    logminer, reader = _try_reader(OracleLogMinerCdc, cfg, schema=schema, **common)
    if logminer:
        from connectors.oracle_logminer_txn import oracle_txn_buffer_enabled

        if not oracle_txn_buffer_enabled():
            try:
                reader._ensure_open_txn_detection()
            except Exception as exc:  # noqa: BLE001 — unsafe opt-out must block preflight
                message = str(exc)
                classification_text = message
                grant_object = ""
                if "ORA-00942" in message.upper() or "ORA-01031" in message.upper():
                    view = (
                        "GV$INSTANCE"
                        if "GV$INSTANCE" in message.upper()
                        else "V$TRANSACTION"
                    )
                    grant_object = (
                        "GV_$INSTANCE" if view == "GV$INSTANCE" else "V_$TRANSACTION"
                    )
                    classification_text = (
                        f"insufficient privilege to inspect {view}: {message}"
                    )
                refusal = classify_log_capture_failure("oracle", classification_text)
                detail = refusal.detail
                if grant_object and f"SELECT ON {grant_object}" not in detail.upper():
                    detail += (
                        f". DATAFLOW_CDC_ORACLE_TXN_BUFFER=0 requires SELECT ON "
                        f"{grant_object} (for example, GRANT SELECT ON "
                        f"{grant_object} TO <cdc user>)."
                    )
                return LogCaptureProbe(
                    dialect,
                    False,
                    "logminer",
                    refusal.cause,
                    detail,
                )
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


def _message_with_probe_detail(message: str, detail: str) -> str:
    """Keep ``wal_level=replica`` in the gate text the plan actually returns.

    The plan projection keeps ``message`` and drops ``details``. A refusal that
    only said ``server_not_configured`` never named wal_level (R3-H1).
    """
    extra = str(detail or "").strip()
    text = str(message or "").strip()
    if extra and extra not in text:
        return f"{text} {extra}"
    return text


#: Sources whose CDC run falls back to polling the declared cursor when the
#: server emits no change log (``src/transfer/cdc_transfer.py``).
_QUERY_POLL_DIALECTS = frozenset({"mysql", "mariadb", "sqlserver", "oracle", "postgresql", "mongodb"})


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
            "message": _message_with_probe_detail(refused, probe.detail),
            "duration_ms": 0,
            "details": probe.to_dict(),
        }
    if probe.dialect not in _QUERY_POLL_DIALECTS:
        # No query-poll fallback at run time for this source: say so up front.
        return {
            "id": "g9c_cdc_log_capture",
            "status": block_status,
            "message": _message_with_probe_detail(
                (
                    f"{probe.dialect or 'this source'} does not emit a change log, "
                    "so CDC cannot start. Run this sync as incremental."
                ),
                probe.detail,
            ),
            "duration_ms": 0,
            "details": {**probe.to_dict(), "cdc_delete_capture": False},
        }
    # The operator named the poll column and declared what it means (D28). The
    # run polls exactly that column (``cdc_transfer`` resolves the same
    # contract fields), so Validate must not refuse what Execute would run
    # correctly. It is not log CDC, and the gate says so: inserts and updates
    # land, a DELETE on the source is not captured.
    return {
        "id": "g9c_cdc_log_capture",
        "status": pass_status,
        "severity": "warn",
        "message": _message_with_probe_detail(
            (
                f"{probe.dialect} does not emit a change log, so this CDC run "
                f"polls `{cursor}` (declared {cursor_semantics or 'cursor'}): "
                "inserts and updates are captured, DELETEs on the source are "
                "not. Enable the change log to capture deletes."
            ),
            probe.detail,
        ),
        "duration_ms": 0,
        "details": {
            **probe.to_dict(),
            "cdc_delete_capture": False,
            "cdc_capture_used": "query_cursor",
            "poll_cursor_field": cursor,
            "poll_cursor_semantics": cursor_semantics,
        },
    }
