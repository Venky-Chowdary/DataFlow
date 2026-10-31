"""Oracle LogMiner CDC — Debezium-class redo log mining.

Uses ``DBMS_LOGMNR`` + ``V$LOGMNR_CONTENTS`` for INSERT/UPDATE/DELETE with SCN
watermarks. Flashback versions (``oracle_change_stream.py``) remain the
fallback when LogMiner privileges or supplemental logging are unavailable.
"""

from __future__ import annotations

import json
import logging
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from connectors.lsn_guards import parse_oracle_rs_id
from connectors.oracle_logminer_txn import mining_position
from connectors.sql_identifiers import quote_sql_identifier
from connectors.write_resilience import (
    is_connection_lost,
    reconnect_backoff_seconds,
    should_retry_connection_lost,
)
from services.cdc_cursor_gap import CdcScnGapError
from services.cdc_engine import ChangeBatch
from services.value_serializer import cell_to_string, wire_cell

logger = logging.getLogger(__name__)

_ORACLE_CONNECTION_LOST_CODES = (
    "DPY-4011",
    "ORA-01012",
    "ORA-01033",
    "ORA-01034",
    "ORA-01109",
    "ORA-03113",
    "ORA-03114",
    "ORA-03135",
    "ORA-12537",
    "ORA-12547",
    "ORA-16331",
)

def _is_oracle_connection_lost(exc: BaseException | str) -> bool:
    if is_connection_lost(exc):
        return True
    pending = [exc] if isinstance(exc, BaseException) else []
    seen: set[int] = set()
    if not pending:
        text = str(exc).upper()
        return any(code in text for code in _ORACLE_CONNECTION_LOST_CODES)
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        text = str(current).upper()
        if any(code in text for code in _ORACLE_CONNECTION_LOST_CODES):
            return True
        for inner in (
            current.__cause__,
            None if current.__suppress_context__ else current.__context__,
            getattr(current, "orig", None),
        ):
            if isinstance(inner, BaseException):
                pending.append(inner)
    return False


def _oracle_error_number(exc: BaseException) -> int | None:
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        code = getattr(current, "code", None)
        if isinstance(code, int) and code > 0:
            return code
        match = re.search(r"\bORA-(\d{5})\b", str(current).upper())
        if match:
            return int(match.group(1))
        for inner in (
            current.__cause__,
            None if current.__suppress_context__ else current.__context__,
            getattr(current, "orig", None),
        ):
            if isinstance(inner, BaseException):
                pending.append(inner)
    return None


class OracleLogMinerReadError(RuntimeError):
    error_number: int | None
    capture_instance: str
    cursor_key: str
    transient: bool
    table: str

    def __init__(
        self,
        exc: BaseException,
        *,
        table: str = "",
        cursor_key: str = "",
        message: str | None = None,
    ) -> None:
        self.error_number = _oracle_error_number(exc)
        self.capture_instance = ""
        self.table = str(table or "")
        self.cursor_key = str(cursor_key or "")
        self.transient = _is_oracle_connection_lost(exc)
        number = (
            f"Oracle error {self.error_number}"
            if self.error_number is not None
            else "Oracle error number unavailable"
        )
        super().__init__(
            message
            or (
                f"Oracle LogMiner read failed for table {self.table!r} "
                f"({number}) at cursor {self.cursor_key!r}."
            )
        )


class OracleOpenTxnDetectionUnavailable(OracleLogMinerReadError):
    code = "oracle_open_txn_detection_unavailable"

    def __init__(
        self,
        exc: BaseException,
        *,
        table: str = "",
        cursor_key: str = "",
    ) -> None:
        self.code = "oracle_open_txn_detection_unavailable"
        message = (
            "Oracle LogMiner buffer-off requires open-transaction detection, but "
            f"the V$TRANSACTION probe failed ({exc}). Grant SELECT ON "
            "V_$TRANSACTION (for example, GRANT SELECT ON V_$TRANSACTION TO "
            "<cdc user>) or re-enable the Oracle transaction buffer (unset "
            "DATAFLOW_CDC_ORACLE_TXN_BUFFER)."
        )
        super().__init__(
            exc,
            table=table,
            cursor_key=cursor_key,
            message=message,
        )


class OracleInstanceDetectionUnavailable(OracleLogMinerReadError):
    code = "oracle_instance_detection_unavailable"

    def __init__(
        self,
        exc: BaseException,
        *,
        table: str = "",
        cursor_key: str = "",
    ) -> None:
        message = (
            "Oracle LogMiner buffer-off requires Oracle instance detection, but "
            f"the GV$INSTANCE check failed ({exc}). Grant SELECT ON GV_$INSTANCE "
            "(for example, GRANT SELECT ON GV_$INSTANCE TO <cdc user>) or unset "
            "DATAFLOW_CDC_ORACLE_TXN_BUFFER to use the transaction buffer."
        )
        self.code = "oracle_instance_detection_unavailable"
        super().__init__(exc, table=table, cursor_key=cursor_key, message=message)


class OracleBufferOffRacUnsupported(OracleLogMinerReadError):
    code = "oracle_buffer_off_rac_unsupported"

    def __init__(self, *, table: str = "", cursor_key: str = "") -> None:
        message = (
            "Oracle LogMiner buffer-off is unsupported on Oracle RAC because "
            "V$TRANSACTION is instance-local. unset DATAFLOW_CDC_ORACLE_TXN_BUFFER "
            "to use the transaction buffer."
        )
        self.code = "oracle_buffer_off_rac_unsupported"
        super().__init__(
            RuntimeError(message),
            table=table,
            cursor_key=cursor_key,
            message=message,
        )


_OP_MAP = {
    "INSERT": "insert",
    "UPDATE": "update",
    "DELETE": "delete",
}

UNPARSED_SQL_REDO_FLAG = "_df_unparsed_sql_redo"
_LOB_LOCATOR_MARKER = "_df_lob_locator_trigger"
_LOB_LOOKUP_MARKER = "_df_lob_lookup_required"
_UNPARSED_TRUTHY = frozenset({"1", "true", "yes"})


def is_unparsed_sql_redo(row: Any) -> bool:
    """True when LogMiner text could not be mapped to dest columns."""
    if not isinstance(row, dict):
        return False
    return str(row.get(UNPARSED_SQL_REDO_FLAG) or "").strip().lower() in _UNPARSED_TRUTHY


def sql_redo_reject_detail(
    row: dict[str, Any] | None,
    *,
    op: str = "",
    sql_redo: str = "",
    table: str = "",
) -> dict[str, Any]:
    """Module-9-shaped quarantine payload — never a dest upsert."""
    rec = dict(row or {})
    reason = str(
        rec.get("_df_parse_error")
        or "Oracle LogMiner SQL_REDO could not be parsed — refuse destination write"
    )
    redo = str(rec.get("_df_sql_redo") or sql_redo or "")[:2000]
    return {
        "failure_reason": reason,
        "reason": reason,
        "original_value": {
            "op": op,
            "table": table,
            "sql_redo": redo,
            "parsed": {k: v for k, v in rec.items() if not str(k).startswith("_df_")},
        },
        "expected_type": "parsed_sql_redo_row",
        "actual_type": "unparsed_sql_redo",
        "transform_attempted": "logminer_sql_redo_parse",
        "recovery_suggestion": (
            "Inspect SQL_REDO in quarantine, confirm supplemental logging and "
            "the LogMiner dictionary, then replay. Unparsed redo is never "
            "written to the destination."
        ),
        "values": rec,
        "connector": "oracle",
        "source": "oracle_logminer",
    }


def classify_sql_redo(
    sql_redo: str,
    *,
    op: str,
    table: str = "",
) -> tuple[str, dict[str, Any]]:
    """Return ``('ok', row)`` or ``('unparsed', reject_detail)``."""
    if not str(sql_redo or "").strip():
        parsed = {
            UNPARSED_SQL_REDO_FLAG: "1",
            "_df_parse_error": "empty SQL_REDO",
        }
        return "unparsed", sql_redo_reject_detail(
            parsed, op=op, sql_redo=sql_redo, table=table
        )
    parsed = _parse_sql_redo(sql_redo or "", op=op)
    if is_unparsed_sql_redo(parsed):
        if sql_redo and not parsed.get("_df_sql_redo"):
            parsed["_df_sql_redo"] = str(sql_redo)[:2000]
        return "unparsed", sql_redo_reject_detail(
            parsed, op=op, sql_redo=sql_redo, table=table
        )
    return "ok", parsed


def encode_logminer_token(
    scn: int,
    *,
    table: str,
    phase: str = "streaming",
    rs_id: str = "",
    ssn: int = 0,
    offset: int = 0,
    last_pk: str = "",
    low_scn: int = 0,
    poll_scn: int = 0,
    poll_rs_id: str = "",
    poll_ssn: int = 0,
    commit_scn: int = 0,
    commit_rs_id: str = "",
    commit_ssn: int = 0,
    commit_xid: str = "",
    txn_buffer: bool = False,
    scn_complete: bool = False,
) -> str:
    """Encode a LogMiner resume token.

    Debezium's Oracle connector commits ``(scn, rsId, ssn)`` — SCN alone is not
    unique per change. Carrying ``rs_id``/``ssn`` lets a truncated batch resume
    mid-SCN instead of replaying (or, worse, skipping) sibling rows.

    Snapshot progress is ``last_pk`` (PK-seek) plus legacy ``offset``. Those
    fields are snapshot-only — streaming handoff must not carry them.
    """
    payload: dict[str, Any] = {
        "kind": "oracle-logminer",
        "table": table,
        "scn": int(scn),
        "phase": phase,
    }
    payload["scn_complete"] = bool(scn_complete)
    if rs_id:
        payload["rs_id"] = str(rs_id)
        payload["ssn"] = int(ssn or 0)
    if low_scn and int(low_scn) != int(scn):
        # Oldest open transaction start SCN (transaction-buffer mode). A
        # restart re-mines from here; exact (commit_scn, xid) matches are
        # suppressed, while distinct late commits are preserved.
        payload["low_scn"] = int(low_scn)
    if poll_scn:
        payload["poll_scn"] = int(poll_scn)
        if poll_rs_id:
            payload["poll_rs_id"] = str(poll_rs_id)
            payload["poll_ssn"] = int(poll_ssn or 0)
    if commit_scn:
        payload["commit_scn"] = int(commit_scn)
        if commit_rs_id:
            payload["commit_rs_id"] = str(commit_rs_id)
            payload["commit_ssn"] = int(commit_ssn or 0)
        if commit_xid:
            payload["commit_xid"] = str(commit_xid)
    if txn_buffer:
        payload["txn_buffer"] = True
    if phase == "snapshot":
        if offset:
            payload["offset"] = int(offset)
        if last_pk:
            payload["last_pk"] = str(last_pk)
    return json.dumps(payload, separators=(",", ":"))


def decode_logminer_token(token: Any) -> dict[str, Any]:
    empty = {
        "scn": 0,
        "phase": "initial",
        "table": "",
        "rs_id": "",
        "ssn": 0,
        "offset": 0,
        "last_pk": "",
        "low_scn": 0,
        "poll_scn": 0,
        "poll_rs_id": "",
        "poll_ssn": 0,
        "commit_scn": 0,
        "commit_rs_id": "",
        "commit_ssn": 0,
        "commit_xid": "",
        "txn_buffer": False,
        "scn_complete": False,
    }
    if not token:
        return empty
    try:
        from services.cdc_resume_tokens import unwrap_resume_token

        data = unwrap_resume_token(token)
        if not isinstance(data, dict):
            parsed = json.loads(str(data))
            data = unwrap_resume_token(parsed)
        if isinstance(data, dict) and data.get("kind") != "oracle-logminer":
            logger.warning(
                "Ignoring resume token with unexpected kind=%r",
                data.get("kind"),
            )
        if isinstance(data, dict) and data.get("kind") == "oracle-logminer":
            scn = int(data.get("scn") or 0)
            phase = str(data.get("phase") or "streaming")
            rs_id = str(data.get("rs_id") or "")
            scn_complete = (
                bool(data.get("scn_complete"))
                if "scn_complete" in data
                else phase == "streaming" and not rs_id and scn > 0
            )
            return {
                "scn": scn,
                "phase": phase,
                "table": str(data.get("table") or ""),
                "rs_id": rs_id,
                "ssn": int(data.get("ssn") or 0),
                "offset": int(data.get("offset") or 0),
                "last_pk": str(data.get("last_pk") or ""),
                # Legacy tokens predate the buffer: no open txns to re-mine.
                "low_scn": int(data.get("low_scn") or 0) or scn,
                "poll_scn": int(data.get("poll_scn") or 0),
                "poll_rs_id": str(data.get("poll_rs_id") or ""),
                "poll_ssn": int(data.get("poll_ssn") or 0),
                "commit_scn": int(data.get("commit_scn") or 0),
                "commit_rs_id": str(data.get("commit_rs_id") or ""),
                "commit_ssn": int(data.get("commit_ssn") or 0),
                "commit_xid": str(data.get("commit_xid") or ""),
                "txn_buffer": bool(data.get("txn_buffer")),
                "scn_complete": scn_complete,
            }
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return dict(empty)


def _logminer_options_sql(*, committed_only: bool = True) -> str:
    """START_LOGMNR OPTIONS for committed-only mining without CONTINUOUS_MINE.

    ``CONTINUOUS_MINE`` was desupported in 19c — on those hosts START_LOGMNR
    raises and the prior ``except: return`` path made CDC look healthy while
    delivering nothing forever. ``COMMITTED_DATA_ONLY`` excludes in-flight and
    rolled-back DML so a ROLLBACK cannot land as a real destination change.

    ``committed_only=False`` is the opt-in transaction-buffer mode: the reader
    mines in-flight redo and does the commit/rollback filtering itself, so a
    transaction that commits after the cursor has passed its DML SCNs is still
    delivered (see :mod:`connectors.oracle_logminer_txn`).
    """
    if not committed_only:
        return "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
    return (
        "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    )


def _logminer_warning(
    step: str,
    *,
    reader_identity: str,
    start_scn: int,
    end_scn: int,
    step_context: str,
    exc: BaseException,
) -> None:
    logger.warning(
        "Oracle LogMiner failure step=%s reader=%s SCN=[%s,%s] context=%s: %s",
        step,
        reader_identity or "<unknown>",
        start_scn,
        end_scn,
        step_context or "<unspecified>",
        exc,
        exc_info=exc.__traceback__ is not None,
    )


_CURRENT_INCARNATION_FILTER = """
              AND RESETLOGS_CHANGE# = (
                    SELECT RESETLOGS_CHANGE# FROM V$DATABASE
              )
"""


def register_logminer_logs(
    cur: Any,
    *,
    start_scn: int,
    end_scn: int,
    reader_identity: str = "",
    step_context: str = "",
) -> int:
    """Register one redo file per thread/sequence covering the SCN window.

    Available archived copies take precedence over online members. Returns the
    number of files registered; zero is a hard failure because mining without
    files yields an empty contents view and a silent no-op poll.

    Raises if neither catalog view could be read, so an unreadable catalog is
    never mistaken for "no redo covers this window".
    """
    start = int(start_scn or 0)
    end = int(end_scn or 0)
    online_files: dict[tuple[int, int], str] = {}
    archived_files: dict[tuple[int, int], tuple[int, str]] = {}
    listing_errors = 0
    try:
        cur.execute(
            """
            SELECT l.THREAD#, l.SEQUENCE#, lf.MEMBER
            FROM V$LOGFILE lf
            JOIN V$LOG l ON l.GROUP# = lf.GROUP#
            WHERE l.STATUS <> 'UNUSED'
              AND l.FIRST_CHANGE# > 0
              AND l.FIRST_CHANGE# <= :end_scn
              AND (l.NEXT_CHANGE# IS NULL OR l.NEXT_CHANGE# > :start_scn)
            ORDER BY l.THREAD#, l.SEQUENCE#, lf.MEMBER
            """,
            {"start_scn": start, "end_scn": end},
        )
        for row in cur.fetchall() or []:
            if not row or len(row) < 3 or not row[2]:
                continue
            key = (int(row[0]), int(row[1]))
            path = str(row[2])
            current = online_files.get(key)
            if current is None or path < current:
                online_files[key] = path
    except Exception as exc:
        listing_errors += 1
        logger.debug("Oracle V$LOGFILE listing failed: %s", exc)
    try:
        cur.execute(
            """
            SELECT THREAD#, SEQUENCE#, DEST_ID, STATUS, NAME
            FROM V$ARCHIVED_LOG
            WHERE DELETED = 'NO'
              AND STANDBY_DEST = 'NO'
              AND STATUS = 'A'
            """
            + _CURRENT_INCARNATION_FILTER
            + """
              AND FIRST_CHANGE# <= :end_scn
              AND (NEXT_CHANGE# IS NULL OR NEXT_CHANGE# > :start_scn)
            ORDER BY THREAD#, SEQUENCE#, DEST_ID, NAME
            """,
            {"start_scn": start, "end_scn": end},
        )
        for row in cur.fetchall() or []:
            if not row or len(row) < 5 or not row[4]:
                continue
            key = (int(row[0]), int(row[1]))
            candidate = (int(row[2] or 0), str(row[4]))
            current = archived_files.get(key)
            if current is None or candidate < current:
                archived_files[key] = candidate
    except Exception as exc:
        listing_errors += 1
        logger.debug("Oracle V$ARCHIVED_LOG listing failed: %s", exc)

    if not online_files and not archived_files and listing_errors >= 2:
        error = RuntimeError(
            "Oracle LogMiner could not read V$LOGFILE or V$ARCHIVED_LOG; "
            "grant SELECT on the redo catalog views before mining."
        )
        _logminer_warning(
            "add_logfile",
            reader_identity=reader_identity,
            start_scn=start,
            end_scn=end,
            step_context=step_context,
            exc=error,
        )
        raise error

    selected = dict(online_files)
    selected.update(
        (key, archive[1]) for key, archive in archived_files.items()
    )
    ordered_paths = [selected[key] for key in sorted(selected)]

    for index, path in enumerate(ordered_paths):
        add_option = "DBMS_LOGMNR.NEW" if index == 0 else "DBMS_LOGMNR.ADDFILE"
        try:
            cur.execute(
                f"""
                BEGIN
                  DBMS_LOGMNR.ADD_LOGFILE(
                    LOGFILENAME => :fname,
                    OPTIONS => {add_option}
                  );
                END;
                """,
                {"fname": path},
            )
        except Exception as exc:
            _logminer_warning(
                "add_logfile",
                reader_identity=reader_identity,
                start_scn=start,
                end_scn=end,
                step_context=step_context,
                exc=exc,
            )
            raise
    return len(ordered_paths)


def start_logminer_session(
    cur: Any,
    *,
    start_scn: int,
    end_scn: int,
    committed_only: bool = True,
    reader_identity: str = "",
    step_context: str = "",
) -> None:
    """Start a committed-data-only LogMiner session over the SCN window.

    Raises on failure — callers must not treat a failed start as an empty poll.
    """
    try:
        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
    except Exception as exc:
        logger.debug("No prior Oracle LogMiner session to end: %s", exc)
    registered = register_logminer_logs(
        cur,
        start_scn=start_scn,
        end_scn=end_scn,
        reader_identity=reader_identity,
        step_context=step_context,
    )
    if registered <= 0:
        # The catalog was readable but no redo covers the window — the changes
        # we need have aged out. That is a redo gap, so raise the gap error the
        # resume path already knows how to handle (re-snapshot) rather than a
        # generic failure that would retry the same empty window forever.
        error = CdcScnGapError(
            f"Oracle LogMiner found no redo covering SCN [{start_scn}, {end_scn}]; "
            "the required redo has aged out — a fresh snapshot is required."
        )
        _logminer_warning(
            "add_logfile",
            reader_identity=reader_identity,
            start_scn=int(start_scn),
            end_scn=int(end_scn),
            step_context=step_context,
            exc=error,
        )
        raise error
    options = _logminer_options_sql(committed_only=committed_only)
    try:
        cur.execute(
            f"""
            BEGIN
              DBMS_LOGMNR.START_LOGMNR(
                STARTSCN => :start_scn,
                ENDSCN => :end_scn,
                OPTIONS => {options}
              );
            END;
            """,  # nosec B608 — options is a fixed constant from _logminer_options_sql
            {"start_scn": int(start_scn), "end_scn": int(end_scn)},
        )
    except Exception as exc:
        _logminer_warning(
            "start_logminer",
            reader_identity=reader_identity,
            start_scn=int(start_scn),
            end_scn=int(end_scn),
            step_context=step_context,
            exc=exc,
        )
        raise


@contextmanager
def _logminer_session(
    cur: Any,
    *,
    start_scn: int,
    end_scn: int,
    committed_only: bool = True,
    reader_identity: str = "",
    step_context: str = "",
) -> Iterator[None]:
    try:
        start_logminer_session(
            cur,
            start_scn=start_scn,
            end_scn=end_scn,
            committed_only=committed_only,
            reader_identity=reader_identity,
            step_context=step_context,
        )
        try:
            yield
        except Exception as exc:
            _logminer_warning(
                "mine",
                reader_identity=reader_identity,
                start_scn=int(start_scn),
                end_scn=int(end_scn),
                step_context=step_context,
                exc=exc,
            )
            raise
    finally:
        try:
            cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
        except Exception as exc:
            _logminer_warning(
                "end_logminer",
                reader_identity=reader_identity,
                start_scn=int(start_scn),
                end_scn=int(end_scn),
                step_context=step_context,
                exc=exc,
            )


def _canonical_rs_id(rs_id: str) -> str:
    raw = str(rs_id or "")
    if not raw.strip():
        return ""
    parts = parse_oracle_rs_id(raw)
    if parts is None or parts[1] > 0xFFFFFFFF or parts[2] > 0xFFFF:
        raise OracleLogMinerReadError(
            ValueError(f"Invalid Oracle RS_ID {raw!r}"),
            message=(
                f"Oracle LogMiner RS_ID {raw!r} violates the canonical "
                "ordering assumption."
            ),
        )
    seq, blk, off = parts
    return f" 0x{seq:06x}.{blk:08x}.{off:04x} "


def _row_rs_id(row_value: Any) -> str:
    raw = str(row_value or "")
    if not raw.strip():
        return ""
    canonical = _canonical_rs_id(raw)
    if raw != canonical:
        raise OracleLogMinerReadError(
            ValueError(f"Noncanonical Oracle RS_ID {raw!r}"),
            message=(
                f"Oracle LogMiner RS_ID {raw!r} violates the canonical "
                "ordering assumption."
            ),
        )
    return raw


_RS_ID_RESUME_PREDICATE = """
    (
          SCN > :start_scn
       OR (SCN = :start_scn AND :rs_id IS NULL AND :scn_complete = 0)
       OR (
            SCN = :start_scn
            AND (
                 LENGTH(RS_ID) > :rs_len
              OR (LENGTH(RS_ID) = :rs_len AND RS_ID > :rs_id)
              OR (RS_ID = :rs_id AND SSN > :ssn)
            )
          )
    )
"""
_RS_ID_ORDER_BY = "ORDER BY SCN, LENGTH(RS_ID), RS_ID, SSN, DF_RN"


def logminer_contents_sql(
    *,
    table_predicate: str,
    limit_bind: str = ":lim",
    include_xid: bool = False,
) -> str:
    """Build the contents query with ORDER BY inside, ROWNUM outside.

    Oracle applies ``ROWNUM`` before ``ORDER BY`` in a flat SELECT, so a
    ``ROWNUM <= :lim ... ORDER BY SCN`` keeps an arbitrary mining-order subset
    and then sorts it — the retained rows are not the oldest. Pushing the
    order into an inline view and the cap outside is the Debezium-class fix.
    Canonical RS_ID values sort numerically by length then text; ``DF_RN``
    keeps continuation rows sharing a position stable.
    """
    xid_cols = ", XIDUSN, XIDSLT, XIDSQN" if include_xid else ""
    return f"""
        SELECT SCN, RS_ID, SSN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER{xid_cols},
               SEG_NAME, TIMESTAMP, CSF
        FROM (
          SELECT SCN, RS_ID, SSN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER{xid_cols},
                 SEG_NAME, TIMESTAMP, CSF, ROWNUM AS DF_RN
          FROM v$logmnr_contents
          WHERE SEG_OWNER = :owner
            AND ({table_predicate})
            AND (
              OPERATION IN ('INSERT','UPDATE','DELETE','SEL_LOB_LOCATOR')
              OR OPERATION = 'DDL'
            )
            AND {_RS_ID_RESUME_PREDICATE}
          {_RS_ID_ORDER_BY}
        )
        WHERE ROWNUM <= {limit_bind}
    """  # nosec B608 — table_predicate / limit_bind are caller-built from safe ids


def logminer_txn_contents_sql(
    *,
    table_predicate: str,
    limit_bind: str = ":lim",
) -> str:
    """Contents query for transaction-buffered mining (lifecycle, DML, and DDL).

    Without ``COMMITTED_DATA_ONLY`` the reader must see ``START`` / ``COMMIT``
    / ``ROLLBACK`` records to know a transaction's fate. Those rows carry no
    ``SEG_OWNER``/``TABLE_NAME``, so the owner/table filter only applies to the
    DML rows. ``ROLLBACK = 1`` marks compensating redo of a partial (savepoint)
    rollback, which is kept in order so the committed net state is correct.
    """
    return f"""
        SELECT SCN, RS_ID, SSN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER,
               XIDUSN, XIDSLT, XIDSQN, "ROLLBACK", ROW_ID, SEG_NAME, TIMESTAMP,
               CSF
        FROM (
          SELECT SCN, RS_ID, SSN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER,
                 XIDUSN, XIDSLT, XIDSQN, "ROLLBACK", ROW_ID, SEG_NAME, TIMESTAMP,
                 CSF, ROWNUM AS DF_RN
          FROM v$logmnr_contents
          WHERE {_RS_ID_RESUME_PREDICATE}
            AND (
                  (
                    SEG_OWNER = :owner
                    AND ({table_predicate})
                    AND OPERATION IN ('INSERT','UPDATE','DELETE','SEL_LOB_LOCATOR')
                  )
               OR (
                    SEG_OWNER = :owner
                    AND ({table_predicate})
                    AND OPERATION = 'DDL'
                  )
               OR (
                    OPERATION IN ('START','COMMIT','ROLLBACK')
                    AND (XIDUSN, XIDSLT, XIDSQN) IN (
                      SELECT XIDUSN, XIDSLT, XIDSQN
                      FROM v$logmnr_contents
                      WHERE SEG_OWNER = :owner
                        AND ({table_predicate})
                        AND OPERATION IN ('INSERT','UPDATE','DELETE','SEL_LOB_LOCATOR')
                    )
                  )
            )
          {_RS_ID_ORDER_BY}
        )
        WHERE ROWNUM <= {limit_bind}
    """  # nosec B608 — table_predicate / limit_bind are caller-built from safe ids


def _logminer_position_key(row: Any) -> tuple[int, str, int]:
    return int(row[0] or 0), _row_rs_id(row[1]), int(row[2] or 0)


def _logminer_row_has_csf(row: Any, csf_index: int) -> bool:
    index = csf_index
    if len(row) <= index:
        # Keep the fake-reader and legacy adapter shape usable while real
        # contents queries include XID columns before CSF.
        index = 9 if csf_index == 12 and len(row) > 9 else -1
    if index < 0:
        return False
    try:
        return int(row[index] or 0) == 1
    except (TypeError, ValueError):
        return str(row[index] or "").strip() == "1"


def _raise_logminer_window_error(
    *,
    table: str,
    cursor_key: str,
    key: tuple[int, str, int],
    reason: str,
) -> None:
    scn, rs_id, _ssn = key
    message = (
        f"Oracle LogMiner {reason} at SCN {scn}, RS_ID {rs_id!r}."
    )
    raise OracleLogMinerReadError(
        ValueError(message),
        table=table,
        cursor_key=cursor_key,
        message=message,
    )


def _reassemble_logminer_csf_rows(
    rows: list[Any],
    *,
    csf_index: int,
    table: str,
    cursor_key: str,
) -> list[Any]:
    reassembled: list[Any] = []
    fragments: list[Any] = []
    fragment_key: tuple[int, str, int] | None = None

    for row in rows:
        key = _logminer_position_key(row)
        if fragments and mining_position(*key) != mining_position(*fragment_key):
            _raise_logminer_window_error(
                table=table,
                cursor_key=cursor_key,
                key=fragment_key or key,
                reason="returned an incomplete SQL_REDO statement (CSF=1)",
            )
        if _logminer_row_has_csf(row, csf_index):
            if not fragments:
                fragment_key = key
            fragments.append(row)
            continue
        if fragments:
            fragments.append(row)
            joined = list(row)
            joined[4] = "".join(str(fragment[4] or "") for fragment in fragments)
            reassembled.append(tuple(joined))
            fragments.clear()
            fragment_key = None
        else:
            reassembled.append(row)

    if fragments:
        _raise_logminer_window_error(
            table=table,
            cursor_key=cursor_key,
            key=fragment_key or _logminer_position_key(fragments[-1]),
            reason="returned an incomplete SQL_REDO statement (CSF=1)",
        )
    return reassembled


def _fetch_logminer_csf_window(
    cur: Any,
    *,
    sql: str,
    binds: dict[str, Any],
    limit: int,
    csf_index: int,
    table: str,
    cursor_key: str,
) -> tuple[list[Any], bool]:
    configured_limit = max(1, int(limit))
    window_limit = configured_limit
    max_limit = configured_limit * 64

    while True:
        cur.execute(sql, {**binds, "lim": window_limit})
        rows = list(cur.fetchall() or [])
        full = len(rows) == window_limit
        if full:
            last_key = _logminer_position_key(rows[-1])
            first_trailing = len(rows) - 1
            while (
                first_trailing > 0
                and mining_position(*_logminer_position_key(rows[first_trailing - 1]))
                == mining_position(*last_key)
            ):
                first_trailing -= 1
            rows = rows[:first_trailing]
            if not rows:
                if window_limit >= max_limit:
                    _raise_logminer_window_error(
                        table=table,
                        cursor_key=cursor_key,
                        key=last_key,
                        reason="key group exceeds the 64x window cap",
                    )
                window_limit = min(window_limit * 2, max_limit)
                continue
        elif rows and _logminer_row_has_csf(rows[-1], csf_index):
            _raise_logminer_window_error(
                table=table,
                cursor_key=cursor_key,
                key=_logminer_position_key(rows[-1]),
                reason="returned an incomplete SQL_REDO statement (CSF=1)",
            )

        return (
            _reassemble_logminer_csf_rows(
                rows,
                csf_index=csf_index,
                table=table,
                cursor_key=cursor_key,
            ),
            full,
        )


def _ordered_txn_ddl_actions(
    committed: list[Any],
    ddl_events: list[tuple[int, str, int, str, str, str, Any]],
) -> list[tuple[tuple[int, str, int], int, str, Any]]:
    actions: list[tuple[tuple, int, str, Any]] = [
        (txn.position, 0, "txn", txn) for txn in committed
    ]
    actions.extend(
        (mining_position(event[0], event[1], event[2]), 1, "ddl", event)
        for event in ddl_events
    )
    return sorted(actions, key=lambda action: (action[0], action[1]))


# LogMiner / redo missing-file class errors (Oracle).
_ORA_REDO_GAP_CODES = (
    "ORA-01291",  # missing logfile
    "ORA-01292",  # no log file found
    "ORA-01284",  # file cannot be opened
    "ORA-01332",  # LogMiner dictionary build from redo failed / SCN out of range
)


def is_oracle_redo_gap_error(exc: BaseException) -> bool:
    msg = str(exc).upper()
    return any(code in msg for code in _ORA_REDO_GAP_CODES)


def assert_resume_scn_in_redo(
    resume_scn: int,
    oldest_available_scn: int | None,
    *,
    cursor_key: str = "",
) -> None:
    """Raise :class:`CdcScnGapError` when resume is strictly before retained redo.

    ``oldest_available_scn`` is the min ``FIRST_CHANGE#`` across online + archived
    redo still present. ``None`` / ``0`` means undetermined — do not false-positive.
    """
    resume = int(resume_scn or 0)
    oldest = int(oldest_available_scn or 0)
    if resume <= 0 or oldest <= 0:
        return
    if resume < oldest:
        raise CdcScnGapError(
            "Oracle LogMiner resume SCN is before available redo "
            f"(resume={resume}, oldest_available={oldest}). Likely archived log "
            "purge or RAC/Data Guard failover gap — re-snapshot / reset watermark; "
            "do not claim continuous CDC across the gap.",
            resume_scn=resume,
            oldest_scn=oldest,
            cursor_key=cursor_key,
        )


def fetch_oldest_available_scn(cur: Any) -> int | None:
    """Return oldest ``FIRST_CHANGE#`` still present in online/archived redo.

    Fail-open (``None``) when ``V$LOG`` / ``V$ARCHIVED_LOG`` are unavailable so
    privilege gaps do not block CDC; gap detection still covers LogMiner ORA-*.
    """
    candidates: list[int] = []
    try:
        cur.execute("SELECT MIN(FIRST_CHANGE#) FROM V$LOG")
        row = cur.fetchone()
        if row and row[0] is not None:
            candidates.append(int(row[0]))
    except Exception as exc:
        logger.debug("Oracle V$LOG FIRST_CHANGE# unavailable: %s", exc)
    try:
        cur.execute(
            """
            SELECT MIN(FIRST_CHANGE#)
            FROM V$ARCHIVED_LOG
            WHERE DELETED = 'NO'
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            candidates.append(int(row[0]))
    except Exception as exc:
        logger.debug("Oracle V$ARCHIVED_LOG FIRST_CHANGE# unavailable: %s", exc)
    if not candidates:
        return None
    return min(candidates)


def fetch_redo_inventory(cur: Any) -> list[tuple[int, int, int, int, str]] | None:
    """Return deduplicated per-thread online and archived redo ranges."""
    inventory: dict[tuple[int, int], tuple[int, int, int, int, str]] = {}
    try:
        cur.execute(
            """
            SELECT THREAD#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#
            FROM V$ARCHIVED_LOG
            WHERE DELETED = 'NO'
              AND STANDBY_DEST = 'NO'
            """
            + _CURRENT_INCARNATION_FILTER
        )
        for row in cur.fetchall() or []:
            thread, sequence, first_change, next_change = row
            key = (int(thread), int(sequence))
            inventory.setdefault(
                key,
                (key[0], key[1], int(first_change), int(next_change), "archived"),
            )
        cur.execute(
            """
            SELECT THREAD#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#
            FROM V$LOG
            WHERE STATUS <> 'UNUSED'
              AND FIRST_CHANGE# > 0
            """
        )
        for row in cur.fetchall() or []:
            thread, sequence, first_change, next_change = row
            key = (int(thread), int(sequence))
            inventory[key] = (
                key[0],
                key[1],
                int(first_change),
                int(next_change),
                "online",
            )
    except Exception as exc:
        logger.warning("Oracle per-thread redo inventory is unavailable: %s", exc)
        return None
    return sorted(inventory.values(), key=lambda item: (item[0], item[1]))


_REDO_INVENTORY_UNVERIFIED_WARNED: set[str] = set()


def assert_redo_continuity(
    resume_scn: int,
    inventory: list[tuple[int, int, int, int, str]] | None,
    *,
    cursor_key: str = "",
) -> None:
    """Raise :class:`CdcScnGapError` when retained redo has a thread sequence hole."""
    resume = int(resume_scn or 0)
    if resume <= 0:
        return

    def warn_unverified(reason: str) -> None:
        warning_key = str(cursor_key or "<unknown>")
        if warning_key in _REDO_INVENTORY_UNVERIFIED_WARNED:
            return
        _REDO_INVENTORY_UNVERIFIED_WARNED.add(warning_key)
        logger.warning(
            "Oracle redo inventory %s; continuity is unverified for cursor %s",
            reason,
            warning_key,
        )

    if not inventory:
        warn_unverified("is empty or undetermined")
        return

    by_thread: dict[int, dict[int, tuple[int, int, int, int, str]]] = {}
    for row in inventory:
        try:
            thread, sequence, first_change, next_change, source = row
            thread, sequence = int(thread), int(sequence)
            first_change, next_change = int(first_change), int(next_change)
            if thread <= 0 or sequence <= 0 or next_change <= first_change:
                raise ValueError("invalid redo range")
            entries = by_thread.setdefault(thread, {})
            candidate = (thread, sequence, first_change, next_change, str(source))
            current = entries.get(sequence)
            if current is None or candidate[4] == "online":
                entries[sequence] = candidate
        except (TypeError, ValueError, IndexError):
            logger.warning(
                "Skipping invalid Oracle redo inventory row for cursor %s: %r",
                cursor_key or "<unknown>",
                row,
            )
    if not by_thread:
        warn_unverified("contains no valid rows")
        return

    def raise_gap(thread: int, first_missing: int, last_missing: int) -> None:
        raise CdcScnGapError(
            "Oracle redo continuity gap on thread "
            f"{thread}: missing sequence range {first_missing}-{last_missing} "
            f"for resume SCN {resume}. Restore the archived logs with "
            "RMAN RESTORE ARCHIVELOG or re-snapshot.",
            resume_scn=resume,
            oldest_scn=0,
            cursor_key=cursor_key,
        )

    for thread, sequence_map in sorted(by_thread.items()):
        logs = [sequence_map[seq] for seq in sorted(sequence_map)]
        earliest = logs[0]
        if earliest[2] > resume and earliest[1] > 1:
            raise_gap(thread, 1, earliest[1] - 1)

        containing = [log for log in logs if log[2] <= resume < log[3]]
        if containing:
            resume_sequence = max(log[1] for log in containing)
        else:
            before_resume = [log for log in logs if log[2] <= resume]
            if not before_resume:
                continue
            resume_sequence = max(log[1] for log in before_resume)
        later_sequences = [log[1] for log in logs if log[1] > resume_sequence]
        previous = resume_sequence
        for sequence in later_sequences:
            if sequence > previous + 1:
                raise_gap(thread, previous + 1, sequence - 1)
            previous = sequence


_SET_RE = re.compile(r'"?(\w+)"?\s*=\s*(?:\'([^\']*)\'|([^\s,]+))')


def _split_sql_csv_aware(text: str) -> list[str]:
    """Split a SQL list on commas outside quotes and parentheses.

    Handles ``'a,b'``, ``''`` escapes, and ``TO_DATE('…','…')`` so LogMiner
    INSERT/UPDATE parsing does not corrupt valid CDC rows (Airbyte-class gap).
    """
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_str = False
    i = 0
    while i < len(text):
        ch = text[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    buf.append(text[i + 1])
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            i += 1
            continue
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            i += 1
            continue
        # LogMiner WHERE uses ``col = val AND col = val`` (not commas).
        if (
            depth == 0
            and text[i : i + 3].upper() == "AND"
            and (i == 0 or not text[i - 1].isalnum())
            and (i + 3 >= len(text) or not text[i + 3].isalnum())
        ):
            parts.append("".join(buf).strip())
            buf = []
            i += 3
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail or parts:
        parts.append(tail)
    return parts


def _unquote_sql_literal(value: str) -> str:
    v = (value or "").strip().rstrip(";").strip()
    if v.upper() == "NULL":
        # SQL NULL is the wire NULL sentinel, never '' (D203 family).
        return wire_cell(None)
    if not v:
        return ""
    if len(v) >= 2 and v[0] == "'" and v[-1] == "'":
        return v[1:-1].replace("''", "'")
    return v


def _parse_sql_redo(sql_redo: str, *, op: str) -> dict[str, str]:
    """Column extraction from LogMiner SQL_REDO text (quoted / function-aware)."""
    out: dict[str, str] = {}
    if not sql_redo:
        return out
    text = sql_redo
    if op == "insert" and "VALUES" in text.upper():
        # INSERT INTO t("A","B") VALUES('1','a,b') / TO_DATE(...)
        cols_m = re.search(r"\((.*)\)\s*VALUES\s*\((.*)\)\s*$", text, re.I | re.S)
        if not cols_m:
            # Fallback: first (...) VALUES (...) pair (legacy LogMiner shapes).
            cols_m = re.search(r"\(([^;]+)\)\s*VALUES\s*\(([^;]+)\)", text, re.I | re.S)
        if cols_m:
            cols = [c.strip().strip('"') for c in _split_sql_csv_aware(cols_m.group(1))]
            vals = [_unquote_sql_literal(v) for v in _split_sql_csv_aware(cols_m.group(2))]
            if len(cols) != len(vals):
                # Refuse to invent misaligned columns — surface as unparsed.
                return {
                    UNPARSED_SQL_REDO_FLAG: "1",
                    "_df_parse_error": f"insert col/val mismatch ({len(cols)} vs {len(vals)})",
                    "_df_sql_redo": text[:2000],
                }
            for c, v in zip(cols, vals):
                if c:
                    out[c.upper()] = v
            return out
    # UPDATE … SET "COL"=… / DELETE … WHERE "COL"=…
    # Parse WHERE first (row identity / old values) then SET (new values) so a
    # column updated by SET overwrites the WHERE value, while still surfacing
    # primary-key columns from the WHERE clause for updates.
    set_m = re.search(r"\bSET\s+(.+?)(?:\s+WHERE\b|$)", text, re.I | re.S)
    where_m = re.search(r"\bWHERE\s+(.+)$", text, re.I | re.S)
    chunks: list[str] = []
    if where_m and op in {"update", "delete"}:
        chunks.extend(_split_sql_csv_aware(where_m.group(1)))
    if set_m:
        chunks.extend(_split_sql_csv_aware(set_m.group(1)))
    if chunks:
        for chunk in chunks:
            m = re.match(r'"?(\w+)"?\s*=\s*(.+)$', chunk.strip(), re.I | re.S)
            if not m:
                continue
            col = m.group(1).upper()
            out[col] = _unquote_sql_literal(m.group(2).strip())
        if out:
            return out
    for m in _SET_RE.finditer(text):
        col = m.group(1).upper()
        val = m.group(2) if m.group(2) is not None else m.group(3)
        out[col] = wire_cell(None) if val is None or str(val).upper() == "NULL" else str(val)
    if out:
        return out
    # Non-empty redo that yielded no columns is unparsed — never a dest upsert.
    if text.strip():
        return {
            UNPARSED_SQL_REDO_FLAG: "1",
            "_df_parse_error": f"unparsed {op} SQL_REDO",
            "_df_sql_redo": text[:2000],
        }
    return out


class OracleLogMinerCdc:
    """Continuous LogMiner mining between SCN watermarks.

    Pass ``table`` as a list (and optional ``primary_keys``) to share one
    LogMiner session across N tables — Debezium-class demux with ``ack_barrier``.
    LOB values are read by primary key at capture time. The final state converges,
    but between two changes a row may briefly carry a LOB value newer than its
    other columns. Delivery remains **at-least-once**.
    """

    def __init__(
        self,
        cfg: dict[str, Any],
        *,
        table: str | list[str],
        primary_key: str = "",
        primary_keys: dict[str, str] | None = None,
        schema: str = "",
        batch_size: int = 500,
        resume_token: Any = None,
        cursor_key: str = "",
    ) -> None:
        from services.cdc_multi_table import normalize_table_list, tables_digest
        from services.cdc_identity import require_cdc_primary_keys_map

        from services.cdc_table_name import bare_table_keys, split_cdc_tables

        self.cfg = cfg
        default_schema = schema or cfg.get("schema") or cfg.get("username") or ""
        # D223: 'APP.ORDERS' is table ORDERS in schema APP, joined once.
        resolved_schema, raw_tables = split_cdc_tables(
            normalize_table_list(table), default_schema, dialect="oracle"
        )
        if not raw_tables:
            raise ValueError("Oracle LogMiner CDC requires at least one table")
        self.tables = [t.upper() for t in raw_tables]
        self.table = self.tables[0]
        self.schema = (resolved_schema or "").upper()
        # Normalize caller keys to upper for Oracle identifiers.
        normalized_pks: dict[str, Any] | None = None
        if primary_keys:
            normalized_pks = {
                str(k).upper(): str(v).upper()
                for k, v in (bare_table_keys(primary_keys, default_schema) or {}).items()
                if k and v
            }
        mapped = require_cdc_primary_keys_map(
            self.tables,
            primary_key=(primary_key or "").upper() or None,
            primary_keys=normalized_pks,
        )
        self.primary_keys = {t: str(v).upper() for t, v in mapped.items()}
        self.primary_key = self.primary_keys[self.table]
        self.batch_size = max(1, int(batch_size or 500))
        self._shared = len(self.tables) > 1
        state = decode_logminer_token(resume_token)
        self.scn = int(state.get("scn") or 0)
        # Intra-SCN resume — required when a prior poll truncated mid-SCN.
        self.rs_id = str(state.get("rs_id") or "")
        self.ssn = int(state.get("ssn") or 0)
        self.scn_complete = bool(state.get("scn_complete"))
        # Oldest open transaction start SCN (transaction-buffer mode only).
        self.low_scn = int(state.get("low_scn") or 0) or self.scn
        from connectors.oracle_logminer_txn import oracle_txn_buffer_enabled

        self._txn_buffer_enabled = oracle_txn_buffer_enabled()
        self._open_txn_detection_checked = False
        self._open_txn_hold_warnings: dict[tuple[int, int, int], float] = {}
        self._poll_scn = int(state.get("poll_scn") or self.scn)
        self._poll_rs_id = str(state.get("poll_rs_id") or self.rs_id)
        self._poll_ssn = int(state.get("poll_ssn") or self.ssn)
        self._last_completed_window_scn = 0
        self._emitted_commit = (
            int(state.get("commit_scn") or (0 if state.get("txn_buffer") else self.scn)),
            str(state.get("commit_rs_id") or ("" if state.get("txn_buffer") else self.rs_id)),
            int(
                state.get("commit_ssn")
                or (0 if state.get("txn_buffer") else self.ssn)
            ),
        )
        self._emitted_commit_xid = str(state.get("commit_xid") or "")
        self._txn_buffer = None
        if self._txn_buffer_enabled:
            from connectors.oracle_logminer_txn import OracleTxnBuffer

            # The in-memory XID map is intentionally not serialized. Re-mine
            # from the low SCN's beginning; its RS_ID cannot inherit the later
            # emitted-commit position.
            self.scn = self.low_scn or self.scn
            self.rs_id = ""
            self.ssn = 0
            self.scn_complete = False
            self._txn_buffer = OracleTxnBuffer(
                emitted_commit=(self._emitted_commit[0], self._emitted_commit_xid)
                if self._emitted_commit[0] and self._emitted_commit_xid
                else None
            )
        self.phase = str(state.get("phase") or "initial")
        self.snapshot_offset = int(state.get("offset") or 0)
        self.snapshot_last_pk = str(state.get("last_pk") or "")
        self.snapshot_table = str(state.get("table") or "")
        self.resume_token = resume_token
        self._last_acked_token = resume_token
        self._last_event_at: datetime | None = None
        self.last_ddl_at: str | None = None
        self._lob_column_cache: dict[str, tuple[str, ...]] = {}
        self._lob_lookup_missing = 0
        self._lob_locator_xids: dict[tuple[str, int, int, int], None] = {}
        from services.cdc_schema_history import connection_fingerprint

        self.source_key = connection_fingerprint(
            {**cfg, "type": "oracle"},
            connector_id=str(cfg.get("connector_id") or ""),
        )
        digest = tables_digest(self.tables)
        self.cursor_key = cursor_key or (
            f"oracle-logminer-shared:{self.schema}:{digest}"
            if self._shared
            else f"oracle-logminer:{self.schema}.{self.table}"
        )
        from services.cdc_lease import (
            CdcLeaseGuard,
            oracle_cdc_resource,
            oracle_cdc_shared_resource,
        )

        host = str(cfg.get("host") or "")
        resource = (
            oracle_cdc_shared_resource(
                self.schema, self.tables, mode="logminer", host=host
            )
            if self._shared
            else oracle_cdc_resource(
                self.schema, self.table, mode="logminer", host=host
            )
        )
        self._lease = CdcLeaseGuard(
            cursor_key=self.cursor_key,
            resource=resource,
            holder_id=str(cfg.get("lease_holder_id") or ""),
            job_id=str(cfg.get("job_id") or ""),
            meta={
                "engine": "oracle_logminer",
                "table": self.table,
                "tables": list(self.tables),
                "shared_reader": self._shared,
            },
        )

    def _acquire_cdc_lease(self) -> None:
        self._lease.ensure()

    def close(self) -> None:
        self._lease.release()

    def cdc_metadata(self) -> dict[str, Any]:
        return {
            "plugin": "oracle_logminer",
            "phase": self.phase,
            "delivery": "at-least-once",
            "tables": list(self.tables),
            "shared_reader": self._shared,
            "last_ddl_at": self.last_ddl_at,
            "lob_lookup_missing": self._lob_lookup_missing,
            **self._lease.theater_fields(),
        }

    def _raise_lob_read_error(
        self,
        table: str,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        error = cause or ValueError(message)
        raise OracleLogMinerReadError(
            error,
            table=f"{self.schema}.{table}",
            cursor_key=self.cursor_key,
            message=message,
        ) from error

    def _required_lob_primary_key(self, table: str) -> str:
        table_name = str(table or self.table).upper()
        primary_key = self.primary_keys.get(table_name)
        if primary_key:
            return primary_key
        self._raise_lob_read_error(
            table_name,
            f"Oracle LogMiner LOB activity for {self.schema}.{table_name} "
            "requires a primary key; add a primary key before enabling LOB CDC.",
        )
        raise AssertionError("unreachable")

    def _remember_lob_locator_xid(
        self, table: str, xid_parts: tuple[int, int, int]
    ) -> None:
        if xid_parts == (0, 0, 0):
            return
        key = (str(table or "").upper(), *xid_parts)
        self._lob_locator_xids.pop(key, None)
        self._lob_locator_xids[key] = None
        while len(self._lob_locator_xids) > 4096:
            self._lob_locator_xids.pop(next(iter(self._lob_locator_xids)))

    def _has_lob_locator_xid(
        self, table: str, xid_parts: tuple[int, int, int]
    ) -> bool:
        return (str(table or "").upper(), *xid_parts) in self._lob_locator_xids

    def _lob_columns_for_table(self, table: str) -> list[str]:
        table_name = str(table or self.table).upper()
        cached = self._lob_column_cache.get(table_name)
        if cached is not None:
            return list(cached)
        try:
            snapshot = self._oracle_ddl_schema_snapshot(table_name)
        except Exception as exc:
            self._raise_lob_read_error(
                table_name,
                f"Oracle LogMiner could not discover LOB columns for "
                f"{self.schema}.{table_name}: {exc}",
                exc,
            )
        columns = snapshot.get("columns") if isinstance(snapshot, dict) else None
        lob_columns: list[str] = []
        for column in columns if isinstance(columns, list) else []:
            if not isinstance(column, dict):
                continue
            name = str(column.get("name") or "").strip()
            data_type = str(
                column.get("data_type") or column.get("type") or ""
            ).strip().upper()
            if name and data_type.split("(", 1)[0].strip() in {
                "CLOB",
                "NCLOB",
                "BLOB",
            }:
                lob_columns.append(name.upper())
        self._lob_column_cache[table_name] = tuple(lob_columns)
        return lob_columns

    def _parse_lob_locator_pk(
        self,
        sql_redo: str,
        primary_key: str,
        *,
        table: str | None = None,
    ) -> dict[str, str]:
        table_name = str(table or self.table).upper()
        pk = str(primary_key or "").upper()
        if not pk:
            pk = self._required_lob_primary_key(table_name)
        parsed = _parse_sql_redo(str(sql_redo or ""), op="update")
        if parsed.get(UNPARSED_SQL_REDO_FLAG) or pk not in parsed:
            self._raise_lob_read_error(
                table_name,
                "Oracle LogMiner SEL_LOB_LOCATOR could not parse primary key "
                f"{pk!r} for {self.schema}.{table_name}; refusing to emit it.",
            )
        return {pk: parsed[pk]}

    def _lookup_lob_rows_by_pk(
        self,
        table: str,
        primary_key: str,
        lob_columns: list[str],
        values: list[Any],
    ) -> dict[str, dict[str, str]]:
        unique_values: dict[str, Any] = {}
        for value in values:
            if value is not None:
                unique_values.setdefault(str(value), value)
        found: dict[str, dict[str, str]] = {}
        if not unique_values:
            return found
        selected_columns = [primary_key, *lob_columns]
        select_sql = ", ".join(
            quote_sql_identifier(column) for column in selected_columns
        )
        table_ref = self._qualified(table)
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    requested = list(unique_values.items())
                    for offset in range(0, len(requested), 500):
                        chunk = requested[offset : offset + 500]
                        binds = {
                            f"pk_{index}": value
                            for index, (_key, value) in enumerate(chunk)
                        }
                        placeholders = ", ".join(
                            f":pk_{index}" for index in range(len(chunk))
                        )
                        cur.execute(
                            f"SELECT {select_sql} FROM {table_ref} "
                            f"WHERE {quote_sql_identifier(primary_key)} "
                            f"IN ({placeholders})",
                            binds,
                        )
                        for row in cur.fetchall() or []:
                            key = str(row[0])
                            lob_values: dict[str, str] = {}
                            for index, column in enumerate(lob_columns, start=1):
                                value = row[index]
                                read_lob = getattr(value, "read", None)
                                if callable(read_lob):
                                    value = read_lob()
                                lob_values[column] = cell_to_string(
                                    value, preserve_sql_null=True
                                )
                            found[key] = lob_values
        except OracleLogMinerReadError:
            raise
        except Exception as exc:
            self._raise_lob_read_error(
                table,
                f"Oracle LogMiner LOB lookup failed for "
                f"{self.schema}.{table}: {exc}",
                exc,
            )
        return found

    @staticmethod
    def _event_row_value(row: dict[str, Any], column: str) -> Any:
        return next(
            (
                value
                for name, value in row.items()
                if str(name).upper() == column.upper()
            ),
            None,
        )

    @staticmethod
    def _set_event_row_value(row: dict[str, Any], column: str, value: Any) -> None:
        existing = next(
            (
                name
                for name in row
                if str(name).upper() == column.upper()
            ),
            column,
        )
        row[existing] = value

    @staticmethod
    def _drop_event_row_column(row: dict[str, Any], column: str) -> None:
        for name in tuple(row):
            if str(name).upper() == column.upper():
                row.pop(name, None)

    def _coalesce_lob_locator_events(
        self, events: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        dml_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        locator_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        locator_indexes: dict[tuple[str, str, str], list[int]] = {}

        def event_key(event: dict[str, Any]) -> tuple[str, str, str] | None:
            xid = event.get("xid")
            if xid is None or xid == "":
                return None
            table = str(event.get("table") or self.table).upper()
            primary_key = self.primary_keys.get(table, self.primary_key)
            row = event.get("row")
            if not primary_key or not isinstance(row, dict):
                return None
            value = self._event_row_value(row, primary_key)
            if value is None:
                return None
            xid_key = (
                ".".join(str(part) for part in xid)
                if isinstance(xid, (tuple, list))
                else str(xid)
            )
            return xid_key, table, str(value)

        for index, event in enumerate(events):
            row = event.get("row")
            if not isinstance(row, dict):
                continue
            operation = str(event.get("op") or "").lower()
            operation = {"c": "insert", "u": "update", "d": "delete"}.get(
                operation, operation
            )
            is_locator = bool(event.get("lob_locator")) or bool(
                row.get(_LOB_LOCATOR_MARKER)
            )
            key = event_key(event)
            if key is None:
                continue
            if is_locator:
                locator_by_key.setdefault(key, []).append(event)
                locator_indexes.setdefault(key, []).append(index)
            elif operation in {"insert", "update", "delete"}:
                dml_by_key.setdefault(key, []).append(event)

        drop_indexes: set[int] = set()
        for key, locator_events in locator_by_key.items():
            dml_events = dml_by_key.get(key, [])
            if dml_events:
                if not any(
                    str(event.get("op") or "").lower() in {"delete", "d"}
                    for event in dml_events
                ):
                    for event in dml_events:
                        row = event.get("row")
                        if not isinstance(row, dict):
                            continue
                        operation = str(event.get("op") or "").lower()
                        if operation in {"update", "u"}:
                            row[_LOB_LOOKUP_MARKER] = True
                drop_indexes.update(locator_indexes[key])
                continue
            indexes = locator_indexes[key]
            drop_indexes.update(indexes[:-1])

        return [
            event for index, event in enumerate(events) if index not in drop_indexes
        ]

    def _enrich_lob_lookup_events(
        self, events: list[dict[str, Any]]
    ) -> dict[str, list[str]]:
        candidates: dict[str, list[tuple[dict[str, Any], dict[str, Any], Any]]] = {}
        lob_columns_by_table: dict[str, list[str]] = {}
        primary_keys_by_table: dict[str, str] = {}
        for event in events:
            row = event.get("row")
            if not isinstance(row, dict):
                continue
            table_name = str(event.get("table") or self.table).upper()
            locator_trigger = bool(event.pop("lob_locator", False))
            locator_trigger = bool(row.pop(_LOB_LOCATOR_MARKER, False)) or locator_trigger
            force_lookup = bool(row.pop(_LOB_LOOKUP_MARKER, False))
            operation = str(event.get("op") or "").lower()
            operation = {"c": "insert", "u": "update", "d": "delete"}.get(
                operation, operation
            )
            if operation not in {"insert", "update"}:
                continue
            lob_columns = self._lob_columns_for_table(table_name)
            if not lob_columns:
                if locator_trigger:
                    self._raise_lob_read_error(
                        table_name,
                        "Oracle LogMiner SEL_LOB_LOCATOR did not resolve to "
                        f"LOB columns for {self.schema}.{table_name}.",
                    )
                continue
            lob_names = {column.upper() for column in lob_columns}
            references_lob = (
                locator_trigger
                or force_lookup
                or operation == "insert"
                or any(str(column).upper() in lob_names for column in row)
            )
            if not references_lob:
                continue
            primary_key = self._required_lob_primary_key(table_name)
            pk_value = self._event_row_value(row, primary_key)
            if pk_value is None:
                self._raise_lob_read_error(
                    table_name,
                    f"Oracle LogMiner LOB {operation} for {self.schema}.{table_name} "
                    f"does not contain primary key {primary_key!r}.",
                )
            if locator_trigger:
                for column in tuple(row):
                    if str(column).upper() != primary_key.upper():
                        row.pop(column, None)
            candidates.setdefault(table_name, []).append((event, row, pk_value))
            lob_columns_by_table[table_name] = lob_columns
            primary_keys_by_table[table_name] = primary_key

        for table_name, table_candidates in candidates.items():
            lob_columns = lob_columns_by_table[table_name]
            primary_key = primary_keys_by_table[table_name]
            found = self._lookup_lob_rows_by_pk(
                table_name,
                primary_key,
                lob_columns,
                [candidate[2] for candidate in table_candidates],
            )
            for _event, row, pk_value in table_candidates:
                values = found.get(str(pk_value))
                if values is None:
                    for column in lob_columns:
                        self._drop_event_row_column(row, column)
                    self._lob_lookup_missing += 1
                    continue
                for column in lob_columns:
                    self._set_event_row_value(row, column, values[column])
        return {
            table_name: list(columns)
            for table_name, columns in lob_columns_by_table.items()
        }

    @staticmethod
    def _logminer_row_xid(row: Any) -> tuple[int, int, int] | None:
        try:
            values = tuple(int(value or 0) for value in row[7:10])
        except (TypeError, ValueError):
            return None
        return values if len(values) == 3 else None

    def _conn(self):
        from connectors.generic_sql import get_connection

        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 1521,
            database=self.cfg.get("database") or self.cfg.get("service_name") or "ORCL",
            username=self.cfg.get("username") or "",
            password=self.cfg.get("password") or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
            db_type="oracle",
        )

    def capture_head_position(self) -> str | None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_scn FROM v$database")
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return f"scn:{int(row[0])}.c"

    def capture_reader_position(self) -> str | None:
        window_scn = int(getattr(self, "_last_completed_window_scn", 0) or 0)
        if window_scn <= 0:
            return None
        return f"scn:{window_scn}.c"

    @staticmethod
    def infer_cdb_service(pdb_service: str) -> str:
        """Map a common XE/Free PDB service to its CDB root service.

        ``DBMS_LOGMNR.ADD_LOGFILE`` is illegal inside a PDB (ORA-65040).
        Debezium mines from CDB$ROOT and filters SEG_OWNER for the PDB schema.
        """
        key = str(pdb_service or "").strip().upper()
        return {
            "XEPDB1": "XE",
            "FREEPDB1": "FREE",
            "ORCLPDB1": "ORCLCDB",
        }.get(key, "")

    def _mining_target(self) -> tuple[str, str]:
        """Return ``(service, username)`` for ADD_LOGFILE / START_LOGMNR."""
        cdb = str(
            self.cfg.get("cdb_service")
            or self.cfg.get("cdb_database")
            or self.cfg.get("logminer_service")
            or ""
        ).strip()
        pdb = str(self.cfg.get("database") or self.cfg.get("service_name") or "").strip()
        if not cdb:
            cdb = self.infer_cdb_service(pdb)
        user = str(
            self.cfg.get("logminer_username")
            or self.cfg.get("cdb_username")
            or ""
        ).strip()
        if not user:
            local = str(self.cfg.get("username") or "")
            if cdb and local and not local.upper().startswith("C##"):
                user = f"C##{local}"
            else:
                user = local
        service = cdb or pdb or "ORCL"
        return service, user

    def _mining_conn(self):
        """CDB connection for LogMiner — PDB connections raise ORA-65040."""
        from connectors.generic_sql import get_connection

        service, user = self._mining_target()
        return get_connection(
            host=self.cfg.get("host") or "localhost",
            port=self.cfg.get("port") or 1521,
            database=service,
            username=user,
            password=self.cfg.get("logminer_password")
            or self.cfg.get("cdb_password")
            or self.cfg.get("password")
            or "",
            connection_string=self.cfg.get("connection_string") or "",
            ssl=bool(self.cfg.get("ssl")),
            db_type="oracle",
        )

    def _qualified(self, table: str | None = None) -> str:
        tbl = quote_sql_identifier((table or self.table).upper())
        if self.schema:
            return f"{quote_sql_identifier(self.schema)}.{tbl}"
        return tbl

    def _token_table_label(self) -> str:
        if self._shared:
            return ",".join(self.tables)
        return self.table

    def _logminer_reader_identity(self) -> str:
        return (
            f"job={self.cfg.get('job_id') or '<unknown>'} "
            f"slot={self.cursor_key} table={self._token_table_label()}"
        )

    def _table_in_sql(self) -> str:
        """Safe IN-list for LogMiner TABLE_NAME filter (uppercased identifiers)."""
        from connectors.sql_identifiers import require_safe_identifier

        parts = []
        for t in self.tables:
            safe = require_safe_identifier(t, preserve_case=True).upper()
            parts.append(f"'{safe}'")
        return ", ".join(parts)

    def _oracle_ddl_schema_snapshot(self, table: str) -> dict[str, Any]:
        from services.schema_introspect import introspect_schema

        result = introspect_schema(
            "oracle",
            **{
                **self.cfg,
                "schema": self.schema,
                "table": table,
                "ssl": bool(self.cfg.get("ssl", True)),
            },
        )
        if not result.get("ok"):
            raise RuntimeError(
                str(result.get("error") or "Oracle schema introspection failed")
            )
        columns = result.get("columns")
        if not isinstance(columns, list) or not columns:
            raise RuntimeError(
                f"Oracle schema introspection returned no columns for "
                f"{self.schema}.{table}"
            )
        snapshot_columns = [
            {
                "name": str(column.get("name") or ""),
                "type": str(
                    column.get("data_type") or column.get("inferred_type") or ""
                ),
                "nullable": bool(column.get("nullable", True)),
                "ordinal": ordinal,
            }
            for ordinal, column in enumerate(columns, start=1)
            if isinstance(column, dict) and column.get("name")
        ]
        if not snapshot_columns:
            raise RuntimeError(
                f"Oracle schema introspection returned no named columns for "
                f"{self.schema}.{table}"
            )
        return {"columns": snapshot_columns}

    def _oracle_ddl_timing_delta_seconds(
        self, owner: str, table: str, redo_timestamp: Any
    ) -> float | None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT (o.LAST_DDL_TIME - CAST(:redo_ts AS DATE)) * 86400
                    FROM ALL_OBJECTS o
                    WHERE o.OWNER = :owner
                      AND o.OBJECT_NAME = :object_name
                      AND o.OBJECT_TYPE = 'TABLE'
                    """,
                    {
                        "owner": owner,
                        "object_name": table,
                        "redo_ts": redo_timestamp,
                    },
                )
                row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def _record_oracle_ddl(
        self,
        *,
        owner: str,
        table: str,
        ddl: str,
        offset: dict[str, Any],
        ddl_timestamp: Any = None,
        ambiguous_later_ddl: bool = False,
    ) -> None:
        """Record a tracked DDL only when its schema-at-SCN snapshot is safe.

        Oracle catalog introspection is current-state, not guaranteed flashback.
        A later DDL or a later ``ALL_OBJECTS.LAST_DDL_TIME`` therefore makes the
        schema version ambiguous; the stream pauses rather than recording a
        potentially incorrect history entry. An operator can acknowledge the
        matching mapping review to record the then-current schema and release
        only that DDL marker. Catalog timing is compared with a database-side
        DATE difference so host and session timezone settings do not alter it.
        """
        owner_name = str(owner or "").upper()
        table_name = str(table or "").upper()
        if owner_name != self.schema or table_name not in self.tables:
            return
        qualified = f"{owner_name}.{table_name}"
        scn = int(offset.get("scn") or 0)

        def raise_ddl_error(step: str, exc: BaseException) -> None:
            message = (
                f"Oracle LogMiner DDL for {qualified} at SCN {scn} failed during "
                f"{step}; CDC is paused at the DDL barrier: {exc}"
            )
            logger.warning("%s", message, exc_info=True)
            raise OracleLogMinerReadError(
                exc,
                table=qualified,
                cursor_key=self.cursor_key,
                message=message,
            ) from exc

        timing_ambiguity = False
        timing_error: BaseException | None = None
        acknowledged_review = False
        if ddl_timestamp is not None:
            try:
                delta_seconds = self._oracle_ddl_timing_delta_seconds(
                    owner_name, table_name, ddl_timestamp
                )
                if delta_seconds is None:
                    timing_error = LookupError(
                        "DDL timing could not be verified: "
                        "ALL_OBJECTS.LAST_DDL_TIME row was not found"
                    )
                else:
                    timing_ambiguity = delta_seconds > 1
            except Exception as exc:
                timing_error = exc
        if ambiguous_later_ddl or timing_ambiguity or timing_error is not None:
            ambiguity = (
                "another DDL for this table occurs later in the mined window"
                if ambiguous_later_ddl
                else (
                    "ALL_OBJECTS.LAST_DDL_TIME is more than one second later "
                    "than the LogMiner DDL timestamp"
                    if timing_ambiguity
                    else f"DDL timing could not be verified: {timing_error}"
                )
            )
            try:
                from services.cdc_mapping_review import (
                    flag_mapping_review,
                    list_reviews,
                )

                marker = (
                    f"oracle-ddl@scn={scn},rs_id={str(offset.get('rs_id') or '')},"
                    f"ssn={int(offset.get('ssn') or 0)}"
                )
                matching_reviews = [
                    review
                    for review in list_reviews(
                        source_key=self.source_key, status="all", limit=1000
                    )
                    if review.get("table") == qualified
                    and review.get("reason") == "cdc_schema_drift"
                    and str(review.get("ddl") or "").startswith(f"[{marker}]")
                ]
            except Exception as exc:
                raise_ddl_error("mapping-review read", exc)

            open_review = next(
                (
                    review
                    for review in matching_reviews
                    if review.get("status") == "open"
                ),
                None,
            )
            acknowledged = next(
                (
                    review
                    for review in matching_reviews
                    if review.get("status") == "acknowledged"
                ),
                None,
            )
            if open_review:
                raise_ddl_error(
                    "ambiguous schema as of DDL SCN",
                    timing_error or RuntimeError(ambiguity),
                )
            if acknowledged:
                acknowledged_review = True
                logger.info(
                    "Acknowledged Oracle DDL mapping review %s releases %s at SCN %s",
                    acknowledged.get("id"),
                    qualified,
                    scn,
                )
            else:
                try:
                    flag_mapping_review(
                        source_key=self.source_key,
                        table=qualified,
                        reason="cdc_schema_drift",
                        ddl=(
                            f"[{marker}] [ambiguous schema as of DDL SCN: "
                            f"{ambiguity}] {str(ddl or '')}"
                        ),
                        column_names=[],
                    )
                except Exception as exc:
                    raise_ddl_error("mapping-review write", exc)
                cause = timing_error or RuntimeError(ambiguity)
                raise_ddl_error("ambiguous schema as of DDL SCN", cause)

        try:
            from services.cdc_schema_history import (
                last_ddl_at as get_last_ddl_at,
                list_history,
                record_ddl,
            )
        except Exception as exc:
            raise_ddl_error("schema-history setup", exc)
        try:
            snapshot = self._oracle_ddl_schema_snapshot(table_name)
        except Exception as exc:
            raise_ddl_error("schema introspection", exc)
        try:
            fingerprint = json.dumps(
                snapshot.get("columns") or [],
                sort_keys=True,
                separators=(",", ":"),
            )
            history = list_history(self.source_key, qualified)
            if history:
                previous = history[-1].get("schema_snapshot") or {}
                previous_fingerprint = json.dumps(
                    previous.get("columns") or [],
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if previous_fingerprint == fingerprint:
                    self.last_ddl_at = get_last_ddl_at(
                        self.source_key, qualified
                    )
                    logger.info(
                        "Observed Oracle LogMiner DDL for %s; schema fingerprint is unchanged",
                        qualified,
                    )
                    return
        except Exception as exc:
            raise_ddl_error("schema-history read", exc)
        try:
            entry = record_ddl(
                self.source_key,
                qualified,
                ddl=str(ddl or "")[:2000],
                offset=offset,
                schema_snapshot=snapshot,
            )
        except Exception as exc:
            raise_ddl_error("schema-history write", exc)
        self._lob_column_cache.pop(table_name, None)
        self.last_ddl_at = str(entry.get("recorded_at") or "") or None
        if not acknowledged_review:
            try:
                from services.cdc_mapping_review import flag_mapping_review

                flag_mapping_review(
                    source_key=self.source_key,
                    table=qualified,
                    reason="cdc_schema_drift",
                    schema_version=int(entry.get("version") or 0) or None,
                    ddl=str(entry.get("ddl") or ""),
                    column_names=[
                        str(column.get("name") or "")
                        for column in snapshot["columns"]
                    ],
                )
            except Exception as exc:
                raise_ddl_error("mapping-review write", exc)
        logger.info(
            "Recorded Oracle LogMiner schema change for %s v%s",
            qualified,
            entry.get("version"),
        )

    def _empty_window_retry_sec(self) -> float:
        from services.brand_env import getenv_brand

        try:
            return max(0.0, float(getenv_brand("ORACLE_EMPTY_RETRY_SEC", "0.25") or 0.25))
        except (TypeError, ValueError):
            return 0.25

    def _query_logminer_rows(
        self,
        cur: Any,
        *,
        table_predicate: str,
        limit: int,
    ) -> list[Any]:
        rows, _ = _fetch_logminer_csf_window(
            cur,
            sql=logminer_contents_sql(
                table_predicate=table_predicate,
                include_xid=True,
            ),
            binds={
                "owner": self.schema,
                "tbl": self.table,
                **self._resume_binds(),
            },
            limit=limit,
            csf_index=12,
            table=f"{self.schema}.{self.table}",
            cursor_key=self.cursor_key,
        )
        return rows

    def _fetch_logminer_rows_visible(
        self,
        cur: Any,
        *,
        start_scn: int,
        end_scn: int,
        table_predicate: str,
        limit: int,
    ) -> tuple[list[Any], int]:
        """Start LogMiner and fetch rows, retrying once if LGWR has not flushed.

        An empty first read with ``end_scn > start_scn`` used to advance the
        watermark past not-yet-visible committed DML (silent dest leftover).
        """
        with _logminer_session(
            cur,
            start_scn=max(1, int(start_scn)),
            end_scn=int(end_scn),
            reader_identity=self._logminer_reader_identity(),
            step_context="visibility_initial",
        ):
            rows = self._query_logminer_rows(
                cur, table_predicate=table_predicate, limit=limit
            )
        if rows or int(end_scn) <= int(start_scn):
            return rows, int(end_scn)
        delay = self._empty_window_retry_sec()
        if delay > 0:
            time.sleep(delay)
            if self._txn_buffer_enabled:
                cur.execute("SELECT current_scn FROM v$database")
                head = cur.fetchone()
                end_scn = max(
                    int(end_scn),
                    int(head[0] or end_scn) if head else int(end_scn),
                )
            else:
                end_scn, held = self._buffer_off_window_end()
                if held and end_scn <= self.scn:
                    return [], int(end_scn)
            with _logminer_session(
                cur,
                start_scn=max(1, int(start_scn)),
                end_scn=int(end_scn),
                reader_identity=self._logminer_reader_identity(),
                step_context="visibility_retry",
            ):
                rows = self._query_logminer_rows(
                    cur, table_predicate=table_predicate, limit=limit
                )
        return rows, int(end_scn)

    def _probe_oldest_open_txn(self) -> tuple[int, int, int, int, float] | None:
        sql = """
            SELECT START_SCN, XIDUSN, XIDSLOT, XIDSQN, (SYSDATE - START_DATE) * 86400
              FROM V$TRANSACTION ORDER BY START_SCN FETCH FIRST 1 ROWS ONLY
        """
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
            if not row:
                return None
            if len(row) < 5:
                raise ValueError("V$TRANSACTION probe returned fewer than five columns")
            return (
                int(row[0]),
                int(row[1]),
                int(row[2]),
                int(row[3]),
                float(row[4] or 0),
            )
        except OracleOpenTxnDetectionUnavailable:
            raise
        except Exception as exc:
            raise OracleOpenTxnDetectionUnavailable(
                exc,
                table=f"{self.schema}.{self.table}",
                cursor_key=self.cursor_key,
            ) from exc

    def _ensure_open_txn_detection(self) -> None:
        if self._txn_buffer_enabled or self._open_txn_detection_checked:
            return
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM GV$INSTANCE")
                    row = cur.fetchone()
            if not row or len(row) < 1:
                raise ValueError("GV$INSTANCE query returned no instance count")
            instance_count = int(row[0] or 0)
        except Exception as exc:
            raise OracleInstanceDetectionUnavailable(
                exc,
                table=f"{self.schema}.{self.table}",
                cursor_key=self.cursor_key,
            ) from exc
        if instance_count > 1:
            raise OracleBufferOffRacUnsupported(
                table=f"{self.schema}.{self.table}",
                cursor_key=self.cursor_key,
            )
        self._probe_oldest_open_txn()
        self._open_txn_detection_checked = True

    def _buffer_off_window_end(self) -> tuple[int, bool]:
        """Sample the source head and oldest open transaction in one query."""
        # V$DATABASE and V$TRANSACTION are not guaranteed to be read atomically;
        # transaction buffering remains the safe default for the narrow residual.
        sql = """
            SELECT d.CURRENT_SCN, t.START_SCN, t.XIDUSN, t.XIDSLOT, t.XIDSQN,
                   (SYSDATE - t.START_DATE) * 86400
              FROM V$DATABASE d
              LEFT JOIN (
                  SELECT START_SCN, XIDUSN, XIDSLOT, XIDSQN, START_DATE
                    FROM V$TRANSACTION
                   ORDER BY START_SCN
                   FETCH FIRST 1 ROWS ONLY
              ) t ON 1 = 1
        """
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
            if not row or len(row) < 6:
                raise ValueError("Oracle buffer-off window query returned an invalid row")
            head_scn = int(row[0] or self.scn)
            if row[1] is None:
                return head_scn, False
            start_scn = int(row[1])
            xid_usn = int(row[2])
            xid_slot = int(row[3])
            xid_seq = int(row[4])
            age_seconds = float(row[5] or 0)
        except OracleOpenTxnDetectionUnavailable:
            raise
        except Exception as exc:
            raise OracleOpenTxnDetectionUnavailable(
                exc,
                table=f"{self.schema}.{self.table}",
                cursor_key=self.cursor_key,
            ) from exc

        held_end_scn = min(head_scn, start_scn - 1)
        held = held_end_scn < head_scn
        if held:
            self._warn_oldest_open_txn_hold(
                xid_usn=xid_usn,
                xid_slot=xid_slot,
                xid_seq=xid_seq,
                start_scn=start_scn,
                age_seconds=age_seconds,
                held_end_scn=held_end_scn,
            )
        return held_end_scn, held

    def _warn_oldest_open_txn_hold(
        self,
        *,
        xid_usn: int,
        xid_slot: int,
        xid_seq: int,
        start_scn: int,
        age_seconds: float,
        held_end_scn: int,
    ) -> None:
        xid = (xid_usn, xid_slot, xid_seq)
        now = time.monotonic()
        # Warn on the first hold per XID, then at most once every 60 seconds per XID.
        # Expire old entries so each XID can be throttled without an unbounded cache.
        for prior_xid, last_warning in tuple(self._open_txn_hold_warnings.items()):
            if now - last_warning >= 60:
                self._open_txn_hold_warnings.pop(prior_xid, None)
        if xid not in self._open_txn_hold_warnings:
            logger.warning(
                "Oracle LogMiner holding window at oldest open transaction "
                "cursor_key=%s xid=%s.%s.%s start_scn=%s age_seconds=%s "
                "held_end_scn=%s",
                self.cursor_key,
                xid_usn,
                xid_slot,
                xid_seq,
                start_scn,
                age_seconds,
                held_end_scn,
            )
            self._open_txn_hold_warnings[xid] = now

    def is_available(self) -> bool:
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    if not cur.fetchone():
                        return False
                    # Privilege / dictionary probe
                    cur.execute("SELECT COUNT(*) FROM v$logmnr_contents WHERE ROWNUM < 1")
                    cur.fetchone()
            # ADD_LOGFILE must succeed on the CDB mining session (ORA-65040 in PDB).
            with self._mining_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_scn FROM v$database")
                    if not cur.fetchone():
                        return False
            return True
        except Exception as exc:
            logger.debug("Oracle LogMiner unavailable: %s", exc)
            return False

    def _rows_to_records(self, cols: list[Any], rows: list[Any]) -> list[dict[str, str]]:
        clean_cols = [c for c in cols if str(c).upper() != "DF_RN"]
        rn_idx = next(
            (i for i, c in enumerate(cols) if str(c).upper() == "DF_RN"),
            None,
        )
        records: list[dict[str, str]] = []
        for row in rows:
            if rn_idx is not None:
                values = [row[i] for i in range(len(cols)) if i != rn_idx]
            else:
                values = list(row)
            records.append(
                {
                    str(clean_cols[i]).upper(): wire_cell(values[i])
                    for i in range(len(clean_cols))
                }
            )
        return records

    def snapshot(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self._shared:
            yield from self._snapshot_shared()
            return
        offset = self.snapshot_offset if self.phase == "snapshot" else 0
        last_pk = self.snapshot_last_pk if self.phase == "snapshot" else ""
        # Mid-dump resume keeps the original SCN (not a new tip).
        handoff = self.scn if (self.phase == "snapshot" and self.scn) else 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                if not handoff:
                    cur.execute("SELECT current_scn FROM v$database")
                    row = cur.fetchone()
                    handoff = int(row[0] or 0) if row else 0
                yield from self._iter_snapshot_table(
                    cur,
                    self.table,
                    handoff=handoff,
                    offset=offset,
                    last_pk=last_pk,
                    ack_barrier=False,
                )
        self.scn = max(0, handoff - 1)
        self.rs_id = ""
        self.ssn = 0
        self.scn_complete = True
        self._poll_scn = self.scn
        self._poll_rs_id = self.rs_id
        self._poll_ssn = self.ssn
        self.phase = "streaming"
        self.snapshot_offset = 0
        self.snapshot_last_pk = ""
        self.resume_token = encode_logminer_token(
            self.scn,
            table=self._token_table_label(),
            phase="streaming",
            rs_id=self.rs_id,
            ssn=self.ssn,
            scn_complete=self.scn_complete,
        )
        yield ChangeBatch(
            resume_token=self.resume_token,
            table=self.table,
        )

    def _snapshot_shared(self) -> Iterator[ChangeBatch]:
        """Multi-table initial dump under one SCN handoff (at-least-once)."""
        resume_table = (self.snapshot_table or "").upper() if self.phase == "snapshot" else ""
        resume_last_pk = self.snapshot_last_pk if self.phase == "snapshot" else ""
        resume_offset = self.snapshot_offset if self.phase == "snapshot" else 0
        if resume_table and "," in resume_table:
            resume_table = ""
            resume_last_pk = ""
            resume_offset = 0
        tables = list(self.tables)
        if resume_table in tables:
            tables = tables[tables.index(resume_table) :]
        handoff = self.scn if (self.phase == "snapshot" and self.scn) else 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                if not handoff:
                    cur.execute("SELECT current_scn FROM v$database")
                    row = cur.fetchone()
                    handoff = int(row[0] or 0) if row else 0
                for table_name in tables:
                    table_last_pk = resume_last_pk if table_name == resume_table else ""
                    table_offset = resume_offset if table_name == resume_table else 0
                    yield from self._iter_snapshot_table(
                        cur,
                        table_name,
                        handoff=handoff,
                        offset=table_offset,
                        last_pk=table_last_pk,
                        ack_barrier=False,
                    )
        self.scn = max(0, handoff - 1)
        self.rs_id = ""
        self.ssn = 0
        self.scn_complete = True
        self._poll_scn = self.scn
        self._poll_rs_id = self.rs_id
        self._poll_ssn = self.ssn
        self.phase = "streaming"
        self.snapshot_offset = 0
        self.snapshot_last_pk = ""
        self.resume_token = encode_logminer_token(
            self.scn,
            table=self._token_table_label(),
            phase="streaming",
            rs_id=self.rs_id,
            ssn=self.ssn,
            scn_complete=self.scn_complete,
        )
        yield ChangeBatch(
            resume_token=self.resume_token,
            ack_barrier=True,
        )

    def _iter_snapshot_table(
        self,
        cur: Any,
        table_name: str,
        *,
        handoff: int,
        offset: int,
        last_pk: str,
        ack_barrier: bool,
    ) -> Iterator[ChangeBatch]:
        """Page one table: held scan, PK-seek, or legacy ROW_NUMBER."""
        from connectors.sql_snapshot_scan import fetch_scan_page
        from services.cdc_snapshot_resume import (
            classify_snapshot_resume,
            last_pk_from_records,
            quoted_pk_columns,
            snapshot_keyset_sql,
        )
        from services.cdc_snapshot_window import _pk_columns

        pk_cols = [c.upper() for c in _pk_columns(self.primary_keys.get(table_name, self.primary_key))]
        quoted = quoted_pk_columns(pk_cols, '"')
        order_sql = ", ".join(quoted)
        qualified = self._qualified(table_name)
        mode = classify_snapshot_resume(last_pk=last_pk, offset=offset)
        if mode == "scan":
            # Held scan: one ordered SELECT paged with fetchmany. ROW_NUMBER()
            # would rank the whole table before the first page returns, and the
            # rank is unused — resume seeks on the last PK.
            cur.execute(
                f"SELECT t.* FROM {qualified} t ORDER BY {order_sql}"  # nosec B608
            )
        while True:
            if mode == "scan":
                rows = fetch_scan_page(cur, self.batch_size)
            elif mode == "keyset":
                sql, params = snapshot_keyset_sql(
                    table_ref=qualified,
                    quoted_pk_columns=quoted,
                    last_pk=last_pk,
                    limit=self.batch_size,
                    dialect="oracle",
                )
                cur.execute(sql, params)
                rows = cur.fetchall() or []
            else:
                pk = quote_sql_identifier(self.primary_keys.get(table_name, self.primary_key))
                cur.execute(
                    f"""
                    SELECT * FROM (
                      SELECT t.*, ROW_NUMBER() OVER (ORDER BY t.{pk}) AS df_rn
                      FROM {qualified} t
                    ) WHERE df_rn > :off AND df_rn <= :lim
                    """,  # nosec B608
                    {"off": offset, "lim": offset + self.batch_size},
                )
                rows = cur.fetchall() or []
            cols = [d[0] for d in (cur.description or [])]
            if not rows:
                break
            records = self._rows_to_records(cols, rows)
            offset += len(rows)
            last_pk = last_pk_from_records(records, pk_cols) or last_pk
            self.snapshot_offset = offset
            self.snapshot_last_pk = last_pk
            self._last_event_at = datetime.now(timezone.utc)
            yield ChangeBatch(
                inserts=records,
                resume_token=encode_logminer_token(
                    handoff,
                    table=table_name,
                    phase="snapshot",
                    offset=offset,
                    last_pk=last_pk,
                ),
                table=table_name,
                ack_barrier=ack_barrier,
            )
            if len(rows) < self.batch_size:
                break

    def _fetch_incremental_chunk(self, sig: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        """PK-ordered chunk for signal-driven incremental snapshots."""
        from services.cdc_snapshot_filter import signal_filter_sql
        from services.cdc_snapshot_resume import (
            last_pk_from_records,
            quoted_pk_columns,
            snapshot_keyset_sql,
        )
        from services.cdc_snapshot_window import _pk_columns

        pk_name = (sig.primary_key or self.primary_key or "").upper()
        if not pk_name:
            raise ValueError(
                "Oracle LogMiner incremental snapshot requires primary_key — "
                "refuse inventing default 'ID'"
            )
        pk_cols = [c.upper() for c in _pk_columns(sig.primary_key or self.primary_key)]
        quoted = quoted_pk_columns(pk_cols, '"')
        limit = int(sig.chunk_size or self.batch_size)
        last_pk = sig.last_pk or ""
        qualified = self._qualified()
        with self._conn() as conn:
            with conn.cursor() as cur:
                # Optional operator filter (Debezium additional-conditions);
                # binds only, compiled from the structured spec on the signal.
                filter_sql, filter_params = signal_filter_sql(sig, dialect="oracle", upper_columns=True)
                sql, params = snapshot_keyset_sql(
                    table_ref=qualified,
                    quoted_pk_columns=quoted,
                    last_pk=last_pk,
                    limit=limit,
                    dialect="oracle",
                    filter_sql=filter_sql,
                    filter_params=filter_params,
                )
                cur.execute(sql, params)
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
        records = self._rows_to_records(cols, rows)
        new_last = last_pk_from_records(records, pk_cols) or last_pk
        done = len(records) < limit
        return records, str(new_last) if new_last is not None else last_pk, done

    def _resume_binds(self) -> dict[str, Any]:
        rs_id = _canonical_rs_id(str(self.rs_id or ""))
        rs_len = len(rs_id) if rs_id else None
        return {
            "start_scn": int(self.scn or 0),
            # Oracle treats an empty VARCHAR2 bind as NULL; the query's
            # ``:rs_id IS NULL`` branch includes the SCN only for incomplete
            # cursors; a token marked complete remains exclusive.
            "rs_id": rs_id or None,
            "rs_len": rs_len,
            "ssn": int(self.ssn or 0),
            "scn_complete": int(self.scn_complete),
        }

    def _advance_offset(
        self,
        *,
        last_scn: int,
        last_rs_id: str,
        last_ssn: int,
        end_scn: int,
        fetched: int,
        limit: int,
    ) -> None:
        """Advance the resume watermark without skipping unread changes.

        Keep every non-empty window at its last consumed ``(scn, rs_id, ssn)``.
        Advancing to the observed current SCN can skip redo that LogMiner exposes
        later at that same SCN.
        """
        if fetched <= 0:
            # Empty contents are not proof the window is exhausted. LGWR can
            # still be flushing committed DML — jumping to end_scn skipped
            # those SCNs forever (dest leftover after resume delete).
            # Keep the cursor so the next poll re-mines the same range.
            return
        self.scn = int(last_scn or self.scn or 0)
        self.rs_id = str(last_rs_id or "")
        self.ssn = int(last_ssn or 0)
        self.scn_complete = False

    def _token(self, *, table: str | None = None) -> str:
        return encode_logminer_token(
            self.scn,
            table=table or self.table,
            phase="streaming",
            rs_id=self.rs_id,
            ssn=self.ssn,
            scn_complete=self.scn_complete,
            low_scn=self.low_scn if self._txn_buffer_enabled else 0,
            txn_buffer=self._txn_buffer_enabled,
        )

    def _peek_stream_events_during_chunk(self, sig: Any) -> list[dict[str, Any]]:
        """Non-acking LogMiner peek for DDD-3 stream-wins (does not advance SCN)."""
        events: list[dict[str, Any]] = []
        if self.scn <= 0:
            return events
        peek_limit = min(int(sig.chunk_size or self.batch_size), 200)
        try:
            with self._mining_conn() as conn:
                with conn.cursor() as cur:
                    if self._txn_buffer_enabled:
                        cur.execute("SELECT current_scn FROM v$database")
                        head = cur.fetchone()
                        end_scn = int(head[0] or self.scn) if head else self.scn
                    else:
                        end_scn, held = self._buffer_off_window_end()
                        if held and end_scn <= self.scn:
                            return events
                    # An intra-SCN (rs_id, ssn) cursor still has unread changes
                    # at self.scn even when current_scn has not advanced. Bailing
                    # out here lets a mid-chunk UPDATE lose to the snapshot row
                    # (DDD-3 stream-wins relies on this peek).
                    if end_scn < self.scn or (end_scn == self.scn and not self.rs_id):
                        return events
                    with _logminer_session(
                        cur,
                        start_scn=max(1, self.scn),
                        end_scn=end_scn,
                        reader_identity=self._logminer_reader_identity(),
                        step_context="snapshot_peek",
                    ):
                        peek_rows = self._query_logminer_rows(
                            cur,
                            table_predicate="TABLE_NAME = :tbl OR SEG_NAME = :tbl",
                            limit=peek_limit,
                        )
                    ddl_positions = [
                        (
                            str(candidate[5] or candidate[10] or "").upper(),
                            int(candidate[0] or 0),
                            str(candidate[1] or ""),
                            int(candidate[2] or 0),
                        )
                        for candidate in peek_rows
                        if str(candidate[3] or "").upper() == "DDL"
                    ]
                    lob_locator_scns = {
                        tuple(int(value or 0) for value in candidate[7:10])
                        for candidate in peek_rows
                        if str(candidate[3] or "").upper() == "SEL_LOB_LOCATOR"
                    }
                    for row in peek_rows:
                        scn = int(row[0] or 0)
                        rs_id = _row_rs_id(row[1])
                        ssn = int(row[2] or 0)
                        operation = str(row[3] or "").upper()
                        sql_redo = row[4]
                        if operation == "DDL":
                            self._record_oracle_ddl(
                                owner=str(row[6] or ""),
                                table=str(row[5] or row[10] or ""),
                                ddl=str(sql_redo or ""),
                                offset={"scn": scn, "rs_id": rs_id, "ssn": ssn},
                                ddl_timestamp=row[11] if len(row) > 11 else None,
                                ambiguous_later_ddl=any(
                                    name == str(row[5] or row[7] or "").upper()
                                    and mining_position(
                                        later_scn, later_rs, later_ssn
                                    )
                                    > mining_position(scn, rs_id, ssn)
                                    for name, later_scn, later_rs, later_ssn
                                    in ddl_positions
                                ),
                            )
                            continue
                        if operation == "SEL_LOB_LOCATOR":
                            self._remember_lob_locator_xid(
                                self.table,
                                tuple(int(value or 0) for value in row[7:10]),
                            )
                            primary_key = self._required_lob_primary_key(self.table)
                            parsed = self._parse_lob_locator_pk(
                                sql_redo or "",
                                primary_key,
                                table=self.table,
                            )
                            events.append(
                                {
                                    "op": "u",
                                    "table": self.table,
                                    "xid": self._logminer_row_xid(row),
                                    "position": (scn, rs_id, ssn),
                                    "lob_locator": True,
                                    "row": {
                                        **parsed,
                                        _LOB_LOCATOR_MARKER: True,
                                    },
                                }
                            )
                            continue
                        if (
                            operation == "UPDATE"
                            and not str(sql_redo or "").strip()
                            and tuple(int(value or 0) for value in row[7:10])
                            in lob_locator_scns
                        ):
                            continue
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op:
                            continue
                        kind, parsed = classify_sql_redo(
                            sql_redo or "", op=op, table=self.table
                        )
                        if kind == "unparsed":
                            # Refuse stream-wins on unparsed redo — do not invent a row.
                            continue
                        key = parsed.get(self.primary_key, "")
                        if op == "delete" and key:
                            events.append(
                                {
                                    "op": "d",
                                    "table": self.table,
                                    "xid": self._logminer_row_xid(row),
                                    "position": (scn, rs_id, ssn),
                                    "pk": key,
                                    "row": {self.primary_key: key},
                                }
                            )
                        elif op == "insert":
                            events.append(
                                {
                                    "op": "c",
                                    "table": self.table,
                                    "xid": self._logminer_row_xid(row),
                                    "position": (scn, rs_id, ssn),
                                    "row": parsed,
                                }
                            )
                        else:
                            events.append(
                                {
                                    "op": "u",
                                    "table": self.table,
                                    "xid": self._logminer_row_xid(row),
                                    "position": (scn, rs_id, ssn),
                                    "row": parsed,
                                }
                            )
        except OracleLogMinerReadError:
            raise
        except Exception as exc:
            if _is_oracle_connection_lost(exc):
                read_error = OracleLogMinerReadError(
                    exc,
                    table=f"{self.schema}.{self.table}",
                    cursor_key=self.cursor_key,
                )
                raise read_error from exc
            events = self._coalesce_lob_locator_events(events)
            self._enrich_lob_lookup_events(events)
            return events
        events = self._coalesce_lob_locator_events(events)
        self._enrich_lob_lookup_events(events)
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        if self.phase != "streaming" or self.scn <= 0:
            yield from self._poll_once()
            return
        attempt = 0
        started_at = time.monotonic()
        while True:
            yielded = False
            try:
                for batch in self._poll_once():
                    yielded = True
                    yield batch
                return
            except Exception as exc:
                if isinstance(exc, OracleLogMinerReadError):
                    read_error = exc
                elif _is_oracle_connection_lost(exc):
                    read_error = OracleLogMinerReadError(
                        exc,
                        table=f"{self.schema}.{self.table}",
                        cursor_key=self.cursor_key,
                    )
                else:
                    raise
                if not read_error.transient:
                    raise
                if yielded:
                    if read_error is exc:
                        raise
                    raise read_error from exc
                attempt += 1
                if not should_retry_connection_lost(
                    attempt=attempt,
                    started_at=started_at,
                ):
                    if read_error is exc:
                        raise
                    raise read_error from exc
                logger.warning(
                    "Oracle LogMiner poll connection lost; reconnecting (attempt=%d)",
                    attempt,
                )
                time.sleep(reconnect_backoff_seconds(attempt))

    def _poll_once(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self.phase != "streaming" or self.scn <= 0:
            yield from self.snapshot()
            return

        if self._txn_buffer_enabled:
            yield from self._poll_txn_buffered()
            return

        self._ensure_open_txn_detection()

        if self._shared:
            yield from self._poll_shared_multi()
            return

        from services.cdc_incremental_runner import interleave_incremental_snapshot

        yield from interleave_incremental_snapshot(
            self.source_key,
            table=self.table,
            fetch_chunk=self._fetch_incremental_chunk,
            stream_events_during_chunk=self._peek_stream_events_during_chunk,
            max_chunks_per_poll=1,
            dest_resume=self.resume_token,
        )

        events: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        last_scn = self.scn
        last_rs_id = self.rs_id
        last_ssn = self.ssn
        fetched = 0
        try:
            with self._mining_conn() as conn:
                with conn.cursor() as cur:
                    assert_resume_scn_in_redo(
                        self.scn,
                        fetch_oldest_available_scn(cur),
                        cursor_key=self.cursor_key,
                    )
                    assert_redo_continuity(
                        self.scn,
                        fetch_redo_inventory(cur),
                        cursor_key=self.cursor_key,
                    )
                    if self._txn_buffer_enabled:
                        cur.execute("SELECT current_scn FROM v$database")
                        head = cur.fetchone()
                        end_scn = int(head[0] or self.scn) if head else self.scn
                    else:
                        end_scn, held = self._buffer_off_window_end()
                        if held and end_scn <= self.scn:
                            yield ChangeBatch(
                                resume_token=self._token(),
                                table=self.table,
                            )
                            return
                    if end_scn <= self.scn and not self.rs_id:
                        yield ChangeBatch(
                            resume_token=self._token(),
                            table=self.table,
                        )
                        return
                    rows, end_scn = self._fetch_logminer_rows_visible(
                        cur,
                        start_scn=self.scn,
                        end_scn=end_scn,
                        table_predicate="TABLE_NAME = :tbl OR SEG_NAME = :tbl",
                        limit=self.batch_size,
                    )
                    fetched = len(rows)
                    ddl_positions = [
                        (
                            str(candidate[5] or candidate[10] or "").upper(),
                            int(candidate[0] or 0),
                            str(candidate[1] or ""),
                            int(candidate[2] or 0),
                        )
                        for candidate in rows
                        if str(candidate[3] or "").upper() == "DDL"
                    ]
                    for row in rows:
                        scn = int(row[0] or 0)
                        rs_id = _row_rs_id(row[1])
                        ssn = int(row[2] or 0)
                        operation = str(row[3] or "").upper()
                        sql_redo = row[4]
                        table_name = str(row[5] or row[10] or "").upper()
                        owner = str(row[6] or "").upper()
                        last_scn, last_rs_id, last_ssn = scn, rs_id, ssn
                        if operation == "DDL":
                            self._last_event_at = datetime.now(timezone.utc)
                            self._record_oracle_ddl(
                                owner=owner,
                                table=table_name,
                                ddl=str(sql_redo or ""),
                                offset={
                                    "scn": scn,
                                    "rs_id": rs_id,
                                    "ssn": ssn,
                                },
                                ddl_timestamp=row[11] if len(row) > 11 else None,
                                ambiguous_later_ddl=any(
                                    name == table_name
                                    and mining_position(
                                        later_scn, later_rs, later_ssn
                                    )
                                    > mining_position(scn, rs_id, ssn)
                                    for name, later_scn, later_rs, later_ssn in ddl_positions
                                ),
                            )
                            continue
                        if operation == "SEL_LOB_LOCATOR":
                            self._remember_lob_locator_xid(
                                self.table,
                                tuple(int(value or 0) for value in row[7:10]),
                            )
                            primary_key = self._required_lob_primary_key(self.table)
                            parsed = self._parse_lob_locator_pk(
                                sql_redo or "",
                                primary_key,
                                table=self.table,
                            )
                            events.append(
                                {
                                    "op": "update",
                                    "table": self.table,
                                    "xid": tuple(
                                        int(value or 0) for value in row[7:10]
                                    ),
                                    "position": (scn, rs_id, ssn),
                                    "lob_locator": True,
                                    "row": {
                                        **parsed,
                                        _LOB_LOCATOR_MARKER: True,
                                    },
                                }
                            )
                            continue
                        if (
                            operation == "UPDATE"
                            and not str(sql_redo or "").strip()
                            and self._has_lob_locator_xid(
                                self.table,
                                tuple(int(value or 0) for value in row[7:10]),
                            )
                        ):
                            continue
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op:
                            continue
                        self._last_event_at = datetime.now(timezone.utc)
                        kind, parsed = classify_sql_redo(
                            sql_redo or "", op=op, table=self.table
                        )
                        if kind == "unparsed":
                            # Consume the SCN so we do not replay forever, but
                            # never upsert an unparsed row.
                            rejected.append(parsed)
                            continue
                        key = parsed.get(self.primary_key, "")
                        if op == "delete":
                            if key:
                                events.append(
                                    {
                                        "op": "delete",
                                        "table": self.table,
                                        "xid": self._logminer_row_xid(row),
                                        "position": (scn, rs_id, ssn),
                                        "row": {self.primary_key: key},
                                    }
                                )
                        elif op == "insert":
                            events.append(
                                {
                                    "op": "insert",
                                    "table": self.table,
                                    "xid": self._logminer_row_xid(row),
                                    "position": (scn, rs_id, ssn),
                                    "row": parsed,
                                }
                            )
                        else:
                            events.append(
                                {
                                    "op": "update",
                                    "table": self.table,
                                    "xid": self._logminer_row_xid(row),
                                    "position": (scn, rs_id, ssn),
                                    "row": parsed,
                                }
                            )
        except CdcScnGapError:
            raise
        except OracleLogMinerReadError:
            raise
        except Exception as exc:
            if is_oracle_redo_gap_error(exc):
                raise CdcScnGapError(
                    f"Oracle LogMiner redo unavailable for resume SCN {self.scn}: {exc}",
                    resume_scn=self.scn,
                    cursor_key=self.cursor_key,
                ) from exc
            # Fail closed: a START_LOGMNR / ADD_LOGFILE failure must surface.
            # Returning an empty poll here used to keep the job "healthy" while
            # CDC delivered nothing forever (CONTINUOUS_MINE desupport path).
            read_error = OracleLogMinerReadError(
                exc,
                table=f"{self.schema}.{self.table}",
                cursor_key=self.cursor_key,
            )
            raise read_error from exc

        events = self._coalesce_lob_locator_events(events)
        lookup_columns = self._enrich_lob_lookup_events(events)
        inserts = [
            event["row"] for event in events if event.get("op") == "insert"
        ]
        updates = [
            event["row"] for event in events if event.get("op") == "update"
        ]
        deletes = [
            str(event["row"].get(self.primary_key) or "")
            for event in events
            if event.get("op") == "delete" and event["row"].get(self.primary_key)
        ]
        self._advance_offset(
            last_scn=last_scn,
            last_rs_id=last_rs_id,
            last_ssn=last_ssn,
            end_scn=end_scn,
            fetched=fetched,
            limit=self.batch_size,
        )
        if fetched < self.batch_size:
            self._last_completed_window_scn = max(
                self._last_completed_window_scn, int(end_scn)
            )

        token = self._token()
        if inserts or updates or deletes or rejected:
            yield ChangeBatch(
                inserts=inserts,
                updates=updates,
                deletes=deletes,
                resume_token=token,
                table=self.table,
                rejected=rejected,
                lookup_columns=lookup_columns,
            )
        else:
            yield ChangeBatch(resume_token=token, table=self.table)

    def _buffered_token(
        self,
        *,
        position_scn: int,
        position_rs_id: str,
        position_ssn: int,
        low_scn: int,
        commit_cursor: tuple[int, str, int] | None = None,
        commit_xid: str | None = None,
        table: str | None = None,
    ) -> str:
        commit = commit_cursor or self._emitted_commit
        return encode_logminer_token(
            position_scn,
            table=table or self._token_table_label(),
            phase="streaming",
            rs_id=position_rs_id,
            ssn=position_ssn,
            low_scn=low_scn,
            poll_scn=self._poll_scn,
            poll_rs_id=self._poll_rs_id,
            poll_ssn=self._poll_ssn,
            commit_scn=commit[0],
            commit_rs_id=commit[1],
            commit_ssn=commit[2],
            commit_xid=(
                self._emitted_commit_xid if commit_xid is None else commit_xid
            ),
            txn_buffer=True,
            scn_complete=False,
        )

    def _poll_txn_buffered(self) -> Iterator[ChangeBatch]:
        """Mine lifecycle and DML rows, emitting only whole committed XIDs."""
        from connectors.oracle_logminer_txn import (
            OracleCommittedTxn,
            OracleTxnBuffer,
            chunk_txn_rows,
            oracle_txn_max_bytes,
            oracle_txn_max_open,
        )
        from services.cdc_transaction_buffer import CdcTxnBufferOverflow
        from services.cdc_multi_table import MultiTableTransactionBuffer

        if self._txn_buffer is None:
            self._txn_buffer = OracleTxnBuffer(
                max_txns=oracle_txn_max_open(),
                max_bytes=oracle_txn_max_bytes(),
            )
        buffer: OracleTxnBuffer = self._txn_buffer
        table_set = {t.upper() for t in self.tables}
        table_by_lower = {t.lower(): t for t in self.tables}
        limit = max(self.batch_size * 10, 1000)
        emitted: list[OracleCommittedTxn] = []
        ddl_events: list[tuple[int, str, int, str, str, str, Any]] = []
        fetched = 0
        head_scn = self.scn
        try:
            with self._mining_conn() as conn:
                with conn.cursor() as cur:
                    mine_from_scn = (
                        min(self.scn, self.low_scn) if self.low_scn else self.scn
                    )
                    assert_resume_scn_in_redo(
                        mine_from_scn,
                        fetch_oldest_available_scn(cur),
                        cursor_key=self.cursor_key,
                    )
                    assert_redo_continuity(
                        mine_from_scn,
                        fetch_redo_inventory(cur),
                        cursor_key=self.cursor_key,
                    )
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    head_scn = int(head[0] or self.scn) if head else self.scn
                    end_scn = max(self.scn, head_scn)
                    in_list = self._table_in_sql()
                    table_predicate = (
                        f"TABLE_NAME IN ({in_list}) OR SEG_NAME IN ({in_list})"
                    )
                    with _logminer_session(
                        cur,
                        start_scn=max(1, mine_from_scn),
                        end_scn=end_scn,
                        committed_only=False,
                        reader_identity=self._logminer_reader_identity(),
                        step_context="transaction_buffer",
                    ):
                        raw_rows, was_truncated = _fetch_logminer_csf_window(
                            cur,
                            sql=logminer_txn_contents_sql(
                                table_predicate=table_predicate
                            ),
                            binds={
                                "owner": self.schema,
                                **self._resume_binds(),
                            },
                            limit=limit,
                            csf_index=14,
                            table=self._token_table_label(),
                            cursor_key=self.cursor_key,
                        )
                    fetched = len(raw_rows)
        except (CdcScnGapError, CdcTxnBufferOverflow, OracleLogMinerReadError):
            raise
        except Exception as exc:
            if is_oracle_redo_gap_error(exc):
                raise CdcScnGapError(
                    f"Oracle LogMiner redo unavailable for resume SCN {self.scn}: {exc}",
                    resume_scn=self.scn,
                    cursor_key=self.cursor_key,
                ) from exc
            read_error = OracleLogMinerReadError(
                exc,
                table=self._token_table_label(),
                cursor_key=self.cursor_key,
            )
            raise read_error from exc

        for row in raw_rows:
            scn = int(row[0] or 0)
            rs_id = _row_rs_id(row[1])
            ssn = int(row[2] or 0)
            operation = str(row[3] or "").upper()
            sql_redo = row[4]
            table_raw = str(row[5] or "").upper()
            row_id = str(row[11] or "") if len(row) > 11 else ""
            if operation == "DDL":
                owner = str(row[6] or "").upper()
                ddl_table = str(row[5] or row[12] or "").upper()
                mapped_ddl_table = table_by_lower.get(
                    ddl_table.lower(), ddl_table
                )
                if owner == self.schema and mapped_ddl_table in table_set:
                    ddl_events.append(
                        (
                            scn,
                            rs_id,
                            ssn,
                            owner,
                            mapped_ddl_table,
                            str(sql_redo or ""),
                            row[13] if len(row) > 13 else None,
                        )
                    )
                self._poll_scn, self._poll_rs_id, self._poll_ssn = (
                    scn,
                    rs_id,
                    ssn,
                )
                continue
            xid_parts = tuple(int(value or 0) for value in row[7:10])
            if xid_parts == (0, 0, 0):
                raise RuntimeError(
                    "Oracle transaction-buffer LogMiner row lacks an XID "
                    f"(operation={operation}, scn={scn}); refusing to "
                    "emit an uncorrelated transaction."
                )
            xid = ".".join(str(value) for value in xid_parts)
            mapped_table = table_by_lower.get(table_raw.lower(), table_raw)
            if operation == "SEL_LOB_LOCATOR":
                self._remember_lob_locator_xid(mapped_table, xid_parts)
            if (
                operation == "UPDATE"
                and not str(sql_redo or "").strip()
                and self._has_lob_locator_xid(mapped_table, xid_parts)
            ):
                self._poll_scn, self._poll_rs_id, self._poll_ssn = (
                    scn,
                    rs_id,
                    ssn,
                )
                continue
            feed_args: dict[str, Any] = {
                "xid": xid,
                "scn": scn,
                "rs_id": rs_id,
                "ssn": ssn,
                "operation": operation,
            }
            if table_raw in table_set and (
                operation in _OP_MAP or operation == "SEL_LOB_LOCATOR"
            ):
                pk = self.primary_keys.get(mapped_table, self.primary_key)
                if operation == "SEL_LOB_LOCATOR":
                    pk = self._required_lob_primary_key(mapped_table)
                    parsed = self._parse_lob_locator_pk(
                        sql_redo or "",
                        pk,
                        table=mapped_table,
                    )
                    parsed[_LOB_LOCATOR_MARKER] = True
                    op = "update"
                    kind = "ok"
                    event_row_id = ""
                else:
                    op = _OP_MAP[operation]
                    kind, parsed = classify_sql_redo(
                        sql_redo or "", op=op, table=mapped_table
                    )
                    event_row_id = row_id
                feed_args.update(
                    operation=op,
                    table=mapped_table,
                    row=parsed if kind == "ok" else {"_df_rejected": parsed},
                    rollback=row[10] in (1, "1", True),
                    row_id=event_row_id,
                )
                if kind == "ok" and pk:
                    feed_args["primary_key"] = pk
                if kind == "unparsed":
                    feed_args["operation"] = "UNPARSED"
            emitted.extend(buffer.feed(**feed_args))
            self._poll_scn, self._poll_rs_id, self._poll_ssn = (
                scn,
                rs_id,
                ssn,
            )

        buffer.check_abandoned(head_scn=head_scn)
        self.scn, self.rs_id, self.ssn = (
            self._poll_scn,
            self._poll_rs_id,
            self._poll_ssn,
        )
        if buffer.low_scn:
            self.low_scn = buffer.low_scn
        elif self._emitted_commit[0]:
            self.low_scn = self._emitted_commit[0]
        elif fetched:
            self.low_scn = self._poll_scn

        actions = _ordered_txn_ddl_actions(emitted, ddl_events)
        yielded = False
        working_commit = self._emitted_commit
        working_commit_xid = self._emitted_commit_xid
        window_poll_cursor = (self._poll_scn, self._poll_rs_id, self._poll_ssn)
        first_ddl_position = next(
            (position for position, _order, kind, _payload in actions if kind == "ddl"),
            None,
        )
        for _position, _order, action_type, payload in actions:
            if action_type == "ddl":
                scn, rs_id, ssn, owner, table_name, sql_redo, ddl_timestamp = payload
                self._poll_scn, self._poll_rs_id, self._poll_ssn = scn, rs_id, ssn
                try:
                    self._record_oracle_ddl(
                        owner=owner,
                        table=table_name,
                        ddl=sql_redo,
                        offset={"scn": scn, "rs_id": rs_id, "ssn": ssn},
                        ddl_timestamp=ddl_timestamp,
                        ambiguous_later_ddl=any(
                            later[4] == table_name
                            and mining_position(later[0], later[1], later[2])
                            > mining_position(scn, rs_id, ssn)
                            for later in ddl_events
                        ),
                    )
                except OracleLogMinerReadError:
                    self._restore_last_acked_cursor()
                    raise
                replay_boundaries = [
                    position
                    for position in (
                        buffer.low_scn,
                        *(committed.start_scn for committed in emitted),
                    )
                    if position
                ]
                token = self._buffered_token(
                    position_scn=scn,
                    position_rs_id=rs_id,
                    position_ssn=ssn,
                    low_scn=min(replay_boundaries) if replay_boundaries else scn,
                    commit_cursor=working_commit,
                    commit_xid=working_commit_xid,
                    table=self._token_table_label(),
                )
                yielded = True
                yield ChangeBatch(
                    resume_token=token,
                    table=table_name,
                    ack_barrier=True,
                )
                self._poll_scn, self._poll_rs_id, self._poll_ssn = window_poll_cursor
                continue

            txn: OracleCommittedTxn = payload
            row_events = [
                {
                    "xid": txn.xid,
                    "position": (event.scn, event.rs_id, event.ssn),
                    "op": event.op,
                    "table": event.table,
                    "row": event.row,
                    "_source": event,
                }
                for event in txn.rows
            ]
            retained_rows = {
                id(event["_source"])
                for event in self._coalesce_lob_locator_events(row_events)
            }
            txn.rows = [event for event in txn.rows if id(event) in retained_rows]
            if first_ddl_position is not None and _position < first_ddl_position:
                self._poll_scn = txn.commit_scn
                self._poll_rs_id = txn.commit_rs_id
                self._poll_ssn = txn.commit_ssn
            else:
                self._poll_scn, self._poll_rs_id, self._poll_ssn = window_poll_cursor
            parts = chunk_txn_rows(txn.rows, self.batch_size)
            for chunk_index, chunk in enumerate(parts):
                final_chunk = chunk_index == len(parts) - 1
                endpoint = chunk[-1]
                chunk_lookup_columns = self._enrich_lob_lookup_events(
                    [
                        {
                            "op": event.op,
                            "table": event.table,
                            "row": event.row,
                        }
                        for event in chunk
                        if event.op in {"insert", "update"}
                    ]
                )
                commit_cursor = (
                    (txn.commit_scn, txn.commit_rs_id, txn.commit_ssn)
                    if final_chunk
                    else working_commit
                )
                replay_boundaries = [
                    position
                    for position in (
                        buffer.low_scn,
                        *(committed.start_scn for committed in emitted),
                    )
                    if position
                ]
                chunk_low_scn = min(replay_boundaries)
                token = self._buffered_token(
                    position_scn=txn.commit_scn,
                    position_rs_id=(
                        txn.commit_rs_id if final_chunk else endpoint.rs_id
                    ),
                    position_ssn=(
                        txn.commit_ssn if final_chunk else endpoint.ssn
                    ),
                    low_scn=chunk_low_scn,
                    commit_cursor=commit_cursor,
                    commit_xid=txn.xid if final_chunk else working_commit_xid,
                )
                txn_buf = MultiTableTransactionBuffer(max_events=max(1, len(chunk) + 1))
                txn_buf.begin(txn.xid, lsn=str(txn.commit_scn))
                rejected: list[dict[str, Any]] = []
                for event in chunk:
                    if event.op == "unparsed":
                        rejected.append(dict(event.row.get("_df_rejected") or event.row))
                        continue
                    pk = self.primary_keys.get(event.table, self.primary_key)
                    key_value = event.row.get(pk) if pk else None
                    if key_value is None and pk:
                        key_value = next(
                            (
                                value
                                for column, value in event.row.items()
                                if column.upper() == pk.upper()
                            ),
                            None,
                        )
                    key = str(key_value) if key_value is not None else ""
                    if event.op == "delete":
                        if key:
                            txn_buf.delete(event.table, key, lsn=str(txn.commit_scn))
                        else:
                            rejected.append(
                                {
                                    "table": event.table,
                                    "reason": (
                                        "Oracle LogMiner DELETE lacks the configured "
                                        f"primary key {pk!r}."
                                    ),
                                    "row": event.row,
                                }
                            )
                    elif event.op == "insert":
                        if pk and not key:
                            rejected.append(
                                {
                                    "table": event.table,
                                    "reason": (
                                        "Oracle LogMiner INSERT lacks the configured "
                                        f"primary key {pk!r}."
                                    ),
                                    "row": event.row,
                                }
                            )
                            continue
                        txn_buf.insert(
                            event.table,
                            event.row,
                            pk=key if pk else None,
                            lsn=str(txn.commit_scn),
                        )
                    else:
                        if pk and not key:
                            rejected.append(
                                {
                                    "table": event.table,
                                    "reason": (
                                        "Oracle LogMiner UPDATE lacks the configured "
                                        f"primary key {pk!r}."
                                    ),
                                    "row": event.row,
                                }
                            )
                            continue
                        txn_buf.update(
                            event.table,
                            event.row,
                            pk=key if pk else None,
                            lsn=str(txn.commit_scn),
                        )
                batches = txn_buf.commit(
                    lsn=str(txn.commit_scn),
                    resume_token=token,
                    table_order=self.tables,
                )
                for batch in batches:
                    table_name = str(batch.table or "").upper()
                    if table_name in chunk_lookup_columns:
                        batch.lookup_columns = {
                            table_name: chunk_lookup_columns[table_name]
                        }
                if rejected:
                    batches.append(
                        ChangeBatch(
                            rejected=rejected,
                            resume_token=token,
                            table=self._token_table_label(),
                            ack_barrier=True,
                        )
                    )
                    if len(batches) > 1:
                        batches[-2].ack_barrier = False
                for batch in batches:
                    yielded = True
                    self._last_event_at = datetime.now(timezone.utc)
                    yield batch
            working_commit = (txn.commit_scn, txn.commit_rs_id, txn.commit_ssn)
            working_commit_xid = txn.xid
        self._poll_scn, self._poll_rs_id, self._poll_ssn = window_poll_cursor

        if not was_truncated:
            self._last_completed_window_scn = max(
                self._last_completed_window_scn, int(end_scn)
            )

        if not yielded:
            token = self._buffered_token(
                position_scn=self._poll_scn,
                position_rs_id=self._poll_rs_id,
                position_ssn=self._poll_ssn,
                low_scn=self.low_scn,
            )
            yield ChangeBatch(
                resume_token=token,
                table=self._token_table_label(),
                ack_barrier=True,
            )

    def _poll_shared_multi(self) -> Iterator[ChangeBatch]:
        """One LogMiner session for N tables; demux by XID/SCN with ack_barrier."""
        from itertools import groupby

        from services.cdc_multi_table import MultiTableTransactionBuffer

        table_set = {t.upper() for t in self.tables}
        table_by_lower = {t.lower(): t for t in self.tables}
        tagged: list[tuple[str, int, str, int, str, str, dict[str, Any]]] = []
        # (xid_key, scn, rs_id, ssn, table, op, row)
        end_scn = self.scn
        was_truncated = False
        try:
            with self._mining_conn() as conn:
                with conn.cursor() as cur:
                    assert_resume_scn_in_redo(
                        self.scn,
                        fetch_oldest_available_scn(cur),
                        cursor_key=self.cursor_key,
                    )
                    assert_redo_continuity(
                        self.scn,
                        fetch_redo_inventory(cur),
                        cursor_key=self.cursor_key,
                    )
                    if self._txn_buffer_enabled:
                        cur.execute("SELECT current_scn FROM v$database")
                        head = cur.fetchone()
                        end_scn = int(head[0] or self.scn) if head else self.scn
                    else:
                        end_scn, held = self._buffer_off_window_end()
                        if held and end_scn <= self.scn:
                            yield ChangeBatch(
                                resume_token=self._token(table=self._token_table_label()),
                                ack_barrier=True,
                            )
                            return
                    if end_scn <= self.scn and not self.rs_id:
                        yield ChangeBatch(
                            resume_token=self._token(table=self._token_table_label()),
                            ack_barrier=True,
                        )
                        return
                    in_list = self._table_in_sql()  # nosec: B608 — safe identifiers
                    # Look-ahead past batch_size so we can keep complete XID groups.
                    lim = max(self.batch_size + 1, 64) * max(1, len(self.tables))
                    with _logminer_session(
                        cur,
                        start_scn=max(1, self.scn),
                        end_scn=end_scn,
                        reader_identity=self._logminer_reader_identity(),
                        step_context="shared_multi",
                    ):
                        raw_rows, was_truncated = _fetch_logminer_csf_window(
                            cur,
                            sql=logminer_contents_sql(
                                table_predicate=(
                                    f"TABLE_NAME IN ({in_list}) OR SEG_NAME IN ({in_list})"
                                ),
                                include_xid=True,
                            ),
                            binds={
                                "owner": self.schema,
                                **self._resume_binds(),
                            },
                            limit=lim,
                            csf_index=12,
                            table=self._token_table_label(),
                            cursor_key=self.cursor_key,
                        )
                    ddl_positions = [
                        (
                            str(candidate[5] or candidate[10] or "").upper(),
                            int(candidate[0] or 0),
                            str(candidate[1] or ""),
                            int(candidate[2] or 0),
                        )
                        for candidate in raw_rows
                        if str(candidate[3] or "").upper() == "DDL"
                    ]
                    for row in raw_rows:
                        scn = int(row[0] or 0)
                        rs_id = _row_rs_id(row[1])
                        ssn = int(row[2] or 0)
                        operation = str(row[3] or "").upper()
                        sql_redo = row[4]
                        tbl_raw = str(row[5] or row[10] or "").upper()
                        owner = str(row[6] or "").upper()
                        if operation == "DDL":
                            if owner == self.schema and tbl_raw in table_set:
                                table_name = table_by_lower.get(
                                    tbl_raw.lower(), tbl_raw
                                )
                                tagged.append(
                                    (
                                        f"ddl:{scn}:{rs_id}:{ssn}",
                                        scn,
                                        rs_id,
                                        ssn,
                                        table_name,
                                        "ddl",
                                        {
                                            "owner": owner,
                                            "sql_redo": str(sql_redo or ""),
                                            "timestamp": row[11] if len(row) > 11 else None,
                                            "ambiguous_later": any(
                                                name == table_name
                                                and mining_position(
                                                    later_scn, later_rs, later_ssn
                                                )
                                                > mining_position(scn, rs_id, ssn)
                                                for name, later_scn, later_rs, later_ssn
                                                in ddl_positions
                                            ),
                                        },
                                    )
                                )
                            continue
                        xid_key = f"{row[7] or 0}.{row[8] or 0}.{row[9] or 0}"
                        if not xid_key or xid_key == "0.0.0":
                            xid_key = f"scn:{scn}"
                        if tbl_raw not in table_set:
                            continue
                        table_name = table_by_lower.get(tbl_raw.lower(), tbl_raw)
                        if operation == "SEL_LOB_LOCATOR":
                            self._remember_lob_locator_xid(
                                tbl_raw,
                                tuple(int(value or 0) for value in row[7:10]),
                            )
                            primary_key = self._required_lob_primary_key(table_name)
                            parsed = self._parse_lob_locator_pk(
                                sql_redo or "",
                                primary_key,
                                table=table_name,
                            )
                            parsed[_LOB_LOCATOR_MARKER] = True
                            tagged.append(
                                (
                                    xid_key,
                                    scn,
                                    rs_id,
                                    ssn,
                                    table_name,
                                    "update",
                                    parsed,
                                )
                            )
                            continue
                        if (
                            operation == "UPDATE"
                            and not str(sql_redo or "").strip()
                            and self._has_lob_locator_xid(
                                tbl_raw,
                                tuple(int(value or 0) for value in row[7:10]),
                            )
                        ):
                            continue
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op:
                            continue
                        kind, parsed = classify_sql_redo(
                            sql_redo or "", op=op, table=table_name
                        )
                        tagged_op = "unparsed" if kind == "unparsed" else op
                        # (xid, scn, rs_id, ssn, table, op, row)
                        tagged.append(
                            (xid_key, scn, rs_id, ssn, table_name, tagged_op, parsed)
                        )
        except CdcScnGapError:
            raise
        except OracleLogMinerReadError:
            raise
        except Exception as exc:
            if is_oracle_redo_gap_error(exc):
                raise CdcScnGapError(
                    f"Oracle LogMiner redo unavailable for resume SCN {self.scn}: {exc}",
                    resume_scn=self.scn,
                    cursor_key=self.cursor_key,
                ) from exc
            read_error = OracleLogMinerReadError(
                exc,
                table=self._token_table_label(),
                cursor_key=self.cursor_key,
            )
            raise read_error from exc

        if not tagged:
            self._advance_offset(
                last_scn=self.scn,
                last_rs_id=self.rs_id,
                last_ssn=self.ssn,
                end_scn=end_scn,
                fetched=0,
                limit=1,
            )
            if not was_truncated:
                self._last_completed_window_scn = max(
                    self._last_completed_window_scn, int(end_scn)
                )
            yield ChangeBatch(
                resume_token=self._token(table=self._token_table_label()),
                ack_barrier=True,
            )
            return

        # Truncate was sized against the look-ahead fetch; treat a full look-ahead
        # as truncated so we never jump past unread XIDs.
        look_ahead_lim = max(self.batch_size + 1, 64) * max(1, len(self.tables))
        tagged = self._truncate_tagged_at_xid_boundary(tagged, self.batch_size)
        buf = MultiTableTransactionBuffer()
        emitted = False
        last_scn = self.scn
        last_rs_id = self.rs_id
        last_ssn = self.ssn
        shared_rejected: list[dict[str, Any]] = []

        for xid_key, group_iter in groupby(tagged, key=lambda x: x[0]):
            group = list(group_iter)
            group_scn = max(item[1] for item in group)
            group_endpoint = group[-1][1:4]
            group_events = [
                {
                    "xid": item[0],
                    "position": item[1:4],
                    "table": item[4],
                    "op": item[5],
                    "row": item[6],
                    "_source": item,
                }
                for item in group
            ]
            retained_items = {
                id(event["_source"])
                for event in self._coalesce_lob_locator_events(group_events)
            }
            group = [item for item in group if id(item) in retained_items]
            if len(group) == 1 and group[0][5] == "ddl":
                _xid, scn, rs_id, ssn, table_name, _op, ddl_row = group[0]
                last_scn, last_rs_id, last_ssn = scn, rs_id, ssn
                self.scn, self.rs_id, self.ssn = last_scn, last_rs_id, last_ssn
                self.scn_complete = False
                try:
                    self._record_oracle_ddl(
                        owner=str(ddl_row.get("owner") or ""),
                        table=table_name,
                        ddl=str(ddl_row.get("sql_redo") or ""),
                        offset={"scn": scn, "rs_id": rs_id, "ssn": ssn},
                        ddl_timestamp=ddl_row.get("timestamp"),
                        ambiguous_later_ddl=bool(
                            ddl_row.get("ambiguous_later")
                        ),
                    )
                except OracleLogMinerReadError:
                    self._restore_last_acked_cursor()
                    raise
                self._last_event_at = datetime.now(timezone.utc)
                yield ChangeBatch(
                    resume_token=self._token(table=self._token_table_label()),
                    table=table_name,
                    ack_barrier=True,
                )
                emitted = True
                continue
            lookup_columns = self._enrich_lob_lookup_events(
                [
                    {
                        "xid": item[0],
                        "position": item[1:4],
                        "op": item[5],
                        "table": item[4],
                        "row": item[6],
                    }
                    for item in group
                    if item[5] in {"insert", "update"}
                ]
            )
            buf.begin(xid_key, lsn=str(group_scn))
            for _xid, scn, rs_id, ssn, table_name, op, row in group:
                last_scn, last_rs_id, last_ssn = scn, rs_id, ssn
                if op == "unparsed":
                    # Consume SCN; quarantine instead of dest upsert.
                    shared_rejected.append(row)
                    continue
                pk = self.primary_keys.get(table_name, self.primary_key)
                key = row.get(pk, "")
                if op == "delete":
                    if key:
                        buf.delete(table_name, str(key), lsn=str(scn))
                elif op == "insert":
                    buf.insert(table_name, row, lsn=str(scn))
                else:
                    buf.update(table_name, row, lsn=str(scn))
            self.scn = last_scn
            self.rs_id = last_rs_id
            self.ssn = last_ssn
            last_scn, last_rs_id, last_ssn = group_endpoint
            self.scn, self.rs_id, self.ssn = last_scn, last_rs_id, last_ssn
            self.scn_complete = False
            token = self._token(table=self._token_table_label())
            for batch in buf.commit(
                lsn=str(group_scn), resume_token=token, table_order=self.tables
            ):
                table_name = str(batch.table or "").upper()
                if table_name in lookup_columns:
                    batch.lookup_columns = {
                        table_name: lookup_columns[table_name]
                    }
                emitted = True
                self._last_event_at = datetime.now(timezone.utc)
                yield batch

        if was_truncated:
            self.scn = last_scn
            self.rs_id = last_rs_id
            self.ssn = last_ssn
        else:
            self._advance_offset(
                last_scn=last_scn,
                last_rs_id=last_rs_id,
                last_ssn=last_ssn,
                end_scn=end_scn,
                fetched=len(tagged),
                limit=look_ahead_lim,
            )
        if not was_truncated:
            self._last_completed_window_scn = max(
                self._last_completed_window_scn, int(end_scn)
            )
        token = self._token(table=self._token_table_label())
        if shared_rejected:
            yield ChangeBatch(
                rejected=shared_rejected,
                resume_token=token,
                ack_barrier=True,
            )
        elif not emitted:
            yield ChangeBatch(
                resume_token=token,
                ack_barrier=True,
            )

    @staticmethod
    def _truncate_tagged_at_xid_boundary(
        tagged: list[tuple],
        batch_size: int,
    ) -> list[tuple]:
        """Keep complete XID groups within ``batch_size`` events."""
        if not tagged or len(tagged) <= batch_size:
            return tagged
        edge = tagged[batch_size - 1][0]
        if tagged[batch_size][0] != edge:
            return tagged[:batch_size]
        keep: list[tuple] = []
        for item in tagged[:batch_size]:
            if item[0] == edge:
                break
            keep.append(item)
        if keep:
            return keep
        keep = list(tagged[: batch_size + 1])
        for item in tagged[batch_size + 1 :]:
            if item[0] != edge:
                break
            keep.append(item)
        return keep

    def ack(self, resume_token: Any = None) -> None:
        if resume_token:
            state = decode_logminer_token(resume_token)
            self.resume_token = resume_token
            self._last_acked_token = resume_token
            if self._txn_buffer_enabled:
                self.scn = int(state.get("poll_scn") or state.get("scn") or self.scn)
                self.rs_id = str(state.get("poll_rs_id") or "")
                self.ssn = int(state.get("poll_ssn") or 0)
                self._poll_scn = self.scn
                self._poll_rs_id = self.rs_id
                self._poll_ssn = self.ssn
                self.low_scn = int(state.get("low_scn") or self.scn)
                self.scn_complete = bool(state.get("scn_complete"))
                self._emitted_commit = (
                    int(state.get("commit_scn") or 0),
                    str(state.get("commit_rs_id") or ""),
                    int(state.get("commit_ssn") or 0),
                )
                self._emitted_commit_xid = str(state.get("commit_xid") or "")
                if self._txn_buffer is not None and self._emitted_commit[0]:
                    self._txn_buffer.emitted_commit = (
                        self._emitted_commit[0],
                        self._emitted_commit_xid,
                    )
                    if not self._emitted_commit_xid:
                        self._txn_buffer.emitted_commit = None
                return
            self.scn = int(state.get("scn") or self.scn)
            self.rs_id = str(state.get("rs_id") or "")
            self.ssn = int(state.get("ssn") or 0)
            self.scn_complete = bool(state.get("scn_complete"))

    def _restore_last_acked_cursor(self) -> None:
        if self._last_acked_token:
            self.ack(self._last_acked_token)
        self._txn_buffer = None

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
