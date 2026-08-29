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
from datetime import datetime, timezone
from typing import Any, Iterator

from connectors.sql_identifiers import quote_sql_identifier
from services.cdc_cursor_gap import CdcScnGapError
from services.cdc_engine import ChangeBatch

logger = logging.getLogger(__name__)


_OP_MAP = {
    "INSERT": "insert",
    "UPDATE": "update",
    "DELETE": "delete",
}

UNPARSED_SQL_REDO_FLAG = "_df_unparsed_sql_redo"
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
    if rs_id:
        payload["rs_id"] = str(rs_id)
        payload["ssn"] = int(ssn or 0)
    if phase == "snapshot":
        if offset:
            payload["offset"] = int(offset)
        if last_pk:
            payload["last_pk"] = str(last_pk)
    return json.dumps(payload, separators=(",", ":"))


def decode_logminer_token(token: str | None) -> dict[str, Any]:
    empty = {
        "scn": 0,
        "phase": "initial",
        "table": "",
        "rs_id": "",
        "ssn": 0,
        "offset": 0,
        "last_pk": "",
    }
    if not token:
        return empty
    try:
        from services.cdc_resume_tokens import unwrap_resume_token

        data = unwrap_resume_token(token)
        if not isinstance(data, dict):
            parsed = json.loads(str(token))
            data = unwrap_resume_token(parsed) if not isinstance(parsed, dict) else parsed
        if isinstance(data, dict) and data.get("kind") == "oracle-logminer":
            return {
                "scn": int(data.get("scn") or 0),
                "phase": str(data.get("phase") or "streaming"),
                "table": str(data.get("table") or ""),
                "rs_id": str(data.get("rs_id") or ""),
                "ssn": int(data.get("ssn") or 0),
                "offset": int(data.get("offset") or 0),
                "last_pk": str(data.get("last_pk") or ""),
            }
    except Exception as exc:
        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
    return dict(empty)


def _logminer_options_sql() -> str:
    """START_LOGMNR OPTIONS for committed-only mining without CONTINUOUS_MINE.

    ``CONTINUOUS_MINE`` was desupported in 19c — on those hosts START_LOGMNR
    raises and the prior ``except: return`` path made CDC look healthy while
    delivering nothing forever. ``COMMITTED_DATA_ONLY`` excludes in-flight and
    rolled-back DML so a ROLLBACK cannot land as a real destination change.
    """
    return (
        "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    )


def register_logminer_logs(cur: Any, *, start_scn: int, end_scn: int) -> int:
    """ADD_LOGFILE every online/archived redo covering ``[start_scn, end_scn]``.

    Returns the number of files registered. Zero is a hard failure — mining
    without files yields an empty contents view and a silent no-op poll.

    Raises if neither catalog view could be read, so an unreadable catalog is
    never mistaken for "no redo covers this window".
    """
    start = int(start_scn or 0)
    end = int(end_scn or 0)
    files: list[str] = []
    listing_errors = 0
    try:
        cur.execute(
            """
            SELECT DISTINCT lf.MEMBER
            FROM V$LOGFILE lf
            JOIN V$LOG l ON l.GROUP# = lf.GROUP#
            WHERE l.FIRST_CHANGE# <= :end_scn
              AND (l.NEXT_CHANGE# IS NULL OR l.NEXT_CHANGE# > :start_scn)
            """,
            {"start_scn": start, "end_scn": end},
        )
        files.extend(str(r[0]) for r in (cur.fetchall() or []) if r and r[0])
    except Exception as exc:
        listing_errors += 1
        logger.debug("Oracle V$LOGFILE listing failed: %s", exc)
    try:
        cur.execute(
            """
            SELECT NAME
            FROM V$ARCHIVED_LOG
            WHERE DELETED = 'NO'
              AND FIRST_CHANGE# <= :end_scn
              AND (NEXT_CHANGE# IS NULL OR NEXT_CHANGE# > :start_scn)
            """,
            {"start_scn": start, "end_scn": end},
        )
        files.extend(str(r[0]) for r in (cur.fetchall() or []) if r and r[0])
    except Exception as exc:
        listing_errors += 1
        logger.debug("Oracle V$ARCHIVED_LOG listing failed: %s", exc)

    if not files and listing_errors >= 2:
        raise RuntimeError(
            "Oracle LogMiner could not read V$LOGFILE or V$ARCHIVED_LOG; "
            "grant SELECT on the redo catalog views before mining."
        )

    # Deduplicate while preserving order.
    seen: set[str] = set()
    unique = []
    for path in files:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)

    for path in unique:
        cur.execute(
            """
            BEGIN
              DBMS_LOGMNR.ADD_LOGFILE(
                LOGFILENAME => :fname,
                OPTIONS => DBMS_LOGMNR.ADDFILE
              );
            END;
            """,
            {"fname": path},
        )
    return len(unique)


def start_logminer_session(cur: Any, *, start_scn: int, end_scn: int) -> None:
    """Start a committed-data-only LogMiner session over the SCN window.

    Raises on failure — callers must not treat a failed start as an empty poll.
    """
    registered = register_logminer_logs(cur, start_scn=start_scn, end_scn=end_scn)
    if registered <= 0:
        # The catalog was readable but no redo covers the window — the changes
        # we need have aged out. That is a redo gap, so raise the gap error the
        # resume path already knows how to handle (re-snapshot) rather than a
        # generic failure that would retry the same empty window forever.
        raise CdcScnGapError(
            f"Oracle LogMiner found no redo covering SCN [{start_scn}, {end_scn}]; "
            "the required redo has aged out — a fresh snapshot is required."
        )
    options = _logminer_options_sql()
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
    ``(SCN, RS_ID, SSN)`` is LogMiner's true total order.
    """
    xid_cols = ", XIDUSN, XIDSLT, XIDSEQ" if include_xid else ""
    return f"""
        SELECT SCN, RS_ID, SSN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER{xid_cols}
        FROM (
          SELECT SCN, RS_ID, SSN, OPERATION, SQL_REDO, TABLE_NAME, SEG_OWNER{xid_cols}
          FROM v$logmnr_contents
          WHERE SEG_OWNER = :owner
            AND {table_predicate}
            AND OPERATION IN ('INSERT','UPDATE','DELETE')
            AND (
                  SCN > :start_scn
               OR (SCN = :start_scn AND RS_ID > :rs_id)
               OR (SCN = :start_scn AND RS_ID = :rs_id AND SSN > :ssn)
            )
          ORDER BY SCN, RS_ID, SSN
        )
        WHERE ROWNUM <= {limit_bind}
    """  # nosec B608 — table_predicate / limit_bind are caller-built from safe ids


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
    if not v or v.upper() == "NULL":
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
        out[col] = "" if val is None or str(val).upper() == "NULL" else str(val)
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
    Delivery remains **at-least-once**.
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
        resume_token: str | None = None,
        cursor_key: str = "",
    ) -> None:
        from services.cdc_multi_table import normalize_table_list, tables_digest
        from services.cdc_identity import require_cdc_primary_keys_map

        self.cfg = cfg
        raw_tables = normalize_table_list(table)
        if not raw_tables:
            raise ValueError("Oracle LogMiner CDC requires at least one table")
        self.tables = [t.upper() for t in raw_tables]
        self.table = self.tables[0]
        self.schema = (schema or cfg.get("schema") or cfg.get("username") or "").upper()
        # Normalize caller keys to upper for Oracle identifiers.
        normalized_pks: dict[str, Any] | None = None
        if primary_keys:
            normalized_pks = {
                str(k).upper(): str(v).upper() for k, v in primary_keys.items() if k and v
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
        self.phase = str(state.get("phase") or "initial")
        self.snapshot_offset = int(state.get("offset") or 0)
        self.snapshot_last_pk = str(state.get("last_pk") or "")
        self.snapshot_table = str(state.get("table") or "")
        self.resume_token = resume_token
        self._last_event_at: datetime | None = None
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
            **self._lease.theater_fields(),
        }

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

    def _table_in_sql(self) -> str:
        """Safe IN-list for LogMiner TABLE_NAME filter (uppercased identifiers)."""
        from connectors.sql_identifiers import require_safe_identifier

        parts = []
        for t in self.tables:
            safe = require_safe_identifier(t, preserve_case=True).upper()
            parts.append(f"'{safe}'")
        return ", ".join(parts)

    def _empty_window_retry_sec(self) -> float:
        from services.brand_env import getenv_brand

        try:
            return max(0.0, float(getenv_brand("ORACLE_EMPTY_RETRY_SEC", "0.25") or 0.25))
        except (TypeError, ValueError):
            return 0.25

    def _query_logminer_rows(self, cur: Any, *, table_predicate: str, limit: int) -> list[Any]:
        cur.execute(
            logminer_contents_sql(table_predicate=table_predicate),
            {
                "owner": self.schema,
                "tbl": self.table,
                "lim": int(limit),
                **self._resume_binds(),
            },
        )
        return list(cur.fetchall() or [])

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
        start_logminer_session(cur, start_scn=max(1, int(start_scn)), end_scn=int(end_scn))
        rows = self._query_logminer_rows(cur, table_predicate=table_predicate, limit=limit)
        if rows or int(end_scn) <= int(start_scn):
            return rows, int(end_scn)
        delay = self._empty_window_retry_sec()
        if delay > 0:
            try:
                cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
            except Exception as exc:
                logger.debug("Oracle END_LOGMNR before visibility retry: %s", exc)
            time.sleep(delay)
            cur.execute("SELECT current_scn FROM v$database")
            head = cur.fetchone()
            end_scn = max(int(end_scn), int(head[0] or end_scn) if head else int(end_scn))
            start_logminer_session(
                cur, start_scn=max(1, int(start_scn)), end_scn=int(end_scn)
            )
            rows = self._query_logminer_rows(
                cur, table_predicate=table_predicate, limit=limit
            )
        return rows, int(end_scn)

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
                    str(clean_cols[i]).upper(): "" if values[i] is None else str(values[i])
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
        self.scn = handoff
        self.phase = "streaming"
        self.snapshot_offset = 0
        self.snapshot_last_pk = ""
        self.resume_token = encode_logminer_token(
            self.scn, table=self._token_table_label(), phase="streaming"
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
        self.scn = handoff
        self.phase = "streaming"
        self.snapshot_offset = 0
        self.snapshot_last_pk = ""
        self.resume_token = encode_logminer_token(
            self.scn, table=self._token_table_label(), phase="streaming"
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
                if last_pk:
                    sql, params = snapshot_keyset_sql(
                        table_ref=qualified,
                        quoted_pk_columns=quoted,
                        last_pk=last_pk,
                        limit=limit,
                        dialect="oracle",
                    )
                    cur.execute(sql, params)
                else:
                    order_sql = ", ".join(quoted)
                    cur.execute(
                        f"SELECT * FROM ("  # nosec B608
                        f"SELECT * FROM {qualified} ORDER BY {order_sql}"
                        f") WHERE ROWNUM <= :lim",
                        {"lim": limit},
                    )
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() or []
        records = self._rows_to_records(cols, rows)
        new_last = last_pk_from_records(records, pk_cols) or last_pk
        done = len(records) < limit
        return records, str(new_last) if new_last is not None else last_pk, done

    def _resume_binds(self) -> dict[str, Any]:
        return {
            "start_scn": int(self.scn or 0),
            # Empty rs_id sorts before any real RS_ID under Oracle string order
            # when resuming at a fresh SCN (ssn=0, rs_id="").
            "rs_id": self.rs_id or "",
            "ssn": int(self.ssn or 0),
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

        A truncated window (``fetched == limit``) stops at the last consumed
        ``(scn, rs_id, ssn)``. Only a short window may jump to ``end_scn``.
        """
        if fetched <= 0:
            # Empty contents are not proof the window is exhausted. LGWR can
            # still be flushing committed DML — jumping to end_scn skipped
            # those SCNs forever (dest leftover after resume delete).
            # Keep the cursor so the next poll re-mines the same range.
            return
        if fetched >= int(limit):
            self.scn = int(last_scn or self.scn or 0)
            self.rs_id = str(last_rs_id or "")
            self.ssn = int(last_ssn or 0)
            return
        self.scn = max(int(last_scn or 0), int(end_scn or 0))
        self.rs_id = ""
        self.ssn = 0

    def _token(self, *, table: str | None = None) -> str:
        return encode_logminer_token(
            self.scn,
            table=table or self.table,
            phase="streaming",
            rs_id=self.rs_id,
            ssn=self.ssn,
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
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
                    # An intra-SCN (rs_id, ssn) cursor still has unread changes
                    # at self.scn even when current_scn has not advanced. Bailing
                    # out here lets a mid-chunk UPDATE lose to the snapshot row
                    # (DDD-3 stream-wins relies on this peek).
                    if end_scn < self.scn or (end_scn == self.scn and not self.rs_id):
                        return events
                    start_logminer_session(
                        cur, start_scn=max(1, self.scn), end_scn=end_scn
                    )
                    cur.execute(
                        logminer_contents_sql(table_predicate="TABLE_NAME = :tbl"),
                        {
                            "owner": self.schema,
                            "tbl": self.table,
                            "lim": peek_limit,
                            **self._resume_binds(),
                        },
                    )
                    for row in cur.fetchall() or []:
                        operation = row[3]
                        sql_redo = row[4]
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
                            events.append({"op": "d", "pk": key, "row": {self.primary_key: key}})
                        elif op == "insert":
                            events.append({"op": "c", "row": parsed})
                        else:
                            events.append({"op": "u", "row": parsed})
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception as exc:
                        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        except Exception:
            return events
        return events

    def poll(self) -> Iterator[ChangeBatch]:
        self._acquire_cdc_lease()
        if self.phase != "streaming" or self.scn <= 0:
            yield from self.snapshot()
            return

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

        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        deletes: list[str] = []
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
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
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
                        table_predicate="TABLE_NAME = :tbl",
                        limit=self.batch_size,
                    )
                    fetched = len(rows)
                    for row in rows:
                        scn = int(row[0] or 0)
                        rs_id = str(row[1] or "")
                        ssn = int(row[2] or 0)
                        operation = row[3]
                        sql_redo = row[4]
                        last_scn, last_rs_id, last_ssn = scn, rs_id, ssn
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
                                deletes.append(key)
                        elif op == "insert":
                            inserts.append(parsed)
                        else:
                            updates.append(parsed)
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception as exc:
                        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
                    self._advance_offset(
                        last_scn=last_scn,
                        last_rs_id=last_rs_id,
                        last_ssn=last_ssn,
                        end_scn=end_scn,
                        fetched=fetched,
                        limit=self.batch_size,
                    )
        except CdcScnGapError:
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
            raise RuntimeError(f"Oracle LogMiner poll failed: {exc}") from exc

        token = self._token()
        if inserts or updates or deletes or rejected:
            yield ChangeBatch(
                inserts=inserts,
                updates=updates,
                deletes=deletes,
                resume_token=token,
                table=self.table,
                rejected=rejected,
            )
        else:
            yield ChangeBatch(resume_token=token, table=self.table)

    def _poll_shared_multi(self) -> Iterator[ChangeBatch]:
        """One LogMiner session for N tables; demux by XID/SCN with ack_barrier."""
        from itertools import groupby

        from services.cdc_multi_table import MultiTableTransactionBuffer

        table_set = {t.upper() for t in self.tables}
        table_by_lower = {t.lower(): t for t in self.tables}
        tagged: list[tuple[str, int, str, int, str, str, dict[str, Any]]] = []
        # (xid_key, scn, rs_id, ssn, table, op, row)
        end_scn = self.scn
        try:
            with self._mining_conn() as conn:
                with conn.cursor() as cur:
                    assert_resume_scn_in_redo(
                        self.scn,
                        fetch_oldest_available_scn(cur),
                        cursor_key=self.cursor_key,
                    )
                    cur.execute("SELECT current_scn FROM v$database")
                    head = cur.fetchone()
                    end_scn = int(head[0] or self.scn) if head else self.scn
                    if end_scn <= self.scn and not self.rs_id:
                        yield ChangeBatch(
                            resume_token=self._token(table=self._token_table_label()),
                            ack_barrier=True,
                        )
                        return
                    start_logminer_session(
                        cur, start_scn=max(1, self.scn), end_scn=end_scn
                    )
                    in_list = self._table_in_sql()  # nosec: B608 — safe identifiers
                    # Look-ahead past batch_size so we can keep complete XID groups.
                    lim = max(self.batch_size + 1, 64) * max(1, len(self.tables))
                    cur.execute(
                        logminer_contents_sql(
                            table_predicate=f"TABLE_NAME IN ({in_list})",
                            include_xid=True,
                        ),
                        {
                            "owner": self.schema,
                            "lim": lim,
                            **self._resume_binds(),
                        },
                    )
                    raw_rows = list(cur.fetchall() or [])
                    for row in raw_rows:
                        scn = int(row[0] or 0)
                        rs_id = str(row[1] or "")
                        ssn = int(row[2] or 0)
                        operation = row[3]
                        sql_redo = row[4]
                        tbl_raw = str(row[5] or "").upper()
                        xid_key = f"{row[7] or 0}.{row[8] or 0}.{row[9] or 0}"
                        if not xid_key or xid_key == "0.0.0":
                            xid_key = f"scn:{scn}"
                        op = _OP_MAP.get(str(operation or "").upper())
                        if not op or tbl_raw not in table_set:
                            continue
                        table_name = table_by_lower.get(tbl_raw.lower(), tbl_raw)
                        kind, parsed = classify_sql_redo(
                            sql_redo or "", op=op, table=table_name
                        )
                        tagged_op = "unparsed" if kind == "unparsed" else op
                        # (xid, scn, rs_id, ssn, table, op, row)
                        tagged.append(
                            (xid_key, scn, rs_id, ssn, table_name, tagged_op, parsed)
                        )
                    try:
                        cur.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
                    except Exception as exc:
                        logger.warning("Exception suppressed: %s", exc, exc_info=exc)
        except CdcScnGapError:
            raise
        except Exception as exc:
            if is_oracle_redo_gap_error(exc):
                raise CdcScnGapError(
                    f"Oracle LogMiner redo unavailable for resume SCN {self.scn}: {exc}",
                    resume_scn=self.scn,
                    cursor_key=self.cursor_key,
                ) from exc
            raise RuntimeError(f"Oracle shared LogMiner poll failed: {exc}") from exc

        if not tagged:
            self._advance_offset(
                last_scn=self.scn,
                last_rs_id=self.rs_id,
                last_ssn=self.ssn,
                end_scn=end_scn,
                fetched=0,
                limit=1,
            )
            yield ChangeBatch(
                resume_token=self._token(table=self._token_table_label()),
                ack_barrier=True,
            )
            return

        # Truncate was sized against the look-ahead fetch; treat a full look-ahead
        # as truncated so we never jump past unread XIDs.
        look_ahead_lim = max(self.batch_size + 1, 64) * max(1, len(self.tables))
        was_truncated = len(tagged) >= look_ahead_lim
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
            token = self._token(table=self._token_table_label())
            for batch in buf.commit(
                lsn=str(group_scn), resume_token=token, table_order=self.tables
            ):
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
            state = decode_logminer_token(str(resume_token))
            self.scn = int(state.get("scn") or self.scn)
            self.rs_id = str(state.get("rs_id") or "")
            self.ssn = int(state.get("ssn") or 0)

    def lag_seconds(self) -> float | None:
        if self._last_event_at is None:
            return None
        return max(0.0, (datetime.now(timezone.utc) - self._last_event_at).total_seconds())

    def replication_lag_seconds(self) -> float | None:
        return self.lag_seconds()
