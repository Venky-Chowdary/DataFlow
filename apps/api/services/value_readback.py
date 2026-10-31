"""Cell-value read-back helpers for verification (D212).

QA D212: an Oracle -> SQL Server engine copy stored '東京' as '??' and the
job went green, because the route proved ``COUNT(*)`` only — the key census
that compares values had "no census driver" for Oracle, and a count-only
verdict with no value compare still passed.

* :func:`oracle_census_connection` gives the census an Oracle side (CLOB
  cells read as text, NUMBER as exact decimals);
* :func:`text_round_trip_risks` names the mapped text columns whose
  destination carrier is a code page while the source can hold wider text —
  a write there may be lossy, so a count-only verdict must not pass without
  a value compare.
"""

from __future__ import annotations

from typing import Any, Iterable


def oracle_census_connection(cfg: dict[str, Any]) -> Any:
    """An ``oracledb`` connection whose CLOB/NCLOB cells fetch as ``str``."""
    import oracledb

    from services.copy_oracle_oracle import _oracle_connect

    conn = _oracle_connect(cfg)
    number_handler = getattr(conn, "outputtypehandler", None)

    def handler(cursor: Any, metadata: Any) -> Any:
        code = getattr(metadata, "type_code", None)
        if code is oracledb.DB_TYPE_CLOB or code is oracledb.DB_TYPE_NCLOB:
            return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
        if code is oracledb.DB_TYPE_BLOB:
            return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
        return number_handler(cursor, metadata) if number_handler else None

    conn.outputtypehandler = handler
    return conn


def _code_page_carrier(dest_db: str, dest_type: str) -> bool:
    """True when a destination text carrier stores a single-byte code page."""
    from services.encoding_capacity import classify_capacity, parse_declared_charset
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        _SQLSERVER_FAMILY,
        _normalize_dest_db,
        is_national_string_carrier,
        normalize_logical_type,
    )

    text = (dest_type or "").strip()
    if not text or is_national_string_carrier(text):
        return False
    if normalize_logical_type(text) not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    if parse_declared_charset(text).strip():
        cap = classify_capacity(dest_db, text)
        return 0 < cap.max_code_point <= 0xFF
    # SQL Server VARCHAR/CHAR/TEXT without a *_UTF8 collation is a code page.
    return _normalize_dest_db(dest_db) in _SQLSERVER_FAMILY


def text_round_trip_risks(
    *,
    source_db: str,
    dest_db: str,
    pairs: Iterable[tuple[str, str]],
    source_types: dict[str, str],
    dest_types: dict[str, str],
) -> list[str]:
    """Target columns where a Unicode-capable source lands on a code page."""
    from services.type_system import source_text_declared_single_byte

    src_l = {str(k).lower(): str(v or "") for k, v in (source_types or {}).items()}
    dst_l = {str(k).lower(): str(v or "") for k, v in (dest_types or {}).items()}
    out: list[str] = []
    for source_col, target_col in pairs:
        dest_type = dst_l.get(str(target_col).lower(), "")
        if not _code_page_carrier(dest_db, dest_type):
            continue
        source_type = src_l.get(str(source_col).lower(), "")
        if source_type and source_text_declared_single_byte(source_db, source_type):
            continue
        out.append(str(target_col))
    return out


def count_only_text_risk_verdict(
    *,
    source_endpoint: Any,
    endpoint: Any,
    mappings: list[dict[str, Any]] | None,
    reason: str | None,
) -> dict[str, Any] | None:
    """A failed verdict when no value compare ran over a possibly lossy text write.

    ``None`` when every mapped text column round-trips by construction (a
    national / UTF-8 carrier, or a source that declares the same single-byte
    code page), so the count-only verdict stands as before.
    """
    pairs: list[tuple[str, str]] = []
    source_types: dict[str, str] = {}
    dest_types: dict[str, str] = {}
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        source_col = str(m.get("source") or "").strip()
        target_col = str(m.get("target") or m.get("destination") or source_col).strip()
        if not source_col or not target_col:
            continue
        pairs.append((source_col, target_col))
        source_types[source_col] = str(m.get("source_type") or m.get("type") or "")
        dest_types[target_col] = str(m.get("target_type") or "")
    dest_db = str(getattr(endpoint, "format", "") or "")
    live = _live_dest_types(endpoint, [t for _s, t in pairs])
    dest_types.update({k: v for k, v in live.items() if v})
    risky = text_round_trip_risks(
        source_db=str(getattr(source_endpoint, "format", "") or ""),
        dest_db=dest_db,
        pairs=pairs,
        source_types=source_types,
        dest_types=dest_types,
    )
    if not risky:
        return None
    why = reason or "no value compare ran on this route"
    return {
        "passed": False,
        "verification_verdict": "failed",
        "culprit_keys": {"text_columns_not_round_trip_proven": risky},
        "value_compare": {
            "ran": False,
            "passed": False,
            "reason": why,
            "text_columns_on_code_page": risky,
        },
        "message": (
            f"Verification failed: {', '.join(risky[:6])} store a single-byte code "
            "page on the destination while the source text can hold any character, "
            f"and {why}. Characters such as '東京' or emoji may have been written "
            "as '?'. Row counts match, but values are not proven — use an NVARCHAR "
            "(or UTF-8 collation) destination column."
        ),
    }


def _live_dest_types(endpoint: Any, columns: list[str]) -> dict[str, str]:
    """Physical SQL Server column types (with collation) — best effort."""
    from services.type_system import _SQLSERVER_FAMILY, _normalize_dest_db

    if _normalize_dest_db(str(getattr(endpoint, "format", "") or "")) not in _SQLSERVER_FAMILY:
        return {}
    table = str(getattr(endpoint, "table", "") or "").strip()
    if not table or not columns:
        return {}
    try:
        from services.copy_sqlserver_sqlserver import _ss_connect
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(endpoint)
        conn = _ss_connect(cfg)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLLATION_NAME "
                "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? "
                "AND TABLE_SCHEMA = ?",
                (table.split(".")[-1], str(cfg.get("schema") or "dbo")),
            )
            out: dict[str, str] = {}
            for name, dtype, width, collation in cur.fetchall():
                w = "MAX" if width in (-1, None) else str(width)
                spelled = f"{str(dtype).upper()}({w})" if width is not None else str(dtype).upper()
                if collation:
                    spelled += f" COLLATE {collation}"
                out[str(name)] = spelled
            return out
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 - the mapping's target type stays the answer
        return {}
