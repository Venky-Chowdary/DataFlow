"""Post-write proof that a requested dedupe left one row per key (D402).

An operator who declares ``dedupe_key`` (or ``in_file_dedupe=keep_last``) is
told the run lands one row per key. The collapse itself runs on the read side,
and each source route owned its own copy of that step: S3 skipped it while
SFTP ran it, so 31 rows landed with id 3 twice and Gate-8 still matched 31
source rows to 31 target rows and reported "verified".

This module is the route-independent half: the request's dedupe ask is stamped
on the destination summary, and Gate-8 counts duplicated keys on the
destination itself. A duplicated key fails the job in every validation mode;
a destination that cannot be grouped by key is "unverified", never green.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: dest_summary keys this module owns.
KEYS_SOURCE = "dedupe_key_columns"
KEYS_TARGET = "dedupe_key_targets"


def requested_dedupe_keys(request: Any) -> list[str]:
    """Source-side key columns the operator asked to be unique, or ``[]``.

    ``in_file_dedupe=keep_last`` collapses on the contract key, else the
    declared ``dedupe_key``. A ``dedupe_key`` declared without keep_last (an
    upsert on that key) asks for the same thing: one row per key.
    """
    from services.duplicate_policy import (
        declared_dedupe_columns,
        keep_last_key_columns,
        route_duplicate_options,
    )
    from services.sync_cursor import resolve_sync_contract

    source = getattr(request, "source", None)
    dest = getattr(request, "destination", None)
    extras = (
        dict(getattr(source, "extra", None) or {}),
        dict(getattr(dest, "extra", None) or {}),
    )
    if route_duplicate_options(*extras).get("in_file_dedupe") == "keep_last":
        contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
        return keep_last_key_columns(request, contract)
    return declared_dedupe_columns(*extras)


def _target_names(keys: list[str], mappings: Any) -> list[str]:
    by_source: dict[str, str] = {}
    for m in mappings or []:
        if isinstance(m, dict) and m.get("source") and m.get("target"):
            by_source.setdefault(str(m["source"]), str(m["target"]))
            by_source.setdefault(str(m["source"]).lower(), str(m["target"]))
    return [by_source.get(k) or by_source.get(k.lower()) or k for k in keys]


def stamp_dedupe_request(dest_summary: dict[str, Any], request: Any) -> None:
    """Publish the run's dedupe ask so Gate-8 and the transform proof grade it."""
    keys = requested_dedupe_keys(request)
    if not keys:
        return
    from services.duplicate_policy import route_duplicate_options

    dest_summary[KEYS_SOURCE] = list(keys)
    dest_summary[KEYS_TARGET] = _target_names(keys, getattr(request, "mappings", None))
    options = route_duplicate_options(
        dict(getattr(getattr(request, "source", None), "extra", None) or {}),
        dict(getattr(getattr(request, "destination", None), "extra", None) or {}),
    )
    if options.get("in_file_dedupe") == "keep_last":
        dest_summary.setdefault("in_file_dedupe", "keep_last")
    from services.dedupe_equality import DESTINATION, EXACT, equality_choice

    # The post-write check groups with the equality the collapse used.
    choice = equality_choice(
        dict(getattr(getattr(request, "source", None), "extra", None) or {}),
        dict(getattr(getattr(request, "destination", None), "extra", None) or {}),
    )
    dest_summary["dedupe_key_equality"] = DESTINATION if choice == DESTINATION else EXACT
    if billed_scan_opted_in(request):
        dest_summary[OPT_IN_KEY] = True


def _opt_in_values(request: Any) -> list[Any]:
    return [
        extra[OPT_IN_KEY]
        for extra in (getattr(getattr(request, "source", None), "extra", None),
                      getattr(getattr(request, "destination", None), "extra", None))
        if isinstance(extra, dict) and OPT_IN_KEY in extra
    ]


def billed_scan_opted_in(request: Any) -> bool:
    """Only a JSON ``true`` asks for the scan; the string "false" never does."""
    return any(value is True for value in _opt_in_values(request))


def billed_scan_opt_in_refusal(request: Any) -> str:
    """Why the route's ``dedupe_check_billed_scan`` is not a boolean, or ""."""
    bad = [value for value in _opt_in_values(request) if not isinstance(value, bool)]
    if not bad:
        return ""
    return (
        f"{OPT_IN_KEY} must be true or false (a JSON boolean), not {bad[0]!r}. It decides "
        "whether a billed warehouse is scanned to prove the dedupe."
    )


def _mongo_duplicate_keys(cfg: dict[str, Any], table: str, keys: list[str]) -> int:
    from connectors.mongodb_common import _mongo_client
    from connectors.mongodb_reader import _connection_string

    # The shared pooled client the read-back uses; it is not ours to close.
    db = _mongo_client(_connection_string(cfg))[str(cfg.get("database") or "")]
    group = {"_id": {k: f"${k}" for k in keys}, "n": {"$sum": 1}}
    pipeline = [
        {"$match": {k: {"$ne": None} for k in keys}},
        {"$group": group},
        {"$match": {"n": {"$gt": 1}}},
        {"$count": "dups"},
    ]
    out = list(db[table].aggregate(pipeline, allowDiskUse=True))
    return int(out[0]["dups"]) if out else 0


#: Warehouses that bill a full scan: no unasked GROUP BY over the table (D402).
BILLED_WAREHOUSES = frozenset({"bigquery", "snowflake", "redshift", "databricks"})
OPT_IN_KEY = "dedupe_check_billed_scan"
_TIMEOUT_S = 30


def _key_expr(dialect: str, quoted: str, exact: bool) -> str:
    """The key as the collapse compared it: exact bytes, or the column's own equality."""
    if not exact:
        return quoted
    if dialect == "mysql":
        return f"CAST({quoted} AS BINARY)"
    if dialect in {"mssql", "sqlserver"}:
        return f"CAST({quoted} AS VARBINARY(8000))"
    if dialect == "sqlite":
        return f"{quoted} COLLATE BINARY"
    if dialect == "postgresql":
        # A column can carry a nondeterministic (ICU) collation: "C" is bytes.
        return f'CAST({quoted} AS TEXT) COLLATE "C"'
    return quoted  # the warehouses compare text exactly by default


def _set_timeout(conn: Any, dialect: str) -> None:
    import sqlalchemy as sa

    statements = {
        "postgresql": [f"SET LOCAL statement_timeout = {_TIMEOUT_S * 1000}"],
        # MariaDB, then MySQL: each server ignores the other's variable.
        "mysql": [f"SET SESSION max_statement_time = {_TIMEOUT_S}",
                  f"SET SESSION max_execution_time = {_TIMEOUT_S * 1000}"],
    }.get(dialect, [])
    for stmt in statements:
        try:
            conn.execute(sa.text(stmt))
        except Exception:  # noqa: BLE001 - the variable belongs to the other server
            logger.debug("dedupe check timeout %s not applied", stmt)


def count_duplicate_keys(
    dialect: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table: str,
    columns: list[str],
    exact: bool,
) -> int | None:
    """Repeated keys on the destination (up to 3), ``None`` when a unique key covers them.

    NULL key parts never equal one another, so those rows are left out. The
    query stops at three groups and runs under a statement timeout.
    """
    import sqlalchemy as sa

    from connectors.sql_identifiers import require_safe_identifier
    from services.dedupe_equality import _unique_key_covers
    from services.dest_precount import warehouse_sql_engine, warehouse_sql_table_ref

    table_ref = warehouse_sql_table_ref(dialect, cfg, schema, table)
    with warehouse_sql_engine(dialect, cfg) as engine:
        quote = engine.dialect.identifier_preparer.quote
        cols = [quote(require_safe_identifier(c, preserve_case=True)) for c in columns]
        with engine.connect() as conn:
            if _unique_key_covers(conn, dialect, schema="" if dialect == "mysql" else schema,
                                  table=table, columns=columns):
                return None
            keys = ", ".join(_key_expr(dialect, c, exact) for c in cols)
            where = " AND ".join(f"{c} IS NOT NULL" for c in cols)
            top, limit = ("TOP 3 ", "") if dialect in {"mssql", "sqlserver"} else ("", " LIMIT 3")
            stmt = sa.text(
                f"SELECT {top}1 FROM {table_ref} WHERE {where} "  # nosec B608 - quoted identifiers
                f"GROUP BY {keys} HAVING COUNT(*) > 1{limit}"
            )
            _set_timeout(conn, dialect)
            found = len(conn.execute(stmt).fetchall())
            conn.rollback()
            return found


def _artifact_duplicate_keys(dest_summary: dict[str, Any], keys: list[str], fmt: str) -> int | None:
    """Repeated keys in a written file, read back from disk; ``None`` when unreadable."""
    import os

    from services.dest_precount import UnmeasuredArtifact, _iter_artifact_records

    path = str(dest_summary.get("path") or dest_summary.get("export_path") or "")
    if not path or not os.path.isfile(path):
        return None
    seen: set[tuple] = set()
    repeated: set[tuple] = set()
    try:
        with open(path, "rb") as fh:
            for row in _iter_artifact_records(fh, name=os.path.basename(path), fmt=fmt,
                                              written_here=True,
                                              tsv_null=dest_summary.get("tsv_null")):
                key = tuple(row.get(k) for k in keys)
                if any(v is None or v == "" for v in key):
                    continue
                (repeated if key in seen else seen).add(key)
    except (UnmeasuredArtifact, OSError, ValueError):
        return None
    return len(repeated)


def artifact_dedupe_check(dest_summary: dict[str, Any], *, fmt: str) -> dict[str, Any] | None:
    """The same post-write check on a file/object export (D402): never green unchecked."""
    keys = [str(k) for k in dest_summary.get(KEYS_TARGET) or [] if str(k).strip()]
    if not keys:
        return None
    base: dict[str, Any] = {"check": "dedupe_key_unique", "keys": keys, "equality": EXACT_TEXT}
    dups = _artifact_duplicate_keys(dest_summary, keys, fmt)
    if dups is None:
        return {**base, "status": "unverified", "reason": "export not readable back"}
    return _verdict(base, keys, dups)


EXACT_TEXT = "exact"


def _verdict(base: dict[str, Any], keys: list[str], dups: int) -> dict[str, Any]:
    if dups:
        return {
            **base,
            "status": "failed",
            "duplicates": int(dups),
            "detail": (
                f"dedupe on {', '.join(keys)} was requested but "
                f"{'at least ' if dups >= 3 else ''}{int(dups)} key(s) "
                "are held more than once on the destination"
            ),
        }
    return {**base, "status": "passed"}


def infile_dedupe_check(
    dest_summary: dict[str, Any],
    *,
    db_type: str,
    cfg: dict[str, Any],
    schema: str,
    table_name: str,
    whole_table: bool,
    sql_dialect: str | None,
) -> dict[str, Any] | None:
    """Grade the destination for keys held more than once, or ``None`` (no ask)."""
    keys = [str(k) for k in dest_summary.get(KEYS_TARGET) or [] if str(k).strip()]
    if not keys:
        return None
    equality = str(dest_summary.get("dedupe_key_equality") or EXACT_TEXT)
    base: dict[str, Any] = {"check": "dedupe_key_unique", "keys": keys, "equality": equality}
    if not whole_table:
        # Prior rows were not written by this run; an append keeps every
        # version (R6), so uniqueness is not this run's contract there.
        return {**base, "status": "not_applicable", "reason": "destination held prior rows"}
    if not table_name:
        return {**base, "status": "unverified", "reason": "no destination table"}
    kind = str(db_type or "").lower()
    if kind in BILLED_WAREHOUSES and not dest_summary.get(OPT_IN_KEY):
        return {**base, "status": "unverified",
                "reason": "dedupe not checked on a billed warehouse (no scan was asked for)"}
    try:
        if sql_dialect is not None:
            dups = count_duplicate_keys(sql_dialect, cfg, schema=schema, table=table_name,
                                        columns=keys, exact=equality != "destination")
            if dups is None:
                return {**base, "status": "passed", "reason": "a unique key on the destination covers it"}
        elif kind == "mongodb":
            dups = _mongo_duplicate_keys(cfg, table_name, keys)
        else:
            return {**base, "status": "unverified", "reason": "unsupported_destination"}
    except Exception as exc:  # noqa: BLE001 - an unrun check is unverified, never green
        logger.warning("dedupe key check failed: error_type=%s", type(exc).__name__)
        return {**base, "status": "unverified", "reason": "count_error"}
    return _verdict(base, keys, dups)


def apply_infile_dedupe_check(stamped: dict[str, Any], check: dict[str, Any] | None) -> None:
    """Fold the check into the Gate-8 report: a duplicated key is never green."""
    if not check:
        return
    stamped.setdefault("dedupe_checks", []).append(check)
    status = check.get("status")
    if status == "failed":
        stamped["passed"] = False
        stamped["verification_verdict"] = "failed"
        stamped["message"] = f"Dedupe verification failed: {check['detail']}"
    elif status == "unverified":
        stamped["unproven"] = True
        stamped["migration_proven"] = False
        stamped["verification_verdict"] = "partial"
        prior = str(stamped.get("message") or "").strip()
        stamped["message"] = (
            f"Dedupe unverified ({check.get('reason')}): one row per key is not proven"
            + (f" — {prior}" if prior else "")
        )
