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
from typing import Any, Callable

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


def infile_dedupe_check(
    dest_summary: dict[str, Any],
    *,
    db_type: str,
    cfg: dict[str, Any],
    table_name: str,
    whole_table: bool,
    sql_dialect: str | None,
    count_sql_duplicates: Callable[[list[str]], int],
) -> dict[str, Any] | None:
    """Grade the destination for keys held more than once, or ``None`` (no ask)."""
    keys = [str(k) for k in dest_summary.get(KEYS_TARGET) or [] if str(k).strip()]
    if not keys:
        return None
    base: dict[str, Any] = {"check": "dedupe_key_unique", "keys": keys}
    if not whole_table:
        # Prior rows were not written by this run; an append keeps every
        # version (R6), so uniqueness is not this run's contract there.
        return {**base, "status": "not_applicable", "reason": "destination held prior rows"}
    if not table_name:
        return {**base, "status": "unverified", "reason": "no destination table"}
    try:
        if sql_dialect is not None:
            dups = count_sql_duplicates(keys)
        elif str(db_type or "").lower() == "mongodb":
            dups = _mongo_duplicate_keys(cfg, table_name, keys)
        else:
            return {**base, "status": "unverified", "reason": "unsupported_destination"}
    except Exception as exc:  # noqa: BLE001 - an unrun check is unverified, never green
        logger.warning("dedupe key check failed: error_type=%s", type(exc).__name__)
        return {**base, "status": "unverified", "reason": "count_error"}
    if dups:
        return {
            **base,
            "status": "failed",
            "duplicates": int(dups),
            "detail": (
                f"dedupe on {', '.join(keys)} was requested but {int(dups)} key(s) "
                "are held more than once on the destination"
            ),
        }
    return {**base, "status": "passed"}


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
