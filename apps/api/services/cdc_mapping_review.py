"""CDC schema drift → Map review signals.

When native CDC detects DDL / catalog drift, operators must re-open Map — never
silently remap. Signals are append-only and acknowledgeable.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.shared_doc_store import SharedLog
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "cdc_mapping_reviews.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = (getenv_brand("CDC_MAPPING_REVIEW_STORE", "") or "").strip()
    return Path(env) if env else STORE_PATH


# Shared across replicas when a control-plane store is configured: the CDC
# worker flags a review, an API pod lists and acknowledges it (H-5).
_LOG = SharedLog("cdc_mapping_reviews", file_path=_store_path, key_field="id")


def flag_mapping_review(
    *,
    source_key: str,
    table: str,
    reason: str,
    schema_version: int | None = None,
    ddl: str = "",
    column_names: list[str] | None = None,
    workspace_id: str = "",
) -> dict[str, Any]:
    """Record that CDC schema drifted and Map review is required."""
    from services.workspace_filter import current_workspace_stamp

    signal = {
        "id": str(uuid.uuid4()),
        "source_key": str(source_key or "").strip(),
        "workspace_id": str(workspace_id or "") or current_workspace_stamp(),
        "table": str(table or "").strip(),
        "reason": reason or "schema_drift",
        "schema_version": schema_version,
        "ddl": (ddl or "")[:500],
        "column_names": list(column_names or [])[:200],
        "status": "open",
        "created_at": _now(),
        "acknowledged_at": None,
        "honesty": (
            "CDC delivery remains at-least-once. Schema drift does not auto-remap — "
            "open Map, review fidelity, and re-Validate before continuing."
        ),
    }
    coll = _LOG.backend()
    if coll is not None:
        _LOG.append(coll, signal)
    else:
        path = _store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(signal, default=json_default) + "\n")
    logger.warning(
        "CDC mapping review required: %s.%s (%s)",
        signal["source_key"],
        signal["table"],
        reason,
    )
    return signal


def list_reviews(
    *,
    source_key: str | None = None,
    status: str = "open",
    limit: int = 50,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    coll = _LOG.backend()
    if coll is not None:
        rows: list[Any] = _LOG.records(coll)
    else:
        path = _store_path()
        if not path.exists():
            return []
        try:
            rows = path.read_text(encoding="utf-8").splitlines()
        except Exception as exc:
            logger.error("Failed reading CDC mapping review store: %s", exc)
            return []
    from services.workspace_filter import bound_allows

    for line in reversed(rows):
        if isinstance(line, dict):
            row = line
        else:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
        if status and status != "all" and row.get("status") != status:
            continue
        if source_key and row.get("source_key") != source_key:
            continue
        # Rows flagged before workspace stamping carry no workspace_id; they
        # are only visible to callers whose scope includes unscoped rows.
        if not bound_allows(row.get("workspace_id")):
            continue
        out.append(row)
        if len(out) >= limit:
            break
    return out


def acknowledge_review(review_id: str) -> dict[str, Any] | None:
    from services.workspace_filter import bound_allows

    coll = _LOG.backend()
    if coll is not None:
        done: list[dict[str, Any]] = []

        def acknowledge(row: dict[str, Any]) -> dict[str, Any] | None:
            if row.get("status") != "open" or not bound_allows(row.get("workspace_id")):
                return None
            row = {**row, "status": "acknowledged", "acknowledged_at": _now()}
            done[:] = [row]
            return row

        _LOG.update(coll, review_id, acknowledge)
        return done[0] if done else None
    path = _store_path()
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    updated: dict[str, Any] | None = None
    rewritten: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            rewritten.append(line)
            continue
        if (
            row.get("id") == review_id
            and row.get("status") == "open"
            and bound_allows(row.get("workspace_id"))
        ):
            row["status"] = "acknowledged"
            row["acknowledged_at"] = _now()
            updated = row
        rewritten.append(json.dumps(row, default=json_default))
    if updated is None:
        return None
    path.write_text("\n".join(rewritten) + "\n", encoding="utf-8")
    return updated


#: The two doors that release a held review: the MCP tools Pilot exposes and
#: the REST routes of ``cdc_mapping_review_router`` (D346).
LIST_REMEDY = "MCP tool list_cdc_mapping_reviews, or GET /api/v1/cdc/mapping-reviews?source_key=<key>"


def acknowledge_remedy(review_id: str) -> str:
    """How a caller acknowledges one review, by both doors."""
    rid = str(review_id or "").strip() or "<review_id>"
    return (
        f"MCP tool acknowledge_cdc_mapping_review with review_id '{rid}', or "
        f"POST /api/v1/cdc/mapping-reviews/{rid}/acknowledge"
    )


def review_source_keys(cfg: dict[str, Any], connector_id: str = "") -> list[str]:
    """Every source key a CDC reader may have flagged a review under.

    A reader stamps ``connector:<id>`` when it knows its saved connector and the
    host fingerprint otherwise, so both are looked up.
    """
    from services.cdc_schema_history import connection_fingerprint

    keys: list[str] = []
    for key in (
        connection_fingerprint(cfg, connector_id=connector_id) if connector_id else "",
        connection_fingerprint(cfg),
    ):
        if key and key not in keys:
            keys.append(key)
    return keys


def _table_parts(table: str, default_schema: str = "") -> tuple[str, str]:
    from connectors.sql_identifiers import split_qualified_table

    schema, name = split_qualified_table(str(table or ""), default_schema or None)
    return str(schema or "").casefold(), str(name or "").casefold()


def review_matches_table(review_table: str, table: str, default_schema: str = "") -> bool:
    """Whether a review flagged on ``review_table`` concerns ``table``.

    Readers stamp the table in their own spelling (``"public"."orders"``,
    ``dbo.orders``, ``orders``) while a schedule stores the operator's spelling,
    so both are unquoted and compared case-insensitively. A side that names no
    schema matches on the table name alone (the hold stays on when the schema
    cannot tell them apart). An empty table matches nothing: an unnamed table
    is never a wildcard for every table of the source (D346).
    """
    r_schema, r_name = _table_parts(review_table)
    t_schema, t_name = _table_parts(table, default_schema)
    if not r_name or not t_name or r_name != t_name:
        return False
    return not (r_schema and t_schema) or r_schema == t_schema


def open_review_for_tables(
    source_key: str, tables: list[str], *, default_schema: str = ""
) -> dict[str, Any] | None:
    """Latest open review on a CDC source that concerns one of ``tables``.

    A review holds the table it was flagged on, never every table that shares
    the source connector; no table named means nothing is held (D346).
    """
    wanted = [str(t or "").strip() for t in tables or [] if str(t or "").strip()]
    if not wanted:
        return None
    # The store is read whole either way; the bound only keeps reviews on
    # other tables of a busy source from hiding this table's review.
    for row in list_reviews(source_key=source_key, status="open", limit=10_000):
        review_table = str(row.get("table") or "")
        if any(review_matches_table(review_table, t, default_schema) for t in wanted):
            return row
    return None


def open_review_for_source(
    source_key: str, table: str = "", *, default_schema: str = ""
) -> dict[str, Any] | None:
    """Latest open review for ``table`` on a CDC source (``""`` matches nothing)."""
    return open_review_for_tables(source_key, [table], default_schema=default_schema)
