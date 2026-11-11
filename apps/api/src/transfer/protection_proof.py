"""Post-write protection proof: no protected column holds a source value in clear.

D355: ``mask_pii`` on a JSON document left every value no PII pattern matched
in clear, and the job was green. Gate-8 compares the written stream with the
destination and the G-1 transform proof re-applies the same transform, so both
agree with a transform that protects nothing. This proof asks the question the
operator asked instead: for every column a protecting transform covers (the
column's Map transform, or a step of its pipeline), does the destination still
hold a value — the whole cell, or any value inside a JSON document — that the
source holds in clear? If it does the run fails, naming the columns, never the
values.

The check is bounded: up to ``_ROW_CAP`` source rows and destination rows, so
a table up to that size is checked completely. It runs only where the
destination population is this run's (an overwrite, or a table this run
created); an append or upsert into an occupied table may hold rows another run
wrote, so it is recorded as skipped.
"""

from __future__ import annotations

import logging
from typing import Any

from .models import EndpointConfig

_logger = logging.getLogger(__name__)

PROTECTING_TRANSFORMS = frozenset({"mask_pii", "hash_pii", "redact"})
_ROW_CAP = 1000


def protected_columns(
    mappings: list[dict] | None, shape_recipe: dict[str, Any] | None
) -> list[tuple[str, str, str]]:
    """``(source, target, transform)`` for every column a protection covers."""
    from services.sync_cursor import map_source_to_target
    from services.transform_engine import normalize_engine_transform

    mappings = [m for m in (mappings or []) if isinstance(m, dict)]
    out: dict[tuple[str, str], str] = {}
    for m in mappings:
        src = str(m.get("source") or "").strip()
        canon = normalize_engine_transform(str(m.get("transform") or "")) or ""
        if src and canon.lower() in PROTECTING_TRANSFORMS:
            out[(src, str(m.get("target") or src))] = canon.lower()
    steps = (shape_recipe or {}).get("steps") if isinstance(shape_recipe, dict) else None
    mapped_sources = {str(m.get("source") or "") for m in mappings}
    for step in steps or []:
        if not isinstance(step, dict) or step.get("enabled") is False:
            continue
        op = str(step.get("op") or "").lower()
        col = str(step.get("column") or (step.get("options") or {}).get("column") or "")
        if op in PROTECTING_TRANSFORMS and col and col in mapped_sources:
            out.setdefault((col, map_source_to_target(col, mappings)), op)
    return [(s, t, tf) for (s, t), tf in out.items()]


def _population_is_this_runs(dest_summary: dict[str, Any]) -> bool:
    from services.sync_cursor import is_overwrite_sync

    sync_mode = str(
        dest_summary.get("sync_mode") or dest_summary.get("effective_sync_mode") or ""
    )
    return is_overwrite_sync(sync_mode) or dest_summary.get("dest_table_existed_before") is False


def _cell(row: dict[str, Any], col: str) -> Any:
    if col in row:
        return row[col]
    folded = {str(k).casefold(): v for k, v in row.items()}
    return folded.get(col.casefold())


def _source_tokens(
    source_endpoint: EndpointConfig, cols: list[str]
) -> tuple[dict[str, set[str]], int, bool]:
    """Clear values per source column, from up to ``_ROW_CAP`` source rows."""
    from services.pii_mask_transform import clear_tokens

    from .adapters import resolve_connector_config
    from .connector_capabilities import resolve_driver_type
    from .reconcile_step import _proof_source_batches

    src_cfg = resolve_connector_config(source_endpoint)
    src_type = resolve_driver_type(str(source_endpoint.format or src_cfg.get("type") or ""))
    tokens: dict[str, set[str]] = {c: set() for c in cols}
    rows = 0
    batches = _proof_source_batches(
        source_endpoint=source_endpoint, src_type=src_type, src_cfg=src_cfg, read_cols=cols
    )
    try:
        for batch in batches:
            for row in batch:
                if rows >= _ROW_CAP:
                    return tokens, rows, False
                rows += 1
                for c in cols:
                    tokens[c] |= clear_tokens(_cell(row, c))
    finally:
        close = getattr(batches, "close", None)
        if close is not None:
            close()
    return tokens, rows, True


def protection_proof(
    *,
    endpoint: EndpointConfig,
    source_endpoint: EndpointConfig | None,
    dest_summary: dict[str, Any],
    mappings: list[dict] | None,
    shape_recipe: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """``None`` when no column is protected; otherwise the proof's verdict."""
    covered = protected_columns(mappings, shape_recipe)
    if not covered:
        return None
    columns = sorted({t for _s, t, _tf in covered})
    if not _population_is_this_runs(dest_summary):
        return {
            "status": "skipped",
            "columns": columns,
            "reason": "the destination may hold rows another run wrote",
        }
    if source_endpoint is None:
        return {"status": "unverifiable", "columns": columns,
                "reason": "the run's source was not handed to the verifier"}
    from services.dialect_profiles import schema_from_cfg
    from services.pii_mask_transform import clear_tokens
    from services.reconciliation import read_target_sample

    from .adapters import resolve_connector_config
    from .connector_capabilities import resolve_driver_type
    from .proof_api_source import proof_unavailable_reason

    if proof_unavailable_reason(source_endpoint, resolve_driver_type(source_endpoint.format or "")):
        return {"status": "unverifiable", "columns": columns,
                "reason": "this source cannot be re-read for the proof"}
    src_cols = list(dict.fromkeys(s for s, _t, _tf in covered))
    try:
        tokens, src_rows, complete = _source_tokens(source_endpoint, src_cols)
        cfg = resolve_connector_config(endpoint)
        db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or "")).lower()
        dest_rows = read_target_sample(
            db_type,
            cfg,
            schema=str(dest_summary.get("schema") or schema_from_cfg(db_type, cfg) or ""),
            table_name=str(
                dest_summary.get("table") or dest_summary.get("collection")
                or endpoint.table or endpoint.collection or ""
            ),
            columns=columns,
            limit=_ROW_CAP,
        )
    except Exception as exc:  # noqa: BLE001 - an unreadable side is unverifiable, never verified
        _logger.warning("protection proof could not read: %s", type(exc).__name__)
        return {"status": "unverifiable", "columns": columns,
                "reason": "the source or destination could not be re-read"}
    leaked: dict[str, int] = {}
    for row in dest_rows:
        if not isinstance(row, dict):
            continue
        for src, tgt, _tf in covered:
            if clear_tokens(_cell(row, tgt)) & tokens.get(src, set()):
                leaked[tgt] = leaked.get(tgt, 0) + 1
    result: dict[str, Any] = {
        "columns": columns,
        "source_rows_checked": src_rows,
        "destination_rows_checked": len(dest_rows),
        "complete": complete and len(dest_rows) < _ROW_CAP,
    }
    if leaked:
        cols_txt = ", ".join(f"{c} ({n} row{'s' if n != 1 else ''})" for c, n in sorted(leaked.items()))
        return {
            **result,
            "status": "failed",
            "leaked_columns": sorted(leaked),
            "reason": (
                f"the destination still holds source values in clear in {cols_txt}, "
                "which a protecting transform (mask_pii / hash_pii / redact) covers"
            ),
        }
    return {**result, "status": "verified"}


def apply_protection_proof(recon: dict[str, Any], proof: dict[str, Any] | None) -> None:
    """Record the proof; a leak fails the run whatever else agreed."""
    if not proof:
        return
    recon["protection_proof"] = proof
    if proof.get("status") != "failed":
        return
    head = f"Protection proof failed: {proof['reason']}."
    prior = str(recon.get("message") or "")
    # A passing message ("checksums match") would contradict the verdict; a
    # failing one keeps its own cause first.
    recon["message"] = f"{prior} {head}" if prior and recon.get("passed") is False else head
    recon["passed"] = False
    recon["verification_verdict"] = "failed"
