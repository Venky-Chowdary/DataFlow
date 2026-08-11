"""Real transfer planning and confirm-gated execution for Datawrap Pilot.

The pilot could describe transfers but never run one, and what it *did* describe
was invented: the old ``plan_transfer_route`` matched substrings like "csv" in a
connector name and returned a hardcoded gate list whose IDs did not even exist in
``PREFLIGHT_GATES``. Advice that looks authoritative and is fabricated is worse
than no advice, because an operator acts on it.

This module replaces that with the engine's own answers:

``plan_transfer``
    Introspects **both** endpoints live, runs the canonical
    ``run_mapping_pipeline`` for column mapping, type conversions and fidelity
    risk, then runs the real 9-gate preflight (``run_file_preflight`` +
    ``apply_policy_gates``) and persists it so the operator gets a citable
    ``run_id``. Nothing here is heuristic — every claim traces to a gate result
    or a mapping proof entry.

``start_transfer``
    Never executes. It re-plans, refuses outright when preflight blocks, and
    otherwise stages the ``TransferRequest`` in the server-side ack ledger,
    returning ``requires_confirm`` plus a redacted preview. The transfer only
    runs after an explicit Confirm through ``POST /copilot/confirm``, which is
    also where credentials stay — the chat transcript never carries them.

Two safety rules are deliberate and load-bearing:

* ``skip_preflight`` is never settable from chat. The gates are the only thing
  standing between a typo and a truncated destination table.
* An overwriting sync mode has to be asked for in words. Defaulting to
  overwrite because it "just works" would let one ambiguous sentence delete a
  destination table's contents.
"""

from __future__ import annotations

import logging
from typing import Any

from .query_tools import _tool_result
from .schema_tools import (
    AmbiguousConnectorError,
    _safe_connector,
    introspect_connector_table as _introspect,
)

_LOG = logging.getLogger(__name__)

# Engine tokens, not prose. The old stub returned labels like "Incremental CDC"
# that no engine call accepts. Every token here resolves to a canonical mode in
# ``services.sync_cursor``, which :func:`normalize_sync_mode` enforces.
SYNC_MODES = (
    "full_refresh_append",
    "full_refresh_overwrite",
    "incremental_append",
    "incremental_upsert",
    "cdc_incremental",
)

# Only these words authorise destroying rows that are already there. Matching is
# whole-word (see :func:`sync_mode_from_phrase`), not substring: plain
# ``in`` matching let a table named ``replacements`` select overwrite and wipe a
# destination the operator never asked to clear.
_OVERWRITE_PHRASES = (
    "overwrite",
    "replace",
    "truncate",
    "wipe",
    # "full refresh" alone is NOT destructive — operators say that for a
    # reload-append. Only the explicit overwrite form authorises a wipe.
    "full refresh overwrite",
    "full_refresh_overwrite",
)
_UPSERT_PHRASES = ("upsert", "merge", "dedupe", "deduplicate", "incremental upsert", "incremental_upsert")
_APPEND_PHRASES = ("append", "add to", "insert into", "full refresh append", "full_refresh_append")

_MAX_PREVIEW_MAPPINGS = 40


def sync_mode_from_phrase(spoken: str, *, default: str = "full_refresh_append") -> str:
    """Map a natural-language operator phrase to a sync-mode token.

    Returns a raw candidate that is then canonicalised by
    ``services.sync_cursor.normalize_sync_mode``. Phrase matching uses
    whole-phrase / word-boundary checks so a table named ``replacements``
    cannot authorise a destructive overwrite.
    """
    text = (spoken or "").strip().lower()
    if not text:
        return default
    if text in SYNC_MODES:
        return text
    # Exact phrase first, then token-boundary contains for multi-word phrases.
    for phrase in _OVERWRITE_PHRASES:
        if text == phrase or f" {phrase} " in f" {text} ":
            return "full_refresh_overwrite"
    for phrase in _UPSERT_PHRASES:
        if text == phrase or f" {phrase} " in f" {text} ":
            return "incremental_upsert"
    if text == "cdc" or "change data capture" in text or text.startswith("cdc "):
        return "cdc_incremental"
    for phrase in _APPEND_PHRASES:
        if text == phrase or f" {phrase} " in f" {text} ":
            return "full_refresh_append"
    return default


def normalize_sync_mode(spoken: str, *, default: str = "full_refresh_append") -> str:
    """Pilot-facing wrapper: phrase → sync-mode token, engine-validated.

    The Pilot keeps emitting its historical spellings because every engine path
    already aliases them onto canonical modes. What changed is that the result
    is now *checked* against the one canonical table in ``services.sync_cursor``
    before it is returned, so a phrase can no longer resolve to a token the
    engine would quietly ignore and degrade to full-read + insert.
    """
    from services.sync_cursor import CANONICAL_SYNC_MODES
    from services.sync_cursor import normalize_sync_mode as _canonical

    candidate = sync_mode_from_phrase(spoken, default=default)
    if _canonical(candidate, default=default) not in CANONICAL_SYNC_MODES:
        _LOG.warning(
            "Pilot phrase %r produced sync_mode %r, which no engine mode "
            "accepts; falling back to the non-destructive default %r.",
            spoken,
            candidate,
            default,
        )
        return default
    return candidate


def _column_samples(rows: list[dict[str, Any]], columns: list[str]) -> dict[str, list[str]]:
    """Per-column sample values, nulls excluded, in the pipeline's shape."""
    out: dict[str, list[str]] = {}
    for name in columns:
        vals: list[str] = []
        for row in rows:
            v = row.get(name)
            if v is None or v == "":
                continue
            vals.append(str(v))
            if len(vals) >= 20:
                break
        out[name] = vals
    return out


def _schema_rows(
    columns: list[dict[str, Any]],
    samples: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    """Shape introspected columns the way the mapping pipeline expects.

    Samples matter more than they look. The confidence blend carries a
    ``data_quality`` axis derived from real values; with no samples it settles
    near 0.375 and drags an exact ``id``→``id`` match down to 0.70, which is
    below G4/G9 thresholds. A schema-only plan would therefore block transfers
    between *identical* tables. Feeding the rows we already sampled for the
    gates fixes the confidence at its source rather than lowering the gate.
    """
    samples = samples or {}
    rows: list[dict[str, Any]] = []
    for col in columns:
        name = str(col.get("name") or "").strip()
        if not name:
            continue
        rows.append({
            "name": name,
            "inferred_type": str(col.get("inferred_type") or "TEXT"),
            "nullable": bool(col.get("nullable", True)),
            "samples": list(col.get("samples") or samples.get(name) or [])[:20],
        })
    return rows


def _transfer_decision(preflight: dict[str, Any]) -> str:
    """Execute decision from proof bundle — never invent approve from passed alone."""
    return str(
        (
            (preflight.get("proof_bundle") or {}).get("transfer_decision") or {}
        ).get("decision")
        or ""
    ).strip().lower()


def _is_execute_cleared(preflight: dict[str, Any]) -> bool:
    """Same bar as Studio Execute — passed + approve; never local / review-grade."""
    run_id = str(preflight.get("run_id") or "")
    if run_id.startswith("pf_local_"):
        return False
    return bool(preflight.get("passed") and _transfer_decision(preflight) == "approve")


_RISKY_FIDELITY = frozenset({"lossy_cast", "cast", "mutate"})


def _risky_conversions(conversions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Cast / mutate / lossy_cast — never call these round-trip-safe."""
    return [
        c for c in conversions
        if str(c.get("fidelity") or "").strip().lower() in _RISKY_FIDELITY
    ]


def _dest_table_exists_tri_state(dst_info: dict[str, Any]) -> bool | None:
    """True / False / None — never invent create-new from failed introspect.

    None = schema pending / incomplete (Studio schema_pending / schema_incomplete).
    False = proven missing (error names the table as absent).
    True = columns loaded from an existing table.
    """
    if dst_info.get("columns"):
        return True
    if dst_info.get("ok"):
        # Connected but zero columns — incomplete metadata, not create-new.
        return None
    err = str(dst_info.get("error") or "").lower()
    if any(
        tok in err
        for tok in (
            "not found",
            "does not exist",
            "doesn't exist",
            "unknown relation",
            "unknown table",
            "no such table",
            "invalid object name",
        )
    ):
        return False
    return None


def _type_conversions(mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every column whose carrier changes, with the fidelity verdict attached."""
    out: list[dict[str, Any]] = []
    for m in mappings:
        src_type = str(m.get("source_type") or m.get("inferred_type") or "").upper()
        dst_type = str(m.get("target_type") or m.get("destination_type") or "").upper()
        transform = str(m.get("transform") or "")
        fidelity = str(m.get("fidelity") or m.get("risk") or "")
        # Create-new domain risks are fidelity risks even when fidelity stamp is empty.
        if not fidelity and m.get("create_new_risks"):
            fidelity = "cast"
        if not src_type and not dst_type:
            continue
        if (
            src_type == dst_type
            and not transform
            and not m.get("create_new_risks")
            and str(fidelity).strip().lower() not in _RISKY_FIDELITY
        ):
            continue
        out.append({
            "source_column": m.get("source_column") or m.get("source"),
            "target_column": m.get("target_column") or m.get("target"),
            "from_type": src_type,
            "to_type": dst_type,
            "transform": transform,
            "fidelity": fidelity,
            "confidence": m.get("confidence"),
        })
    return out


def plan_transfer(
    source_connector_id: str = "",
    source_connector_name: str = "",
    source_table: str = "",
    dest_connector_id: str = "",
    dest_connector_name: str = "",
    dest_table: str = "",
    sync_mode: str = "",
    schema_policy: str = "manual_review",
    validation_mode: str = "balanced",
    write_via_staging: bool = False,
):
    """Plan a real transfer: live schemas, real mapping, real preflight gates."""
    tool = "plan_transfer"
    src_table = (source_table or "").strip()
    if not src_table:
        from .example_phrases import example_connector_name, example_dest_connector_name

        src_ex = example_connector_name()
        dst_ex = example_dest_connector_name(source_hint=src_ex)
        return _tool_result(
            tool,
            success=False,
            error=(
                "Which table should I move? Example: "
                f'"plan a transfer of orders from {src_ex} to {dst_ex}".'
            ),
        )
    dst_table = (dest_table or src_table).strip()

    try:
        src_conn, err = _safe_connector(source_connector_id, source_connector_name, tool)
        if err:
            return err
        dst_conn, err = _safe_connector(dest_connector_id, dest_connector_name, tool)
        if err:
            return err
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)

    if str(src_conn.get("id")) == str(dst_conn.get("id")) and src_table == dst_table:
        return _tool_result(
            tool,
            success=False,
            error="Source and destination are the same table — nothing to move.",
        )

    try:
        src_info = _introspect(src_conn, src_table, purpose="source")
    except Exception as exc:
        _LOG.warning("plan_transfer source introspect failed: %s", exc, exc_info=True)
        return _tool_result(tool, success=False, error=f"Could not read the source: {exc}")
    if not src_info["ok"] or not src_info["columns"]:
        return _tool_result(
            tool,
            success=False,
            error=(
                f"Could not read `{src_table}` on {src_conn.get('name')}"
                + (f": {src_info['error']}" if src_info["error"] else "")
                + '. Ask me to "list tables on that connector".'
            ),
        )

    try:
        dst_info = _introspect(dst_conn, dst_table, purpose="destination")
    except Exception as exc:
        _LOG.warning("plan_transfer dest introspect failed: %s", exc, exc_info=True)
        dst_info = {"ok": False, "error": str(exc), "columns": [], "db_type": "", "cfg": {}}

    dest_exists = _dest_table_exists_tri_state(dst_info)
    mode = normalize_sync_mode(sync_mode)

    # Sample once: the same real rows drive mapping confidence and the gates.
    sample_rows = _sample_rows(src_conn, src_table)
    src_names = [str(c.get("name")) for c in src_info["columns"] if c.get("name")]
    samples = _column_samples(sample_rows, src_names)
    src_rows = _schema_rows(src_info["columns"], samples)
    dst_rows = _schema_rows(dst_info.get("columns") or [])

    from services.mapping_pipeline import run_mapping_pipeline

    mapping = run_mapping_pipeline(
        [r["name"] for r in src_rows],
        [r["name"] for r in dst_rows],
        source_schemas=src_rows,
        target_schemas=dst_rows,
        source_samples=samples,
        validation_mode=validation_mode,
        destination_db_type=str(dst_info.get("db_type") or ""),
        schema_policy=schema_policy,
        sync_mode=mode,
        destination_table_exists=dest_exists,
        # Both ends were introspected, so their DDL is fact, not a guess.
        source_types_authoritative=True,
        use_llm=False,
    )
    mappings = list(mapping.get("mappings") or [])

    preflight = _run_preflight(
        src_conn=src_conn,
        dst_conn=dst_conn,
        src_table=src_table,
        dst_table=dst_table,
        src_rows=src_rows,
        sample_rows=sample_rows,
        mappings=mappings,
        mode=mode,
        schema_policy=schema_policy,
        validation_mode=validation_mode,
        src_db_type=str(src_info.get("db_type") or ""),
        source_config=_endpoint_dict(src_info.get("endpoint")),
        dest_db_type=str(dst_info.get("db_type") or ""),
        dest_exists=dest_exists,
        write_via_staging=bool(write_via_staging),
    )

    conversions = _type_conversions(mappings)
    unmapped = [
        r["name"]
        for r in src_rows
        if not any((m.get("source_column") or m.get("source")) == r["name"] for m in mappings)
    ]
    proof = mapping.get("mapping_proof") or {}

    return _tool_result(
        tool,
        success=True,
        output={
            "action": "plan_transfer",
            "risk": "safe",
            "source": {
                "connector_id": str(src_conn.get("id") or ""),
                "connector_name": src_conn.get("name"),
                "type": src_info.get("db_type"),
                "schema": str(src_conn.get("schema") or ""),
                "table": src_table,
                "column_count": len(src_rows),
            },
            "destination": {
                "connector_id": str(dst_conn.get("id") or ""),
                "connector_name": dst_conn.get("name"),
                "type": dst_info.get("db_type"),
                "schema": str(dst_conn.get("schema") or ""),
                "table": dst_table,
                "column_count": len(dst_rows),
                "table_exists": dest_exists,
            },
            # Display lists below are truncated for readability; execution must
            # use this untouched list or wide tables would lose columns.
            "engine_mappings": mappings,
            "column_types": {r["name"]: r["inferred_type"] for r in src_rows},
            "sync_mode": mode,
            "schema_policy": schema_policy,
            "validation_mode": validation_mode,
            "mapped_count": len(mappings),
            "unmapped_source_columns": unmapped[:20],
            "type_conversions": conversions[:_MAX_PREVIEW_MAPPINGS],
            "lossy_conversions": _risky_conversions(conversions),
            "mappings": [
                {
                    "source": m.get("source_column") or m.get("source"),
                    "target": m.get("target_column") or m.get("target"),
                    "confidence": m.get("confidence"),
                    "transform": m.get("transform") or "",
                }
                for m in mappings[:_MAX_PREVIEW_MAPPINGS]
            ],
            "mapping_proof": {
                "dest_mode": proof.get("dest_mode"),
                "identity_score": proof.get("identity_score"),
                "risks": (proof.get("risks") or [])[:10],
            },
            "quality_issues": (mapping.get("quality_issues") or [])[:10],
            "coercion_issues": (mapping.get("coercion_issues") or [])[:10],
            "preflight": preflight,
            # Align with Execute unlock — passed alone must not invent safe_to_start.
            "safe_to_start": _is_execute_cleared(preflight),
        },
    )


def _run_preflight(
    *,
    src_conn: dict[str, Any],
    dst_conn: dict[str, Any],
    src_table: str,
    dst_table: str,
    src_rows: list[dict[str, Any]],
    sample_rows: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    mode: str,
    schema_policy: str,
    validation_mode: str,
    src_db_type: str,
    source_config: dict[str, Any],
    dest_db_type: str,
    dest_exists: bool | None,
    write_via_staging: bool = False,
) -> dict[str, Any]:
    """Run the real 9 gates and persist the run so the operator can cite it."""
    from services.preflight_run_store import save_preflight_run
    from services.preflight_service import (
        apply_policy_gates,
        confidence_threshold_for_mode,
        inspect_destination_for_preflight,
        run_file_preflight,
        run_transfer_policy_gates,
    )

    try:
        dest_probe = inspect_destination_for_preflight(
            connector_id=str(dst_conn.get("id") or ""),
            dest_type=dest_db_type,
            dest_table=dst_table,
            dest_collection=dst_table,
            dest_schema=str(dst_conn.get("schema") or ""),
        )
    except Exception as exc:
        _LOG.warning("destination probe failed: %s", exc, exc_info=True)
        dest_probe = {"connected": False, "error": str(exc)}

    columns = [r["name"] for r in src_rows]
    column_types = {r["name"]: r["inferred_type"] for r in src_rows}
    # G7 capacity sizes batches from the real volume, so send the exact count
    # rather than the sample size, which would understate a large table.
    row_count = _exact_row_count(src_conn, src_table)
    if row_count is None:
        row_count = len(sample_rows)

    try:
        policy_gates = run_transfer_policy_gates(
            sync_mode=mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            stream_contracts=[],
            backfill_new_fields=False,
            source_columns=columns,
            dest_type=dest_db_type,
            source_type=src_db_type,
            source_kind="database",
            # G12 must match Studio / Execute — Pilot cannot soft-skip staging policy.
            write_via_staging=bool(write_via_staging),
        )
        # These must mirror ``UniversalTransferEngine`` exactly. The source
        # config and table are what enable the live coercion probe; without
        # them the pilot would promise "safe to start" and the engine would
        # then block the run, which is worse than refusing up front.
        # Never invent can_create=True when privilege probe omitted the flag.
        can_create = dest_probe.get("can_create_table")
        result = run_file_preflight(
            columns=columns,
            column_types=column_types,
            column_nullability={r["name"]: bool(r.get("nullable", True)) for r in src_rows},
            row_count=row_count,
            mappings=mappings,
            destination_connected=bool(dest_probe.get("connected")),
            sample_rows=sample_rows,
            sync_mode=mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            destination_column_types=dest_probe.get("column_types") or {},
            destination_column_nullability=dest_probe.get("column_nullability") or {},
            destination_column_defaults=dest_probe.get("column_defaults") or {},
            destination_identity_columns=dest_probe.get("identity_columns") or [],
            destination_generated_columns=dest_probe.get("generated_columns") or [],
            destination_table_exists=dest_exists,
            destination_can_create=can_create if isinstance(can_create, bool) else None,
            destination_db_type=dest_db_type,
            destination_table=dst_table,
            source_kind="database",
            source_format=src_db_type,
            source_table=src_table,
            source_connector_id=str(src_conn.get("id") or ""),
            source_config=source_config,
            confidence_threshold=confidence_threshold_for_mode(validation_mode),
        )
        result = apply_policy_gates(
            result,
            policy_gates,
            validation_mode=validation_mode,
            destination_db_type=dest_db_type,
        )
        result = save_preflight_run(
            result,
            source_label=f"{src_conn.get('name')}.{src_table}",
            dest_label=f"{dst_conn.get('name')}.{dst_table}",
            validation_mode=validation_mode,
            route=f"{src_conn.get('type')}->{dst_conn.get('type')}",
        )
    except Exception as exc:
        _LOG.warning("preflight failed: %s", exc, exc_info=True)
        # A preflight that could not run is never reported as a pass.
        return {
            "passed": False,
            "error": str(exc),
            "gates": [],
            "blockers": [{"id": "preflight", "message": f"Preflight could not run: {exc}"}],
        }

    return {
        "run_id": result.get("run_id"),
        "passed": bool(result.get("passed")),
        "readiness_score": result.get("readiness_score"),
        "passed_count": result.get("passed_count"),
        "total_gates": result.get("total_gates"),
        "gates": [
            {"id": g.get("id"), "status": g.get("status"), "message": g.get("message")}
            for g in (result.get("gates") or [])
        ],
        "blockers": [
            {"id": b.get("id") or b.get("gate_id"), "message": b.get("message")}
            for b in (result.get("blockers") or [])
        ][:10],
        "warnings": (result.get("warnings") or [])[:10],
    }


def _endpoint_dict(endpoint: Any) -> dict[str, Any]:
    """Serialise an endpoint the way the engine hands it to preflight."""
    if endpoint is None:
        return {}
    try:
        from src.transfer.models import endpoint_to_dict

        return dict(endpoint_to_dict(endpoint) or {})
    except Exception as exc:
        _LOG.info("endpoint serialisation unavailable: %s", exc)
        return {}


def _exact_row_count(conn: dict[str, Any], table: str) -> int | None:
    """Server-side COUNT(*) through the shared aggregation engine."""
    from .aggregate_tools import aggregate_connector_data

    try:
        res = aggregate_connector_data(
            connector_id=str(conn.get("id") or ""),
            table=table,
            metric="count",
        )
        if res.success:
            return int((res.output or {}).get("value") or 0)
    except Exception as exc:
        _LOG.info("row count unavailable for %s: %s", table, exc)
    return None


def _sample_rows(conn: dict[str, Any], table: str, limit: int = 50) -> list[dict[str, Any]]:
    """Real source rows for the sampling gates — never synthesised.

    G5 dry-run and G9 integrity judge actual values. Feeding them invented rows
    would turn preflight into theatre, so an unavailable sample yields an empty
    list and lets those gates report SKIP honestly.
    """
    from .query_tools import sample_connector_object

    try:
        res = sample_connector_object(
            connector_id=str(conn.get("id") or ""),
            table=table,
            limit=limit,
            analyze=False,
        )
        if not res.success:
            return []
        return list((res.output or {}).get("rows") or [])
    except Exception as exc:
        _LOG.info("sample for preflight unavailable: %s", exc)
        return []


def start_transfer(
    source_connector_id: str = "",
    source_connector_name: str = "",
    source_table: str = "",
    dest_connector_id: str = "",
    dest_connector_name: str = "",
    dest_table: str = "",
    sync_mode: str = "",
    schema_policy: str = "manual_review",
    validation_mode: str = "balanced",
    limit: int = 0,
):
    """Stage a transfer for explicit Confirm. This never moves data by itself."""
    tool = "start_transfer"
    planned = plan_transfer(
        source_connector_id=source_connector_id,
        source_connector_name=source_connector_name,
        source_table=source_table,
        dest_connector_id=dest_connector_id,
        dest_connector_name=dest_connector_name,
        dest_table=dest_table,
        sync_mode=sync_mode,
        schema_policy=schema_policy,
        validation_mode=validation_mode,
    )
    if not planned.success:
        return _tool_result(tool, success=False, error=planned.error)

    plan = planned.output or {}
    preflight = plan.get("preflight") or {}
    if not _is_execute_cleared(preflight):
        decision = _transfer_decision(preflight) or ("blocked" if not preflight.get("passed") else "review")
        blockers = preflight.get("blockers") or []
        listed = "; ".join(
            f"{b.get('id')}: {b.get('message')}" for b in blockers[:4] if b.get("message")
        )
        if not preflight.get("passed"):
            err = (
                "Preflight blocked this transfer, so I won't start it"
                + (f" — {listed}" if listed else "")
                + (f" (run {preflight.get('run_id')})." if preflight.get("run_id") else ".")
            )
        elif str(preflight.get("run_id") or "").startswith("pf_local_"):
            err = (
                "Local / browser-only preflight cannot unlock Confirm — "
                "re-run Validate against the API until decision is approve."
            )
        else:
            err = (
                f"Preflight is {decision}-grade, not approve — Confirm is blocked "
                "until Studio Execute would unlock "
                + (f"(run {preflight.get('run_id')})." if preflight.get("run_id") else ".")
            )
        return _tool_result(
            tool,
            success=False,
            output={**plan, "action": "plan_transfer"},
            error=err,
        )

    source = plan["source"]
    destination = plan["destination"]
    engine_mappings = plan.get("engine_mappings") or []
    if not engine_mappings:
        return _tool_result(
            tool,
            success=False,
            error="No column mapping was produced, so there is nothing safe to run.",
        )
    payload = {
        "source": {
            "kind": "database",
            # ``format`` is what tells the engine this is a database read; without
            # it the run falls through to the file path and finds no records.
            "format": source.get("type") or "",
            "connector_id": source["connector_id"],
            "schema": source.get("schema") or "",
            "table": source["table"],
        },
        "destination": {
            "kind": "database",
            "format": destination.get("type") or "",
            "connector_id": destination["connector_id"],
            "schema": destination.get("schema") or "",
            "table": destination["table"],
        },
        "mappings": engine_mappings,
        "column_types": plan.get("column_types") or {},
        "sync_mode": plan.get("sync_mode"),
        "schema_policy": plan.get("schema_policy"),
        "validation_mode": plan.get("validation_mode"),
        "limit": max(0, int(limit or 0)),
        # Chat can never turn the gates off.
        "skip_preflight": False,
        "preflight_run_id": preflight.get("run_id"),
    }
    # Belt-and-suspenders: ignore any injected/mutated skip even if schema drifts.
    payload["skip_preflight"] = False  # hard deny - Pilot never bypasses Validate
    preview = {
        "source": f"{source['connector_name']}.{source['table']}",
        "destination": f"{destination['connector_name']}.{destination['table']}",
        "sync_mode": plan.get("sync_mode"),
        "mapped_columns": plan.get("mapped_count"),
        "unmapped_source_columns": plan.get("unmapped_source_columns"),
        "lossy_conversions": len(plan.get("lossy_conversions") or []),
        "destination_table_exists": destination.get("table_exists"),
        "preflight_run_id": preflight.get("run_id"),
        "readiness_score": preflight.get("readiness_score"),
    }

    from .ack_ledger import get_ack_ledger

    ack_id = get_ack_ledger().put(
        kind="start_transfer",
        payload=payload,
        preview=preview,
    )
    overwrite = plan.get("sync_mode") == "full_refresh_overwrite"
    label = (
        f"Transfer {preview['source']} → {preview['destination']} "
        f"({plan.get('sync_mode')})"
    )
    return _tool_result(
        tool,
        success=True,
        output={
            "action": "start_transfer",
            "label": label,
            "risk": "mutate",
            "requires_confirm": True,
            "ack_id": ack_id,
            "preview": preview,
            "destructive": overwrite,
            # The full mapping now lives in the ledger; the chat payload keeps
            # only what the operator needs to read before confirming.
            "plan": {k: v for k, v in plan.items() if k != "engine_mappings"},
        },
    )
