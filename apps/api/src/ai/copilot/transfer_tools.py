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

import json
import logging
import re
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
    # Engine-only tokens Pilot used to drop to append. Keep them so callable
    # refusal and policy gates see SCD2 / mirror instead of a silent rewrite.
    raw = text.replace("-", "_").replace(" ", "_")
    if raw in {"scd2", "scd_2", "slowly_changing_dimension"}:
        return "scd2"
    if raw in {"mirror", "full_refresh_mirror"}:
        return "mirror"
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


def normalize_sync_mode(
    spoken: str,
    *,
    default: str = "full_refresh_append",
    strict: bool = False,
) -> str:
    """Pilot-facing wrapper: phrase → sync-mode token, engine-validated.

    The Pilot keeps emitting its historical spellings because every engine path
    already aliases them onto canonical modes. What changed is that the result
    is now *checked* against the one canonical table in ``services.sync_cursor``
    before it is returned, so a phrase can no longer resolve to a token the
    engine would quietly ignore and degrade to full-read + insert.

    ``strict=True`` (used when the operator *explicitly* named a mode) refuses
    an unresolvable token instead of silently changing load semantics — a typo
    like ``teleport_mode`` must not quietly become ``full_refresh_append``
    (QA T06: that silently duplicates rows on every re-run).
    """
    from services.sync_cursor import CANONICAL_SYNC_MODES
    from services.sync_cursor import normalize_sync_mode as _canonical

    if strict and (spoken or "").strip():
        # Passing default=None distinguishes "unrecognized token" from "the
        # phrase legitimately resolved to the default mode".
        recognized = sync_mode_from_phrase(spoken, default=None)  # type: ignore[arg-type]
        if recognized is None or _canonical(recognized, default="") not in CANONICAL_SYNC_MODES:
            valid = ", ".join(sorted(SYNC_MODES))
            raise ValueError(
                f"Unknown sync_mode {spoken!r} — I will not guess the load semantics. "
                f"Valid modes: {valid}."
            )

    candidate = sync_mode_from_phrase(spoken, default=default)
    canonical = _canonical(candidate, default=default)
    if canonical not in CANONICAL_SYNC_MODES:
        _LOG.warning(
            "Pilot phrase %r produced sync_mode %r, which no engine mode "
            "accepts; falling back to the non-destructive default %r.",
            spoken,
            candidate,
            default,
        )
        return default
    return candidate


_CALLABLE_TABLE_RE = re.compile(r"^\s*(CALL|EXEC(?:UTE)?|SELECT)\b", re.IGNORECASE)


def resolve_callable_plan_source(
    *,
    source_table: str = "",
    source_read_mode: str = "",
    procedure_call: str = "",
    source_query: str = "",
    procedure_params: Any = None,
    dialect: str = "",
) -> dict[str, Any] | None:
    """Detect a CALL/SELECT extract in Pilot args. None means a table plan.

    A bare stream label (``get_orders``) is not enough — that is how a colliding
    table gets introspected. The operator must paste the statement, or put
    CALL/SELECT in the table slot.
    """
    from services.procedure_source import (
        CALLABLE_MODES,
        ProcedureSourceError,
        parse_callable_source,
        stream_name_for_callable,
    )

    mode = (source_read_mode or "").strip().lower()
    text = str(procedure_call or source_query or "").strip()
    table = (source_table or "").strip()
    if not text and _CALLABLE_TABLE_RE.match(table):
        text = table
        if mode not in CALLABLE_MODES:
            mode = "query" if table.lstrip().upper().startswith("SELECT") else "procedure"
    elif mode not in CALLABLE_MODES:
        if text:
            # LLM often sends CALL/SELECT without setting source_read_mode.
            mode = (
                "query"
                if (source_query or text).lstrip().upper().startswith("SELECT")
                else "procedure"
            )
        else:
            return None
    if not text:
        raise ProcedureSourceError(
            "Callable plan needs the CALL/SELECT text, not just a stream name."
        )
    params = procedure_params
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            params = {}
    if not isinstance(params, dict):
        params = {}
    spec = parse_callable_source(text, dialect=dialect, mode=mode, params=params)
    return {
        "mode": spec.mode,
        "text": text,
        "params": dict(spec.params),
        "stream_name": stream_name_for_callable(spec),
        "sql": spec.sql,
    }


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


def _pii_acknowledgement(raw: dict[str, Any] | None) -> dict[str, str] | None:
    """Validate the operator's PII/compliance acknowledgement (or None)."""
    if not raw:
        return None
    if not isinstance(raw, dict):
        raise ValueError("pii_acknowledgement must be an object with approved_by and reason.")
    approved_by = str(raw.get("approved_by") or "").strip()
    reason = str(raw.get("reason") or "").strip()
    if not approved_by or not reason:
        raise ValueError(
            "pii_acknowledgement needs approved_by and reason — an unsigned "
            "acknowledgement cannot clear a PII/compliance review."
        )
    _LOG.info("PII/compliance review acknowledged by %s: %s", approved_by, reason)
    return {"approved_by": approved_by, "reason": reason}


def _is_execute_cleared(preflight: dict[str, Any]) -> bool:
    """Same bar as Studio Execute — passed + approve; never local / review-grade."""
    run_id = str(preflight.get("run_id") or "")
    if run_id.startswith("pf_local_"):
        return False
    # Defense-in-depth (QA T04/T12): a severity=block blocker must override any
    # stale approve verdict instead of letting safe_to_start stay true beside it.
    # Root-cause blockers are block-severity by construction even when the
    # severity field is only in root_causes[].
    blocking_root_ids = {
        str(r.get("root_id") or "")
        for r in preflight.get("root_causes") or []
        if isinstance(r, dict) and str(r.get("severity") or "").lower() == "block"
    }
    for blocker in preflight.get("blockers") or []:
        if not isinstance(blocker, dict):
            continue
        sev = str(
            blocker.get("severity")
            or (blocker.get("details") or {}).get("severity")
            or ""
        ).lower()
        if sev == "block":
            return False
        if blocking_root_ids and str(blocker.get("id") or "") in blocking_root_ids:
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
    measured = dst_info.get("table_exists")
    if isinstance(measured, bool):
        # The catalog was asked directly. Prefer that over the error-text guess
        # below, which no engine promises to phrase any particular way.
        return measured
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


def _declare_zone_on_schema(
    src_rows: list[dict],
    zone: str,
) -> tuple[list[dict], set[str]]:
    """Apply an operator-declared zone to the zoneless temporal source columns.

    A declared zone is a fact about the source schema, so it has to be true
    *before* mapping runs: the mapper, its proof, and every gate read the source
    type, and a declaration applied afterwards would change the value the writer
    sends while leaving the run blocked for the zoneless problem the operator
    just answered.

    Only zoneless columns are touched. A column that already carries an offset
    has an instant the source was explicit about, and overriding it would move a
    timestamp on the operator's behalf.
    """
    from services.timezone_policy import effective_source_type
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX
    from services.type_system import datetime_timezone_polarity

    declared: set[str] = set()
    out: list[dict] = []
    transform = f"{ASSUME_TIMEZONE_PREFIX}{zone}"
    for row in src_rows:
        entry = dict(row)
        name = str(entry.get("name") or "")
        src_type = str(entry.get("inferred_type") or "")
        polarity = datetime_timezone_polarity(src_type) if src_type else None
        if polarity == "ntz":
            entry["inferred_type"] = effective_source_type(src_type, transform)
            declared.add(name)
        elif polarity in {"tz", "ltz"} and _samples_are_naive_wall_clock(
            entry.get("samples") or []
        ):
            # MySQL/Maria TIMESTAMP is an instant in the catalog and a naive
            # wall clock on the wire (session time_zone strips the offset).
            # The declared zone is that wall clock. Leaving the column out
            # kept TIMESTAMPTZ blocked on "refuses naive wall-clock".
            declared.add(name)
        out.append(entry)
    return out, declared


def _samples_are_naive_wall_clock(samples: Any) -> bool:
    """True when at least one sample is a datetime with no offset."""
    from connectors.sql_temporal import parse_sql_datetime
    from services.type_system import temporal_value_has_timezone

    saw = False
    for sample in samples or []:
        if sample is None or not str(sample).strip():
            continue
        if temporal_value_has_timezone(sample):
            continue
        if parse_sql_datetime(sample) is not None:
            saw = True
    return saw


def _stamp_zone_transform(
    mappings: list[dict],
    zone: str,
    columns: set[str],
) -> list[dict]:
    """Carry the declaration to the writer as the transform that applies it.

    The type says what the column means; the transform is what attaches the zone
    to each value. Both are required — a declared type with no transform would
    write the naive wall clock under an instant contract.
    """
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX

    out: list[dict] = []
    for m in mappings:
        entry = dict(m)
        existing = str(entry.get("transform") or "").strip().lower()
        # A plain temporal transform is the mapper parsing the value, not a
        # decision about its zone — the declaration refines it. Anything else is
        # an operator choice and is left alone.
        already_typed = existing not in {"", "none", "identity", "datetime", "date", "timestamp"}
        if str(entry.get("source") or "") in columns and not already_typed:
            entry["transform"] = f"{ASSUME_TIMEZONE_PREFIX}{zone}"
        out.append(entry)
    return out


# risk_acceptance.on_cast_failure → signed contract quarantine_policy.
_ON_CAST_FAILURE_POLICIES = {
    "quarantine": "holdout_rejected_rows",
    "null": "coerce_null",
}


def _sign_required_risk_contracts(
    mappings: list[dict[str, Any]],
    acceptance: dict[str, Any],
    *,
    table: str,
) -> list[dict[str, Any]]:
    """Sign a continue-policy contract only on mappings a gate already requires.

    Omitted acceptance signs nothing. A partial acceptance is refused: the
    operator named the action and left out who approved it or why. Safe
    mappings are never given a contract they do not need.
    """
    if not isinstance(acceptance, dict) or not acceptance:
        return list(mappings or [])
    from preflight.risk_contract import (
        mapping_is_structural_review,
        mapping_requires_risk_contract,
    )
    from services.decision_kernel import is_lossy_coercion
    from services.migration_risk_contract import (
        CONTINUE_POLICIES,
        create_migration_risk_contract,
    )

    approved_by = str(acceptance.get("approved_by") or "").strip()
    reason = str(acceptance.get("reason") or "").strip()
    policy = str(acceptance.get("execution_policy") or "").strip().upper()
    if policy == "CAST_FAIL_QUARANTINE":
        policy = "CAST_AND_CONTINUE"
    if not approved_by or not reason or not policy:
        raise ValueError(
            "risk_acceptance needs approved_by, reason, and execution_policy. "
            "Nothing was signed."
        )
    if policy not in CONTINUE_POLICIES:
        raise ValueError(
            f"execution_policy {policy} does not clear a gate. "
            "Use QUARANTINE_ROW, CAST_AND_CONTINUE, TRANSFORM_AND_CONTINUE, "
            "SKIP_ROW, or STOP_COLUMN."
        )
    # QA MX3-14: without this the NULL variant of CAST_AND_CONTINUE was
    # unreachable and the policy behaved exactly like QUARANTINE_ROW.
    on_cast_failure = str(acceptance.get("on_cast_failure") or "").strip().lower()
    if on_cast_failure and on_cast_failure not in _ON_CAST_FAILURE_POLICIES:
        raise ValueError(
            f"on_cast_failure {on_cast_failure!r} is not supported. Use "
            "'quarantine' (hold the row out) or 'null' (write NULL). Nothing was signed."
        )
    if on_cast_failure and policy not in {"CAST_AND_CONTINUE", "TRANSFORM_AND_CONTINUE"}:
        raise ValueError(
            f"on_cast_failure applies only to CAST_AND_CONTINUE / "
            f"TRANSFORM_AND_CONTINUE, not {policy}. Nothing was signed."
        )
    quarantine_policy = _ON_CAST_FAILURE_POLICIES[on_cast_failure or "quarantine"]
    named = {
        str(c).strip()
        for c in (acceptance.get("columns") or [])
        if str(c or "").strip()
    }
    out: list[dict[str, Any]] = []
    signed = 0
    for raw in mappings or []:
        row = dict(raw)
        source = str(row.get("source") or "")
        target = str(row.get("target") or "")
        if named and source not in named and target not in named:
            out.append(row)
            continue
        src_t = str(row.get("source_type") or "")
        tgt_t = str(row.get("target_type") or "")
        needs = mapping_requires_risk_contract(row) or mapping_is_structural_review(row)
        if src_t and tgt_t:
            needs = needs or bool(is_lossy_coercion(src_t, tgt_t))
        if not needs or row.get("risk_contract") or row.get("riskContract"):
            out.append(row)
            continue
        contract = create_migration_risk_contract(
            column=source or target,
            source_type=src_t,
            destination_type=tgt_t,
            approved_by=approved_by,
            reason=reason,
            execution_policy=policy,
            target=target or source,
            table=table,
            fidelity=str(row.get("fidelity") or ""),
            transform=row.get("transform"),
            quarantine_policy=quarantine_policy,
            expected_nulls=quarantine_policy == "coerce_null",
        )
        row["risk_contract"] = contract.to_dict()
        signed += 1
        out.append(row)
    if named and signed == 0:
        raise ValueError(
            "risk_acceptance named columns that do not require a Migration Risk Contract. "
            "Nothing was signed."
        )
    return out


def _plan_source_types_authoritative(
    src_conn: dict, src_info: dict, callable_plan: dict | None
) -> bool:
    """Whether Map may treat the peeked source types as declared DDL.

    A procedure extract and an object store do not declare precision. Calling
    them authoritative made the plan bind TEXT while the integrity check
    expected the profiled DECIMAL. Warehouse catalogs stay authoritative.
    """
    if callable_plan:
        return False
    from services.data_profiler import source_types_are_authoritative

    kind = str(src_conn.get("kind") or "database")
    fmt = str(
        src_info.get("db_type")
        or src_conn.get("type")
        or src_conn.get("db_type")
        or ""
    )
    return source_types_are_authoritative(kind, fmt)


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
    source_timezone: str = "",
    source_read_mode: str = "",
    procedure_call: str = "",
    source_query: str = "",
    procedure_params: Any = None,
    contract_id: str = "",
    require_signed_contract: Any = None,
    source_filter: dict[str, Any] | None = None,
    upsert_key: str = "",
    primary_key: str = "",
    dedupe_key: str = "",
    cursor_column: str = "",
    cursor_semantics: str = "",
    rule_questions: list[str] | None = None,
    applied_rules: list[str] | None = None,
    cadence: str = "",
    all_tables: bool = False,
    risk_acceptance: dict[str, Any] | None = None,
    pii_acknowledgement: dict[str, Any] | None = None,
):
    """Plan a real transfer: live schemas, real mapping, real preflight gates.

    ``source_timezone`` answers the one question the tool cannot answer itself:
    a zoneless source column has no instant, so landing it on a carrier that
    stores instants has to pick a zone. Guessing UTC is how a business day moves
    for anyone whose data was not UTC, so the plan is blocked until the operator
    says which zone the source meant.
    """
    tool = "plan_transfer"
    upsert_key = _column_arg(upsert_key or primary_key)
    cursor_column = _column_arg(cursor_column)
    cursor_semantics = str(cursor_semantics or "").strip()
    src_table = (source_table or "").strip()
    unapplied = [str(q) for q in (rule_questions or []) if str(q or "").strip()]
    if unapplied:
        return _tool_result(tool, success=False, error=_unapplied_rules_error(unapplied))
    if all_tables and not src_table:
        return _tool_result(
            tool,
            success=False,
            error=(
                "I move one table per run, so “all tables” needs a list I can prove — "
                "ask me to “list tables on <source>” and name the ones to move, or use "
                "Transfer Studio for a multi-table migration."
            ),
        )
    from services.procedure_source import ProcedureSourceError

    try:
        callable_plan = resolve_callable_plan_source(
            source_table=src_table,
            source_read_mode=source_read_mode,
            procedure_call=procedure_call,
            source_query=source_query,
            procedure_params=procedure_params,
        )
    except ProcedureSourceError as exc:
        return _tool_result(tool, success=False, error=str(exc))
    if not src_table and not callable_plan:
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
    dst_table = (dest_table or (callable_plan["stream_name"] if callable_plan else src_table)).strip()

    # An omitted mode plus a key means "dedupe this". A mode the operator
    # actually named must stay that mode — a primary key on overwrite is an
    # identity, not permission to switch the run to incremental upsert.
    # T06: an *explicitly named* mode that resolves to nothing is refused, not
    # coerced — silently staging full_refresh_append duplicates rows on re-run.
    requested_sync_mode = bool((sync_mode or "").strip())
    try:
        mode = normalize_sync_mode(sync_mode, strict=requested_sync_mode)
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))
    if callable_plan:
        from services.procedure_source import assert_callable_sync_allowed

        try:
            assert_callable_sync_allowed(
                mode,
                {
                    "source_read_mode": callable_plan["mode"],
                    "procedure_call": callable_plan["text"]
                    if callable_plan["mode"] == "procedure"
                    else "",
                    "source_query": callable_plan["text"]
                    if callable_plan["mode"] == "query"
                    else "",
                },
            )
        except ProcedureSourceError as exc:
            return _tool_result(tool, success=False, error=str(exc))
        src_table = callable_plan["stream_name"]

    try:
        src_conn, err = _safe_connector(source_connector_id, source_connector_name, tool)
        if err:
            return err
        dst_conn, err = _safe_connector(dest_connector_id, dest_connector_name, tool)
        if err:
            return err
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)

    if (
        not callable_plan
        and str(src_conn.get("id")) == str(dst_conn.get("id"))
        and src_table == dst_table
    ):
        return _tool_result(
            tool,
            success=False,
            error="Source and destination are the same table — nothing to move.",
        )

    try:
        dst_info = _introspect(dst_conn, dst_table, purpose="destination")
    except Exception as exc:
        _LOG.warning("plan_transfer dest introspect failed: %s", exc, exc_info=True)
        dst_info = {"ok": False, "error": str(exc), "columns": [], "db_type": "", "cfg": {}}

    dest_exists = _dest_table_exists_tri_state(dst_info)

    if callable_plan:
        try:
            src_info = _peek_callable_source(src_conn, callable_plan)
        except Exception as exc:
            _LOG.warning("plan_transfer callable peek failed: %s", exc, exc_info=True)
            return _tool_result(
                tool,
                success=False,
                error=f"Could not execute the procedure extract: {exc}",
            )
        sample_rows = list(src_info.get("sample_rows") or [])
    else:
        try:
            src_info = _introspect(src_conn, src_table, purpose="source", execute_shape=True)
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
        sample_rows = _sample_rows(src_conn, src_table)

    src_names = [str(c.get("name")) for c in src_info["columns"] if c.get("name")]

    row_rules, rules_error = _ground_data_rules(
        source_filter=source_filter,
        upsert_key=upsert_key,
        dedupe_key=dedupe_key,
        source_columns=src_names,
        source_label=f"{src_conn.get('name')}.{src_table}",
        mode=mode,
        honor_requested_mode=requested_sync_mode,
    )
    if rules_error:
        return _tool_result(tool, success=False, error=rules_error)
    mode = row_rules["sync_mode"]
    # A filter narrows what the run must move, so every gate that reasons about
    # "the rows about to be written" has to see the filtered sample.
    if row_rules["source_filter"]:
        from services.row_filter import apply_row_filter

        sample_rows = apply_row_filter(sample_rows, row_rules["source_filter"])

    samples = _column_samples(sample_rows, src_names)
    src_rows = _schema_rows(src_info["columns"], samples)
    zone_columns: set[str] = set()
    if source_timezone:
        src_rows, zone_columns = _declare_zone_on_schema(src_rows, source_timezone)
        # The declaration is a fact about the source, so it travels with the
        # schema every gate reads — not just with the mapping.
        src_info["schema"] = {
            **(src_info.get("schema") or {}),
            **{r["name"]: r["inferred_type"] for r in src_rows if r["name"] in zone_columns},
        }
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
        # Warehouse DDL is fact. Object stores, files, and procedure extracts
        # are a sample: marking them authoritative made the plan say TEXT
        # while the integrity check bound the profiled DECIMAL.
        source_types_authoritative=_plan_source_types_authoritative(
            src_conn, src_info, callable_plan
        ),
        use_llm=False,
    )
    mappings = list(mapping.get("mappings") or [])
    if zone_columns:
        mappings = _stamp_zone_transform(mappings, source_timezone, zone_columns)

    contracts, identity_error = _identity_stream_contract(
        mode=mode,
        source_table=src_table,
        source_columns=src_names,
        mappings=mappings,
        operator_key=str(row_rules.get("upsert_key") or ""),
        catalog_key=_source_primary_key(src_info),
        cursor_column=cursor_column,
        cursor_semantics=cursor_semantics,
    )
    if identity_error:
        return _tool_result(tool, success=False, error=identity_error)
    row_rules["stream_contracts"] = contracts
    if contracts and not row_rules.get("upsert_key") and contracts[0].get("primary_key"):
        row_rules["upsert_key"] = _primary_key_csv(contracts[0].get("primary_key"))
    if contracts and contracts[0].get("cursor_field"):
        row_rules["cursor_column"] = str(contracts[0]["cursor_field"])
    if contracts and contracts[0].get("cursor_semantics"):
        row_rules["cursor_semantics"] = str(contracts[0]["cursor_semantics"])
    if contracts and contracts[0].get("cursor_inferred"):
        row_rules["cursor_inferred"] = True

    try:
        pii_ack = _pii_acknowledgement(pii_acknowledgement)
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))

    if risk_acceptance:
        try:
            mappings = _sign_required_risk_contracts(
                mappings,
                risk_acceptance,
                table=dst_table,
            )
        except ValueError as exc:
            return _tool_result(tool, success=False, error=str(exc))

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
        source_config=_stamp_callable_source_config(
            _endpoint_dict(src_info.get("endpoint")),
            callable_plan,
        ),
        dest_db_type=str(dst_info.get("db_type") or ""),
        dest_exists=dest_exists,
        source_primary_key=(
            _primary_key_csv(contracts[0].get("primary_key"))
            if contracts
            else _source_primary_key(src_info)
        ),
        write_via_staging=bool(write_via_staging),
        source_read_mode=str((callable_plan or {}).get("mode") or ""),
        source_filter=row_rules["source_filter"] or None,
        stream_contracts=row_rules["stream_contracts"] or None,
        pii_ack=pii_ack,
    )

    contract_refusal = _plan_contract_refusal(contract_id, require_signed_contract)
    if contract_refusal:
        preflight = _block_plan_on_contract(preflight, contract_refusal)

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
                "source_read_mode": str((callable_plan or {}).get("mode") or "table"),
                "procedure_call": (
                    callable_plan["text"]
                    if callable_plan and callable_plan["mode"] == "procedure"
                    else ""
                ),
                "source_query": (
                    callable_plan["text"]
                    if callable_plan and callable_plan["mode"] == "query"
                    else ""
                ),
                "procedure_params": (callable_plan or {}).get("params") or {},
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
            "source_filter": row_rules["source_filter"],
            "stream_contracts": row_rules["stream_contracts"],
            "data_rules": _data_rules_preview(row_rules, applied_rules, cadence),
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
            **_preview_bound_contract(contract_id, require_signed_contract),
            **({"contract_blocker": contract_refusal} if contract_refusal else {}),
        },
    )


def _unapplied_rules_error(questions: list[str]) -> str:
    """Refuse the run and hand back the exact missing detail.

    Staging a transfer that ignores a stated rule would move rows the operator
    excluded, and it would look like success. So the run is refused, not
    degraded.
    """
    head = (
        "I can set up this transfer, but I will not run it while part of what you "
        "asked for would be dropped:"
    )
    return head + "\n" + "\n".join(f"• {q}" for q in questions[:4])


def _column_arg(value: Any) -> str:
    """A tool argument naming columns: one string, or a list of names.

    MCP clients send either. A list must not be stringified into ``"['id']"``.
    """
    if isinstance(value, (list, tuple)):
        return ",".join(str(part).strip() for part in value if str(part).strip())
    return str(value or "").strip()


def _data_rules_preview(
    row_rules: dict[str, Any],
    applied_rules: list[str] | None,
    cadence: str,
) -> dict[str, Any]:
    """The row rules the confirm preview and a schedule both read."""
    preview: dict[str, Any] = {
        "applied": [str(r) for r in (applied_rules or [])],
        "upsert_key": row_rules.get("upsert_key") or "",
        "row_filter": row_rules.get("filter_description") or "",
        # Chat stages one run; a cadence is a Schedules object, so it is
        # echoed back as an unmet request rather than silently honoured.
        "cadence_not_scheduled": str(cadence or ""),
    }
    if row_rules.get("cursor_column"):
        preview["cursor_column"] = row_rules["cursor_column"]
    if row_rules.get("cursor_semantics"):
        preview["cursor_semantics"] = row_rules["cursor_semantics"]
    return preview


def _ground_data_rules(
    *,
    source_filter: dict[str, Any] | None,
    upsert_key: str,
    dedupe_key: str,
    source_columns: list[str],
    source_label: str,
    mode: str,
    honor_requested_mode: bool = False,
) -> tuple[dict[str, Any], str]:
    """Bind spoken row rules to real source columns, or refuse.

    Every column named in a filter or a key must exist in the introspected
    source. An unknown column cannot be silently ignored: the run would move
    more rows than the operator asked for, and reconcile green while doing it.
    """
    from services.sync_cursor import normalize_sync_mode as engine_sync_mode
    from .transfer_rules import filter_columns

    # Pilot says cdc_incremental / incremental_upsert. The gates and the CDC
    # runner speak cdc / incremental_deduped. Leaving the alias here downgraded
    # CDC to upsert and then blocked it for a table cursor the log does not use.
    if (mode or "").strip():
        mode = engine_sync_mode(mode)
    spec = dict(source_filter or {})
    known = {c.lower(): c for c in source_columns}
    out: dict[str, Any] = {
        "source_filter": {},
        "stream_contracts": [],
        "upsert_key": "",
        "filter_description": "",
        "sync_mode": mode,
    }

    def resolve(column: str, role: str) -> tuple[str, str]:
        actual = known.get(column.strip().lower(), "")
        if actual:
            return actual, ""
        listed = ", ".join(source_columns[:12]) or "none readable"
        return "", (
            f"{source_label} has no column `{column}`, so I cannot apply the {role} "
            f"you asked for — running without it would move rows you excluded. "
            f"Columns I can see: {listed}."
        )

    if spec:
        for column in filter_columns(spec):
            _, err = resolve(column, "row filter")
            if err:
                return out, err
        # Bind to the source's own spelling: the filter runs against read rows,
        # whose keys carry the DDL's case.
        spec = _rebind_filter_columns(spec, known)
        out["source_filter"] = spec
        out["filter_description"] = _describe_row_filter(spec)

    key = (upsert_key or dedupe_key or "").strip()
    if key:
        parts = [part.strip() for part in key.replace(";", ",").split(",") if part.strip()]
        bound: list[str] = []
        for part in parts:
            actual, err = resolve(part, "upsert key")
            if err:
                return out, err
            bound.append(actual)
        out["upsert_key"] = ",".join(bound)
        # A key with no spoken mode means upsert: the operator named an
        # identity and left the mode at the non-destructive default. CDC,
        # SCD2, and mirror already require a key — do not downgrade them.
        # incremental_append is cursor-bounded insert. A key the operator
        # attached to a mode they named (overwrite, append) is recorded and
        # the mode stays. Rewriting that to upsert also inferred updated_at.
        from services.preflight_cursor_gate import MODES_REQUIRING_PRIMARY_KEY

        if (
            not honor_requested_mode
            and mode not in MODES_REQUIRING_PRIMARY_KEY
            and mode != "incremental_append"
        ):
            out["sync_mode"] = normalize_sync_mode("upsert")
    return out, ""


def _primary_key_csv(raw: Any) -> str:
    """Join a contract key without inventing ``i,d`` from the string ``id``.

    A cursor-only incremental_append contract has no ``primary_key`` key.
    Indexing it raised KeyError, and the error humanizer then told the
    operator the run needed an identity column.
    """
    if raw is None:
        return ""
    if isinstance(raw, (list, tuple)):
        return ",".join(str(part).strip() for part in raw if str(part).strip())
    return str(raw).strip()


def _identity_stream_contract(
    *,
    mode: str,
    source_table: str,
    source_columns: list[str],
    mappings: list[dict[str, Any]],
    operator_key: str,
    catalog_key: str,
    cursor_column: str = "",
    cursor_semantics: str = "",
) -> tuple[list[dict[str, Any]], str]:
    """The stream contract Execute and preflight both read.

    The operator's key wins. Otherwise a mode that requires identity uses the
    source catalog key when every column is mapped. No column named ``id`` is
    invented. CDC's cursor is the log position (``cdc_position``), not a
    guessed ``updated_at``. An incremental cursor is the column the operator
    named. When they name none and the source has exactly one conventional
    modification-timestamp column, that column is selected and declared
    ``modification_timestamp`` — the plan says so. Two candidates are not a
    guess, a missing column is not invented, and a named cursor with no
    semantics stays undeclared so the gate can still refuse it.
    """
    from services.preflight_cursor_gate import MODES_REQUIRING_PRIMARY_KEY
    from services.primary_key import mapped_catalog_upsert_key
    from services.sync_cursor import normalize_sync_mode as engine_sync_mode

    if (mode or "").strip():
        mode = engine_sync_mode(mode)
    mapping_rows: list[dict[str, Any]] = []
    for item in mappings or []:
        row = item
        if not isinstance(row, dict):
            dump = getattr(row, "model_dump", None)
            row = dump() if callable(dump) else {}
        if not isinstance(row, dict):
            continue
        src = str(row.get("source") or row.get("source_column") or "").strip()
        tgt = str(row.get("target") or row.get("target_column") or src).strip()
        if src and tgt:
            mapping_rows.append({"source": src, "target": tgt})

    def _fully_mapped(cols: list[str]) -> bool:
        if not cols:
            return False
        sources, _targets = mapped_catalog_upsert_key(cols, mapping_rows)
        return [s.lower() for s in sources] == [c.lower() for c in cols]

    chosen: list[str] = []
    if operator_key:
        chosen = [part for part in operator_key.split(",") if part]
        if not _fully_mapped(chosen):
            return [], (
                f"Upsert key `{operator_key}` is not in the column mapping, "
                "so the write cannot merge on it. Map that column, or name a "
                "key that is mapped."
            )
    elif mode in MODES_REQUIRING_PRIMARY_KEY and catalog_key:
        known = {col.lower(): col for col in source_columns}
        parts = [part.strip() for part in catalog_key.split(",") if part.strip()]
        if parts and all(part.lower() in known for part in parts):
            bound = [known[part.lower()] for part in parts]
            if _fully_mapped(bound):
                chosen = bound
    cursor = ""
    semantics = ""
    inferred = False
    if mode == "cdc":
        # The log is the cursor. A table column here would make the snapshot
        # reader filter on it and skip rows the log had already captured.
        semantics = "cdc_position"
    else:
        raw_cursor = (cursor_column or "").strip()
        if raw_cursor:
            known = {col.lower(): col for col in source_columns}
            actual = known.get(raw_cursor.lower(), "")
            if not actual:
                listed = ", ".join(source_columns[:12]) or "none readable"
                return [], (
                    f"Source has no column `{raw_cursor}`, so I cannot advance "
                    f"on it. Columns I can see: {listed}."
                )
            cursor = actual
        raw_sem = (cursor_semantics or "").strip().lower()
        if raw_sem:
            from services.cursor_semantics import CURSOR_SEMANTICS

            if raw_sem not in CURSOR_SEMANTICS:
                return [], (
                    f"Unknown cursor semantics '{raw_sem}' — declare one of: "
                    + ", ".join(sorted(CURSOR_SEMANTICS))
                )
            semantics = raw_sem
        elif not cursor:
            from services.cursor_semantics import (
                MODIFICATION_TIMESTAMP,
                sole_modification_timestamp_column,
            )
            from services.preflight_cursor_gate import MODES_REQUIRING_CURSOR

            picked = ""
            if mode in MODES_REQUIRING_CURSOR:
                picked = sole_modification_timestamp_column(source_columns)
            if picked:
                cursor = picked
                semantics = MODIFICATION_TIMESTAMP
                inferred = True
    if not chosen and not cursor:
        return [], ""
    contract: dict[str, Any] = {
        "name": source_table or "stream",
        "selected": True,
    }
    if chosen:
        contract["primary_key"] = chosen
    if cursor:
        contract["cursor_field"] = cursor
    if semantics:
        contract["cursor_semantics"] = semantics
    if inferred:
        contract["cursor_inferred"] = True
    if mode:
        # Execute prefers the contract mode. The canonical token is what the
        # CDC branch and the progress check compare against.
        contract["sync_mode"] = mode
    if mode == "cdc":
        # Debezium default: snapshot when no resume exists, then tail the log.
        contract["snapshot_mode"] = "initial"
    return [contract], ""


def _rebind_filter_columns(
    spec: dict[str, Any],
    known: dict[str, str],
) -> dict[str, Any]:
    """Rewrite filter column names to the source's own spelling."""
    node = dict(spec)
    for joiner in ("and", "or"):
        if joiner in node:
            node[joiner] = [
                _rebind_filter_columns(child, known)
                for child in (node.get(joiner) or [])
                if isinstance(child, dict)
            ]
            return node
    for field in ("column", "field"):
        raw = str(node.get(field) or "").strip()
        if raw:
            node[field] = known.get(raw.lower(), raw)
    return node


def _describe_row_filter(spec: dict[str, Any] | None) -> str:
    """One-line echo of the filter for the confirm preview."""
    node = spec or {}
    for joiner in ("and", "or"):
        if joiner in node:
            parts = [_describe_row_filter(c) for c in (node.get(joiner) or []) if c]
            return f" {joiner} ".join(p for p in parts if p)
    column = node.get("column") or node.get("field") or ""
    if not column:
        return ""
    op = str(node.get("operator") or node.get("op") or "eq")
    if op in {"is_null", "is_not_null"}:
        return f"{column} {op.replace('_', ' ')}"
    value = node.get("value")
    shown = ", ".join(str(v) for v in value) if isinstance(value, list) else str(value)
    return f"{column} {op} {shown}"


def _source_primary_key(src_info: dict[str, Any]) -> str:
    """The source's declared identity, as a comma-joined composite key.

    Studio collects this on the Map screen; a chat request has no such screen,
    so the catalog is the only place the Pilot can learn it — and it is the
    better source anyway, since the source already declared it.
    """
    raw = src_info.get("raw") if isinstance(src_info.get("raw"), dict) else {}
    cols = [str(c).strip() for c in (raw.get("primary_key_columns") or []) if str(c).strip()]
    return ",".join(cols)


def _preflight_issue_lines(details: dict[str, Any]) -> list[str]:
    """The sentences an operator can act on, not the gate's count summary."""
    lines: list[str] = []
    for issue in (details.get("issues") or [])[:1]:
        text = str(issue).strip()
        if text:
            lines.append(text)
    for row in (details.get("issues_detail") or [])[:2]:
        if not isinstance(row, dict):
            continue
        for failure in (row.get("sample_failures") or [])[:1]:
            if not isinstance(failure, dict):
                continue
            reason = str(failure.get("reason") or "").strip()
            if reason and reason not in lines:
                lines.append(reason)
    return lines[:3]


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
    source_primary_key: str = "",
    write_via_staging: bool = False,
    source_read_mode: str = "",
    source_filter: dict[str, Any] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    source_kind: str = "database",
    known_row_count: int | None = None,
    pii_ack: dict[str, str] | None = None,
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
    if known_row_count is not None:
        row_count = max(0, int(known_row_count))
    elif (source_read_mode or "").strip().lower() in {"procedure", "query"}:
        # COUNT(*) against a procedure stream name would hit a colliding table.
        row_count = len(sample_rows)
    else:
        row_count = _exact_row_count(src_conn, src_table)
        if row_count is None:
            row_count = len(sample_rows)

    try:
        policy_gates = run_transfer_policy_gates(
            sync_mode=mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            stream_contracts=list(stream_contracts or []),
            backfill_new_fields=False,
            source_columns=columns,
            catalog_primary_key_columns=[
                part.strip()
                for part in str(source_primary_key or "").split(",")
                if part.strip()
            ] or None,
            mappings=mappings,
            source_table=src_table,
            dest_type=dest_db_type,
            source_type=src_db_type,
            source_kind=source_kind or "database",
            # G12 must match Studio / Execute — Pilot cannot soft-skip staging policy.
            write_via_staging=bool(write_via_staging),
            source_read_mode=source_read_mode,
            source_endpoint=source_config,
            destination_endpoint=dst_conn,
        )
        # These must mirror ``UniversalTransferEngine`` exactly. The source
        # config and table are what enable the live coercion probe; without
        # them the pilot would promise "safe to start" and the engine would
        # then block the run, which is worse than refusing up front.
        # Never invent can_create=True when privilege probe omitted the flag.
        can_create = dest_probe.get("can_create_table")
        from services.db_type_utils import dest_schema_is_recreated_on_overwrite
        from services.sync_cursor import is_overwrite_sync

        dest_recreated = is_overwrite_sync(mode) and dest_schema_is_recreated_on_overwrite(
            dest_db_type
        )
        dest_types = dest_probe.get("column_types") or {}
        destination_live_column_types = (
            dict(dest_types) if dest_recreated and dest_types else None
        )
        # The inspect above is what Gate-2 uses to say the table exists. The
        # collision probe has to use that same connection. Dropping it made
        # every append into a readable table warn that the destination was
        # unavailable, then Execute discovered the duplicate.
        measured_exists = dest_probe.get("table_exists")
        table_exists = measured_exists if isinstance(measured_exists, bool) else dest_exists

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
            destination_column_types=dest_types,
            destination_live_column_types=destination_live_column_types,
            destination_column_nullability=dest_probe.get("column_nullability") or {},
            destination_column_defaults=dest_probe.get("column_defaults") or {},
            destination_identity_columns=dest_probe.get("identity_columns") or [],
            destination_generated_columns=dest_probe.get("generated_columns") or [],
            destination_table_exists=table_exists,
            destination_can_create=can_create if isinstance(can_create, bool) else None,
            # Connectivity is not INSERT. Dropping the probe here made Gate-2
            # say "write access" for a role that can only SELECT, and Execute
            # then failed with the denial the probe had already measured.
            destination_can_write=(
                dest_probe.get("can_write")
                if isinstance(dest_probe.get("can_write"), bool)
                else None
            ),
            privilege_probe=(
                dest_probe.get("privilege_probe")
                if isinstance(dest_probe.get("privilege_probe"), dict)
                else None
            ),
            redshift_staging_probe=(
                dest_probe.get("redshift_staging_probe")
                if isinstance(dest_probe.get("redshift_staging_probe"), dict)
                else None
            ),
            destination_db_type=dest_db_type,
            destination_table=dst_table,
            destination_pk_columns=(
                dest_probe.get("primary_key_columns") or dest_probe.get("pk_columns")
            ),
            destination_unique_keys=list(dest_probe.get("unique_keys") or []),
            destination_foreign_keys=list(dest_probe.get("foreign_keys") or []),
            destination_config=dest_probe.get("_probe_cfg") or None,
            source_kind=source_kind or "database",
            source_format=src_db_type,
            source_table=src_table,
            source_connector_id=str(src_conn.get("id") or ""),
            source_config=source_config,
            confidence_threshold=confidence_threshold_for_mode(validation_mode),
            # Key-addressed destinations (Mongo, Redis, vector stores) refuse a
            # write they cannot address. Studio carries the key on a stream
            # contract; the Pilot has no Map screen to fill one in, so without
            # this the gate asked the operator to set a key the source catalog
            # had already declared.
            contract_primary_key=source_primary_key or None,
            source_filter=source_filter or None,
            stream_contracts=list(stream_contracts or []),
            compliance_acknowledged=bool(pii_ack),
            acknowledgment_actor=(pii_ack or {}).get("approved_by", ""),
            acknowledgment_reason=(pii_ack or {}).get("reason", ""),
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

    # Confirm is gated on the proof bundle's transfer_decision, so the slim
    # projection has to carry it. Dropping it did not make Confirm stricter, it
    # made the decision unreadable: _transfer_decision saw nothing and every run
    # fell back to "review", so no Pilot transfer could ever be confirmed however
    # clean it was. Only the decision travels; the rest of the bundle stays out
    # of the chat payload.
    proof_bundle = result.get("proof_bundle")
    decision = (
        (proof_bundle or {}).get("transfer_decision")
        if isinstance(proof_bundle, dict)
        else None
    )
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
            {
                "id": b.get("id") or b.get("gate_id"),
                "message": b.get("message"),
                # Only the fix travels from the details blob: without it the chat
                # refusal names a problem and no way out of it.
                "details": {
                    "recommended_fix": ((b.get("details") or {}).get("recommended_fix") or ""),
                    # The gate summary is "1 type coercion issue(s)". The issue
                    # sentence names the column; the sample reason names the value.
                    "issues": _preflight_issue_lines(b.get("details") or {}),
                },
            }
            for b in (result.get("blockers") or [])
        ][:10],
        "warnings": (result.get("warnings") or [])[:10],
        "proof_bundle": {"transfer_decision": decision} if isinstance(decision, dict) else {},
    }


def _stamp_callable_source_config(
    source_config: dict[str, Any],
    callable_plan: dict[str, Any] | None,
) -> dict[str, Any]:
    """Put CALL/SELECT fields on the preflight/execute source cfg."""
    cfg = dict(source_config or {})
    # ``endpoint_to_dict`` carries the driver as ``format``. The SQL URL
    # builder and the procedure dialect both read ``type``.
    if not str(cfg.get("type") or "").strip():
        driver = str(cfg.get("format") or cfg.get("db_type") or "").strip()
        if driver:
            cfg["type"] = driver
    if not callable_plan:
        return cfg
    extra = dict(cfg.get("extra") or {}) if isinstance(cfg.get("extra"), dict) else {}
    extra["source_read_mode"] = callable_plan["mode"]
    if callable_plan["mode"] == "procedure":
        extra["procedure_call"] = callable_plan["text"]
        cfg["procedure_call"] = callable_plan["text"]
    else:
        extra["source_query"] = callable_plan["text"]
        cfg["source_query"] = callable_plan["text"]
    extra["procedure_params"] = callable_plan.get("params") or {}
    cfg["source_read_mode"] = callable_plan["mode"]
    cfg["procedure_params"] = extra["procedure_params"]
    cfg["extra"] = extra
    return cfg


def _peek_callable_source(conn: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    """Execute CALL/SELECT once for schema — never introspect a colliding table."""
    from services.procedure_source import close_callable_spool, read_callable_batch
    from src.transfer.models import endpoint_to_dict

    from .schema_tools import _endpoint_from_connector

    endpoint = _endpoint_from_connector(conn, table=str(plan.get("stream_name") or ""))
    cfg = _stamp_callable_source_config(endpoint_to_dict(endpoint), plan)
    try:
        batch = read_callable_batch(cfg, offset=0, limit=50, peek=True)
    finally:
        close_callable_spool()
    headers = list(batch.headers or [])
    if not headers:
        raise ValueError("Procedure extract returned no columns")
    sample_rows: list[dict[str, Any]] = []
    for row in batch.rows or []:
        if isinstance(row, dict):
            sample_rows.append(dict(row))
        else:
            sample_rows.append(
                {
                    headers[i]: (row[i] if i < len(row) else "")
                    for i in range(len(headers))
                }
            )
    schema = {}
    meta = getattr(batch, "meta", None) or {}
    native = meta.get("native_types") if isinstance(meta, dict) else {}
    if isinstance(native, dict):
        schema = {str(k): str(v) for k, v in native.items()}
    columns = [
        {
            "name": h,
            "inferred_type": schema.get(h, "VARCHAR"),
            "nullable": True,
        }
        for h in headers
    ]
    return {
        "ok": True,
        "columns": columns,
        "schema": schema,
        "db_type": str(conn.get("type") or conn.get("format") or ""),
        "cfg": cfg,
        "endpoint": endpoint,
        "sample_rows": sample_rows,
        "error": "",
        "raw": {},
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
    from services.preflight_sample import engine_sample_rows

    from .query_tools import sample_connector_object

    try:
        res = sample_connector_object(
            connector_id=str(conn.get("id") or ""),
            table=table,
            limit=limit,
            analyze=False,
        )
        if res.success:
            rows = list((res.output or {}).get("rows") or [])
            if rows:
                return rows
        else:
            _LOG.warning("preview sampler failed for %s: %s", table, res.error)
    except Exception as exc:
        _LOG.warning("preview sampler failed for %s: %s", table, exc)
    # The query sampler and the Execute reader are different paths; a failure
    # in the first must not leave Map and Gate-8 judging no rows.
    return engine_sample_rows(
        source_kind="database",
        source_format=str(conn.get("type") or ""),
        source_connector_id=str(conn.get("id") or ""),
        source_table=table,
        limit=limit,
    ).rows


def _require_signed_flag(contract_id: str, require_signed_contract: Any) -> bool:
    """Selecting a contract defaults require-signed the same way Studio does."""
    cid = str(contract_id or "").strip()
    if require_signed_contract is None:
        return bool(cid)
    if isinstance(require_signed_contract, str):
        return require_signed_contract.strip().lower() in {"1", "true", "yes", "on"}
    return bool(require_signed_contract)


def _preview_bound_contract(
    contract_id: str = "",
    require_signed_contract: Any = None,
) -> dict[str, Any]:
    """Read-only bind for plan_transfer. Never invents. Never raises.

    OPEN / unsigned / missing still appear so the operator can see why Confirm
    would refuse. Staging uses ``_stage_bound_contract`` instead.
    """
    cid = str(contract_id or "").strip()
    require = _require_signed_flag(cid, require_signed_contract)
    from services.contract_store import bound_contract_preview, get_contract_store

    preview = bound_contract_preview(cid, require_signed=require)
    if not cid:
        return preview
    contract = get_contract_store().get_contract(cid)
    preview["contract_status"] = (
        str(getattr(contract.status, "value", contract.status)).upper()
        if contract is not None
        else "not_found"
    )
    return preview


def _plan_contract_refusal(contract_id: str = "", require_signed_contract: Any = None) -> str:
    """Why Confirm would refuse this contract bind; ``""`` when it would not.

    Same check as staging, so plan_transfer cannot approve a bind that
    start_transfer then refuses (QA MX3-02).
    """
    try:
        _stage_bound_contract(contract_id, require_signed_contract)
    except ValueError as exc:
        _LOG.warning("plan_transfer: contract bind %r refused: %s", contract_id, exc)
        return str(exc)
    return ""


def _block_plan_on_contract(preflight: dict[str, Any], refusal: str) -> dict[str, Any]:
    """Turn the plan's verdict into block for a contract bind Confirm refuses."""
    out = dict(preflight)
    bundle = dict(out.get("proof_bundle") or {})
    bundle["transfer_decision"] = {
        **dict(bundle.get("transfer_decision") or {}),
        "decision": "block",
        "reason": refusal,
    }
    out["proof_bundle"] = bundle
    out["blockers"] = [
        *(out.get("blockers") or []),
        {
            "id": "contract_bind",
            "severity": "block",
            "message": refusal,
            "fix": "Bind an existing SIGNED contract, or turn off require_signed_contract.",
        },
    ]
    return out


def _stage_bound_contract(
    contract_id: str = "",
    require_signed_contract: Any = None,
) -> dict[str, Any]:
    """Fail-closed SIGNED bind at Pilot staging. Empty id leaves enforce unset.

    Selecting a contract defaults ``require_signed`` the same way Studio and
    ScheduleForm do. Confirm is not offered for an unsigned bind.
    """
    cid = str(contract_id or "").strip()
    require = _require_signed_flag(cid, require_signed_contract)
    from services.contract_store import assert_contract_breaker_allows, bound_contract_preview
    from services.schedule_store import assert_signed_contract

    assert_signed_contract(cid, require_signed=require)
    if cid:
        assert_contract_breaker_allows(cid)
    return bound_contract_preview(cid, require_signed=require)


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
    source_timezone: str = "",
    source_read_mode: str = "",
    procedure_call: str = "",
    source_query: str = "",
    procedure_params: Any = None,
    contract_id: str = "",
    require_signed_contract: Any = None,
    source_filter: dict[str, Any] | None = None,
    upsert_key: str = "",
    primary_key: str = "",
    dedupe_key: str = "",
    cursor_column: str = "",
    cursor_semantics: str = "",
    rule_questions: list[str] | None = None,
    applied_rules: list[str] | None = None,
    cadence: str = "",
    all_tables: bool = False,
    risk_acceptance: dict[str, Any] | None = None,
    pii_acknowledgement: dict[str, Any] | None = None,
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
        source_timezone=source_timezone,
        source_read_mode=source_read_mode,
        procedure_call=procedure_call,
        source_query=source_query,
        procedure_params=procedure_params,
        source_filter=source_filter,
        upsert_key=upsert_key or primary_key,
        primary_key=primary_key,
        dedupe_key=dedupe_key,
        cursor_column=cursor_column,
        cursor_semantics=cursor_semantics,
        rule_questions=rule_questions,
        applied_rules=applied_rules,
        cadence=cadence,
        all_tables=all_tables,
        risk_acceptance=risk_acceptance,
        pii_acknowledgement=pii_acknowledgement,
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
            # A refusal without the exit is the dead end operators write in about:
            # the engine already resolved the fix per root cause, so it travels
            # with the refusal instead of staying in a payload nobody reads.
            fixes = "; ".join(
                fix
                for fix in (
                    str((b.get("details") or {}).get("recommended_fix") or "").strip()
                    for b in blockers[:4]
                )
                if fix
            )
            err = (
                "Preflight blocked this transfer, so I won't start it"
                + (f" — {listed}" if listed else "")
                + (f" (run {preflight.get('run_id')})." if preflight.get("run_id") else ".")
                + (f" To proceed: {fixes}" if fixes else "")
            )
        elif str(preflight.get("run_id") or "").startswith("pf_local_"):
            err = (
                "Local / browser-only preflight cannot unlock Confirm — "
                "re-run Validate against the API until decision is approve."
            )
        else:
            reason = str(
                ((preflight.get("proof_bundle") or {}).get("transfer_decision") or {}).get("reason")
                or ""
            ).strip()
            err = (
                f"Preflight is {decision}-grade, not approve — Confirm is blocked "
                "until Studio Execute would unlock "
                + (f"(run {preflight.get('run_id')})." if preflight.get("run_id") else ".")
                + (f" Reason: {reason}" if reason else "")
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
            "source_read_mode": source.get("source_read_mode") or "table",
            "procedure_call": source.get("procedure_call") or "",
            "source_query": source.get("source_query") or "",
            "procedure_params": source.get("procedure_params") or {},
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
        # Row rules the operator stated: the run must carry them, or the rows it
        # writes are not the rows that were asked for.
        "source_filter": plan.get("source_filter") or {},
        "stream_contracts": plan.get("stream_contracts") or [],
        # Chat can never turn the gates off.
        "skip_preflight": False,
        "preflight_run_id": preflight.get("run_id"),
    }
    pii_ack = _pii_acknowledgement(pii_acknowledgement)
    if pii_ack:
        # Execute re-runs Validate: the same ack and its trail ride on the job.
        payload["compliance_acknowledged"] = True
        payload["acknowledgment_actor"] = pii_ack["approved_by"]
        payload["acknowledgment_reason"] = pii_ack["reason"]
    try:
        bound = _stage_bound_contract(contract_id, require_signed_contract)
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))
    payload.update(bound)
    # Belt-and-suspenders: ignore any injected/mutated skip even if schema drifts.
    payload["skip_preflight"] = False  # hard deny - Pilot never bypasses Validate
    preview = {
        "source": f"{source['connector_name']}.{source['table']}",
        "destination": f"{destination['connector_name']}.{destination['table']}",
        "sync_mode": plan.get("sync_mode"),
        "source_read_mode": source.get("source_read_mode") or "table",
        "procedure_call": source.get("procedure_call") or "",
        "source_query": source.get("source_query") or "",
        "mapped_columns": plan.get("mapped_count"),
        "unmapped_source_columns": plan.get("unmapped_source_columns"),
        "lossy_conversions": len(plan.get("lossy_conversions") or []),
        "destination_table_exists": destination.get("table_exists"),
        "preflight_run_id": preflight.get("run_id"),
        "readiness_score": preflight.get("readiness_score"),
        "validation_mode": plan.get("validation_mode"),
        "schema_policy": plan.get("schema_policy"),
    }
    if pii_ack:
        preview["pii_acknowledgement"] = dict(pii_ack)
    rules_preview = plan.get("data_rules") or {}
    if rules_preview.get("row_filter"):
        preview["row_filter"] = rules_preview["row_filter"]
    if rules_preview.get("upsert_key"):
        preview["upsert_key"] = rules_preview["upsert_key"]
    if rules_preview.get("cursor_column"):
        preview["cursor_column"] = rules_preview["cursor_column"]
    if rules_preview.get("cursor_semantics"):
        preview["cursor_semantics"] = rules_preview["cursor_semantics"]
    if rules_preview.get("cursor_inferred"):
        col = rules_preview.get("cursor_column") or "the watermark"
        preview["cursor_inferred"] = True
        preview["cursor_assumption"] = (
            f"{col} is the only modification-timestamp column on the source, "
            "so this incremental upsert advances on it as modification_timestamp. "
            "That assumes the source maintains the column on every change. "
            "Pass cursor_column and cursor_semantics to choose a different watermark."
        )
    if payload.get("limit"):
        preview["row_limit"] = payload["limit"]
    if rules_preview.get("cadence_not_scheduled"):
        # Said plainly in the confirm preview: this run is one-off. Nothing here
        # creates the schedule the operator asked for.
        preview["cadence_not_scheduled"] = rules_preview["cadence_not_scheduled"]
    if bound.get("contract_id"):
        preview["contract_id"] = bound["contract_id"]
        preview["require_signed_contract"] = bound["require_signed_contract"]
        preview["enforce_contract"] = True
        if bound.get("breaker_state"):
            preview["breaker_state"] = bound["breaker_state"]

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
