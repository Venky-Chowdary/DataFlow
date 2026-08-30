"""Gate G16 — Field Reduction Ledger: a typed, evidenced disposition per source field.

G13 already refuses to write while a source column is neither mapped nor
declared omitted, and the proof pack carries the omitted *names*. A regulator
reviewing a field-reducing migration (10 mainframe fields into a 7-field
screen) asks a harder question than "was the drop declared?": *why* was each
field dropped, who accepted that, on what evidence, and — when the answer is
"the data is retained elsewhere" — where.

This module turns the boolean ``intentional_omit`` into a ledger:

  * every source field gets exactly one typed disposition (carried, folded
    into a target, or reduced);
  * every reduction carries a reason code, and the codes that make a factual
    claim about the data (``dropped_empty``, ``dropped_constant``) are checked
    against observed values instead of trusted;
  * a claim contradicted by the sample blocks — the operator must either fix
    the code or accept it as a judgement call (``dropped_not_required``) which
    is recorded as a judgement, not as a fact;
  * ``archive_only`` without an archive reference blocks: "it is kept
    somewhere else" is not evidence unless the somewhere is named.

Honesty boundaries, deliberately narrow:

  * Justification statistics come from the preflight sample, not the
    population. A sample that is 100% NULL is not proof that the column is
    empty, so the ledger labels the basis (``sample``) and never claims
    population proof. Contradiction (a non-null value in the sample) *is*
    conclusive in one direction: the "always empty" claim is false.
  * The ledger records approvals; it does not authenticate approvers. Identity
    comes from the caller's session, and the signature binds the ledger to a
    job, not to a person.
  * A signed ledger proves the reduction decisions were not edited after
    approval. It does not prove the reduction was a good idea.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from services.brand_env import getenv_brand
from services.mapping_constraints import is_intentional_omit

GATE_ID = "g16_field_reduction"
LEDGER_SCHEMA = "field_reduction_ledger_v1"
_NAMED_LIMIT = 8

# ── Dispositions ──────────────────────────────────────────────────────────────
# Carried: the source value reaches the destination in some form.
CARRIED_1_1 = "carried"
CARRIED_TRANSFORMED = "carried_transformed"
FOLDED_MANY_TO_ONE = "merged_into"
SPLIT_ONE_TO_MANY = "split_into"

CARRIED_DISPOSITIONS = frozenset(
    {CARRIED_1_1, CARRIED_TRANSFORMED, FOLDED_MANY_TO_ONE, SPLIT_ONE_TO_MANY}
)

# Reduced: the source value does not reach the destination.
REDUCTION_DISPOSITIONS: dict[str, dict[str, Any]] = {
    "dropped_empty": {
        "label": "Empty in source",
        "claim": "no_values",
        "requires": (),
        "evidence": "observed",
    },
    "dropped_constant": {
        "label": "Single constant value",
        "claim": "single_value",
        "requires": (),
        "evidence": "observed",
    },
    "dropped_redundant": {
        "label": "Redundant — value available from another field",
        "claim": None,
        "requires": ("reason_text",),
        "evidence": "declared",
    },
    "dropped_obsolete": {
        "label": "Obsolete in the target process",
        "claim": None,
        "requires": ("reason_text",),
        "evidence": "declared",
    },
    "dropped_pii_minimization": {
        "label": "Dropped for data minimisation",
        "claim": None,
        "requires": ("reason_text",),
        "evidence": "declared",
    },
    "dropped_not_required": {
        "label": "Not required by the target process",
        "claim": None,
        "requires": ("reason_text",),
        "evidence": "declared",
    },
    "archive_only": {
        "label": "Retained in an archive, not in the target",
        "claim": None,
        "requires": ("reason_text", "archive_reference"),
        "evidence": "declared",
    },
    "deferred_phase": {
        "label": "Deferred to a later migration phase",
        "claim": None,
        "requires": ("reason_text",),
        "evidence": "declared",
    },
    # Terminal fallback for a legacy boolean omit with no reason code. Never
    # silently upgraded into one of the codes above — an unexplained drop is
    # reported as unexplained.
    "dropped_unclassified": {
        "label": "Dropped without a recorded reason",
        "claim": None,
        "requires": (),
        "evidence": "none",
    },
}

UNACCOUNTED = "unaccounted"

_REASON_ALIASES = {
    "empty": "dropped_empty",
    "all_null": "dropped_empty",
    "null": "dropped_empty",
    "constant": "dropped_constant",
    "redundant": "dropped_redundant",
    "duplicate": "dropped_redundant",
    "obsolete": "dropped_obsolete",
    "deprecated": "dropped_obsolete",
    "pii": "dropped_pii_minimization",
    "minimization": "dropped_pii_minimization",
    "minimisation": "dropped_pii_minimization",
    "not_required": "dropped_not_required",
    "not_needed": "dropped_not_required",
    "archive": "archive_only",
    "archived": "archive_only",
    "archive_only": "archive_only",
    "deferred": "deferred_phase",
    "later_phase": "deferred_phase",
}

_ISSUE_BLOCK = "block"
_ISSUE_WARN = "warn"


def strict_field_reduction() -> bool:
    """True when every reduction must carry a reason code and an approver.

    Off by default: turning an unexplained legacy omission into a hard block
    would fail jobs that were already approved under G13. Regulated tenants
    turn it on (``DATAWRAP_FIELD_REDUCTION_STRICT=true``) and then an
    unexplained drop cannot reach Execute.
    """
    return str(getenv_brand("FIELD_REDUCTION_STRICT", "false")).strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
        "required",
    }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_text(mapping: dict[str, Any], *keys: str) -> str:
    for key in keys:
        val = _text(mapping.get(key))
        if val:
            return val
    return ""


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def definition_hash(columns: list[str] | None, types: dict[str, str] | None = None) -> str:
    """Stable hash of a field inventory (name + declared type, order-independent).

    Binds a ledger to the schema it described. A ledger approved against a
    10-field copybook must not be presentable as evidence for an 11-field one.
    """
    items = []
    for col in sorted({_text(c) for c in (columns or []) if _text(c)}):
        items.append(f"{col}:{_text((types or {}).get(col))}")
    return _sha256("|".join(items))


def normalize_reason_code(raw: Any) -> str:
    """Map an operator/UI reason string onto a canonical reduction code."""
    key = _text(raw).lower().replace("-", "_").replace(" ", "_")
    if not key:
        return ""
    if key in REDUCTION_DISPOSITIONS:
        return key
    if key in _REASON_ALIASES:
        return _REASON_ALIASES[key]
    if key.startswith("dropped_") or key.startswith("drop_"):
        # An unknown dropped_* code is reported as unknown, not coerced into a
        # neighbouring meaning.
        return key
    return key


# ── Sample-derived evidence ───────────────────────────────────────────────────
def observe_field(rows: list[dict[str, Any]] | None, column: str) -> dict[str, Any]:
    """Sample statistics for one column: emptiness and cardinality only.

    Intentionally not a profiler: this exists to contradict false claims, so it
    reports what was seen and how many rows it saw, never an inferred type.
    """
    sample = [r for r in (rows or []) if isinstance(r, dict)]
    if not sample:
        return {"basis": "none", "sampled_rows": 0}
    present = 0
    non_empty: list[str] = []
    for row in sample:
        if column not in row:
            continue
        present += 1
        val = row.get(column)
        if val is None:
            continue
        text = str(val)
        if text.strip() == "":
            continue
        non_empty.append(text)
    if present == 0:
        return {"basis": "none", "sampled_rows": 0, "note": "column absent from sample rows"}
    distinct = len({v for v in non_empty})
    return {
        "basis": "sample",
        "sampled_rows": present,
        "non_empty_rows": len(non_empty),
        "null_or_empty_rows": present - len(non_empty),
        "distinct_non_empty": distinct,
        "sample_all_empty": len(non_empty) == 0,
        "sample_single_value": len(non_empty) > 0 and distinct == 1,
        "example_values": sorted({v[:48] for v in non_empty})[:3],
        "note": (
            "Sample evidence. A sample that is all-empty is not population "
            "proof that the column is empty; a non-empty sample IS proof that "
            "an 'always empty' claim is false."
        ),
    }


def _claim_contradiction(claim: str | None, observed: dict[str, Any]) -> str | None:
    """Return a message when observed values disprove the reason code's claim."""
    if not claim or observed.get("basis") != "sample":
        return None
    if claim == "no_values" and not observed.get("sample_all_empty"):
        examples = ", ".join(observed.get("example_values") or [])
        return (
            f"declared empty, but {observed.get('non_empty_rows')} of "
            f"{observed.get('sampled_rows')} sampled rows carry a value"
            + (f" (e.g. {examples})" if examples else "")
        )
    if claim == "single_value" and int(observed.get("distinct_non_empty") or 0) > 1:
        return (
            f"declared constant, but the sample holds "
            f"{observed.get('distinct_non_empty')} distinct values"
        )
    return None


# ── Entry construction ────────────────────────────────────────────────────────
def _mapping_index(
    mappings: list[dict[str, Any]] | None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, set[str]]]:
    """Return (source → its mappings, target → contributing sources)."""
    by_source: dict[str, list[dict[str, Any]]] = {}
    target_sources: dict[str, set[str]] = {}
    for raw in mappings or []:
        if not isinstance(raw, dict):
            continue
        src = _text(raw.get("source"))
        if not src:
            continue
        by_source.setdefault(src.lower(), []).append(raw)
        if is_intentional_omit(raw):
            continue
        tgt = _text(raw.get("target"))
        if tgt:
            target_sources.setdefault(tgt.lower(), set()).add(src.lower())
    return by_source, target_sources


def _carried_disposition(
    mapping_rows: list[dict[str, Any]],
    target_sources: dict[str, set[str]],
) -> tuple[str, list[str], str]:
    """Classify a carried field as 1:1, transformed, 1:N split, or N:1 fold."""
    targets = [
        _text(m.get("target"))
        for m in mapping_rows
        if not is_intentional_omit(m) and _text(m.get("target"))
    ]
    transform = ""
    for m in mapping_rows:
        transform = _first_text(m, "transform", "engine_transform", "engineTransform", "expression")
        if transform and transform.lower() not in {"none", "identity", "passthrough"}:
            break
        transform = ""
    unique_targets = list(dict.fromkeys(targets))
    if len(unique_targets) > 1:
        return SPLIT_ONE_TO_MANY, unique_targets, transform
    if unique_targets:
        contributors = target_sources.get(unique_targets[0].lower(), set())
        if len(contributors) > 1:
            return FOLDED_MANY_TO_ONE, unique_targets, transform
    if transform:
        return CARRIED_TRANSFORMED, unique_targets, transform
    return CARRIED_1_1, unique_targets, transform


def _reduction_entry(
    *,
    source: str,
    mapping_rows: list[dict[str, Any]],
    observed: dict[str, Any],
    strict: bool,
) -> dict[str, Any]:
    raw_reason = ""
    reason_text = ""
    archive_reference = ""
    retention_until = ""
    approved_by = ""
    approved_at = ""
    for m in mapping_rows:
        raw_reason = raw_reason or _first_text(
            m, "omit_reason", "omitReason", "omit_reason_code", "reduction_reason"
        )
        reason_text = reason_text or _first_text(
            m, "omit_reason_text", "omitReasonText", "omit_note", "reduction_note"
        )
        archive_reference = archive_reference or _first_text(
            m, "archive_reference", "archiveReference", "archive_location"
        )
        retention_until = retention_until or _first_text(
            m, "retention_until", "retentionUntil"
        )
        approved_by = approved_by or _first_text(
            m, "omit_approved_by", "omitApprovedBy", "approved_by", "approvedBy"
        )
        approved_at = approved_at or _first_text(
            m, "omit_approved_at", "omitApprovedAt", "approved_at", "approvedAt"
        )

    code = normalize_reason_code(raw_reason)
    issues: list[dict[str, str]] = []
    if not code:
        code = "dropped_unclassified"
        issues.append(
            {
                "severity": _ISSUE_BLOCK if strict else _ISSUE_WARN,
                "code": "reason_code_missing",
                "message": (
                    "dropped with no recorded reason — the drop is declared but "
                    "unexplained"
                ),
            }
        )
    spec = REDUCTION_DISPOSITIONS.get(code)
    if spec is None:
        issues.append(
            {
                "severity": _ISSUE_BLOCK,
                "code": "reason_code_unknown",
                "message": (
                    f"reason code {code!r} is not a Field Reduction Ledger "
                    "disposition — Datawrap will not record a reduction it "
                    "cannot classify"
                ),
            }
        )
        spec = {"claim": None, "requires": (), "evidence": "none", "label": code}

    values = {
        "reason_text": reason_text,
        "archive_reference": archive_reference,
        "retention_until": retention_until,
    }
    for required in spec.get("requires") or ():
        if not values.get(required):
            issues.append(
                {
                    "severity": _ISSUE_BLOCK,
                    "code": f"{required}_missing",
                    "message": (
                        f"{code} requires {required} — "
                        + (
                            "an archive claim must name where the data is kept"
                            if required == "archive_reference"
                            else "record why the field is not carried"
                        )
                    ),
                }
            )

    contradiction = _claim_contradiction(spec.get("claim"), observed)
    if contradiction:
        issues.append(
            {
                "severity": _ISSUE_BLOCK,
                "code": "claim_contradicted_by_sample",
                "message": (
                    f"{contradiction}. Use a judgement code "
                    "(dropped_not_required / dropped_redundant) with a reason "
                    "instead of a factual claim the data disproves."
                ),
            }
        )
    elif spec.get("claim") and observed.get("basis") != "sample":
        issues.append(
            {
                "severity": _ISSUE_WARN,
                "code": "claim_unverified",
                "message": (
                    f"{code} states a fact about the data, but no sample was "
                    "available to check it — recorded as unverified"
                ),
            }
        )

    if strict and not approved_by:
        issues.append(
            {
                "severity": _ISSUE_BLOCK,
                "code": "approval_missing",
                "message": "strict field reduction requires a named approver for each drop",
            }
        )

    return {
        "source": source,
        "disposition": code,
        "disposition_label": spec.get("label") or code,
        "carried": False,
        "targets": [],
        "reason_code": code,
        "reason_text": reason_text,
        "reason_evidence_kind": spec.get("evidence") or "none",
        "archive_reference": archive_reference or None,
        "retention_until": retention_until or None,
        "approved_by": approved_by or None,
        "approved_at": approved_at or None,
        "observed": observed,
        "issues": issues,
        "evidence_complete": not issues,
    }


def classify_field_dispositions(
    *,
    source_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    sample_rows: list[dict[str, Any]] | None = None,
    strict: bool | None = None,
) -> list[dict[str, Any]]:
    """One typed disposition per declared source column, in source order."""
    strict_mode = strict_field_reduction() if strict is None else bool(strict)
    by_source, target_sources = _mapping_index(mappings)
    entries: list[dict[str, Any]] = []
    for raw_col in source_columns or []:
        source = _text(raw_col)
        if not source:
            continue
        rows = by_source.get(source.lower(), [])
        carried_rows = [m for m in rows if not is_intentional_omit(m) and _text(m.get("target"))]
        if carried_rows:
            disposition, targets, transform = _carried_disposition(rows, target_sources)
            entries.append(
                {
                    "source": source,
                    "disposition": disposition,
                    "disposition_label": disposition.replace("_", " "),
                    "carried": True,
                    "targets": targets,
                    "transform": transform or None,
                    "transform_sha256": _sha256(transform) if transform else None,
                    "reason_code": None,
                    "reason_text": None,
                    "issues": [],
                    "evidence_complete": True,
                }
            )
            continue
        if any(is_intentional_omit(m) for m in rows):
            entries.append(
                _reduction_entry(
                    source=source,
                    mapping_rows=rows,
                    observed=observe_field(sample_rows, source),
                    strict=strict_mode,
                )
            )
            continue
        entries.append(
            {
                "source": source,
                "disposition": UNACCOUNTED,
                "disposition_label": "No decision recorded",
                "carried": False,
                "targets": [],
                "reason_code": None,
                "reason_text": None,
                "observed": observe_field(sample_rows, source),
                "issues": [
                    {
                        "severity": _ISSUE_BLOCK,
                        "code": "no_disposition",
                        "message": (
                            "neither mapped nor declared omitted — an unanswered "
                            "question, not a decision (see G13)"
                        ),
                    }
                ],
                "evidence_complete": False,
            }
        )
    return entries


def build_field_reduction_ledger(
    *,
    source_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    target_columns: list[str] | None = None,
    source_column_types: dict[str, str] | None = None,
    target_column_types: dict[str, str] | None = None,
    sample_rows: list[dict[str, Any]] | None = None,
    mapping_hash: str | None = None,
    strict: bool | None = None,
) -> dict[str, Any]:
    """Assemble the (unsigned) Field Reduction Ledger for a mapping set."""
    strict_mode = strict_field_reduction() if strict is None else bool(strict)
    entries = classify_field_dispositions(
        source_columns=source_columns,
        mappings=mappings,
        sample_rows=sample_rows,
        strict=strict_mode,
    )
    counts: dict[str, int] = {}
    for entry in entries:
        key = str(entry.get("disposition") or UNACCOUNTED)
        counts[key] = counts.get(key, 0) + 1
    carried = [e for e in entries if e.get("carried")]
    reduced = [
        e
        for e in entries
        if not e.get("carried") and str(e.get("disposition")) != UNACCOUNTED
    ]
    unaccounted = [
        str(e.get("source")) for e in entries if str(e.get("disposition")) == UNACCOUNTED
    ]
    blocking = [
        {"source": e.get("source"), **issue}
        for e in entries
        for issue in e.get("issues") or []
        if issue.get("severity") == _ISSUE_BLOCK
    ]
    warnings = [
        {"source": e.get("source"), **issue}
        for e in entries
        for issue in e.get("issues") or []
        if issue.get("severity") == _ISSUE_WARN
    ]
    return {
        "schema": LEDGER_SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strict": strict_mode,
        "source_field_count": len(entries),
        "target_field_count": len([c for c in (target_columns or []) if _text(c)]),
        "source_definition_sha256": definition_hash(
            [str(e.get("source")) for e in entries], source_column_types
        ),
        "target_definition_sha256": definition_hash(target_columns, target_column_types),
        "mapping_hash": _text(mapping_hash) or None,
        "carried_count": len(carried),
        "reduced_count": len(reduced),
        "unaccounted": unaccounted,
        "counts_by_disposition": counts,
        "entries": entries,
        "blocking_issues": blocking,
        "warnings": warnings,
        "complete": not blocking and not unaccounted,
        "evidence_basis": "preflight_sample",
        "honesty": (
            "Justification statistics are sample-based: an all-empty sample is "
            "not population proof of emptiness. Approvals are recorded, not "
            "authenticated. A signed ledger proves the reduction decisions were "
            "not edited after approval — not that the reduction was correct."
        ),
        "documentation": "docs/FIELD_REDUCTION_LEDGER.md",
    }


def sign_field_reduction_ledger(ledger: dict[str, Any], *, job_id: str) -> dict[str, Any]:
    """Bind a ledger to a job with a content hash + HMAC (same scheme as proof packs)."""
    from services.signed_proof_pack import sign_body

    return sign_body(dict(ledger or {}), subject=f"field_reduction:{job_id}")


def verify_field_reduction_ledger(
    ledger: dict[str, Any], *, job_id: str
) -> dict[str, Any]:
    """Recompute hash + HMAC for a signed ledger. Returns {ok, errors[]}."""
    from services.signed_proof_pack import verify_body

    if not isinstance(ledger, dict):
        return {"ok": False, "errors": ["ledger must be an object"]}
    actual, errors = verify_body(ledger, subject=f"field_reduction:{job_id}")
    if str(ledger.get("schema") or "") != LEDGER_SCHEMA:
        errors = [*errors, f"unexpected ledger schema {ledger.get('schema')!r}"]
    return {"ok": not errors, "errors": errors, "content_sha256": actual}


def build_field_reduction_gate(ledger: dict[str, Any]) -> dict[str, Any]:
    """Return the G16 gate for a ledger.

    Unaccounted columns are G13's block, not this gate's — repeating it here
    would double-count the same blocker in the operator's blocker list. G16
    judges the *quality* of the recorded reductions.
    """
    reduced = int(ledger.get("reduced_count") or 0)
    blocking = [
        issue
        for issue in ledger.get("blocking_issues") or []
        if issue.get("code") != "no_disposition"
    ]
    warnings = list(ledger.get("warnings") or [])

    if blocking:
        named = ", ".join(
            f"{issue.get('source')} ({issue.get('code')})" for issue in blocking[:_NAMED_LIMIT]
        )
        more = (
            f" (+{len(blocking) - _NAMED_LIMIT} more)"
            if len(blocking) > _NAMED_LIMIT
            else ""
        )
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"{len(blocking)} field reduction(s) cannot be recorded as "
                f"governed decisions: {named}{more}"
            ),
            "duration_ms": 0,
            "details": {
                "blocking_issues": blocking,
                "reduced_count": reduced,
                "strict": bool(ledger.get("strict")),
                "rule_id": f"{GATE_ID}.reduction_not_governed",
                "remediation_kind": "review_mappings",
            },
        }

    if warnings:
        named = ", ".join(
            f"{issue.get('source')} ({issue.get('code')})" for issue in warnings[:_NAMED_LIMIT]
        )
        more = (
            f" (+{len(warnings) - _NAMED_LIMIT} more)"
            if len(warnings) > _NAMED_LIMIT
            else ""
        )
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                f"{len(warnings)} field reduction(s) are declared but not fully "
                f"evidenced: {named}{more} — the drop is recorded as unexplained "
                "in the proof pack"
            ),
            "duration_ms": 0,
            "details": {
                "warnings": warnings,
                "reduced_count": reduced,
                "strict": bool(ledger.get("strict")),
                "rule_id": f"{GATE_ID}.reduction_unexplained",
                "remediation_kind": "review_mappings",
            },
        }

    if not reduced:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"No field reduction — all {int(ledger.get('carried_count') or 0)} "
                "carried source field(s) reach the destination"
            ),
            "duration_ms": 0,
            "details": {
                "reduced_count": 0,
                "counts_by_disposition": ledger.get("counts_by_disposition") or {},
            },
        }

    return {
        "id": GATE_ID,
        "status": "pass",
        "message": (
            f"{reduced} field reduction(s) recorded with a typed reason and, where "
            "the reason claims a fact about the data, sample evidence that does "
            "not contradict it"
        ),
        "duration_ms": 0,
        "details": {
            "reduced_count": reduced,
            "counts_by_disposition": ledger.get("counts_by_disposition") or {},
            "strict": bool(ledger.get("strict")),
        },
    }


def build_field_reduction_evidence(
    *,
    source_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
    target_columns: list[str] | None = None,
    source_column_types: dict[str, str] | None = None,
    target_column_types: dict[str, str] | None = None,
    sample_rows: list[dict[str, Any]] | None = None,
    mapping_hash: str | None = None,
    strict: bool | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Convenience for preflight: returns ``(ledger, gate)``."""
    ledger = build_field_reduction_ledger(
        source_columns=source_columns,
        mappings=mappings,
        target_columns=target_columns,
        source_column_types=source_column_types,
        target_column_types=target_column_types,
        sample_rows=sample_rows,
        mapping_hash=mapping_hash,
        strict=strict,
    )
    return ledger, build_field_reduction_gate(ledger)
