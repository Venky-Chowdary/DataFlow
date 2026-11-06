"""Module 7 — Validation Mode Contract (guarantees / non-guarantees / coverage).

Charter modes:

* STRICT — fail on every fidelity risk
* BALANCED — allow approved risks
* MIGRATION — warn on recoverable issues
* DISCOVERY — only report (never unlocks Execute)
* AUDIT — never write

Legacy Studio mode ``maximum`` is retained as a stricter STRICT variant
(confidence floor 0.95). Unknown modes fail closed to STRICT.

See ``docs/VALIDATION_MODE_CONTRACT.md``.
"""

from __future__ import annotations

from typing import Any

VALIDATION_MODES = frozenset(
    {
        "strict",
        "maximum",
        "balanced",
        "migration",
        "discovery",
        "audit",
        "lenient",
    }
)

# Modes that must refuse destination writes (Migration Assurance).
WRITE_REFUSED_MODES = frozenset({"discovery", "audit"})


class ValidationModeWriteRefused(RuntimeError):
    """Execute attempted under a mode that never writes."""


#: Unknown names still fail closed. ``lenient`` is its own mode (same floor
#: as balanced) so a plan does not silently rename the operator's word.
VALIDATION_MODE_ALIASES: dict[str, str] = {}


def normalize_validation_mode(mode: str | None) -> str:
    m = (mode or "strict").strip().lower()
    m = VALIDATION_MODE_ALIASES.get(m, m)
    if m not in VALIDATION_MODES:
        return "strict"
    return m


def mode_contract(mode: str | None) -> dict[str, Any]:
    """Return the immutable-style contract for a validation mode."""
    m = normalize_validation_mode(mode)
    specs: dict[str, dict[str, Any]] = {
        "strict": {
            "id": "strict",
            "label": "Strict",
            "confidence_floor": 0.85,
            "allows_write": True,
            "allows_approved_risks": True,
            "hard_block_fidelity": True,
            "warn_recoverable": False,
            "report_only": False,
            "coverage": "sample_unless_probe_says_otherwise",
            "guarantees": [
                "Fidelity / schema contract failures hard-block Execute unless an "
                "approved Migration Risk Contract continues the column.",
                "Gate-8 strict checksum mismatch fails the job.",
                "Sample validation never claims population proof.",
            ],
            "non_guarantees": [
                "Passing Strict Validate does not prove full-table RI or population checksum.",
                "Approved risks still follow their execution / quarantine / rollback policies.",
            ],
        },
        "maximum": {
            "id": "maximum",
            "label": "Maximum",
            "confidence_floor": 0.95,
            "allows_write": True,
            "allows_approved_risks": True,
            "hard_block_fidelity": True,
            "warn_recoverable": False,
            "report_only": False,
            "coverage": "sample_unless_probe_says_otherwise",
            "guarantees": [
                "Same hard-block posture as Strict with a higher mapping confidence floor (0.95).",
                "Sample validation never claims population proof.",
            ],
            "non_guarantees": [
                "Maximum is not population proof.",
                "Does not invent warehouse restore or exactly-once CDC.",
            ],
        },
        "balanced": {
            "id": "balanced",
            "label": "Balanced",
            "confidence_floor": 0.75,
            "allows_write": True,
            "allows_approved_risks": True,
            "hard_block_fidelity": True,
            "warn_recoverable": True,
            "report_only": False,
            "coverage": "sample_unless_probe_says_otherwise",
            "guarantees": [
                "Approved Migration Risk Contracts may unlock Execute for lossy columns.",
                "Unapproved fidelity risks still block.",
                "Sample validation never claims population proof.",
            ],
            "non_guarantees": [
                "Checksum mismatch always fails Gate-8 — sample success never "
                "overrides diverging digests; population proof remains false.",
                "Not a substitute for Strict on financial cutovers.",
            ],
        },
        "lenient": {
            "id": "lenient",
            "label": "Lenient",
            "confidence_floor": 0.75,
            "allows_write": True,
            # A text cell that does not parse into its typed column is held
            # out with its reason; the rows that parse load (every gate).
            "holds_out_bad_cells": True,
            "allows_approved_risks": True,
            "hard_block_fidelity": True,
            "warn_recoverable": True,
            "report_only": False,
            "coverage": "sample_unless_probe_says_otherwise",
            "guarantees": [
                "Lenient is a named policy with the balanced confidence floor (0.75).",
                "It is not silently rewritten to balanced.",
                "Unapproved fidelity risks still block. Sample validation never claims population proof.",
            ],
            "non_guarantees": [
                "Checksum mismatch always fails Gate-8.",
                "Lenient is not a license to drop rows.",
            ],
        },
        "migration": {
            "id": "migration",
            "label": "Migration",
            "confidence_floor": 0.75,
            "allows_write": True,
            "allows_approved_risks": True,
            "hard_block_fidelity": True,
            "warn_recoverable": True,
            "report_only": False,
            "coverage": "sample_unless_probe_says_otherwise",
            "guarantees": [
                "Recoverable issues surface as warnings with Root Cause + Risk Contract path.",
                "Unrecoverable fidelity / identity failures still hard-block.",
                "Sample validation never claims population proof.",
            ],
            "non_guarantees": [
                "Migration mode does not auto-accept risks — operator contract still required.",
                "Does not prove population RI or full checksum by itself.",
            ],
        },
        "discovery": {
            "id": "discovery",
            "label": "Discovery",
            "confidence_floor": 0.0,
            "allows_write": False,
            "allows_approved_risks": False,
            "hard_block_fidelity": False,
            "warn_recoverable": True,
            "report_only": True,
            "coverage": "sample_report_only",
            "guarantees": [
                "Findings are reported for explainability without inventing a green Execute path.",
                "Destination writes are refused (allows_write=false).",
            ],
            "non_guarantees": [
                "Discovery does not assure migration correctness.",
                "Discovery never unlocks Execute — rematerialize under Strict/Balanced/Migration.",
            ],
        },
        "audit": {
            "id": "audit",
            "label": "Audit",
            "confidence_floor": 0.85,
            "allows_write": False,
            "allows_approved_risks": False,
            "hard_block_fidelity": True,
            "warn_recoverable": False,
            "report_only": False,
            "coverage": "sample_audit_trail",
            "guarantees": [
                "Validate runs with hard blocks recorded for audit trail.",
                "Destination writes are refused (allows_write=false) — AUDIT never writes.",
            ],
            "non_guarantees": [
                "Audit mode is not a substitute for post-write Gate-8 proof on a real load.",
                "Population proof is not invented from samples.",
            ],
        },
    }
    contract = dict(specs[m])
    contract["population_proof"] = False
    contract["documentation"] = "docs/VALIDATION_MODE_CONTRACT.md"
    return contract


def confidence_floor_for_mode(mode: str | None) -> float:
    return float(mode_contract(mode)["confidence_floor"])


#: Lenient's column bound (``mode_holdout_kind``): at least this share of the
#: non-empty sampled cells must cast for the rest to be held out per cell.
LENIENT_MIN_CAST_SHARE = 0.5


def mode_holds_out_refusal(
    mode: str | None, reason: str | None = None, *, record_batch: bool = True
) -> bool:
    """THE policy: does this validation mode hold out a cell its cast refused?

    Every gate that grades a refused cell asks this one question (coercion
    probe, G3, G5, G8, Target DDL, schema drift, the integrity audit):

    * lenient — every per-cell cast refusal (``holds_out_bad_cells``);
    * balanced — only a refusal whose REASON is the invalid-calendar rule
      (``connectors.sql_temporal.is_invalid_calendar_refusal``), on a
      record-batch (file) source (QA browser 3);
    * strict / maximum / review — nothing.

    ``reason=None`` asks for any reason, which only lenient answers yes to.
    """
    if _holds_out_every_refusal(mode):
        return True
    if reason is None or not record_batch:
        return False
    if str(mode_contract(mode).get("id") or "") != "balanced":
        return False
    from connectors.sql_temporal import is_invalid_calendar_refusal

    return is_invalid_calendar_refusal(reason)


def mode_holdout_kind(
    mode: str | None, *, failed: int, held: int, cast_ok: int, record_batch: bool
) -> str:
    """The column-level answer: ``"cast_failure"`` (lenient), ``"invalid_calendar"``
    (balanced, fewer than one in five sampled values — ``connectors.sql_temporal.
    INVALID_CALENDAR_MAX_SHARE``), or ``""`` (block). ``held`` counts the failures
    :func:`mode_holds_out_refusal` held out; every failure must be one, and some
    sampled cell must cast — an all-bad column is never a holdout.
    """
    if failed <= 0 or held != failed or cast_ok <= 0:
        return ""
    if _holds_out_every_refusal(mode):
        # A column is a cast with some bad cells only while most sampled cells
        # cast: at least half (LENIENT_MIN_CAST_SHARE). 1 castable of 500 is a
        # wrong mapping, not 499 rows to quarantine.
        return "cast_failure" if cast_ok >= LENIENT_MIN_CAST_SHARE * (cast_ok + failed) else ""
    from connectors.sql_temporal import invalid_calendar_share_holds_out

    if record_batch and invalid_calendar_share_holds_out(failed, failed + cast_ok):
        return "invalid_calendar"
    return ""


def _holds_out_every_refusal(mode: str | None) -> bool:
    """Private to the policy above: the contract field ``holds_out_bad_cells``."""
    return bool(mode_contract(mode).get("holds_out_bad_cells"))


def mode_allows_write(mode: str | None) -> bool:
    return bool(mode_contract(mode)["allows_write"])


def assert_mode_allows_write(mode: str | None) -> None:
    """Fail closed before any destination mutation."""
    m = normalize_validation_mode(mode)
    if not mode_allows_write(m):
        c = mode_contract(m)
        raise ValidationModeWriteRefused(
            f"Validation mode `{m}` refuses destination writes "
            f"(allows_write=false). Guarantees: {c['guarantees'][0]} "
            f"Non-guarantees: {c['non_guarantees'][0]} "
            "See docs/VALIDATION_MODE_CONTRACT.md."
        )


def stamp_validation_mode(preflight_or_summary: dict[str, Any], mode: str | None) -> dict[str, Any]:
    """Attach mode contract to a preflight / job summary dict."""
    contract = mode_contract(mode)
    preflight_or_summary["validation_mode"] = contract["id"]
    preflight_or_summary["validation_mode_contract"] = contract
    return preflight_or_summary
