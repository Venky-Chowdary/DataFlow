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


#: The column bound for every per-row cast holdout (``mode_holdout_kind``): at
#: least this share of the non-empty sampled cells must cast for the rest to be
#: held out per row. 1 castable of 500 is a wrong mapping, not 499 rows to
#: quarantine. The one owner of the share; every gate reads it through here.
LENIENT_MIN_CAST_SHARE = 0.5
CAST_SHARE_RULE = (
    "a column's refused cells are held out per row only while at least half of "
    "its sampled cells cast; past that the mapping itself is refused"
)


def cast_share_holds_out(failed: int, cast_ok: int) -> bool:
    """True while enough sampled cells cast for the refused ones to be row holdouts."""
    return cast_ok > 0 and cast_ok >= LENIENT_MIN_CAST_SHARE * (cast_ok + failed)


def is_numeric_cast_refusal(reason: str | None) -> bool:
    """Is ``reason`` a numeric cast refusal — the R4-D02 class?

    ``Invalid integer: 'abc'`` / ``Invalid decimal`` / ``Invalid number`` /
    ``Invalid float`` (``decision_kernel`` ``INVALID_NUMERIC``). A fractional
    value into an integer, a carrier overflow, a NOT NULL or a time-zone
    invent are not: those change or lose a value the cell holds.
    """
    from services.decision_kernel.findings import FailureClass, classify_transform_failure

    text = str(reason or "").strip()
    return bool(text) and classify_transform_failure(text) == FailureClass.INVALID_NUMERIC


def _is_balanced(mode: str | None) -> bool:
    return str(mode_contract(mode).get("id") or "") == "balanced"


def mode_holds_out_refusal(
    mode: str | None, reason: str | None = None, *, record_batch: bool
) -> bool:
    """THE policy: does this validation mode hold out a cell its cast refused?

    Every gate that grades a refused cell asks this one question (coercion
    probe, G3, G5, G8, g3f, Target DDL, schema drift, the integrity audit), and
    the writer's quarantine on Execute applies the same classes:

    * lenient — every per-cell cast refusal (``holds_out_bad_cells``);
    * balanced — on a record-batch (file) source only: a numeric cast refusal
      (:func:`is_numeric_cast_refusal`, QA R4-D02) and a date the read cannot
      pin to one real day (``connectors.sql_temporal.is_date_holdout_refusal``:
      an invalid calendar day, QA browser 3, or an ambiguous DMY/MDY order,
      QA R5). A database extract holds nothing out;
    * strict / maximum / review — nothing.

    ``reason=None`` asks for any reason, which only lenient answers yes to.
    ``record_batch`` is required (review): a caller always says whether the
    source is a file, so balanced can never hold out on a database by default.
    """
    if _holds_out_every_refusal(mode):
        return True
    if reason is None or not record_batch or not _is_balanced(mode):
        return False
    from connectors.sql_temporal import is_date_holdout_refusal

    return is_date_holdout_refusal(reason) or is_numeric_cast_refusal(reason)


def mode_holds_out_numeric_casts(mode: str | None, *, record_batch: bool) -> bool:
    """Does the mode hold out a numeric cast refusal on this source? (no reason text)"""
    return mode_holds_out_refusal(mode, "Invalid decimal", record_batch=record_batch)


def mode_may_hold_out(mode: str | None, *, record_batch: bool) -> bool:
    """Can this mode hold ANY refused cell out on this source? (gate pre-check)"""
    return _holds_out_every_refusal(mode) or bool(record_batch and _is_balanced(mode))


def holdout_tally(
    mode: str | None, reasons: Any, *, record_batch: bool
) -> tuple[int, int]:
    """``(held, calendar)`` over refusal reasons, for :func:`mode_holdout_kind`."""
    from connectors.sql_temporal import is_date_holdout_refusal

    held = calendar = 0
    for reason in reasons or ():
        if mode_holds_out_refusal(mode, reason, record_batch=record_batch):
            held += 1
            calendar += int(is_date_holdout_refusal(reason))
    return held, calendar


def mode_holdout_kind(
    mode: str | None,
    *,
    failed: int,
    held: int,
    cast_ok: int,
    record_batch: bool,
    calendar: int = 0,
) -> str:
    """THE column verdict: ``"cast_failure"``, ``"invalid_calendar"`` or ``""`` (block).

    ``held`` counts the failures :func:`mode_holds_out_refusal` held out (every
    failure must be one); ``calendar`` how many of those are invalid-calendar
    refusals (:func:`holdout_tally`), counting every date the read cannot pin
    to one day (invalid calendar or ambiguous order). The share bounds are
    stated once:

    * a cast holdout (lenient; balanced numeric on a file) needs at least half
      the sampled cells to cast — :func:`cast_share_holds_out`, ``CAST_SHARE_RULE``;
    * an invalid-calendar holdout (balanced on a file) needs fewer than one in
      five sampled values bad — ``connectors.sql_temporal.INVALID_CALENDAR_MAX_SHARE``;
    * strict, and balanced on a database extract, never hold out.

    An all-bad column is never a holdout. The coercion probe (G3), the dry run
    (G5), Gate-8's pre-write check, g3f, Target DDL, schema drift and the
    integrity audit's financial and expectation checks all ask this function.
    """
    if failed <= 0 or held != failed or cast_ok <= 0:
        return ""
    if _holds_out_every_refusal(mode):
        return "cast_failure" if cast_share_holds_out(failed, cast_ok) else ""
    if not record_batch or not _is_balanced(mode):
        return ""
    from connectors.sql_temporal import invalid_calendar_share_holds_out

    calendar = max(0, min(int(calendar or 0), failed))
    total = failed + cast_ok
    if calendar and not invalid_calendar_share_holds_out(calendar, total):
        return ""
    if calendar == failed:
        return "invalid_calendar"
    return "cast_failure" if cast_share_holds_out(failed, cast_ok) else ""


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
