"""Canonical Validation Finding model — Migration Decision Kernel.

Transform Engine → Canonical Findings → Decision Kernel → gates / root causes /
Risk Contract / Execute / UI. No surface may re-derive failure class or
remediation rank independently.

Schema version: ``validation_finding_v1``.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping


FINDING_SCHEMA = "validation_finding_v1"


class FailureClass(str, Enum):
    """Operator-facing failure taxonomy (distinct remediation paths)."""

    TYPE_CAST_FAILURE = "TYPE_CAST_FAILURE"
    FRACTIONAL_PRECISION_LOSS = "FRACTIONAL_PRECISION_LOSS"
    EMPTY_VALUE_NOT_NULLABLE = "EMPTY_VALUE_NOT_NULLABLE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_BOOLEAN = "INVALID_BOOLEAN"
    INVALID_NUMERIC = "INVALID_NUMERIC"
    OVERFLOW = "OVERFLOW"
    UNDERFLOW = "UNDERFLOW"
    ENCODING_FAILURE = "ENCODING_FAILURE"
    LENGTH_OVERFLOW = "LENGTH_OVERFLOW"
    SEMANTIC_TRANSFORM_FAILURE = "SEMANTIC_TRANSFORM_FAILURE"
    FIDELITY_COLLAPSE = "FIDELITY_COLLAPSE"
    #: Destination column type was never read — no conversion was compared, so
    #: this is a catalog/introspect gap, not a fidelity verdict. Its only
    #: remediation is reloading the destination schema (which either binds real
    #: types or proves the table absent, i.e. create-new).
    DEST_SCHEMA_UNLOADED = "DEST_SCHEMA_UNLOADED"
    UNKNOWN = "UNKNOWN"


# Typed engine transforms that must not survive a Widen-to-text remediation.
_TYPED_CAST_TRANSFORMS = frozenset(
    {
        "integer",
        "decimal",
        "boolean",
        "datetime",
        "date",
        "time",
        "timestamp",
        "cast_integer",
        "cast_number",
        "cast_boolean",
        "date_iso",
        "time_iso",
    }
)

_FRACTIONAL_RE = re.compile(r"^-?\d+\.\d+$")


_QUOTED_VALUE_RE = re.compile(r"""['"]([^'"]+)['"]""")


#: Source logicals whose value space carries a fraction the integer target drops.
_FRACTIONAL_LOGICALS = frozenset({"float", "double", "decimal", "numeric", "money"})


def classify_declared_collapse(source_type: str, target_type: str) -> FailureClass:
    """Failure class for a type path that collapses fidelity by declaration.

    A preview sample that happens to round-trip is not evidence the path is
    safe — ``FLOAT → INT`` drops the fraction of every value that has one, and
    the first such row is in the load, not the sample. Naming the class here
    keeps one root cause pointing at one remediation: a fractional loss is
    remedied by widening the numeric carrier, which is a different action from
    the generic collapse (remap / transform / Risk Contract).
    """
    from services.decision_kernel.type_invent import normalize_logical_type

    src_logical = normalize_logical_type(source_type or "")
    tgt_logical = normalize_logical_type(target_type or "")
    if tgt_logical == "integer" and src_logical in _FRACTIONAL_LOGICALS:
        return FailureClass.FRACTIONAL_PRECISION_LOSS
    return FailureClass.FIDELITY_COLLAPSE


def classify_transform_failure(
    reason: str,
    *,
    source_type: str = "",
    target_type: str = "",
    source_value: str = "",
) -> FailureClass:
    """Map a transform/coercion error string (+ types) to a FailureClass."""
    msg = (reason or "").strip().lower()
    raw = (source_value or "").strip()
    # Gate error lines often embed the value: Invalid integer: '94.5'
    if not raw:
        m = _QUOTED_VALUE_RE.search(reason or "")
        if m:
            raw = m.group(1).strip()
    src_l = (source_type or "").strip().lower()
    tgt_l = (target_type or "").strip().lower()

    if any(
        k in msg
        for k in (
            "format-control",
            "replacement character",
            "encoding",
            "zero-width",
            "null byte",
        )
    ):
        return FailureClass.ENCODING_FAILURE
    if "too long" in msg or "length" in msg and "overflow" in msg:
        return FailureClass.LENGTH_OVERFLOW
    if "overflow" in msg:
        return FailureClass.OVERFLOW
    if "underflow" in msg:
        return FailureClass.UNDERFLOW
    # Population-fit / writer: "N value(s) do not fit NUMBER(9,6)" and
    # "decimal does not fit" / "value exceeds NUMBER". Not a cast class —
    # the dest carrier exists and the cell is wider than it.
    if ("does not fit" in msg or "do not fit" in msg or "value exceeds" in msg) and any(
        t in msg or t in tgt_l for t in ("decimal", "number", "numeric", "bignumeric")
    ):
        return FailureClass.OVERFLOW
    if ("does not fit" in msg or "do not fit" in msg or "value exceeds" in msg) and re.search(
        r"\b(var)?char|nvarchar|string|text\b", f"{msg} {tgt_l}"
    ):
        return FailureClass.LENGTH_OVERFLOW
    if ("does not fit" in msg or "do not fit" in msg or "value exceeds" in msg) and re.search(
        r"\b(tiny|small|big|byte)?int(eger)?\b", f"{msg} {tgt_l}"
    ):
        return FailureClass.OVERFLOW
    if any(
        k in msg
        for k in (
            "not in enum",
            "not in set",
            "enum domain",
            "enum ordinal",
            "enum index 0",
            "set domain",
            "interval family",
            "interval wire",
            "year outside",
            "store 0000",
            "coerce to year",
            "not valid base64",
            "0/1 digits",
        )
    ) or (
        ("does not fit" in msg or "do not fit" in msg)
        and re.search(r"\b(enum|set|interval|year)\b", f"{msg} {tgt_l}")
    ):
        return FailureClass.TYPE_CAST_FAILURE
    if (
        "binary length" in msg
        or "bitstring length" in msg
        or (
            ("does not fit" in msg or "do not fit" in msg)
            and re.search(r"\b(var)?binary|varbit|\bbit\b", f"{msg} {tgt_l}")
        )
    ):
        return FailureClass.LENGTH_OVERFLOW
    if "empty value cannot coerce" in msg or (
        "empty" in msg and ("cannot coerce" in msg or "cannot cast" in msg)
    ):
        return FailureClass.EMPTY_VALUE_NOT_NULLABLE
    if "invalid boolean" in msg:
        return FailureClass.INVALID_BOOLEAN
    if any(
        k in msg
        for k in (
            "invalid datetime",
            "invalid timestamp",
            "invalid date",
            "cannot coerce to datetime",
            "cannot coerce to date",
            "cannot parse",
        )
    ) and any(t in msg or t in tgt_l for t in ("date", "time", "timestamp")):
        return FailureClass.INVALID_TIMESTAMP
    if "invalid integer" in msg or (
        "cannot be cast to integer" in msg or "cannot cast to integer" in msg
    ):
        if _FRACTIONAL_RE.match(raw) or (
            ("float" in src_l or "double" in src_l or "decimal" in src_l or "number" in src_l)
            and ("int" in tgt_l)
        ):
            return FailureClass.FRACTIONAL_PRECISION_LOSS
        return FailureClass.INVALID_NUMERIC
    if "invalid decimal" in msg or "invalid number" in msg or "invalid float" in msg:
        return FailureClass.INVALID_NUMERIC
    if "fractional value" in msg:
        return FailureClass.FRACTIONAL_PRECISION_LOSS
    if any(
        k in msg
        for k in (
            "url",
            "email",
            "phone",
            "iban",
            "postal",
            "semantic",
        )
    ):
        return FailureClass.SEMANTIC_TRANSFORM_FAILURE
    if "fidelity" in msg or "lossy" in msg or "collapse" in msg:
        return FailureClass.FIDELITY_COLLAPSE
    if "cannot be cast" in msg or "cannot cast" in msg or "invalid" in msg:
        return FailureClass.TYPE_CAST_FAILURE
    return FailureClass.UNKNOWN


def rank_suggested_target_type(
    *,
    source_type: str,
    target_type: str,
    dest_db: str = "",
    failure_class: FailureClass | str | None = None,
    failure_examples: list[str] | None = None,
) -> str:
    """Rank remediation target types — preserve semantic type before text sink.

    Order (contractual):
      1. Preserve / widen numeric precision (FLOAT→DOUBLE/DECIMAL)
      2. Compatible canonical type for the destination
      3. Explicit user-approved semantic conversion (caller)
      4. Text fallback
      5. Quarantine / reject (not a type — returned empty)
    """
    from services.decision_kernel.type_invent import (
        create_new_mapping_target_type,
        normalize_logical_type,
    )
    from services.type_system import suggest_remap_target

    fc = (
        failure_class
        if isinstance(failure_class, FailureClass)
        else FailureClass(str(failure_class))
        if failure_class
        else None
    )
    examples = list(failure_examples or [])
    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    db = (dest_db or "").strip() or "mysql"

    # Empty → typed: do not suggest LONGTEXT; nullability/quarantine owns the fix.
    if fc is FailureClass.EMPTY_VALUE_NOT_NULLABLE:
        return ""

    # Unread destination type: suggesting a target here would invent the very
    # fact that is missing. The fix is a catalog reload, not a type.
    if fc is FailureClass.DEST_SCHEMA_UNLOADED:
        return ""

    # Fractional → integer: prefer DOUBLE/DECIMAL, never text-first.
    # Do not replace this with a dest NUMBER widen — FLOAT→INT is a
    # different root cause than scale overflow of an existing DECIMAL.
    if fc is FailureClass.FRACTIONAL_PRECISION_LOSS or (
        src_l in {"float", "decimal", "number"}
        and tgt_l == "integer"
        and any(_FRACTIONAL_RE.match((e or "").strip()) for e in examples)
    ):
        return create_new_mapping_target_type("DOUBLE", db) or suggest_remap_target(
            source_type or "DOUBLE", target_type or "INTEGER", dest_db=db
        )

    # Dest NUMBER/DECIMAL cannot hold the cell: dest-spelled widen, never
    # truncate. Auto-ambiguous ``1.234`` has no write-path bind, so the
    # widen helper returns empty and we fall through.
    widened = _decimal_overflow_widen(
        examples, target_type=target_type, dest_db=db
    )
    if widened:
        return widened

    # Integer range overflow: next integer/decimal carrier, never TEXT-first.
    if tgt_l == "integer" or fc is FailureClass.OVERFLOW:
        int_widen = _integer_overflow_widen(
            examples, target_type=target_type, dest_db=db
        )
        if int_widen:
            return int_widen

    # Encoding / length: text widen is appropriate.
    if fc in {FailureClass.ENCODING_FAILURE, FailureClass.LENGTH_OVERFLOW}:
        return create_new_mapping_target_type("TEXT", db) or "TEXT"

    # Default: fidelity-preserving remap SSOT (FLOAT→INT already → DOUBLE).
    return suggest_remap_target(source_type, target_type, dest_db=db)


def _decimal_overflow_widen(
    examples: list[str],
    *,
    target_type: str,
    dest_db: str,
) -> str:
    """Dest-spelled NUMBER/DECIMAL that would hold the first overflowing cell."""
    from connectors.writer_common import fits_decimal, parse_decimal_precision_scale
    from services.decimal_observe import decimal_widen_carrier

    parsed = parse_decimal_precision_scale(target_type, dest_db=dest_db)
    if not parsed:
        return ""
    precision, scale = parsed
    for ex in examples:
        if not (ex or "").strip():
            continue
        if fits_decimal(ex, precision, scale, dest_db=dest_db):
            continue
        widened: str = decimal_widen_carrier(
            ex, dest_db=dest_db, current_type=target_type
        )
        if widened:
            return widened
    return ""


def _integer_overflow_widen(
    examples: list[str],
    *,
    target_type: str,
    dest_db: str,
) -> str:
    """BIGINT / NUMBER(38,0) / DOUBLE that would hold the overflowing cell."""
    from connectors.writer_common import integer_overflow_suggested_type

    for ex in examples:
        if not (ex or "").strip():
            continue
        widened: str = integer_overflow_suggested_type(
            ex, target_type, dest_db=dest_db
        )
        if widened:
            return widened
    return ""


def recommended_action_for_failure(
    failure_class: FailureClass | str,
    *,
    source: str = "",
    suggested_target_type: str = "",
) -> str:
    """Human remediation line keyed by FailureClass (not a single boilerplate)."""
    fc = (
        failure_class
        if isinstance(failure_class, FailureClass)
        else FailureClass(str(failure_class or "UNKNOWN"))
    )
    col = f"'{source}' " if source else ""
    if fc is FailureClass.FRACTIONAL_PRECISION_LOSS:
        tgt = suggested_target_type or "DOUBLE"
        return (
            f"Open Map → widen {col}to {tgt} (preserve numeric semantics) → re-Validate. "
            f"LONGTEXT preserves digits but destroys numeric meaning — prefer last."
        )
    if fc is FailureClass.EMPTY_VALUE_NOT_NULLABLE:
        return (
            f"Open Map → allow NULL / default for {col or 'empty cells'}, or Accept risk "
            f"with QUARANTINE_ROW / CAST_AND_CONTINUE → re-Validate. "
            f"Empty→typed is a nullability policy problem, not a cast widen."
        )
    if fc is FailureClass.SEMANTIC_TRANSFORM_FAILURE:
        return (
            f"Open Map → set transform to none/identity for {col or 'semantic'} carriers, "
            f"or Accept risk with QUARANTINE_ROW / CAST_AND_CONTINUE → re-Validate."
        )
    if fc is FailureClass.ENCODING_FAILURE:
        return "Open Fix bad data → Strip controls / Quarantine unfit cells → re-Validate."
    if fc is FailureClass.DEST_SCHEMA_UNLOADED:
        return (
            f"Open Map → Reload destination schema for {col or 'the destination'}— "
            "no widen or Risk Contract applies until the destination type is read. "
            "If the table does not exist the probe proves it absent and the column "
            "becomes a CREATE."
        )
    if fc is FailureClass.OVERFLOW and suggested_target_type:
        return (
            f"Open Map → widen {col} to {suggested_target_type} (or ALTER the destination) "
            f"→ re-Validate. Map type alone does not ALTER live destination DDL. "
            f"Do not silently truncate."
        )
    if suggested_target_type:
        return (
            f"Open Map → widen {col} to {suggested_target_type} (or remap / ALTER) → re-Validate."
        )
    return (
        "Open Map → fix type/transform or Accept risk with a continue-policy Risk Contract "
        "→ re-Validate."
    )


def typed_cast_incompatible_with_text_sink(transform: str, target_logical: str) -> bool:
    """True when a typed cast must not survive Widen-to-text without CAST contract."""
    raw = (transform or "").strip().lower()
    tgt = (target_logical or "").strip().lower()
    return tgt in {"string", "text"} and raw in _TYPED_CAST_TRANSFORMS


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    """One immutable cell/column finding consumed by every Validate surface."""

    finding_id: str
    failure_class: FailureClass
    source_column: str
    target_column: str
    target_type: str = ""
    source_type: str = ""
    row_number: int | None = None
    source_value_fingerprint: str = ""
    operation: str = ""
    result: str = "FAIL"  # OK | FAIL | HOLDOUT | COALESCE
    failure_message: str = ""
    severity: str = "high"
    lossiness: str = ""
    blocking: bool = True
    policy: str = "BLOCK"
    recommended_action: str = ""
    suggested_target_type: str = ""
    gate_ids: tuple[str, ...] = ()
    root_cause_id: str | None = None
    risk_contract_id: str | None = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["failure_class"] = self.failure_class.value
        d["schema_version"] = FINDING_SCHEMA
        d["gate_ids"] = list(self.gate_ids)
        return {k: v for k, v in d.items() if v not in (None, "", (), [], {})}


def fingerprint_value(value: Any, *, limit: int = 64) -> str:
    """Dest-canonical preview + digest. Reader-null fingerprints as empty.

    ``str(value)`` invented ``True`` / the SQL NULL wire token. ``if value``
    at the call site dropped integer ``0``.
    """
    from services.value_serializer import present_cell_text

    text = present_cell_text(value) or ""
    digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:16]
    preview = text[:limit].replace("\n", " ")
    return f"{digest}:{preview}"


def build_finding(
    *,
    source_column: str,
    target_column: str,
    failure_message: str,
    source_type: str = "",
    target_type: str = "",
    source_value: str = "",
    row_number: int | None = None,
    operation: str = "",
    dest_db: str = "",
    blocking: bool = True,
    gate_ids: tuple[str, ...] | list[str] | None = None,
    failure_class: FailureClass | None = None,
    suggested_target_type: str = "",
) -> ValidationFinding:
    """Construct a canonical finding from a transform/coercion failure."""
    from services.value_serializer import present_cell_text

    present = present_cell_text(source_value)
    fc = failure_class or classify_transform_failure(
        failure_message,
        source_type=source_type,
        target_type=target_type,
        source_value=source_value,
    )
    suggested = (suggested_target_type or "").strip() or rank_suggested_target_type(
        source_type=source_type,
        target_type=target_type,
        dest_db=dest_db,
        failure_class=fc,
        failure_examples=[present] if present is not None else None,
    )
    action = recommended_action_for_failure(
        fc, source=source_column, suggested_target_type=suggested
    )
    fid_src = (
        f"{source_column}|{target_column}|{fc.value}|{row_number}|{failure_message[:80]}"
    )
    finding_id = "vf-" + hashlib.sha256(fid_src.encode()).hexdigest()[:12]
    return ValidationFinding(
        finding_id=finding_id,
        failure_class=fc,
        source_column=source_column,
        target_column=target_column,
        target_type=target_type,
        source_type=source_type,
        row_number=row_number,
        source_value_fingerprint=(
            fingerprint_value(source_value) if present is not None else ""
        ),
        operation=operation,
        result="FAIL",
        failure_message=failure_message,
        severity="high" if blocking else "medium",
        blocking=blocking,
        policy="BLOCK" if blocking else "WARN",
        recommended_action=action,
        suggested_target_type=suggested,
        gate_ids=tuple(gate_ids or ()),
    )


def findings_from_coercion_report(
    report: Mapping[str, Any] | None,
    *,
    dest_db: str = "",
    max_findings: int = 64,
) -> list[dict[str, Any]]:
    """Promote coercion_probe columns into canonical ValidationFinding dicts.

    One finding per blocking/warning column (not per sample cell) so Map /
    Proof / root-cause share a single ranked remediation surface.
    """
    if not isinstance(report, Mapping):
        return []
    columns = report.get("columns")
    if not isinstance(columns, list):
        return []
    out: list[dict[str, Any]] = []
    for col in columns:
        if not isinstance(col, Mapping):
            continue
        severity = str(col.get("severity") or "").lower()
        failed = int(col.get("failed") or 0)
        fidelity = bool(col.get("fidelity_collapse"))
        if severity not in {"block", "warn"} and failed <= 0 and not fidelity:
            continue
        samples = col.get("sample_failures") if isinstance(col.get("sample_failures"), list) else []
        first = samples[0] if samples and isinstance(samples[0], Mapping) else {}
        reason = str(
            (first or {}).get("reason")
            or col.get("suggested_fix")
            or ("fidelity collapse" if fidelity else "coercion failure")
        )
        from services.value_serializer import present_cell_text

        source_value = present_cell_text((first or {}).get("value")) or ""
        row_number = (first or {}).get("row")
        if row_number is not None:
            try:
                row_number = int(row_number)
            except (TypeError, ValueError):
                row_number = None
        fc_raw = str(col.get("failure_class") or "").strip()
        fc: FailureClass | None = None
        if fc_raw:
            try:
                fc = FailureClass(fc_raw)
            except ValueError:
                fc = None
        if fidelity and fc is None:
            fc = classify_declared_collapse(
                str(col.get("source_type") or ""),
                str(col.get("target_type") or ""),
            )
        finding = build_finding(
            source_column=str(col.get("source") or ""),
            target_column=str(col.get("target") or ""),
            failure_message=reason,
            source_type=str(col.get("source_type") or ""),
            target_type=str(col.get("target_type") or col.get("target_logical") or ""),
            source_value=source_value,
            row_number=row_number,
            operation=str(col.get("transform") or ""),
            dest_db=dest_db,
            blocking=severity == "block" or fidelity,
            gate_ids=("g3_coercion",),
            failure_class=fc,
            suggested_target_type=str(col.get("suggested_target_type") or ""),
        )
        out.append(finding.to_dict())
        if len(out) >= max_findings:
            break
    return out


def findings_from_population_fit(
    report: Mapping[str, Any] | None,
    *,
    dest_db: str = "",
    max_findings: int = 64,
) -> list[dict[str, Any]]:
    """Promote ``g3f_population_fit`` overflows into ValidationFinding dicts.

    The 25-row coercion preview never sees row 293. Validate's Remap CTA
    reads ``validation_findings[].suggested_target_type`` — without this
    promotion the dest-spelled widen stays buried under ``population_fit``
    and the button shows ``—``.
    """
    if not isinstance(report, Mapping):
        return []
    findings = report.get("findings")
    if not isinstance(findings, list):
        return []
    out: list[dict[str, Any]] = []
    for col in findings:
        if not isinstance(col, Mapping):
            continue
        try:
            unfit = int(col.get("unfit_rows") or 0)
        except (TypeError, ValueError):
            unfit = 0
        if unfit <= 0:
            continue
        examples = (
            col.get("example_values")
            if isinstance(col.get("example_values"), list)
            else []
        )
        from services.value_serializer import present_cell_text

        source_value = ""
        if examples:
            source_value = present_cell_text(examples[0]) or str(examples[0] or "")
        rows = (
            col.get("example_rows")
            if isinstance(col.get("example_rows"), list)
            else []
        )
        row_number = None
        if rows:
            try:
                row_number = int(rows[0])
            except (TypeError, ValueError):
                row_number = None
        reason = str(
            col.get("reason")
            or col.get("unfit_reason")
            or col.get("suggested_fix")
            or "value does not fit destination NUMBER/DECIMAL"
        )
        suggested = str(col.get("suggested_target_type") or "").strip()
        finding = build_finding(
            source_column=str(col.get("source") or ""),
            target_column=str(col.get("target") or col.get("source") or ""),
            failure_message=reason,
            source_type=str(col.get("source_type") or ""),
            target_type=str(col.get("target_type") or ""),
            source_value=source_value,
            row_number=row_number,
            dest_db=dest_db,
            blocking=True,
            gate_ids=("g3f_population_fit",),
            suggested_target_type=suggested,
        )
        payload = finding.to_dict()
        scan_fix = str(col.get("suggested_fix") or "").strip()
        if scan_fix:
            payload["recommended_action"] = scan_fix
        out.append(payload)
        if len(out) >= max_findings:
            break
    return out


def merge_validation_findings(
    coercion: list[dict[str, Any]] | None,
    population: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """One finding per source+target. Population-fit dest widen wins.

    A preview coercion finding for the same column is a 25-row guess.
    The population scan names the first overflowing row and the dest-spelled
    carrier that would hold it.
    """
    coercion_list = [f for f in (coercion or []) if isinstance(f, Mapping)]
    population_list = [f for f in (population or []) if isinstance(f, Mapping)]

    def _key(finding: Mapping[str, Any]) -> tuple[str, str]:
        return (
            str(finding.get("source_column") or "").strip().lower(),
            str(finding.get("target_column") or "").strip().lower(),
        )

    pop_keys = {_key(f) for f in population_list}
    out = [dict(f) for f in coercion_list if _key(f) not in pop_keys]
    out.extend(dict(f) for f in population_list)
    return out
