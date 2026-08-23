"""Population fit scan — decide bounded-type fit on rows, not on a preview.

Validate screens a preview (25 rows in Studio, up to ``PREFLIGHT_SAMPLE_LIMIT``
at Execute preflight). A destination column with a *bounded* carrier —
``DECIMAL(p,s)`` / ``NUMBER(p,s)``, ``VARCHAR(n)``, a sized integer — can pass
that screen and still meet a value at row 431 that the carrier cannot hold. Up
to now the per-row fit predicates in the write path were the only thing that
ever proved that, which is far too late: the operator sees a Run failure with
zero rows committed for a defect that was decidable before a single row moved.

This module names the columns whose fit is *decidable but unproven*, scans rows
with the **same** writer predicates (``fits_decimal`` / ``fits_varchar`` /
``fits_integer`` — never a second numeric rule set), and reports evidence
honestly.

Asymmetry is deliberate and load-bearing:

* A finding is **proof of failure** — those rows exist and that carrier cannot
  hold them. Whether the job then aborts or quarantines is the Migration Risk
  Contract's decision, resolved here through the same
  ``resolve_write_action_for_mapping`` SSOT the writer uses.
* A clean scan is **not proof of success**. It is scanned-row evidence under the
  declared types; the write image can still differ (a transform, a locale, a
  wider row later in a stream). The write-time checks stay authoritative and
  nothing here relaxes them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

#: Every source row was scanned — a clean column is clean for the population.
EVIDENCE_EXACT = "exact"
#: A bounded scan that stopped early (budget/stream) — silence proves nothing.
EVIDENCE_PARTIAL = "partial"
#: Preview-sized evidence only (Studio's 25 rows).
EVIDENCE_SAMPLED = "sampled"
#: No rows were available to scan at all.
EVIDENCE_UNMEASURED = "unmeasured"

#: Carrier families this scan can decide. Everything else is out of scope and
#: must not be reported as proven either way.
CARRIER_DECIMAL = "decimal"
CARRIER_STRING = "string"
CARRIER_INTEGER = "integer"

#: Writer actions that abort the write unit rather than hold rows out.
_ABORTING_ACTIONS = frozenset(
    {"fail", "stop_table", "abort_transaction", "retry_then_fail"}
)

#: Rows scanned per column before the scan reports PARTIAL. Generous: the
#: engine already holds the batch in memory, so the cost is a Decimal parse.
DEFAULT_SCAN_BUDGET = 5_000_000

#: Offending row numbers / values kept per column for the operator.
DEFAULT_MAX_EXAMPLES = 10

GATE_ID = "g3f_population_fit"


@dataclass(frozen=True)
class BoundedTarget:
    """One mapped column whose destination carrier has a decidable bound."""

    source: str
    target: str
    target_type: str
    carrier: str
    #: Effective writer action for a fit failure on this column, resolved from
    #: the column's Migration Risk Contract and the job error policy.
    write_action: str = "fail"
    execution_policy: str = ""
    risk_id: str = ""

    @property
    def aborts_job(self) -> bool:
        return self.write_action in _ABORTING_ACTIONS

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "target_type": self.target_type,
            "carrier": self.carrier,
            "write_action": self.write_action,
            "execution_policy": self.execution_policy,
            "risk_id": self.risk_id,
            "aborts_job": self.aborts_job,
        }


@dataclass(frozen=True)
class ColumnFitFinding:
    """Values that the destination carrier provably cannot hold."""

    target: BoundedTarget
    unfit_rows: int
    example_rows: tuple[int, ...] = ()
    example_values: tuple[str, ...] = ()
    #: Writer's own words for the first unfit value — a fractional value and an
    #: out-of-range value are different defects with different remediations.
    unfit_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.target.to_dict(),
            "unfit_rows": self.unfit_rows,
            "example_rows": list(self.example_rows),
            "example_values": list(self.example_values),
            "unfit_reason": self.unfit_reason,
            "reason": self.reason(),
        }

    def reason(self) -> str:
        t = self.target
        rows = ", ".join(str(r) for r in self.example_rows[:3])
        where = f" (first at row {rows})" if rows else ""
        why = f" — {self.unfit_reason}" if self.unfit_reason else ""
        return (
            f"{self.unfit_rows} value(s) in '{t.source}' do not fit "
            f"{t.target}{' ' if t.target_type else ''}{t.target_type}{where}{why}"
        )


@dataclass(frozen=True)
class FitScanReport:
    """What the scan looked at, and what it proved."""

    evidence: str = EVIDENCE_UNMEASURED
    rows_scanned: int = 0
    rows_total: int = 0
    targets: tuple[BoundedTarget, ...] = ()
    findings: tuple[ColumnFitFinding, ...] = ()
    note: str = ""
    #: Columns skipped because their carrier is not decidable here.
    undecidable: tuple[str, ...] = field(default=())
    #: Columns whose declared source type cannot exceed the destination carrier,
    #: so no value scan is needed to decide them.
    safe_by_declaration: tuple[str, ...] = field(default=())

    @property
    def scanned_population(self) -> bool:
        return self.evidence == EVIDENCE_EXACT

    @property
    def unfit_rows(self) -> int:
        """Upper bound on affected rows (a row may fail in several columns)."""
        return sum(f.unfit_rows for f in self.findings)

    @property
    def aborting_findings(self) -> tuple[ColumnFitFinding, ...]:
        return tuple(f for f in self.findings if f.target.aborts_job)

    @property
    def held_out_findings(self) -> tuple[ColumnFitFinding, ...]:
        return tuple(f for f in self.findings if not f.target.aborts_job)

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence": self.evidence,
            "rows_scanned": self.rows_scanned,
            "rows_total": self.rows_total,
            "scanned_population": self.scanned_population,
            "bounded_columns": [t.to_dict() for t in self.targets],
            "findings": [f.to_dict() for f in self.findings],
            "unfit_rows": self.unfit_rows,
            "undecidable_columns": list(self.undecidable),
            "safe_by_declaration": list(self.safe_by_declaration),
            "note": self.note,
        }


def _resolve_action(mapping: Any, job_error_policy: str) -> tuple[str, str, str]:
    from services.migration_risk_contract import resolve_write_action_for_mapping

    action, exec_pol, risk_id = resolve_write_action_for_mapping(
        mapping, job_error_policy or "quarantine"
    )
    return str(action or "fail"), str(exec_pol or ""), str(risk_id or "")


def _carrier_for(target_type: str, *, dest_db: str) -> str:
    """Name the bounded carrier family, or "" when the bound is undecidable."""
    from connectors.writer_common import parse_decimal_precision_scale
    from services.ddl_compatibility import parse_varchar_width
    from services.type_system import integer_storage_bounds

    typ = str(target_type or "").strip()
    if not typ:
        return ""
    if parse_decimal_precision_scale(typ, dest_db=dest_db):
        return CARRIER_DECIMAL
    if parse_varchar_width(typ) is not None:
        return CARRIER_STRING
    if integer_storage_bounds(typ, dest_db=dest_db) is not None:
        return CARRIER_INTEGER
    return ""


def _source_cannot_exceed(
    source_type: str,
    target: BoundedTarget,
    *,
    dest_db: str,
) -> bool:
    """True when the declared source type provably fits the destination carrier.

    A widening or identical path (``NUMBER(11,8) → NUMBER(11,8)``,
    ``VARCHAR(50) → VARCHAR(200)``) needs no value scan: the declaration already
    decides it. Anything narrowing, unbounded, or unparseable stays in scope —
    unknown never means safe here.
    """
    from connectors.writer_common import parse_decimal_precision_scale
    from services.ddl_compatibility import parse_varchar_width
    from services.type_system import integer_storage_bounds

    src = str(source_type or "").strip()
    if not src:
        return False

    if target.carrier == CARRIER_DECIMAL:
        src_parsed = parse_decimal_precision_scale(src, dest_db=dest_db)
        tgt_parsed = parse_decimal_precision_scale(target.target_type, dest_db=dest_db)
        if not src_parsed or not tgt_parsed:
            return False
        src_p, src_s = src_parsed
        tgt_p, tgt_s = tgt_parsed
        return src_s <= tgt_s and (src_p - src_s) <= (tgt_p - tgt_s)

    if target.carrier == CARRIER_STRING:
        src_width = parse_varchar_width(src)
        tgt_width = parse_varchar_width(target.target_type)
        if src_width is None or tgt_width is None:
            return False
        return src_width <= tgt_width

    if target.carrier == CARRIER_INTEGER:
        src_bounds = integer_storage_bounds(src, dest_db=dest_db)
        tgt_bounds = integer_storage_bounds(target.target_type, dest_db=dest_db)
        if not src_bounds or not tgt_bounds:
            return False
        return bool(
            src_bounds[0] >= tgt_bounds[0] and src_bounds[1] <= tgt_bounds[1]
        )

    return False


def bounded_targets(
    mappings: Iterable[Any] | None,
    *,
    dest_types: Mapping[str, str] | None = None,
    source_types: Mapping[str, str] | None = None,
    dest_db: str = "",
    job_error_policy: str = "",
) -> tuple[tuple[BoundedTarget, ...], tuple[str, ...], tuple[str, ...]]:
    """Mapped columns whose destination carrier has a decidable bound *and* a
    source declaration that could exceed it.

    Returns ``(targets, undecidable, safe_by_declaration)``. ``undecidable``
    names mapped columns with a target type this scan cannot bound — they are
    reported, never silently treated as safe. ``safe_by_declaration`` names the
    widening/identical paths that need no value scan at all, which is what keeps
    this off the cost path of an ordinary transfer.
    """
    types = {str(k): str(v or "") for k, v in (dest_types or {}).items()}
    lowered = {k.lower(): v for k, v in types.items()}
    src_types = {str(k): str(v or "") for k, v in (source_types or {}).items()}
    src_lowered = {k.lower(): v for k, v in src_types.items()}
    out: list[BoundedTarget] = []
    undecidable: list[str] = []
    safe: list[str] = []
    for m in mappings or []:
        if not isinstance(m, Mapping):
            continue
        source = str(m.get("source") or "")
        target = str(m.get("target") or "")
        if not source or not target:
            continue
        if m.get("intentional_omit"):
            continue
        declared = str(m.get("target_type") or m.get("dest_type") or "").strip()
        target_type = declared or types.get(target) or lowered.get(target.lower(), "")
        carrier = _carrier_for(target_type, dest_db=dest_db)
        if not carrier:
            if target_type:
                undecidable.append(target)
            continue
        action, exec_pol, risk_id = _resolve_action(m, job_error_policy)
        candidate = BoundedTarget(
            source=source,
            target=target,
            target_type=target_type,
            carrier=carrier,
            write_action=action,
            execution_policy=exec_pol,
            risk_id=risk_id,
        )
        declared_source = (
            str(m.get("source_type") or "").strip()
            or src_types.get(source)
            or src_lowered.get(source.lower(), "")
        )
        if _source_cannot_exceed(declared_source, candidate, dest_db=dest_db):
            safe.append(target)
            continue
        out.append(candidate)
    return (
        tuple(out),
        tuple(dict.fromkeys(undecidable)),
        tuple(dict.fromkeys(safe)),
    )


def _is_fractional(value: Any) -> bool:
    """True when the value carries a fraction a zero-scale carrier cannot hold."""
    if isinstance(value, bool):
        return False
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return False
    return bool(dec.is_finite() and dec != dec.to_integral_value())


def _fit_predicate(
    target: BoundedTarget,
    *,
    dest_db: str,
    dialect_label: str,
) -> Callable[[Any], str | None]:
    """Bind one column's writer predicate once, then reuse it per value.

    A million-row scan must not re-parse ``NUMBER(11,8)`` a million times; the
    predicate itself is still the write path's, never a second numeric rule set.
    Returns the writer's reason for an unfit value, or ``None`` when it fits.
    """
    from connectors.writer_common import (
        fits_decimal,
        fits_varchar,
        integer_fit_failure,
        parse_decimal_precision_scale,
    )
    from services.ddl_compatibility import parse_varchar_width

    if target.carrier == CARRIER_DECIMAL:
        parsed = parse_decimal_precision_scale(target.target_type, dest_db=dest_db)
        if not parsed:
            return lambda _value: None
        precision, scale = parsed
        decimal_type = target.target_type

        def _decimal_reason(value: Any) -> str | None:
            if fits_decimal(value, precision, scale, dest_db=dest_db):
                return None
            if scale == 0 and _is_fractional(value):
                # NUMBER(38,0) / NUMBER(10,0) are integer carriers wearing a
                # decimal spelling — name the defect the same way MySQL's INT
                # does, so one remediation covers every dialect.
                return (
                    f"fractional value {str(value).strip()} is not an integer for "
                    f"{decimal_type} — widen the destination to a scaled "
                    "DECIMAL/DOUBLE, or round it explicitly before the write"
                )
            return f"value exceeds {decimal_type}"

        return _decimal_reason
    if target.carrier == CARRIER_STRING:
        width = parse_varchar_width(target.target_type)
        if width is None:
            return lambda _value: None
        type_str = target.target_type
        label = dialect_label or dest_db
        return lambda value: (
            None
            if fits_varchar(value, width, type_str, dialect_label=label)
            else f"value is longer than {type_str}"
        )
    if target.carrier == CARRIER_INTEGER:
        type_str = target.target_type
        return lambda value: integer_fit_failure(value, type_str, dest_db=dest_db)
    return lambda _value: None


def _skip_value(value: Any, is_missing_sentinel: Callable[[Any], bool]) -> bool:
    """NULL-ish cells are a nullability question, not a fit question."""
    if value is None:
        return True
    if is_missing_sentinel(value):
        return True
    return isinstance(value, str) and not value.strip()


def scan_rows(
    rows: Iterable[Mapping[str, Any]] | None,
    targets: Iterable[BoundedTarget],
    *,
    dest_db: str = "",
    dialect_label: str = "",
    rows_total: int = 0,
    rows_are_population: bool = False,
    budget: int = DEFAULT_SCAN_BUDGET,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
    undecidable: Iterable[str] = (),
    safe_by_declaration: Iterable[str] = (),
) -> FitScanReport:
    """Scan ``rows`` against the writer's own fit predicates.

    ``rows_are_population`` is the caller's claim that ``rows`` is every source
    row (the file/batch it is about to write, not a preview). The scan downgrades
    that claim to PARTIAL by itself if the budget cuts the walk short — evidence
    is never stronger than the walk that produced it.
    """
    bounded = tuple(targets)
    if not bounded:
        return FitScanReport(
            evidence=EVIDENCE_UNMEASURED,
            rows_total=int(rows_total or 0),
            undecidable=tuple(undecidable),
            safe_by_declaration=tuple(safe_by_declaration),
            note=(
                "No mapped column can exceed its destination carrier by "
                "declaration — no value scan needed."
            ),
        )

    counts: dict[int, int] = {}
    example_rows: dict[int, list[int]] = {}
    example_values: dict[int, list[str]] = {}
    reasons: dict[int, str] = {}
    scanned = 0
    truncated = False

    from services.value_serializer import cell_to_string, is_missing_sentinel

    # Bound once per column, then applied per value — see _fit_predicate.
    probes = tuple(
        (idx, t.source, _fit_predicate(t, dest_db=dest_db, dialect_label=dialect_label))
        for idx, t in enumerate(bounded)
    )

    for row in rows or ():
        if scanned >= budget:
            truncated = True
            break
        scanned += 1
        if not isinstance(row, Mapping):
            continue
        for idx, source, fit_reason in probes:
            if source not in row:
                continue
            value = row.get(source)
            if _skip_value(value, is_missing_sentinel):
                continue
            why = fit_reason(value)
            if why is None:
                continue
            counts[idx] = counts.get(idx, 0) + 1
            reasons.setdefault(idx, why)
            if len(example_rows.setdefault(idx, [])) < max_examples:
                example_rows[idx].append(scanned)
                example_values.setdefault(idx, []).append(cell_to_string(value)[:120])

    if scanned == 0:
        evidence = EVIDENCE_UNMEASURED
    elif rows_are_population and not truncated:
        evidence = EVIDENCE_EXACT
    elif truncated:
        evidence = EVIDENCE_PARTIAL
    else:
        evidence = EVIDENCE_SAMPLED

    findings = tuple(
        ColumnFitFinding(
            target=bounded[idx],
            unfit_rows=counts[idx],
            example_rows=tuple(example_rows.get(idx, ())),
            example_values=tuple(example_values.get(idx, ())),
            unfit_reason=reasons.get(idx, ""),
        )
        for idx in sorted(counts)
    )
    total = int(rows_total or 0) or (scanned if evidence == EVIDENCE_EXACT else 0)
    return FitScanReport(
        evidence=evidence,
        rows_scanned=scanned,
        rows_total=total,
        targets=bounded,
        findings=findings,
        undecidable=tuple(undecidable),
        safe_by_declaration=tuple(safe_by_declaration),
        note=(
            "Scanned every source row with the write path's own fit predicates."
            if evidence == EVIDENCE_EXACT
            else (
                "Scan stopped at the row budget — unscanned rows are unproven."
                if evidence == EVIDENCE_PARTIAL
                else "Preview-sized evidence only — population fit is unproven."
            )
        ),
    )


def scan_population_fit(
    rows: Iterable[Mapping[str, Any]] | None,
    mappings: Iterable[Any] | None,
    *,
    dest_types: Mapping[str, str] | None = None,
    source_types: Mapping[str, str] | None = None,
    dest_db: str = "",
    dialect_label: str = "",
    job_error_policy: str = "",
    rows_total: int = 0,
    rows_are_population: bool = False,
    budget: int = DEFAULT_SCAN_BUDGET,
) -> FitScanReport:
    """Resolve bounded targets from the mappings, then scan the rows."""
    targets, undecidable, safe = bounded_targets(
        mappings,
        dest_types=dest_types,
        source_types=source_types,
        dest_db=dest_db,
        job_error_policy=job_error_policy,
    )
    return scan_rows(
        rows,
        targets,
        dest_db=dest_db,
        dialect_label=dialect_label,
        rows_total=rows_total,
        rows_are_population=rows_are_population,
        budget=budget,
        undecidable=undecidable,
        safe_by_declaration=safe,
    )


def build_population_fit_gate(report: FitScanReport) -> dict[str, Any]:
    """Gate row for the Validate panel.

    * Findings whose column would abort the write → BLOCK. Those rows exist and
      the resolved policy for that column is fail-closed, so Execute would end
      with zero rows committed. Naming that here is the whole point.
    * Findings under a continue-policy contract → WARN with the row count that
      will be held out, so "27 rows quarantined" is a forecast, not a surprise.
    * No findings → PASS, worded to the evidence: population-clean only when
      every row was scanned.
    """
    aborting = report.aborting_findings
    held_out = report.held_out_findings
    details: dict[str, Any] = report.to_dict()

    if not report.targets:
        safe = len(report.safe_by_declaration)
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                "No mapped column can exceed its destination carrier by "
                f"declaration ({safe} widening/identical bounded path(s)) — "
                "no value scan required"
            ),
            "duration_ms": 0,
            "details": details,
        }

    if aborting:
        cols = ", ".join(f"{f.target.source} → {f.target.target_type}" for f in aborting)
        rows = sum(f.unfit_rows for f in aborting)
        scope = (
            f"{report.rows_scanned} scanned row(s)"
            if not report.scanned_population
            else f"all {report.rows_scanned} source row(s)"
        )
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"{rows} value(s) in {scope} cannot fit the destination carrier "
                f"({cols}); the resolved write policy for those column(s) aborts "
                "the load, so Execute would commit nothing"
            ),
            "duration_ms": 0,
            "details": {
                **details,
                "corrective_action": (
                    "Widen the destination column, or sign a continue-policy "
                    "Migration Risk Contract so the offending rows are held out "
                    "in quarantine instead of failing the load."
                ),
            },
        }

    if held_out:
        rows = sum(f.unfit_rows for f in held_out)
        cols = ", ".join(f"{f.target.source} → {f.target.target_type}" for f in held_out)
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                f"{rows} row(s) will be held out in quarantine on write — "
                f"{cols} exceeds the destination carrier under a signed "
                "continue-policy contract"
            ),
            "duration_ms": 0,
            "details": details,
        }

    if report.evidence == EVIDENCE_UNMEASURED:
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                "Population fit unmeasured — no rows were scanned, so bounded "
                "destination carriers are unproven until write"
            ),
            "duration_ms": 0,
            "details": details,
        }

    if report.evidence == EVIDENCE_PARTIAL:
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                f"No unfit value in {report.rows_scanned} of "
                f"{report.rows_total or report.rows_scanned} source row(s), but the "
                "scan stopped at the row budget — the remaining rows are unproven "
                f"for {len(report.targets)} bounded column(s)"
            ),
            "duration_ms": 0,
            "details": details,
        }

    if report.scanned_population:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"Every value in {report.rows_scanned} source row(s) fits its "
                f"destination carrier ({len(report.targets)} bounded column(s))"
            ),
            "duration_ms": 0,
            "details": details,
        }

    return {
        "id": GATE_ID,
        "status": "warn",
        "message": (
            f"No unfit value in {report.rows_scanned} scanned row(s), but "
            f"{len(report.targets)} bounded column(s) are not population-proven — "
            "a later row may still exceed the carrier"
        ),
        "duration_ms": 0,
        "details": details,
    }
