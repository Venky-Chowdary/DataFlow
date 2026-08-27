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
``fits_integer`` / ``coerce_enum_wire`` / ``is_interval_wire`` /
``coerce_year_wire`` / ``fits_binary`` / ``coerce_bitstring_wire`` — never a
second numeric or domain rule set), and reports evidence honestly.

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

import time
from collections.abc import Callable
from dataclasses import dataclass, field
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
#: Not a width bound — a *parse* bound. ``'2024-02-31' → DATE``,
#: ``'maybe' → BOOLEAN``, ``'nope' → CHAR(36)`` have no size a bound could
#: measure, yet the write refuses every one of them. The scan decides them on
#: the write's own coercion (``apply_transform``), so Validate speaks for them
#: instead of leaving them to fail at row 1.
CARRIER_TYPED = "typed"
#: Closed ENUM/SET membership. Not a width — a *domain* bound. MySQL
#: non-strict ENUM stores an invalid label as ``''`` (silent wipe). The
#: write already refuses via ``coerce_enum_wire`` / ``coerce_set_wire``.
CARRIER_DOMAIN = "domain"
#: Bounded BIT(n) / VARBIT(n) / BINARY(n) / VARBINARY(n). Silent truncate
#: and UTF-8 invent into BYTEA are the write refuses this scan names first.
CARRIER_BYTES = "bytes"

#: Transforms whose parse is cheap, deterministic and row-local enough to run
#: over a population. JSON/vector/binary payloads are deliberately excluded:
#: their cost is unbounded in the cell size and other probes own them.
_SCANNABLE_TRANSFORMS = frozenset(
    {
        "integer",
        "decimal",
        "currency",
        "percentage",
        "boolean",
        "date",
        "datetime",
        "time",
        "uuid",
    }
)

#: Writer actions that abort the write unit rather than hold rows out.
_ABORTING_ACTIONS = frozenset(
    {"fail", "stop_table", "abort_transaction", "retry_then_fail"}
)

#: Rows scanned per column before the scan reports PARTIAL. Generous: the
#: engine already holds the batch in memory, so the cost is a Decimal parse.
DEFAULT_SCAN_BUDGET = 5_000_000

# Interactive Studio Validate. A 1M CSV on a small replica sat in
# GET /preflight?run_id=… for 5+ minutes while the UI looped fake G1–G9
# stages. Remaining rows stay unproven (PARTIAL) — write-time checks still
# bind. 12s is the operator's last measured 1M sample-Validate wall clock.
STUDIO_FIT_SCAN_SECONDS = 12.0
_PROGRESS_EVERY_ROWS = 4_000
#: After the first widenable overflow, finish the numeric/string envelope so
#: Apply does not offer a type that a later scanned-prefix sibling still
#: refuses. Does not run when the prefix found nothing (PARTIAL stays honest).
ENVELOPE_CONTINUE_SECONDS = 20.0
_WIDENABLE_CARRIERS = frozenset(
    {CARRIER_DECIMAL, CARRIER_INTEGER, CARRIER_STRING, CARRIER_BYTES}
)

#: Offending row numbers / values kept per column for the operator.
DEFAULT_MAX_EXAMPLES = 10
_MAX_PROVE_WITNESSES = 32

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
    #: Engine transform the write resolves for this column (typed carriers).
    transform: str = ""
    #: True when ``target_type`` is live destination DDL, not a Map remap.
    #: Map type alone does not ALTER Snowflake/MySQL/PG — Execute binds this.
    binds_live_ddl: bool = False

    @property
    def aborts_job(self) -> bool:
        return self.write_action in _ABORTING_ACTIONS

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "target_type": self.target_type,
            "carrier": self.carrier,
            "transform": self.transform,
            "write_action": self.write_action,
            "execution_policy": self.execution_policy,
            "risk_id": self.risk_id,
            "aborts_job": self.aborts_job,
            "binds_live_ddl": self.binds_live_ddl,
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
    suggested_target_type: str = ""
    suggested_fix: str = ""
    #: True when ``suggested_target_type`` was re-checked with the write
    #: predicate on every overflow witness. Empty suggestion is fail-closed.
    apply_proven: bool = False
    #: ``file`` = envelope walk finished; ``scanned`` = proven on prefix only.
    apply_proven_scope: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.target.to_dict(),
            "unfit_rows": self.unfit_rows,
            "example_rows": list(self.example_rows),
            "example_values": list(self.example_values),
            "unfit_reason": self.unfit_reason,
            "suggested_target_type": self.suggested_target_type,
            "suggested_fix": self.suggested_fix,
            "apply_proven": self.apply_proven,
            "apply_proven_scope": self.apply_proven_scope,
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
    #: Why a population walk stopped early: ``row`` or ``time``. Empty when
    #: the walk finished or never started.
    truncated_reason: str = ""
    #: Wall-clock of this walk. Studio hero must not sum other gates as 10ms
    #: after a multi-second 1M scan.
    duration_ms: int = 0
    #: Extra rows walked only to finish a widen envelope after the first hit.
    envelope_rows_scanned: int = 0
    #: True when that cheap continuation exhausted the iterator.
    envelope_complete: bool = False

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
            "truncated_reason": self.truncated_reason,
            "duration_ms": self.duration_ms,
            "envelope_rows_scanned": self.envelope_rows_scanned,
            "envelope_complete": self.envelope_complete,
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
    # DATE / DATETIME / TIME have no width, but they have a parse bound — the
    # write's ``coerce_sql_temporal``. Leaving them undecidable is how
    # ``2024-02-31`` passed Validate and died at the driver.
    from connectors.sql_temporal import sql_type_is_temporal

    if sql_type_is_temporal(typ):
        return CARRIER_TYPED
    from services.type_system import (
        interval_family,
        is_bitstring_carrier,
        is_year_carrier,
        normalize_logical_type,
        parse_binary_carrier_width,
        parse_bitstring_width,
        parse_enum_or_set_ordered_members,
    )

    if normalize_logical_type(typ) in {"boolean", "uuid"}:
        return CARRIER_TYPED
    parsed_domain = parse_enum_or_set_ordered_members(typ)
    if parsed_domain and parsed_domain[1]:
        return CARRIER_DOMAIN
    if interval_family(typ) or normalize_logical_type(typ) == "interval":
        return CARRIER_TYPED
    if is_year_carrier(typ):
        return CARRIER_TYPED
    if is_bitstring_carrier(typ) and parse_bitstring_width(typ) is not None:
        return CARRIER_BYTES
    if parse_binary_carrier_width(typ) is not None:
        return CARRIER_BYTES
    return ""


def _calendar_parse_required(transform: str, target_type: str) -> bool:
    """True when a type name cannot vouch that the cell is a real calendar value.

    A DECIMAL wire parses by construction. A column *named* DATE does not:
    files infer DATE from a sample, and MySQL can hold zero / invalid dates.
    Those cells must meet the write's temporal bind, not the declaration.
    """
    if transform in {"date", "datetime", "time"}:
        return True
    from connectors.sql_temporal import sql_type_is_temporal

    return sql_type_is_temporal(target_type)


def _is_interval_carrier(type_str: str) -> bool:
    from services.type_system import interval_family, normalize_logical_type

    typ = str(type_str or "").strip()
    if not typ:
        return False
    if interval_family(typ):
        return True
    return normalize_logical_type(typ) == "interval"


def _interval_parse_required(target_type: str, source_type: str) -> bool:
    """True when dest INTERVAL cannot be vouched by a non-interval source.

    Warehouse INTERVAL→INTERVAL wires parse by construction. VARCHAR→INTERVAL
    still holds ``not-an-interval`` past the preview — same class as
    ``maybe`` → BOOLEAN.
    """
    if not _is_interval_carrier(target_type):
        return False
    from services.type_system import normalize_logical_type

    return normalize_logical_type(source_type) != "interval"


def _is_year_carrier(type_str: str) -> bool:
    from services.type_system import is_year_carrier

    return is_year_carrier(type_str)


def _year_parse_required(target_type: str, source_type: str) -> bool:
    """True when dest YEAR cannot be vouched by a non-YEAR source.

    Warehouse YEAR→YEAR wires expand by construction. VARCHAR→YEAR still
    holds ``1899`` past the preview — non-strict MySQL would store 0000.
    """
    if not _is_year_carrier(target_type):
        return False
    return not _is_year_carrier(source_type)


def _parse_in_doubt(source_type: str) -> bool:
    """True when the source declaration cannot vouch for the value's parse.

    A DB column declared ``DECIMAL(12,9)`` hands the write a numeric wire, so a
    decimal parse over it proves nothing a million times over. Text, unknown and
    undeclared sources are where ``'ABC-1' → INT`` lives.
    """
    from services.type_system import normalize_logical_type

    declared = str(source_type or "").strip()
    if not declared:
        return True
    return normalize_logical_type(declared) in {"string", "text", "unknown"}


def _scannable_transform(
    mapping: Mapping[str, Any],
    *,
    source_types: Mapping[str, str],
    dest_types: Mapping[str, str],
) -> str:
    """The write's own transform for this column, when its parse is decidable.

    Resolved through ``resolve_transform`` — the same SSOT ``build_mapped_rows``
    binds through — so Validate never screens a column with a rule the write
    does not use. A native path resolves to ``none`` and costs nothing.
    """
    from services.transform_resolver import resolve_transform

    resolved = str(
        resolve_transform(
            dict(mapping),
            column_types=dict(source_types),
            dest_types=dict(dest_types),
        )
        or ""
    ).strip().lower()
    return resolved if resolved in _SCANNABLE_TRANSFORMS else ""


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

    if target.carrier == CARRIER_DOMAIN:
        from services.type_system import enum_set_domain_would_reject

        # Same membership rule the Map gate uses — dest members must cover
        # every declared source member. Open VARCHAR → ENUM is never safe.
        return not enum_set_domain_would_reject(src, target.target_type)

    if target.carrier == CARRIER_BYTES:
        from services.type_system import (
            bitstring_width_would_narrow,
            is_bitstring_carrier,
            parse_binary_carrier_width,
        )

        if is_bitstring_carrier(target.target_type):
            if not is_bitstring_carrier(src):
                return False
            return not bitstring_width_would_narrow(src, target.target_type)
        src_w = parse_binary_carrier_width(src)
        tgt_w = parse_binary_carrier_width(target.target_type)
        if src_w is None or tgt_w is None:
            return False
        return src_w <= tgt_w

    return False


def _write_bind_target_type(
    declared: str,
    live: str,
    *,
    sync_mode: str = "",
    dest_table_exists: bool | None = None,
) -> tuple[str, bool]:
    """Carrier the write will bind — live DDL wins on an existing object.

    Remapping ``target_type`` to ``NUMBER(10,7)`` while Snowflake still holds
    ``NUMBER(9,6)`` used to green Validate and fail Execute on the same cells.
    That is the 'errors every Run' loop. Overwrite drops and recreates, so
    mapping / create-new types win there. ``dest_table_exists is False`` is
    create-new even when a projected schema is sitting in ``dest_types``.
    """
    from services.sync_cursor import is_overwrite_sync

    declared = str(declared or "").strip()
    live = str(live or "").strip()
    if live and not is_overwrite_sync(sync_mode) and dest_table_exists is not False:
        return live, True
    return declared or live, False


def _live_ddl_fix_suffix(*, binds_live_ddl: bool) -> str:
    if not binds_live_ddl:
        return ""
    return (
        " Map type alone does not ALTER live destination DDL — ALTER the "
        "column or map to a new *_wide column, then start a new transfer. "
        "Do not Resume the failed job."
    )


def bounded_targets(
    mappings: Iterable[Any] | None,
    *,
    dest_types: Mapping[str, str] | None = None,
    source_types: Mapping[str, str] | None = None,
    dest_db: str = "",
    job_error_policy: str = "",
    source_kind: str = "",
    source_format: str = "",
    sync_mode: str = "",
    dest_table_exists: bool | None = None,
) -> tuple[tuple[BoundedTarget, ...], tuple[str, ...], tuple[str, ...]]:
    """Mapped columns whose destination carrier has a decidable bound *and* a
    source declaration that could exceed it.

    Returns ``(targets, undecidable, safe_by_declaration)``. ``undecidable``
    names mapped columns with a target type this scan cannot bound — they are
    reported, never silently treated as safe. ``safe_by_declaration`` names the
    widening/identical paths that need no value scan at all, which is what keeps
    this off the cost path of an ordinary transfer.

    File / upload / object-store types are inferred from a peek sample, not
    declared DDL. Treating ``DECIMAL(9,6)`` from 25 CSV rows as a warehouse
    domain skipped the 1M-row scan and let Execute fail at write on
    ``7.9166665`` → Snowflake ``NUMBER(9,6)``. Untyped sources never skip.
    """
    from services.data_profiler import source_types_are_authoritative

    types = {str(k): str(v or "") for k, v in (dest_types or {}).items()}
    lowered = {k.lower(): v for k, v in types.items()}
    src_types = {str(k): str(v or "") for k, v in (source_types or {}).items()}
    src_lowered = {k.lower(): v for k, v in src_types.items()}
    # Empty source_kind is not warehouse DDL. Treating it as a declared
    # domain skipped the flights CSV scan whenever a caller omitted kind.
    declared_domain = source_types_are_authoritative(source_kind, source_format)
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
        live = types.get(target) or lowered.get(target.lower(), "")
        target_type, binds_live_ddl = _write_bind_target_type(
            declared,
            live,
            sync_mode=sync_mode,
            dest_table_exists=dest_table_exists,
        )
        carrier = _carrier_for(target_type, dest_db=dest_db)
        transform = _scannable_transform(m, source_types=src_types, dest_types=types)
        if not carrier:
            if not transform:
                if target_type:
                    undecidable.append(target)
                continue
            carrier = CARRIER_TYPED
        declared_source = (
            str(m.get("source_type") or "").strip()
            or src_types.get(source)
            or src_lowered.get(source.lower(), "")
        )
        parse_in_doubt = bool(transform) and _parse_in_doubt(declared_source)
        calendar = _calendar_parse_required(transform, target_type)
        interval = _interval_parse_required(target_type, declared_source)
        year = _year_parse_required(target_type, declared_source)
        if (
            carrier == CARRIER_TYPED
            and declared_domain
            and not parse_in_doubt
            and not calendar
            and not interval
            and not year
        ):
            # Warehouse BOOLEAN/UUID/INTERVAL/YEAR wires parse by construction.
            # File peek inferred BOOLEAN is not a domain — ``maybe`` still
            # lives past the sample. Calendar types never skip:
            # ``2024-02-31`` is DATE. VARCHAR→INTERVAL/YEAR still scans.
            safe.append(target)
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
            transform=transform,
            binds_live_ddl=binds_live_ddl,
        )
        if carrier == CARRIER_TYPED:
            # A parse bound has no width to compare declarations against: a
            # DATE-declared source column of text still holds '2024-02-31'.
            out.append(candidate)
            continue
        if (
            declared_domain
            and not parse_in_doubt
            and _source_cannot_exceed(
                declared_source, candidate, dest_db=dest_db
            )
        ):
            # Width is decided by declaration; a parse is only decided with it
            # when the source declares the same typed carrier. Text into a
            # typed column still holds 'ABC-1' at any width.
            safe.append(target)
            continue
        out.append(candidate)
    return (
        tuple(out),
        tuple(dict.fromkeys(undecidable)),
        tuple(dict.fromkeys(safe)),
    )


def _is_fractional(value: Any) -> bool:
    """True when the write path binds a fraction a zero-scale carrier cannot hold."""
    from services.transform_engine import is_fractional_wire_value

    return is_fractional_wire_value(value)


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
    if target.carrier == CARRIER_DOMAIN:
        from connectors.sql_bind import coerce_enum_wire, coerce_set_wire
        from services.type_system import parse_enum_or_set_ordered_members

        parsed = parse_enum_or_set_ordered_members(target.target_type)
        if not parsed or not parsed[1]:
            return lambda _value: None
        kind, _members = parsed
        type_str = target.target_type

        def _domain_reason(value: Any) -> str | None:
            try:
                if kind == "ENUM":
                    coerce_enum_wire(value, ddl_type=type_str)
                else:
                    coerce_set_wire(value, ddl_type=type_str)
            except ValueError as exc:
                return str(exc) or f"value not in {kind} domain"
            return None

        return _domain_reason
    if target.carrier == CARRIER_BYTES:
        from connectors.sql_bind import coerce_bitstring_wire
        from connectors.writer_common import binary_storage_bytes, fits_binary
        from services.type_system import (
            is_bitstring_carrier,
            is_varying_bitstring_carrier,
            parse_binary_carrier_width,
            parse_bitstring_width,
        )

        type_str = target.target_type
        if is_bitstring_carrier(type_str):
            width = parse_bitstring_width(type_str)
            varying = is_varying_bitstring_carrier(type_str)

            def _bit_reason(value: Any) -> str | None:
                try:
                    coerce_bitstring_wire(value, width=width, varying=varying)
                except ValueError as exc:
                    return str(exc)
                return None

            return _bit_reason
        width = parse_binary_carrier_width(type_str)
        if width is None:
            return lambda _value: None

        def _bin_reason(value: Any) -> str | None:
            raw = binary_storage_bytes(value)
            if raw is None:
                return (
                    f"binary wire is not valid base64 for {type_str} "
                    "— refuse silent UTF-8 encode"
                )
            if fits_binary(value, width):
                return None
            return f"binary length {len(raw)} exceeds {type_str}"

        return _bin_reason
    return lambda _value: None


def _temporal_bind_reason(value: Any, target_type: str, dest_db: str) -> str | None:
    """Ask the write's temporal bind, not a second calendar.

    ``coerce_sql_temporal`` is what the SQL writers call. When it cannot parse
    it currently *returns the original string* (passthrough). The driver then
    accepts or rejects per dialect — PostgreSQL errors, MySQL may store a zero
    date. Validate must not copy that passthrough: a cell the parser could not
    bind is unfit, named the same way ``apply_transform(..., "date")`` names it.
    Native ``date`` / ``datetime`` / ``time`` objects already bound.
    """
    from datetime import date, datetime, time

    from connectors.sql_temporal import coerce_sql_temporal, sql_base_type

    base = sql_base_type(target_type).lower() or "date"
    if isinstance(value, (date, datetime, time)):
        try:
            coerce_sql_temporal(value, target_type, engine=dest_db)
        except ValueError as exc:
            return str(exc)
        return None
    try:
        coerced = coerce_sql_temporal(value, target_type, engine=dest_db)
    except ValueError as exc:
        return str(exc)
    if coerced is value and not isinstance(value, (date, datetime, time)):
        shown = value if isinstance(value, str) else str(value)
        return f"Invalid {base}: {shown!r}"
    return None


def _interval_bind_reason(value: Any, target_type: str) -> str | None:
    """Ask the write's interval quarantine, not a second family table."""
    from services.schema_inference import interval_wire_family, is_interval_wire
    from services.type_system import interval_family

    if not is_interval_wire(value):
        shown = value if isinstance(value, str) else str(value)
        return f"value is not a valid interval wire payload: {shown!r}"
    dest_fam = interval_family(target_type)
    wire_fam = interval_wire_family(value)
    if dest_fam and wire_fam and dest_fam != wire_fam:
        return (
            f"interval family mismatch wire={wire_fam} dest={dest_fam} "
            "— YEAR-MONTH ↔ DAY-SECOND collapse"
        )
    return None


def _year_bind_reason(value: Any) -> str | None:
    """Ask the write's YEAR bind. Non-strict MySQL stores 0000 — silent wipe."""
    from connectors.sql_bind import coerce_year_wire

    try:
        coerce_year_wire(value)
    except ValueError as exc:
        return str(exc)
    return None


def _typed_predicate(
    transform: str,
    target_type: str,
    dest_db: str,
) -> Callable[[Any], str | None] | None:
    """The write's coercion, asked as a question instead of at row 1."""
    from connectors.sql_temporal import sql_type_is_temporal
    from services.transform_engine import apply_transform

    temporal = sql_type_is_temporal(target_type)
    interval = _is_interval_carrier(target_type)
    year = _is_year_carrier(target_type)
    if not transform and not temporal and not interval and not year:
        return None

    def _typed_reason(value: Any) -> str | None:
        # Write order is apply_transform then coerce_sql_temporal on the
        # converted cell. Binding the raw token after a successful date parse
        # would refuse ``31/12/2024`` that the write already ISO-normalized.
        bound = value
        if transform:
            out, err = apply_transform(value, transform)
            if err:
                return err
            if out is None:
                return None
            bound = out
        if temporal:
            return _temporal_bind_reason(bound, target_type, dest_db)
        if interval:
            return _interval_bind_reason(bound, target_type)
        if year:
            return _year_bind_reason(bound)
        return None

    return _typed_reason


def fit_predicate_for(
    target: BoundedTarget,
    *,
    dest_db: str,
    dialect_label: str,
) -> Callable[[Any], str | None]:
    """Everything the write decides about one cell of this column.

    A cell must survive two questions, in the order the write asks them: does
    the declared transform accept the value at all (``'2024-02-31'`` into DATE,
    ``'maybe'`` into BOOLEAN, ``'nope'`` into CHAR(36)), and does the result fit
    the carrier's bound. Asking only the second is how a passing Validate was
    followed by a write that refused every row.
    """
    typed = _typed_predicate(target.transform, target.target_type, dest_db)
    if target.carrier == CARRIER_TYPED:
        return typed or (lambda _value: None)
    bound = _fit_predicate(target, dest_db=dest_db, dialect_label=dialect_label)
    if typed is None:
        return bound

    def _both(value: Any) -> str | None:
        return typed(value) or bound(value)

    return _both


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
    deadline_monotonic: float | None = None,
    on_progress: Callable[[int], None] | None = None,
) -> FitScanReport:
    """Scan ``rows`` against the writer's own fit predicates.

    ``rows_are_population`` is the caller's claim that ``rows`` is every source
    row (the file/batch it is about to write, not a preview). The scan downgrades
    that claim to PARTIAL by itself if the budget cuts the walk short — evidence
    is never stronger than the walk that produced it.
    """
    bounded = tuple(targets)
    started = time.monotonic()
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
    env_int: dict[int, int] = {}
    env_scale: dict[int, int] = {}
    env_str_len: dict[int, int] = {}
    env_int_frac: dict[int, bool] = {}
    witnesses: dict[int, list[Any]] = {}
    scanned = 0
    truncated = False
    truncated_reason = ""
    last_progress_at = 0.0
    envelope_rows_scanned = 0
    envelope_complete = False
    envelope_truncated = False

    from services.decimal_observe import write_int_digits_and_scale
    from services.value_serializer import cell_to_string, is_missing_sentinel, present_cell_text

    # Bound once per column, then applied per value — see _fit_predicate.
    probes = tuple(
        (
            idx,
            t.source,
            fit_predicate_for(t, dest_db=dest_db, dialect_label=dialect_label),
        )
        for idx, t in enumerate(bounded)
    )

    def _record_unfit(idx: int, value: Any, why: str, row_no: int) -> None:
        counts[idx] = counts.get(idx, 0) + 1
        reasons.setdefault(idx, why)
        carrier = bounded[idx].carrier
        grew = False
        if carrier == CARRIER_DECIMAL:
            idig, scale = write_int_digits_and_scale(value)
            if idig > env_int.get(idx, 0):
                env_int[idx] = idig
                grew = True
            else:
                env_int.setdefault(idx, idig)
            if scale > env_scale.get(idx, 0):
                env_scale[idx] = scale
                grew = True
            else:
                env_scale.setdefault(idx, scale)
        elif carrier == CARRIER_INTEGER:
            if "fractional" in why.lower():
                env_int_frac[idx] = True
            idig, _scale = write_int_digits_and_scale(value)
            if idig > env_int.get(idx, 0):
                env_int[idx] = idig
                grew = True
            else:
                env_int.setdefault(idx, idig)
        elif carrier == CARRIER_STRING:
            text = present_cell_text(value)
            if text is None:
                text = str(value or "")
            n = len(text)
            if n > env_str_len.get(idx, 0):
                env_str_len[idx] = n
                grew = True
            else:
                env_str_len.setdefault(idx, n)
        held = witnesses.setdefault(idx, [])
        if grew or len(held) < _MAX_PROVE_WITNESSES:
            if grew or len(held) < max_examples:
                held.append(value)
        if len(example_rows.setdefault(idx, [])) < max_examples:
            example_rows[idx].append(row_no)
            example_values.setdefault(idx, []).append(cell_to_string(value)[:120])

    def _scan_one_row(row: Any, row_no: int, *, widenable_only: bool) -> None:
        if not isinstance(row, Mapping):
            return
        for idx, source, fit_reason in probes:
            if widenable_only and bounded[idx].carrier not in _WIDENABLE_CARRIERS:
                continue
            if source not in row:
                continue
            value = row.get(source)
            if value is None or is_missing_sentinel(value):
                continue
            # Blank strings are a nullability question for width/typed
            # carriers. ENUM/SET treat '' as the MySQL error member; YEAR
            # refuses empty as a silent 0000 wipe — Validate must name both.
            if (
                isinstance(value, str)
                and not value.strip()
                and bounded[idx].carrier != CARRIER_DOMAIN
                and not _is_year_carrier(bounded[idx].target_type)
            ):
                continue
            why = fit_reason(value)
            if why is None:
                continue
            _record_unfit(idx, value, why, row_no)

    row_iter = iter(rows or ())
    for row in row_iter:
        if scanned >= budget:
            truncated = True
            truncated_reason = "row"
            break
        if (
            deadline_monotonic is not None
            and scanned > 0
            and time.monotonic() >= deadline_monotonic
        ):
            truncated = True
            truncated_reason = "time"
            break
        scanned += 1
        now = time.monotonic()
        if on_progress is not None and (
            scanned == 1
            or scanned % _PROGRESS_EVERY_ROWS == 0
            or now - last_progress_at >= 0.5
        ):
            last_progress_at = now
            try:
                on_progress(scanned)
            except Exception:
                pass
        _scan_one_row(row, scanned, widenable_only=False)
    else:
        row_iter = iter(())

    if (
        truncated
        and any(
            idx in counts and bounded[idx].carrier in _WIDENABLE_CARRIERS
            for idx in counts
        )
    ):
        envelope_deadline = time.monotonic() + ENVELOPE_CONTINUE_SECONDS
        for row in row_iter:
            if time.monotonic() >= envelope_deadline:
                envelope_truncated = True
                break
            envelope_rows_scanned += 1
            _scan_one_row(row, scanned + envelope_rows_scanned, widenable_only=True)
        else:
            envelope_complete = True
            envelope_truncated = False

    if on_progress is not None and scanned:
        try:
            on_progress(scanned)
        except Exception:
            pass

    if scanned == 0:
        evidence = EVIDENCE_UNMEASURED
    elif rows_are_population and not truncated:
        evidence = EVIDENCE_EXACT
    elif truncated:
        evidence = EVIDENCE_PARTIAL
    else:
        evidence = EVIDENCE_SAMPLED

    from connectors.writer_common import (
        fits_decimal,
        fits_integer,
        fits_varchar,
        integer_overflow_suggested_type,
        parse_decimal_precision_scale,
        varchar_overflow_suggested_type,
    )
    from services.ddl_compatibility import parse_varchar_width
    from services.decimal_observe import (
        CREATE_NEW_NUMERIC_SAFETY_MARGIN,
        decimal_scale_overflow_fix,
        decimal_widen_carrier,
        proven_decimal_widen,
    )

    prove_scope = (
        "file"
        if (not truncated) or envelope_complete
        else "scanned"
    )
    create_new_margin = (
        CREATE_NEW_NUMERIC_SAFETY_MARGIN
        if truncated and not envelope_complete
        else 0
    )

    findings_list: list[ColumnFitFinding] = []
    for idx in sorted(counts):
        target = bounded[idx]
        examples = tuple(example_values.get(idx, ()))
        first = examples[0] if examples else ""
        prove_values = tuple(witnesses.get(idx, ())) or examples
        suggested_type = ""
        suggested_fix = ""
        apply_proven = False
        margin = 0
        why = reasons.get(idx, "")
        if target.carrier == CARRIER_DECIMAL and (first or prove_values):
            if "fractional" in why.lower():
                suggested_type = integer_overflow_suggested_type(
                    first or prove_values[0], target.target_type, dest_db=dest_db
                )
                if not suggested_type:
                    dialect = (dest_db or "").strip().lower()
                    if dialect in {"snowflake"}:
                        suggested_type = "FLOAT"
                    elif dialect in {"bigquery", "bq"}:
                        suggested_type = "FLOAT64"
                    else:
                        suggested_type = "DOUBLE"
                apply_proven = bool(suggested_type)
            else:
                margin = create_new_margin if not target.binds_live_ddl else 0
                suggested_type = proven_decimal_widen(
                    values=prove_values,
                    dest_db=dest_db,
                    current_type=target.target_type,
                    max_int_digits=env_int.get(idx, 0),
                    max_scale=env_scale.get(idx, 0),
                    safety_margin=margin,
                )
                if not suggested_type:
                    suggested_type = decimal_widen_carrier(
                        first or (prove_values[0] if prove_values else ""),
                        dest_db=dest_db,
                        current_type=target.target_type,
                    )
                    parsed = parse_decimal_precision_scale(
                        suggested_type, dest_db=dest_db
                    )
                    apply_proven = bool(
                        parsed
                        and all(
                            fits_decimal(v, parsed[0], parsed[1], dest_db=dest_db)
                            for v in prove_values
                        )
                    )
                    if not apply_proven:
                        suggested_type = ""
                else:
                    apply_proven = True
            if suggested_type and "fractional" in why.lower():
                suggested_fix = (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(preserve the fraction, or ALTER the destination) "
                    "→ re-Validate. Do not silently truncate."
                )
            elif suggested_type:
                example_row = (example_rows.get(idx) or [None])[0]
                suggested_fix = decimal_scale_overflow_fix(
                    first,
                    dest_db=dest_db,
                    current_type=target.target_type,
                    column=target.target,
                    widened=suggested_type,
                    create_new=not target.binds_live_ddl,
                    unfit_rows=counts[idx],
                    example_row=example_row,
                ) or (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently truncate."
                )
                if margin and prove_scope == "scanned" and not target.binds_live_ddl:
                    suggested_fix += (
                        f" Scan did not finish the file; CREATE includes a +{margin} "
                        "scale margin for the unscanned tail. Write-time fit still binds."
                    )
            elif target.carrier == CARRIER_DECIMAL:
                suggested_fix = (
                    f"No destination NUMBER/DECIMAL can hold every overflow in "
                    f"'{target.source}' under this engine's precision cap. "
                    "Remap to FLOAT/text or quarantine — do not Apply a type "
                    "the writer would still refuse."
                )
        elif target.carrier == CARRIER_INTEGER and (first or prove_values):
            seed = first or prove_values[0]
            if env_int_frac.get(idx):
                suggested_type = integer_overflow_suggested_type(
                    seed, target.target_type, dest_db=dest_db
                )
            else:
                widest = prove_values[-1] if prove_values else seed
                suggested_type = integer_overflow_suggested_type(
                    widest, target.target_type, dest_db=dest_db
                )
            if suggested_type:
                parsed = parse_decimal_precision_scale(
                    suggested_type, dest_db=dest_db
                )
                if parsed:
                    apply_proven = all(
                        fits_decimal(v, parsed[0], parsed[1], dest_db=dest_db)
                        for v in prove_values
                    )
                elif suggested_type.upper() in {"FLOAT", "FLOAT64", "DOUBLE", "REAL"}:
                    apply_proven = True
                else:
                    apply_proven = all(
                        fits_integer(v, suggested_type, dest_db=dest_db)
                        for v in prove_values
                    )
                if not apply_proven:
                    suggested_type = ""
            if suggested_type:
                suggested_fix = (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently truncate."
                )
        elif target.carrier == CARRIER_STRING and (first or prove_values):
            longest = first
            if idx in env_str_len:
                for raw in prove_values:
                    text = present_cell_text(raw)
                    if text is None:
                        text = str(raw or "")
                    if len(text) >= env_str_len[idx]:
                        longest = text
                        break
            suggested_type = varchar_overflow_suggested_type(
                longest or first, target.target_type, dest_db=dest_db
            )
            width = parse_varchar_width(suggested_type) if suggested_type else None
            if suggested_type and (
                suggested_type.upper() == "TEXT"
                or (
                    width is not None
                    and all(
                        fits_varchar(v, width, suggested_type)
                        for v in prove_values
                    )
                )
            ):
                apply_proven = True
            else:
                suggested_type = ""
                apply_proven = False
            if suggested_type:
                suggested_fix = (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently truncate."
                )
        elif target.carrier == CARRIER_TYPED:
            why_l = why.lower()
            if "boolean" in why_l or target.transform == "boolean":
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source value (writer accepts 1/0/true/false/yes/no) "
                    "→ re-Validate. Do not silently coerce."
                )
            elif "uuid" in why_l or target.transform == "uuid":
                suggested_type = "VARCHAR(36)"
                suggested_fix = (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(or fix the source UUID) → re-Validate. "
                    "Do not silently coerce."
                )
            elif target.transform == "time" or "invalid time" in why_l:
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source time value → re-Validate. "
                    "Do not silently coerce an invalid time."
                )
            elif _is_interval_carrier(target.target_type) or "interval" in why_l:
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source interval → re-Validate. "
                    "Do not silently coerce YEAR-MONTH into DAY-SECOND."
                )
            elif _is_year_carrier(target.target_type) or "year" in why_l:
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source year → re-Validate. "
                    "Do not silently store 0000."
                )
            else:
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source calendar value → re-Validate. "
                    "Do not silently coerce an invalid date."
                )
        elif target.carrier == CARRIER_BYTES:
            from connectors.writer_common import binary_overflow_suggested_type

            suggested_type = binary_overflow_suggested_type(
                first, target.target_type
            )
            if suggested_type:
                suggested_fix = (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently truncate or UTF-8 invent."
                )
            else:
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source binary/bit wire → re-Validate. "
                    "Do not silently truncate or UTF-8 invent."
                )
        elif target.carrier == CARRIER_DOMAIN:
            from services.type_system import enum_domain_union_carrier

            suggested_type = enum_domain_union_carrier(
                target.target_type, examples
            )
            if suggested_type:
                suggested_fix = (
                    f"Open Map → widen {target.target} to {suggested_type} "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently store '' / drop SET members."
                )
            else:
                suggested_fix = (
                    f"Open Map → remap {target.target} off {target.target_type} "
                    "or fix the source label → re-Validate. "
                    "Do not silently store '' / drop SET members."
                )
        findings_list.append(
            ColumnFitFinding(
                target=target,
                unfit_rows=counts[idx],
                example_rows=tuple(example_rows.get(idx, ())),
                example_values=examples,
                unfit_reason=reasons.get(idx, ""),
                suggested_target_type=suggested_type,
                suggested_fix=suggested_fix + _live_ddl_fix_suffix(
                    binds_live_ddl=target.binds_live_ddl
                ),
                apply_proven=apply_proven and bool(suggested_type),
                apply_proven_scope=(
                    prove_scope if apply_proven and suggested_type else ""
                ),
            )
        )
    findings = tuple(findings_list)
    total = int(rows_total or 0) or (scanned if evidence == EVIDENCE_EXACT else 0)
    duration_ms = max(0, int(round((time.monotonic() - started) * 1000)))
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
                "Scan stopped at the time budget — unscanned rows are unproven."
                if truncated_reason == "time"
                else (
                    "Scan stopped at the row budget — unscanned rows are unproven."
                    if evidence == EVIDENCE_PARTIAL
                    else "Preview-sized evidence only — population fit is unproven."
                )
            )
        ),
        truncated_reason=truncated_reason,
        duration_ms=duration_ms,
        envelope_rows_scanned=envelope_rows_scanned,
        envelope_complete=envelope_complete and not envelope_truncated,
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
    source_kind: str = "",
    source_format: str = "",
    sync_mode: str = "",
    dest_table_exists: bool | None = None,
    deadline_monotonic: float | None = None,
    on_progress: Callable[[int], None] | None = None,
) -> FitScanReport:
    """Resolve bounded targets from the mappings, then scan the rows."""
    targets, undecidable, safe = bounded_targets(
        mappings,
        dest_types=dest_types,
        source_types=source_types,
        dest_db=dest_db,
        job_error_policy=job_error_policy,
        source_kind=source_kind,
        source_format=source_format,
        sync_mode=sync_mode,
        dest_table_exists=dest_table_exists,
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
        deadline_monotonic=deadline_monotonic,
        on_progress=on_progress,
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
    duration_ms = int(report.duration_ms or 0)

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
            "duration_ms": duration_ms,
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
        create_new = all(not f.target.binds_live_ddl for f in aborting)
        widen_actions = [
            {
                "kind": "change_target_type",
                "column": f.target.source,
                "target": f.target.target,
                "to_type": f.suggested_target_type,
                "label": (
                    f"Widen '{f.target.source}' CREATE type to {f.suggested_target_type}"
                    if create_new
                    else f"Widen '{f.target.source}' to {f.suggested_target_type}"
                ),
                "requires_ddl": f.target.binds_live_ddl,
                "mapping_applyable": (
                    bool(f.apply_proven) and not f.target.binds_live_ddl
                ),
                "apply_proven": bool(f.apply_proven),
                "apply_proven_scope": f.apply_proven_scope,
            }
            for f in aborting
            if f.suggested_target_type and f.apply_proven
        ]
        widen_names = ", ".join(
            f"{f.target.source} → {f.suggested_target_type}"
            for f in aborting
            if f.suggested_target_type
        )
        if create_new and widen_names:
            message = (
                f"{rows} value(s) in {scope} cannot fit the peeked CREATE type "
                f"({cols}). This table does not exist yet — widen Map to "
                f"{widen_names} so CREATE can hold the file. The CSV is not "
                "the defect. Execute would create a too-narrow table and commit nothing"
            )
            corrective = (
                "Approve the CREATE-type widen below, then re-Validate. "
                "Nothing is written to the warehouse until Execute."
            )
        else:
            message = (
                f"{rows} value(s) in {scope} cannot fit the destination carrier "
                f"({cols}); the resolved write policy for those column(s) aborts "
                "the load, so Execute would commit nothing"
            )
            corrective = (
                "Widen the destination column, or sign a continue-policy "
                "Migration Risk Contract so the offending rows are held out "
                "in quarantine instead of failing the load."
            )
        return {
            "id": GATE_ID,
            "status": "block",
            "message": message,
            "duration_ms": duration_ms,
            "details": {
                **details,
                "corrective_action": corrective,
                "create_new_table": create_new,
                "suggested_actions": widen_actions,
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
            "duration_ms": duration_ms,
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
            "duration_ms": duration_ms,
            "details": details,
        }

    if report.evidence == EVIDENCE_PARTIAL:
        stop = "time budget" if report.truncated_reason == "time" else "row budget"
        return {
            "id": GATE_ID,
            "status": "warn",
            "message": (
                f"No unfit value in {report.rows_scanned} of "
                f"{report.rows_total or report.rows_scanned} source row(s), but the "
                f"scan stopped at the {stop} — the remaining rows are unproven "
                f"for {len(report.targets)} bounded column(s)"
            ),
            "duration_ms": duration_ms,
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
            "duration_ms": duration_ms,
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
        "duration_ms": duration_ms,
        "details": details,
    }


def applyable_widen_actions(report: FitScanReport) -> list[dict[str, Any]]:
    """change_target_type actions Approve may stamp — proven and not live DDL."""
    gate = build_population_fit_gate(report)
    return [
        a
        for a in (gate.get("details") or {}).get("suggested_actions") or []
        if a.get("kind") == "change_target_type"
        and a.get("to_type")
        and a.get("apply_proven")
        and not a.get("requires_ddl")
        and a.get("mapping_applyable") is not False
    ]


def apply_suggested_widens_and_rescan(
    rows: Iterable[Mapping[str, Any]] | None,
    mappings: list[dict[str, Any]],
    report: FitScanReport,
    **scan_kw: Any,
) -> tuple[list[dict[str, Any]], FitScanReport]:
    """Apply proven CREATE widens, then re-scan the same population.

    This is the Apply-then-Validate proof: a suggested type that still
    produces findings is a product defect, not an operator mistake.
    """
    from services.agentic_repair import apply_actions_to_mappings

    actions = applyable_widen_actions(report)
    updated = apply_actions_to_mappings(list(mappings), actions)
    after = scan_population_fit(rows, updated, **scan_kw)
    return updated, after
