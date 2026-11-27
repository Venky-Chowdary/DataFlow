"""The column plan: which columns CE1 encodes in-engine, how, and which are streamed.

A plan is computed once from the mapping, both catalogs, the transforms and the
risk contracts, serialised, and hashed (``plan_hash``). Both engines run exactly
the same plan; a resume whose plan hash differs restarts the scan.

Per mapped column pair the plan records:

* ``cls`` — ``engine`` (CE1 in SQL), ``stream`` (compared by the streamed
  fingerprint pass with Gate-8's own fingerprint) or ``excluded`` (a declared
  omission: outside the population by rule);
* ``kind`` — the CE1 kind (:data:`services.parity_scale.ce1.KINDS`);
* ``projection`` — the source-side rule replaying the write contract;
* ``width`` — ``inline`` or ``digest``.

Classification is conservative: anything this phase cannot prove the engines
render identically goes to ``stream`` (slower, still exact under fingerprint
v2) — never to a looser in-engine rule.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Final

from services.parity_scale.ce1 import CE_VERSION, INLINE_MAX_CHARS, DecimalFit, FieldSpec

_PG_FAMILY: Final[frozenset[str]] = frozenset(
    {"postgresql", "postgres", "pg", "timescaledb", "alloydb", "supabase"}
)

#: Engines with an in-engine CE1 dialect in this build.
DIALECT_ENGINES: Final[frozenset[str]] = frozenset({"sqlite", "postgresql", "mysql", "sqlserver"})
#: The first certification wave (owner decision 3). MariaDB runs as the
#: ``mysql`` dialect and is certified as its own leg by the server it reports.
WAVE1_PAIRS: Final[frozenset[tuple[str, str]]] = frozenset(
    {
        ("postgresql", "postgresql"),
        ("postgresql", "sqlserver"),
        ("sqlserver", "postgresql"),
        ("mysql", "postgresql"),
        ("postgresql", "mysql"),
        ("sqlserver", "sqlserver"),
        ("mysql", "mysql"),
    }
)
#: Pairs the driver will run: SQLite with itself (the reference engine), and
#: any pair of the SQL dialects. Whether a pair's kinds are *certified* is the
#: certification file's answer (:mod:`services.parity_scale.certification`).
SUPPORTED_PAIRS: Final[frozenset[tuple[str, str]]] = frozenset(
    {("sqlite", "sqlite")}
    | {(a, b) for a in ("postgresql", "mysql", "sqlserver") for b in ("postgresql", "mysql", "sqlserver")}
)
PHASE1_PAIRS = SUPPORTED_PAIRS  # kept for callers of the phase-1 name

#: Same set as ``engine_checksum._NO_OP_TYPE_TRANSFORMS``: coercions into a type
#: that cannot move a value already of that type.
_NO_OP_TRANSFORMS: Final[frozenset[str]] = frozenset(
    {"", "none", "identity", "passthrough", "string", "varchar", "text", "decimal",
     "integer", "boolean", "date", "datetime", "time", "uuid"}
)
_BINARY_TEXT_COLLATIONS: Final[frozenset[str]] = frozenset(
    {"c", "posix", "ucs_basic", "binary", "pg_c_utf8"}
)


class PlanError(ValueError):
    """The table cannot be planned (no columns, unknown engine, …)."""


class PairNotCertified(PlanError):
    """The engine pair has no in-engine CE1 dialect in this build."""


def normalize_engine(engine: str | None) -> str:
    eng = (engine or "").strip().lower()
    if eng in _PG_FAMILY:
        return "postgresql"
    if eng in {"sqlite", "sqlite3"}:
        return "sqlite"
    if eng in {"mysql", "mariadb", "tidb"}:
        # The product names a MariaDB destination ``mysql``; the dialect reads
        # VERSION() and certification records the server it talked to.
        return "mysql"
    if eng in {"mssql", "sqlserver", "azuresql", "azure_sql"}:
        return "sqlserver"
    return eng


@dataclass(frozen=True)
class ColumnPlan:
    source: str
    target: str
    cls: str  # engine | stream | excluded
    kind: str = ""
    width: str = "inline"
    projection: str = ""
    source_type: str = ""
    dest_type: str = ""
    digits: int = 6
    transform: str = ""
    reason: str = ""
    decimal_fit: dict[str, Any] | None = None
    empty_is_null: bool = False
    #: Code-crosswalked columns: the mapping keys the writer's pipeline reads, so the
    #: judge replays exactly that pipeline (services.write_replay). None otherwise.
    write_mapping: dict[str, Any] | None = field(default=None, hash=False, compare=False)

    def field_spec(self, dest_engine: str) -> FieldSpec:
        fit = DecimalFit(**self.decimal_fit) if self.decimal_fit else None
        return FieldSpec(
            kind=self.kind or "native",
            width=self.width,
            dest_type=self.dest_type,
            dest_engine=dest_engine,
            digits=self.digits,
            empty_is_null=self.empty_is_null,
            decimal_fit=fit,
        )


@dataclass(frozen=True)
class TablePlan:
    source_engine: str
    dest_engine: str
    columns: tuple[ColumnPlan, ...]
    #: ``(source, target)`` key column pairs, empty for a keyless table.
    key: tuple[tuple[str, str], ...] = ()
    #: Mode R is allowed: every key column orders identically on both engines.
    order_portable: bool = False
    order_reason: str = ""
    #: Every key column is ``engine`` class, so keys can be encoded in-engine.
    key_encodable: bool = True
    ce_version: str = CE_VERSION
    plan_hash: str = ""
    notes: tuple[str, ...] = field(default_factory=tuple)
    #: The run's number and date locales: the judge and the streamed pass read a source cell
    #: under them, as the writer did (a DMY ``to_date``, an EU number).
    run_number_locale: str = ""
    run_date_locale: str = ""

    @property
    def keyless(self) -> bool:
        return not self.key

    @property
    def engine_columns(self) -> tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.cls == "engine")

    @property
    def stream_columns(self) -> tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.cls == "stream")

    @property
    def compared_columns(self) -> tuple[ColumnPlan, ...]:
        return tuple(c for c in self.columns if c.cls != "excluded")

    def key_plans(self) -> tuple[ColumnPlan, ...]:
        by_source = {c.source: c for c in self.columns}
        out = []
        for src, tgt in self.key:
            col = by_source.get(src)
            if col is None:
                raise PlanError(f"key column {src!r} is not mapped")
            out.append(col)
        return tuple(out)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        # write_mapping is left out when absent, so plans without one hash as before.
        data["columns"] = [{k: v for k, v in asdict(c).items() if k != "write_mapping" or v is not None}
                           for c in self.columns]
        data["key"] = [list(k) for k in self.key]
        for name in ("run_number_locale", "run_date_locale"):
            if not data.get(name):
                data.pop(name, None)  # plans without one hash as before
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TablePlan":
        cols = tuple(ColumnPlan(**c) for c in data["columns"])
        return cls(
            source_engine=data["source_engine"],
            dest_engine=data["dest_engine"],
            columns=cols,
            key=tuple(tuple(k) for k in data.get("key") or ()),  # type: ignore[misc]
            order_portable=bool(data.get("order_portable")),
            order_reason=str(data.get("order_reason") or ""),
            key_encodable=bool(data.get("key_encodable", True)),
            ce_version=data.get("ce_version") or CE_VERSION,
            plan_hash=data.get("plan_hash") or "",
            notes=tuple(data.get("notes") or ()),
            run_number_locale=str(data.get("run_number_locale") or ""),
            run_date_locale=str(data.get("run_date_locale") or ""),
        )


def compute_plan_hash(plan: TablePlan) -> str:
    body = plan.to_dict()
    body.pop("plan_hash", None)
    text = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── type classification ─────────────────────────────────────────────────────


def _logical(ddl: str) -> str:
    from services.type_system import normalize_logical_type

    return normalize_logical_type(ddl or "")


def _declared_length(ddl: str) -> int | None:
    match = re.search(r"\(\s*(\d+)\s*\)", ddl or "")
    return int(match.group(1)) if match else None


def _is_fixed_char(ddl: str) -> bool:
    return bool(re.match(r"^\s*(n?char|character|bpchar|nchar)\b(?!\s*varying)", ddl or "", re.I))


def _is_aware(ddl: str) -> bool:
    text = (ddl or "").lower()
    return "with time zone" in text or "timestamptz" in text or "timetz" in text


def kind_of(ddl: str, engine: str) -> str | None:
    """CE1 kind of a declared column type on ``engine``, or ``None`` (not in-engine)."""
    eng = normalize_engine(engine)
    logical = _logical(ddl)
    if eng == "mysql":
        if logical == "boolean" and re.match(r"^\s*bit\b", ddl or "", re.I):
            return None  # BIT reads back as bytes: no portable rendering
        if logical == "time":
            return None  # TIME is an interval up to 838 h and reads back as one
        if logical == "datetime":
            return "timestamp"  # DATETIME / TIMESTAMP read back naive (session UTC)
        if logical == "float":
            return "native"
        if logical == "json":
            return "native"
    if eng == "sqlserver":
        base = (ddl or "").strip().lower()
        if logical == "datetime" and (base.startswith("datetime2") or base.startswith("datetimeoffset")):
            return "timestamptz" if base.startswith("datetimeoffset") else "timestamp"
        if logical in {"datetime", "time"}:
            return None  # datetime (1/300 s), smalldatetime and time: streamed
        if logical == "float":
            return "native"
        if logical in {"string", "text"}:
            return "char" if re.match(r"^\s*n?char\b(?!\s*varying)", base) else "text"
    if eng == "sqlite":
        # SQLite has no enforced types: values keep their storage class, so only
        # the classes CE1 renders natively are typed; everything else is the
        # exact same-engine rendering.
        return {
            "integer": "integer",
            "decimal": "decimal",
            "boolean": "boolean",
            "string": "text",
            "text": "text",
            "binary": "binary",
        }.get(logical, "native")
    if logical == "integer":
        return "integer"
    if logical == "decimal":
        return "decimal"
    if logical == "boolean":
        return "boolean"
    if logical in {"string", "text"}:
        return "char" if _is_fixed_char(ddl) else "text"
    if logical == "date":
        return "date"
    if logical == "datetime":
        return "timestamptz" if _is_aware(ddl) else "timestamp"
    if logical == "time":
        return None if _is_aware(ddl) else "time"
    if logical == "uuid":
        return "uuid"
    if logical == "binary":
        return "binary"
    return None


def _width(kind: str, ddl: str) -> str:
    if kind == "binary":
        return "digest"
    if kind in {"text", "char"}:
        length = _declared_length(ddl)
        return "inline" if length is not None and length <= INLINE_MAX_CHARS else "digest"
    return "inline"


def _carrier_digits(ddl: str, engine: str) -> int:
    from services.carrier_instant import carrier_instant_digits

    digits = carrier_instant_digits(ddl, engine=engine)
    cap = 7 if normalize_engine(engine) == "sqlserver" else 6
    if digits is None:
        return cap
    return max(0, min(cap, int(digits)))


def _canonical_type(engine: str, ddl: str) -> str:
    from services.reconciliation import _canonical_fingerprint_ddl

    return _canonical_fingerprint_ddl(engine, str(ddl or "").strip())


# ── the write contract: decimal fit, read from the real writer ──────────────


def probe_decimal_fit(
    mapping: dict[str, Any],
    dest_type: str,
    dest_engine: str,
    *,
    error_policy: str = "quarantine",
    number_locale: str = "WIRE",
) -> DecimalFit | None:
    """Read the writer's action on each class of value that does not fit.

    Runs :func:`connectors.writer_common.map_rows_for_fingerprint` — the same
    mapping Gate-8 replays on the source side — on one synthetic value per
    class (scale overflow, integer overflow, both, NaN, infinity). The
    projection then *replays* what the writer did instead of re-implementing
    it. ``None`` when the type has no ``(p,s)`` or the writer wrote a
    non-fitting value as some other number (nothing a predicate could replay).
    """
    from connectors.writer_common import map_rows_for_fingerprint, parse_decimal_precision_scale

    parsed = parse_decimal_precision_scale(dest_type, dest_db=dest_engine)
    if not parsed:
        return None
    precision, scale = int(parsed[0]), int(parsed[1])
    max_int = max(precision - scale, 0)
    target = str(mapping.get("target") or mapping.get("source") or "")
    source = str(mapping.get("source") or target)
    fraction = Decimal("0." + "0" * scale + "11")
    big = Decimal(10) ** max_int + Decimal(1)
    probes = {
        "scale": fraction,
        "int": big,
        "both": big + fraction,
        "nan": Decimal("NaN"),
        "inf": Decimal("Infinity"),
    }
    probe_mapping = dict(mapping)
    probe_mapping["source"] = source
    probe_mapping["target"] = target
    from services.transform_engine import reset_active_number_locale, set_active_number_locale

    actions: dict[str, str] = {}
    for name, value in probes.items():
        # The run's number locale: a typed database source reads as WIRE, so
        # ``12.345`` is a number and not a US/EU-ambiguous spelling.
        token = set_active_number_locale(number_locale)
        try:
            mapped, _rejected = map_rows_for_fingerprint(
                headers=[source],
                data_rows=[[value]],
                mappings=[probe_mapping],
                target_cols=[target],
                dest_types={target: dest_type},
                dest_kind=dest_engine,
                error_policy=error_policy,
            )
        finally:
            reset_active_number_locale(token)
        if not mapped:
            actions[name] = "hold"
        elif mapped[0][0] is None:
            actions[name] = "null"
        elif name in {"nan", "inf"} and isinstance(mapped[0][0], Decimal) and not mapped[0][0].is_finite():
            actions[name] = "keep"
        else:
            # The writer wrote an overflow as some other value: nothing a
            # predicate can replay without rounding, so the column streams.
            return None
    return DecimalFit(precision=precision, scale=scale, dest_db=dest_engine, actions=actions)


# ── plan construction ───────────────────────────────────────────────────────


def _lookup(types: dict[str, str], name: str) -> str:
    if name in types:
        return str(types[name] or "")
    lowered = name.lower()
    for k, v in types.items():
        if str(k).lower() == lowered:
            return str(v or "")
    return ""


def classify_column(
    mapping: dict[str, Any],
    *,
    source_type: str,
    dest_type: str,
    source_engine: str,
    dest_engine: str,
    allow_native: bool,
    error_policy: str,
    number_locale: str = "WIRE",
) -> ColumnPlan:
    from services.code_crosswalk import declared_crosswalk
    from services.mapping_constraints import is_intentional_omit
    from services.migration_risk_contract import mapping_declares_risk_contract

    source = str(mapping.get("source") or "").strip()
    target = str(mapping.get("target") or source).strip()
    transform = str(mapping.get("transform") or "").strip().lower()
    base = dict(source=source, target=target, source_type=source_type, dest_type=dest_type,
                transform=transform)
    if is_intentional_omit(mapping):
        return ColumnPlan(cls="excluded", reason="declared omission", **base)
    if declared_crosswalk(mapping):
        # Transform and crosswalk together: the writer's own pipeline is replayed (both, in its order).
        from services.write_replay import write_mapping

        return ColumnPlan(cls="stream", reason="code crosswalk", write_mapping=write_mapping(mapping), **base)
    if transform not in _NO_OP_TRANSFORMS:
        return ColumnPlan(cls="stream", reason=f"transform {transform!r} is replayed by the judge", **base)
    if mapping.get("rule_contracts"):
        return ColumnPlan(cls="stream", reason="rule contract (held-out rows are phase 3)", **base)
    if not source_type or not dest_type:
        return ColumnPlan(cls="stream", reason="catalog type unknown", **base)
    same_dialect = normalize_engine(source_engine) == normalize_engine(dest_engine)
    src_kind = kind_of(source_type, source_engine)
    dst_kind = kind_of(dest_type, dest_engine)
    identical = _canonical_type(dest_engine, source_type) == _canonical_type(dest_engine, dest_type)
    contract = mapping_declares_risk_contract(mapping)
    if contract:
        if dst_kind == "decimal" and src_kind == "decimal":
            fit = probe_decimal_fit(mapping, dest_type, normalize_engine(dest_engine),
                                    error_policy=error_policy, number_locale=number_locale)
            if fit is not None:
                return ColumnPlan(cls="engine", kind="decimal", projection=fit.spec(),
                                  decimal_fit=asdict(fit), **base)
        return ColumnPlan(cls="stream", reason="risk contract without an in-engine projection", **base)
    if dst_kind is None or src_kind is None or src_kind != dst_kind:
        if same_dialect and identical and allow_native:
            return ColumnPlan(cls="engine", kind="native", projection="same-engine rendering",
                              **base)
        why = "type has no portable CE1 rule" if dst_kind is None else "source and destination kinds differ"
        return ColumnPlan(cls="stream", reason=why, **base)
    if dst_kind == "native":
        if same_dialect and allow_native and identical:
            return ColumnPlan(cls="engine", kind="native", projection="same-engine rendering",
                              **base)
        return ColumnPlan(cls="stream", reason="no portable CE1 rule", **base)
    if _is_fixed_char(source_type) and dst_kind != "char":
        return ColumnPlan(cls="stream", reason="fixed-width source into a varying carrier", **base)
    projection = ""
    digits = 6
    if dst_kind in {"timestamp", "timestamptz", "time"}:
        digits = _carrier_digits(dest_type, normalize_engine(dest_engine))
        projection = f"instant({digits})"
    if dst_kind == "decimal" and not identical:
        from connectors.writer_common import parse_decimal_precision_scale

        src_ps = parse_decimal_precision_scale(source_type, dest_db=normalize_engine(source_engine))
        dst_ps = parse_decimal_precision_scale(dest_type, dest_db=normalize_engine(dest_engine))
        if dst_ps and (not src_ps or src_ps[1] > dst_ps[1] or (src_ps[0] - src_ps[1]) > (dst_ps[0] - dst_ps[1])):
            # A narrowing with no signed contract: the writer's job policy
            # decides; replay it like a contract.
            fit = probe_decimal_fit(mapping, dest_type, normalize_engine(dest_engine),
                                    error_policy=error_policy, number_locale=number_locale)
            if fit is None:
                return ColumnPlan(cls="stream", reason="decimal narrowing the writer does not enforce", **base)
            return ColumnPlan(cls="engine", kind="decimal", projection=fit.spec(),
                              decimal_fit=asdict(fit), **base)
    return ColumnPlan(
        cls="engine",
        kind=dst_kind,
        width=_width(dst_kind, dest_type),
        projection=projection,
        digits=digits,
        empty_is_null=normalize_engine(dest_engine) == "oracle" and dst_kind in {"text", "char"},
        **base,
    )


def _order_portable(
    key_cols: list[ColumnPlan],
    source_engine: str,
    dest_engine: str,
    key_collations: dict[str, str] | None,
) -> tuple[bool, str]:
    if not key_cols:
        return False, "keyless table"
    same = normalize_engine(source_engine) == normalize_engine(dest_engine)
    for col in key_cols:
        if col.cls != "engine":
            return False, f"key column {col.source!r} is not encoded in-engine"
        kind = col.kind
        if kind in {"integer", "decimal", "date", "timestamp", "timestamptz", "time"}:
            if col.projection.startswith("fit") or (
                kind in {"timestamp", "timestamptz", "time"} and col.digits < 6
            ):
                return False, f"key column {col.source!r} has a lossy key projection"
            continue
        if kind in {"uuid", "binary"} and same:
            continue
        if kind in {"text", "char"}:
            collations = key_collations or {}
            src_coll = str(collations.get(col.source) or "").strip('"').lower()
            dst_coll = str(collations.get(col.target) or collations.get(col.source) or "").strip('"').lower()
            if normalize_engine(source_engine) == "sqlite" and same:
                if src_coll in {"", "binary"} and dst_coll in {"", "binary"}:
                    continue
            if src_coll in _BINARY_TEXT_COLLATIONS and dst_coll in _BINARY_TEXT_COLLATIONS:
                continue
            return False, f"text key {col.source!r} without a binary collation on both sides"
        if kind == "native" and same and normalize_engine(source_engine) == "sqlite":
            continue
        return False, f"key column {col.source!r} kind {kind!r} is not order-portable"
    return True, "every key column orders identically on both engines"


def build_plan(
    mappings: list[dict[str, Any]],
    *,
    source_types: dict[str, str],
    dest_types: dict[str, str],
    source_engine: str,
    dest_engine: str,
    key_columns: list[str] | None = None,
    error_policy: str = "quarantine",
    allow_native: bool = True,
    key_collations: dict[str, str] | None = None,
    number_locale: str | None = None,
    run_number_locale: str = "",
    run_date_locale: str = "",
) -> TablePlan:
    """Plan a table. ``key_columns`` are *source* column names (empty: keyless).

    ``run_number_locale`` / ``run_date_locale``: the run's locales, under which the judge
    replays a source cell's transform or crosswalk (as the writer did)."""
    src_eng = normalize_engine(source_engine)
    dst_eng = normalize_engine(dest_engine)
    if not mappings:
        raise PlanError("no mapped columns")
    if number_locale is None:
        from src.transfer.connector_capabilities import typed_wire_number_locale

        number_locale = typed_wire_number_locale("database", src_eng) or ""
    columns: list[ColumnPlan] = []
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or source).strip()
        if not source or not target:
            raise PlanError("mapping without a source or target column")
        columns.append(
            classify_column(
                mapping,
                source_type=_lookup(source_types, source),
                dest_type=_lookup(dest_types, target),
                source_engine=src_eng,
                dest_engine=dst_eng,
                allow_native=allow_native,
                error_policy=error_policy,
                number_locale=number_locale,
            )
        )
    if not any(c.cls != "excluded" for c in columns):
        raise PlanError("every column is excluded")
    by_source = {c.source: c for c in columns}
    key_pairs: list[tuple[str, str]] = []
    for name in key_columns or []:
        col = by_source.get(name)
        if col is None:
            raise PlanError(f"key column {name!r} is not mapped")
        if col.cls == "excluded":
            raise PlanError(f"key column {name!r} is an omitted column")
        key_pairs.append((col.source, col.target))
    key_cols = [by_source[s] for s, _t in key_pairs]
    key_encodable = all(c.cls == "engine" for c in key_cols)
    if key_cols and any(c.projection.startswith("fit") for c in key_cols):
        key_encodable = False
    if key_cols and src_eng != "sqlite" and any(c.kind == "native" for c in key_cols):
        # A native rendering has no Python reference, so the driver could not
        # name such a key outside the engine.
        key_encodable = False
    portable, why = _order_portable(key_cols, src_eng, dst_eng, key_collations)
    notes: list[str] = []
    if not key_encodable and key_cols:
        notes.append("key not encodable in-engine: the table is compared by streamed fingerprint")
    if (src_eng, dst_eng) not in SUPPORTED_PAIRS:
        notes.append(f"pair {src_eng}->{dst_eng} has no in-engine dialect in this build")
    plan = TablePlan(
        source_engine=src_eng,
        dest_engine=dst_eng,
        columns=tuple(columns),
        key=tuple(key_pairs),
        order_portable=portable and key_encodable,
        order_reason=why,
        key_encodable=key_encodable,
        notes=tuple(notes),
        run_number_locale=str(run_number_locale or ""),
        run_date_locale=str(run_date_locale or ""),
    )
    return TablePlan(**{**plan.__dict__, "plan_hash": compute_plan_hash(plan)})


def require_pair(plan: TablePlan) -> None:
    pair = (plan.source_engine, plan.dest_engine)
    if pair not in SUPPORTED_PAIRS:
        raise PairNotCertified(
            f"{plan.source_engine}->{plan.dest_engine} has no in-engine CE1 dialect in this build"
        )
