"""Property 6 — schema fidelity beyond column types.

Every migration must either CARRY a schema aspect or emit an explicit
``unsupported`` / ``skipped`` line. Silence is a bug.

v1 create-new (PostgreSQL + SQLite):
  CARRY: primary_key, not_null, simple DEFAULT, unique (column-list)
  CERTIFY as unsupported: CHECK, FK, views, triggers, partial/expression
  indexes, generated expr, identity RESTART, partitioning, comments, …

Name-collision policy is also explicit (fold + length + deterministic suffix).
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterable

from services.physical_placement_ddl import plan_physical_placement, verify_placement

logger = logging.getLogger(__name__)

SCHEMA_FIDELITY_VERSION = 1

# Aspects Property 6 requires on every certificate (present or explicitly absent).
REQUIRED_ASPECTS: tuple[str, ...] = (
    "primary_key",
    "unique",
    "foreign_key",
    "check",
    "not_null",
    "default",
    "identity_sequence",
    "generated",
    "collation",
    "charset",
    "index",
    "partitioning",
    "tablespace",
    "clustering",
    "comment",
    "view",
    "trigger",
    "enum_domain",
    "nested_shape",
    "column_order",
    "name_case",
    "name_collision",
)

# Literals / clock defaults safe to emit on CREATE (no arbitrary SQL injection).
# PostgreSQL introspect often returns `'active'::text` / `('active'::text)`.
_SAFE_DEFAULT_RE = re.compile(
    r"^(?:"
    r"null|"
    r"true|false|"
    r"-?\d+(?:\.\d+)?|"
    r"'(?:[^']|'')*'"
    r"(?:::(?:text|character varying|varchar|bpchar|char|name|cstring))?"
    r"|"
    r"current_timestamp|current_date|current_time|"
    r"\(datetime\('now'\)\)|datetime\('now'\)|"
    r"now\(\)|"
    r"CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME"
    r")$",
    re.IGNORECASE,
)


@dataclass
class SchemaFidelityItem:
    aspect: str
    name: str
    # carried | unsupported | skipped | unknown.
    # "skipped" means measured-and-absent on source; "unknown" means the source
    # catalog was never read for this aspect, which must never be presented as
    # proof that the source does not have it.
    status: str
    reason: str
    source_detail: str = ""
    dest_ddl: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SchemaFidelityReport:
    version: int = SCHEMA_FIDELITY_VERSION
    source_dialect: str = ""
    dest_dialect: str = ""
    dest_mode: str = "create_new"
    items: list[SchemaFidelityItem] = field(default_factory=list)
    name_collision_policy: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        carried = sum(1 for i in self.items if i.status == "carried")
        unsupported = sum(1 for i in self.items if i.status == "unsupported")
        skipped = sum(1 for i in self.items if i.status == "skipped")
        unknown = sum(1 for i in self.items if i.status == "unknown")
        return {
            "version": self.version,
            "source_dialect": self.source_dialect,
            "dest_dialect": self.dest_dialect,
            "dest_mode": self.dest_mode,
            "carried_count": carried,
            "unsupported_count": unsupported,
            "skipped_count": skipped,
            "unknown_count": unknown,
            "items": [i.to_dict() for i in self.items],
            "name_collision_policy": dict(self.name_collision_policy or {}),
        }


@dataclass
class SourceSchemaCatalog:
    """Normalized source catalog used to plan create-new fidelity."""

    dialect: str
    columns: list[str]
    column_types: dict[str, str] = field(default_factory=dict)
    nullable: dict[str, bool] = field(default_factory=dict)
    defaults: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    unique_keys: list[list[str]] = field(default_factory=list)
    # Aspects present on source that v1 cannot carry.
    foreign_keys: list[dict[str, Any]] = field(default_factory=list)
    check_constraints: list[str] = field(default_factory=list)
    # services.check_constraints.CheckConstraints payload: keeps "catalog
    # unreadable" distinct from "table has no CHECK constraints".
    check_constraints_meta: dict[str, Any] | None = None
    indexes: list[dict[str, Any]] = field(default_factory=list)
    # services.secondary_indexes.SourceIndexes payload: keeps "catalog
    # unreadable" distinct from "table has no secondary indexes".
    indexes_meta: dict[str, Any] | None = None
    views: list[str] = field(default_factory=list)
    triggers: list[str] = field(default_factory=list)
    generated_columns: list[str] = field(default_factory=list)
    identity_columns: list[str] = field(default_factory=list)
    collations: dict[str, str] = field(default_factory=dict)
    comments: dict[str, str] = field(default_factory=dict)
    # Tri-state: None means the source catalog was not read for this aspect.
    has_partitioning: bool | None = None
    has_enums: bool | None = None
    has_nested_shapes: bool | None = None
    # services.physical_storage_metadata.PhysicalStorage payload, when measured.
    physical_storage: dict[str, Any] | None = None


@dataclass
class CreateFidelityPlan:
    """DDL fragments + certificate for one create-new table."""

    report: SchemaFidelityReport
    # column -> fragments after type, e.g. ["NOT NULL", "DEFAULT 'x'"]
    column_suffixes: dict[str, list[str]] = field(default_factory=dict)
    # Table-level clauses inside CREATE (...), e.g. PRIMARY KEY (id)
    table_constraints: list[str] = field(default_factory=list)
    # Post-CREATE statements (CREATE UNIQUE INDEX ...)
    post_create_sql: list[str] = field(default_factory=list)
    # Final target column names after collision policy
    dest_columns: list[str] = field(default_factory=list)
    # source_col -> dest_col remaps from collision policy
    column_renames: dict[str, str] = field(default_factory=dict)
    # Structured mirror of the DDL above, for writers that build CREATE TABLE
    # with a toolkit instead of string concatenation (generic_sql/SQLAlchemy).
    # Both views must come from this one planner: a destination whose DDL is
    # assembled elsewhere is a destination the certificate cannot vouch for.
    primary_key: list[str] = field(default_factory=list)
    unique_constraints: list[list[str]] = field(default_factory=list)
    check_predicates: list[tuple[str, str]] = field(default_factory=list)
    not_null_columns: list[str] = field(default_factory=list)
    column_defaults: dict[str, str] = field(default_factory=dict)
    # Clause appended after CREATE TABLE (...): PARTITION BY … / TABLESPACE …
    # (see services/physical_placement_ddl.py). Empty when nothing is carried.
    create_suffix: str = ""
    placement_decisions: list[Any] = field(default_factory=list)
    # The measured source placement the decisions were made from, kept so the
    # post-CREATE destination re-read can compare like for like.
    source_storage: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "report": self.report.to_dict(),
            "create_suffix": self.create_suffix,
            "column_suffixes": {k: list(v) for k, v in self.column_suffixes.items()},
            "table_constraints": list(self.table_constraints),
            "post_create_sql": list(self.post_create_sql),
            "dest_columns": list(self.dest_columns),
            "column_renames": dict(self.column_renames),
            "primary_key": list(self.primary_key),
            "unique_constraints": [list(u) for u in self.unique_constraints],
            "check_predicates": [list(c) for c in self.check_predicates],
            "not_null_columns": list(self.not_null_columns),
            "column_defaults": dict(self.column_defaults),
        }


def dialect_identifier_max_len(dialect: str) -> int:
    d = (dialect or "").strip().lower()
    if d in {"sqlserver", "mssql"}:
        return 128
    if d in {"mysql", "mariadb"}:
        return 64
    if d in {"oracle"}:
        return 128
    return 63  # PostgreSQL / SQLite / Redshift default


def resolve_identifier_collisions(
    names: Iterable[str],
    *,
    dialect: str,
    max_len: int | None = None,
    preserve_case: bool = False,
) -> tuple[list[str], list[SchemaFidelityItem], dict[str, str]]:
    """Fold/truncate identifiers and resolve collisions with deterministic suffixes.

    Returns (dest_names_in_order, fidelity_items, source_to_dest_rename_map).
    """
    from connectors.sql_identifiers import sanitize_identifier

    cap = int(max_len if max_len is not None else dialect_identifier_max_len(dialect))
    dest: list[str] = []
    items: list[SchemaFidelityItem] = []
    renames: dict[str, str] = {}
    used: dict[str, int] = {}

    for raw in names:
        source = str(raw or "")
        base = sanitize_identifier(source, preserve_case=preserve_case, max_len=cap)
        if not base:
            base = "col_field"
        candidate = base
        if candidate in used:
            n = used[candidate] + 1
            while True:
                suffix = f"_{n}"
                trimmed = base[: max(1, cap - len(suffix))] + suffix
                if trimmed not in used:
                    candidate = trimmed
                    used[base] = n
                    used[candidate] = 0
                    items.append(
                        SchemaFidelityItem(
                            aspect="name_collision",
                            name=source,
                            status="carried",
                            reason=(
                                f"Identifier folded/truncated to {base!r} collided; "
                                f"remapped to {candidate!r} (max_len={cap})."
                            ),
                            source_detail=source,
                            dest_ddl=candidate,
                        )
                    )
                    break
                n += 1
        else:
            used[candidate] = 0
            if candidate != source:
                items.append(
                    SchemaFidelityItem(
                        aspect="name_case",
                        name=source,
                        status="carried",
                        reason=(
                            f"Identifier sanitized for {dialect or 'sql'}: "
                            f"{source!r} → {candidate!r} (max_len={cap})."
                        ),
                        source_detail=source,
                        dest_ddl=candidate,
                    )
                )
        dest.append(candidate)
        if candidate != source:
            renames[source] = candidate

    if not any(i.aspect == "name_collision" for i in items):
        items.append(
            SchemaFidelityItem(
                aspect="name_collision",
                name="*",
                status="skipped",
                reason="No identifier collisions after dialect fold/truncate.",
            )
        )
    if not any(i.aspect == "name_case" for i in items):
        items.append(
            SchemaFidelityItem(
                aspect="name_case",
                name="*",
                status="skipped",
                reason="No identifier renames required for dialect case/length rules.",
            )
        )
    return dest, items, renames


def is_safe_default_expr(expr: str) -> bool:
    text = (expr or "").strip()
    if not text:
        return False
    # Strip wrapping parens once.
    if text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if _SAFE_DEFAULT_RE.match(inner):
            return True
    return bool(_SAFE_DEFAULT_RE.match(text))


def build_catalog_from_introspect(
    *,
    dialect: str,
    columns: list[str],
    column_types: dict[str, str] | None = None,
    nullable: dict[str, bool] | None = None,
    keys: dict[str, Any] | None = None,
    defaults: dict[str, str] | None = None,
    check_constraints: list[str] | None = None,
    foreign_keys: list[dict[str, Any]] | None = None,
    views: list[str] | None = None,
    triggers: list[str] | None = None,
) -> SourceSchemaCatalog:
    keys = keys or {}
    unique_keys: list[list[str]] = []
    for uk in keys.get("unique_keys") or []:
        if isinstance(uk, dict):
            if uk.get("primary"):
                continue  # PRIMARY KEY is carried separately
            cols = [str(c) for c in (uk.get("columns") or uk.get("cols") or []) if c]
        elif isinstance(uk, (list, tuple)):
            cols = [str(c) for c in uk if c]
        else:
            cols = []
        if cols:
            unique_keys.append(cols)
    merged_defaults = {
        str(k): str(v)
        for k, v in (defaults or keys.get("defaults") or {}).items()
        if v is not None and str(v).strip()
    }
    return SourceSchemaCatalog(
        dialect=(dialect or "").strip().lower(),
        columns=list(columns or []),
        column_types=dict(column_types or {}),
        nullable={str(k): bool(v) for k, v in (nullable or {}).items()},
        defaults=merged_defaults,
        primary_key=[str(c) for c in (keys.get("primary_key_columns") or []) if c],
        unique_keys=unique_keys,
        foreign_keys=list(foreign_keys or keys.get("foreign_keys") or []),
        check_constraints=list(check_constraints or keys.get("check_constraints") or []),
        check_constraints_meta=_check_payload(keys.get("check_constraints_meta")),
        views=list(views or keys.get("views") or []),
        triggers=list(triggers or keys.get("triggers") or []),
        indexes=list(keys.get("indexes") or []),
        indexes_meta=_check_payload(keys.get("indexes_meta")),
        generated_columns=list(keys.get("generated_columns") or []),
        identity_columns=list(keys.get("identity_columns") or []),
        collations=dict(keys.get("collations") or {}),
        comments=dict(keys.get("comments") or {}),
        has_partitioning=_partitioning_flag(keys),
        has_enums=_tristate(keys.get("has_enums")),
        has_nested_shapes=_tristate(keys.get("has_nested_shapes")),
        physical_storage=_storage_payload(keys.get("physical_storage")),
    )


def _tristate(value: Any) -> bool | None:
    """Keep "never measured" distinct from "measured and absent"."""
    return None if value is None else bool(value)


def _check_payload(value: Any) -> dict[str, Any] | None:
    """Accept a CheckConstraints dataclass or its dict form."""
    return _storage_payload(value)


def _storage_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    return to_dict() if callable(to_dict) else None


def _partitioning_flag(source: dict[str, Any]) -> bool | None:
    """Prefer a measured physical probe over any caller-supplied hint."""
    storage = _storage_payload(source.get("physical_storage"))
    if storage and storage.get("status") == "measured":
        return _tristate(storage.get("partitioned"))
    return _tristate(source.get("has_partitioning"))


def source_to_target_from_mappings(mappings: list[dict[str, Any]] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        src = str(m.get("source") or m.get("source_column") or "").strip()
        tgt = str(m.get("target") or m.get("destination") or "").strip()
        if src and tgt:
            out[src] = tgt
    return out


def catalog_from_payload(payload: Any) -> SourceSchemaCatalog | None:
    """Accept SourceSchemaCatalog or a dict produced for stream→writer handoff."""
    if payload is None:
        return None
    if isinstance(payload, SourceSchemaCatalog):
        return payload
    if not isinstance(payload, dict):
        return None
    if payload.get("columns") is not None or payload.get("primary_key") is not None:
        return SourceSchemaCatalog(
            dialect=str(payload.get("dialect") or ""),
            columns=list(payload.get("columns") or []),
            column_types=dict(payload.get("column_types") or {}),
            nullable={str(k): bool(v) for k, v in (payload.get("nullable") or {}).items()},
            defaults={str(k): str(v) for k, v in (payload.get("defaults") or {}).items()},
            primary_key=[str(c) for c in (payload.get("primary_key") or []) if c],
            unique_keys=[
                [str(c) for c in uk if c]
                for uk in (payload.get("unique_keys") or [])
                if isinstance(uk, (list, tuple))
            ],
            foreign_keys=list(payload.get("foreign_keys") or []),
            check_constraints=list(payload.get("check_constraints") or []),
            check_constraints_meta=_check_payload(payload.get("check_constraints_meta")),
            indexes=list(payload.get("indexes") or []),
            indexes_meta=_check_payload(payload.get("indexes_meta")),
            views=list(payload.get("views") or []),
            triggers=list(payload.get("triggers") or []),
            generated_columns=list(payload.get("generated_columns") or []),
            identity_columns=list(payload.get("identity_columns") or []),
            collations=dict(payload.get("collations") or {}),
            comments=dict(payload.get("comments") or {}),
            has_partitioning=_partitioning_flag(payload),
            has_enums=_tristate(payload.get("has_enums")),
            has_nested_shapes=_tristate(payload.get("has_nested_shapes")),
            physical_storage=_storage_payload(payload.get("physical_storage")),
        )
    return build_catalog_from_introspect(
        dialect=str(payload.get("dialect") or ""),
        columns=list(payload.get("column_names") or payload.get("headers") or []),
        column_types=payload.get("column_types"),
        nullable=payload.get("nullable"),
        keys=payload,
        defaults=payload.get("defaults"),
    )


def catalog_to_payload(catalog: SourceSchemaCatalog) -> dict[str, Any]:
    return asdict(catalog)


def resolve_create_fidelity_plan(
    *,
    source_schema_catalog: Any,
    mappings: list[dict[str, Any]] | None,
    target_columns: list[str],
    target_types: list[str],
    dest_dialect: str,
    table_already_exists: bool = False,
    dest_table: str = "",
    dest_schema: str = "",
    dest_tablespaces: set[str] | None = None,
) -> CreateFidelityPlan:
    """Build a create-new fidelity plan; always returns a certificate (never silent)."""
    dest = (dest_dialect or "").strip().lower()
    if dest in {"postgres", "redshift"}:
        dest = "postgresql"
    catalog = catalog_from_payload(source_schema_catalog)
    if catalog is None:
        # Types-only CREATE still requires an explicit unsupported certificate.
        return CreateFidelityPlan(
            report=empty_unsupported_report(
                source_dialect="",
                dest_dialect=dest,
                reason=(
                    "Source schema catalog unavailable; create-new emitted column "
                    "types only — PK/NOT NULL/DEFAULT/UNIQUE not certified carried."
                ),
            ),
            dest_columns=list(target_columns),
        )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect=dest,
        target_columns=list(target_columns),
        target_types=list(target_types),
        source_to_target=source_to_target_from_mappings(mappings),
        dest_table=dest_table,
        dest_schema=dest_schema,
        dest_tablespaces=dest_tablespaces,
    )
    if table_already_exists:
        # CREATE IF NOT EXISTS will not re-apply constraints — certify honestly.
        for item in plan.report.items:
            if item.status == "carried":
                item.status = "skipped"
                item.reason = (
                    "Destination table already exists; create-new DDL was not "
                    f"re-applied for aspect {item.aspect}. "
                    f"Would have: {item.reason}"
                )
                item.dest_ddl = ""
        plan.column_suffixes = {}
        plan.table_constraints = []
        plan.post_create_sql = []
        plan.primary_key = []
        plan.unique_constraints = []
        plan.check_predicates = []
        plan.not_null_columns = []
        plan.column_defaults = {}
        plan.create_suffix = ""
        plan.placement_decisions = []
    return plan


def plan_create_new_fidelity(
    catalog: SourceSchemaCatalog,
    *,
    dest_dialect: str,
    target_columns: list[str],
    target_types: list[str],
    source_to_target: dict[str, str] | None = None,
    dest_table: str = "",
    dest_schema: str = "",
    dest_tablespaces: set[str] | None = None,
) -> CreateFidelityPlan:
    """Plan CREATE TABLE fidelity for mapped columns on dest_dialect."""
    dest = (dest_dialect or "").strip().lower()
    if dest in {"postgres", "redshift"}:
        dest = "postgresql"
    src_to_tgt = dict(source_to_target or {})
    # Invert for target→source when needed.
    tgt_to_src = {v: k for k, v in src_to_tgt.items() if v}

    # Collision policy on the target column names we are about to emit.
    # Case is preserved on every dialect because every create-new writer emits
    # quoted identifiers verbatim. Folding here produced a plan whose PK/NOT
    # NULL/UNIQUE column names ("id") did not match the columns the writer
    # created ("ID"), so on an uppercase source — every Oracle and most SQL
    # Server catalogs — those constraints were dropped while the certificate
    # still read "carried". Sanitizing, truncation and collision suffixes stay.
    dest_cols, name_items, renames = resolve_identifier_collisions(
        target_columns,
        dialect=dest,
        preserve_case=True,
    )
    # Apply renames to dest column list already returned.

    report = SchemaFidelityReport(
        source_dialect=catalog.dialect,
        dest_dialect=dest,
        dest_mode="create_new",
        name_collision_policy={
            "strategy": "sanitize_fold_truncate_suffix",
            "max_len": dialect_identifier_max_len(dest),
            "renames": renames,
        },
    )
    report.items.extend(name_items)

    suffixes: dict[str, list[str]] = {c: [] for c in dest_cols}
    table_constraints: list[str] = []
    post_sql: list[str] = []
    struct_unique: list[list[str]] = []
    struct_checks: list[tuple[str, str]] = []
    struct_not_null: list[str] = []
    struct_defaults: dict[str, str] = {}

    # Map catalog PK/unique/null/default through source→target→dest rename.
    def _dest_name_for_source(src: str) -> str | None:
        tgt = src_to_tgt.get(src, src)
        if tgt in renames:
            return renames[tgt]
        if tgt in dest_cols:
            return tgt
        # target_columns order alignment
        try:
            idx = list(target_columns).index(tgt)
            return dest_cols[idx]
        except ValueError:
            return None

    # --- CARRY: primary key ---
    pk_dest: list[str] = [
        c for c in (_dest_name_for_source(x) for x in catalog.primary_key) if c
    ]
    if catalog.primary_key and len(pk_dest) == len(catalog.primary_key):
        quoted = ", ".join(_q(c, dest) for c in pk_dest)
        table_constraints.append(f"PRIMARY KEY ({quoted})")
        report.items.append(
            SchemaFidelityItem(
                aspect="primary_key",
                name=",".join(pk_dest),
                status="carried",
                reason="PRIMARY KEY emitted on CREATE TABLE.",
                source_detail=",".join(catalog.primary_key),
                dest_ddl=f"PRIMARY KEY ({quoted})",
            )
        )
    elif catalog.primary_key:
        report.items.append(
            SchemaFidelityItem(
                aspect="primary_key",
                name=",".join(catalog.primary_key),
                status="unsupported",
                reason=(
                    "Source PRIMARY KEY columns are not all mapped to the destination; "
                    "refuse partial PK carry."
                ),
                source_detail=",".join(catalog.primary_key),
            )
        )
    else:
        report.items.append(
            SchemaFidelityItem(
                aspect="primary_key",
                name="*",
                status="skipped",
                reason="No PRIMARY KEY on source.",
            )
        )

    # --- CARRY: NOT NULL ---
    nn_carried = 0
    for src_col, is_null in (catalog.nullable or {}).items():
        if is_null:
            continue
        dest_col = _dest_name_for_source(src_col)
        if not dest_col:
            continue
        suffixes.setdefault(dest_col, []).append("NOT NULL")
        struct_not_null.append(dest_col)
        nn_carried += 1
        report.items.append(
            SchemaFidelityItem(
                aspect="not_null",
                name=dest_col,
                status="carried",
                reason="NOT NULL carried from source nullability.",
                source_detail=src_col,
                dest_ddl="NOT NULL",
            )
        )
    if nn_carried == 0:
        report.items.append(
            SchemaFidelityItem(
                aspect="not_null",
                name="*",
                status="skipped",
                reason="No NOT NULL columns mapped from source (or source omitted nullability).",
            )
        )

    # --- CARRY: simple DEFAULT ---
    def_carried = 0
    for src_col, expr in (catalog.defaults or {}).items():
        dest_col = _dest_name_for_source(src_col)
        if not dest_col:
            continue
        if not is_safe_default_expr(expr):
            report.items.append(
                SchemaFidelityItem(
                    aspect="default",
                    name=dest_col,
                    status="unsupported",
                    reason=(
                        "Expression/default is not on the safe literal whitelist; "
                        "refuse silent SQL injection risk."
                    ),
                    source_detail=str(expr)[:200],
                )
            )
            continue
        default_sql = _normalize_default_sql(expr, dest)
        suffixes.setdefault(dest_col, []).append(f"DEFAULT {default_sql}")
        struct_defaults[dest_col] = default_sql
        def_carried += 1
        report.items.append(
            SchemaFidelityItem(
                aspect="default",
                name=dest_col,
                status="carried",
                reason="Safe DEFAULT literal/clock function carried.",
                source_detail=str(expr)[:200],
                dest_ddl=f"DEFAULT {default_sql}",
            )
        )
    if def_carried == 0 and not catalog.defaults:
        report.items.append(
            SchemaFidelityItem(
                aspect="default",
                name="*",
                status="skipped",
                reason="No column defaults on source catalog.",
            )
        )

    # --- CARRY: UNIQUE (column-list only; skip if equals PK) ---
    uniq_carried = 0
    pk_set = set(pk_dest)
    for uk in catalog.unique_keys:
        dest_uk = [_dest_name_for_source(c) for c in uk]
        if any(c is None for c in dest_uk):
            report.items.append(
                SchemaFidelityItem(
                    aspect="unique",
                    name=",".join(uk),
                    status="unsupported",
                    reason="UNIQUE key includes unmapped columns.",
                    source_detail=",".join(uk),
                )
            )
            continue
        dest_uk_s = [c for c in dest_uk if c]
        if set(dest_uk_s) == pk_set and pk_set:
            continue  # covered by PRIMARY KEY
        quoted = ", ".join(_q(c, dest) for c in dest_uk_s)
        table_constraints.append(f"UNIQUE ({quoted})")
        struct_unique.append(list(dest_uk_s))
        uniq_carried += 1
        report.items.append(
            SchemaFidelityItem(
                aspect="unique",
                name=",".join(dest_uk_s),
                status="carried",
                reason="UNIQUE constraint emitted on CREATE TABLE.",
                source_detail=",".join(uk),
                dest_ddl=f"UNIQUE ({quoted})",
            )
        )
    if uniq_carried == 0 and not catalog.unique_keys:
        report.items.append(
            SchemaFidelityItem(
                aspect="unique",
                name="*",
                status="skipped",
                reason="No UNIQUE keys on source.",
            )
        )

    # --- CARRY: CHECK constraints (portable predicates only) ---
    check_handled = _emit_check_aspect(
        report,
        catalog,
        dest=dest,
        dest_name_for_source=_dest_name_for_source,
        table_constraints=table_constraints,
        check_predicates=struct_checks,
    )

    # --- CARRY: secondary indexes (portable key/uniqueness only) ---
    index_handled = _emit_index_aspect(
        report,
        catalog,
        dest=dest,
        dest_name_for_source=_dest_name_for_source,
        post_create_sql=post_sql,
        dest_table=dest_table,
        dest_schema=dest_schema,
        pk_columns=list(pk_dest),
        unique_constraints=struct_unique,
    )

    # --- CARRY: physical placement (partitioning / tablespace / clustering) ---
    placement = plan_physical_placement(
        source_storage=catalog.physical_storage,
        source_dialect=catalog.dialect,
        dest_dialect=dest,
        dest_schema=dest_schema,
        dest_table=dest_table,
        dest_columns=list(dest_cols),
        primary_key=list(pk_dest),
        unique_constraints=struct_unique,
        dest_tablespaces=dest_tablespaces,
    )
    post_sql.extend(placement.post_create_sql)
    report.items.extend(placement_items(placement.decisions))

    # --- Explicit unsupported / skipped for remaining aspects ---
    _emit_unsupported_catalog(
        report, catalog, skip_check=check_handled, skip_index=index_handled
    )

    # Column order — carried as mapping order (honest).
    report.items.append(
        SchemaFidelityItem(
            aspect="column_order",
            name="*",
            status="carried",
            reason=(
                "Destination column order follows the approved Map target order, "
                "not source physical attnum order."
            ),
            source_detail=",".join(catalog.columns[:40]),
            dest_ddl=",".join(dest_cols[:40]),
        )
    )

    _ensure_all_aspects_present(report)

    return CreateFidelityPlan(
        report=report,
        column_suffixes=suffixes,
        table_constraints=table_constraints,
        post_create_sql=post_sql,
        dest_columns=dest_cols,
        column_renames=renames,
        primary_key=list(pk_dest) if len(pk_dest) == len(catalog.primary_key) else [],
        unique_constraints=struct_unique,
        check_predicates=struct_checks,
        not_null_columns=struct_not_null,
        column_defaults=struct_defaults,
        create_suffix=placement.create_suffix,
        placement_decisions=list(placement.decisions),
        source_storage=catalog.physical_storage,
    )


def placement_items(decisions: list[Any]) -> list[SchemaFidelityItem]:
    """Certificate items for placement decisions.

    ``planned`` is deliberately certified ``unsupported`` until
    ``finalize_placement`` re-reads the destination: emitted DDL is a claim, and
    a certificate must never award a carry on intent alone.
    """
    items: list[SchemaFidelityItem] = []
    for decision in decisions:
        planned = decision.status == "planned"
        items.append(
            SchemaFidelityItem(
                aspect=decision.aspect,
                name="*",
                status="unsupported" if planned else decision.status,
                reason=(
                    f"{decision.reason} Not yet verified on the destination catalog."
                    if planned
                    else decision.reason
                ),
                source_detail=decision.source_detail,
                dest_ddl=decision.dest_ddl,
            )
        )
    return items


def finalize_placement(
    plan: CreateFidelityPlan | None,
    *,
    source_storage: dict[str, Any] | None,
    dest_storage: dict[str, Any] | None,
) -> None:
    """Replace placement items with the verdict of a destination catalog re-read."""
    if plan is None or not plan.placement_decisions:
        return
    verified = verify_placement(
        decisions=list(plan.placement_decisions),
        source_storage=source_storage,
        dest_storage=dest_storage,
    )
    plan.placement_decisions = list(verified)
    aspects = {d.aspect for d in verified}
    plan.report.items = [i for i in plan.report.items if i.aspect not in aspects]
    plan.report.items.extend(
        SchemaFidelityItem(
            aspect=d.aspect,
            name="*",
            status=d.status,
            reason=d.reason,
            source_detail=d.source_detail,
            dest_ddl=d.dest_ddl,
        )
        for d in verified
    )


def certify_placement_on_destination(
    plan: CreateFidelityPlan | None,
    *,
    dialect: str,
    cursor: Any,
    schema: str,
    table: str,
) -> None:
    """Re-read the destination catalog and settle every placement aspect.

    The one place a writer needs to call after CREATE: emitting
    ``PARTITION BY``/``TABLESPACE`` is a claim, and engines are free to ignore
    or redirect placement, so the certificate is only allowed to say "carried"
    after the destination itself reports it.
    """
    if plan is None or not plan.placement_decisions:
        return
    from services.physical_storage_metadata import probe_physical_storage

    try:
        dest_storage = probe_physical_storage(dialect, cursor, schema, table).to_dict()
    except Exception as exc:  # noqa: BLE001 — verification must not fail the load
        logger.debug("destination placement re-read failed: %s", exc)
        dest_storage = None
    finalize_placement(
        plan, source_storage=plan.source_storage, dest_storage=dest_storage
    )


def apply_post_create_sql(
    plan: CreateFidelityPlan | None,
    execute: Callable[[str], Any],
) -> list[str]:
    """Run the plan's post-CREATE statements, downgrading what does not apply.

    A CREATE INDEX can fail on data the source tolerated but the destination
    does not (a unique index over rows a lossy type conversion collapsed). The
    certificate must then say the index was *not* carried: a plan is a claim,
    and only execution makes it true.
    """
    failures: list[str] = []
    if plan is None:
        return failures
    for stmt in list(plan.post_create_sql):
        try:
            execute(stmt)
        except Exception as exc:  # noqa: BLE001 — a refused DDL is evidence
            failures.append(f"{stmt}: {exc}")
            for item in plan.report.items:
                if item.dest_ddl == stmt and item.status == "carried":
                    item.status = "unsupported"
                    item.reason = (
                        f"Destination refused the statement ({type(exc).__name__}: "
                        f"{exc}); the index does not exist on the destination. "
                        f"Would have: {item.reason}"
                    )
                    item.dest_ddl = ""
    return failures


def render_create_column_defs(
    *,
    columns: list[str],
    types: list[str],
    plan: CreateFidelityPlan | None,
    dialect: str,
) -> str:
    """Render `col type [NOT NULL] [DEFAULT …], … [, PRIMARY KEY …]` body."""
    parts: list[str] = []
    suffixes = (plan.column_suffixes if plan else {}) or {}
    for col, typ in zip(columns, types):
        frag = f"{_q(col, dialect)} {typ}"
        for s in suffixes.get(col) or []:
            frag += f" {s}"
        parts.append(frag)
    if plan:
        parts.extend(plan.table_constraints)
    return ", ".join(parts)


def empty_unsupported_report(
    *,
    source_dialect: str,
    dest_dialect: str,
    reason: str,
) -> SchemaFidelityReport:
    """Certificate when create-new fidelity could not be planned."""
    report = SchemaFidelityReport(
        source_dialect=source_dialect,
        dest_dialect=dest_dialect,
        dest_mode="create_new",
    )
    for aspect in REQUIRED_ASPECTS:
        report.items.append(
            SchemaFidelityItem(
                aspect=aspect,
                name="*",
                status="unsupported",
                reason=reason,
            )
        )
    return report


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


def _q(ident: str, dialect: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier

    d = (dialect or "").lower()
    if d in {"mysql", "mariadb"}:
        return quote_sql_identifier(ident, "`")
    return quote_sql_identifier(ident, '"')


def _normalize_default_sql(expr: str, dest_dialect: str) -> str:
    text = (expr or "").strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    # Strip PG type cast: 'active'::text → 'active'
    cast_m = re.match(
        r"^('(?:[^']|'')*')::(?:text|character varying|varchar|bpchar|char|name|cstring)$",
        text,
        re.IGNORECASE,
    )
    if cast_m:
        text = cast_m.group(1)
    if text.lower() in {"current_timestamp", "now()"}:
        return "CURRENT_TIMESTAMP"
    if text.lower() in {"current_date"}:
        return "CURRENT_DATE"
    if text.lower() in {"current_time"}:
        return "CURRENT_TIME"
    if text.lower() in {"datetime('now')", "(datetime('now'))"}:
        if (dest_dialect or "").lower() == "sqlite":
            return "(datetime('now'))"
        return "CURRENT_TIMESTAMP"
    return text


def _emit_check_aspect(
    report: SchemaFidelityReport,
    catalog: SourceSchemaCatalog,
    *,
    dest: str,
    dest_name_for_source: Any,
    table_constraints: list[str],
    check_predicates: list[tuple[str, str]] | None = None,
) -> bool:
    """Carry portable CHECK predicates; return True when the aspect is settled.

    A CHECK is an integrity guarantee, so a predicate the destination would
    evaluate differently is refused, not approximated. Returning False leaves
    the legacy "not introspected — refuse to certify absence" line in charge.
    """
    from services.check_constraints import CheckConstraint, CheckConstraints, plan_check_carry

    payload = catalog.check_constraints_meta
    if not payload:
        return False
    status = str(payload.get("status") or "")
    if status != "measured":
        report.items.append(
            SchemaFidelityItem(
                aspect="check",
                name="*",
                status="unknown",
                reason=(
                    str(payload.get("detail"))
                    or "Source CHECK catalog was unreadable; unmeasured, not proven absent."
                ),
            )
        )
        return True

    items = tuple(
        CheckConstraint(
            name=str(i.get("name") or ""),
            predicate=str(i.get("predicate") or ""),
            columns=tuple(str(c) for c in (i.get("columns") or [])),
        )
        for i in (payload.get("items") or [])
        if isinstance(i, dict)
    )
    if not items:
        report.items.append(
            SchemaFidelityItem(
                aspect="check",
                name="*",
                status="skipped",
                reason="Source catalog read: table has no CHECK constraints.",
            )
        )
        return True

    column_map: dict[str, str] = {}
    for src_col in catalog.columns:
        dest_col = dest_name_for_source(src_col)
        if dest_col:
            column_map[str(src_col).casefold()] = dest_col

    decisions = plan_check_carry(
        CheckConstraints(
            dialect=str(payload.get("dialect") or catalog.dialect),
            status="measured",
            items=items,
        ),
        dest_dialect=dest,
        column_map=column_map,
        quote=_q,
    )
    for decision in decisions:
        name = decision.source.name or "check"
        if decision.carried:
            ddl = f"CHECK ({decision.dest_sql})"
            table_constraints.append(ddl)
            if check_predicates is not None:
                check_predicates.append((name, decision.dest_sql))
            report.items.append(
                SchemaFidelityItem(
                    aspect="check",
                    name=name,
                    status="carried",
                    reason="CHECK predicate re-rendered for the destination dialect.",
                    source_detail=decision.source.predicate[:240],
                    dest_ddl=ddl,
                )
            )
        else:
            report.items.append(
                SchemaFidelityItem(
                    aspect="check",
                    name=name,
                    status="unsupported",
                    reason=(
                        f"{decision.reason} Destination will not enforce this rule — "
                        "apply it manually or reject the source rows upstream."
                    ),
                    source_detail=decision.source.predicate[:240],
                )
            )
    return True


def _emit_index_aspect(
    report: SchemaFidelityReport,
    catalog: SourceSchemaCatalog,
    *,
    dest: str,
    dest_name_for_source: Any,
    post_create_sql: list[str],
    dest_table: str,
    dest_schema: str,
    pk_columns: list[str],
    unique_constraints: list[list[str]],
) -> bool:
    """Carry portable secondary indexes; return True when the aspect is settled.

    A UNIQUE index is an integrity guarantee and a filtered index scopes that
    guarantee, so anything whose rule cannot be reproduced exactly is refused
    rather than emitted as an approximation. Returning False leaves the legacy
    "not carried in v1" line in charge.
    """
    from services.secondary_indexes import (
        IndexColumn,
        SourceIndex,
        SourceIndexes,
        plan_index_carry,
    )

    payload = catalog.indexes_meta
    if not payload:
        return False
    status = str(payload.get("status") or "")
    if status != "measured":
        report.items.append(
            SchemaFidelityItem(
                aspect="index",
                name="*",
                status="unknown",
                reason=(
                    str(payload.get("detail"))
                    or "Source index catalog was unreadable; unmeasured, not proven absent."
                ),
            )
        )
        return True

    items = tuple(
        SourceIndex(
            name=str(i.get("name") or ""),
            columns=tuple(
                IndexColumn(str(c.get("name") or ""), bool(c.get("descending")))
                for c in (i.get("columns") or [])
                if isinstance(c, dict)
            ),
            unique=bool(i.get("unique")),
            predicate=str(i.get("predicate") or ""),
            include_columns=tuple(str(c) for c in (i.get("include_columns") or [])),
            expression=str(i.get("expression") or ""),
            method=str(i.get("method") or ""),
            constraint_backed=bool(i.get("constraint_backed")),
        )
        for i in (payload.get("items") or [])
        if isinstance(i, dict)
    )
    if not items:
        report.items.append(
            SchemaFidelityItem(
                aspect="index",
                name="*",
                status="skipped",
                reason="Source catalog read: table has no secondary indexes.",
            )
        )
        return True

    if not dest_table:
        # CREATE INDEX names the table; without it the DDL cannot be emitted,
        # and a caller that did not supply it must not read "no indexes".
        report.items.append(
            SchemaFidelityItem(
                aspect="index",
                name="*",
                status="unsupported",
                reason=(
                    f"{len(items)} source index(es) were measured but the destination "
                    "table name was not supplied to the planner, so CREATE INDEX "
                    "could not be emitted."
                ),
                source_detail=",".join(i.name for i in items)[:240],
            )
        )
        return True

    column_map: dict[str, str] = {}
    for src_col in catalog.columns:
        dest_col = dest_name_for_source(src_col)
        if dest_col:
            column_map[str(src_col)] = dest_col

    source_dialect = str(payload.get("dialect") or catalog.dialect)

    def _render_filter(predicate: str) -> tuple[str, str]:
        from services.check_constraints import render_check_for_dialect

        return render_check_for_dialect(
            predicate,
            source_dialect=source_dialect,
            dest_dialect=dest,
            column_map={k.casefold(): v for k, v in column_map.items()},
            quote=_q,
        )

    decisions = plan_index_carry(
        SourceIndexes(dialect=source_dialect, status="measured", items=items),
        dest_dialect=dest,
        dest_table=dest_table,
        dest_schema=dest_schema,
        column_map=column_map,
        quote=lambda ident: _q(ident, dest),
        pk_columns=list(pk_columns),
        unique_constraints=[list(u) for u in unique_constraints],
        check_renderer=_render_filter,
    )
    for decision in decisions:
        name = decision.source.name or "index"
        detail = ",".join(c.name for c in decision.source.columns)[:240]
        if decision.carried:
            post_create_sql.append(decision.dest_sql)
            report.items.append(
                SchemaFidelityItem(
                    aspect="index",
                    name=name,
                    status="carried",
                    reason=decision.reason,
                    source_detail=detail,
                    dest_ddl=decision.dest_sql,
                )
            )
        elif decision.skipped:
            report.items.append(
                SchemaFidelityItem(
                    aspect="index",
                    name=name,
                    status="skipped",
                    reason=decision.reason,
                    source_detail=detail,
                )
            )
        else:
            report.items.append(
                SchemaFidelityItem(
                    aspect="index",
                    name=name,
                    status="unsupported",
                    reason=(
                        f"{decision.reason} Destination will not have this index — "
                        "create it manually if the rule or the read path depends on it."
                    ),
                    source_detail=detail,
                )
            )
    return True


def _emit_unsupported_catalog(
    report: SchemaFidelityReport,
    catalog: SourceSchemaCatalog,
    *,
    skip_check: bool = False,
    skip_index: bool = False,
) -> None:
    if catalog.foreign_keys:
        for fk in catalog.foreign_keys[:20]:
            report.items.append(
                SchemaFidelityItem(
                    aspect="foreign_key",
                    name=str(fk.get("name") or fk.get("constraint") or "fk"),
                    status="unsupported",
                    reason=(
                        "CREATE TABLE does not add this reference: the parent must "
                        "hold its rows first. A multi-table transfer adds and "
                        "re-reads it after the load (job report 'foreign_keys'); a "
                        "single-table create leaves it uncarried."
                    ),
                    source_detail=str(fk)[:240],
                )
            )
    else:
        # Introspect does not yet load FKs for all dialects — never certify absence.
        report.items.append(
            SchemaFidelityItem(
                aspect="foreign_key",
                name="*",
                status="unsupported",
                reason=(
                    "Source foreign keys were not read for this catalog; refuse to "
                    "certify absence. Where they are measured, a multi-table "
                    "transfer carries them after the load and proves them from the "
                    "destination catalog."
                ),
            )
        )

    if skip_check:
        pass
    elif catalog.check_constraints:
        for chk in catalog.check_constraints[:20]:
            report.items.append(
                SchemaFidelityItem(
                    aspect="check",
                    name="check",
                    status="unsupported",
                    reason="CHECK constraints are not carried on create-new in v1.",
                    source_detail=str(chk)[:240],
                )
            )
    else:
        # CHECK is not loaded from PG/SQLite catalogs yet — never certify absence.
        report.items.append(
            SchemaFidelityItem(
                aspect="check",
                name="*",
                status="unsupported",
                reason=(
                    "CHECK constraints are not introspected for this source dialect "
                    "in v1; refuse to certify absence."
                ),
            )
        )

    def _aspect_list(
        aspect: str,
        present: bool | None,
        reason_unsup: str,
        reason_skip: str,
        reason_unknown: str = "",
    ) -> None:
        if present is None:
            report.items.append(
                SchemaFidelityItem(
                    aspect=aspect,
                    name="*",
                    status="unknown",
                    reason=reason_unknown
                    or (
                        f"Source catalog was not read for {aspect}; "
                        "unmeasured, not proven absent."
                    ),
                )
            )
            return
        report.items.append(
            SchemaFidelityItem(
                aspect=aspect,
                name="*",
                status="unsupported" if present else "skipped",
                reason=reason_unsup if present else reason_skip,
            )
        )

    if not skip_index:
        _aspect_list(
            "index",
            bool(catalog.indexes),
            "Non-UNIQUE / partial / expression indexes are not carried in v1.",
            "No secondary indexes listed on source catalog.",
        )
    _aspect_list(
        "view",
        bool(catalog.views),
        "Views / materialized views are not created by table transfer.",
        "No views in scope for this table transfer.",
    )
    _aspect_list(
        "trigger",
        bool(catalog.triggers),
        "Triggers are not carried.",
        "No triggers discovered on source.",
    )
    _aspect_list(
        "generated",
        bool(catalog.generated_columns),
        "Generated/computed expressions are not carried (insert-omit only).",
        "No generated columns on source.",
    )
    _aspect_list(
        "identity_sequence",
        bool(catalog.identity_columns),
        "Identity/sequence RESTART values are not carried; SERIAL polarity may widen.",
        "No identity columns flagged on source.",
    )
    _aspect_list(
        "collation",
        bool(catalog.collations),
        "Collation is annotated on introspect but not emitted on create-new DDL.",
        "No per-column collations on source catalog.",
    )
    report.items.append(
        SchemaFidelityItem(
            aspect="charset",
            name="*",
            status="unknown",
            reason="Character set is not introspected for this source dialect.",
        )
    )
    _aspect_list(
        "comment",
        bool(catalog.comments),
        "Table/column comments are not carried.",
        "No comments on source catalog.",
    )
    _aspect_list(
        "enum_domain",
        catalog.has_enums,
        "Enums may create PG types via type invent; domains are not carried.",
        "No enums/domains flagged on source.",
    )
    _aspect_list(
        "nested_shape",
        catalog.has_nested_shapes,
        "Array/struct nested shapes are type-level only; structural reshape uncertified.",
        "No nested structural shapes flagged on source.",
    )


def _ensure_all_aspects_present(report: SchemaFidelityReport) -> None:
    have = {i.aspect for i in report.items}
    for aspect in REQUIRED_ASPECTS:
        if aspect in have:
            continue
        report.items.append(
            SchemaFidelityItem(
                aspect=aspect,
                name="*",
                status="unknown",
                reason=(
                    "No source evidence was collected for this aspect; it is "
                    "unmeasured, not proven absent."
                ),
            )
        )
