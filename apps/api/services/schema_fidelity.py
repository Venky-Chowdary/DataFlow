"""Property 6 — schema fidelity beyond column types.

Every migration must either CARRY a schema aspect or emit an explicit
``unsupported`` / ``skipped`` line. Silence is a bug.

create-new:
  CARRY: primary_key, not_null, simple DEFAULT, unique (column-list), CHECK,
  secondary indexes, physical placement, key generators (identity /
  AUTO_INCREMENT / IDENTITY)
  CERTIFY as unsupported: FK on single-table create, views, triggers,
  partial/expression indexes, generated expr, comments, …

Name-collision policy is also explicit (fold + length + deterministic suffix).
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterable

from services.collation_carry import destination_column_collations, plan_collation_carry
from services.encoding_capacity import plan_encoding_carry
from services.identity_carry import plan_identity_carry
from services.offset_label import plan_offset_label_carry
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
    "offset_label",
    "encoding",
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
    charsets: dict[str, str] = field(default_factory=dict)
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
    # Destination columns created as key generators, and the subset whose engine
    # refuses a client-supplied value unless the session opts in (SQL Server
    # SET IDENTITY_INSERT). The load must know: the rows carry explicit keys.
    identity_columns: dict[str, str] = field(default_factory=dict)
    identity_insert_columns: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "report": self.report.to_dict(),
            "identity_columns": dict(self.identity_columns),
            "identity_insert_columns": list(self.identity_insert_columns),
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
        charsets=dict(keys.get("charsets") or {}),
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
            charsets=dict(payload.get("charsets") or {}),
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

    def _dest_type(col: str) -> str:
        try:
            return str(target_types[dest_cols.index(col)] or "")
        except (ValueError, IndexError):
            return ""

    # --- CARRY: primary key ---
    pk_dest: list[str] = [
        c for c in (_dest_name_for_source(x) for x in catalog.primary_key) if c
    ]
    if catalog.primary_key and len(pk_dest) == len(catalog.primary_key):
        unindexable = [
            c for c in pk_dest if dest in {"mysql", "mariadb"} and _mysql_index_requires_prefix(_dest_type(c))
        ]
        if unindexable:
            report.items.append(
                SchemaFidelityItem(
                    aspect="primary_key",
                    name=",".join(pk_dest),
                    status="unsupported",
                    reason=(
                        "MySQL/MariaDB cannot PRIMARY KEY TEXT/BLOB/JSON without a "
                        "prefix length; refusing an invented prefix (that would "
                        "enforce a different uniqueness rule than the source). "
                        f"Unindexable: {','.join(unindexable)}."
                    ),
                    source_detail=",".join(catalog.primary_key),
                )
            )
            pk_dest = []
        else:
            quoted = ", ".join(_q(c, dest) for c in pk_dest)
            table_constraints.append(f"PRIMARY KEY ({quoted})")
            report.items.append(
                SchemaFidelityItem(
                    aspect="primary_key",
                    name=",".join(pk_dest),
                    status="carried",
                    reason=(
                        "PRIMARY KEY emitted on CREATE TABLE (provisional until "
                        "destination catalog re-read)."
                    ),
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
        if dest in {"mysql", "mariadb"} and any(
            _mysql_index_requires_prefix(_dest_type(c)) for c in dest_uk_s
        ):
            report.items.append(
                SchemaFidelityItem(
                    aspect="unique",
                    name=",".join(dest_uk_s),
                    status="unsupported",
                    reason=(
                        "MySQL/MariaDB cannot UNIQUE-index TEXT/BLOB/JSON without a "
                        "prefix length; refusing an invented prefix (that would "
                        "enforce a different uniqueness rule than the source)."
                    ),
                    source_detail=",".join(uk),
                )
            )
            continue
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

    # --- CARRY: key generator (identity / AUTO_INCREMENT / IDENTITY) ---
    # Placed after PK/NOT NULL/DEFAULT so the generator is the last fragment on
    # the column (MySQL requires AUTO_INCREMENT after NOT NULL) and so a DEFAULT
    # planned above can be withdrawn: a column cannot both generate and default.
    identity_plan = plan_identity_carry(
        catalog=catalog,
        dest_dialect=dest,
        dest_name_for_source=_dest_name_for_source,
        dest_type_for_column=lambda c: (
            target_types[dest_cols.index(c)] if c in dest_cols else ""
        ),
        primary_key=list(pk_dest),
    )
    for decision in identity_plan.decisions:
        report.items.append(SchemaFidelityItem(**decision.to_item_kwargs()))
    for dest_col, suffix in identity_plan.column_suffixes.items():
        dropped_default = struct_defaults.pop(dest_col, None)
        if dropped_default is not None:
            suffixes[dest_col] = [
                s for s in suffixes.get(dest_col) or []
                if not s.upper().startswith("DEFAULT ")
            ]
            report.items.append(
                SchemaFidelityItem(
                    aspect="default",
                    name=dest_col,
                    status="unsupported",
                    reason=(
                        "DEFAULT withdrawn: the column is created as a key "
                        "generator, and a generator and a default cannot both "
                        "fill the same column."
                    ),
                    source_detail=dropped_default,
                )
            )
        if dest in {"oracle", "sqlserver"}:
            # Both engines imply NOT NULL on an identity column and reject the
            # redundant fragment in the same definition.
            suffixes[dest_col] = [
                s for s in suffixes.get(dest_col) or [] if s.upper() != "NOT NULL"
            ]
        suffixes.setdefault(dest_col, []).append(suffix)

    # --- CARRY: collation equality (uniqueness polarity, not name-copy) ---
    keyed_cols = set(pk_dest)
    for uk in struct_unique:
        keyed_cols.update(uk)
    keyed_cols.update(str(c) for c in catalog.primary_key)
    for uk in catalog.unique_keys:
        keyed_cols.update(str(c) for c in uk)
    collation_plan = plan_collation_carry(
        catalog=catalog,
        dest_dialect=dest,
        dest_name_for_source=_dest_name_for_source,
        dest_type_for_column=lambda c: (
            target_types[dest_cols.index(c)] if c in dest_cols else ""
        ),
        unique_or_pk=keyed_cols,
    )
    charset_emitted = False
    for decision in collation_plan.decisions:
        report.items.append(SchemaFidelityItem(**decision.to_item_kwargs()))
        if decision.dest_charset:
            charset_emitted = True
            report.items.append(
                SchemaFidelityItem(
                    aspect="charset",
                    name=decision.dest_column,
                    status=decision.status,
                    reason=(
                        f"Character set {decision.dest_charset} paired with "
                        f"collation {decision.dest_collation or 'destination default'}."
                    ),
                    dest_ddl=(
                        f"CHARACTER SET {decision.dest_charset}"
                        if decision.status == "carried"
                        else ""
                    ),
                )
            )
        prefixes = collation_plan.column_prefixes.get(decision.dest_column) or []
        if prefixes and decision.status == "carried":
            # MySQL: CHARACTER SET / COLLATE must sit on the type, before NOT NULL.
            suffixes[decision.dest_column] = (
                list(prefixes) + list(suffixes.get(decision.dest_column) or [])
            )

    offset_plan = plan_offset_label_carry(
        catalog=catalog,
        dest_dialect=dest,
        dest_name_for_source=_dest_name_for_source,
        dest_type_for_column=lambda c: (
            target_types[dest_cols.index(c)] if c in dest_cols else ""
        ),
    )
    for decision in offset_plan:
        report.items.append(SchemaFidelityItem(**decision.to_item_kwargs()))

    dest_charsets = {
        d.dest_column: d.dest_charset
        for d in collation_plan.decisions
        if d.dest_charset
    }
    encoding_plan = plan_encoding_carry(
        catalog=catalog,
        dest_dialect=dest,
        dest_name_for_source=_dest_name_for_source,
        dest_type_for_column=lambda c: (
            target_types[dest_cols.index(c)] if c in dest_cols else ""
        ),
        dest_charset_for_column=dest_charsets.get,
    )
    for decision in encoding_plan:
        report.items.append(SchemaFidelityItem(**decision.to_item_kwargs()))

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
        report,
        catalog,
        skip_check=check_handled,
        skip_index=index_handled,
        skip_identity=bool(identity_plan.decisions),
        skip_collation=bool(collation_plan.decisions),
        skip_offset_label=bool(offset_plan),
        skip_encoding=bool(encoding_plan),
        skip_charset=charset_emitted,
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
        identity_columns=dict(identity_plan.column_suffixes),
        identity_insert_columns=list(identity_plan.identity_insert_columns),
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


def certify_identity_on_destination(
    plan: CreateFidelityPlan | None,
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> None:
    """Settle the identity aspect from the destination catalog, not the DDL.

    A generator clause an engine quietly ignored (SQLAlchemy drops
    ``autoincrement`` on a non-integer key) would otherwise be certified as
    carried while the client's first insert after cutover still fails.
    """
    if plan is None or not plan.identity_columns:
        return
    from services.identity_carry import destination_generator_columns

    generators = destination_generator_columns(
        dialect=dialect, schema=schema, table=table, fetchall=fetchall
    )
    for item in plan.report.items:
        if item.aspect != "identity_sequence" or item.status != "carried":
            continue
        if generators is None:
            item.status = "unknown"
            item.reason = (
                "The destination catalog could not be read after CREATE, so the "
                "generator is unverified. Emitted DDL is not proof. "
                f"Would have: {item.reason}"
            )
            continue
        folded = {g.casefold() for g in generators}
        if item.name.casefold() not in folded:
            item.status = "unsupported"
            item.reason = (
                "The destination did not take the generator: its catalog reports "
                "no identity/AUTO_INCREMENT on this column, so a client insert "
                "without a key will fail. "
                f"Attempted: {item.dest_ddl or 'generator clause'}."
            )
            item.dest_ddl = ""
            plan.identity_columns.pop(item.name, None)
            plan.identity_insert_columns = [
                c for c in plan.identity_insert_columns if c != item.name
            ]


def certify_collation_on_destination(
    plan: CreateFidelityPlan | None,
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> None:
    """Settle collation equality from the destination catalog, not the DDL.

    Emitting ``COLLATE utf8mb4_bin`` is a claim. Engines silently ignore unknown
    collations or substitute the table default (CI), which is exactly the DMS
    failure this carry exists to prevent.
    """
    if plan is None:
        return
    items = [
        i for i in plan.report.items
        if i.aspect == "collation" and i.status == "carried"
    ]
    if not items:
        return
    from services.collation_carry import classify_equality, destination_column_collations

    found = destination_column_collations(
        dialect=dialect, schema=schema, table=table, fetchall=fetchall
    )
    for item in items:
        if found is None:
            item.status = "unknown"
            item.reason = (
                "The destination collation catalog could not be read after CREATE, "
                "so equality is unverified. Emitted DDL is not proof. "
                f"Would have: {item.reason}"
            )
            continue
        folded = {k.casefold(): v for k, v in found.items()}
        actual = folded.get(item.name.casefold(), "")
        actual_eq = classify_equality(dialect, collation=actual)
        ddl = (item.dest_ddl or "").upper()
        want_cs = "_BIN" in ddl or 'COLLATE "C"' in ddl or "POSIX" in ddl
        want_ci = "_CI" in ddl or "CI_AS" in ddl or "CI_AI" in ddl
        if want_cs and actual_eq.case != "sensitive":
            item.status = "unsupported"
            item.reason = (
                f"Destination catalog collates as {actual or 'default'} "
                f"(case={actual_eq.case}); case-sensitive uniqueness was not taken. "
                f"Attempted: {item.dest_ddl}."
            )
            item.dest_ddl = ""
        elif want_ci and actual_eq.case != "insensitive":
            item.status = "unsupported"
            item.reason = (
                f"Destination catalog collates as {actual or 'default'} "
                f"(case={actual_eq.case}); case-insensitive equality was not taken. "
                f"Attempted: {item.dest_ddl}."
            )
            item.dest_ddl = ""
        elif not ddl:
            # Destination-default CS (PostgreSQL). Empty/C/POSIX/default is CS.
            if actual_eq.case == "insensitive":
                item.status = "unsupported"
                item.reason = (
                    f"Destination default collation {actual or 'default'} is "
                    "case-insensitive; source uniqueness was case-sensitive."
                )


# --------------------------------------------------------------------------
# Structure verification (PK / NOT NULL / DEFAULT / UNIQUE)
# --------------------------------------------------------------------------
# Emitting CONSTRAINT / NOT NULL / DEFAULT in CREATE TABLE is a claim. Engines
# may ignore or rewrite clauses; the certificate may only say "carried" after
# the destination catalog reports the same structure.
_STRUCTURE_PK_QUERY: dict[str, str] = {
    "postgresql": (
        "SELECT kcu.column_name FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "  ON tc.constraint_schema = kcu.constraint_schema "
        " AND tc.constraint_name = kcu.constraint_name "
        " AND tc.table_schema = kcu.table_schema "
        " AND tc.table_name = kcu.table_name "
        "WHERE tc.table_schema = COALESCE(?, 'public') AND tc.table_name = ? "
        "AND tc.constraint_type = 'PRIMARY KEY' "
        "ORDER BY kcu.ordinal_position"
    ),
    "mysql": (
        "SELECT column_name FROM information_schema.key_column_usage "
        "WHERE table_schema = COALESCE(?, DATABASE()) AND table_name = ? "
        "AND constraint_name = 'PRIMARY' ORDER BY ordinal_position"
    ),
    "sqlserver": (
        "SELECT c.name FROM sys.indexes i "
        "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
        "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "JOIN sys.tables t ON t.object_id = i.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE i.is_primary_key = 1 AND t.name = ? AND s.name = COALESCE(?, SCHEMA_NAME()) "
        "ORDER BY ic.key_ordinal"
    ),
    "oracle": (
        "SELECT cols.column_name FROM all_constraints cons "
        "JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name "
        " AND cons.owner = cols.owner "
        "WHERE cons.constraint_type = 'P' AND cons.table_name = ? "
        "AND cons.owner = COALESCE(?, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) "
        "ORDER BY cols.position"
    ),
    "sqlite": (
        "SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk"
    ),
}
_STRUCTURE_PK_QUERY["mariadb"] = _STRUCTURE_PK_QUERY["mysql"]

_STRUCTURE_NULLABLE_QUERY: dict[str, str] = {
    "postgresql": (
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = COALESCE(?, 'public') AND table_name = ?"
    ),
    "mysql": (
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = COALESCE(?, DATABASE()) AND table_name = ?"
    ),
    "sqlserver": (
        "SELECT c.name, CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END "
        "FROM sys.columns c "
        "JOIN sys.tables t ON t.object_id = c.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE t.name = ? AND s.name = COALESCE(?, SCHEMA_NAME())"
    ),
    "oracle": (
        "SELECT column_name, nullable FROM all_tab_columns "
        "WHERE table_name = ? AND owner = COALESCE(?, SYS_CONTEXT('USERENV', "
        "'CURRENT_SCHEMA'))"
    ),
    "sqlite": (
        "SELECT name, CASE WHEN \"notnull\" = 1 THEN 'NO' ELSE 'YES' END "
        "FROM pragma_table_info(?)"
    ),
}
_STRUCTURE_NULLABLE_QUERY["mariadb"] = _STRUCTURE_NULLABLE_QUERY["mysql"]

_STRUCTURE_DEFAULT_QUERY: dict[str, str] = {
    "postgresql": (
        "SELECT column_name, column_default FROM information_schema.columns "
        "WHERE table_schema = COALESCE(?, 'public') AND table_name = ? "
        "AND column_default IS NOT NULL"
    ),
    "mysql": (
        "SELECT column_name, column_default FROM information_schema.columns "
        "WHERE table_schema = COALESCE(?, DATABASE()) AND table_name = ? "
        "AND column_default IS NOT NULL"
    ),
    "sqlserver": (
        "SELECT c.name, dc.definition FROM sys.columns c "
        "JOIN sys.tables t ON t.object_id = c.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id "
        "WHERE t.name = ? AND s.name = COALESCE(?, SCHEMA_NAME()) "
        "AND c.default_object_id <> 0"
    ),
    "oracle": (
        "SELECT column_name, data_default FROM all_tab_columns "
        "WHERE table_name = ? AND owner = COALESCE(?, SYS_CONTEXT('USERENV', "
        "'CURRENT_SCHEMA')) AND data_default IS NOT NULL"
    ),
    "sqlite": (
        "SELECT name, dflt_value FROM pragma_table_info(?) WHERE dflt_value IS NOT NULL"
    ),
}
_STRUCTURE_DEFAULT_QUERY["mariadb"] = _STRUCTURE_DEFAULT_QUERY["mysql"]

# UNIQUE probes return (constraint/index identifier, column) so composite keys can
# be grouped and matched as exact column sets — a single-column claim must not be
# certified merely because the column appears inside a wider composite unique key,
# and a real composite must not be falsely downgraded.
_STRUCTURE_UNIQUE_QUERY: dict[str, str] = {
    "postgresql": (
        "SELECT tc.constraint_name, kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "  ON tc.constraint_schema = kcu.constraint_schema "
        " AND tc.constraint_name = kcu.constraint_name "
        " AND tc.table_schema = kcu.table_schema "
        " AND tc.table_name = kcu.table_name "
        "WHERE tc.table_schema = COALESCE(?, 'public') AND tc.table_name = ? "
        "AND tc.constraint_type = 'UNIQUE'"
    ),
    "mysql": (
        "SELECT index_name, column_name FROM information_schema.statistics "
        "WHERE table_schema = COALESCE(?, DATABASE()) AND table_name = ? "
        "AND non_unique = 0 AND index_name <> 'PRIMARY'"
    ),
    "sqlserver": (
        "SELECT i.name, c.name FROM sys.indexes i "
        "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
        " AND ic.is_included_column = 0 "
        "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "JOIN sys.tables t ON t.object_id = i.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE i.is_unique = 1 AND i.is_primary_key = 0 "
        "AND t.name = ? AND s.name = COALESCE(?, SCHEMA_NAME())"
    ),
    "oracle": (
        "SELECT cons.constraint_name, cols.column_name FROM all_constraints cons "
        "JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name "
        " AND cons.owner = cols.owner "
        "WHERE cons.constraint_type = 'U' AND cons.table_name = ? "
        "AND cons.owner = COALESCE(?, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))"
    ),
    "sqlite": (
        "SELECT il.name, ii.name FROM pragma_index_list(?) il "
        "JOIN pragma_index_info(il.name) ii "
        "WHERE il.\"unique\" = 1 AND il.origin <> 'pk'"
    ),
}
_STRUCTURE_UNIQUE_QUERY["mariadb"] = _STRUCTURE_UNIQUE_QUERY["mysql"]

# CHECK clause texts on the destination table. Engines rewrite predicates
# aggressively (``x IN ('a','b')`` -> ``x = ANY(ARRAY[...])``), so certification
# matches by destination COLUMN COVERAGE (identifiers survive rewrites), never by
# brittle predicate-string equality. NOT NULL system checks are excluded — they
# are certified by the nullability probe, not here.
_STRUCTURE_CHECK_QUERY: dict[str, str] = {
    "postgresql": (
        "SELECT cc.check_clause FROM information_schema.check_constraints cc "
        "JOIN information_schema.table_constraints tc "
        "  ON cc.constraint_schema = tc.constraint_schema "
        " AND cc.constraint_name = tc.constraint_name "
        "WHERE tc.table_schema = COALESCE(?, 'public') AND tc.table_name = ? "
        "AND tc.constraint_type = 'CHECK'"
    ),
    "mysql": (
        "SELECT cc.check_clause FROM information_schema.check_constraints cc "
        "JOIN information_schema.table_constraints tc "
        "  ON cc.constraint_schema = tc.constraint_schema "
        " AND cc.constraint_name = tc.constraint_name "
        "WHERE tc.table_schema = COALESCE(?, DATABASE()) AND tc.table_name = ? "
        "AND tc.constraint_type = 'CHECK'"
    ),
    "sqlserver": (
        "SELECT cc.definition FROM sys.check_constraints cc "
        "JOIN sys.tables t ON t.object_id = cc.parent_object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE t.name = ? AND s.name = COALESCE(?, SCHEMA_NAME())"
    ),
    "oracle": (
        "SELECT search_condition_vc FROM all_constraints "
        "WHERE constraint_type = 'C' AND table_name = ? "
        "AND owner = COALESCE(?, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))"
    ),
}
_STRUCTURE_CHECK_QUERY["mariadb"] = _STRUCTURE_CHECK_QUERY["mysql"]


def _structure_norm_dialect(dialect: str) -> str:
    d = (dialect or "").lower().strip()
    if d in {"postgres", "pg"}:
        return "postgresql"
    if d in {"mssql", "sql_server"}:
        return "sqlserver"
    if d == "mariadb":
        return "mariadb"
    return d


def _structure_args(dialect: str, schema: str, table: str) -> tuple[Any, ...]:
    dest = _structure_norm_dialect(dialect)
    unquoted = str(table).strip().strip('"').strip("`").strip("[").strip("]")
    schema_arg = (schema or "").strip() or None
    if dest == "oracle":
        unquoted = unquoted if unquoted != unquoted.lower() else unquoted.upper()
        schema_arg = (schema_arg or "").upper() or None
        return (unquoted, schema_arg)
    if dest == "sqlite":
        return (unquoted,)
    if dest == "sqlserver":
        return (unquoted, schema_arg)
    # postgresql / mysql / mariadb: schema first, then table
    return (schema_arg, unquoted)


def _fetch_structure_set(
    query_map: dict[str, str],
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> set[str] | None:
    dest = _structure_norm_dialect(dialect)
    query = query_map.get(dest)
    if not query:
        return None
    try:
        rows = fetchall(query, _structure_args(dialect, schema, table))
    except Exception as exc:  # noqa: BLE001 — unknown keeps claim as unverified
        logger.debug("structure catalog probe failed (%s): %s", dest, exc)
        return None
    out: set[str] = set()
    for row in rows or []:
        try:
            name = row[0] if not isinstance(row, dict) else next(iter(row.values()))
        except Exception:
            continue
        if name is not None and str(name).strip():
            out.add(str(name))
    return out


def _fetch_not_null_columns(
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> set[str] | None:
    dest = _structure_norm_dialect(dialect)
    query = _STRUCTURE_NULLABLE_QUERY.get(dest)
    if not query:
        return None
    try:
        rows = fetchall(query, _structure_args(dialect, schema, table))
    except Exception as exc:  # noqa: BLE001
        logger.debug("nullability catalog probe failed (%s): %s", dest, exc)
        return None
    not_null: set[str] = set()
    for row in rows or []:
        try:
            if isinstance(row, dict):
                vals = list(row.values())
                name, flag = vals[0], vals[1]
            else:
                name, flag = row[0], row[1]
        except Exception:
            continue
        flag_s = str(flag or "").strip().upper()
        # Oracle uses Y/N; information_schema uses YES/NO.
        if flag_s in {"NO", "N", "FALSE", "0"}:
            not_null.add(str(name))
    return not_null


# Clock/boolean default synonyms treated as equivalent across dialects so a
# faithfully-carried default is not falsely downgraded on cosmetic differences.
_CLOCK_DEFAULTS = {
    "current_timestamp", "current_timestamp()", "now()", "now", "getdate()",
    "getutcdate()", "sysdate", "systimestamp", "localtimestamp",
    "localtimestamp()", "statement_timestamp()", "transaction_timestamp()",
    "clock_timestamp()", "sysdatetime()",
}
_TRUE_DEFAULTS = {"true", "t", "1", "b'1'"}
_FALSE_DEFAULTS = {"false", "f", "0", "b'0'"}


def _normalize_default_expr(expr: Any) -> str:
    """Fold a catalog/planned default into a comparable literal.

    Iteratively strips wrapping parens, trailing type casts (``'x'::text``,
    ``'x'::character varying``), national/escape/bit/hex string-literal prefixes
    (``N'x'``, ``E'x'``, ``B'1'``), and surrounding quotes to a fixed point — so
    ``('active'::character varying)``, ``N'active'`` and ``active`` all unify —
    then collapses clock precision (``current_timestamp(6)`` -> ``()``) and
    casefolds.
    """
    s = str(expr if expr is not None else "").strip()
    prev: str | None = None
    while s and s != prev:
        prev = s
        if len(s) >= 2 and s[0] == "(" and s[-1] == ")":
            s = s[1:-1].strip()
            continue
        stripped_cast = re.sub(r"::\s*[A-Za-z0-9_ \"\.\[\]]+\s*$", "", s).strip()
        if stripped_cast != s:
            s = stripped_cast
            continue
        prefix = re.match(r"^(?:[NnEeBbXx]|[Uu]&)(['\"].*)$", s)
        if prefix:
            s = prefix.group(1).strip()
            continue
        if len(s) >= 2 and s[0] in "'\"" and s[-1] == s[0]:
            s = s[1:-1].strip()
            continue
    # current_timestamp(6) / localtimestamp(3) → drop precision for clock compare.
    s = re.sub(r"\(\s*\d+\s*\)", "()", s)
    return s.casefold()


def _default_exprs_equivalent(a: str, b: str) -> bool:
    if a == b:
        return True
    if a in _CLOCK_DEFAULTS and b in _CLOCK_DEFAULTS:
        return True
    if a in _TRUE_DEFAULTS and b in _TRUE_DEFAULTS:
        return True
    if a in _FALSE_DEFAULTS and b in _FALSE_DEFAULTS:
        return True
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        return False


def _claimed_default_literal(item: Any) -> str:
    """Extract the planned default literal from an item's emitted DDL clause."""
    ddl = str(getattr(item, "dest_ddl", "") or "")
    m = re.search(r"default\s+(.+)$", ddl, re.IGNORECASE | re.DOTALL)
    return _normalize_default_expr(m.group(1)) if m else ""


def _fetch_default_exprs(
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> dict[str, str] | None:
    """Map casefolded column name -> normalized default expression (or ""). ``None``
    means the catalog could not be read (unverified, not proven absent)."""
    dest = _structure_norm_dialect(dialect)
    query = _STRUCTURE_DEFAULT_QUERY.get(dest)
    if not query:
        return None
    try:
        rows = fetchall(query, _structure_args(dialect, schema, table))
    except Exception as exc:  # noqa: BLE001 — unknown keeps claim as unverified
        logger.debug("default catalog probe failed (%s): %s", dest, exc)
        return None
    out: dict[str, str] = {}
    for row in rows or []:
        try:
            if isinstance(row, dict):
                vals = list(row.values())
                name = vals[0]
                expr = vals[1] if len(vals) > 1 else None
            else:
                name = row[0]
                expr = row[1] if len(row) > 1 else None
        except Exception:
            continue
        if name is None or not str(name).strip():
            continue
        out[str(name).strip().casefold()] = _normalize_default_expr(expr)
    return out


def _fetch_grouped_columns(
    query_map: dict[str, str],
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> list[set[str]] | None:
    """Group a (constraint/index-id, column) probe into per-constraint column
    sets (casefolded). ``None`` means the catalog could not be read."""
    dest = _structure_norm_dialect(dialect)
    query = query_map.get(dest)
    if not query:
        return None
    try:
        rows = fetchall(query, _structure_args(dialect, schema, table))
    except Exception as exc:  # noqa: BLE001
        logger.debug("grouped structure probe failed (%s): %s", dest, exc)
        return None
    groups: dict[str, set[str]] = {}
    for row in rows or []:
        try:
            if isinstance(row, dict):
                vals = list(row.values())
                gkey, col = vals[0], (vals[1] if len(vals) > 1 else None)
            else:
                gkey, col = row[0], (row[1] if len(row) > 1 else None)
        except Exception:
            continue
        if col is None or not str(col).strip():
            continue
        groups.setdefault(str(gkey), set()).add(str(col).strip().casefold())
    return list(groups.values())


def _extract_sqlite_checks(ddl: str) -> list[str]:
    """Pull balanced ``CHECK (...)`` clauses out of a SQLite CREATE TABLE text.

    SQLite exposes no catalog view for CHECKs; ``sqlite_master.sql`` is the only
    source of truth. Predicates may nest parens (``CHECK (a IN (1,2))``), so a
    depth counter is used rather than a regex. String literals (and the parens
    inside them, e.g. ``DEFAULT 'check (1)'``) are skipped so a literal is never
    mistaken for a CHECK clause.
    """
    out: list[str] = []
    n = len(ddl)
    i = 0
    while i < n:
        ch = ddl[i]
        if ch == "'":
            # skip a single-quoted string literal (handles '' escape)
            i += 1
            while i < n:
                if ddl[i] == "'":
                    if i + 1 < n and ddl[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if ch == '"':
            i += 1
            while i < n and ddl[i] != '"':
                i += 1
            i += 1
            continue
        if (ch in "cC") and ddl[i : i + 5].lower() == "check":
            before_ok = i == 0 or not (ddl[i - 1].isalnum() or ddl[i - 1] == "_")
            k = i + 5
            while k < n and ddl[k].isspace():
                k += 1
            if before_ok and k < n and ddl[k] == "(":
                depth = 0
                start = k
                in_lit = False
                while k < n:
                    c = ddl[k]
                    if in_lit:
                        if c == "'":
                            if k + 1 < n and ddl[k + 1] == "'":
                                k += 2
                                continue
                            in_lit = False
                        k += 1
                        continue
                    if c == "'":
                        in_lit = True
                    elif c == "(":
                        depth += 1
                    elif c == ")":
                        depth -= 1
                        if depth == 0:
                            out.append(ddl[start : k + 1])
                            k += 1
                            break
                    k += 1
                i = k
                continue
        i += 1
    return out


# Tokens engine rewrites inject that are implausible as unquoted column names
# (``= ANY(ARRAY[...])``, ``CAST(x AS ...)``). Cast *type* names like ``text`` are
# handled by stripping ``::type`` from the clause blob, so real columns named
# after a type are still certifiable.
_CHECK_NOISE_TOKENS = {"any", "all", "array", "cast", "as"}


def _pure_not_null_clause(clause: str) -> bool:
    """A clause that is only ``<col> IS NOT NULL`` — engine NOT NULL echo, owned
    by the nullability probe, not a real CHECK."""
    t = re.sub(r"[\"'`\[\]\s()]", "", str(clause or "")).casefold()
    return bool(re.fullmatch(r"[a-z0-9_.$#]+isnotnull", t))


def _strip_check_type_noise(text: str) -> str:
    """Remove rewrite noise so a column named ``text``/``age`` is not matched by a
    ``::text`` cast token or by a value inside a string literal.

    Strips ``::type`` and ``CAST(... AS type)`` casts and blanks single-quoted
    string-literal contents (``note = 'age'`` must not certify a CHECK on ``age``).
    """
    s = str(text or "")
    s = re.sub(r"'(?:[^']|'')*'", " '' ", s)  # blank literal contents
    s = re.sub(r"::\s*[A-Za-z0-9_ \"\.\[\]]+", " ", s)  # ::type casts
    s = re.sub(r"\bas\s+[A-Za-z_][A-Za-z0-9_]*", " ", s, flags=re.IGNORECASE)  # CAST(x AS type)
    return s


def _fetch_check_clauses(
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> list[str] | None:
    """Destination CHECK clause texts. ``None`` means the catalog could not be
    read (unverified); ``[]`` means the table was read and has no CHECKs."""
    dest = _structure_norm_dialect(dialect)
    if dest == "sqlite":
        try:
            rows = fetchall(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                _structure_args(dialect, schema, table),
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("sqlite check probe failed: %s", exc)
            return None
        ddl = ""
        for row in rows or []:
            val = row[0] if not isinstance(row, dict) else next(iter(row.values()))
            if val:
                ddl = str(val)
                break
        if not ddl:
            return None  # table not found → unverified, not "no checks"
        return [c for c in _extract_sqlite_checks(ddl) if not _pure_not_null_clause(c)]

    query = _STRUCTURE_CHECK_QUERY.get(dest)
    if not query:
        return None
    try:
        rows = fetchall(query, _structure_args(dialect, schema, table))
    except Exception as exc:  # noqa: BLE001
        logger.debug("check catalog probe failed (%s): %s", dest, exc)
        return None
    out: list[str] = []
    for row in rows or []:
        try:
            val = row[0] if not isinstance(row, dict) else next(iter(row.values()))
        except Exception:
            continue
        if val is not None and str(val).strip() and not _pure_not_null_clause(val):
            out.append(str(val))
    return out


def certify_structure_on_destination(
    plan: CreateFidelityPlan | None,
    *,
    dialect: str,
    schema: str,
    table: str,
    fetchall: Callable[[str, tuple[Any, ...]], Any],
) -> None:
    """Settle PK / NOT NULL / DEFAULT / UNIQUE from the destination catalog.

    Plan-time ``status=carried`` only means DDL was *emitted*. Client cutover
    requires the destination itself to report the constraint — same bar as
    identity and placement certification.
    """
    if plan is None or not plan.report.items:
        return

    aspects = {"primary_key", "not_null", "default", "unique", "check"}
    claimed = [i for i in plan.report.items if i.aspect in aspects and i.status == "carried"]
    if not claimed:
        return

    pk_cols = _fetch_structure_set(
        _STRUCTURE_PK_QUERY,
        dialect=dialect,
        schema=schema,
        table=table,
        fetchall=fetchall,
    )
    nn_cols = _fetch_not_null_columns(
        dialect=dialect, schema=schema, table=table, fetchall=fetchall
    )
    default_exprs = _fetch_default_exprs(
        dialect=dialect, schema=schema, table=table, fetchall=fetchall
    )
    unique_groups = _fetch_grouped_columns(
        _STRUCTURE_UNIQUE_QUERY,
        dialect=dialect,
        schema=schema,
        table=table,
        fetchall=fetchall,
    )
    # CHECK certification only runs when a CHECK was actually carried.
    want_check = any(i.aspect == "check" for i in claimed)
    check_clauses = (
        _fetch_check_clauses(dialect=dialect, schema=schema, table=table, fetchall=fetchall)
        if want_check
        else None
    )
    dest_cols_universe = {
        str(c).strip().casefold()
        for c in (getattr(plan, "dest_columns", None) or [])
        if str(c).strip()
    }

    for item in claimed:
        if item.aspect == "primary_key":
            expected = {
                c.strip().casefold()
                for c in str(item.name or "").split(",")
                if c.strip()
            }
            if pk_cols is None:
                item.status = "unknown"
                item.reason = (
                    "The destination catalog could not be read after CREATE, so the "
                    "PRIMARY KEY is unverified. Emitted DDL is not proof. "
                    f"Would have: {item.reason}"
                )
            else:
                present_pk = {str(c).strip().casefold() for c in pk_cols}
                # Exact key shape: a wider or narrower live key is not the claimed
                # contract (case-insensitive to satisfy folding dialects).
                if expected and expected != present_pk:
                    item.status = "unsupported"
                    item.reason = (
                        "The destination did not take the exact PRIMARY KEY: "
                        f"catalog reports {sorted(present_pk) or 'none'}, expected "
                        f"{sorted(expected)}. Attempted: {item.dest_ddl or 'PRIMARY KEY'}."
                    )
                    item.dest_ddl = ""
                else:
                    item.reason = (
                        "PRIMARY KEY verified on the destination catalog after CREATE."
                    )
            continue

        folded_name = str(item.name or "").casefold()

        if item.aspect == "default":
            if default_exprs is None:
                item.status = "unknown"
                item.reason = (
                    "The destination catalog could not be read after CREATE, so "
                    f"DEFAULT on {item.name!r} is unverified. Emitted DDL is not "
                    f"proof. Would have: {item.reason}"
                )
                continue
            catalog_expr = default_exprs.get(folded_name)
            if catalog_expr is None:
                item.status = "unsupported"
                item.reason = (
                    f"The destination did not take a DEFAULT on {item.name!r}. "
                    f"Attempted: {item.dest_ddl or 'DEFAULT'}."
                )
                item.dest_ddl = ""
                continue
            claimed_lit = _claimed_default_literal(item)
            if (
                claimed_lit
                and catalog_expr
                and not _default_exprs_equivalent(claimed_lit, catalog_expr)
            ):
                item.status = "unsupported"
                item.reason = (
                    f"The destination DEFAULT on {item.name!r} differs from the plan: "
                    f"catalog has {catalog_expr!r}, planned {claimed_lit!r}."
                )
                item.dest_ddl = ""
            elif claimed_lit and catalog_expr:
                item.reason = (
                    "DEFAULT value verified on the destination catalog after CREATE."
                )
            else:
                # A default is present but the exact literal could not be parsed
                # for value comparison — presence verified, value asserted only.
                item.reason = (
                    "A DEFAULT is present on the destination column (value "
                    "expression not compared)."
                )
            continue

        if item.aspect == "unique":
            if unique_groups is None:
                item.status = "unknown"
                item.reason = (
                    "The destination catalog could not be read after CREATE, so "
                    f"UNIQUE on {item.name!r} is unverified. Emitted DDL is not "
                    f"proof. Would have: {item.reason}"
                )
                continue
            want = {
                c.strip().casefold()
                for c in str(item.name or "").split(",")
                if c.strip()
            }
            if want and not any(want == g for g in unique_groups):
                item.status = "unsupported"
                item.reason = (
                    f"The destination did not take the UNIQUE key {sorted(want)}. "
                    f"Attempted: {item.dest_ddl or 'UNIQUE'}."
                )
                item.dest_ddl = ""
            else:
                item.reason = (
                    "UNIQUE constraint verified on the destination catalog after CREATE."
                )
            continue

        if item.aspect == "check":
            if check_clauses is None:
                item.status = "unknown"
                item.reason = (
                    "The destination catalog could not be read after CREATE, so the "
                    f"CHECK {item.name!r} is unverified. Emitted DDL is not proof. "
                    f"Would have: {item.reason}"
                )
                continue
            if not check_clauses:
                item.status = "unsupported"
                item.reason = (
                    f"The destination did not take the CHECK {item.name!r}: the "
                    "catalog reports no CHECK constraints on the table. "
                    f"Attempted: {item.dest_ddl or 'CHECK'}."
                )
                item.dest_ddl = ""
                continue
            # Column-coverage match: engines rewrite the predicate but preserve
            # column identifiers. Every destination column the carried predicate
            # constrains must appear in the union of the live CHECK clauses. Cast
            # type suffixes and injected keyword tokens are stripped first so a
            # column named ``text``/``any`` is not matched by rewrite noise.
            predicate_text = _strip_check_type_noise(
                f"{item.dest_ddl or ''} {item.source_detail or ''}"
            ).casefold()
            referenced = {
                col
                for col in dest_cols_universe
                if col not in _CHECK_NOISE_TOKENS
                and re.search(rf"(?<![A-Za-z0-9_]){re.escape(col)}(?![A-Za-z0-9_])", predicate_text)
            }
            clause_blob = _strip_check_type_noise(" ".join(check_clauses)).casefold()
            missing = {
                col
                for col in referenced
                if not re.search(
                    rf"(?<![A-Za-z0-9_]){re.escape(col)}(?![A-Za-z0-9_])", clause_blob
                )
            }
            if referenced and missing:
                item.status = "unsupported"
                item.reason = (
                    f"The destination did not take the CHECK on {sorted(missing)}: "
                    "no live CHECK constraint references "
                    f"{'these columns' if len(missing) > 1 else 'this column'}. "
                    f"Attempted: {item.dest_ddl or 'CHECK'}."
                )
                item.dest_ddl = ""
            elif referenced:
                item.reason = (
                    "CHECK verified present on the destination catalog after CREATE "
                    "(column coverage confirmed; engines may rewrite the predicate "
                    "text, so exact-expression equivalence is not asserted)."
                )
            else:
                # A CHECK exists on the table, but the carried predicate's columns
                # could not be resolved against the destination catalog — coverage
                # is unproven, so this is unknown, never a green carried claim.
                item.status = "unknown"
                item.reason = (
                    "A CHECK constraint is present on the destination table but the "
                    "carried predicate's columns could not be matched to certify "
                    "coverage. Emitted DDL is not proof."
                )
            continue

        # not_null — single-column membership (casefolded)
        observed = nn_cols
        label = "NOT NULL"
        if observed is None:
            item.status = "unknown"
            item.reason = (
                f"The destination catalog could not be read after CREATE, so {label} "
                f"on {item.name!r} is unverified. Emitted DDL is not proof. "
                f"Would have: {item.reason}"
            )
            continue
        present = {c.casefold() for c in observed}
        if folded_name and folded_name not in present:
            item.status = "unsupported"
            item.reason = (
                f"The destination did not take {label} on {item.name!r}. "
                f"Attempted: {item.dest_ddl or label}."
            )
            item.dest_ddl = ""
        else:
            item.reason = (
                f"{label} verified on the destination catalog after CREATE."
            )


def plan_covers_unique(
    plan: CreateFidelityPlan | None,
    columns: list[str] | None,
) -> bool:
    """True when CREATE already emits a PK or UNIQUE on exactly these columns.

    Upsert writers must not add a second UNIQUE KEY for conflict columns the
    Property 6 plan already carried — that is a duplicate index, not a
    write-path requirement.
    """
    if plan is None or not columns:
        return False
    want = {str(c).casefold() for c in columns if str(c).strip()}
    if not want:
        return False
    if plan.primary_key and {str(c).casefold() for c in plan.primary_key} == want:
        return True
    return any(
        {str(c).casefold() for c in uk} == want
        for uk in (plan.unique_constraints or [])
    )


def settle_create_new_on_destination(
    plan: CreateFidelityPlan | None,
    *,
    dest_dialect: str,
    dest_schema: str,
    dest_table: str,
    table_already_exists: bool,
    execute: Callable[[str], Any],
    fetchall: Callable[[str, tuple[Any, ...]], Any],
    cursor: Any | None = None,
) -> dict[str, Any] | None:
    """Run post-CREATE DDL and certify from the destination catalog.

    Callers render CREATE from the plan, then call this. An emitted clause is a
    claim; only the destination catalog may settle ``carried``.
    """
    if plan is None:
        return None
    apply_post_create_sql(plan, execute)
    if not table_already_exists:
        certify_structure_on_destination(
            plan,
            dialect=dest_dialect,
            schema=dest_schema,
            table=dest_table,
            fetchall=fetchall,
        )
        certify_identity_on_destination(
            plan,
            dialect=dest_dialect,
            schema=dest_schema,
            table=dest_table,
            fetchall=fetchall,
        )
        certify_collation_on_destination(
            plan,
            dialect=dest_dialect,
            schema=dest_schema,
            table=dest_table,
            fetchall=fetchall,
        )
        if cursor is not None:
            certify_placement_on_destination(
                plan,
                dialect=dest_dialect,
                cursor=cursor,
                schema=dest_schema,
                table=dest_table,
            )
    return plan.report.to_dict()


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


def _mysql_index_requires_prefix(typ: str) -> bool:
    """True when MySQL/MariaDB would need a prefix length to index this type.

    TEXT/BLOB/JSON cannot be a PRIMARY KEY or UNIQUE without a prefix. Inventing
    ``(255)`` would enforce a different uniqueness rule than the source, so the
    planner refuses rather than approximating.
    """
    u = re.sub(r"\s+", "", (typ or "").strip().upper())
    if not u:
        return False
    return bool(re.match(r"^(TINY|MEDIUM|LONG)?(TEXT|BLOB)\b|^JSON\b", u))


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
        # Legacy catalog shape: a list of predicate strings, no meta envelope.
        # Same planner — never a second "v1 unsupported" path that drops a
        # portable IN/comparison CHECK the meta path would carry.
        raw = [
            str(pred).strip()
            for pred in (catalog.check_constraints or [])
            if str(pred).strip()
        ]
        if not raw:
            return False
        payload = {
            "dialect": catalog.dialect,
            "status": "measured",
            "items": [
                {"name": f"check_{idx}", "predicate": pred, "columns": []}
                for idx, pred in enumerate(raw, start=1)
            ],
        }
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
    skip_identity: bool = False,
    skip_collation: bool = False,
    skip_offset_label: bool = False,
    skip_encoding: bool = False,
    skip_charset: bool = False,
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
    else:
        # No CHECK catalog was measured (dialect without a reader, or probe
        # never ran). Absence is unproven — never a silent skip.
        report.items.append(
            SchemaFidelityItem(
                aspect="check",
                name="*",
                status="unsupported",
                reason=(
                    "CHECK constraints were not measured on this source catalog; "
                    "refuse to certify absence."
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
    if not skip_identity:
        _aspect_list(
            "identity_sequence",
            bool(catalog.identity_columns),
            "Identity/sequence RESTART values are not carried; SERIAL polarity may widen.",
            "No identity columns flagged on source.",
        )
    if not skip_collation:
        _aspect_list(
            "collation",
            bool(catalog.collations),
            "Collation equality could not be planned for this destination.",
            "No per-column collations on source catalog.",
        )
    if not skip_offset_label:
        _aspect_list(
            "offset_label",
            False,
            "Originating offset label could not be planned for this destination.",
            "No aware-temporal columns on the source catalog.",
        )
    if not skip_encoding:
        _aspect_list(
            "encoding",
            bool(catalog.charsets),
            "Unicode encoding capacity could not be planned for this destination.",
            "No character columns on the source catalog.",
        )
    if not skip_charset:
        if catalog.charsets:
            report.items.append(
                SchemaFidelityItem(
                    aspect="charset",
                    name="*",
                    status="unsupported",
                    reason=(
                        "Source character sets were measured but not emitted on "
                        "create-new for this destination."
                    ),
                )
            )
        else:
            report.items.append(
                SchemaFidelityItem(
                    aspect="charset",
                    name="*",
                    status="skipped",
                    reason="No per-column character set measured on the source catalog.",
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
