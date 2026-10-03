"""FOREIGN KEY metadata — one measured shape for every SQL dialect.

Referential integrity is the one schema aspect a row-by-row transfer cannot
infer: it lives between tables, so a single-table create can neither carry nor
disprove it. Until now only PostgreSQL and MySQL introspection read foreign
keys at all, and the fidelity certificate had to say "not introspected" on SQL
Server, Oracle and SQLite — honest, but it also meant a destination could be
declared faithful while every parent/child relationship was gone.

This module measures the foreign keys of one table and says whether it managed
to. ``status="unavailable"`` keeps "the catalog could not be read" distinct
from "this table has no foreign keys": the second is proof, the first is not.

Shape mirrors ``services.physical_storage_metadata``: one connector-agnostic
entry point with a per-dialect catalog query, reused by source introspection,
by the carry planner and by the post-load destination re-read, so all three
compare like for like.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from services.dialect_profiles import normalize_driver
from services.fk_tuple_scan import normalize_match
from services.physical_storage_metadata import as_driver_cursor

logger = logging.getLogger(__name__)

ForeignKeyStatus = Literal["measured", "unavailable"]

SUPPORTED_DIALECTS = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "oracle",
    "sqlite",
})

# Referential actions we reproduce verbatim. Anything else (SET DEFAULT on
# engines that only parse it, Oracle's absent ON UPDATE) is reported, never
# silently downgraded to NO ACTION.
KNOWN_ACTIONS = frozenset({"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"})


@dataclass(frozen=True)
class ForeignKey:
    """One foreign key as the source catalog holds it."""

    name: str
    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str]
    on_delete: str = ""
    on_update: str = ""
    #: True when the catalog records that existing rows were checked.
    #: False when it records that they were not (PostgreSQL NOT VALID,
    #: SQL Server untrusted or disabled, Oracle NOT VALIDATED).
    #: None when this dialect has no separate validation bit.
    validated: bool | None = None
    #: ``""`` when the catalog did not name a match type. ``simple``,
    #: ``full``, ``partial``, and ``unknown`` are :func:`normalize_match`.
    match: str = ""
    #: ``""`` when this read did not name a deferral mode.
    #: ``not_deferrable``, ``immediate``, ``deferred``, and ``unknown`` are
    #: :func:`normalize_deferral`. The relationship identity does not include
    #: it: NOT DEFERRABLE and INITIALLY DEFERRED are one relationship with
    #: two check times.
    deferral: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ForeignKeys:
    """Measured foreign keys of one table, or an honest failure to measure."""

    dialect: str
    status: ForeignKeyStatus
    table: str = ""
    schema: str = ""
    detail: str = ""
    items: list[ForeignKey] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dialect": self.dialect,
            "status": self.status,
            "table": self.table,
            "schema": self.schema,
            "detail": self.detail,
            "items": [i.to_dict() for i in self.items],
        }

    @property
    def measured(self) -> bool:
        return self.status == "measured"


def _unavailable(dialect: str, detail: str, schema: str, table: str) -> ForeignKeys:
    return ForeignKeys(
        dialect=dialect, status="unavailable", detail=detail, schema=schema, table=table
    )


def normalize_action(action: str | None) -> str:
    """Uppercase a referential action; empty when the catalog did not report one."""
    text = str(action or "").strip().upper().replace("_", " ")
    if not text or text == "NONE":
        return ""
    return text if text in KNOWN_ACTIONS else text


def _as_bool(value: Any) -> bool | None:
    """A catalog boolean, or None when the value is not a yes/no bit."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    text = str(value).strip().casefold()
    if text in {"1", "t", "true", "y", "yes"}:
        return True
    if text in {"0", "f", "false", "n", "no"}:
        return False
    return None


def _deferral_spelling(value: Any) -> str:
    """One deferral token. ``""`` when this read did not name a mode."""
    if value is None:
        return ""
    text = " ".join(str(value).strip().casefold().replace("_", " ").split())
    if not text or text == "unreported":
        return ""
    if text in {"not deferrable", "nondeferrable", "non deferrable"}:
        return "not_deferrable"
    if text in {"immediate", "initially immediate", "deferrable initially immediate"}:
        return "immediate"
    if text in {"deferred", "initially deferred", "deferrable initially deferred"}:
        return "deferred"
    if text == "deferrable":
        return "immediate"
    if text == "unknown":
        return "unknown"
    if "not deferrable" in text:
        rest = text.replace("not deferrable", " ")
        if "deferrable" in rest:
            return "unknown"
        return "not_deferrable"
    if "initially deferred" in text or text.endswith(" deferred"):
        return "deferred"
    if "deferrable" in text or "initially immediate" in text:
        return "immediate"
    return "unknown"


def _deferral_pair(deferrable: Any, initially_deferred: Any) -> str | None:
    """Postgres ``condeferrable``/``condeferred`` or Oracle DEFERRABLE/DEFERRED."""
    defer_text = "" if deferrable is None else str(deferrable).strip()
    initial_text = "" if initially_deferred is None else str(initially_deferred).strip()
    if not defer_text and not initial_text and deferrable is None and initially_deferred is None:
        return ""
    if not defer_text and not initial_text:
        return ""
    flag = _as_bool(deferrable)
    initial = _as_bool(initially_deferred)
    folded_defer = " ".join(defer_text.casefold().replace("_", " ").split())
    folded_initial = " ".join(initial_text.casefold().replace("_", " ").split())
    if folded_defer in {"not deferrable", "non deferrable"}:
        if folded_initial in {"deferred", "initially deferred"} or initial is True:
            return "unknown"
        return "not_deferrable"
    if flag is False:
        if initial is True or folded_initial in {"deferred", "initially deferred"}:
            return "unknown"
        return "not_deferrable"
    if folded_defer == "deferrable" or flag is True:
        if initial is True or folded_initial in {"deferred", "initially deferred"}:
            return "deferred"
        return "immediate"
    return None


def normalize_deferral(
    deferrable: Any = None,
    initially_deferred: Any = None,
    *,
    spelling: Any = None,
) -> str:
    """Catalog deferral mode.

    ``""`` when this read did not name one. ``not_deferrable`` checks at the
    end of the statement and cannot be postponed. ``immediate`` is DEFERRABLE
    INITIALLY IMMEDIATE (including bare DEFERRABLE, whose SQL default is
    IMMEDIATE). ``deferred`` is DEFERRABLE INITIALLY DEFERRED. ``unknown`` is
    a pair or a spelling this rule does not recognize, including a constraint
    that is both deferred and not deferrable.
    """
    if deferrable is None and initially_deferred is None:
        return _deferral_spelling(spelling)
    parsed = _deferral_pair(deferrable, initially_deferred)
    if parsed is not None:
        return parsed
    if spelling is not None:
        return _deferral_spelling(spelling)
    return "unknown"


def deferral_label(mode: str) -> str:
    """Operator name for a deferral mode. Unreported is the SQL default."""
    kind = normalize_deferral(spelling=mode)
    if kind == "deferred":
        return "DEFERRABLE INITIALLY DEFERRED"
    if kind == "immediate":
        return "DEFERRABLE INITIALLY IMMEDIATE"
    if kind == "unknown":
        return "an unreadable deferral mode"
    if kind == "not_deferrable":
        return "NOT DEFERRABLE"
    return "NOT DEFERRABLE (unreported)"


def deferral_modes_agree(planned: str, measured: str) -> bool:
    """True when the destination checks at the same time as the source.

    Unreported is NOT DEFERRABLE, the SQL default. Neither direction is a
    stricter rule that still keeps the promise. A destination that can
    postpone the check accepts a state the source would reject at the
    statement. A destination that cannot postpone rejects an intermediate
    state the source accepts until commit.
    """
    want = normalize_deferral(spelling=planned) or "not_deferrable"
    got = normalize_deferral(spelling=measured) or "not_deferrable"
    if want == "unknown" or got == "unknown":
        return False
    return want == got


def deferral_disagreement(planned: str, measured: str) -> str:
    """Why the destination deferral mode does not keep the source rule.

    Empty when :func:`deferral_modes_agree` is true. An unreported mode is
    NOT DEFERRABLE. ``unknown`` means the catalog named two modes, or a pair
    that cannot exist, so the rule was not certified.
    """
    want = normalize_deferral(spelling=planned)
    got = normalize_deferral(spelling=measured)
    if want == "unknown" or got == "unknown":
        return (
            "Foreign key deferral mode could not be read, so the "
            "relationship was not certified."
        )
    if deferral_modes_agree(planned, measured):
        return ""
    return (
        f"Destination checks this relationship as {deferral_label(measured)}; "
        f"the source rule is {deferral_label(planned)}. "
        "A different deferral mode is not the source rule."
    )


def _rows(cursor: Any, sql: str, params: tuple | dict) -> list[tuple]:
    cursor.execute(sql, params)
    return list(cursor.fetchall() or [])


def _rows_any_paramstyle(cursor: Any, sql: str, params: tuple) -> list[tuple]:
    """SQL Server arrives through pymssql (``%s``) and pyodbc (``?``) alike."""
    last: Exception | None = None
    for style in ("%s", "?"):
        try:
            return _rows(cursor, sql.replace("{p}", style), params)
        except Exception as exc:  # noqa: BLE001 — driver paramstyle fallback
            last = exc
    raise last if last else RuntimeError("no paramstyle attempted")


def coerce_validated(value: Any) -> bool | None:
    """The catalog's existing-row bit, or None when this value does not say.

    ``NOT VALIDATED`` is checked before ``VALIDATED`` so the longer Oracle
    spelling is not read as a yes.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"", "none"}:
        return None
    if text in {"0", "f", "false", "no", "not validated", "not_validated"}:
        return False
    if text in {"1", "t", "true", "yes", "validated"}:
        return True
    return None


# These catalogs record whether existing rows were checked. MySQL and SQLite
# do not: the constraint itself is the check they report.
_VALIDATION_BIT_DIALECTS = frozenset(
    {"postgresql", "sqlserver", "mssql", "oracle"}
)

# These engines store a primary key, a unique constraint, and a foreign key
# and do not check rows against them. The object is planner metadata.
# A stored bit cannot override that: there is no check to report.
# Redshift: never enforced.
# BigQuery: only NOT ENFORCED is supported.
# Databricks: primary, foreign, and unique keys are informational.
# Snowflake: not enforced on a standard table. A hybrid table does enforce
# them. INFORMATION_SCHEMA.TABLES.IS_HYBRID is the measurement (YES or NO).
# SHOW TABLES is_hybrid is the same fact as a boolean. The dialect string
# is not that column. An unreported kind stays unenforced. The orphan scan
# still runs until the kind is hybrid, and a carried unique on a standard
# table is not a duplicate-row count.
_INFORMATIONAL_KEY_DIALECTS = frozenset(
    {"redshift", "snowflake", "bigquery", "databricks"}
)
_UNENFORCED_FK_DIALECTS = _INFORMATIONAL_KEY_DIALECTS

# Hosted Databricks names that are this engine. Hive, Spark, and Flink are
# not in this set. ``databricks_sql`` is already folded by normalize_driver.
_DATABRICKS_FAMILY = frozenset(
    {
        "databricks",
        "databricks_azure",
        "databricks_aws",
        "databricks_gcp",
        "unity_catalog",
    }
)


def _dialect_key(dialect: str) -> str:
    """Engine family for the row-proof rule.

    Hosted twins use the family their copy path already names, so
    ``snowflake_aws`` and ``google_bigquery`` are not a second rule.
    """
    key = normalize_driver(dialect)
    if key == "postgres":
        key = "postgresql"
    from services.copy_bigquery_common import bigquery_family_name
    from services.copy_snowflake_common import snowflake_family_name

    key = snowflake_family_name(key)
    key = bigquery_family_name(key)
    if key in _DATABRICKS_FAMILY:
        return "databricks"
    return key


def normalize_snowflake_table_kind(value: Any) -> str:
    """Measured Snowflake table kind. Empty when this read did not say.

    ``INFORMATION_SCHEMA.TABLES.IS_HYBRID`` is ``YES`` or ``NO``.
    ``SHOW TABLES`` ``is_hybrid`` is a boolean. A dialect name, a constraint
    ``ENFORCED`` flag, and ``TABLE_TYPE`` (``BASE TABLE`` for both kinds)
    are not this value. An unrecognized spelling stays unreported.
    """
    if value is True:
        return "hybrid"
    if value is False:
        return "standard"
    if value is None:
        return ""
    text = str(value).strip().lower()
    if text in {"hybrid", "yes", "y", "true"}:
        return "hybrid"
    if text in {"standard", "no", "n", "false"}:
        return "standard"
    if text == "iceberg":
        return "iceberg"
    if text == "dynamic":
        return "dynamic"
    if text in {"immutable", "read only", "readonly"}:
        return "immutable"
    return ""


def _snowflake_flag_yes(value: Any) -> bool:
    """True for the documented ``YES`` flag and a boolean true."""
    if value is True:
        return True
    text = str(value or "").strip().lower()
    return text in {"yes", "y", "true"}


def snowflake_table_kind_from_row(row: Any) -> str:
    """Kind from one ``INFORMATION_SCHEMA.TABLES`` row.

    Four cells are ``IS_HYBRID``, ``IS_ICEBERG``, ``IS_DYNAMIC``,
    ``IS_IMMUTABLE``. One cell is the hybrid-only read. Hybrid wins when
    ``IS_HYBRID`` is ``YES``, because that is the table that enforces keys.
    Iceberg, dynamic, and read-only tables do not.
    """
    if row is None:
        return ""
    if not isinstance(row, (tuple, list)):
        return normalize_snowflake_table_kind(row)
    cells = list(row)
    if len(cells) == 1:
        return normalize_snowflake_table_kind(cells[0])
    if len(cells) < 4:
        return ""
    hybrid, iceberg, dynamic, immutable = cells[:4]
    if _snowflake_flag_yes(hybrid):
        return "hybrid"
    if _snowflake_flag_yes(iceberg):
        return "iceberg"
    if _snowflake_flag_yes(dynamic):
        return "dynamic"
    if _snowflake_flag_yes(immutable):
        return "immutable"
    if normalize_snowflake_table_kind(hybrid) == "standard":
        return "standard"
    return ""


def _snowflake_measured_kind_reason(kind: str, *, foreign_key: bool) -> str:
    """Sentence for a measured non-hybrid Snowflake table. Empty when unreported."""
    if foreign_key:
        stored = "Destination stores this foreign key and does not enforce it. "
        proof = "The catalog fact is not proof the loaded rows match."
    else:
        stored = (
            "Destination stores this primary key or unique constraint and "
            "does not enforce it. "
        )
        proof = "The catalog object is not proof the loaded rows are unique."
    if kind == "standard":
        return (
            stored
            + "INFORMATION_SCHEMA.TABLES.IS_HYBRID is NO, so this standard "
            "table keeps the key for the planner. "
            + proof
        )
    if kind == "iceberg":
        return (
            stored
            + "INFORMATION_SCHEMA.TABLES.IS_ICEBERG is YES. An Iceberg table "
            "keeps the key for the planner. "
            + proof
        )
    if kind == "dynamic":
        return (
            stored
            + "INFORMATION_SCHEMA.TABLES.IS_DYNAMIC is YES. A dynamic table "
            "is a pipeline result, and its key is planner metadata. "
            + proof
        )
    if kind == "immutable":
        return (
            stored
            + "INFORMATION_SCHEMA.TABLES.IS_IMMUTABLE is YES. A read-only "
            "table still does not prove the stored rows with this key. "
            + proof
        )
    return ""


def informational_key_engine(dialect: str) -> bool:
    """True when this engine stores PK, UNIQUE, and FK and does not check rows.

    Snowflake hybrid tables do enforce those keys. The dialect name does not
    say the table is hybrid. :func:`normalize_snowflake_table_kind` is the
    catalog fact that does.
    """
    return _dialect_key(dialect) in _INFORMATIONAL_KEY_DIALECTS


def normalize_snowflake_index_status(value: Any) -> str:
    """``SHOW INDEXES`` status, or empty when this cell was not a known status.

    ``ACTIVE`` is the only status that proves rows already stored.
    ``BUILD IN PROGRESS`` is still building. ``BUILD FAILURE`` and
    ``BUILD VALIDATION FAILURE`` mean the build did not validate existing
    rows. ``SUSPENDED`` is not a completed check. New writes can still be
    rejected while the status is a validation failure.
    """
    text = " ".join(str(value or "").strip().upper().split())
    if text == "ACTIVE":
        return "active"
    if text in {"BUILD IN PROGRESS", "BUILDING"}:
        return "building"
    if text in {"BUILD VALIDATION FAILURE", "BUILD FAILURE", "FAILED"}:
        return "failed"
    if text == "SUSPENDED":
        return "suspended"
    return ""


def snowflake_index_proof_gap(index_status: str) -> str:
    """Existing-row gap from one summarized ``SHOW INDEXES`` status.

    Empty only for ``ACTIVE``. A failed or suspended index was measured and
    is not that proof. Any other spelling, including an unread command, stays
    unreported.
    """
    status = normalize_snowflake_index_status(index_status)
    if status == "active":
        return ""
    if status in {"failed", "suspended"}:
        return "not_checked"
    return "unreported"


def _snowflake_index_status_reason(index_status: str, *, foreign_key: bool) -> str:
    """Sentence for a hybrid table whose index build is not ACTIVE."""
    status = normalize_snowflake_index_status(index_status)
    noun = "foreign key" if foreign_key else "primary key or unique constraint"
    if status == "building":
        return (
            f"Destination stores this {noun} on a hybrid table. "
            "SHOW INDEXES status is BUILD IN PROGRESS. The index does not "
            "yet prove the rows already stored. New writes are still enforced."
        )
    if status == "failed":
        return (
            f"Destination stores this {noun} on a hybrid table. "
            "SHOW INDEXES did not finish as ACTIVE (BUILD FAILURE or "
            "BUILD VALIDATION FAILURE). Existing rows were not validated. "
            "New writes are still rejected."
        )
    if status == "suspended":
        return (
            f"Destination stores this {noun} on a hybrid table. "
            "SHOW INDEXES status is SUSPENDED. That index is not proof the "
            "rows already stored were checked."
        )
    return (
        f"Destination stores this {noun} on a hybrid table. "
        "SHOW INDEXES did not report ACTIVE. "
        "INFORMATION_SCHEMA.TABLE_CONSTRAINTS can list ENFORCED YES while "
        "the index is still building or was not read. That is not proof "
        "the rows already stored were checked."
    )


def uniqueness_proof_gap(
    dialect: str, *, table_kind: str = "", index_status: str = ""
) -> str:
    """Why a catalog primary key or unique constraint does not prove the rows.

    Empty when the engine rejects a duplicate and the existing rows were
    checked. ``unenforced`` when the catalog object is planner metadata.
    A stray ``enforced=True`` on the key dict cannot override a Snowflake
    dialect. A measured ``IS_HYBRID`` of ``YES`` can reject a new duplicate.
    Existing rows on that hybrid table are proven only when ``SHOW INDEXES``
    status is ``ACTIVE``. The same hybrid label on BigQuery, Redshift, or
    Databricks does not.
    """
    if (
        _dialect_key(dialect) == "snowflake"
        and normalize_snowflake_table_kind(table_kind) == "hybrid"
    ):
        return snowflake_index_proof_gap(index_status)
    if informational_key_engine(dialect):
        return "unenforced"
    return ""


def uniqueness_proof_reason(
    dialect: str, *, table_kind: str = "", index_status: str = ""
) -> str:
    """Operator sentence for a non-empty :func:`uniqueness_proof_gap`.

    Empty when this engine rejects a duplicate and the index build is
    ACTIVE. The sentence is not emitted for Postgres, SQL Server, or an
    unnamed dialect.
    """
    gap = uniqueness_proof_gap(
        dialect, table_kind=table_kind, index_status=index_status
    )
    if not gap:
        return ""
    if gap != "unenforced" and _dialect_key(dialect) == "snowflake":
        return _snowflake_index_status_reason(index_status, foreign_key=False)
    if gap != "unenforced":
        return ""
    key = _dialect_key(dialect)
    if key == "redshift":
        return (
            "Destination stores this primary key or unique constraint and "
            "does not enforce it. A Redshift key is visible to the planner "
            "and is not proof the loaded rows are unique."
        )
    if key == "bigquery":
        return (
            "Destination stores this primary key or unique constraint and "
            "does not enforce it. BigQuery accepts only NOT ENFORCED, so "
            "the catalog object is not proof the loaded rows are unique."
        )
    if key == "databricks":
        return (
            "Destination stores this primary key or unique constraint and "
            "does not enforce it. A Databricks primary key or unique "
            "constraint is informational and is not proof the loaded rows "
            "are unique."
        )
    if key == "snowflake":
        measured = _snowflake_measured_kind_reason(
            normalize_snowflake_table_kind(table_kind), foreign_key=False
        )
        if measured:
            return measured
        return (
            "Destination stores this primary key or unique constraint and "
            "does not enforce it. A Snowflake key on a standard table is "
            "not proof the loaded rows are unique. A hybrid table does "
            "enforce the key; this dialect name does not say the table "
            "is hybrid."
        )
    return (
        "Destination stores this primary key or unique constraint and does "
        "not enforce it. Redshift, BigQuery, and Databricks keep the key "
        "for the planner. Snowflake does the same on a standard table. "
        "This catalog object is not proof the loaded rows are unique."
    )


def row_proof_gap(
    dialect: str,
    validated: bool | None,
    *,
    table_kind: str = "",
    index_status: str = "",
) -> str:
    """Why a catalog foreign key does not prove the rows already stored.

    Empty when it does. ``unenforced`` is an engine that never checks the
    constraint. ``not_checked`` is a bit that says the check was skipped.
    ``unreported`` is an engine that has the bit and did not return it.
    A Snowflake hybrid table enforces a foreign key. Existing rows are
    proven only when ``IS_HYBRID`` is ``YES``, ``ENFORCED`` is ``YES``, and
    ``SHOW INDEXES`` status is ``ACTIVE``.
    """
    key = _dialect_key(dialect)
    if key == "snowflake" and normalize_snowflake_table_kind(table_kind) == "hybrid":
        if validated is True:
            return snowflake_index_proof_gap(index_status)
        if validated is False:
            return "not_checked"
        return "unreported"
    if key in _UNENFORCED_FK_DIALECTS:
        return "unenforced"
    if key in _VALIDATION_BIT_DIALECTS:
        if validated is True:
            return ""
        if validated is False:
            return "not_checked"
        return "unreported"
    if validated is False:
        return "not_checked"
    return ""


def covers_existing_rows(
    dialect: str,
    validated: bool | None,
    *,
    table_kind: str = "",
    index_status: str = "",
) -> bool:
    """Whether a catalog foreign key proves the rows already stored."""
    return (
        row_proof_gap(
            dialect, validated, table_kind=table_kind, index_status=index_status
        )
        == ""
    )


def row_proof_reason(
    gap: str,
    dialect: str = "",
    *,
    table_kind: str = "",
    index_status: str = "",
) -> str:
    """Operator sentence for a non-empty :func:`row_proof_gap`.

    Empty when the catalog fact proves the rows. Carry and the catalog diff
    share this sentence, so one relationship is not described two ways.
    ``dialect`` names the destination engine. An empty dialect keeps the
    class sentence, because the caller did not say which engine it was.
    """
    if gap == "unenforced":
        key = _dialect_key(dialect) if dialect else ""
        if key == "redshift":
            return (
                "Destination stores this foreign key and does not enforce "
                "it. A Redshift constraint is visible to the planner and "
                "is not proof the loaded rows match."
            )
        if key == "bigquery":
            return (
                "Destination stores this foreign key and does not enforce "
                "it. BigQuery accepts only NOT ENFORCED, so the constraint "
                "is not proof the loaded rows match."
            )
        if key == "databricks":
            return (
                "Destination stores this foreign key and does not enforce "
                "it. A Databricks foreign key is informational and is not "
                "proof the loaded rows match."
            )
        if key == "snowflake":
            measured = _snowflake_measured_kind_reason(
                normalize_snowflake_table_kind(table_kind), foreign_key=True
            )
            if measured:
                return measured
            return (
                "Destination stores this foreign key and does not enforce "
                "it. A Snowflake foreign key on a standard table is visible "
                "to the planner and is not proof the loaded rows match. "
                "A hybrid table does enforce the key; this dialect name does "
                "not say the table is hybrid."
            )
        return (
            "Destination stores this foreign key and does not enforce "
            "it. Redshift, BigQuery, and Databricks keep the constraint "
            "for the planner. Snowflake does the same on a standard table. "
            "This catalog fact is not proof the loaded rows match."
        )
    if gap == "unreported":
        if (
            _dialect_key(dialect) == "snowflake"
            and normalize_snowflake_table_kind(table_kind) == "hybrid"
        ):
            if normalize_snowflake_index_status(index_status):
                return _snowflake_index_status_reason(
                    index_status, foreign_key=True
                )
            return (
                "Destination table is a hybrid table. Existing rows are "
                "proven only when ENFORCED is YES and SHOW INDEXES status "
                "is ACTIVE. This catalog did not report both, so the "
                "relationship is not proof the loaded rows match."
            )
        return (
            "Destination reports this relationship, and the catalog did "
            "not say whether existing rows were checked. The constraint "
            "is not that proof."
        )
    if gap == "not_checked":
        if (
            _dialect_key(dialect) == "snowflake"
            and normalize_snowflake_table_kind(table_kind) == "hybrid"
        ):
            if normalize_snowflake_index_status(index_status) in {
                "failed",
                "suspended",
            }:
                return _snowflake_index_status_reason(
                    index_status, foreign_key=True
                )
            return (
                "Destination reports this relationship on a hybrid table, "
                "and INFORMATION_SCHEMA.TABLE_CONSTRAINTS.ENFORCED is NO. "
                "That catalog fact does not prove the loaded rows match."
            )
        return (
            "Destination reports this relationship, and the catalog records "
            "that existing rows were not checked. A PostgreSQL NOT VALID "
            "constraint, a SQL Server foreign key that is untrusted or "
            "disabled, or an Oracle NOT VALIDATED constraint does not prove "
            "the loaded rows."
        )
    return ""


def validation_catalog_dialect(dialect: str) -> str | None:
    """Probe dialect for the validation bit, or None when no probe is required.

    Redshift, Snowflake, BigQuery, and Databricks are not asked for a
    validation bit here. Redshift has no ``convalidated`` column. A Snowflake
    hybrid table enforces foreign keys, and that proof is
    ``IS_HYBRID`` plus ``TABLE_CONSTRAINTS.ENFORCED``, not this probe.
    Asking the other engines for a validation bit would fail the catalog
    read or invent a yes. The unenforced rule covers them without a probe.
    """
    key = _dialect_key(dialect)
    if key in _UNENFORCED_FK_DIALECTS:
        return None
    if key in _VALIDATION_BIT_DIALECTS:
        return "sqlserver" if key == "mssql" else key
    return None


def catalog_probe_dialect(dialect: str) -> str | None:
    """Probe dialect for foreign-key actions and match, or None when unsupported.

    This is wider than :func:`validation_catalog_dialect`. SQLite and MySQL
    have no separate validation bit, and they do name ON DELETE and ON UPDATE.
    Redshift is unenforced and still names the actions the planner stored.
    """
    key = _dialect_key(dialect)
    if key == "mssql":
        key = "sqlserver"
    return key if key in _PROBES else None


def _collect(
    rows: list[tuple],
) -> list[ForeignKey]:
    """Group catalog rows into one foreign key per constraint name.

    Each row is ``(name, col, ref_schema, ref_table, ref_col, on_del, on_upd)``
    plus an optional existing-row flag, an optional match type, and an
    optional deferral mode. Rows must already be ordered by
    constraint then ordinal position: a composite key whose columns arrive
    out of order would reference the wrong column pairs. A False flag on any
    row of the constraint wins.
    """
    by_name: dict[str, dict[str, Any]] = {}
    for raw in rows:
        fields = tuple(raw)
        if len(fields) >= 8:
            name, col, ref_schema, ref_table, ref_col, on_delete, on_update, flag = fields[:8]
            validated = coerce_validated(flag)
        elif len(fields) >= 7:
            name, col, ref_schema, ref_table, ref_col, on_delete, on_update = fields[:7]
            validated = None
        else:
            continue
        match = normalize_match(fields[8]) if len(fields) >= 9 else ""
        deferral = normalize_deferral(spelling=fields[9]) if len(fields) >= 10 else ""
        key = str(name or "").strip()
        if not key:
            continue
        bucket = by_name.setdefault(
            key,
            {
                "columns": [],
                "referenced_columns": [],
                "referenced_schema": str(ref_schema or "").strip(),
                "referenced_table": str(ref_table or "").strip(),
                "on_delete": normalize_action(on_delete),
                "on_update": normalize_action(on_update),
                "validated": validated,
                "match": match,
                "deferral": deferral,
            },
        )
        if bucket["validated"] is not False and validated is False:
            bucket["validated"] = False
        elif bucket["validated"] is None:
            bucket["validated"] = validated
        if match and bucket["match"] and bucket["match"] != match:
            bucket["match"] = "unknown"
        elif match and not bucket["match"]:
            bucket["match"] = match
        if deferral and bucket["deferral"] and bucket["deferral"] != deferral:
            bucket["deferral"] = "unknown"
        elif deferral and not bucket["deferral"]:
            bucket["deferral"] = deferral
        col_s = str(col or "").strip()
        ref_s = str(ref_col or "").strip()
        if col_s:
            bucket["columns"].append(col_s)
        if ref_s:
            bucket["referenced_columns"].append(ref_s)
    return [
        ForeignKey(
            name=name,
            columns=list(b["columns"]),
            referenced_schema=str(b["referenced_schema"]),
            referenced_table=str(b["referenced_table"]),
            referenced_columns=list(b["referenced_columns"]),
            on_delete=str(b["on_delete"]),
            on_update=str(b["on_update"]),
            validated=b["validated"],
            match=str(b.get("match") or ""),
            deferral=str(b.get("deferral") or ""),
        )
        for name, b in by_name.items()
    ]


_PG_SQL_REDSHIFT = """
SELECT con.conname,
       att.attname,
       nsp_ref.nspname,
       cls_ref.relname,
       att_ref.attname,
       con.confdeltype,
       con.confupdtype,
       ord.n
  FROM pg_constraint con
  JOIN pg_class cls ON cls.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
  JOIN pg_class cls_ref ON cls_ref.oid = con.confrelid
  JOIN pg_namespace nsp_ref ON nsp_ref.oid = cls_ref.relnamespace
  JOIN LATERAL generate_subscripts(con.conkey, 1) AS ord(n) ON TRUE
  JOIN pg_attribute att
    ON att.attrelid = con.conrelid AND att.attnum = con.conkey[ord.n]
  JOIN pg_attribute att_ref
    ON att_ref.attrelid = con.confrelid AND att_ref.attnum = con.confkey[ord.n]
 WHERE con.contype = 'f' AND nsp.nspname = %s AND cls.relname = %s
 ORDER BY con.conname, ord.n
"""

# ``convalidated`` is PostgreSQL 9.1+. Redshift stores foreign keys and does
# not expose that column; its probe keeps the older select.
_PG_SQL = """
SELECT con.conname,
       att.attname,
       nsp_ref.nspname,
       cls_ref.relname,
       att_ref.attname,
       con.confdeltype,
       con.confupdtype,
       con.convalidated,
       ord.n,
       con.confmatchtype,
       con.condeferrable,
       con.condeferred
  FROM pg_constraint con
  JOIN pg_class cls ON cls.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
  JOIN pg_class cls_ref ON cls_ref.oid = con.confrelid
  JOIN pg_namespace nsp_ref ON nsp_ref.oid = cls_ref.relnamespace
  JOIN LATERAL generate_subscripts(con.conkey, 1) AS ord(n) ON TRUE
  JOIN pg_attribute att
    ON att.attrelid = con.conrelid AND att.attnum = con.conkey[ord.n]
  JOIN pg_attribute att_ref
    ON att_ref.attrelid = con.confrelid AND att_ref.attnum = con.confkey[ord.n]
 WHERE con.contype = 'f' AND nsp.nspname = %s AND cls.relname = %s
 ORDER BY con.conname, ord.n
"""

# pg_constraint stores the action as a single char; spelling it out here keeps
# the emitted DDL identical to what the source declared.
_PG_ACTIONS = {
    "a": "NO ACTION",
    "r": "RESTRICT",
    "c": "CASCADE",
    "n": "SET NULL",
    "d": "SET DEFAULT",
}


_DEFAULT_NAMESPACE_SQL = {
    "postgresql": "SELECT current_schema()",
    "mysql": "SELECT DATABASE()",
    "sqlserver": "SELECT SCHEMA_NAME()",
    "oracle": "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual",
    "snowflake": "SELECT CURRENT_SCHEMA()",
}


def _resolve_namespace(cursor: Any, dialect: str, schema: str) -> str:
    """The namespace the probe will actually read — never a blank one.

    A caller whose connector config keeps the namespace elsewhere (MySQL puts
    it in ``database``) used to hand an empty schema down here, and the catalog
    query then returned an empty *measured* answer: a carried foreign key read
    back as "not enforced on the destination". Resolving the session default is
    the only honest answer; an unresolvable one raises so the caller reports
    unknown rather than absent.
    """
    if schema:
        return schema
    sql = _DEFAULT_NAMESPACE_SQL.get(dialect)
    rows = _rows(cursor, sql, ()) if sql else []
    resolved = str(rows[0][0]) if rows and rows[0] and rows[0][0] else ""
    if not resolved:
        raise ValueError(
            f"No {dialect} schema/database bound on this connection; the foreign "
            "key catalog namespace is unknown, not empty."
        )
    return resolved


def _probe_pg(
    cursor: Any,
    schema: str,
    table: str,
    *,
    dialect: str,
    sql: str,
    with_validated: bool,
) -> ForeignKeys:
    schema = _resolve_namespace(cursor, "postgresql", schema)
    rows = _rows(cursor, sql, (schema, table))
    mapped: list[tuple] = []
    for row in rows:
        name, col, ref_schema, ref_table, ref_col, on_del, on_upd = row[:7]
        item = (
            name,
            col,
            ref_schema,
            ref_table,
            ref_col,
            _PG_ACTIONS.get(str(on_del or "").strip(), ""),
            _PG_ACTIONS.get(str(on_upd or "").strip(), ""),
        )
        if with_validated:
            item = (*item, row[7])
            # Ordinal stays at index 8. Match is the column after it, so an
            # older 9-tuple fixture (validated, ordinal) does not become a
            # match type. Deferral is the pair after match, so a 10-tuple
            # fixture (match, no deferral) stays unreported.
            if len(row) > 9:
                item = (*item, row[9])
            if len(row) > 11:
                item = (*item, normalize_deferral(row[10], row[11]))
        elif len(row) > 9:
            # Redshift selects the ordinal, then condeferrable, condeferred.
            # An older fixture that stops at the ordinal does not become a mode.
            item = (*item, None, "", normalize_deferral(row[8], row[9]))
        mapped.append(item)
    return ForeignKeys(
        dialect=dialect,
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


def _probe_postgres(cursor: Any, schema: str, table: str) -> ForeignKeys:
    return _probe_pg(
        cursor, schema, table, dialect="postgresql", sql=_PG_SQL, with_validated=True
    )


def _probe_redshift(cursor: Any, schema: str, table: str) -> ForeignKeys:
    return _probe_pg(
        cursor,
        schema,
        table,
        dialect="redshift",
        sql=_PG_SQL_REDSHIFT,
        with_validated=False,
    )


_MYSQL_SQL = """
SELECT k.CONSTRAINT_NAME,
       k.COLUMN_NAME,
       k.REFERENCED_TABLE_SCHEMA,
       k.REFERENCED_TABLE_NAME,
       k.REFERENCED_COLUMN_NAME,
       r.DELETE_RULE,
       r.UPDATE_RULE
  FROM information_schema.KEY_COLUMN_USAGE k
  JOIN information_schema.REFERENTIAL_CONSTRAINTS r
    ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
   AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
   AND r.TABLE_NAME = k.TABLE_NAME
 WHERE k.TABLE_SCHEMA = %s
   AND k.TABLE_NAME = %s
   AND k.REFERENCED_TABLE_NAME IS NOT NULL
 ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
"""


def _probe_mysql(cursor: Any, schema: str, table: str) -> ForeignKeys:
    # MySQL has no schema layer above the database: the namespace lives in
    # ``database``, so an empty schema must resolve to the session's own.
    schema = _resolve_namespace(cursor, "mysql", schema)
    rows = _rows(cursor, _MYSQL_SQL, (schema, table))
    return ForeignKeys(
        dialect="mysql",
        status="measured",
        schema=schema,
        table=table,
        # InnoDB has no DEFERRABLE foreign key. That is a measured fact, not
        # an unread column: the check cannot be postponed until commit.
        items=_collect(
            [(*tuple(r)[:7], None, "", "not_deferrable") for r in rows]
        ),
    )


_SQLSERVER_SQL = """
SELECT fk.name,
       cpa.name,
       SCHEMA_NAME(tref.schema_id),
       tref.name,
       cref.name,
       fk.delete_referential_action_desc,
       fk.update_referential_action_desc,
       fk.is_disabled,
       fk.is_not_trusted
  FROM sys.foreign_keys fk
  JOIN sys.tables t ON t.object_id = fk.parent_object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id
  JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
  JOIN sys.columns cpa
    ON cpa.object_id = fkc.parent_object_id
   AND cpa.column_id = fkc.parent_column_id
  JOIN sys.tables tref ON tref.object_id = fk.referenced_object_id
  JOIN sys.columns cref
    ON cref.object_id = fkc.referenced_object_id
   AND cref.column_id = fkc.referenced_column_id
 WHERE s.name = {p} AND t.name = {p}
 ORDER BY fk.name, fkc.constraint_column_id
"""


def _probe_sqlserver(cursor: Any, schema: str, table: str) -> ForeignKeys:
    schema = _resolve_namespace(cursor, "sqlserver", schema)
    rows = _rows_any_paramstyle(cursor, _SQLSERVER_SQL, (schema, table))
    mapped = []
    for row in rows:
        fields = tuple(row)
        disabled = coerce_validated(fields[7]) if len(fields) > 7 else None
        untrusted = coerce_validated(fields[8]) if len(fields) > 8 else None
        # Either bit means the engine did not check the rows already stored.
        # A missing bit is not a yes.
        checked = disabled is False and untrusted is False
        # SQL Server has no DEFERRABLE foreign key. The check cannot be
        # postponed, so the mode is measured rather than left unread.
        mapped.append((*fields[:7], checked, "", "not_deferrable"))
    return ForeignKeys(
        dialect="sqlserver",
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


_ORACLE_SQL = """
SELECT c.constraint_name,
       cc.column_name,
       rc.owner,
       rc.table_name,
       rcc.column_name,
       c.delete_rule,
       'NO ACTION',
       c.validated,
       c.deferrable,
       c.deferred
  FROM all_constraints c
  JOIN all_cons_columns cc
    ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
  JOIN all_constraints rc
    ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name
  JOIN all_cons_columns rcc
    ON rcc.owner = rc.owner
   AND rcc.constraint_name = rc.constraint_name
   AND rcc.position = cc.position
 WHERE c.constraint_type = 'R' AND c.owner = :owner AND c.table_name = :tab
 ORDER BY c.constraint_name, cc.position
"""


def _probe_oracle(cursor: Any, schema: str, table: str) -> ForeignKeys:
    # Oracle folds unquoted identifiers to upper case in the catalog; a lower
    # case argument would silently measure "no foreign keys".
    schema = _resolve_namespace(cursor, "oracle", schema)
    rows = _rows(
        cursor, _ORACLE_SQL, {"owner": schema.upper(), "tab": table.upper()}
    )
    mapped = []
    for row in rows:
        fields = tuple(row)
        flag = fields[7] if len(fields) > 7 else None
        checked = coerce_validated(flag)
        if checked is None:
            checked = False
        # DEFERRABLE and DEFERRED sit after VALIDATED. An older fixture that
        # stops at the validation flag does not become NOT DEFERRABLE.
        deferral = (
            normalize_deferral(fields[8], fields[9]) if len(fields) > 9 else ""
        )
        mapped.append((*fields[:7], checked, "", deferral))
    return ForeignKeys(
        dialect="oracle",
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


_SQL_IDENT = re.compile(
    r'"(?:[^"]|"")+"|\[(?:[^\]]|\]\])+\]|`(?:[^`]|``)+`|[A-Za-z_][A-Za-z0-9_]*'
)


def _sql_idents(text: str) -> list[str]:
    """Identifiers in ``text``, quotes removed, compared case-insensitively."""
    names: list[str] = []
    for match in _SQL_IDENT.finditer(text):
        token = match.group(0)
        if token[0] in {'"', "[", "`"}:
            inner = token[1:-1].replace(token[0] * 2, token[0])
            names.append(inner.casefold())
        else:
            names.append(token.casefold())
    return names


def _strip_sql_strings(text: str) -> str:
    """Replace single-quoted literals so a default cannot look like a clause."""
    out: list[str] = []
    index = 0
    length = len(text)
    while index < length:
        if text[index] != "'":
            out.append(text[index])
            index += 1
            continue
        index += 1
        while index < length:
            if text[index] == "'":
                if index + 1 < length and text[index + 1] == "'":
                    index += 2
                    continue
                index += 1
                break
            index += 1
        out.append("''")
    return "".join(out)


def _sqlite_table_segments(ddl: str) -> list[str]:
    """Top-level column and table constraints inside one CREATE TABLE."""
    segments: list[str] = []
    buf: list[str] = []
    depth = 0
    started = False
    in_string = False
    index = 0
    length = len(ddl)
    while index < length:
        char = ddl[index]
        if in_string:
            if started:
                buf.append(char)
            if char == "'":
                if index + 1 < length and ddl[index + 1] == "'":
                    if started:
                        buf.append("'")
                    index += 2
                    continue
                in_string = False
            index += 1
            continue
        if char == "'":
            in_string = True
            if started:
                buf.append(char)
            index += 1
            continue
        if char == "(":
            depth += 1
            if depth == 1 and not started:
                started = True
                buf = []
                index += 1
                continue
            if started:
                buf.append(char)
            index += 1
            continue
        if char == ")":
            if depth == 1 and started:
                segment = "".join(buf).strip()
                if segment:
                    segments.append(segment)
                return segments
            depth = max(0, depth - 1)
            if started:
                buf.append(char)
            index += 1
            continue
        if char == "," and depth == 1 and started:
            segment = "".join(buf).strip()
            if segment:
                segments.append(segment)
            buf = []
            index += 1
            continue
        if started:
            buf.append(char)
        index += 1
    return segments


def _deferral_in_clause(segment: str) -> str:
    """Mode named by one foreign-key clause. Absent keyword is NOT DEFERRABLE."""
    scrubbed = _strip_sql_strings(segment).casefold()
    without_not = re.sub(r"\bnot\s+deferrable\b", " ", scrubbed)
    has_not = without_not != scrubbed
    has_deferrable = re.search(r"\bdeferrable\b", without_not) is not None
    if has_not and has_deferrable:
        return "unknown"
    if has_not or not has_deferrable:
        return "not_deferrable"
    if re.search(r"\binitially\s+deferred\b", scrubbed):
        return "deferred"
    return "immediate"


def sqlite_clause_deferral(ddl: str, column: str, referenced_table: str) -> str:
    """Deferral of one SQLite foreign key, read from its CREATE TABLE text.

    ``""`` when the CREATE text is missing or no clause binds this column to
    that parent. SQLite can postpone a check, so an unread clause is not
    NOT DEFERRABLE. A clause that binds and does not say DEFERRABLE is
    NOT DEFERRABLE, which is the SQL default.
    """
    if not str(ddl or "").strip() or not str(column or "").strip():
        return ""
    wanted_column = column.casefold()
    wanted_table = referenced_table.casefold()
    found: list[str] = []
    for segment in _sqlite_table_segments(ddl):
        marker = re.search(r"\breferences\b", segment, re.IGNORECASE)
        if marker is None:
            continue
        if wanted_column not in _sql_idents(segment[: marker.start()]):
            continue
        head = segment[marker.end() :].split("(", 1)[0]
        if wanted_table not in _sql_idents(head):
            continue
        found.append(_deferral_in_clause(segment))
    if not found:
        return ""
    if any(mode != found[0] for mode in found):
        return "unknown"
    return found[0]


def _probe_sqlite(cursor: Any, schema: str, table: str) -> ForeignKeys:
    """``PRAGMA foreign_key_list`` — id, seq, table, from, to, on_update, on_delete.

    The pragma does not name DEFERRABLE. ``sqlite_master.sql`` does. A missing
    CREATE text stays unreported: SQLite can postpone a check, so an unread
    clause is not NOT DEFERRABLE.
    """
    from connectors.writer_common import quote_sql_identifier

    rows = _rows(cursor, f"PRAGMA foreign_key_list({quote_sql_identifier(table)})", ())
    ddl = ""
    try:
        ddl_rows = _rows(
            cursor,
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        )
    except Exception:  # noqa: BLE001 — an unread CREATE is not a mode
        ddl_rows = []
    if ddl_rows and ddl_rows[0] and ddl_rows[0][0]:
        ddl = str(ddl_rows[0][0])
    mapped: list[tuple] = []
    for row in rows:
        fk_id, _seq, ref_table, from_col, to_col, on_update, on_delete = list(row)[:7]
        deferral = sqlite_clause_deferral(ddl, str(from_col or ""), str(ref_table or ""))
        mapped.append(
            (
                f"fk_{table}_{fk_id}",
                from_col,
                "",
                ref_table,
                # SQLite reports NULL for "references the parent's primary key".
                to_col if to_col is not None else "",
                on_delete,
                on_update,
                None,
                "",
                deferral,
            )
        )
    return ForeignKeys(
        dialect="sqlite",
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


_SNOWFLAKE_TABLE_KIND_SQL = """
SELECT is_hybrid, is_iceberg, is_dynamic, is_immutable
  FROM information_schema.tables
 WHERE UPPER(table_schema) = UPPER(%s)
   AND table_name = %s
"""

_SNOWFLAKE_IS_HYBRID_SQL = """
SELECT is_hybrid
  FROM information_schema.tables
 WHERE UPPER(table_schema) = UPPER(%s)
   AND table_name = %s
"""


def read_snowflake_table_kind(cursor_or_connection: Any, schema: str, table: str) -> str:
    """Table kind from ``INFORMATION_SCHEMA.TABLES``. Empty when unread.

    The first read asks for hybrid, Iceberg, dynamic, and read-only. When
    that select fails, the hybrid column is read alone so an older account
    still reports a hybrid table. A failed read stays unreported.
    """
    params = (schema or "", table)
    try:
        cursor = as_driver_cursor(cursor_or_connection)
    except Exception:  # noqa: BLE001 — an unread kind is not a standard table
        return ""
    rows: list[tuple] = []
    try:
        rows = _rows(cursor, _SNOWFLAKE_TABLE_KIND_SQL, params)
    except Exception:  # noqa: BLE001 — fall back to the hybrid column
        try:
            rows = _rows(cursor, _SNOWFLAKE_IS_HYBRID_SQL, params)
        except Exception:  # noqa: BLE001 — an unread kind is not a standard table
            return ""
    if not rows or rows[0] is None:
        return ""
    return snowflake_table_kind_from_row(rows[0])


def _quote_snowflake_ident(name: str) -> str:
    """One Snowflake identifier, quoted so ``SHOW INDEXES`` cannot be rewritten."""
    return '"' + str(name).replace('"', '""') + '"'


def summarize_snowflake_index_statuses(rows: Any, columns: list[str]) -> str:
    """Worst ``SHOW INDEXES`` status on this table.

    Empty when no status column or no index row was returned. ``active``
    only when every reported index is ``ACTIVE``. A secondary index that is
    still building or failed validation keeps the table unproven, because
    ``TABLE_CONSTRAINTS`` does not say which index failed.
    """
    names = [str(name).strip().lower() for name in columns]
    if "status" not in names:
        return ""
    status_at = names.index("status")
    rank = {"": 1, "active": 0, "building": 2, "suspended": 3, "failed": 4}
    worst = ""
    worst_rank = -1
    saw = False
    for row in rows or []:
        cells = list(row)
        if status_at >= len(cells):
            continue
        saw = True
        status = normalize_snowflake_index_status(cells[status_at])
        score = rank.get(status, 1)
        if score > worst_rank:
            worst = status
            worst_rank = score
    if not saw:
        return ""
    return worst


def read_snowflake_index_status(
    cursor_or_connection: Any, schema: str, table: str
) -> str:
    """Summarized ``SHOW INDEXES`` status. Empty when the command was not read.

    A failed read stays unreported. It does not invent ``ACTIVE``.
    """
    if not str(table or "").strip():
        return ""
    try:
        cursor = as_driver_cursor(cursor_or_connection)
    except Exception:  # noqa: BLE001 — an unread index is not ACTIVE
        return ""
    ident = _quote_snowflake_ident(table)
    if str(schema or "").strip():
        ident = f"{_quote_snowflake_ident(schema)}.{ident}"
    try:
        cursor.execute(f"SHOW INDEXES IN TABLE {ident}")
        rows = list(cursor.fetchall() or [])
    except Exception:  # noqa: BLE001 — an unread index is not ACTIVE
        return ""
    description = getattr(cursor, "description", None) or []
    columns: list[str] = []
    for col in description:
        if isinstance(col, (tuple, list)) and col:
            columns.append(str(col[0]))
        else:
            columns.append(str(getattr(col, "name", col)))
    return summarize_snowflake_index_statuses(rows, columns)


_SNOWFLAKE_FK_SQL = """
SELECT tc.constraint_name,
       kcu.column_name,
       rc.unique_constraint_schema,
       pk_tc.table_name,
       pk_kcu.column_name,
       rc.delete_rule,
       rc.update_rule,
       tc.enforced,
       rc.match_option,
       tc.is_deferrable,
       tc.initially_deferred
  FROM information_schema.table_constraints tc
  JOIN information_schema.referential_constraints rc
    ON tc.constraint_catalog = rc.constraint_catalog
   AND tc.constraint_schema = rc.constraint_schema
   AND tc.constraint_name = rc.constraint_name
  JOIN information_schema.key_column_usage kcu
    ON tc.constraint_catalog = kcu.constraint_catalog
   AND tc.constraint_schema = kcu.constraint_schema
   AND tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
   AND tc.table_name = kcu.table_name
  JOIN information_schema.table_constraints pk_tc
    ON rc.unique_constraint_catalog = pk_tc.constraint_catalog
   AND rc.unique_constraint_schema = pk_tc.constraint_schema
   AND rc.unique_constraint_name = pk_tc.constraint_name
  JOIN information_schema.key_column_usage pk_kcu
    ON pk_tc.constraint_catalog = pk_kcu.constraint_catalog
   AND pk_tc.constraint_schema = pk_kcu.constraint_schema
   AND pk_tc.constraint_name = pk_kcu.constraint_name
   AND pk_tc.table_schema = pk_kcu.table_schema
   AND pk_tc.table_name = pk_kcu.table_name
   AND pk_kcu.ordinal_position = kcu.position_in_unique_constraint
 WHERE UPPER(tc.table_schema) = UPPER(%s)
   AND tc.table_name = %s
   AND tc.constraint_type = 'FOREIGN KEY'
 ORDER BY tc.constraint_name, kcu.ordinal_position
"""


def _probe_snowflake(cursor: Any, schema: str, table: str) -> ForeignKeys:
    """Foreign keys from Snowflake information_schema.

    ``ENFORCED`` is the existing-row bit. Hybrid tables record ``YES``.
    A standard table records ``NO``. ``MATCH_OPTION`` and the deferral pair
    are the rule the catalog stored. This probe does not read ``IS_HYBRID``;
    the table kind is a separate measurement.
    """
    schema = _resolve_namespace(cursor, "snowflake", schema)
    rows = _rows(cursor, _SNOWFLAKE_FK_SQL, (schema, table))
    mapped: list[tuple] = []
    for row in rows:
        name, col, ref_schema, ref_table, ref_col, on_del, on_upd = row[:7]
        enforced = row[7] if len(row) > 7 else None
        match = row[8] if len(row) > 8 else ""
        deferral = (
            normalize_deferral(row[9], row[10]) if len(row) > 10 else ""
        )
        mapped.append(
            (
                name,
                col,
                ref_schema,
                ref_table,
                ref_col,
                on_del,
                on_upd,
                enforced,
                match,
                deferral,
            )
        )
    return ForeignKeys(
        dialect="snowflake",
        status="measured",
        schema=schema,
        table=table,
        items=_collect(mapped),
    )


_PROBES = {
    "postgresql": _probe_postgres,
    "mysql": _probe_mysql,
    "mariadb": _probe_mysql,
    "sqlserver": _probe_sqlserver,
    "mssql": _probe_sqlserver,
    "oracle": _probe_oracle,
    "redshift": _probe_redshift,
    "sqlite": _probe_sqlite,
    "snowflake": _probe_snowflake,
}


def _inspector_action(fk: Mapping[str, Any], *keys: str) -> str:
    """One referential action from an inspector foreign key, or empty."""
    for key in keys:
        if key in fk and str(fk.get(key) or "").strip():
            return normalize_action(fk.get(key))
    options = fk.get("options")
    if isinstance(options, Mapping):
        for key in keys:
            if key in options and str(options.get(key) or "").strip():
                return normalize_action(options.get(key))
    return ""


def relationship_actions(
    identity: tuple[Any, ...] | None,
    measured: ForeignKeys | None,
    inspector_fks: list[Any],
) -> tuple[str, str]:
    """``(on_delete, on_update)`` the catalog recorded for this relationship.

    The metadata probe wins when it names an action. Inspector ``ondelete``
    and ``onupdate`` are the fallback SQLAlchemy keeps. Empty means
    unreported, which matches only the engine default NO ACTION. Two different
    actions on the probe are ``unknown``.
    """
    from services.foreign_key_identity import fk_identity, same_relationship

    if identity is None:
        return "", ""
    if measured is not None and measured.measured:
        seen: tuple[str, str] | None = None
        for item in measured.items:
            ident = fk_identity(
                {
                    "constrained_columns": item.columns,
                    "referred_schema": item.referenced_schema,
                    "referred_table": item.referenced_table,
                    "referred_columns": item.referenced_columns,
                }
            )
            if not same_relationship(identity, ident):
                continue
            pair = (
                normalize_action(item.on_delete),
                normalize_action(item.on_update),
            )
            if seen is not None and pair != seen:
                return "unknown", "unknown"
            seen = pair
        if seen is not None and (seen[0] or seen[1]):
            return seen
    for fk in inspector_fks:
        if not isinstance(fk, dict):
            continue
        if not same_relationship(identity, fk_identity(fk)):
            continue
        return (
            _inspector_action(fk, "ondelete", "on_delete"),
            _inspector_action(fk, "onupdate", "on_update"),
        )
    return "", ""


def relationship_match_type(
    identity: tuple[Any, ...] | None,
    measured: ForeignKeys | None,
    inspector_fks: list[Any],
) -> str:
    """Match type the catalog recorded for this relationship.

    The metadata probe wins. Inspector ``options["match"]`` is the fallback
    SQLAlchemy keeps when the DDL names the clause. Empty means unreported,
    which a scan treats as MATCH SIMPLE. Two different spellings on the probe
    are ``unknown``: the catalog did not name one rule.
    """
    from services.foreign_key_identity import (
        fk_identity,
        parse_foreign_key,
        same_relationship,
    )

    if identity is None:
        return ""
    if measured is not None and measured.measured:
        named = ""
        for item in measured.items:
            ident = fk_identity(
                {
                    "constrained_columns": item.columns,
                    "referred_schema": item.referenced_schema,
                    "referred_table": item.referenced_table,
                    "referred_columns": item.referenced_columns,
                    "match": item.match,
                }
            )
            if not same_relationship(identity, ident):
                continue
            kind = normalize_match(item.match)
            if kind and named and kind != named:
                return "unknown"
            if kind:
                named = kind
        if named:
            return named
    for fk in inspector_fks:
        if not isinstance(fk, dict):
            continue
        parsed = parse_foreign_key(fk)
        if parsed.conflict or not same_relationship(identity, fk_identity(fk)):
            continue
        return normalize_match(parsed.match)
    return ""


def relationship_deferral(
    identity: tuple[Any, ...] | None,
    measured: ForeignKeys | None,
    inspector_fks: list[Any],
) -> str:
    """Deferral mode the catalog recorded for this relationship.

    The metadata probe wins. Empty means unreported, which a comparison
    treats as NOT DEFERRABLE. Two different modes on the probe are
    ``unknown``: the catalog did not name one rule. Inspector options are
    the fallback only when the probe did not see this relationship.
    """
    from services.foreign_key_identity import fk_identity, same_relationship

    if identity is None:
        return ""
    if measured is not None and measured.measured:
        named = ""
        found = False
        for item in measured.items:
            ident = fk_identity(
                {
                    "constrained_columns": item.columns,
                    "referred_schema": item.referenced_schema,
                    "referred_table": item.referenced_table,
                    "referred_columns": item.referenced_columns,
                }
            )
            if not same_relationship(identity, ident):
                continue
            found = True
            kind = normalize_deferral(spelling=item.deferral)
            if kind and named and kind != named:
                return "unknown"
            if kind:
                named = kind
        if named or found:
            return named
    for fk in inspector_fks:
        if not isinstance(fk, dict):
            continue
        if not same_relationship(identity, fk_identity(fk)):
            continue
        options = fk.get("options") if isinstance(fk.get("options"), Mapping) else {}
        if "deferral" in fk:
            return normalize_deferral(spelling=fk.get("deferral"))
        if "deferrable" in fk or "deferrable" in options:
            flag = fk.get("deferrable", options.get("deferrable"))
            initial = fk.get("initially", options.get("initially"))
            return normalize_deferral(flag, initial)
    return ""


def inspector_row_proof_gaps(
    dialect: str,
    inspector_fks: list[Any],
    measured: ForeignKeys | None,
    *,
    table_kind: str = "",
    index_status: str = "",
) -> list[str]:
    """One :func:`row_proof_gap` per inspector foreign key, in that order.

    Carry, the destination scan, and the catalog diff all read this list.
    Redshift, Snowflake, BigQuery, and Databricks are ``unenforced`` without
    a validation query. A measured Snowflake ``IS_HYBRID`` of ``YES`` uses
    each constraint's ``validated`` bit (``TABLE_CONSTRAINTS.ENFORCED``)
    and ``SHOW INDEXES`` status. ``ACTIVE`` is the existing-row proof.
    PostgreSQL, SQL Server, and Oracle match the metadata probe
    by relationship identity.
    SQLAlchemy's PostgreSQL reflection omits ``NOT VALID``, so an inspector
    hit alone is ``unreported``, not a yes. An unreadable probe is the same.
    MySQL and SQLite have no separate bit: the constraint itself is the check.
    """
    from services.foreign_key_identity import fk_identity, same_relationship

    hybrid = (
        _dialect_key(dialect) == "snowflake"
        and normalize_snowflake_table_kind(table_kind) == "hybrid"
    )
    if not hybrid and row_proof_gap(dialect, True) == "unenforced":
        return ["unenforced"] * len(inspector_fks)
    requires_bit = hybrid or validation_catalog_dialect(dialect) is not None
    flags: list[tuple[Any, bool | None]] = []
    if requires_bit and measured is not None and measured.measured:
        for item in measured.items:
            ident = fk_identity(
                {
                    "constrained_columns": item.columns,
                    "referred_schema": item.referenced_schema,
                    "referred_table": item.referenced_table,
                    "referred_columns": item.referenced_columns,
                }
            )
            if ident is not None:
                flags.append((ident, item.validated))
    gaps: list[str] = []
    for fk in inspector_fks:
        if not requires_bit:
            gaps.append("")
            continue
        if measured is None or not measured.measured or not isinstance(fk, dict):
            gaps.append("unreported")
            continue
        ident = fk_identity(fk)
        if ident is None:
            gaps.append("unreported")
            continue
        matched = [flag for known, flag in flags if same_relationship(ident, known)]
        if any(flag is False for flag in matched):
            gaps.append(
                row_proof_gap(
                    dialect, False, table_kind=table_kind, index_status=index_status
                )
            )
        elif any(flag is True for flag in matched):
            gaps.append(
                row_proof_gap(
                    dialect, True, table_kind=table_kind, index_status=index_status
                )
            )
        else:
            gaps.append("unreported")
    return gaps


def enforced_relationship_identities(
    dialect: str,
    inspector_fks: list[Any],
    measured: ForeignKeys | None,
    *,
    table_kind: str = "",
    index_status: str = "",
) -> list[tuple[str, tuple[tuple[str, str], ...]]]:
    """Inspector foreign keys that prove the rows already stored.

    The gap comes from :func:`inspector_row_proof_gaps`. An empty gap is the
    proof. Anything else is scanned by the caller.
    """
    from services.foreign_key_identity import fk_identity

    enforced: list[tuple[str, tuple[tuple[str, str], ...]]] = []
    for fk, gap in zip(
        inspector_fks,
        inspector_row_proof_gaps(
            dialect,
            inspector_fks,
            measured,
            table_kind=table_kind,
            index_status=index_status,
        ),
    ):
        if gap or not isinstance(fk, dict):
            continue
        ident = fk_identity(fk)
        if ident is not None:
            enforced.append(ident)
    return enforced


def probe_foreign_keys(
    dialect: str, cursor_or_connection: Any, schema: str, table: str
) -> ForeignKeys:
    """Measure the foreign keys of ``schema.table``, or report why it could not."""
    key = _dialect_key(dialect)
    probe = _PROBES.get(key)
    if probe is None:
        return _unavailable(
            key,
            f"Foreign key catalog probe not implemented for '{key or 'unknown'}'.",
            schema,
            table,
        )
    if not table:
        return _unavailable(key, "No table name supplied.", schema, table)
    try:
        cursor = as_driver_cursor(cursor_or_connection)
        return probe(cursor, schema or "", table)
    except Exception as exc:  # noqa: BLE001 — an unreadable catalog is a state
        logger.debug("foreign key probe failed on %s: %s", key, exc, exc_info=exc)
        return _unavailable(key, f"{type(exc).__name__}: {exc}", schema, table)


def foreign_keys_from_payload(payload: Any) -> list[ForeignKey]:
    """Rebuild foreign keys from a serialized catalog payload.

    Accepts both this module's shape and the older introspect dicts that carry
    no referential actions, so an existing catalog keeps working rather than
    losing its keys to a schema mismatch.
    """
    items: Any = payload
    if isinstance(payload, dict):
        items = payload.get("items") or payload.get("foreign_keys") or []
    out: list[ForeignKey] = []
    for entry in items or []:
        if not isinstance(entry, dict):
            continue
        columns = [str(c) for c in (entry.get("columns") or []) if c]
        ref_columns = [str(c) for c in (entry.get("referenced_columns") or []) if c]
        if not columns:
            continue
        out.append(
            ForeignKey(
                name=str(entry.get("name") or entry.get("constraint") or ""),
                columns=columns,
                referenced_schema=str(entry.get("referenced_schema") or ""),
                referenced_table=str(entry.get("referenced_table") or ""),
                referenced_columns=ref_columns,
                on_delete=normalize_action(entry.get("on_delete")),
                on_update=normalize_action(entry.get("on_update")),
                validated=(
                    coerce_validated(entry.get("validated"))
                    if "validated" in entry
                    else None
                ),
                match=normalize_match(
                    entry.get("match")
                    if entry.get("match") not in (None, "")
                    else entry.get("confmatchtype")
                    or (
                        entry.get("options").get("match")
                        if isinstance(entry.get("options"), dict)
                        else ""
                    )
                ),
                deferral=(
                    normalize_deferral(spelling=entry.get("deferral"))
                    if "deferral" in entry
                    else ""
                ),
            )
        )
    return out
