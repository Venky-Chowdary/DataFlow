"""Recreate physical placement on create-new: partitioning, tablespace, clustering.

``services.physical_storage_metadata`` *measures* placement; this module decides
what of it can be reproduced on the destination and emits the DDL for it. Until
now every measured placement was certified ``unsupported``, so a partitioned,
non-default-tablespace source landed as a heap in the default tablespace — the
certificate said so, which is honest but is not a migration.

Three rules hold everywhere here:

* **A plan is not a carry.** Every decision starts ``planned``; only a re-read of
  the destination catalog (``verify_placement``) promotes it to ``carried``.
* **Never guess a bound.** A partition scheme without the source's per-partition
  bounds would create a parent no row can be inserted into, so it is refused
  rather than half-emitted.
* **Unreadable is not absent.** A destination tablespace catalog we cannot read
  yields ``unknown``, never "the tablespace does not exist".

Cross-dialect placement is deliberately *not* invented: RANGE bounds, hash
modulus semantics and tablespace/filegroup objects do not translate between
engines, so a PostgreSQL→MySQL move reports the placement as not carried with
the exact reason instead of emitting a scheme that means something else.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from connectors.sql_identifiers import quote_sql_identifier

logger = logging.getLogger(__name__)

PLACEMENT_ASPECTS = ("partitioning", "tablespace", "clustering")

#: Dialects whose CREATE TABLE can name a tablespace/filegroup we carry.
_TABLESPACE_CLAUSE = {
    "postgresql": 'TABLESPACE {ident}',
    "oracle": "TABLESPACE {ident}",
    "mysql": "TABLESPACE {ident}",
    "sqlserver": "ON {ident}",
}

_PG_FAMILY = frozenset({"postgresql", "postgres", "redshift"})
_MYSQL_FAMILY = frozenset({"mysql", "mariadb"})
# MySQL partition methods that declare explicit per-partition values.
_MYSQL_VALUE_METHODS = {"range", "list", "range columns", "list columns"}
_SAFE_BOUND = re.compile(r"^[A-Za-z0-9_,\s'\"().:+\-]*$")


def _norm(dialect: str) -> str:
    d = (dialect or "").strip().lower()
    if d in {"postgres", "cockroachdb", "yugabytedb"}:
        return "postgresql"
    if d in {"mariadb"}:
        return "mysql"
    if d in {"mssql", "azure_sql"}:
        return "sqlserver"
    return d


def _same_family(source: str, dest: str) -> bool:
    s, d = _norm(source), _norm(dest)
    if s in _PG_FAMILY and d in _PG_FAMILY:
        return True
    if s in _MYSQL_FAMILY and d in _MYSQL_FAMILY:
        return True
    return s == d and bool(s)


def _q(ident: str, dialect: str) -> str:
    d = _norm(dialect)
    if d == "mysql":
        return quote_sql_identifier(ident, "`")
    if d == "sqlserver":
        return f"[{str(ident).replace(']', ']]')}]"
    return quote_sql_identifier(ident, '"')


@dataclass(frozen=True)
class PlacementDecision:
    """One placement aspect and what the destination will actually get.

    ``status`` is ``planned`` | ``unsupported`` | ``skipped`` | ``unknown`` —
    the same vocabulary the fidelity certificate uses, minus ``carried``, which
    only a destination re-read may award.
    """

    aspect: str
    status: str
    reason: str
    source_detail: str = ""
    dest_ddl: str = ""


@dataclass
class PlacementPlan:
    """DDL to reproduce placement plus the decision behind each aspect."""

    create_suffix: str = ""
    post_create_sql: list[str] = field(default_factory=list)
    decisions: list[PlacementDecision] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "create_suffix": self.create_suffix,
            "post_create_sql": list(self.post_create_sql),
            "decisions": [
                {
                    "aspect": d.aspect,
                    "status": d.status,
                    "reason": d.reason,
                    "source_detail": d.source_detail,
                    "dest_ddl": d.dest_ddl,
                }
                for d in self.decisions
            ],
        }


def _unknown(aspect: str, detail: str) -> PlacementDecision:
    return PlacementDecision(
        aspect=aspect,
        status="unknown",
        reason=(
            "Physical storage catalog was not measured on the source; "
            f"{aspect} is unmeasured, not absent."
            + (f" ({detail})" if detail else "")
        ),
    )


def _dest_spelling(column: str, dest_columns: list[str]) -> str | None:
    """Destination spelling of a source column, or None when it is not mapped."""
    want = str(column).strip().lower()
    for candidate in dest_columns:
        if str(candidate).strip().lower() == want:
            return str(candidate)
    return None


def _partition_decision(
    *,
    storage: dict[str, Any],
    source_dialect: str,
    dest_dialect: str,
    dest_schema: str,
    dest_table: str,
    dest_columns: list[str],
    primary_key: list[str],
    unique_constraints: list[list[str]],
) -> tuple[PlacementDecision, str, list[str]]:
    """Decide partition carry; returns (decision, create_suffix, post_create_sql)."""
    partitioned = bool(storage.get("partitioned"))
    keys = [str(k) for k in (storage.get("partition_keys") or [])]
    strategy = str(storage.get("partition_strategy") or "").strip().lower()
    bounds = [
        {"name": str(b.get("name") or ""), "bound": str(b.get("bound") or "")}
        for b in (storage.get("partition_bounds") or [])
        if isinstance(b, dict)
    ]
    detail = (
        f"{strategy or 'partitioned'} on {', '.join(keys) or 'unreported key'} "
        f"({storage.get('partition_count')} partitions)"
    )
    if not partitioned:
        return (
            PlacementDecision(
                aspect="partitioning",
                status="skipped",
                reason="Source table is not partitioned (measured).",
            ),
            "",
            [],
        )

    def refuse(reason: str) -> tuple[PlacementDecision, str, list[str]]:
        return (
            PlacementDecision(
                aspect="partitioning",
                status="unsupported",
                reason=reason,
                source_detail=detail,
            ),
            "",
            [],
        )

    if not _same_family(source_dialect, dest_dialect):
        return refuse(
            f"Source is partitioned on {source_dialect or 'an unknown engine'} and the "
            f"destination is {_norm(dest_dialect)}; partition bounds and strategy "
            "semantics do not translate between engines, so the destination is "
            "created unpartitioned — reapply a native partition scheme before cutover."
        )
    if not keys:
        return refuse(
            "Source partition key columns were not reported by the catalog; "
            "a partition scheme cannot be reproduced from an unknown key."
        )
    dest_keys = [_dest_spelling(k, dest_columns) for k in keys]
    if any(k is None for k in dest_keys):
        missing = [k for k, d in zip(keys, dest_keys) if d is None]
        return refuse(
            f"Partition key column(s) {', '.join(missing)} are not mapped to the "
            "destination; a partition scheme over columns that do not exist would "
            "be refused by the engine."
        )
    named_keys = [str(k) for k in dest_keys if k]
    dest = _norm(dest_dialect)
    if dest == "postgresql":
        return _pg_partition_ddl(
            keys=named_keys,
            strategy=strategy,
            bounds=_rename_children(
                bounds, str(storage.get("table") or ""), dest_table
            ),
            detail=detail,
            dest_schema=dest_schema,
            dest_table=dest_table,
            primary_key=primary_key,
            unique_constraints=unique_constraints,
            refuse=refuse,
        )
    if dest == "mysql":
        return _mysql_partition_ddl(
            keys=named_keys,
            strategy=strategy,
            bounds=bounds,
            detail=detail,
            primary_key=primary_key,
            unique_constraints=unique_constraints,
            refuse=refuse,
        )
    return refuse(
        f"Partition carry is not implemented for destination '{dest}': its partition "
        "scheme needs catalog objects (partition function/scheme, per-partition "
        "tablespaces) that this transfer does not create. Apply the scheme manually."
    )


def _rename_children(
    bounds: list[dict[str, str]], source_table: str, dest_table: str
) -> list[dict[str, str]]:
    """Name child partitions after the destination table.

    Reusing the source child names silently no-ops under
    ``CREATE TABLE IF NOT EXISTS`` when source and destination share a schema:
    the parent ends up with no partitions and accepts no row, which the
    destination re-read then reports as not carried.
    """
    out: list[dict[str, str]] = []
    for index, bound in enumerate(bounds):
        name = str(bound.get("name") or "")
        suffix = ""
        if source_table and name.lower().startswith(f"{source_table.lower()}_"):
            suffix = name[len(source_table) + 1 :]
        if not suffix:
            suffix = name.rsplit("_", 1)[-1] if "_" in name else name
        if not suffix:
            suffix = f"p{index}"
        out.append({"name": f"{dest_table}_{suffix}", "bound": str(bound.get("bound") or "")})
    return out


def _keys_covered_by_unique(
    keys: list[str], primary_key: list[str], unique_constraints: list[list[str]]
) -> str | None:
    """Name of the unique constraint that does not contain every partition key."""
    wanted = {k.strip().lower() for k in keys}
    for name, cols in [("PRIMARY KEY", primary_key), *[
        (f"UNIQUE ({', '.join(u)})", u) for u in unique_constraints
    ]]:
        if not cols:
            continue
        if not wanted.issubset({str(c).strip().lower() for c in cols}):
            return name
    return None


def _pg_partition_ddl(
    *,
    keys: list[str],
    strategy: str,
    bounds: list[dict[str, str]],
    detail: str,
    dest_schema: str,
    dest_table: str,
    primary_key: list[str],
    unique_constraints: list[list[str]],
    refuse: Any,
) -> tuple[PlacementDecision, str, list[str]]:
    if strategy not in {"range", "list", "hash"}:
        return refuse(
            f"Unsupported PostgreSQL partition strategy '{strategy or 'unknown'}'."
        )
    offender = _keys_covered_by_unique(keys, primary_key, unique_constraints)
    if offender:
        return refuse(
            "PostgreSQL requires every unique constraint of a partitioned table to "
            f"contain the partition key ({', '.join(keys)}); {offender} does not, so "
            "carrying the scheme would cost the key. Widen the key on the source or "
            "partition the destination manually."
        )
    if not bounds:
        return refuse(
            "Source partition bounds were not readable; a partitioned parent with no "
            "partitions accepts no rows, so the destination is created unpartitioned."
        )
    unsafe = [b["name"] for b in bounds if not _SAFE_BOUND.match(b["bound"])]
    if unsafe:
        return refuse(
            f"Partition bound expression for {', '.join(unsafe)} contains characters "
            "outside the literal whitelist; refusing to replay catalog text as DDL."
        )
    suffix = f"PARTITION BY {strategy.upper()} ({', '.join(_q(k, 'postgresql') for k in keys)})"
    parent = (
        f"{_q(dest_schema, 'postgresql')}.{_q(dest_table, 'postgresql')}"
        if dest_schema
        else _q(dest_table, "postgresql")
    )
    post: list[str] = []
    for bound in bounds:
        child = bound["name"]
        if not child or not bound["bound"]:
            continue
        child_ref = (
            f"{_q(dest_schema, 'postgresql')}.{_q(child, 'postgresql')}"
            if dest_schema
            else _q(child, "postgresql")
        )
        post.append(
            f"CREATE TABLE IF NOT EXISTS {child_ref} PARTITION OF {parent} "
            f"{bound['bound']}"
        )
    if not post:
        return refuse(
            "No partition bound could be rendered; refusing a parent that accepts "
            "no rows."
        )
    return (
        PlacementDecision(
            aspect="partitioning",
            status="planned",
            reason=(
                f"{strategy.upper()} partitioning on {', '.join(keys)} with "
                f"{len(post)} partition(s) reproduced from the source catalog."
            ),
            source_detail=detail,
            dest_ddl=suffix,
        ),
        suffix,
        post,
    )


def _mysql_partition_ddl(
    *,
    keys: list[str],
    strategy: str,
    bounds: list[dict[str, str]],
    detail: str,
    primary_key: list[str],
    unique_constraints: list[list[str]],
    refuse: Any,
) -> tuple[PlacementDecision, str, list[str]]:
    method = strategy.replace("_", " ")
    if method not in _MYSQL_VALUE_METHODS | {"hash", "key", "linear hash", "linear key"}:
        return refuse(f"Unsupported MySQL partition method '{strategy or 'unknown'}'.")
    offender = _keys_covered_by_unique(keys, primary_key, unique_constraints)
    if offender:
        return refuse(
            "MySQL requires every unique key of a partitioned table to contain the "
            f"partition columns ({', '.join(keys)}); {offender} does not, so the "
            "CREATE would be refused (errno 1503). Partition the destination manually."
        )
    key_sql = ", ".join(_q(k, "mysql") for k in keys)
    if method in _MYSQL_VALUE_METHODS:
        if not bounds:
            return refuse(
                "Source partition descriptions were not readable; a RANGE/LIST "
                "partitioned table cannot be created without them."
            )
        unsafe = [b["name"] for b in bounds if not _SAFE_BOUND.match(b["bound"])]
        if unsafe:
            return refuse(
                f"Partition description for {', '.join(unsafe)} contains characters "
                "outside the literal whitelist; refusing to replay catalog text as DDL."
            )
        clause = "VALUES LESS THAN" if method.startswith("range") else "VALUES IN"
        parts = ", ".join(
            f"PARTITION {_q(b['name'], 'mysql')} {clause} ({b['bound']})"
            for b in bounds
            if b["name"] and b["bound"]
        )
        if not parts:
            return refuse("No MySQL partition definition could be rendered.")
        suffix = f"PARTITION BY {method.upper()} ({key_sql}) ({parts})"
    else:
        count = len([b for b in bounds if b.get("name")]) or 1
        suffix = f"PARTITION BY {method.upper()} ({key_sql}) PARTITIONS {count}"
    return (
        PlacementDecision(
            aspect="partitioning",
            status="planned",
            reason=(
                f"{method.upper()} partitioning on {', '.join(keys)} reproduced from "
                "the source catalog."
            ),
            source_detail=detail,
            dest_ddl=suffix,
        ),
        suffix,
        [],
    )


def _tablespace_decision(
    *,
    storage: dict[str, Any],
    dest_dialect: str,
    dest_tablespaces: set[str] | None,
) -> tuple[PlacementDecision, str]:
    name = str(storage.get("tablespace") or "").strip()
    is_default = storage.get("is_default_tablespace")
    if not name or is_default is True:
        return (
            PlacementDecision(
                aspect="tablespace",
                status="skipped",
                reason="Source table uses the default tablespace/filegroup (measured).",
            ),
            "",
        )
    dest = _norm(dest_dialect)
    clause = _TABLESPACE_CLAUSE.get(dest)
    if clause is None:
        return (
            PlacementDecision(
                aspect="tablespace",
                status="unsupported",
                reason=(
                    f"Destination '{dest}' has no tablespace/filegroup clause on "
                    "CREATE TABLE; the table is created in the engine default."
                ),
                source_detail=name,
            ),
            "",
        )
    if dest_tablespaces is None:
        return (
            PlacementDecision(
                aspect="tablespace",
                status="unknown",
                reason=(
                    "Destination tablespace/filegroup catalog was not readable, so "
                    f"'{name}' could not be verified to exist; the table is created "
                    "in the default rather than risking a refused CREATE."
                ),
                source_detail=name,
            ),
            "",
        )
    match = next(
        (t for t in dest_tablespaces if t.strip().lower() == name.strip().lower()),
        None,
    )
    if match is None:
        return (
            PlacementDecision(
                aspect="tablespace",
                status="unsupported",
                reason=(
                    f"Tablespace/filegroup '{name}' does not exist on the destination; "
                    "the table is created in the default. Create it on the destination "
                    "first if placement matters."
                ),
                source_detail=name,
            ),
            "",
        )
    suffix = clause.format(ident=_q(match, dest))
    return (
        PlacementDecision(
            aspect="tablespace",
            status="planned",
            reason=f"Destination tablespace/filegroup '{match}' exists and is named on CREATE.",
            source_detail=name,
            dest_ddl=suffix,
        ),
        suffix,
    )


def _clustering_decision(
    *, storage: dict[str, Any], dest_dialect: str, dest_columns: list[str]
) -> PlacementDecision:
    clustering = [str(c) for c in (storage.get("clustering") or [])]
    if not clustering:
        return PlacementDecision(
            aspect="clustering",
            status="skipped",
            reason="No clustering index on source (measured).",
        )
    dest = _norm(dest_dialect)
    if dest == "sqlserver":
        return PlacementDecision(
            aspect="clustering",
            status="planned",
            reason=(
                "SQL Server clusters on the primary key by default; the carried PK "
                "reproduces the clustered index."
            ),
            source_detail=", ".join(clustering),
        )
    mapped = [c for c in clustering if _dest_spelling(c, dest_columns)]
    return PlacementDecision(
        aspect="clustering",
        status="unsupported",
        reason=(
            "Source table has a clustering index; physical row order is not "
            "reproduced by the load. Run "
            f"CLUSTER on ({', '.join(mapped) or ', '.join(clustering)}) after cutover "
            "if scan locality matters."
            if dest == "postgresql"
            else "Source table has a clustering/IOT organisation; the destination is "
            "created heap-organised and physical row order is not reproduced."
        ),
        source_detail=", ".join(clustering),
    )


def plan_physical_placement(
    *,
    source_storage: dict[str, Any] | None,
    source_dialect: str,
    dest_dialect: str,
    dest_schema: str,
    dest_table: str,
    dest_columns: list[str],
    primary_key: list[str] | None = None,
    unique_constraints: list[list[str]] | None = None,
    dest_tablespaces: set[str] | None = None,
) -> PlacementPlan:
    """Plan the placement DDL for one create-new table.

    Never raises and never emits a partial scheme: any aspect it cannot
    reproduce is returned as a decision explaining why and what the operator
    must do instead.
    """
    storage = dict(source_storage or {})
    if storage.get("status") != "measured":
        detail = str(storage.get("detail") or "").strip()
        return PlacementPlan(
            decisions=[_unknown(aspect, detail) for aspect in PLACEMENT_ASPECTS]
        )

    part_decision, part_suffix, post_sql = _partition_decision(
        storage=storage,
        source_dialect=source_dialect,
        dest_dialect=dest_dialect,
        dest_schema=dest_schema,
        dest_table=dest_table,
        dest_columns=list(dest_columns),
        primary_key=list(primary_key or []),
        unique_constraints=[list(u) for u in (unique_constraints or [])],
    )
    ts_decision, ts_suffix = _tablespace_decision(
        storage=storage,
        dest_dialect=dest_dialect,
        dest_tablespaces=dest_tablespaces,
    )
    cluster_decision = _clustering_decision(
        storage=storage, dest_dialect=dest_dialect, dest_columns=list(dest_columns)
    )
    suffix = " ".join(part for part in (part_suffix, ts_suffix) if part)
    return PlacementPlan(
        create_suffix=suffix,
        post_create_sql=post_sql,
        decisions=[part_decision, ts_decision, cluster_decision],
    )


def list_destination_tablespaces(dialect: str, cursor: Any) -> set[str] | None:
    """Tablespaces/filegroups the destination already has, or None if unreadable.

    ``None`` is load-bearing: it means *we could not tell*, which
    ``plan_physical_placement`` reports as unknown instead of claiming the
    source's tablespace is missing.
    """
    queries = {
        "postgresql": "SELECT spcname FROM pg_tablespace",
        "mysql": "SELECT name FROM information_schema.innodb_tablespaces",
        "oracle": "SELECT tablespace_name FROM user_tablespaces",
        "sqlserver": "SELECT name FROM sys.filegroups",
    }
    sql = queries.get(_norm(dialect))
    if not sql:
        return None
    try:
        from services.physical_storage_metadata import as_driver_cursor

        cur = as_driver_cursor(cursor)
        cur.execute(sql, ())
        return {str(r[0]) for r in (cur.fetchall() or []) if r and r[0]}
    except Exception as exc:  # noqa: BLE001 — an unreadable catalog is evidence
        logger.debug("destination tablespace catalog unreadable: %s", exc)
        return None


def verify_placement(
    *,
    decisions: list[PlacementDecision],
    source_storage: dict[str, Any] | None,
    dest_storage: dict[str, Any] | None,
) -> list[PlacementDecision]:
    """Promote planned aspects to ``carried`` only when the destination proves it.

    A plan that was executed is still just a claim until the destination catalog
    is read back: a partition clause the engine silently ignored, or a filegroup
    the storage engine redirected, must not certify as carried.
    """
    dest = dict(dest_storage or {})
    src = dict(source_storage or {})
    if dest.get("status") != "measured":
        detail = str(dest.get("detail") or "").strip()
        return [
            PlacementDecision(
                aspect=d.aspect,
                status="unknown" if d.status == "planned" else d.status,
                reason=(
                    "Destination placement catalog was not readable after CREATE, so "
                    f"the emitted {d.aspect} DDL is unverified — planned, not proven."
                    + (f" ({detail})" if detail else "")
                    if d.status == "planned"
                    else d.reason
                ),
                source_detail=d.source_detail,
                dest_ddl=d.dest_ddl,
            )
            for d in decisions
        ]

    out: list[PlacementDecision] = []
    for decision in decisions:
        if decision.status != "planned":
            out.append(decision)
            continue
        proven, why = _aspect_proven(decision.aspect, src, dest)
        out.append(
            PlacementDecision(
                aspect=decision.aspect,
                status="carried" if proven else "unsupported",
                reason=(
                    f"{decision.reason} Verified by re-reading the destination catalog."
                    if proven
                    else f"Destination re-read does not show it: {why}"
                ),
                source_detail=decision.source_detail,
                dest_ddl=decision.dest_ddl,
            )
        )
    return out


def _aspect_proven(
    aspect: str, src: dict[str, Any], dest: dict[str, Any]
) -> tuple[bool, str]:
    if aspect == "partitioning":
        if not dest.get("partitioned"):
            return False, "destination table is not partitioned after CREATE."
        src_keys = [str(k).lower() for k in (src.get("partition_keys") or [])]
        dst_keys = [str(k).lower() for k in (dest.get("partition_keys") or [])]
        if src_keys and dst_keys and src_keys != dst_keys:
            return False, f"partition keys differ (source {src_keys}, dest {dst_keys})."
        src_count = int(src.get("partition_count") or 0)
        dst_count = int(dest.get("partition_count") or 0)
        if src_count and dst_count < src_count:
            return (
                False,
                f"destination has {dst_count} partition(s), source has {src_count}.",
            )
        return True, ""
    if aspect == "tablespace":
        want = str(src.get("tablespace") or "").strip().lower()
        got = str(dest.get("tablespace") or "").strip().lower()
        if want and got == want:
            return True, ""
        return False, f"destination tablespace/filegroup is '{got or 'default'}'."
    if aspect == "clustering":
        if dest.get("clustering"):
            return True, ""
        return False, "destination reports no clustering index."
    return False, f"no verification rule for aspect '{aspect}'."
