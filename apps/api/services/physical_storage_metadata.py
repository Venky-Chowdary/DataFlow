"""Physical storage metadata — tablespace / filegroup / partitioning / clustering.

A migration certificate that silently omits physical placement lets an operator
believe a partitioned, non-default-tablespace table was reproduced faithfully
when it was not. This module *measures* the placement of one table and says so;
when the catalog cannot be read it returns ``status="unavailable"`` and leaves
every field ``None`` — never ``False``/``""``, which downstream would read as
"proven absent".

One connector-agnostic entry point (``probe_physical_storage``) with a
per-dialect catalog query, so every writer/introspector shares the same
evidence shape.

Companion to ``services.physical_state_diff``, which compares the *logical*
catalog (keys, indexes, NOT NULL, defaults); this module owns placement only.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

PhysicalStorageStatus = Literal["measured", "unavailable"]

SUPPORTED_DIALECTS = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "oracle",
})


@dataclass(frozen=True)
class PhysicalStorage:
    """Measured physical placement of one table.

    ``None`` on any field means *not measured*. Only ``status="measured"``
    entitles a caller to treat ``partitioned=False`` as proof of absence.
    """

    dialect: str
    status: PhysicalStorageStatus
    detail: str = ""
    # Postgres/Oracle tablespace, SQL Server filegroup, MySQL tablespace.
    tablespace: str | None = None
    is_default_tablespace: bool | None = None
    partitioned: bool | None = None
    partition_strategy: str | None = None
    partition_keys: list[str] | None = None
    partition_count: int | None = None
    # Postgres CLUSTER index, SQL Server clustered index, Oracle IOT.
    clustering: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def measured(self) -> bool:
        return self.status == "measured"


def _unavailable(dialect: str, detail: str) -> PhysicalStorage:
    return PhysicalStorage(dialect=dialect, status="unavailable", detail=detail)


class _DriverCursorShim:
    """Run driver-level SQL on a SQLAlchemy connection.

    Oracle/SQL Server introspection holds a SQLAlchemy ``Connection``; the
    catalog queries here are driver SQL, so ``exec_driver_sql`` (not ``text()``)
    is the correct bridge and keeps one query per dialect.
    """

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._result: Any = None

    def execute(self, sql: str, params: Any) -> None:
        # A dict must stay a dict: Oracle's driver binds `:name` by key.
        bound = params if isinstance(params, dict) else tuple(params)
        self._result = self._connection.exec_driver_sql(sql, bound)

    def fetchall(self) -> list[tuple]:
        return list(self._result.fetchall() or []) if self._result is not None else []


def as_driver_cursor(cursor_or_connection: Any) -> Any:
    """Accept a DB-API cursor or a SQLAlchemy connection; return a cursor."""
    if hasattr(cursor_or_connection, "exec_driver_sql"):
        return _DriverCursorShim(cursor_or_connection)
    return cursor_or_connection


_as_cursor = as_driver_cursor


def _rows(cursor: Any, sql: str, params: tuple) -> list[tuple]:
    cursor.execute(sql, params)
    return list(cursor.fetchall() or [])


def _rows_any_paramstyle(cursor: Any, sql: str, params: tuple) -> list[tuple]:
    """SQL Server reaches us through both pymssql (``%s``) and pyodbc (``?``).

    The catalog query is read-only, so trying the second placeholder style
    after a driver rejects the first is safe and avoids duplicating the query.
    """
    last: Exception | None = None
    for style in ("%s", "?"):
        try:
            return _rows(cursor, sql.replace("{p}", style), params)
        except Exception as exc:  # noqa: BLE001 — driver paramstyle fallback
            last = exc
    raise last if last else RuntimeError("no paramstyle attempted")


def _probe_postgres(cursor: Any, schema: str, table: str) -> PhysicalStorage:
    rows = _rows(
        cursor,
        """
        SELECT COALESCE(ts.spcname, 'pg_default') AS tablespace,
               c.reltablespace = 0                AS default_tablespace,
               c.relkind = 'p'                    AS partitioned,
               pt.partstrat,
               (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid),
               (SELECT array_agg(a2.attname ORDER BY k.ord)
                  FROM unnest(pt.partattrs) WITH ORDINALITY AS k(attnum, ord)
                  JOIN pg_attribute a2
                    ON a2.attrelid = c.oid AND a2.attnum = k.attnum),
               (SELECT array_agg(a3.attname ORDER BY a3.attnum)
                  FROM pg_index x
                  JOIN pg_attribute a3
                    ON a3.attrelid = c.oid AND a3.attnum = ANY (x.indkey)
                 WHERE x.indrelid = c.oid AND x.indisclustered)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
     LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
         WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    if not rows:
        return _unavailable(
            "postgresql",
            f"{schema}.{table} not visible in pg_class for this role; "
            "physical placement unmeasured (not proof of absence)",
        )
    tspace, default_ts, partitioned, strat, child_count, part_keys, cluster_cols = rows[0]
    strategies = {"r": "range", "l": "list", "h": "hash"}
    return PhysicalStorage(
        dialect="postgresql",
        status="measured",
        detail="pg_class/pg_partitioned_table/pg_tablespace",
        tablespace=str(tspace),
        is_default_tablespace=bool(default_ts),
        partitioned=bool(partitioned),
        partition_strategy=strategies.get(str(strat or ""), None) if partitioned else None,
        partition_keys=[str(c) for c in (part_keys or [])],
        partition_count=int(child_count or 0) if partitioned else 0,
        clustering=[str(c) for c in (cluster_cols or [])],
    )


def _mysql_tablespace(cursor: Any, schema: str, table: str) -> str | None:
    """InnoDB tablespace name, or None when the catalog is not readable.

    ``information_schema.TABLES`` dropped ``TABLESPACE_NAME`` in MySQL 8 and
    MariaDB never had it; ``INNODB_TABLES`` needs the PROCESS privilege, so a
    refusal here must not fail the whole probe.
    """
    try:
        rows = _rows(
            cursor,
            """
            SELECT s.name FROM information_schema.innodb_tablespaces s
              JOIN information_schema.innodb_tables t ON t.space = s.space
             WHERE t.name = CONCAT(%s, '/', %s)
            """,
            (schema, table),
        )
    except Exception as exc:  # noqa: BLE001 — a refused catalog is evidence
        logger.debug("innodb tablespace lookup unavailable: %s", exc)
        return None
    return str(rows[0][0]) if rows and rows[0][0] else None


def _probe_mysql(cursor: Any, schema: str, table: str) -> PhysicalStorage:
    rows = _rows(
        cursor,
        """
        SELECT t.table_name,
               (SELECT count(*) FROM information_schema.partitions p
                 WHERE p.table_schema = t.table_schema
                   AND p.table_name = t.table_name
                   AND p.partition_name IS NOT NULL),
               (SELECT MIN(p.partition_method) FROM information_schema.partitions p
                 WHERE p.table_schema = t.table_schema
                   AND p.table_name = t.table_name
                   AND p.partition_name IS NOT NULL),
               (SELECT MIN(p.partition_expression) FROM information_schema.partitions p
                 WHERE p.table_schema = t.table_schema
                   AND p.table_name = t.table_name
                   AND p.partition_name IS NOT NULL)
          FROM information_schema.tables t
         WHERE t.table_schema = %s AND t.table_name = %s
        """,
        (schema, table),
    )
    if not rows:
        return _unavailable(
            "mysql",
            f"{schema}.{table} not visible in information_schema for this user; "
            "physical placement unmeasured (not proof of absence)",
        )
    _name, part_count, method, expression = rows[0]
    tspace = _mysql_tablespace(cursor, schema, table)
    keys = [
        part.strip().strip("`")
        for part in str(expression or "").split(",")
        if part.strip()
    ]
    return PhysicalStorage(
        dialect="mysql",
        status="measured",
        detail="information_schema.tables/partitions",
        tablespace=tspace,
        # A per-table tablespace name is only meaningful for InnoDB general
        # tablespaces; file-per-table is reported by its own implicit name.
        is_default_tablespace=None,
        partitioned=bool(part_count),
        partition_strategy=str(method).lower() if method else None,
        partition_keys=keys,
        partition_count=int(part_count or 0),
        # InnoDB always clusters on the PK; that is engine behaviour, not a
        # per-table choice, so it is reported as an empty explicit measurement.
        clustering=[],
    )


def _probe_sqlserver(cursor: Any, schema: str, table: str) -> PhysicalStorage:
    rows = _rows_any_paramstyle(
        cursor,
        """
        SELECT COALESCE(fg.name, ps.name),
               CASE WHEN ps.data_space_id IS NULL THEN 1 ELSE 0 END,
               (SELECT COUNT(*) FROM sys.partitions p
                 WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)),
               (SELECT STRING_AGG(c.name, ',') FROM sys.index_columns ic
                  JOIN sys.columns c
                    ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                 WHERE ic.object_id = t.object_id AND ic.index_id = 1
                   AND ic.is_included_column = 0)
          FROM sys.tables t
          JOIN sys.schemas s ON s.schema_id = t.schema_id
     LEFT JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
     LEFT JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
     LEFT JOIN sys.filegroups fg ON fg.data_space_id = ds.data_space_id
     LEFT JOIN sys.partition_schemes ps ON ps.data_space_id = ds.data_space_id
         WHERE s.name = {p} AND t.name = {p}
        """,
        (schema or "dbo", table),
    )
    if not rows:
        return _unavailable(
            "sqlserver",
            f"{schema or 'dbo'}.{table} not visible in sys.tables for this login; "
            "physical placement unmeasured (not proof of absence)",
        )
    space, is_default, part_count, cluster_cols = rows[0]
    partitioned = int(part_count or 0) > 1
    return PhysicalStorage(
        dialect="sqlserver",
        status="measured",
        detail="sys.tables/sys.partitions/sys.filegroups",
        tablespace=str(space) if space else None,
        is_default_tablespace=bool(is_default),
        partitioned=partitioned,
        partition_strategy="partition_scheme" if partitioned else None,
        partition_keys=[],
        partition_count=int(part_count or 0),
        clustering=[c.strip() for c in str(cluster_cols or "").split(",") if c.strip()],
    )


def _probe_oracle(cursor: Any, schema: str, table: str) -> PhysicalStorage:
    rows = _rows(
        cursor,
        """
        SELECT t.tablespace_name, t.partitioned, t.iot_type,
               (SELECT COUNT(*) FROM all_tab_partitions p
                 WHERE p.table_owner = t.owner AND p.table_name = t.table_name),
               (SELECT MIN(pt.partitioning_type) FROM all_part_tables pt
                 WHERE pt.owner = t.owner AND pt.table_name = t.table_name)
          FROM all_tables t
         WHERE t.owner = UPPER(:1) AND t.table_name = UPPER(:2)
        """,
        (schema or "", table),
    )
    if not rows:
        return _unavailable(
            "oracle",
            f"{schema}.{table} not visible in all_tables for this user; "
            "physical placement unmeasured (not proof of absence)",
        )
    tspace, partitioned_flag, iot_type, part_count, strategy = rows[0]
    partitioned = str(partitioned_flag or "").strip().upper() == "YES"
    keys: list[str] = []
    if partitioned:
        keys = [
            str(r[0])
            for r in _rows(
                cursor,
                """
                SELECT column_name FROM all_part_key_columns
                 WHERE owner = UPPER(:1) AND name = UPPER(:2)
                 ORDER BY column_position
                """,
                (schema or "", table),
            )
        ]
    return PhysicalStorage(
        dialect="oracle",
        status="measured",
        detail="all_tables/all_part_tables/all_part_key_columns",
        # An Oracle table always names a tablespace; partitioned tables may
        # place it per partition, in which case the table-level value is NULL.
        tablespace=str(tspace) if tspace else None,
        is_default_tablespace=None,
        partitioned=partitioned,
        partition_strategy=str(strategy).lower() if strategy else None,
        partition_keys=keys,
        partition_count=int(part_count or 0),
        clustering=["<IOT primary key>"] if str(iot_type or "").strip() else [],
    )


_PROBES = {
    "postgresql": _probe_postgres,
    "redshift": _probe_postgres,
    "mysql": _probe_mysql,
    "mariadb": _probe_mysql,
    "sqlserver": _probe_sqlserver,
    "mssql": _probe_sqlserver,
    "oracle": _probe_oracle,
}


def probe_physical_storage(
    dialect: str,
    cursor: Any,
    schema: str,
    table: str,
) -> PhysicalStorage:
    """Measure tablespace / partitioning / clustering for one table.

    Never raises: a catalog the caller may not read yields ``unavailable`` so
    an operator sees "unmeasured" instead of a fabricated "no partitioning".
    """
    key = (dialect or "").strip().lower()
    probe = _PROBES.get(key)
    if probe is None:
        return _unavailable(key, f"No physical storage catalog query for '{key}'")
    if not table:
        return _unavailable(key, "No table named; physical placement unmeasured")
    try:
        return probe(_as_cursor(cursor), schema, table)
    except Exception as exc:  # noqa: BLE001 — probe must never fail the run
        logger.debug("physical storage probe failed for %s.%s: %s", schema, table, exc)
        return _unavailable(key, f"Physical storage catalog unreadable: {exc}")


@dataclass(frozen=True)
class PhysicalStorageComparison:
    """Source vs destination placement, with unmeasured sides called out."""

    status: Literal["measured", "unavailable"]
    carried: bool | None
    differences: list[str] = field(default_factory=list)
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def compare_physical_storage(
    source: PhysicalStorage | None,
    destination: PhysicalStorage | None,
) -> PhysicalStorageComparison:
    """Report which placement aspects the destination failed to reproduce."""
    if source is None or destination is None or not (source.measured and destination.measured):
        unmeasured = [
            name
            for name, side in (("source", source), ("destination", destination))
            if side is None or not side.measured
        ]
        return PhysicalStorageComparison(
            status="unavailable",
            carried=None,
            detail=(
                f"Physical placement unmeasured on {', '.join(unmeasured)}; "
                "no carry claim can be made."
            ),
        )

    differences: list[str] = []
    if bool(source.partitioned) != bool(destination.partitioned):
        differences.append(
            f"partitioning: source={'yes' if source.partitioned else 'no'}, "
            f"destination={'yes' if destination.partitioned else 'no'}"
        )
    elif source.partitioned and source.partition_keys != destination.partition_keys:
        differences.append(
            f"partition keys: source={source.partition_keys}, "
            f"destination={destination.partition_keys}"
        )
    if (source.tablespace or "") != (destination.tablespace or ""):
        differences.append(
            f"tablespace/filegroup: source={source.tablespace or 'default'}, "
            f"destination={destination.tablespace or 'default'}"
        )
    if (source.clustering or []) != (destination.clustering or []):
        differences.append(
            f"clustering: source={source.clustering or []}, "
            f"destination={destination.clustering or []}"
        )
    return PhysicalStorageComparison(
        status="measured",
        carried=not differences,
        differences=differences,
        detail=(
            "Physical placement reproduced"
            if not differences
            else "Physical placement differs; transfer carries data, not placement"
        ),
    )
