"""Destination identity / sequence counter state, read after the write.

A migration that copies explicit key values into a table whose key is
``GENERATED``/``AUTO_INCREMENT``/``IDENTITY`` leaves the *generator* untouched:
the rows are correct, every checksum matches, and the application's next
``INSERT`` fails with a duplicate key because the counter still points at 1.
Row-level proof cannot see this — the defect lives in the destination's
physical state, not in its data — so it is verified separately here, by reading
the engine's own catalog on an independent connection.

Honesty:
- ``next_value``/``max_value`` are ``None`` when the engine cannot answer;
  ``collides`` then stays ``False`` and ``available`` is ``False`` with a
  reason. An unread counter is never reported as a healthy one.
- Repair only ever moves a counter *forward*. Rewinding a generator would hand
  out keys that already exist.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from typing import Any

import sqlalchemy as sa

from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref
from services.dialect_profiles import fold_identifier, normalize_driver, quote_char_for

logger = logging.getLogger(__name__)

__all__ = [
    "IdentityWatermark",
    "identity_watermark_supported",
    "read_identity_watermark",
    "repair_identity_watermark",
    "verify_identity_watermark",
]

# Engines whose generator state this module can read. Everything else answers
# "unavailable" rather than "healthy".
_SUPPORTED: frozenset[str] = frozenset(
    {
        "postgresql",
        "postgres",
        "redshift",
        "mysql",
        "mariadb",
        "sqlserver",
        "mssql",
        "azure_sql",
        "azure_sql_database",
        "oracle",
        "sqlite",
    }
)

_PG_LIKE = {"postgresql", "postgres", "redshift"}
_MYSQL_LIKE = {"mysql", "mariadb"}
_MSSQL_LIKE = {"sqlserver", "mssql", "azure_sql", "azure_sql_database"}


@dataclass(frozen=True)
class IdentityWatermark:
    """Generator state for one destination key column."""

    column: str
    mechanism: str = ""
    next_value: int | None = None
    max_value: int | None = None
    available: bool = False
    reason: str = ""
    repaired_to: int | None = None
    generator: str = ""

    @property
    def collides(self) -> bool:
        """True when the generator's next key already exists at rest."""
        if self.next_value is None or self.max_value is None:
            return False
        return self.next_value <= self.max_value

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "mechanism": self.mechanism,
            "generator": self.generator,
            "next_value": self.next_value,
            "max_value": self.max_value,
            "available": self.available,
            "collides": self.collides,
            "reason": self.reason,
            "repaired_to": self.repaired_to,
        }


@dataclass
class _Probe:
    """Dialect answers, before they are turned into a watermark."""

    mechanism: str = ""
    generator: str = ""
    next_value: int | None = None
    reason: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def identity_watermark_supported(db_type: str) -> bool:
    """True when this module can read the engine's generator state."""
    return (db_type or "").strip().lower() in _SUPPORTED


def _norm(db_type: str) -> str:
    return normalize_driver(db_type)


def _quote_column(column: str, dialect: str) -> str:
    """Column reference in the destination's own quoting and folding rules."""
    quote = quote_char_for(dialect) or '"'
    return quote_sql_identifier(fold_identifier(dialect, column), quote)


def _scalar(conn: Any, sql: str, params: dict[str, Any] | None = None) -> Any:
    row = conn.execute(sa.text(sql), params or {}).fetchone()
    return row[0] if row else None


def _max_value(conn: Any, table_ref: str, column: str, dialect: str) -> int | None:
    col = _quote_column(column, dialect)
    value = _scalar(conn, f"SELECT MAX({col}) FROM {table_ref}")  # nosec B608
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        # A non-integral key has no counter to outrun.
        return None


def _pg_probe(conn: Any, schema: str, table: str, column: str) -> _Probe:
    seq = _scalar(
        conn,
        "SELECT pg_get_serial_sequence(:qualified, :col)",
        {"qualified": f'"{schema}"."{table}"', "col": column},
    )
    if not seq:
        return _Probe(reason="column has no owned sequence (not serial/identity)")
    # The sequence relation itself is the only place carrying ``is_called``
    # (``pg_sequences`` exposes ``last_value`` alone). It matters: on a fresh
    # sequence ``last_value`` is 1 and ``is_called`` is false, so the next key
    # is 1, not 2.
    row = conn.execute(
        sa.text(f"SELECT last_value, is_called FROM {seq}")  # nosec B608
    ).fetchone()
    if row is None or row[0] is None:
        return _Probe(
            mechanism="sequence",
            generator=str(seq),
            reason=f"sequence {seq} returned no state",
        )
    last_value, is_called = row[0], bool(row[1])
    return _Probe(
        mechanism="sequence",
        generator=str(seq),
        next_value=int(last_value) + (1 if is_called else 0),
    )


def _mysql_probe(conn: Any, schema: str, table: str, column: str) -> _Probe:
    extra = _scalar(
        conn,
        "SELECT extra FROM information_schema.columns "
        "WHERE table_schema = COALESCE(NULLIF(:sch, ''), DATABASE()) "
        "AND table_name = :tbl AND column_name = :col",
        {"sch": schema or "", "tbl": table, "col": column},
    )
    if "auto_increment" not in str(extra or "").lower():
        return _Probe(reason="column is not AUTO_INCREMENT")
    nxt = _scalar(
        conn,
        "SELECT auto_increment FROM information_schema.tables "
        "WHERE table_schema = COALESCE(NULLIF(:sch, ''), DATABASE()) "
        "AND table_name = :tbl",
        {"sch": schema or "", "tbl": table},
    )
    if nxt is None:
        return _Probe(
            mechanism="auto_increment",
            reason="information_schema reported no AUTO_INCREMENT value",
        )
    return _Probe(mechanism="auto_increment", generator=f"{table}.{column}", next_value=int(nxt))


def _mssql_probe(conn: Any, schema: str, table: str, column: str) -> _Probe:
    qualified = f"{schema or 'dbo'}.{table}"
    is_identity = _scalar(
        conn,
        "SELECT COUNT(*) FROM sys.identity_columns ic "
        "JOIN sys.tables t ON t.object_id = ic.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE s.name = :sch AND t.name = :tbl AND ic.name = :col",
        {"sch": schema or "dbo", "tbl": table, "col": column},
    )
    if not int(is_identity or 0):
        return _Probe(reason="column is not an IDENTITY column")
    # IDENT_CURRENT is the last value handed out; the next one is +IDENT_INCR.
    current = _scalar(conn, "SELECT IDENT_CURRENT(:t)", {"t": qualified})
    incr = _scalar(conn, "SELECT IDENT_INCR(:t)", {"t": qualified})
    if current is None:
        return _Probe(mechanism="identity", reason="IDENT_CURRENT returned NULL")
    step = int(incr) if incr is not None else 1
    return _Probe(
        mechanism="identity",
        generator=f"{qualified}.{column}",
        next_value=int(current) + step,
    )


def _oracle_probe(conn: Any, schema: str, table: str, column: str) -> _Probe:
    owner = (schema or "").upper()
    row = conn.execute(
        sa.text(
            "SELECT sequence_name FROM all_tab_identity_cols "
            "WHERE owner = COALESCE(NULLIF(:own, ''), USER) "
            "AND table_name = :tbl AND column_name = :col"
        ),
        {"own": owner, "tbl": table.upper(), "col": column.upper()},
    ).fetchone()
    if row is None or not row[0]:
        return _Probe(reason="column is not a GENERATED AS IDENTITY column")
    seq_name = str(row[0])
    # LAST_NUMBER is the next value the instance will serve (it already accounts
    # for the cache), so it is the next key, not the last one issued.
    last_number = _scalar(
        conn,
        "SELECT last_number FROM all_sequences "
        "WHERE sequence_owner = COALESCE(NULLIF(:own, ''), USER) "
        "AND sequence_name = :seq",
        {"own": owner, "seq": seq_name},
    )
    if last_number is None:
        return _Probe(
            mechanism="identity",
            generator=seq_name,
            reason=f"sequence {seq_name} not visible in ALL_SEQUENCES",
        )
    return _Probe(mechanism="identity", generator=seq_name, next_value=int(last_number))


def _sqlite_probe(conn: Any, table: str, column: str) -> _Probe:
    has_table = _scalar(
        conn,
        "SELECT COUNT(*) FROM sqlite_master "
        "WHERE type='table' AND name='sqlite_sequence'",
    )
    if not int(has_table or 0):
        # No AUTOINCREMENT column anywhere: SQLite derives the next rowid from
        # MAX(rowid) at insert time, so it can never collide.
        return _Probe(
            mechanism="rowid",
            reason="table uses implicit rowid — next key is derived from MAX(rowid)",
        )
    seq = _scalar(
        conn, "SELECT seq FROM sqlite_sequence WHERE name = :t", {"t": table}
    )
    if seq is None:
        return _Probe(
            mechanism="rowid",
            reason="table uses implicit rowid — next key is derived from MAX(rowid)",
        )
    return _Probe(
        mechanism="autoincrement",
        generator=f"sqlite_sequence.{table}",
        next_value=int(seq) + 1,
    )


def _probe(conn: Any, db_type: str, schema: str, table: str, column: str) -> _Probe:
    norm = _norm(db_type)
    if norm in _PG_LIKE:
        return _pg_probe(conn, schema or "public", table, column)
    if norm in _MYSQL_LIKE:
        return _mysql_probe(conn, schema, table, column)
    if norm in _MSSQL_LIKE:
        return _mssql_probe(conn, schema, table, column)
    if norm == "oracle":
        return _oracle_probe(conn, schema, table, column)
    if norm == "sqlite":
        return _sqlite_probe(conn, table, column)
    return _Probe(reason=f"identity state is not readable for '{db_type}'")


def read_identity_watermark(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str = "",
    table: str,
    column: str,
) -> IdentityWatermark:
    """Generator state and current key ceiling for one destination column."""
    if not table or not column:
        return IdentityWatermark(
            column=column or "", reason="destination table or key column unknown"
        )
    if not identity_watermark_supported(db_type):
        return IdentityWatermark(
            column=column, reason=f"identity state is not readable for '{db_type}'"
        )

    from connectors.generic_sql import get_sqlalchemy_engine

    dialect = _norm(db_type)
    engine = get_sqlalchemy_engine({**cfg, "type": db_type})
    with engine.connect() as conn:
        probe = _probe(conn, db_type, schema, table, column)
        table_ref = quote_table_ref(table, schema or None, dialect=dialect)
        ceiling = _max_value(conn, table_ref, column, dialect)

    return IdentityWatermark(
        column=column,
        mechanism=probe.mechanism,
        generator=probe.generator,
        next_value=probe.next_value,
        max_value=ceiling,
        available=probe.next_value is not None and ceiling is not None,
        reason=probe.reason
        or ("" if probe.next_value is not None else "generator state unreadable"),
    )


def _repair_sql(
    db_type: str, wm: IdentityWatermark, schema: str, table: str, target: int
) -> tuple[str, dict[str, Any]] | None:
    """Statement that moves the generator to ``target`` as its next key."""
    norm = _norm(db_type)
    if norm in _PG_LIKE:
        # is_called=true ⇒ nextval() returns target, not target - 1.
        return "SELECT setval(:seq, :last, true)", {
            "seq": wm.generator,
            "last": target - 1,
        }
    if norm in _MYSQL_LIKE:
        ref = quote_table_ref(table, schema or None, dialect="mysql")
        return f"ALTER TABLE {ref} AUTO_INCREMENT = {int(target)}", {}
    if norm in _MSSQL_LIKE:
        # RESEED sets the *last* value; the next identity is reseed + IDENT_INCR.
        qualified = f"{schema or 'dbo'}.{table}"
        return (
            f"DBCC CHECKIDENT ('{qualified}', RESEED, {int(target) - 1})",
            {},
        )
    if norm == "oracle":
        ref = quote_table_ref(table, schema or None, dialect="oracle")
        col = _quote_column(wm.column, "oracle")
        # START WITH LIMIT VALUE re-seeds the identity sequence from the data
        # already in the column — exactly the post-migration ceiling.
        return (
            f"ALTER TABLE {ref} MODIFY ({col} GENERATED BY DEFAULT AS IDENTITY "
            "(START WITH LIMIT VALUE))",
            {},
        )
    if norm == "sqlite" and wm.mechanism == "autoincrement":
        return "UPDATE sqlite_sequence SET seq = :seq WHERE name = :t", {
            "seq": int(target) - 1,
            "t": table,
        }
    return None


def repair_identity_watermark(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str = "",
    table: str,
    watermark: IdentityWatermark,
) -> IdentityWatermark:
    """Advance a colliding generator past the rows at rest.

    Forward-only: a generator already ahead of the data is left alone, because
    rewinding it would re-issue keys that exist.
    """
    if not watermark.collides or watermark.max_value is None:
        return watermark
    target = int(watermark.max_value) + 1
    statement = _repair_sql(db_type, watermark, schema, table, target)
    if statement is None:
        return replace(
            watermark,
            reason=(
                f"no forward-only reseed statement for '{db_type}' "
                f"{watermark.mechanism or 'generator'}"
            ),
        )
    sql, params = statement

    from connectors.generic_sql import get_sqlalchemy_engine

    engine = get_sqlalchemy_engine({**cfg, "type": db_type})
    with engine.begin() as conn:
        conn.execute(sa.text(sql), params)

    verified = read_identity_watermark(
        db_type, cfg, schema=schema, table=table, column=watermark.column
    )
    return replace(verified, repaired_to=verified.next_value)


def verify_identity_watermark(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str = "",
    table: str,
    columns: list[str],
    repair: bool = False,
) -> dict[str, Any]:
    """Read (and optionally repair) generator state for the destination keys.

    The payload always says which columns were actually checked: a caller must
    be able to tell "no collision" from "nothing was verified".
    """
    checked: list[dict[str, Any]] = []
    collisions: list[str] = []
    repaired: list[str] = []
    unverified: list[str] = []
    for column in [str(c) for c in columns if str(c or "").strip()]:
        try:
            wm = read_identity_watermark(
                db_type, cfg, schema=schema, table=table, column=column
            )
            if wm.collides and repair:
                wm = repair_identity_watermark(
                    db_type, cfg, schema=schema, table=table, watermark=wm
                )
                if wm.repaired_to is not None and not wm.collides:
                    repaired.append(column)
        except Exception as exc:  # noqa: BLE001 — probe must never fail the run
            logger.warning(
                "identity watermark probe failed for %s.%s: %s", table, column, exc
            )
            wm = IdentityWatermark(column=column, reason=f"probe failed: {exc}")
        checked.append(wm.to_dict())
        if wm.collides:
            collisions.append(column)
        elif not wm.available:
            unverified.append(column)

    return {
        "checked": checked,
        "columns_verified": [
            c["column"] for c in checked if c["available"] and not c["collides"]
        ],
        "collisions": collisions,
        "repaired": repaired,
        "unverified": unverified,
        "passed": not collisions,
        "verified": bool(checked) and not collisions and not unverified,
    }
