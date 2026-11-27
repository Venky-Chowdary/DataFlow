"""Which key equality a requested dedupe uses, decided before any write (D402).

keep_last collapses copies whose key text is identical. A destination key
column can compare more loosely: MariaDB's ``utf8mb4_general_ci`` calls
``'a'`` and ``'A'`` one key, and PAD SPACE calls ``'b'`` and ``'b '`` one key;
SQL Server pads trailing spaces and its default collations fold case. A file
holding both spellings is then ambiguous: they are two keys to the file and
one key to the table. The run used to collapse by exact text, write both,
and then fail its own post-write check after the table had been replaced.

The ambiguity is found before the write, against the destination's own
equality (a session temp table carrying the key column's collation, the same
oracle the D391 proof uses), and the operator chooses:

* ``dedupe_key_equality=destination``: collapse by the destination's rules,
  last row in source order wins; the post-write check groups natively;
* ``dedupe_key_equality=exact``: keep both spellings as distinct keys; the
  post-write check groups by exact bytes. Refused when an existing unique
  key on the destination would reject the second spelling.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

CHOICE_KEY = "dedupe_key_equality"
EXACT, DESTINATION = "exact", "destination"
_ASKED = frozenset({"mysql", "mariadb", "sqlserver", "mssql", "azure_sql", "postgresql"})


class DedupeEqualityRefused(ValueError):
    """Keys distinct by exact text are one key under the destination's equality."""


def equality_choice(*extras: Any) -> str:
    for extra in extras:
        if isinstance(extra, dict):
            raw = str(extra.get(CHOICE_KEY) or "").strip().lower()
            if raw in {EXACT, DESTINATION}:
                return raw
    return ""


def _target_names(keys: list[str], mappings: Any) -> list[str]:
    from services.dedupe_postwrite import _target_names as names

    return names(keys, mappings)


def _column_facts(conn: Any, kind: str, *, schema: str, table: str, columns: list[str],
                  table_exists: bool, created_collation: str) -> list[Any]:
    """The collation each key column compares with, or None for a non-text column."""
    import sqlalchemy as sa

    from services.collation_equality import (
        ColumnCollation,
        read_column_collation,
        sqlserver_collation_fact,
    )

    if table_exists:
        return [read_column_collation(conn, kind, schema=schema, table=table, column=c)
                for c in columns]
    if kind == "postgresql":
        return [None for _ in columns]  # a database default collation is always deterministic
    mysql = kind in {"mysql", "mariadb"}
    name = created_collation
    if not name:
        sql = ("SELECT @@collation_database" if mysql
               else "SELECT CONVERT(NVARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))")
        name = str(conn.execute(sa.text(sql)).scalar() or "")
    if not name:
        return [None for _ in columns]
    if mysql:
        from services.collation_equality import mysql_collation_fact

        fact = mysql_collation_fact(conn, name)
    else:
        fact = sqlserver_collation_fact(name)
    return [fact for _ in columns]


def _unique_key_covers(conn: Any, kind: str, *, schema: str, table: str, columns: list[str]) -> bool:
    """An existing unique index or key made of exactly these columns (any order)."""
    import sqlalchemy as sa

    try:
        uniques = sa.inspect(conn).get_unique_constraints(table, schema=schema or None)
        pk = sa.inspect(conn).get_pk_constraint(table, schema=schema or None)
        indexes = sa.inspect(conn).get_indexes(table, schema=schema or None)
    except Exception:  # noqa: BLE001 - an unreadable catalog proves nothing
        return False
    wanted = {c.lower() for c in columns}
    sets = [{str(c).lower() for c in (pk or {}).get("constrained_columns") or []}]
    sets += [{str(c).lower() for c in u.get("column_names") or []} for u in uniques or []]
    sets += [{str(c).lower() for c in i.get("column_names") or [] if c}
             for i in indexes or [] if i.get("unique")]
    return any(s and s <= wanted for s in sets)


def destination_key_groups(
    destination: Any,
    key_targets: list[str],
    key_tuples: list[tuple],
    *,
    created_key: bool = False,
) -> tuple[dict[tuple, tuple], bool] | None:
    """Map each exact key to the destination's representative, and whether an
    existing unique key covers the columns. ``None`` when the destination
    cannot be asked (an engine without collations is exact)."""
    from services.collation_equality import _collision_buckets, group_key_values
    from services.dest_precount import warehouse_sql_engine
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    from src.transfer.models import EndpointConfig

    # Only an engine with collations is asked; everything else compares exactly.
    # A caller without an endpoint (no connection to ask) collapses exactly.
    if not isinstance(destination, EndpointConfig) or resolve_driver_type(
        str(getattr(destination, "format", "") or "")
    ).lower() not in _ASKED:
        return None
    cfg = resolve_connector_config(destination)
    kind = resolve_driver_type(str(cfg.get("type") or getattr(destination, "format", "") or "")).lower()
    table = str(getattr(destination, "table", "") or cfg.get("table") or "")
    if kind not in _ASKED or not table or not key_tuples:
        return None
    import sqlalchemy as sa

    # MySQL's schema is the connection's database; SQL Server's defaults to dbo.
    schema = "" if kind in {"mysql", "mariadb"} else str(
        getattr(destination, "schema", "") or cfg.get("schema")
        or ("public" if kind == "postgresql" else "dbo"))
    with warehouse_sql_engine(kind, cfg) as engine:
        with engine.connect() as conn:
            exists = bool(sa.inspect(conn).has_table(table, schema=schema or None))
            created_collation = ""
            if created_key and not exists:
                # CREATE stamps the source's equality on a key column (D205).
                from services.created_key_collation import created_key_collation

                created_collation = created_key_collation(kind, kind)
            facts = _column_facts(conn, kind, schema=schema, table=table, columns=key_targets,
                                  table_exists=exists, created_collation=created_collation)
            # A table this run creates gets its key as PRIMARY KEY when the
            # dedupe key is the stream key, under the collation stamped above.
            covered = _unique_key_covers(
                conn, kind, schema=schema, table=table, columns=key_targets
            ) if exists else created_key
            reps: list[dict[str, str]] = []
            for index, fact in enumerate(facts):
                values = sorted({str(t[index]) for t in key_tuples if t[index] is not None})
                mapping = {v: v for v in values}
                if fact is not None:
                    for bucket in _collision_buckets(values, fact):
                        mapping.update(group_key_values(conn, kind, fact, bucket))
                reps.append(mapping)
            conn.commit()
    out: dict[tuple, tuple] = {}
    for key in key_tuples:
        out[key] = tuple(
            reps[i].get(str(v), str(v)) if v is not None else v for i, v in enumerate(key)
        )
    return out, covered


def _refusal(keys: list[str], groups: list[list[tuple]], *, covered: bool, choice: str) -> str:
    shown = "; ".join(" = ".join(repr(k[0] if len(k) == 1 else k) for k in g) for g in groups[:3])
    head = (
        f"The dedupe key {', '.join(keys)} has values that are different by exact text but "
        f"the same key to the destination ({shown})."
    )
    if choice == EXACT:
        return (
            f"{head} Keeping both is not possible: the destination's unique key on "
            f"{', '.join(keys)} would reject the second one. Nothing was written. "
            "Set dedupe_key_equality=destination to keep the last of them."
        )
    return (
        f"{head} Nothing was written. Choose one: dedupe_key_equality=destination "
        "collapses them by the destination's rules (the last row in the file wins), or "
        "dedupe_key_equality=exact keeps both as distinct keys"
        + (" (not possible here: a unique key on the destination rejects that)." if covered else ".")
    )


def collapse_for_destination(
    rows: list[dict],
    keys: list[str],
    *,
    destination: Any,
    mappings: Any = None,
    extras: tuple = (),
    created_key: bool = False,
) -> list[dict]:
    """keep_last under the equality the operator chose; refuse an ambiguous file."""
    from services.duplicate_policy import collapse_keep_last
    from services.value_serializer import is_reader_null_cell

    collapsed, _ = collapse_keep_last(list(rows), keys)
    if destination is None or str(getattr(destination, "kind", "database") or "") != "database":
        return collapsed  # a file export holds what it is sent: exact text is its equality
    choice = equality_choice(*extras)
    tuples = [
        tuple(r.get(k) for k in keys) for r in collapsed
        if isinstance(r, dict) and not any(
            r.get(k) is None or is_reader_null_cell(r.get(k)) for k in keys)
    ]
    try:
        asked = destination_key_groups(
            destination, _target_names(keys, mappings), tuples, created_key=created_key
        )
    except Exception as exc:  # noqa: BLE001 - the write must not guess the destination's equality
        raise DedupeEqualityRefused(
            "The destination's key equality for the dedupe key could not be read, so "
            "the collapse cannot be proven to match the table. Nothing was written. "
            f"({type(exc).__name__})"
        ) from exc
    if asked is None:
        return collapsed
    mapping, covered = asked
    grouped: dict[tuple, list[tuple]] = {}
    for key, rep in mapping.items():
        grouped.setdefault(rep, []).append(key)
    groups = [g for g in grouped.values() if len(g) > 1]
    if not groups:
        return collapsed
    if choice == DESTINATION:
        def _rep(row: dict) -> tuple:
            key = tuple(row.get(k) for k in keys)
            return mapping.get(key, key)

        return collapse_keep_last(list(rows), keys, key_fn=_rep)[0]
    if choice == EXACT and not covered:
        return collapsed
    raise DedupeEqualityRefused(_refusal(keys, groups, covered=covered, choice=choice))


def _destination_key_sets(endpoint: Any) -> list[set[str]] | None:
    """Unique key column sets on the destination table; ``None`` when it does not exist."""
    import sqlalchemy as sa

    from services.dest_precount import warehouse_sql_engine
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    kind = resolve_driver_type(str(cfg.get("type") or getattr(endpoint, "format", "") or "")).lower()
    table = str(getattr(endpoint, "table", "") or "")
    schema = "" if kind in {"mysql", "mariadb"} else str(getattr(endpoint, "schema", "") or cfg.get("schema") or "")
    with warehouse_sql_engine(kind, cfg) as engine:
        with engine.connect() as conn:
            inspector = sa.inspect(conn)
            if not inspector.has_table(table, schema=schema or None):
                return None
            pk = inspector.get_pk_constraint(table, schema=schema or None) or {}
            sets = [{str(c).lower() for c in pk.get("constrained_columns") or []}]
            sets += [{str(c).lower() for c in u.get("column_names") or []}
                     for u in inspector.get_unique_constraints(table, schema=schema or None) or []]
            sets += [{str(c).lower() for c in i.get("column_names") or [] if c}
                     for i in inspector.get_indexes(table, schema=schema or None) or [] if i.get("unique")]
    return [s for s in sets if s]


def refuse_dedupe_key_edit(sched: Any, new_key: Any) -> None:
    """Refuse a dedupe_key edit the next beat could not write (D402).

    Rows unique on the new key are unique on every key that contains it. A
    destination keyed on columns that do not contain the new key (keyed on
    ``id``, dedupe on ``id,name``) rejects the second row of an id on the next
    beat. The edit is refused now, with the reason, instead of every beat failing.
    """
    from services.duplicate_policy import DuplicatePolicyError, _dedupe_key_text
    from src.transfer.models import EndpointConfig

    new = _dedupe_key_text(new_key)
    if not new or new == _dedupe_key_text(getattr(sched, "dedupe_key", "")):
        return
    from services.connector_store import get_connector

    conn = get_connector(str(getattr(sched, "dest_connector_id", "") or ""),
                         workspace_id=str(getattr(sched, "workspace_id", "") or "") or None)
    if conn is None:
        return  # the save's own connector checks answer for a missing destination
    from src.transfer.connector_capabilities import resolve_driver_type
    from src.transfer.reconcile_step import _unique_check_dialect

    kind = resolve_driver_type(str(getattr(conn, "type", "") or "")).lower()
    if _unique_check_dialect(kind) is None:
        # Files, objects and document stores hold no SQL key that could reject
        # the next beat; the run's own dedupe and its post-write check decide.
        return
    endpoint = EndpointConfig(
        kind="database", format=str(getattr(conn, "type", "") or ""), connector_id=conn.id,
        table=str(getattr(sched, "dest_table", "") or ""),
        schema=str(getattr(sched, "dest_schema", "") or ""),
    )
    try:
        key_sets = _destination_key_sets(endpoint)
    except Exception as exc:  # noqa: BLE001 - an unread destination cannot clear the edit
        logger.warning("dedupe_key edit: destination keys unread: %s", type(exc).__name__)
        raise DuplicatePolicyError(
            f"dedupe_key {new!r} was not saved: Datawrap could not connect to the destination "
            f"{getattr(conn, 'name', '') or conn.id} to read the keys of "
            f"{getattr(sched, 'dest_table', '')}, so it cannot tell whether the next run could "
            "write this key. Test the destination connection, then save again."
        ) from exc
    if not key_sets:
        return
    wanted = {t.lower() for t in _target_names(new.split(","), getattr(sched, "mappings", None))}
    blocking = sorted({tuple(sorted(s)) for s in key_sets if not wanted <= s})
    if blocking:
        shown = ", ".join("(" + ", ".join(cols) + ")" for cols in blocking)
        raise DuplicatePolicyError(
            f"dedupe_key {new!r} was not saved: {getattr(sched, 'dest_table', '')} is keyed on "
            f"{shown}, so the next run would be refused when two rows share that key but differ "
            f"on {new}. Keep a dedupe_key that contains the destination's key, or point the "
            "schedule at a new table (or drop this one) first."
        )
