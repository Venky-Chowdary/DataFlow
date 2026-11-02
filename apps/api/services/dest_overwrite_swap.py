"""Overwrite publish boundary for COPY destinations (C03 / X-OW2, D195).

A COPY overwrite loads a run-scoped staging table, then publishes it. The
staging table is built from DataFlow's own DDL, so publishing it by renaming
it onto the live name replaced the destination's table object: every CHECK,
FOREIGN KEY, NOT NULL, DEFAULT, index, comment and trigger the operator had
declared was gone after the first full refresh (D195).

Publishing now keeps the live table object and replaces only its rows. One
routine per engine, chosen from :data:`OVERWRITE_PUBLISH_CAPABILITIES`:

- ``truncate_in_transaction`` (PostgreSQL, SQL Server): DDL is transactional,
  so ``TRUNCATE`` (``DELETE`` when a foreign key references the table) and
  ``INSERT ... SELECT`` from staging run inside the caller's transaction.
  Readers keep the old rows until the caller commits.
- ``delete_in_transaction`` (MySQL, Oracle, Snowflake): ``TRUNCATE`` commits
  on its own there, so ``DELETE`` and ``INSERT ... SELECT`` share one explicit
  transaction.

The live constraint set is read before and after the rows move (PostgreSQL,
MySQL, SQL Server). Any difference fails the run before it commits. When
staging carries a column the live table lacks, the COPY route declines
(``FastPathUnavailable``) and the row path, which evolves the live table in
place, takes the run.
"""

from __future__ import annotations

import logging
import re
import secrets
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: How each engine replaces the rows of an existing table on overwrite.
OVERWRITE_PUBLISH_CAPABILITIES: dict[str, str] = {
    "postgresql": "truncate_in_transaction",
    "sqlserver": "truncate_in_transaction",
    "mysql": "delete_in_transaction",
    "oracle": "delete_in_transaction",
    "snowflake": "delete_in_transaction",
}


class OverwriteConstraintDrift(RuntimeError):
    """The destination's constraint set changed across an overwrite."""


def mysql_staged_overwrite_name(dest_table: str) -> str:
    """Run-scoped staging name (<=64 chars, MySQL identifier cap)."""
    return f"df_ow_{dest_table}_{secrets.token_hex(4)}"[:64]


def mysql_overwrite_trash_name(dest_table: str) -> str:
    """Kept for callers; publishing no longer parks the live table anywhere."""
    return f"df_trash_{dest_table}_{secrets.token_hex(4)}"[:64]


def staged_overwrite_name(dest_table: str, *, prefix: str = "df_ow") -> str:
    """Run-scoped staging name for any dialect."""
    return f"{prefix}_{dest_table}_{secrets.token_hex(4)}"[:63]


def overwrite_trash_name(dest_table: str) -> str:
    """Kept for callers; publishing no longer parks the live table anywhere."""
    return f"df_trash_{dest_table}_{secrets.token_hex(4)}"[:63]


# ---------------------------------------------------------------- helpers


def _literal(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _rows(cur: Any, sql: str) -> list[tuple]:
    cur.execute(sql)  # nosec B608 - catalog reads over escaped literals
    if not getattr(cur, "description", None):
        return []
    return [tuple(r) for r in cur.fetchall()]


def _columns(cur: Any, ref: str) -> list[str]:
    cur.execute(f"SELECT * FROM {ref} WHERE 1=0")  # nosec B608 - quoted ref
    names = [str(d[0]) for d in (cur.description or [])]
    try:
        cur.fetchall()
    except Exception:  # noqa: BLE001 - some drivers refuse a fetch on 0 rows
        logger.debug("empty column probe fetch skipped", exc_info=True)
    return names


def _decline(reason: str) -> None:
    from services.copy_fast_path import FastPathUnavailable

    raise FastPathUnavailable(reason)


def _pair_columns(staging: list[str], live: list[str], *, live_ref: str) -> list[tuple[str, str]]:
    """``(staging name, live name)`` for every staged column, matched by case."""
    live_by_fold = {c.casefold(): c for c in live}
    pairs: list[tuple[str, str]] = []
    missing: list[str] = []
    for col in staging:
        hit = live_by_fold.get(col.casefold())
        if hit is None:
            missing.append(col)
        else:
            pairs.append((col, hit))
    if missing:
        _decline(
            f"overwrite of {live_ref} stages column(s) the live table lacks "
            f"({', '.join(missing)}); the row path evolves the live table and "
            "keeps its constraints"
        )
    return pairs


def verify_constraint_set(
    before: frozenset[str] | None, after: frozenset[str] | None, *, table: str
) -> None:
    """Fail when the overwrite changed the destination's constraint set."""
    if before is None or after is None:
        return
    lost = sorted(before - after)
    gained = sorted(after - before)
    if lost or gained:
        raise OverwriteConstraintDrift(
            f"overwrite of {table} changed the destination's constraints: "
            f"lost {lost[:6]}, gained {gained[:6]}; the run was rolled back"
        )


# ------------------------------------------------------------ fingerprints


def pg_constraint_fingerprint(
    cur: Any, schema: str, table: str, quote: Callable[[str], str]
) -> frozenset[str]:
    """CHECK / FK / PK / UNIQUE, indexes, NOT NULL, DEFAULT, comments, triggers."""
    reg = _literal(f"{quote(schema)}.{quote(table)}") + "::regclass"
    out: set[str] = set()
    for name, kind, ddl in _rows(
        cur,
        "SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint "
        f"WHERE conrelid = {reg}",
    ):
        out.add(f"constraint:{name}|{kind}|{ddl}")
    for name, ddl in _rows(
        cur,
        "SELECT c.relname, pg_get_indexdef(i.indexrelid) FROM pg_index i "
        f"JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = {reg}",
    ):
        out.add(f"index:{name}|{ddl}")
    for name, notnull, default, typ, comment in _rows(
        cur,
        "SELECT a.attname, a.attnotnull, pg_get_expr(d.adbin, d.adrelid), "
        "format_type(a.atttypid, a.atttypmod), col_description(a.attrelid, a.attnum) "
        "FROM pg_attribute a LEFT JOIN pg_attrdef d "
        "ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
        f"WHERE a.attrelid = {reg} AND a.attnum > 0 AND NOT a.attisdropped",
    ):
        out.add(f"column:{name}|notnull={notnull}|default={default}|{typ}|comment={comment}")
    for (comment,) in _rows(cur, f"SELECT obj_description({reg}, 'pg_class')"):
        out.add(f"comment:{comment}")
    for (name,) in _rows(
        cur, f"SELECT tgname FROM pg_trigger WHERE tgrelid = {reg} AND NOT tgisinternal"
    ):
        out.add(f"trigger:{name}")
    return frozenset(out)


_MYSQL_VOLATILE = re.compile(r"\s+AUTO_INCREMENT=\d+")


def mysql_constraint_fingerprint(cur: Any, ref: str) -> frozenset[str]:
    """``SHOW CREATE TABLE`` lines: columns, keys, CHECK, FK, DEFAULT, comment."""
    rows = _rows(cur, f"SHOW CREATE TABLE {ref}")
    if not rows:
        return frozenset()
    text = _MYSQL_VOLATILE.sub("", str(rows[0][1]))
    return frozenset(
        line.strip().rstrip(",") for line in text.splitlines()[1:] if line.strip()
    )


def sqlserver_constraint_fingerprint(cur: Any, schema: str, table: str) -> frozenset[str]:
    """CHECK / FK / DEFAULT / keys, indexes with their columns, NOT NULL, triggers."""
    oid = f"OBJECT_ID({_literal(f'[{schema}].[{table}]')})"
    out: set[str] = set()
    for name, ddl in _rows(
        cur, f"SELECT name, definition FROM sys.check_constraints WHERE parent_object_id = {oid}"
    ):
        out.add(f"check:{name}|{ddl}")
    for name, ref_obj, col, ref_col in _rows(
        cur,
        "SELECT fk.name, OBJECT_NAME(fk.referenced_object_id), "
        "COL_NAME(fc.parent_object_id, fc.parent_column_id), "
        "COL_NAME(fc.referenced_object_id, fc.referenced_column_id) "
        "FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fc "
        f"ON fc.constraint_object_id = fk.object_id WHERE fk.parent_object_id = {oid}",
    ):
        out.add(f"fk:{name}|{col}->{ref_obj}.{ref_col}")
    for name, ddl, col in _rows(
        cur,
        "SELECT name, definition, COL_NAME(parent_object_id, parent_column_id) "
        f"FROM sys.default_constraints WHERE parent_object_id = {oid}",
    ):
        out.add(f"default:{name}|{col}|{ddl}")
    for name, uniq, pk, col, ordinal in _rows(
        cur,
        "SELECT i.name, i.is_unique, i.is_primary_key, "
        "COL_NAME(ic.object_id, ic.column_id), ic.key_ordinal FROM sys.indexes i "
        "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        f"WHERE i.object_id = {oid} AND i.name IS NOT NULL",
    ):
        out.add(f"index:{name}|unique={uniq}|pk={pk}|{ordinal}:{col}")
    for name, nullable, typ, length, prec, scale in _rows(
        cur,
        "SELECT name, is_nullable, TYPE_NAME(user_type_id), max_length, precision, scale "
        f"FROM sys.columns WHERE object_id = {oid}",
    ):
        out.add(f"column:{name}|nullable={nullable}|{typ}({length},{prec},{scale})")
    for (name,) in _rows(cur, f"SELECT name FROM sys.triggers WHERE parent_id = {oid}"):
        out.add(f"trigger:{name}")
    return frozenset(out)


# ---------------------------------------------------------------- publish


def _pg_empty_in_transaction(cur: Any, live_ref: str, live_reg: str) -> None:
    referenced = bool(_rows(
        cur, f"SELECT 1 FROM pg_constraint WHERE contype = 'f' AND confrelid = {live_reg}"
    ))
    if referenced:
        cur.execute(f"DELETE FROM {live_ref}")  # nosec B608
        return
    cur.execute("SAVEPOINT df_overwrite_publish")
    try:
        cur.execute(f"TRUNCATE TABLE {live_ref}")  # nosec B608
        cur.execute("RELEASE SAVEPOINT df_overwrite_publish")
    except Exception as exc:  # noqa: BLE001 - TRUNCATE refused: DELETE keeps semantics
        logger.info("Overwrite TRUNCATE refused for %s (%s); deleting rows", live_ref, exc)
        cur.execute("ROLLBACK TO SAVEPOINT df_overwrite_publish")
        cur.execute(f"DELETE FROM {live_ref}")  # nosec B608


def pg_empty_live_for_overwrite(
    cur: Any, *, schema: str, table: str, quote: Callable[[str], str],
    column_types: dict[str, str],
) -> bool:
    """Empty an existing live table inside the caller's transaction.

    Returns ``False`` when the table does not exist (the caller creates it).
    Declines (``FastPathUnavailable``) when a loaded column is missing from the
    live table or its declared type differs from ``column_types`` — a binary
    COPY needs the exact type, and the row path evolves the live table.
    """
    live_ref = f"{quote(schema)}.{quote(table)}"
    exists = _rows(cur, f"SELECT to_regclass({_literal(live_ref)})")
    if not exists or exists[0][0] is None:
        return False
    live_reg = _literal(live_ref) + "::regclass"
    live_types = {
        str(name).casefold(): str(typ)
        for name, typ in _rows(
            cur,
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            f"WHERE attrelid = {live_reg} AND attnum > 0 AND NOT attisdropped",
        )
    }
    differs = [
        col for col, typ in column_types.items()
        if live_types.get(col.casefold()) is None
        or (typ and live_types.get(col.casefold()) != typ)
    ]
    if differs:
        _decline(
            f"overwrite of {live_ref}: live column(s) {', '.join(differs)} are absent or "
            "typed differently from the source; the row path keeps the table and "
            "its constraints"
        )
    _pg_empty_in_transaction(cur, live_ref, live_reg)
    return True


def pg_swap_staged_dest(
    cur: Any, *, schema: str, dest_table: str, staging_table: str,
    trash_table: str = "", quote,
) -> None:
    """Replace the live table's rows with staging's inside the caller's txn.

    The live table object stays, with every constraint, index, comment,
    trigger and grant. ``trash_table`` is accepted for old callers and unused.
    """
    live_ref = f"{quote(schema)}.{quote(dest_table)}"
    stage_ref = f"{quote(schema)}.{quote(staging_table)}"
    pairs = _pair_columns(_columns(cur, stage_ref), _columns(cur, live_ref), live_ref=live_ref)
    before = pg_constraint_fingerprint(cur, schema, dest_table, quote)
    live_reg = _literal(live_ref) + "::regclass"
    stage_reg = _literal(stage_ref) + "::regclass"
    types = {
        (str(rel), str(name)): str(typ)
        for rel, name, typ in _rows(
            cur,
            f"SELECT CASE WHEN attrelid = {live_reg} THEN 'live' ELSE 'stage' END, "
            "attname, format_type(atttypid, atttypmod) FROM pg_attribute "
            f"WHERE attrelid IN ({live_reg}, {stage_reg}) AND attnum > 0 AND NOT attisdropped",
        )
    }
    identity = {
        str(name)
        for (name,) in _rows(
            cur,
            f"SELECT attname FROM pg_attribute WHERE attrelid = {live_reg} "
            "AND attnum > 0 AND attidentity <> ''",
        )
    }
    _pg_empty_in_transaction(cur, live_ref, live_reg)
    selects = []
    for stage_col, live_col in pairs:
        live_t = types.get(("live", live_col))
        expr = quote(stage_col)
        if live_t and types.get(("stage", stage_col)) != live_t:
            expr = f"CAST({expr} AS {live_t})"
        selects.append(expr)
    overriding = (
        " OVERRIDING SYSTEM VALUE" if any(live in identity for _s, live in pairs) else ""
    )
    cur.execute(  # nosec B608 - identifiers quoted by the caller's quoter
        f"INSERT INTO {live_ref} ({', '.join(quote(live) for _s, live in pairs)}){overriding} "
        f"SELECT {', '.join(selects)} FROM {stage_ref}"
    )
    cur.execute(f"DROP TABLE {stage_ref}")  # nosec B608
    verify_constraint_set(
        before, pg_constraint_fingerprint(cur, schema, dest_table, quote), table=live_ref
    )


def sqlserver_swap_staged_dest(
    cur: Any, *, schema: str, dest_table: str, staging_table: str,
    trash_table: str = "", quote,
) -> None:
    """Replace the live table's rows with staging's inside the caller's txn.

    T-SQL ``TRUNCATE`` is transactional; ``DELETE`` runs when a foreign key
    references the table. The live object keeps its CHECK, FK, DEFAULT,
    indexes and triggers.
    """
    live_ref = f"{quote(schema)}.{quote(dest_table)}"
    stage_ref = f"{quote(schema)}.{quote(staging_table)}"
    pairs = _pair_columns(_columns(cur, stage_ref), _columns(cur, live_ref), live_ref=live_ref)
    before = sqlserver_constraint_fingerprint(cur, schema, dest_table)
    oid = f"OBJECT_ID({_literal(f'[{schema}].[{dest_table}]')})"
    referenced = bool(
        _rows(cur, f"SELECT 1 FROM sys.foreign_keys WHERE referenced_object_id = {oid}")
    )
    identity = {
        str(n).casefold()
        for (n,) in _rows(
            cur, f"SELECT name FROM sys.columns WHERE object_id = {oid} AND is_identity = 1"
        )
    }
    if referenced:
        cur.execute(f"DELETE FROM {live_ref}")  # nosec B608
    else:
        try:
            cur.execute(f"TRUNCATE TABLE {live_ref}")  # nosec B608
        except Exception as exc:  # noqa: BLE001 - statement-level refusal; txn stays open
            logger.info("Overwrite TRUNCATE refused for %s (%s); deleting rows", live_ref, exc)
            cur.execute(f"DELETE FROM {live_ref}")  # nosec B608
    identity_insert = any(live.casefold() in identity for _s, live in pairs)
    if identity_insert:
        cur.execute(f"SET IDENTITY_INSERT {live_ref} ON")  # nosec B608
    try:
        cur.execute(  # nosec B608
            f"INSERT INTO {live_ref} ({', '.join(quote(live) for _s, live in pairs)}) "
            f"SELECT {', '.join(quote(s) for s, _l in pairs)} FROM {stage_ref}"
        )
    finally:
        if identity_insert:
            cur.execute(f"SET IDENTITY_INSERT {live_ref} OFF")  # nosec B608
    cur.execute(f"DROP TABLE {stage_ref}")  # nosec B608
    verify_constraint_set(
        before, sqlserver_constraint_fingerprint(cur, schema, dest_table), table=live_ref
    )


def _delete_insert_in_transaction(
    cur: Any,
    *,
    live_ref: str,
    stage_ref: str,
    quote: Callable[[str], str],
    begin: str,
    drop_stage: str,
    fingerprint: Callable[[], frozenset[str] | None],
) -> None:
    pairs = _pair_columns(_columns(cur, stage_ref), _columns(cur, live_ref), live_ref=live_ref)
    before = fingerprint()
    if begin:
        cur.execute(begin)
    try:
        cur.execute(f"DELETE FROM {live_ref}")  # nosec B608
        cur.execute(  # nosec B608
            f"INSERT INTO {live_ref} ({', '.join(quote(live) for _s, live in pairs)}) "
            f"SELECT {', '.join(quote(s) for s, _l in pairs)} FROM {stage_ref}"
        )
        verify_constraint_set(before, fingerprint(), table=live_ref)
        cur.execute("COMMIT")
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:  # noqa: BLE001 - surface the original failure
            logger.debug("overwrite publish rollback skipped", exc_info=True)
        raise
    cur.execute(drop_stage)  # nosec B608


def _backtick(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _double_quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def mysql_swap_staged_dest(
    cur: Any, *, dest_q: str, staging_q: str, trash_q: str = ""
) -> None:
    """Replace the live table's rows with staging's in one transaction.

    MySQL ``TRUNCATE`` and ``RENAME`` commit on their own, so the rows move
    with ``DELETE`` + ``INSERT ... SELECT`` between ``START TRANSACTION`` and
    ``COMMIT``. The live table keeps its CHECK, FK, DEFAULT, keys and comment.
    """
    _delete_insert_in_transaction(
        cur,
        live_ref=dest_q,
        stage_ref=staging_q,
        quote=_backtick,
        begin="START TRANSACTION",
        drop_stage=f"DROP TABLE {staging_q}",
        fingerprint=lambda: mysql_constraint_fingerprint(cur, dest_q),
    )


def snowflake_swap_staged_dest(cur: Any, *, dest_ref: str, staging_ref: str) -> None:
    """Replace the live table's rows with staging's in one transaction.

    ``SWAP WITH`` exchanged the whole table object, so the destination's NOT
    NULL, DEFAULT, comments and declared keys went with it.
    """
    _delete_insert_in_transaction(
        cur,
        live_ref=dest_ref,
        stage_ref=staging_ref,
        quote=_double_quote,
        begin="BEGIN",
        drop_stage=f"DROP TABLE IF EXISTS {staging_ref}",
        fingerprint=lambda: None,
    )


def oracle_swap_staged_dest(
    cur: Any, *, dest_ref: str, staging_ref: str,
    dest_table: str = "", trash_table: str = "",
) -> None:
    """Replace the live table's rows with staging's in one transaction.

    Oracle DDL auto-commits, so the boundary is DML: ``DELETE`` + ``INSERT
    ... SELECT`` then ``COMMIT``; the staging table is purged afterwards.
    """
    _delete_insert_in_transaction(
        cur,
        live_ref=dest_ref,
        stage_ref=staging_ref,
        quote=_double_quote,
        begin="",
        drop_stage=f"DROP TABLE {staging_ref} PURGE",
        fingerprint=lambda: None,
    )
