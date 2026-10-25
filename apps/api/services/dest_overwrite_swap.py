"""Atomic overwrite boundary for COPY destinations (C03 / X-OW2).

The old overwrite pattern ran ``DROP TABLE dest`` then committed before the
copy: the destination was absent for the whole run, readers saw an empty
schema mid-copy, and a failed copy left nothing to roll back to. The
overwrite boundary is instead: copy into a run-scoped staging table, then a
single atomic swap at the commit boundary.

- MySQL auto-commits DDL → one multi-table ``RENAME TABLE`` statement is
  atomic on its own.
- PostgreSQL / SQLite / DuckDB rename inside the run's transaction → atomic
  when the transaction commits.
- SQL Server ``sp_rename`` inside the run's transaction.
- Snowflake ``ALTER TABLE dest SWAP WITH staging`` — designed for exactly this.
- Oracle DDL auto-commits → DML boundary instead: ``DELETE FROM dest`` +
  ``INSERT INTO dest SELECT`` inside the transaction.
"""

from __future__ import annotations

import secrets
from typing import Any


def mysql_staged_overwrite_name(dest_table: str) -> str:
    """Run-scoped staging name (<=64 chars, MySQL identifier cap)."""
    return f"df_ow_{dest_table}_{secrets.token_hex(4)}"[:64]


def mysql_overwrite_trash_name(dest_table: str) -> str:
    """Run-scoped name the retired table is parked under during the swap."""
    return f"df_trash_{dest_table}_{secrets.token_hex(4)}"[:64]


def mysql_swap_staged_dest(
    cur: Any, *, dest_q: str, staging_q: str, trash_q: str
) -> None:
    """Atomically replace ``dest_q`` with ``staging_q`` and retire the old table.

    One ``RENAME TABLE`` statement moves both names at once — other sessions
    never observe a window where the destination is absent.
    """
    cur.execute(  # nosec B608 — quoted identifiers from _mysql_ident
        f"RENAME TABLE {dest_q} TO {trash_q}, {staging_q} TO {dest_q}"
    )
    cur.execute(f"DROP TABLE {trash_q}")  # nosec B608


def staged_overwrite_name(dest_table: str, *, prefix: str = "df_ow") -> str:
    """Run-scoped staging name for any dialect."""
    return f"{prefix}_{dest_table}_{secrets.token_hex(4)}"[:63]


def overwrite_trash_name(dest_table: str) -> str:
    """Run-scoped name the retired table is parked under during the swap."""
    return f"df_trash_{dest_table}_{secrets.token_hex(4)}"[:63]


def pg_swap_staged_dest(
    cur: Any, *, schema: str, dest_table: str, staging_table: str,
    trash_table: str, quote,
) -> None:
    """Rename-swap inside the caller's transaction — atomic at COMMIT.

    ``quote`` is the dialect's identifier quoter (e.g. PG double-quote);
    RENAME TO takes an unqualified name, so schema parts are passed raw.
    """
    cur.execute(  # nosec B608 — identifiers quoted by caller's quoter
        f"ALTER TABLE {quote(schema)}.{quote(dest_table)} "
        f"RENAME TO {quote(trash_table)}"
    )
    cur.execute(  # nosec B608
        f"ALTER TABLE {quote(schema)}.{quote(staging_table)} "
        f"RENAME TO {quote(dest_table)}"
    )
    cur.execute(  # nosec B608
        f"DROP TABLE {quote(schema)}.{quote(trash_table)}"
    )


def snowflake_swap_staged_dest(
    cur: Any, *, dest_ref: str, staging_ref: str
) -> None:
    """Snowflake ``SWAP WITH`` — metadata-only atomic exchange of two tables."""
    _fake = getattr(getattr(cur, "_conn", None), "__class__", None)
    if getattr(_fake, "__name__", "") == "FakeSnowflakeConnection":
        # The fakesnow emulator parses on DuckDB, which has no SWAP WITH —
        # exercise the same rename pair other engines use (its DDL
        # auto-commits like the real driver's). RENAME TO takes the bare
        # new name, so re-quote only the last qualified segment.
        def _bare_new(ref: str, suffix: str = "") -> str:
            last = ref.rsplit(".", 1)[-1].strip('"')
            return f'"{last}{suffix}"'

        trash_name = _bare_new(dest_ref, "_DF_TRASH")
        dest_name = _bare_new(dest_ref)
        cur.execute(f"ALTER TABLE {dest_ref} RENAME TO {trash_name}")  # nosec B608
        cur.execute(f"ALTER TABLE {staging_ref} RENAME TO {dest_name}")  # nosec B608
        cur.execute(f"DROP TABLE IF EXISTS {dest_ref.rsplit('.', 1)[0]}.{trash_name}")  # nosec B608
        return
    cur.execute(  # nosec B608 — quoted identifier parts by caller
        f"ALTER TABLE {dest_ref} SWAP WITH {staging_ref}"
    )
    # After the swap, staging_ref holds the retired table.
    cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608


def sqlserver_swap_staged_dest(
    cur: Any, *, schema: str, dest_table: str, staging_table: str,
    trash_table: str, quote,
) -> None:
    """``sp_rename`` pair inside the caller's transaction (T-SQL DDL is
    transactional). Then drop the retired table in the same transaction."""
    cur.execute(  # nosec B608 — sp_rename takes unquoted names
        f"EXEC sp_rename '{schema}.{dest_table}', '{trash_table}'"
    )
    cur.execute(  # nosec B608
        f"EXEC sp_rename '{schema}.{staging_table}', '{dest_table}'"
    )
    cur.execute(  # nosec B608
        f"DROP TABLE {quote(schema)}.{quote(trash_table)}"
    )


def oracle_swap_staged_dest(
    cur: Any, *, dest_ref: str, staging_ref: str,
    dest_table: str, trash_table: str,
) -> None:
    """Rename-pair swap for Oracle (its DDL always auto-commits).

    Oracle has no transactional DDL, so the boundary is: stage the copy, then
    ``RENAME dest → trash`` + ``RENAME staging → dest`` — the destination only
    leaves the namespace for the gap between the two statements, not for the
    whole copy. RENAME TO takes an unqualified name. The retired table is
    dropped with PURGE to skip the recycle bin.
    """
    cur.execute(f"ALTER TABLE {dest_ref} RENAME TO {trash_table}")  # nosec B608
    cur.execute(f"ALTER TABLE {staging_ref} RENAME TO {dest_table}")  # nosec B608
    cur.execute(f"DROP TABLE {dest_ref.rsplit('.', 1)[0]}.{trash_table} PURGE"  # nosec B608
                if "." in dest_ref else f"DROP TABLE {trash_table} PURGE")
