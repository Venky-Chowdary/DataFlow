"""Atomic overwrite boundary for MySQL destinations (C03 / X-OW2).

MySQL auto-commits DDL, so the old ``DROP TABLE dest`` + copy pattern left the
destination table absent for the whole run: readers during the run saw an
empty schema and a failed copy left nothing to roll back to. The overwrite
boundary is instead: copy into a run-scoped staging table, then one atomic
``RENAME TABLE`` swap (MySQL treats a multi-table RENAME as a single atomic
statement) and drop the retired table. A failed run leaves the real
destination untouched and only staging to clean up.
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
