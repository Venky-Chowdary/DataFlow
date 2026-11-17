"""Key equality is the destination column's collation, not a guess.

MySQL 8's default ``utf8mb4_0900_ai_ci`` is accent-insensitive and ``NO PAD``:
``'a'`` and ``'a '`` are different keys, and ``'e'`` / ``'é'`` and ``'İ'`` /
``'i'`` are the same key
([collation pads](https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-sets.html)).
SQL Server pads trailing spaces only, never tabs
([collation precedence](https://learn.microsoft.com/en-us/sql/t-sql/statements/collations)).

The destination decides. Distinct key texts are inserted into a temporary
table whose column uses the live collation, and ``PARTITION BY`` that column
is the equality. A unique index on the same collation accepts and rejects the
same pairs.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")
_NULL_KEY = "\x00null:"


@dataclass(frozen=True)
class ColumnCollation:
    name: str
    pad: str  # "PAD SPACE" or "NO PAD"
    charset: str = ""


def _safe_ident(value: str) -> str:
    token = str(value or "").strip()
    if not _IDENT.match(token):
        raise ValueError(f"collation name {token!r} is not an identifier")
    return token


def read_column_collation(
    conn: Any,
    dialect: str,
    *,
    schema: str,
    table: str,
    column: str,
) -> ColumnCollation | None:
    """The live collation and its PAD attribute. ``None`` when the column has none."""
    import sqlalchemy as sa

    kind = str(dialect or "").lower()
    if kind in {"mysql", "mariadb"}:
        row = conn.execute(
            sa.text(
                "SELECT COLLATION_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c"
            ),
            {"t": table, "c": column},
        ).fetchone()
        if not row or not row[0]:
            return None
        name = str(row[0])
        pad_row = conn.execute(
            sa.text(
                "SELECT PAD_ATTRIBUTE, CHARACTER_SET_NAME FROM information_schema.COLLATIONS "
                "WHERE COLLATION_NAME = :n"
            ),
            {"n": name},
        ).fetchone()
        pad = str(pad_row[0]) if pad_row and pad_row[0] else "NO PAD"
        charset = str(pad_row[1]) if pad_row and len(pad_row) > 1 and pad_row[1] else ""
        return ColumnCollation(name=name, pad=pad, charset=charset)
    if kind in {"sqlserver", "mssql", "azure_sql"}:
        row = conn.execute(
            sa.text(
                "SELECT c.collation_name FROM sys.columns c "
                "JOIN sys.tables t ON c.object_id = t.object_id "
                "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                "WHERE s.name = :s AND t.name = :t AND c.name = :c"
            ),
            {"s": schema or "dbo", "t": table, "c": column},
        ).fetchone()
        if not row or not row[0]:
            return None
        name = str(row[0])
        # Non-binary SQL Server collations pad trailing spaces. Binary ones
        # compare every byte, including trailing spaces. Neither pads a tab.
        binary = name.upper().endswith("_BIN") or name.upper().endswith("_BIN2")
        return ColumnCollation(name=name, pad="NO PAD" if binary else "PAD SPACE")
    return None


def group_key_values(
    conn: Any,
    dialect: str,
    collation: ColumnCollation,
    values: list[str],
) -> dict[str, str]:
    """Map each value to the representative the destination treats as equal.

    The representative is the value with the lowest input index in its group.
    Two values that share a representative compare equal under ``collation``.
    """
    import sqlalchemy as sa

    if not values:
        return {}
    name = _safe_ident(collation.name)
    kind = str(dialect or "").lower()
    if kind in {"mysql", "mariadb"}:
        charset = _safe_ident(collation.charset or "utf8mb4")
        ddl = (
            "CREATE TEMPORARY TABLE df_eq_keys ("
            "id INT NOT NULL PRIMARY KEY, "
            f"k TEXT CHARACTER SET {charset} COLLATE {name})"
        )
        drop = "DROP TEMPORARY TABLE IF EXISTS df_eq_keys"
    elif kind in {"sqlserver", "mssql", "azure_sql"}:
        ddl = (
            "CREATE TABLE #df_eq_keys ("
            "id INT NOT NULL PRIMARY KEY, "
            f"k NVARCHAR(MAX) COLLATE {name})"
        )
        drop = "DROP TABLE IF EXISTS #df_eq_keys"
    else:
        return {value: value for value in values}
    table = "df_eq_keys" if kind in {"mysql", "mariadb"} else "#df_eq_keys"
    conn.execute(sa.text(drop))
    try:
        conn.execute(sa.text(ddl))
        conn.execute(
            sa.text(f"INSERT INTO {table} (id, k) VALUES (:id, :k)"),  # nosec B608
            [{"id": i, "k": value} for i, value in enumerate(values)],
        )
        rows = conn.execute(
            sa.text(
                f"SELECT id, MIN(id) OVER (PARTITION BY k) AS grp FROM {table}"  # nosec B608
            )
        ).fetchall()
    finally:
        try:
            conn.execute(sa.text(drop))
        except Exception:
            logger.debug("equality temp table drop failed", exc_info=True)
    groups: dict[int, list[int]] = {}
    for row in rows:
        groups.setdefault(int(row[1]), []).append(int(row[0]))
    out: dict[str, str] = {}
    for members in groups.values():
        members.sort()
        representative = values[members[0]]
        for index in members:
            out[values[index]] = representative
    for value in values:
        out.setdefault(value, value)
    return out


def collapse_destination_equal_keys(
    db: Any,
    *,
    dialect: str,
    endpoint: Any,
    columns: list[str],
) -> int:
    """Merge spill rows the destination unique index would treat as one key.

    Returns how many extra rows that merge dropped. Zero when the destination
    cannot be asked: exact text was already collapsed, and a guessed pad or
    case fold is not applied (D391).
    """
    rows = list(db.execute("SELECT k, seq FROM spill_key"))
    usable = [(str(k), int(seq)) for k, seq in rows if not str(k).startswith(_NULL_KEY)]
    if len(usable) < 2 or not columns:
        return 0
    grouped = _groups_on_destination(usable, dialect=dialect, endpoint=endpoint, columns=columns)
    if not grouped:
        return 0
    by_group: dict[str, list[tuple[int, str]]] = {}
    for key, seq in usable:
        by_group.setdefault(grouped.get(key, key), []).append((seq, key))
    dropped = 0
    for members in by_group.values():
        if len(members) < 2:
            continue
        members.sort()
        for seq, key in members[:-1]:
            db.execute("DELETE FROM spill WHERE seq = ?", (seq,))
            db.execute("DELETE FROM spill_key WHERE k = ?", (key,))
            dropped += 1
    return dropped


def _groups_on_destination(
    usable: list[tuple[str, int]],
    *,
    dialect: str,
    endpoint: Any,
    columns: list[str],
) -> dict[str, str] | None:
    kind = str(dialect or "").lower()
    if kind not in {"mysql", "mariadb", "sqlserver", "mssql", "azure_sql"}:
        return None
    try:
        from services.dest_precount import warehouse_sql_engine
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(endpoint)
    except Exception:
        logger.debug("destination config for key equality failed", exc_info=True)
        return None
    schema = str(getattr(endpoint, "schema", "") or cfg.get("schema") or "")
    table = str(getattr(endpoint, "table", "") or cfg.get("table") or "")
    if not table:
        return None
    parts = [tuple(key.split("\x1f")) for key, _seq in usable]
    if any(len(part) != len(columns) for part in parts):
        return None
    try:
        with warehouse_sql_engine(kind, cfg) as engine:
            with engine.connect() as conn:
                facts = [
                    read_column_collation(
                        conn, kind, schema=schema, table=table, column=column
                    )
                    for column in columns
                ]
                if any(fact is None for fact in facts):
                    return None
                # One column is the case the unique-index tests cover. A
                # composite key is equal when every part is equal, so each
                # part is grouped on its own collation and the groups are zipped.
                representatives: list[dict[str, str]] = []
                for index, fact in enumerate(facts):
                    values = [part[index] for part in parts]
                    representatives.append(
                        group_key_values(conn, kind, fact, values)  # type: ignore[arg-type]
                    )
                conn.commit()
    except Exception:
        logger.debug("destination key equality failed", exc_info=True)
        return None
    out: dict[str, str] = {}
    for key, part in zip((item[0] for item in usable), parts):
        canonical = "\x1f".join(
            representatives[index].get(part[index], part[index]) for index in range(len(columns))
        )
        out[key] = canonical
    return out
