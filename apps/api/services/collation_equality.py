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
import unicodedata
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


def _local_fold(value: str, collation: ColumnCollation) -> str:
    """A fold that may call two values equal when the destination does not.

    It only decides which values are worth asking the destination about.
    ``NO PAD`` keeps trailing spaces, so ``'a'`` and ``'a '`` are not sent.
    ``PAD SPACE`` strips trailing spaces and not tabs. Accents are folded so
    an accent-insensitive collation still gets the pair.
    """
    text = value
    if str(collation.pad or "").upper() == "PAD SPACE":
        text = text.rstrip(" ")
    folded = unicodedata.normalize("NFKD", text).casefold()
    return "".join(ch for ch in folded if not unicodedata.combining(ch))


def _collision_buckets(values: list[str], collation: ColumnCollation) -> list[list[str]]:
    """Distinct values that might be one key, one list per local fold."""
    buckets: dict[str, list[str]] = {}
    for value in values:
        bucket = buckets.setdefault(_local_fold(value, collation), [])
        if value not in bucket:
            bucket.append(value)
    return [bucket for bucket in buckets.values() if len(bucket) > 1]


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
        # Insert in batches. One temp table still holds this collision
        # bucket only, not every distinct key of the proof (D391).
        for start in range(0, len(values), 100):
            chunk = values[start:start + 100]
            conn.execute(
                sa.text(f"INSERT INTO {table} (id, k) VALUES (:id, :k)"),  # nosec B608
                [{"id": start + offset, "k": value} for offset, value in enumerate(chunk)],
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
    rows = list(db.execute("SELECT k, seq, latest FROM spill_key"))
    usable = [
        (str(k), int(seq), int(latest if latest is not None else seq))
        for k, seq, latest in rows
        if not str(k).startswith(_NULL_KEY)
    ]
    if len(usable) < 2 or not columns:
        return 0
    grouped = _groups_on_destination(usable, dialect=dialect, endpoint=endpoint, columns=columns)
    if not grouped:
        return 0
    by_group: dict[str, list[tuple[int, int, str]]] = {}
    for key, seq, latest in usable:
        by_group.setdefault(grouped.get(key, key), []).append((seq, latest, key))
    dropped = 0
    for members in by_group.values():
        if len(members) < 2:
            continue
        # Upsert keeps the key text of the first insert and the value of
        # the latest write. Keeping the latest row whole kept 'A'/'v2'
        # when MySQL stored 'a'/'v3' (D391).
        first_seq, _first_latest, first_key = min(members, key=lambda item: item[0])
        latest_row_seq, _latest_at, _latest_key = max(members, key=lambda item: item[1])
        payload_row = db.execute(
            "SELECT payload FROM spill WHERE seq = ?",
            (latest_row_seq,),
        ).fetchone()
        if payload_row is None:
            continue
        payload = _payload_with_first_key(payload_row[0], columns, first_key)
        db.execute("UPDATE spill SET payload = ? WHERE seq = ?", (payload, first_seq))
        for seq, _latest, key in members:
            if seq == first_seq:
                continue
            db.execute("DELETE FROM spill WHERE seq = ?", (seq,))
            db.execute("DELETE FROM spill_key WHERE k = ?", (key,))
            dropped += 1
    return dropped


def _payload_with_first_key(payload: bytes, columns: list[str], first_key: str) -> bytes:
    """The latest row, with the key text of the first insert."""
    import pickle

    orow, shaped, origin = pickle.loads(payload)
    parts = first_key.split("\x1f")
    if isinstance(orow, dict):
        for column, part in zip(columns, parts):
            orow[column] = part
    return pickle.dumps((orow, shaped, origin), protocol=4)


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
    keys = [key for key, _seq, _latest in usable]
    parts = [tuple(key.split("\x1f")) for key in keys]
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
                # Ask the destination only about values a local fold thinks
                # might collide, one fold-bucket at a time. Every other key
                # stays itself and is not loaded into a temp table (D391).
                representatives: list[dict[str, str]] = []
                for index, fact in enumerate(facts):
                    values = [part[index] for part in parts]
                    mapping = {value: value for value in values}
                    for bucket in _collision_buckets(values, fact):  # type: ignore[arg-type]
                        mapping.update(group_key_values(conn, kind, fact, bucket))  # type: ignore[arg-type]
                    representatives.append(mapping)
                conn.commit()
    except Exception:
        logger.debug("destination key equality failed", exc_info=True)
        return None
    out: dict[str, str] = {}
    for key, part in zip(keys, parts):
        canonical = "\x1f".join(
            representatives[index].get(part[index], part[index]) for index in range(len(columns))
        )
        out[key] = canonical
    return out
