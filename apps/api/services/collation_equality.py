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
        return mysql_collation_fact(conn, str(row[0]))
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
        return sqlserver_collation_fact(str(row[0]))
    if kind in {"postgresql", "postgres"}:
        return postgres_collation_fact(conn, schema=schema, table=table, column=column)
    return None


def sqlserver_collation_fact(name: str) -> ColumnCollation:
    """SQL Server pads trailing spaces in ``=`` under every collation.

    ``_BIN`` and ``_BIN2`` included: ANSI padding makes ``'b' = 'b '`` true
    whatever the collation, and a unique index rejects the pair. Tabs are not
    padded.
    """
    return ColumnCollation(name=name, pad="PAD SPACE")


def postgres_collation_fact(conn: Any, *, schema: str, table: str, column: str) -> ColumnCollation | None:
    """A nondeterministic (ICU) column collation, or ``None`` when equality is exact.

    PostgreSQL's deterministic collations compare by bytes, so distinct text
    is never one key. A nondeterministic one (``und-u-ks-level2``) can call
    ``'a'`` and ``'A'`` equal (``pg_collation.collisdeterministic``).
    """
    import sqlalchemy as sa

    row = conn.execute(sa.text(
        "SELECT c.collname, n.nspname, c.collisdeterministic FROM pg_attribute a "
        "JOIN pg_class t ON a.attrelid = t.oid "
        "JOIN pg_namespace tn ON t.relnamespace = tn.oid "
        "JOIN pg_collation c ON c.oid = a.attcollation "
        "JOIN pg_namespace n ON c.collnamespace = n.oid "
        "WHERE tn.nspname = :s AND t.relname = :t AND a.attname = :c"
    ), {"s": schema or "public", "t": table, "c": column}).fetchone()
    if not row or row[2]:
        return None
    quoted = ".".join('"' + str(part).replace('"', '""') + '"' for part in (row[1], row[0]))
    return ColumnCollation(name=quoted, pad="NO PAD")


def mysql_collation_fact(conn: Any, name: str) -> ColumnCollation:
    """A MySQL/MariaDB collation's PAD attribute and character set.

    MySQL 8 reports ``PAD_ATTRIBUTE``; MariaDB has no such column and spells
    NO PAD in the name (``utf8mb4_nopad_bin``); every other MariaDB collation,
    ``_bin`` included, pads trailing spaces.
    """
    import sqlalchemy as sa

    try:
        row = conn.execute(
            sa.text(
                "SELECT PAD_ATTRIBUTE, CHARACTER_SET_NAME FROM information_schema.COLLATIONS "
                "WHERE COLLATION_NAME = :n"
            ),
            {"n": name},
        ).fetchone()
        pad = str(row[0]) if row and row[0] else "NO PAD"
        charset = str(row[1]) if row and len(row) > 1 and row[1] else ""
    except Exception:  # noqa: BLE001 - MariaDB: no PAD_ATTRIBUTE column
        row = conn.execute(
            sa.text("SELECT CHARACTER_SET_NAME FROM information_schema.COLLATIONS WHERE COLLATION_NAME = :n"),
            {"n": name},
        ).fetchone()
        pad = "NO PAD" if "nopad" in name.lower() else "PAD SPACE"
        charset = str(row[0]) if row and row[0] else ""
    return ColumnCollation(name=name, pad=pad, charset=charset)


# Letters a UCA collation expands or folds to a base letter that NFKD does not
# decompose (``utf8mb4_0900_ai_ci``: 'Æ' = 'AE', 'Ø' = 'O'). Over-folding is
# safe: the fold only picks which values the destination is asked about.
_EXPANSIONS = {
    "æ": "ae", "œ": "oe", "ø": "o", "þ": "th", "ð": "d", "đ": "d", "ł": "l",
    "ħ": "h", "ı": "i", "ŀ": "l", "ŧ": "t", "ĳ": "ij", "ß": "ss", "ſ": "s",
    "ŋ": "n", "ə": "e", "ɖ": "d", "ƒ": "f",
}


def _local_fold(value: str, collation: ColumnCollation) -> str:
    """A fold that may call two values equal when the destination does not.

    It only decides which values are worth asking the destination about, so
    it must never split a pair the destination calls equal (D391: it missed
    'Œ'/'OE', 'Æ'/'AE', 'Ø'/'O' and a zero-width space, and a correct write
    failed its proof). ``NO PAD`` keeps trailing spaces, so ``'a'`` and
    ``'a '`` are not sent. ``PAD SPACE`` strips trailing spaces and not tabs.
    Accents and format characters (zero-width space, joiners, soft hyphen,
    which UCA ignores) are dropped, and letters with a UCA expansion expand.
    """
    text = value
    if str(collation.pad or "").upper() == "PAD SPACE":
        text = text.rstrip(" ")
    folded = unicodedata.normalize("NFKD", text).casefold()
    return "".join(
        _EXPANSIONS.get(ch, ch)
        for ch in folded
        if not unicodedata.combining(ch) and unicodedata.category(ch) != "Cf"
    )


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
    if not values:
        return {}
    kind = str(dialect or "").lower()
    if kind in {"postgresql", "postgres"}:
        # The name is the catalog's own, already quoted (postgres_collation_fact).
        ddl = f"CREATE TEMPORARY TABLE df_eq_keys (id INT NOT NULL PRIMARY KEY, k TEXT COLLATE {collation.name})"
        drop = "DROP TABLE IF EXISTS pg_temp.df_eq_keys"
        return _group_in_temp_table(conn, ddl, drop, "df_eq_keys", values)
    name = _safe_ident(collation.name)
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
    return _group_in_temp_table(conn, ddl, drop, table, values)


def _group_in_temp_table(conn: Any, ddl: str, drop: str, table: str, values: list[str]) -> dict[str, str]:
    import sqlalchemy as sa

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
    found = _groups_on_destination(usable, dialect=dialect, endpoint=endpoint, columns=columns)
    if not found:
        return 0
    grouped, stored = found
    by_group: dict[str, list[tuple[int, int, str]]] = {}
    for key, seq, latest in usable:
        by_group.setdefault(grouped.get(key, key), []).append((seq, latest, key))
    dropped = 0
    for members in by_group.values():
        if len(members) < 2:
            continue
        # The values are the latest write's (R4). The key's spelling is the
        # engine's: an upsert does not rewrite key columns, and which equal
        # spelling arrived first depends on how the writer batched. So the
        # spelling is read back from the destination (R5); the first source
        # spelling is only the fallback when that row cannot be read (D391).
        first_seq, _first_latest, first_key = min(members, key=lambda item: item[0])
        latest_row_seq, _latest_at, _latest_key = max(members, key=lambda item: item[1])
        payload_row = db.execute(
            "SELECT payload FROM spill WHERE seq = ?",
            (latest_row_seq,),
        ).fetchone()
        if payload_row is None:
            continue
        key_text = stored.get(grouped.get(first_key, first_key)) or first_key
        payload = _payload_with_first_key(payload_row[0], columns, key_text)
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


def _stored_key_texts(
    conn: Any,
    kind: str,
    *,
    schema: str,
    table: str,
    columns: list[str],
    probes: dict[str, tuple[str, ...]],
) -> dict[str, str]:
    """The spelling the destination holds for each collided key group.

    ``WHERE k = :v`` compares under the column's collation, so it finds the one
    stored row of the group whichever equal spelling it was written with.
    """
    import sqlalchemy as sa

    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    mysql = kind in {"mysql", "mariadb"}
    quote = "`" if mysql else "["
    ref = quote_table_ref(
        table, None if mysql else (schema or "dbo"), dialect="mysql" if mysql else "sqlserver",
        sanitize=False,
    )
    cols = ", ".join(quote_sql_identifier(c, quote) for c in columns)
    where = " AND ".join(
        f"{quote_sql_identifier(c, quote)} = :k{i}" for i, c in enumerate(columns)
    )
    head = "SELECT " if mysql else "SELECT TOP 2 "
    tail = " LIMIT 2" if mysql else ""
    stmt = sa.text(f"{head}{cols} FROM {ref} WHERE {where}{tail}")  # nosec B608
    out: dict[str, str] = {}
    for canonical, parts in probes.items():
        rows = conn.execute(stmt, {f"k{i}": part for i, part in enumerate(parts)}).fetchall()
        if len(rows) == 1:
            out[canonical] = "\x1f".join("" if v is None else str(v) for v in rows[0])
    return out


def _groups_on_destination(
    usable: list[tuple[str, int, int]],
    *,
    dialect: str,
    endpoint: Any,
    columns: list[str],
) -> tuple[dict[str, str], dict[str, str]] | None:
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
                out: dict[str, str] = {}
                members: dict[str, int] = {}
                for key, part in zip(keys, parts):
                    canonical = "\x1f".join(
                        representatives[index].get(part[index], part[index])
                        for index in range(len(columns))
                    )
                    out[key] = canonical
                    members[canonical] = members.get(canonical, 0) + 1
                probes = {
                    canonical: tuple(canonical.split("\x1f"))
                    for canonical, count in members.items()
                    if count > 1
                }
                stored = _stored_key_texts(
                    conn, kind, schema=schema, table=table, columns=columns, probes=probes
                ) if probes else {}
                conn.commit()
    except Exception:
        logger.debug("destination key equality failed", exc_info=True)
        return None
    return out, stored
