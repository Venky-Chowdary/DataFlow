"""Ask the destination which distinct source keys its key collation makes equal.

D205's walk models a collation in Python: PAD SPACE, case, accents, width,
kana. That model is not the whole rule. SQL Server's version-80 collations
(``SQL_Latin1_General_CP1_CI_AS``, the default of most installs,
``Latin1_General_CI_AS``) give *no* sort weight to supplementary characters
and to thousands of BMP code points, so ``'😀' = '🚀'`` and
``'𝔘𝔫𝔦𝔠𝔬𝔡𝔢' = '' = '  '``; the version-100 ones still ignore unassigned
code points; MySQL's ``0900`` collations have their own ignorables and
expansions. Two such source keys passed preflight and the run died mid-write
on a duplicate key (or, in an upsert, one row would overwrite the other).

The destination is the only exact authority, and it is live at preflight. For
a text key under a non-binary collation, the distinct written keys go into a
session temp table that carries the key column's collation, and the engine's
own ``GROUP BY`` names every group of distinct keys it treats as one.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

logger = logging.getLogger(__name__)

_SQLSERVER = {"sqlserver", "mssql", "azure_sql", "azure_sql_database"}
_MYSQL = {"mysql", "mariadb"}
#: Keys sent to the destination in one probe (the walk keeps 2M in memory).
_MAX_KEYS = 200_000
_COLLATE_RE = re.compile(r"\bCOLLATE\s+([A-Za-z0-9_]+)", re.I)


def _collation(ddl: str) -> str:
    match = _COLLATE_RE.search(ddl or "")
    return match.group(1) if match else ""


def _needs_oracle(ddl: str) -> bool:
    """A text key whose collation is not binary (or not stated) needs the engine."""
    from services.type_system import normalize_logical_type

    if normalize_logical_type(ddl or "") not in {"string", "text"}:
        return False
    name = _collation(ddl).lower()
    return not (name.endswith("_bin") or name.endswith("_bin2") or name == "binary")


def engine_key_oracle(
    equalizers: Mapping[str, Any],
    key_sources: Sequence[str],
    *,
    dest_kind: str,
    destination_config: Mapping[str, Any] | None,
) -> Callable[[list[tuple[str, ...]]], list[list[int]]] | None:
    """A callable grouping written keys the destination compares equal, or None.

    ``equalizers`` is :func:`services.dest_key_equality.key_equalizers`' answer
    (its ddl is the key column's catalog type, or the type CREATE will stamp).
    """
    kind = (dest_kind or "").strip().lower()
    if kind not in _SQLSERVER | _MYSQL or not destination_config or not key_sources:
        return None
    ddls = [str((equalizers.get(col) or (None, ""))[1] or "") for col in key_sources]
    if not any(_needs_oracle(d) for d in ddls):
        return None
    cfg = dict(destination_config)

    def _groups(keys: list[tuple[str, ...]]) -> list[list[int]]:
        if len(keys) < 2:
            return []
        return _engine_groups(cfg, kind, ddls, keys[:_MAX_KEYS])

    return _groups


#: Keys per OPENJSON round trip; one JSON text parameter carries the whole chunk.
_JSON_CHUNK = 20_000


def _fill_sqlserver_by_json(conn: Any, table: str, names: str, width: int, keys: list) -> bool:
    """Load the keys in a few OPENJSON round trips; False means use executemany.

    pyodbc sends a parameterised executemany one row per round trip, so 200k
    keys cost minutes inside preflight's deadline. OPENJSON needs compatibility
    level 130+ (SQL Server 2016); an older database falls back.
    """
    import json

    import sqlalchemy as sa

    shape = ", ".join(f"c{i} NVARCHAR(4000) '$[{i + 1}]'" for i in range(width))
    load = sa.text(
        f"INSERT INTO {table} (id, {names}) SELECT id, {names} "  # nosec B608
        f"FROM OPENJSON(:doc) WITH (id INT '$[0]', {shape})"
    )
    try:
        for start in range(0, len(keys), _JSON_CHUNK):
            doc = json.dumps(
                [[start + n, *key] for n, key in enumerate(keys[start:start + _JSON_CHUNK])],
                ensure_ascii=False,
            )
            conn.execute(load, {"doc": doc})
    except Exception:  # noqa: BLE001 - old compatibility level: row-wise load
        logger.debug("key oracle OPENJSON load unavailable", exc_info=True)
        conn.execute(sa.text(f"DELETE FROM {table}"))  # nosec B608
        return False
    return True


def _engine_groups(
    cfg: dict[str, Any], kind: str, ddls: list[str], keys: list[tuple[str, ...]]
) -> list[list[int]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    width = len(ddls)
    if kind in _SQLSERVER:
        cols = ", ".join(
            f"c{i} NVARCHAR(4000) COLLATE {_collation(d) or 'DATABASE_DEFAULT'}"
            for i, d in enumerate(ddls)
        )
        create = f"CREATE TABLE #df_key_oracle (id INT NOT NULL, {cols})"
        agg = "STRING_AGG(CAST(id AS VARCHAR(20)), ',')"
        table = "#df_key_oracle"
        drop = "DROP TABLE #df_key_oracle"
    else:
        cols = ", ".join(
            f"c{i} VARCHAR(768) CHARACTER SET utf8mb4"
            + (f" COLLATE {_collation(d)}" if _collation(d) else "")
            for i, d in enumerate(ddls)
        )
        create = f"CREATE TEMPORARY TABLE df_key_oracle (id INT NOT NULL, {cols})"
        agg = "GROUP_CONCAT(id SEPARATOR ',')"
        table = "df_key_oracle"
        drop = "DROP TEMPORARY TABLE df_key_oracle"
    names = ", ".join(f"c{i}" for i in range(width))
    marks = ", ".join(f":c{i}" for i in range(width))
    insert = sa.text(f"INSERT INTO {table} (id, {names}) VALUES (:id, {marks})")  # nosec B608
    query = sa.text(
        f"SELECT {agg} FROM {table} GROUP BY {names} HAVING COUNT(*) > 1"  # nosec B608
    )
    engine = _engine(cfg)
    groups: list[list[int]] = []
    with engine.connect() as conn:
        conn.execute(sa.text(create))
        try:
            if not (kind in _SQLSERVER and _fill_sqlserver_by_json(conn, table, names, width, keys)):
                rows = [
                    {"id": idx, **{f"c{i}": part for i, part in enumerate(key)}}
                    for idx, key in enumerate(keys)
                ]
                for start in range(0, len(rows), 5_000):
                    conn.execute(insert, rows[start:start + 5_000])
            for (members,) in conn.execute(query):
                ids = sorted(int(x) for x in str(members or "").split(",") if x.strip())
                if len(ids) > 1:
                    groups.append(ids)
        finally:
            try:
                conn.execute(sa.text(drop))
            except Exception:  # noqa: BLE001 - the session ends with the connection
                logger.debug("key oracle temp table drop failed", exc_info=True)
        conn.rollback()
    return groups
