"""Long-tail dialect MERGE bodies (DB2, Teradata, Trino, HANA, Vertica, ...).

Each engine spells the same idempotent apply differently — staging tables,
``MERGE`` vs ``UPDATE``+``INSERT``, bracket vs double-quote identifiers. The
strategy inventory lives in :mod:`connectors.merge_registry`; the SQL lives
here so ``generic_sql`` stays about the shared write path.

``update_insert_upsert`` is the portable MERGE when native upsert cannot run
and dest-owned columns must survive (never DELETE+INSERT). All of these are
at-least-once idempotent applies, never exactly-once.
"""

from __future__ import annotations

import contextlib
import re
from typing import Any

import sqlalchemy as sa



def _duckdb_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _duckdb_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_duckdb_quote(table_obj.schema))
    parts.append(_duckdb_quote(table_obj.name))
    return ".".join(parts)


def _duckdb_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native DuckDB MERGE INTO with NULL-safe ON (no PK required).

    DuckDB supports MERGE without a unique index — preferred over delete+insert
    for concurrent readers. Still at-least-once. Caller falls back on error.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = f"df_mrg_{suffix}"
    stage_q = _duckdb_quote(stage)
    target = _duckdb_qualified_table(table_obj)
    col_sql = ", ".join(_duckdb_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE TEMP TABLE {stage_q} AS "
            f"SELECT {col_sql} FROM {target} WHERE 1=0"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage_q} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_duckdb_quote,
        )
        insert_cols = ", ".join(_duckdb_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_duckdb_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"{_duckdb_quote(c)} = s.{_duckdb_quote(c)}" for c in update_cols
            )
            # DuckDB UPDATE SET uses bare column names on the target side.
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_q} s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_q} s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {stage_q}"))


def _db2_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _db2_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_db2_quote(table_obj.schema))
    parts.append(_db2_quote(table_obj.name))
    return ".".join(parts)


def _db2_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native DB2 ``MERGE INTO`` with NULL-safe ON via session temp stage.

    Matches IBM / Fivetran-class LUW upsert: DECLARE GLOBAL TEMPORARY TABLE →
    INSERT stage → MERGE. Falls back to delete+insert when DECLARE/MERGE fails.
    Still at-least-once — not exactly-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    # SESSION. prefix is required for DGTT identity on LUW.
    stage = f"SESSION.DF_MRG_{suffix}"
    target = _db2_qualified_table(table_obj)
    col_sql = ", ".join(_db2_quote(c) for c in target_cols)
    try:
        conn.execute(
            sa.text(
                f"DECLARE GLOBAL TEMPORARY TABLE {stage} AS "
                f"(SELECT {col_sql} FROM {target} WHERE 1=0) "
                f"WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED"  # nosec B608
            )
        )
    except (sa.exc.SQLAlchemyError, OSError, ValueError):
        # Some DB2 z/OS / privilege profiles reject DGTT — let caller fall back.
        raise

    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_db2_quote,
        )
        insert_cols = ", ".join(_db2_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_db2_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_db2_quote(c)} = s.{_db2_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _teradata_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _teradata_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_teradata_quote(table_obj.schema))
    parts.append(_teradata_quote(table_obj.name))
    return ".".join(parts)


def _teradata_merge_on(conflict_cols: list[str]) -> str:
    """Teradata MERGE ON must be PI equality — cannot equate explicitly with NULL.

    Docs: match_condition cannot equate with NULL and must hash to a single AMP
    on the primary index. Do **not** use null_safe OR-IS-NULL form here.
    """
    return " AND ".join(
        f"t.{_teradata_quote(c)} = s.{_teradata_quote(c)}" for c in conflict_cols
    )


def _teradata_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Teradata ``MERGE INTO`` via VOLATILE stage (Fivetran/Vantage class).

    ON uses PI equality only (Teradata forbids NULL equate in MERGE ON).
    Conflict/PI columns are never UPDATEd. Still at-least-once.
    """
    if not rows:
        return 0
    # Never attempt to UPDATE primary-index columns (Teradata rejects it).
    safe_update = [c for c in update_cols if c not in conflict_cols]
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _teradata_quote(f"DF_MRG_{suffix}")
    target = _teradata_qualified_table(table_obj)
    col_sql = ", ".join(_teradata_quote(c) for c in target_cols)
    pi_sql = ", ".join(_teradata_quote(c) for c in conflict_cols)
    conn.execute(
        sa.text(
            f"CREATE MULTISET VOLATILE TABLE {stage} AS "
            f"(SELECT {col_sql} FROM {target} WHERE 1=0) "
            f"WITH DATA PRIMARY INDEX ({pi_sql}) "
            f"ON COMMIT PRESERVE ROWS"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = _teradata_merge_on(conflict_cols)
        insert_cols = ", ".join(_teradata_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_teradata_quote(c)}" for c in target_cols)
        if safe_update:
            set_sql = ", ".join(
                f"{_teradata_quote(c)} = s.{_teradata_quote(c)}" for c in safe_update
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON {on_sql} "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON {on_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _trino_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _trino_qualified_table(table_obj: sa.Table) -> str:
    parts: list[str] = []
    if table_obj.schema:
        # Trino may embed catalog.schema in Table.schema (e.g. "hive.default").
        for part in str(table_obj.schema).split("."):
            if part:
                parts.append(_trino_quote(part))
    parts.append(_trino_quote(table_obj.name))
    return ".".join(parts)


def _trino_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
    *,
    chunk_size: int = 50,
) -> int:
    """Native Trino/Presto/Athena ``MERGE INTO`` with NULL-safe ON (Iceberg MoR).

    Stages via ``VALUES`` chunks — Trino connector MERGE and Athena engine v3
    Iceberg ``MERGE INTO`` (AWS Big Data Blog / Athena MERGE docs). Falls back
    to delete+insert for Trino/Presto; Athena callers must use append-only
    fallback (see ``_upsert_batch``). Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    target = _trino_qualified_table(table_obj)
    on_sql = null_safe_merge_on(
        conflict_cols,
        left_alias="t",
        right_alias="s",
        quote_column=_trino_quote,
    )
    insert_cols = ", ".join(_trino_quote(c) for c in target_cols)
    insert_vals = ", ".join(f"s.{_trino_quote(c)}" for c in target_cols)
    set_sql = ""
    if update_cols:
        set_sql = ", ".join(
            f"{_trino_quote(c)} = s.{_trino_quote(c)}" for c in update_cols
        )
    alias_list = ", ".join(_trino_quote(c) for c in target_cols)
    written = 0
    size = max(1, int(chunk_size))
    for i in range(0, len(rows), size):
        chunk = rows[i : i + size]
        value_rows: list[str] = []
        params: dict[str, Any] = {}
        for ridx, row in enumerate(chunk):
            placeholders = []
            for col in target_cols:
                key = f"r{ridx}_{col}"
                params[key] = row.get(col)
                placeholders.append(f":{key}")
            value_rows.append(f"({', '.join(placeholders)})")
        values_sql = ", ".join(value_rows)
        if set_sql:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING (VALUES {values_sql}) AS s ({alias_list}) "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING (VALUES {values_sql}) AS s ({alias_list}) "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql), params)  # nosec B608
        written += len(chunk)
    return written


def _hana_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _hana_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_hana_quote(table_obj.schema))
    parts.append(_hana_quote(table_obj.name))
    return ".".join(parts)


def _hana_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native SAP HANA ``MERGE INTO`` with NULL-safe ON via local temp stage.

    HANA also offers ``UPSERT … WITH PRIMARY KEY`` for single-row PK paths;
    MERGE is the composite-key / CDC-class algorithm (Airbyte/Fivetran HANA
    destinations). Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    # HANA local temporary tables are session-scoped and named with #.
    stage = f"#DF_MRG_{suffix}"
    target = _hana_qualified_table(table_obj)
    col_sql = ", ".join(_hana_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE LOCAL TEMPORARY COLUMN TABLE {stage} AS "
            f"(SELECT {col_sql} FROM {target} WHERE 1=0)"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_hana_quote,
        )
        insert_cols = ", ".join(_hana_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_hana_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_hana_quote(c)} = s.{_hana_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _vertica_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _vertica_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_vertica_quote(table_obj.schema))
    parts.append(_vertica_quote(table_obj.name))
    return ".".join(parts)


def _vertica_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Vertica ``MERGE INTO`` with NULL-safe ON via local temp stage.

    Vertica docs: one MERGE upserts matched + unmatched in a single transaction
    (Fivetran Vertica / enterprise warehouse pattern). Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _vertica_quote(f"df_mrg_{suffix}")
    target = _vertica_qualified_table(table_obj)
    col_sql = ", ".join(_vertica_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE LOCAL TEMPORARY TABLE {stage} ON COMMIT PRESERVE ROWS AS "
            f"SELECT {col_sql} FROM {target} WHERE FALSE"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_vertica_quote,
        )
        insert_cols = ", ".join(_vertica_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_vertica_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"{_vertica_quote(c)} = s.{_vertica_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _netezza_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _netezza_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_netezza_quote(table_obj.schema))
    parts.append(_netezza_quote(table_obj.name))
    return ".".join(parts)


def _netezza_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Netezza / IBM NPS ``MERGE INTO`` with NULL-safe ON (7.2.1+).

    Stages via ``CREATE TEMP TABLE … AS SELECT … LIMIT 0`` then MERGE —
    IBM Performance Server / Fivetran Netezza class. Still at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _netezza_quote(f"df_mrg_{suffix}")
    target = _netezza_qualified_table(table_obj)
    col_sql = ", ".join(_netezza_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE TEMP TABLE {stage} AS "
            f"SELECT {col_sql} FROM {target} LIMIT 0"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_netezza_quote,
        )
        insert_cols = ", ".join(_netezza_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_netezza_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"{_netezza_quote(c)} = s.{_netezza_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _informix_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _informix_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_informix_quote(table_obj.schema))
    parts.append(_informix_quote(table_obj.name))
    return ".".join(parts)


def _informix_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Informix ``MERGE INTO`` with NULL-safe ON via TEMP stage.

    IBM/HCL docs: TEMP ``WITH NO LOG`` + MERGE join (Fivetran Informix class).
    Still at-least-once — not exactly-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    stage = _informix_quote(f"df_mrg_{suffix}")
    target = _informix_qualified_table(table_obj)
    col_sql = ", ".join(_informix_quote(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"CREATE TEMP TABLE {stage} AS "
            f"SELECT {col_sql} FROM {target} WHERE 1=0 "
            f"WITH NO LOG"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_informix_quote,
        )
        insert_cols = ", ".join(_informix_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_informix_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_informix_quote(c)} = s.{_informix_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _sybase_bracket(ident: str) -> str:
    return "[" + str(ident).replace("]", "]]") + "]"


def _sybase_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_sybase_bracket(table_obj.schema))
    parts.append(_sybase_bracket(table_obj.name))
    return ".".join(parts)


def _sybase_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native SAP ASE / Sybase ``MERGE`` (15.7+) with NULL-safe ON.

    Stages via ``SELECT … INTO #temp WHERE 1=0`` then MERGE — SAP Infocenter
    ASE MERGE class (same family as SQL Server, without HOLDLOCK). Still
    at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    stage = f"#df_mrg_{abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000}"
    target = _sybase_qualified_table(table_obj)
    col_sql = ", ".join(_sybase_bracket(c) for c in target_cols)
    conn.execute(
        sa.text(
            f"SELECT {col_sql} INTO {stage} FROM {target} WHERE 1=0"  # nosec B608
        )
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_sybase_bracket,
        )
        insert_cols = ", ".join(_sybase_bracket(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_sybase_bracket(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_sybase_bracket(c)} = s.{_sybase_bracket(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING {stage} AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


_KEY_HIT_CHUNK = 400


def update_insert_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
) -> int:
    """Portable MERGE: UPDATE matched keys, INSERT missing. Never DELETE.

    Native ``ON CONFLICT`` / ``MERGE`` already have this identity. Delete+insert
    is not the same algorithm: DELETE drops dest-owned columns (mirror
    ``_deleted``, identity, generated defaults) and INSERT materializes
    DEFAULT — a silent un-delete. This fallback is used when the dest table
    has dest-owned columns and native upsert cannot run (no unique index).
    Does not require a unique constraint. At-least-once, not exactly-once.
    """
    if not rows or not conflict_cols:
        return 0
    from connectors.writer_common import (
        _conflict_key_identity,
        assert_dense_upsert_keys_present,
    )

    assert_dense_upsert_keys_present(rows, conflict_cols)
    existing = _existing_conflict_keys(conn, table_obj, rows, conflict_cols)
    update_cols = [c for c in target_cols if c not in conflict_cols]
    to_update: list[dict[str, Any]] = []
    to_insert: list[dict[str, Any]] = []
    for row in rows:
        payload = {c: row.get(c) for c in target_cols}
        key = tuple(_conflict_key_identity(row[c]) for c in conflict_cols)
        if key in existing:
            if update_cols:
                to_update.append(payload)
        else:
            to_insert.append(payload)
    written = 0
    if to_update and update_cols:
        stmt = (
            table_obj.update()
            .where(
                sa.and_(
                    *(table_obj.c[c] == sa.bindparam(f"_k_{c}") for c in conflict_cols)
                )
            )
            .values({c: sa.bindparam(f"_u_{c}") for c in update_cols})
        )
        conn.execute(
            stmt,
            [
                {
                    **{f"_k_{c}": row[c] for c in conflict_cols},
                    **{f"_u_{c}": row.get(c) for c in update_cols},
                }
                for row in to_update
            ],
        )
        written += len(to_update)
    elif to_update:
        written += len(to_update)
    if to_insert:
        conn.execute(table_obj.insert(), to_insert)
        written += len(to_insert)
    return written


def _existing_conflict_keys(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
) -> set[tuple[Any, ...]]:
    from connectors.writer_common import _conflict_key_identity, _is_nullish_conflict_key

    keys: set[tuple[Any, ...]] = set()
    for i in range(0, len(rows), _KEY_HIT_CHUNK):
        chunk = rows[i : i + _KEY_HIT_CHUNK]
        clauses = [
            sa.and_(*[table_obj.c[c] == row[c] for c in conflict_cols])
            for row in chunk
            if not any(_is_nullish_conflict_key(row.get(c)) for c in conflict_cols)
        ]
        if not clauses:
            continue
        stmt = sa.select(*[table_obj.c[c] for c in conflict_cols]).where(
            sa.or_(*clauses)
        )
        for found in conn.execute(stmt):
            keys.add(
                tuple(_conflict_key_identity(found[j]) for j in range(len(conflict_cols)))
            )
    return keys


# --------------------------------------------------------------------------- #
# MSSQL / Oracle / ClickHouse / Firebird MERGE bodies and the sparse-column CDC
# apply, moved out of ``generic_sql`` (Phase F8 size freeze). Same contract as
# the rest of this module: an idempotent at-least-once apply, never a
# DELETE+INSERT that would drop destination-owned columns.
# --------------------------------------------------------------------------- #


def _generic_apply_sparse_upsert(
    conn: Any,
    table_obj: sa.Table,
    target_cols: list[str],
    conflict_columns: list[str],
    sparse_rows: list[dict[str, Any]],
    *,
    dialect_name: str = "",
    rejected_details: list[dict[str, Any]] | None = None,
    policy: str = "quarantine",
) -> tuple[int, int, list[tuple]]:
    """Per-row upsert omitting DF_MISSING — never SET col=NULL for absent CDC fields."""
    from connectors.writer_common import resolve_conflict_targets, run_sparse_cdc_upsert
    from services.value_serializer import is_missing_sentinel

    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        raise ValueError("sparse SQLAlchemy upsert requires conflict_columns")

    # Normalize dict rows to target_cols tuples for the shared loop.
    from services.value_serializer import DF_MISSING_SENTINEL

    as_tuples: list[tuple] = []
    for row in sparse_rows:
        as_tuples.append(
            tuple(
                (
                    row[c]
                    if c in row and not is_missing_sentinel(row.get(c))
                    else DF_MISSING_SENTINEL
                )
                for c in target_cols
            )
        )

    is_clickhouse = dialect_name == "clickhouse" or str(dialect_name).startswith(
        "clickhouse"
    )

    def fetch_existing(pk_vals: list[Any]) -> tuple | None:
        pk_clause = sa.and_(
            *[table_obj.c[c] == pk_vals[i] for i, c in enumerate(conflict)]
        )
        cols = [table_obj.c[c] for c in target_cols if c in table_obj.c]
        if len(cols) != len(target_cols):
            # Missing physical columns — return None so insert path can run.
            return None
        if is_clickhouse:
            # ReplacingMergeTree without FINAL can miss the current version and
            # fall through to a partial INSERT that NULL-wipes omitted attrs.
            from connectors.writer_common import quote_sql_identifier

            parts = []
            if table_obj.schema:
                parts.append(quote_sql_identifier(table_obj.schema))
            parts.append(quote_sql_identifier(table_obj.name))
            table_ref = clickhouse_final_table_sql(".".join(parts))
            col_sql = ", ".join(quote_sql_identifier(c) for c in target_cols)
            where_sql = " AND ".join(
                f"{quote_sql_identifier(c)} = :p{i}" for i, c in enumerate(conflict)
            )
            params = {f"p{i}": pk_vals[i] for i in range(len(conflict))}
            found = conn.execute(
                sa.text(f"SELECT {col_sql} FROM {table_ref} WHERE {where_sql}"),  # nosec B608
                params,
            ).fetchone()
            return tuple(found) if found is not None else None
        found = conn.execute(sa.select(*cols).where(pk_clause)).fetchone()
        return tuple(found) if found is not None else None

    def update_non_pk(non_pk: dict[str, Any], pk_vals: list[Any]) -> int:
        if is_clickhouse:
            # Mutations are not Airbyte-class upsert; force versioned INSERT path.
            return 0
        pk_clause = sa.and_(
            *[table_obj.c[c] == pk_vals[i] for i, c in enumerate(conflict)]
        )
        result = conn.execute(sa.update(table_obj).where(pk_clause).values(**non_pk))
        return int(getattr(result, "rowcount", 0) or 0)

    def insert_present(present: dict[str, Any]) -> None:
        conn.execute(sa.insert(table_obj).values(**present))

    return run_sparse_cdc_upsert(
        target_cols=target_cols,
        conflict_columns=conflict,
        sparse_rows=as_tuples,
        fetch_existing_row=fetch_existing,
        update_non_pk=update_non_pk,
        insert_present=insert_present,
        hydrate_versioned_insert=is_clickhouse,
        rejected_details=rejected_details,
        policy=policy,
    )


def _mssql_bracket(ident: str) -> str:
    """Bracket-quote a SQL Server identifier (escape ``]``)."""
    return "[" + str(ident).replace("]", "]]") + "]"


def _mssql_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_mssql_bracket(table_obj.schema))
    parts.append(_mssql_bracket(table_obj.name))
    return ".".join(parts)


def _mssql_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native T-SQL MERGE with HOLDLOCK + NULL-safe ON; staging temp table.

    Matches Airbyte/Fivetran-class SQL Server upsert: stage → MERGE → drop.
    Caller must fall back to delete+insert when this raises.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    stage = f"#df_mrg_{abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000}"
    target = _mssql_qualified_table(table_obj)
    col_sql = ", ".join(_mssql_bracket(c) for c in target_cols)
    # Clone column shapes from target — never invent VARCHAR widths.
    conn.execute(
        sa.text(f"SELECT TOP 0 {col_sql} INTO {stage} FROM {target}")  # nosec B608
    )
    try:
        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            params = {c: row.get(c) for c in target_cols}
            conn.execute(insert_sql, params)

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias=_mssql_bracket("t"),
            right_alias=_mssql_bracket("s"),
            quote_column=_mssql_bracket,
        )
        insert_cols = ", ".join(_mssql_bracket(c) for c in target_cols)
        insert_vals = ", ".join(
            f"{_mssql_bracket('s')}.{_mssql_bracket(c)}" for c in target_cols
        )
        if update_cols:
            set_sql = ", ".join(
                f"{_mssql_bracket('t')}.{_mssql_bracket(c)} = "
                f"{_mssql_bracket('s')}.{_mssql_bracket(c)}"
                for c in update_cols
            )
            merge_sql = (
                f"MERGE {target} WITH (HOLDLOCK) AS {_mssql_bracket('t')} "
                f"USING {stage} AS {_mssql_bracket('s')} "
                f"ON {on_sql} "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED BY TARGET THEN "
                f"INSERT ({insert_cols}) VALUES ({insert_vals});"
            )
        else:
            # Conflict-key-only rows: insert missing; leave matched alone.
            merge_sql = (
                f"MERGE {target} WITH (HOLDLOCK) AS {_mssql_bracket('t')} "
                f"USING {stage} AS {_mssql_bracket('s')} "
                f"ON {on_sql} "
                f"WHEN NOT MATCHED BY TARGET THEN "
                f"INSERT ({insert_cols}) VALUES ({insert_vals});"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        with contextlib.suppress(Exception):
            conn.execute(sa.text(f"DROP TABLE {stage}"))  # nosec B608


def _oracle_quote(ident: str) -> str:
    """Double-quote an Oracle identifier (escape embedded quotes)."""
    return '"' + str(ident).replace('"', '""') + '"'


def _oracle_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_oracle_quote(table_obj.schema))
    parts.append(_oracle_quote(table_obj.name))
    return ".".join(parts)


def _oracle_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Oracle MERGE with NULL-safe ON via session staging table.

    Stage → MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED (Oracle has no
    ``BY TARGET`` keyword). Prefer PRIVATE TEMPORARY TABLE (18c+); fall back to
    a session GLOBAL TEMPORARY TABLE. Caller falls back to delete+insert on error.
    Still at-least-once — not exactly-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    suffix = abs(hash((table_obj.name, tuple(conflict_cols)))) % 10_000_000
    # Private temp tables require the ORA$PTT_ prefix (Oracle default).
    ptt = f"ORA$PTT_DF_MRG_{suffix}"
    gtt = f"DF_MRG_{suffix}"
    target = _oracle_qualified_table(table_obj)
    col_sql = ", ".join(_oracle_quote(c) for c in target_cols)
    stage_ref = ""
    created: str | None = None
    try:
        try:
            stage_ref = _oracle_quote(ptt)
            conn.execute(
                sa.text(
                    f"CREATE PRIVATE TEMPORARY TABLE {stage_ref} "
                    f"ON COMMIT PRESERVE DEFINITION AS "
                    f"SELECT {col_sql} FROM {target} WHERE 1=0"  # nosec B608
                )
            )
            created = "ptt"
        except (sa.exc.SQLAlchemyError, OSError, ValueError):
            # Older Oracle / privilege gap — try session GTT (definition may persist).
            with contextlib.suppress(Exception):
                conn.rollback()
            stage_ref = _oracle_quote(gtt)
            conn.execute(
                sa.text(
                    f"CREATE GLOBAL TEMPORARY TABLE {stage_ref} "
                    f"ON COMMIT PRESERVE ROWS AS "
                    f"SELECT {col_sql} FROM {target} WHERE 1=0"  # nosec B608
                )
            )
            created = "gtt"

        placeholders = ", ".join(f":{c}" for c in target_cols)
        insert_sql = sa.text(
            f"INSERT INTO {stage_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
        )
        for row in rows:
            conn.execute(insert_sql, {c: row.get(c) for c in target_cols})

        on_sql = null_safe_merge_on(
            conflict_cols,
            left_alias="t",
            right_alias="s",
            quote_column=_oracle_quote,
        )
        insert_cols = ", ".join(_oracle_quote(c) for c in target_cols)
        insert_vals = ", ".join(f"s.{_oracle_quote(c)}" for c in target_cols)
        if update_cols:
            set_sql = ", ".join(
                f"t.{_oracle_quote(c)} = s.{_oracle_quote(c)}" for c in update_cols
            )
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_ref} s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} t "
                f"USING {stage_ref} s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql))  # nosec B608
        return len(rows)
    finally:
        if created == "gtt" and stage_ref:
            # PTT drops with session/definition; GTT definition may linger.
            with contextlib.suppress(Exception):
                conn.execute(sa.text(f"TRUNCATE TABLE {stage_ref}"))
            with contextlib.suppress(Exception):
                conn.execute(sa.text(f"DROP TABLE {stage_ref}"))


def _clickhouse_replacing_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Airbyte-class ClickHouse upsert: INSERT only into ReplacingMergeTree.

    Dedup is **engine-level and lazy** (background merge / ``SELECT … FINAL``).
    Never DELETE+INSERT — ClickHouse mutations race merges and are not
    Fivetran/Airbyte-class upsert semantics. Still at-least-once.
    """
    del conflict_cols, update_cols  # identity is table ORDER BY / version col
    if not rows:
        return 0
    result = conn.execute(table_obj.insert(), rows)
    return max(0, getattr(result, "rowcount", None) or 0) or len(rows)


def clickhouse_final_table_sql(table_ref: str) -> str:
    """``FROM <table> FINAL`` — Gate-8 must collapse ReplacingMergeTree duplicates.

    Airbyte ClickHouse destination docs: without FINAL (or OPTIMIZE), queries
    may see duplicate keys after at-least-once INSERT upserts.
    """
    ref = (table_ref or "").strip()
    if not ref:
        raise ValueError("clickhouse table ref required for FINAL select")
    # Idempotent if caller already appended FINAL.
    if re.search(r"\bFINAL\b", ref, flags=re.IGNORECASE):
        return ref
    return f"{ref} FINAL"


def _firebird_quote(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


def _firebird_qualified_table(table_obj: sa.Table) -> str:
    parts = []
    if table_obj.schema:
        parts.append(_firebird_quote(table_obj.schema))
    parts.append(_firebird_quote(table_obj.name))
    return ".".join(parts)


def _firebird_merge_upsert(
    conn: Any,
    table_obj: sa.Table,
    rows: list[dict[str, Any]],
    conflict_cols: list[str],
    target_cols: list[str],
    update_cols: list[str],
) -> int:
    """Native Firebird ``MERGE INTO`` with NULL-safe ON via ``RDB$DATABASE``.

    Firebird 2.1+ MERGE; staging each row from ``RDB$DATABASE`` avoids inventing
    GTT DDL without typed columns (Firebird Language Reference). Still
    at-least-once.
    """
    from connectors.writer_common import null_safe_merge_on

    if not rows:
        return 0
    target = _firebird_qualified_table(table_obj)
    on_sql = null_safe_merge_on(
        conflict_cols,
        left_alias="t",
        right_alias="s",
        quote_column=_firebird_quote,
    )
    insert_cols = ", ".join(_firebird_quote(c) for c in target_cols)
    insert_vals = ", ".join(f"s.{_firebird_quote(c)}" for c in target_cols)
    set_sql = ""
    if update_cols:
        set_sql = ", ".join(
            f"t.{_firebird_quote(c)} = s.{_firebird_quote(c)}" for c in update_cols
        )
    select_list = ", ".join(f":{c} AS {_firebird_quote(c)}" for c in target_cols)
    for row in rows:
        params = {c: row.get(c) for c in target_cols}
        if set_sql:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING (SELECT {select_list} FROM RDB$DATABASE) AS s "
                f"ON ({on_sql}) "
                f"WHEN MATCHED THEN UPDATE SET {set_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        else:
            merge_sql = (
                f"MERGE INTO {target} AS t "
                f"USING (SELECT {select_list} FROM RDB$DATABASE) AS s "
                f"ON ({on_sql}) "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
        conn.execute(sa.text(merge_sql), params)  # nosec B608
    return len(rows)
