"""Long-tail dialect MERGE bodies (DB2, Teradata, Trino, HANA, Vertica, ...).

Each engine spells the same idempotent apply differently — staging tables,
``MERGE`` vs ``UPDATE``+``INSERT``, bracket vs double-quote identifiers. The
strategy inventory lives in :mod:`connectors.merge_registry`; the SQL lives
here so ``generic_sql`` stays about the shared write path.

All of these are at-least-once idempotent applies, never exactly-once.
"""

from __future__ import annotations

import contextlib
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
