"""BigQuery and Snowflake copies of the watermark tie group, typed (D335).

The sample reader these stores went through compares text: BigQuery's keyed
read is ``CAST(col AS STRING) IN (...)``, and ``CAST(TIMESTAMP AS STRING)`` is
``2026-11-24 10:00:00.123456+00`` - a spelling none of the watermark's ISO
forms take, so a keyless BigQuery append re-appended its boundary row and the
post-write check, reading the same way, called the run clean. Here the cursor
is a typed parameter at the destination's precision (``carrier_values``):

* BigQuery: ``@lo``/``@hi`` as TIMESTAMP, DATETIME or DATE query parameters
  (the writer cuts past the microsecond, so does the carrier); the column type
  comes from the table's own schema;
* Snowflake: ``%s::TIMESTAMP_NTZ`` / ``::TIMESTAMP_TZ`` / ``::DATE`` text casts
  (nine digits survive a text cast; a Python datetime would not), the column
  type from ``DESC TABLE``.

Keys are matched on every key column (OR of ANDs), as on the SQL engines.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal
from typing import Any, Iterator

from .stream_boundary_read import (
    BoundaryStructural,
    _iso,
    _text_spellings,
    carrier_values,
    civil,
    instant,
)

_EPOCH = datetime(1970, 1, 1)
_KEY_CHUNK = 200


def _dt(seconds: Decimal) -> datetime:
    whole = int(seconds.to_integral_value(ROUND_FLOOR))
    micro = int(((seconds - whole) * 1_000_000).to_integral_value(ROUND_FLOOR))
    return _EPOCH + timedelta(seconds=whole, microseconds=micro)


def _window(watermark: str, kind: str, ddl: str, fam: str) -> tuple[Decimal, Decimal] | None:
    """``(lo, hi)`` seconds the destination stores the watermark as; None: not temporal."""
    inst = instant(watermark)
    if inst is None:
        return None
    zoned = kind == "tz"
    seconds = inst[0] if (zoned or not inst[1]) else instant(civil(watermark))[0]
    if kind == "date":
        day = seconds.quantize(Decimal(1), ROUND_FLOOR)
        return day, day
    values = carrier_values(seconds, ddl, fam)
    return min(values), max(values)


# --- BigQuery ------------------------------------------------------------------


def bigquery_rows(*, dialect, dest_cfg, table, cursor_col, watermark, key_cols, keys, columns, ddl=""):
    from google.cloud import bigquery as _bq

    from connectors.bigquery_conn import get_client
    from services.brand import bigquery_default_dataset
    from services.copy_bigquery_common import bigquery_ident, bigquery_table_ref

    project = str(dest_cfg.get("database") or "")
    dataset = str(dest_cfg.get("schema") or "") or bigquery_default_dataset()
    client = get_client(
        project_id=project,
        credentials_path=dest_cfg.get("connection_string", ""),
        service_account=dest_cfg.get("service_account", ""),
        host=dest_cfg.get("host", ""),
        port=int(dest_cfg.get("port") or 0),
        connection_string=dest_cfg.get("connection_string", ""),
    )
    table_id = ".".join(p for p in (project, dataset, table) if p)
    schema = {f.name.lower(): (f.name, str(f.field_type).upper()) for f in client.get_table(table_id).schema}
    need = [cursor_col, *(key_cols if keys is not None else [])]
    missing = [c for c in need if c.lower() not in schema]
    if missing:
        raise BoundaryStructural("the destination table has no column " + ", ".join(missing))
    names = [c for c in dict.fromkeys([*columns, *need]) if c.lower() in schema]
    stored = {c: schema[c.lower()][0] for c in names}
    ctype = schema[cursor_col.lower()][1]
    col = bigquery_ident(stored[cursor_col])
    params: list[Any] = []
    kind = {"TIMESTAMP": "tz", "DATETIME": "ntz", "DATE": "date"}.get(ctype)
    if kind:
        win = _window(str(watermark), kind, ctype, "bigquery")
        if win is None:
            raise BoundaryStructural(f"the watermark {watermark!r} is not a {ctype}")
        lo, hi = win
        if kind == "date":
            value_lo = value_hi = (_EPOCH + timedelta(seconds=int(lo))).date()
        elif kind == "tz":
            value_lo, value_hi = (_dt(lo).replace(tzinfo=timezone.utc), _dt(hi).replace(tzinfo=timezone.utc))
        else:
            value_lo, value_hi = _dt(lo), _dt(hi)
        where = f"{col} BETWEEN @lo AND @hi"
        params += [_bq.ScalarQueryParameter("lo", ctype, value_lo), _bq.ScalarQueryParameter("hi", ctype, value_hi)]
    elif ctype in {"INT64", "INTEGER", "NUMERIC", "BIGNUMERIC", "FLOAT64", "FLOAT"}:
        where = f"CAST({col} AS STRING) = @w"
        params.append(_bq.ScalarQueryParameter("w", "STRING", str(watermark)))
    elif ctype == "STRING":
        where = f"{col} IN UNNEST(@w)"
        params.append(_bq.ArrayQueryParameter("w", "STRING", _text_spellings(str(watermark))))
    else:
        raise BoundaryStructural(f"the cursor column's type {ctype} cannot be compared with the watermark")
    select = ", ".join(f"{bigquery_ident(stored[c])} AS {bigquery_ident(c.lower())}" for c in names)
    base = f"SELECT {select} FROM {bigquery_table_ref(project, dataset, table)} WHERE {where}"  # nosec B608
    chunks = [None] if keys is None else [keys[i : i + _KEY_CHUNK] for i in range(0, len(keys), _KEY_CHUNK)]
    for chunk in chunks:
        sql, chunk_params = base, list(params)
        if chunk is not None:
            terms = []
            for n, key in enumerate(chunk):
                parts = []
                for m, c in enumerate(key_cols):
                    ref = bigquery_ident(stored[c])
                    if key.get(c) is None:
                        parts.append(f"{ref} IS NULL")
                    else:
                        parts.append(f"CAST({ref} AS STRING) = @k{n}_{m}")
                        chunk_params.append(_bq.ScalarQueryParameter(f"k{n}_{m}", "STRING", str(key[c])))
                terms.append("(" + " AND ".join(parts) + ")")
            sql = f"{base} AND (" + " OR ".join(terms) + ")"
        job = client.query(sql, job_config=_bq.QueryJobConfig(query_parameters=chunk_params))
        for row in job.result(timeout=120):
            yield {str(k).lower(): v for k, v in dict(row.items()).items()}


# --- Snowflake -----------------------------------------------------------------


def _snowflake_kind(ddl: str) -> str:
    text = str(ddl or "").upper()
    if text.startswith(("TIMESTAMP_TZ", "TIMESTAMP_LTZ")) or "TIME ZONE" in text:
        return "tz"
    if text.startswith(("TIMESTAMP", "DATETIME")):
        return "ntz"
    if text.startswith("DATE"):
        return "date"
    if text.startswith(("NUMBER", "DECIMAL", "NUMERIC", "INT", "BIGINT", "FLOAT", "DOUBLE", "REAL")):
        return "num"
    if text.startswith(("VARCHAR", "TEXT", "STRING", "CHAR")):
        return "text"
    return ""


def snowflake_rows(*, dialect, dest_cfg, table, cursor_col, watermark, key_cols, keys, columns, ddl=""):
    from connectors.snowflake_conn import (
        get_connection,
        normalize_account,
        resolve_or_fold_snowflake_table,
        snowflake_column_ref,
        snowflake_physical_column_rows,
        snowflake_qualified_table,
    )

    schema = str(dest_cfg.get("schema") or "") or "PUBLIC"
    conn = get_connection(
        account=normalize_account(dest_cfg.get("host", "")),
        username=dest_cfg.get("username", ""),
        password=dest_cfg.get("password", ""),
        database=dest_cfg.get("database", ""),
        schema=schema,
        warehouse=dest_cfg.get("warehouse", ""),
        connection_string=dest_cfg.get("connection_string", ""),
    )
    try:
        cur = conn.cursor()
        resolved = resolve_or_fold_snowflake_table(cur, schema, table)
        rows = snowflake_physical_column_rows(cur, schema, resolved)
        types = {str(r[0]).upper(): (str(r[0]), str(r[1] or "")) for r in rows or []}
        need = [cursor_col, *(key_cols if keys is not None else [])]
        missing = [c for c in need if c.upper() not in types]
        if missing:
            raise BoundaryStructural("the destination table has no column " + ", ".join(missing))
        names = [c for c in dict.fromkeys([*columns, *need]) if c.upper() in types]
        physical = {u: n for u, (n, _t) in types.items()}
        ref = {c: snowflake_column_ref(c, physical) for c in names}
        ctype = types[cursor_col.upper()][1] or ddl
        kind = _snowflake_kind(ctype)
        col = ref[cursor_col]
        args: list[Any] = []
        if kind in ("tz", "ntz", "date"):
            win = _window(str(watermark), kind, ctype, "snowflake")
            if win is None:
                raise BoundaryStructural(f"the watermark {watermark!r} is not a {ctype}")
            lo, hi = win
            if kind == "date":
                where = f"{col} = %s::DATE"
                args.append(_iso(lo, 0)[:10])
            else:
                cast = "TIMESTAMP_TZ" if kind == "tz" else "TIMESTAMP_NTZ"
                suffix = " +00:00" if kind == "tz" else ""
                where = f"{col} BETWEEN %s::{cast} AND %s::{cast}"
                args += [_iso(lo, 9) + suffix, _iso(hi, 9) + suffix]
        elif kind == "num":
            where = f"{col} = %s"
            args.append(str(watermark))
        elif kind == "text":
            spellings = _text_spellings(str(watermark))
            where = f"{col} IN (" + ",".join(["%s"] * len(spellings)) + ")"
            args += spellings
        else:
            raise BoundaryStructural(f"the cursor column's type {ctype} cannot be compared with the watermark")
        select = ", ".join(f"{ref[c]} AS \"{c.lower()}\"" for c in names)
        base = f"SELECT {select} FROM {snowflake_qualified_table(schema, resolved)} WHERE {where}"  # nosec B608
        chunks = [None] if keys is None else [keys[i : i + _KEY_CHUNK] for i in range(0, len(keys), _KEY_CHUNK)]
        for chunk in chunks:
            sql, chunk_args = base, list(args)
            if chunk is not None:
                terms = []
                for key in chunk:
                    parts = []
                    for c in key_cols:
                        if key.get(c) is None:
                            parts.append(f"{ref[c]} IS NULL")
                        else:
                            parts.append(f"{ref[c]} = %s")
                            chunk_args.append(str(key[c]))
                    terms.append("(" + " AND ".join(parts) + ")")
                sql = f"{base} AND (" + " OR ".join(terms) + ")"
            cur.execute(sql, tuple(chunk_args))
            labels = [str(d[0]).lower() for d in cur.description or []]
            while True:
                page = cur.fetchmany(5_000)
                if not page:
                    break
                for row in page:
                    yield dict(zip(labels, row))
    finally:
        conn.close()
