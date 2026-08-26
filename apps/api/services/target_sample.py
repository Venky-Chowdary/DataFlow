"""Destination sample reads for Gate-8 value reconciliation.

One owner for "read a small, ordered, key-scoped sample back out of the
destination we just wrote" across every supported engine family. Split out of
:mod:`services.reconciliation`, which owns the counting/checksum side; the two
must agree on the object identity and the sample scope or Gate-8 compares a
written table against a different one.

A read failure raises :class:`services.reconciliation.TargetSampleUnavailable`
— never an empty list, which would read as "destination is empty" and green a
lost write.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from services.reconciliation import (
    TargetSampleUnavailable,
)

logger = logging.getLogger(__name__)

from services.keyed_read import execute_keyed_read  # noqa: E402


def numeric_sample_key_variants(key: Any) -> set[Any]:
    """Widen one Gate-8 sample key the way the writer may have stored it.

    Always includes the original. Write-path integer/decimal bind locale
    money (``$1,234`` → 1234). Dest-canonical ``1.234`` stays identity.
    Auto ``1,234`` stays the original token only. IEEE-lossy ``2**53+1``
    does not add ``float(2**53)``. Bool is not a magnitude.
    """
    from decimal import Decimal, InvalidOperation

    from connectors.sql_bind import coerce_integer_wire
    from services.transform_engine import decimal_wire_value, float_carrier_or_refuse

    if isinstance(key, bool):
        return {key}

    out: set[Any] = {key}
    try:
        parsed_int = coerce_integer_wire(key, ddl_type="INTEGER")
        if parsed_int is not None and not isinstance(parsed_int, bool):
            out.add(int(parsed_int))
    except (ValueError, TypeError, OverflowError):
        pass

    parsed_dec: Decimal | None = None
    if isinstance(key, Decimal):
        parsed_dec = key if key.is_finite() else None
    elif isinstance(key, int):
        parsed_dec = Decimal(key)
    elif isinstance(key, float):
        if key == key and key not in (float("inf"), float("-inf")):
            try:
                parsed_dec = Decimal(str(key))
            except (InvalidOperation, ValueError, OverflowError):
                parsed_dec = None
    else:
        text = str(key).strip()
        if text:
            try:
                dest = Decimal(text)
                if dest.is_finite():
                    parsed_dec = dest
            except (InvalidOperation, ValueError, ArithmeticError):
                parsed_dec = None
            if parsed_dec is None:
                parsed_dec = decimal_wire_value(text)
    if parsed_dec is not None and parsed_dec.is_finite():
        out.add(parsed_dec)
        try:
            out.add(float_carrier_or_refuse(parsed_dec))
        except ValueError:
            pass
    return out


def mongo_query_key_variants(key: Any) -> set[Any]:
    """Mongo ``$in`` variants: write-path numbers plus ObjectId hex.

    LSN fetch and Gate-8 dest-sample share this. ``isdigit()`` missed
    locale money and did not refuse Auto grouping.
    """
    out = set(numeric_sample_key_variants(key))
    try:
        from bson import ObjectId

        if (
            isinstance(key, str)
            and len(key) == 24
            and all(c in "0123456789abcdefABCDEF" for c in key)
        ):
            out.add(ObjectId(key))
    except Exception:
        pass
    return out


from services.target_sample_vector import (  # noqa: E402
    read_pgvector_target_sample,
)


def read_target_sample(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int = 50,
    sort_key: str | None = None,
    key_values: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Read a small ordered sample from destination for value reconciliation.

    When ``key_values`` is provided with ``sort_key``, prefer a keyed ``IN (...)``
    read so append/upsert Gate-8 can prove fidelity against pre-existing rows
    (ORDER BY … LIMIT alone often misses the batch keys in a large table).

    Returns an empty list only when the destination is genuinely empty (or the
    keyed ``IN`` matched nothing). Read failures raise
    :class:`TargetSampleUnavailable` — never ``[]``.
    """
    from connectors.sql_identifiers import (
        quote_column_list,
        quote_sql_identifier,
        quote_table_ref,
        require_safe_identifier,
    )

    cols = columns or ["*"]
    keys = [k for k in (key_values or []) if k is not None and k != ""][
        : max(1, int(limit or 50))
    ]

    def _row_names(description: Any) -> list[str]:
        # When explicit columns were requested, trust the caller's keys so
        # downstream mapping/reconciliation matches by the mapping target names.
        # Cursor.description names may differ in case (e.g. fakesnow lower-cases
        # quoted identifiers), which would make dict lookups fail for CURRENCY.
        if cols and cols != ["*"]:
            return list(cols)
        return [d[0] for d in (description or [])]

    def _ssl_flag(default: bool = False) -> bool:
        # Match list/probe defaults — ssl=True here previously emptied samples on
        # local / non-TLS hosts while verify_target still counted rows.
        return bool(dest.get("ssl", default))

    try:
        # pgvector is deliberately absent: it is PostgreSQL underneath, but its
        # table is a fixed vector schema (id / content / embedding / metadata),
        # so the mapped source columns are payload inside JSONB rather than
        # columns to select. Reading it here would ask for columns that do not
        # exist; a vector sink's honest assurance is writer_ack, not a per-cell
        # compare it cannot support.
        if db_type in ("postgresql", "redshift"):
            # Redshift speaks the Postgres wire protocol; local CI and many
            # managed endpoints use the PG driver. Checksum verify already
            # treated them as one family — sample compare must too, or Gate-8
            # fails closed with "no sample reader" after a successful write.
            from connectors.postgresql_conn import get_connection

            col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            table_ref = quote_table_ref(
                table_name, schema or "public", dialect="postgresql"
            )
            order_sql = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            ssl_flag = _ssl_flag(False)
            last_exc: Exception | None = None
            for attempt_ssl in (ssl_flag, not ssl_flag):
                try:
                    conn = get_connection(
                        host=dest.get("host", ""),
                        port=dest.get("port", 5432),
                        database=dest.get("database", ""),
                        username=dest.get("username", ""),
                        password=dest.get("password", ""),
                        connection_string=dest.get("connection_string", ""),
                        ssl=attempt_ssl,
                    )
                    with conn.cursor() as cur:
                        if keys and sort_key:
                            key_col = quote_sql_identifier(
                                require_safe_identifier(sort_key, preserve_case=True)
                            )
                            placeholders = ",".join(["%s"] * len(keys))
                            execute_keyed_read(
                                conn,
                                cur,
                                f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                                f"WHERE {{key}} IN ({placeholders}) "
                                f"ORDER BY {order_sql} LIMIT %s",
                                key_col,
                                keys,
                                (int(limit),),
                            )
                        else:
                            cur.execute(
                                f"SELECT {col_sql} FROM {table_ref} ORDER BY {order_sql} LIMIT %s",  # nosec B608
                                (limit,),
                            )
                        names = _row_names(cur.description)
                        rows = cur.fetchall()
                    conn.close()
                    return [dict(zip(names, row)) for row in rows]
                except Exception as exc:
                    last_exc = exc
                    continue
            if last_exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {last_exc}"
                ) from last_exc
            raise TargetSampleUnavailable(
                f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                "postgresql connection failed for both SSL modes"
            )

        if db_type == "mysql":
            from connectors.mysql_conn import get_connection

            mysql_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols],
                    quote_char="`",
                )
            )
            table_ref = quote_table_ref(table_name, dialect="mysql")
            mysql_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True), "`"
                )
                if sort_key
                else "1"
            )
            conn = get_connection(
                host=dest.get("host", ""),
                port=int(dest.get("port", 3306)),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                ssl=dest.get("ssl", False),
            )
            with conn.cursor() as cur:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True), "`"
                    )
                    placeholders = ",".join(["%s"] * len(keys))
                    cur.execute(
                        f"SELECT {mysql_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {mysql_order} LIMIT %s",
                        (*keys, int(limit)),
                    )
                else:
                    cur.execute(
                        f"SELECT {mysql_col_sql} FROM {table_ref} ORDER BY {mysql_order} LIMIT %s",  # nosec B608
                        (limit,),
                    )
                names = _row_names(cur.description)
                rows = cur.fetchall()
            conn.close()
            return [dict(zip(names, row)) for row in rows]

        if db_type in {"sqlserver", "mssql", "azure_sql"}:
            import pymssql

            lim = max(1, int(limit or 50))
            if cols == ["*"]:
                ss_col_sql = "*"
            else:
                ss_col_sql = ", ".join(
                    f"[{require_safe_identifier(c, preserve_case=True).replace(']', ']]')}]"
                    for c in cols
                )
            sch = (schema or dest.get("schema") or "dbo").strip() or "dbo"
            table_ref = quote_table_ref(table_name, schema=sch, dialect="sqlserver")
            ss_order = (
                f"[{require_safe_identifier(sort_key, preserve_case=True).replace(']', ']]')}]"
                if sort_key
                else "1"
            )
            conn = pymssql.connect(
                server=dest.get("host") or "127.0.0.1",
                port=int(dest.get("port") or 1433),
                user=dest.get("username") or "sa",
                password=dest.get("password") or "",
                database=dest.get("database") or "master",
                login_timeout=10,
                timeout=30,
            )
            cur = conn.cursor()
            try:
                if keys and sort_key:
                    key_col = (
                        f"[{require_safe_identifier(sort_key, preserve_case=True).replace(']', ']]')}]"
                    )
                    placeholders = ",".join(["%s"] * len(keys))
                    cur.execute(
                        f"SELECT TOP ({lim}) {ss_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {ss_order}",
                        tuple(keys),
                    )
                else:
                    cur.execute(
                        f"SELECT TOP ({lim}) {ss_col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {ss_order}"
                    )
                names = _row_names(cur.description)
                rows = cur.fetchall()
            finally:
                cur.close()
                conn.close()
            return [dict(zip(names, row)) for row in rows]

        if db_type in {
            "oracle",
            "oracledb",
            "oracle_db",
            "oracle_autonomous",
            "oracle_autonomous_warehouse",
            "amazon_rds_oracle",
        } or (
            db_type == "generic_sql"
            and (dest.get("connection_string") or "").lower().startswith("oracle")
        ):
            from services.reconciliation_oracle import read_oracle_target_sample

            return read_oracle_target_sample(
                dest,
                schema=schema,
                table_name=table_name,
                cols=list(cols),
                keys=list(keys),
                sort_key=sort_key,
                limit=limit,
            )

        if db_type == "duckdb" or (
            db_type == "generic_sql"
            and (
                "duckdb"
                in (dest.get("connection_string") or dest.get("database") or "").lower()
                or (dest.get("connection_string") or dest.get("database") or "")
                .lower()
                .endswith((".duckdb", ".duck"))
            )
        ):
            import sqlalchemy as sa
            from connectors.generic_sql import get_sqlalchemy_engine

            path = dest.get("connection_string") or dest.get("database", "")
            if not path:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "duckdb path missing (connection_string/database)"
                )
            duckdb_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            table_ref = quote_table_ref(table_name, dialect="duckdb")
            duckdb_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            try:
                engine = get_sqlalchemy_engine(
                    {"type": "duckdb", "connection_string": path}
                )
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    params: dict[str, Any] = {f"k{i}": k for i, k in enumerate(keys)}
                    params["lim"] = int(limit)
                    placeholders = ",".join(f":k{i}" for i in range(len(keys)))
                    sql = (
                        f"SELECT {duckdb_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {duckdb_order} LIMIT :lim"
                    )
                else:
                    params = {"lim": int(limit)}
                    sql = f"SELECT {duckdb_col_sql} FROM {table_ref} ORDER BY {duckdb_order} LIMIT :lim"  # nosec B608
                try:
                    result = conn.execute(sa.text(sql), params)
                    rows = result.mappings().all()
                    # DuckDB returns column labels using the sanitized (underscore)
                    # form of names like "fields.Name".  Re-label them with the
                    # requested target column names so reconciliation keys match
                    # the engine's mapping targets.
                    if cols and cols != ["*"]:
                        return [
                            {cols[i]: list(row.values())[i] for i in range(len(cols))}
                            for row in rows
                        ]
                    return [dict(row) for row in rows]
                except TargetSampleUnavailable:
                    raise
                except Exception as exc:
                    raise TargetSampleUnavailable(
                        f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                    ) from exc

        if db_type == "mongodb":
            from connectors.mongodb_common import (
                _mongo_client,
                normalize_mongodb_connection_string,
            )

            try:
                conn_str = normalize_mongodb_connection_string(
                    dest.get("connection_string", ""),
                    database=dest.get("database", ""),
                    host=dest.get("host", ""),
                    port=int(dest.get("port") or 27017),
                    username=dest.get("username", ""),
                    password=dest.get("password", ""),
                    ssl=bool(dest.get("ssl", False)),
                    auth_source=dest.get("auth_source", ""),
                )
                client = _mongo_client(conn_str)
                db = client[dest.get("database") or "test"]
                coll = db[table_name]
                query_filter: dict[str, Any] = {}
                if keys and sort_key:
                    # Mongo $in is type-sensitive; widened key set matches strings,
                    # integers, and decimals that the writer may have produced.
                    widened: set[Any] = set()
                    for k in keys:
                        widened.update(mongo_query_key_variants(k))
                    query_filter = {sort_key: {"$in": list(widened)}}
                cursor = coll.find(query_filter)
                if sort_key:
                    cursor = cursor.sort(sort_key, 1)
                return list(cursor.limit(int(limit)))
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type == "dynamodb":
            from services.target_sample_dynamodb import read_dynamodb_target_sample

            return read_dynamodb_target_sample(
                dest,
                table_name=table_name,
                keys=list(keys),
                sort_key=sort_key,
                limit=limit,
            )

        if db_type == "sqlite" or (
            db_type == "generic_sql"
            and (
                "sqlite"
                in (dest.get("connection_string") or dest.get("database") or "").lower()
                or (dest.get("connection_string") or dest.get("database") or "")
                .lower()
                .endswith((".db", ".sqlite"))
            )
        ):
            import sqlite3

            from connectors.sqlite_common import sqlite_file_path

            path = sqlite_file_path(
                dest.get("database") or "",
                dest.get("connection_string") or "",
                dest.get("host") or "",
            )
            if not path:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "sqlite path missing"
                )
            sqlite_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            table_ref = quote_table_ref(table_name, dialect="sqlite")
            sqlite_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            conn = sqlite3.connect(str(path))
            try:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    placeholders = ",".join(["?"] * len(keys))
                    sql = f"SELECT {sqlite_col_sql} FROM {table_ref} WHERE {key_col} IN ({placeholders}) ORDER BY {sqlite_order} LIMIT ?"  # nosec B608
                    cur = conn.execute(sql, [*keys, int(limit)])
                else:
                    sql = f"SELECT {sqlite_col_sql} FROM {table_ref} ORDER BY {sqlite_order} LIMIT ?"  # nosec B608
                    cur = conn.execute(sql, (int(limit),))
                rows = cur.fetchall()
                names = _row_names(cur.description)
                return [dict(zip(names, row)) for row in rows]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc
            finally:
                conn.close()

        if db_type == "redis":
            from connectors.redis_reader import _decode, _redis_client, redis_json_row
            from connectors.sql_identifiers import sanitize_identifier

            prefix = table_name or "dataflow"
            cfg = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 6379),
                "database": dest.get("database", "0"),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "connection_string": dest.get("connection_string", ""),
                "ssl": bool(dest.get("ssl", False)),
            }
            try:
                client = _redis_client(cfg)
                rows_out: list[dict[str, Any]] = []
                if keys and sort_key:
                    # Writer stores keys as ``prefix:<sanitized_id>``.
                    key_names = [
                        f"{prefix}:{sanitize_identifier(str(k), preserve_case=True)}"
                        for k in keys
                    ]
                    for raw in client.mget(key_names):
                        text = _decode(raw)
                        if not text:
                            continue
                        rows_out.append(redis_json_row(text))
                        if len(rows_out) >= limit:
                            break
                else:
                    pattern = f"{prefix}:*" if prefix else "*"
                    cursor = 0
                    while True:
                        cursor, batch = client.scan(
                            cursor=cursor, match=pattern, count=500
                        )
                        for raw_key in batch:
                            key = (
                                raw_key.decode()
                                if isinstance(raw_key, bytes)
                                else str(raw_key)
                            )
                            raw = client.get(key)
                            text = _decode(raw)
                            rows_out.append(redis_json_row(text))
                            if len(rows_out) >= limit:
                                break
                        if cursor == 0 or len(rows_out) >= limit:
                            break
                if columns:
                    rows_out = [
                        {k: v for k, v in row.items() if k in columns}
                        for row in rows_out
                    ]
                return rows_out[:limit]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type in {"elasticsearch", "opensearch", "elastic"}:
            from connectors.elasticsearch_reader import read_index_batch

            cfg = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 9200),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "connection_string": dest.get("connection_string", ""),
                "ssl": bool(dest.get("ssl", False)),
                "api_key": str(dest.get("api_key") or dest.get("service_account") or ""),
            }
            try:
                batch, _ = read_index_batch(
                    cfg=cfg,
                    index=table_name,
                    columns=None if cols == ["*"] else cols,
                    limit=max(1, int(limit or 50)),
                )
                headers = list(batch.headers or [])
                rows_out: list[dict[str, Any]] = []
                for row in batch.rows or []:
                    if isinstance(row, dict):
                        payload = row
                    elif headers:
                        payload = {
                            headers[i]: row[i] if i < len(row) else None
                            for i in range(len(headers))
                        }
                    else:
                        continue
                    if keys and sort_key:
                        sk = str(payload.get(sort_key, ""))
                        if sk not in {str(k) for k in keys}:
                            continue
                    rows_out.append(payload)
                    if len(rows_out) >= limit:
                        break
                if columns and columns != ["*"]:
                    rows_out = [
                        {k: v for k, v in row.items() if k in columns}
                        for row in rows_out
                    ]
                return rows_out[:limit]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type == "snowflake":
            from connectors.snowflake_conn import (
                get_connection,
                normalize_account,
                resolve_or_fold_snowflake_table,
                snowflake_qualified_table,
            )

            try:
                conn = get_connection(
                    account=normalize_account(dest.get("host", "")),
                    username=dest.get("username", ""),
                    password=dest.get("password", ""),
                    database=dest.get("database", ""),
                    schema=schema or "PUBLIC",
                    warehouse=dest.get("warehouse", ""),
                    connection_string=dest.get("connection_string", ""),
                )
                from connectors.sql_identifiers import (
                    quote_sql_identifier,
                    require_safe_identifier,
                )

                with conn.cursor() as cur:
                    resolved = resolve_or_fold_snowflake_table(
                        cur, schema or "PUBLIC", table_name
                    )
                    qualified_name = snowflake_qualified_table(
                        schema or "PUBLIC", resolved
                    )
                    sf_col_sql = (
                        "*"
                        if cols == ["*"]
                        else quote_column_list(
                            [
                                require_safe_identifier(c, preserve_case=True)
                                for c in cols
                            ]
                        )
                    )
                    sf_order = (
                        quote_sql_identifier(
                            require_safe_identifier(sort_key, preserve_case=True)
                        )
                        if sort_key
                        else "1"
                    )
                    if keys and sort_key:
                        key_col = quote_sql_identifier(
                            require_safe_identifier(sort_key, preserve_case=True)
                        )
                        # Snowflake IN is type-sensitive; widen via write-path variants.
                        widened: set[Any] = set()
                        for k in keys:
                            widened.update(numeric_sample_key_variants(k))
                        placeholders = ",".join(["%s"] * len(widened))
                        cur.execute(
                            f"SELECT {sf_col_sql} FROM {qualified_name} "  # nosec B608
                            f"WHERE {key_col} IN ({placeholders}) "
                            f"ORDER BY {sf_order} LIMIT %s",
                            (*widened, int(limit)),
                        )
                    else:
                        cur.execute(
                            f"SELECT {sf_col_sql} FROM {qualified_name} ORDER BY {sf_order} LIMIT %s",  # nosec B608
                            (int(limit),),
                        )
                    names = _row_names(cur.description)
                    rows = cur.fetchall()
                conn.close()
                return [dict(zip(names, row)) for row in rows]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type == "bigquery":
            from connectors.bigquery_conn import get_client, _is_local_endpoint

            project_id = dest.get("database", "")
            dataset_id = schema or "dataflow"
            is_local, _ = _is_local_endpoint(
                dest.get("host", ""), dest.get("connection_string", "")
            )
            try:
                client = get_client(
                    project_id=project_id,
                    credentials_path=dest.get("connection_string", ""),
                    service_account=dest.get("service_account", ""),
                    host=dest.get("host", ""),
                    port=int(dest.get("port") or 0),
                    connection_string=dest.get("connection_string", ""),
                )
                table_id = f"{project_id}.{dataset_id}.{table_name}"
                if is_local:
                    # Emulator path: scan rows and filter in-process; avoids
                    # query().result() hangs on the goccy emulator for some jobs.
                    out: list[dict[str, Any]] = []
                    scan_limit = (limit or 50) * 10 if (keys and sort_key) else (limit or 50)
                    widened = set()
                    if keys and sort_key:
                        for k in keys:
                            widened.update(numeric_sample_key_variants(k))
                    for row in client.list_rows(table_id, max_results=scan_limit):
                        d = dict(row.items()) if hasattr(row, "items") else {k: v for k, v in zip(cols, row)}
                        if cols and cols != ["*"]:
                            d = {k: v for k, v in d.items() if k in cols}
                        if keys and sort_key:
                            if d.get(sort_key) in widened:
                                out.append(d)
                        else:
                            out.append(d)
                        if len(out) >= (limit or 50):
                            break
                    return out
                # Production: use a real BigQuery query with a bounded timeout.
                col_sql = (
                    "*"
                    if cols == ["*"]
                    else quote_column_list(
                        [require_safe_identifier(c, preserve_case=True) for c in cols]
                    )
                )
                bq_order = (
                    quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    if sort_key
                    else "1"
                )
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    placeholders = ",".join(["%s"] * len(keys))
                    sql = (
                        f"SELECT {col_sql} FROM `{table_id}` "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {bq_order} LIMIT %s"
                    )
                    params = (*keys, int(limit))
                else:
                    sql = f"SELECT {col_sql} FROM `{table_id}` ORDER BY {bq_order} LIMIT %s"  # nosec B608
                    params = (int(limit),)
                res = client.query(sql, timeout=60).result()
                names = list(res.schema) if res.schema else cols
                if names and names[0] and not isinstance(names[0], str):
                    names = [f.name for f in names]
                return [
                    {k: v for k, v in dict(row.items()).items() if k in (cols if cols != ["*"] else dict(row.items()).keys())}
                    for row in res
                ]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        from services.dest_precount import (
            UnmeasuredArtifact,
            _object_store_kind,
            sample_artifact_records,
            sample_object_store,
        )

        if _object_store_kind(db_type) in {"s3", "gcs", "adls"}:
            bucket = (dest.get("database") or schema or "").strip()
            if not bucket or not table_name:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "object-store bucket or key missing"
                )
            cfg = dict(dest)
            cfg["database"] = bucket
            try:
                return sample_object_store(
                    db_type,
                    cfg,
                    table_name=table_name,
                    limit=limit,
                    sort_key=sort_key or "",
                    keys=keys,
                    columns=None if cols == ["*"] else cols,
                )
            except UnmeasuredArtifact as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type in {
            "databricks",
            "databricks_sql",
            "delta",
            "delta_lake",
            "unity_catalog",
            "spark",
        }:
            import sqlalchemy as sa

            from connectors.generic_sql import get_sqlalchemy_engine

            lim = max(1, int(limit or 50))
            db_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            sch = (schema or dest.get("schema") or dest.get("database") or "").strip() or None
            table_ref = quote_table_ref(table_name, schema=sch, dialect="ansi")
            db_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            engine = get_sqlalchemy_engine(
                {
                    "type": "databricks",
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 443),
                    "database": dest.get("database", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "schema": schema or dest.get("schema") or "",
                    "http_path": str(dest.get("http_path") or dest.get("warehouse") or ""),
                }
            )
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    params = {f"k{i}": k for i, k in enumerate(keys)}
                    params["lim"] = lim
                    placeholders = ",".join(f":k{i}" for i in range(len(keys)))
                    sql = (
                        f"SELECT {db_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {db_order} LIMIT :lim"
                    )
                else:
                    params = {"lim": lim}
                    sql = (
                        f"SELECT {db_col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {db_order} LIMIT :lim"
                    )
                result = conn.execute(sa.text(sql), params)
                names = list(cols) if cols and cols != ["*"] else list(result.keys())
                return [dict(zip(names, tuple(row))) for row in result.fetchall()]

        if db_type == "hubspot":
            from connectors.hubspot import read_object

            batch = read_object(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "table": table_name,
                    "database": table_name,
                },
                object=table_name or "contacts",
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or (cols if cols != ["*"] else []) or [])
            out_rows: list[dict[str, Any]] = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = dict(row)
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "salesforce":
            from connectors.salesforce import read_object

            batch = read_object(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "api_key": dest.get("api_key", ""),
                    "table": table_name,
                    "database": table_name,
                },
                object=table_name or "Account",
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or [])
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = {k: v for k, v in row.items() if k != "attributes"}
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "airtable":
            from connectors.airtable import read_object
            from connectors.saas_typed_schema import flatten_airtable_record

            batch = read_object(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "api_key": dest.get("api_key", ""),
                    "database": dest.get("database") or schema or "",
                    "table": table_name,
                    "type": "airtable",
                },
                object=table_name,
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d, _ = flatten_airtable_record(row)
                elif batch.headers:
                    headers = list(batch.headers)
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type in {"stripe", "shopify", "zendesk", "notion"}:
            if db_type == "stripe":
                from connectors.stripe import read_object as _saas_read
            elif db_type == "shopify":
                from connectors.shopify import read_object as _saas_read
            elif db_type == "zendesk":
                from connectors.zendesk import read_object as _saas_read
            else:
                from connectors.notion import read_object as _saas_read

            batch = _saas_read(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "api_key": dest.get("api_key", ""),
                    "table": table_name,
                    "database": dest.get("database") or schema or table_name,
                    "shop": dest.get("host", ""),
                    "type": db_type,
                },
                object=table_name
                or (
                    "customers"
                    if db_type in {"stripe", "shopify"}
                    else ("tickets" if db_type == "zendesk" else "")
                ),
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or [])
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = dict(row)
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "kafka":
            from connectors.kafka_reader import read_topic_batch

            batch, _ = read_topic_batch(
                cfg={
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 9092),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "database": table_name,
                    "table": table_name,
                    "group_id": f"dataflow-gate8-sample-{abs(hash(table_name)) % 10_000_000}",
                    "auto_offset_reset": "earliest",
                    "schema_registry_url": str(
                        dest.get("schema_registry_url") or dest.get("registry_url") or ""
                    ),
                },
                topic=table_name,
                columns=None if cols == ["*"] else cols,
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or [])
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = dict(row)
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "pinecone":
            from connectors.pinecone_writer import _headers, _index_url, _requests_session

            index_url = _index_url(dest.get("host", ""), dest.get("connection_string", ""))
            key = str(
                dest.get("api_key") or dest.get("password") or dest.get("username") or ""
            )
            if not index_url or not key:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "pinecone index URL or API key missing"
                )
            session = _requests_session()
            hdrs = _headers(key)
            ns = (table_name or dest.get("schema") or "").strip()
            ids = [str(k) for k in keys] if keys else []
            if not ids:
                return []
            params = [("ids", i) for i in ids[: max(1, int(limit or 50))]]
            if ns:
                params.append(("namespace", ns))
            fetch = session.get(
                f"{index_url}/vectors/fetch",
                params=params,
                headers=hdrs,
                timeout=30,
            )
            if fetch.status_code not in {200, 201}:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    f"pinecone fetch HTTP {fetch.status_code}"
                )
            vectors = (fetch.json() or {}).get("vectors") or {}
            out_rows = []
            for vid, payload in vectors.items():
                meta = payload.get("metadata") if isinstance(payload, dict) else {}
                if not isinstance(meta, dict):
                    meta = {}
                row = {"id": vid, **meta}
                if cols and cols != ["*"]:
                    row = {k: row.get(k) for k in cols}
                out_rows.append(row)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "qdrant":
            from connectors.qdrant_writer import _base_url, _headers, _requests_session

            api_key = dest.get("password") or dest.get("username") or ""
            base_url = (
                dest.get("connection_string", "").rstrip("/")
                if dest.get("connection_string")
                else _base_url(
                    dest.get("host", ""),
                    int(dest.get("port") or 6333),
                    bool(dest.get("ssl", False)),
                )
            )
            collection = table_name or dest.get("database") or "dataflow_vectors"
            session = _requests_session()
            hdrs = _headers(str(api_key))
            out_rows: list[dict[str, Any]] = []
            if keys:
                retrieve = session.post(
                    f"{base_url}/collections/{collection}/points",
                    data=json.dumps({
                        "ids": [str(k) for k in keys[: max(1, int(limit or 50))]],
                        "with_payload": True,
                        "with_vector": False,
                    }),
                    headers=hdrs,
                    timeout=30,
                )
                points = (
                    (retrieve.json() or {}).get("result") or []
                    if retrieve.status_code in {200, 201}
                    else []
                )
            else:
                scroll = session.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    data=json.dumps({
                        "limit": max(1, int(limit or 50)),
                        "with_payload": True,
                        "with_vector": False,
                    }),
                    headers=hdrs,
                    timeout=30,
                )
                points = (
                    ((scroll.json() or {}).get("result") or {}).get("points") or []
                    if scroll.status_code in {200, 201}
                    else []
                )
            for pt in points:
                if not isinstance(pt, dict):
                    continue
                payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                row = {"id": pt.get("id"), **payload}
                if cols and cols != ["*"]:
                    row = {k: row.get(k) for k in cols}
                out_rows.append(row)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "weaviate":
            from connectors.weaviate_writer import (
                _base_url,
                _class_name,
                _headers,
                _requests_session,
            )

            key = str(
                dest.get("api_key") or dest.get("password") or dest.get("username") or ""
            )
            base_url = _base_url(
                dest.get("host", ""),
                int(dest.get("port") or 8080),
                bool(dest.get("ssl", False)),
                dest.get("connection_string", ""),
            )
            cls = _class_name(table_name or dest.get("database") or "DataflowChunk")
            session = _requests_session()
            hdrs = _headers(key)
            out_rows = []
            if keys:
                for oid in keys[: max(1, int(limit or 50))]:
                    resp = session.get(
                        f"{base_url}/v1/objects/{cls}/{oid}",
                        headers=hdrs,
                        timeout=15,
                    )
                    if resp.status_code not in {200, 201}:
                        resp = session.get(
                            f"{base_url}/v1/objects/{oid}",
                            headers=hdrs,
                            timeout=15,
                        )
                    if resp.status_code not in {200, 201}:
                        continue
                    obj = resp.json() or {}
                    if not isinstance(obj, dict):
                        continue
                    props = (
                        obj.get("properties")
                        if isinstance(obj.get("properties"), dict)
                        else {}
                    )
                    row = {"id": obj.get("id") or oid, **props}
                    if cols and cols != ["*"]:
                        row = {k: row.get(k) for k in cols}
                    out_rows.append(row)
            else:
                agg = session.get(
                    f"{base_url}/v1/objects",
                    params={"class": cls, "limit": max(1, int(limit or 50))},
                    headers=hdrs,
                    timeout=30,
                )
                if agg.status_code in {200, 201}:
                    for obj in (agg.json() or {}).get("objects") or []:
                        if not isinstance(obj, dict):
                            continue
                        props = (
                            obj.get("properties")
                            if isinstance(obj.get("properties"), dict)
                            else {}
                        )
                        row = {"id": obj.get("id"), **props}
                        if cols and cols != ["*"]:
                            row = {k: row.get(k) for k in cols}
                        out_rows.append(row)
                        if len(out_rows) >= int(limit or 50):
                            break
            return out_rows

        if db_type == "milvus":
            from connectors.milvus_writer import (
                _auth_token,
                _base_url,
                _collection_name,
                _headers,
                _ok_response,
                _requests_session,
            )

            coll = _collection_name(table_name or "dataflow_chunks")
            db_name = (dest.get("database") or schema or "").strip()
            if db_name.lower() in {"", "test_db", "default", "public"}:
                db_name = ""
            token = _auth_token(
                api_key=str(dest.get("api_key") or ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
            )
            base_url = _base_url(
                dest.get("host", ""),
                int(dest.get("port") or 19530),
                bool(dest.get("ssl", False)),
                dest.get("connection_string", ""),
            )
            session = _requests_session()
            hdrs = _headers(token)
            query_payload: dict[str, Any] = {
                "collectionName": coll,
                "outputFields": ["id", "content", "source_id", "chunk_index"],
                "limit": max(1, int(limit or 50)),
            }
            if keys:
                quoted = ", ".join(json.dumps(str(k)) for k in keys[: max(1, int(limit or 50))])
                query_payload["filter"] = f"id in [{quoted}]"
            else:
                query_payload["filter"] = ""
            if db_name:
                query_payload["dbName"] = db_name
            query = session.post(
                f"{base_url}/v2/vectordb/entities/query",
                data=json.dumps(query_payload),
                headers=hdrs,
                timeout=30,
            )
            qbody = query.json() if query.content else {}
            out_rows = []
            if _ok_response(qbody if isinstance(qbody, dict) else {}, query.status_code):
                for row in (qbody.get("data") if isinstance(qbody, dict) else []) or []:
                    if not isinstance(row, dict):
                        continue
                    d = {k: v for k, v in row.items() if k != "vector"}
                    if cols and cols != ["*"]:
                        d = {k: d.get(k) for k in cols}
                    out_rows.append(d)
                    if len(out_rows) >= int(limit or 50):
                        break
            return out_rows

        _engine_hint = str(dest.get("type") or dest.get("engine") or "").lower()
        _conn_hint = str(dest.get("connection_string") or dest.get("database") or "").lower()
        if db_type == "clickhouse" or (
            db_type == "generic_sql"
            and ("clickhouse" in _engine_hint or "clickhouse" in _conn_hint)
        ):
            import sqlalchemy as sa

            from connectors.generic_sql import (
                clickhouse_final_table_sql,
                get_sqlalchemy_engine,
            )
            from connectors.sql_identifiers import (
                quote_sql_identifier,
                quote_table_ref,
                require_safe_identifier,
            )

            engine = get_sqlalchemy_engine(
                {
                    "type": "clickhouse",
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 9000),
                    "database": dest.get("database", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "schema": schema or dest.get("schema") or "",
                    "ssl": bool(dest.get("ssl", False)),
                }
            )
            table_ref = quote_table_ref(
                table_name,
                schema=schema or dest.get("schema") or None,
                dialect="clickhouse",
            )
            from_sql = clickhouse_final_table_sql(table_ref)
            col_sql = (
                "*"
                if cols == ["*"]
                else ", ".join(
                    quote_sql_identifier(
                        require_safe_identifier(c, preserve_case=True), "`"
                    )
                    for c in cols
                )
            )
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True), "`"
                    )
                    placeholders = ", ".join(f":k{i}" for i in range(len(keys)))
                    params = {f"k{i}": k for i, k in enumerate(keys)}
                    result = conn.execute(
                        sa.text(
                            f"SELECT {col_sql} FROM {from_sql} "  # nosec B608
                            f"WHERE {key_col} IN ({placeholders}) "
                            f"LIMIT {int(limit or 50)}"
                        ),
                        params,
                    )
                else:
                    result = conn.execute(
                        sa.text(
                            f"SELECT {col_sql} FROM {from_sql} "  # nosec B608
                            f"LIMIT {int(limit or 50)}"
                        )
                    )
                names = list(result.keys()) if result.keys() else (
                    list(cols) if cols != ["*"] else []
                )
                return [dict(zip(names, row)) for row in result.fetchall()]

        if db_type in {"iceberg", "apache_iceberg"}:
            from services.dest_precount import iceberg_target_sample

            sampled = iceberg_target_sample(
                dest,
                schema=schema,
                table_name=table_name,
                columns=None if cols == ["*"] else list(cols),
                limit=int(limit or 50),
                sort_key=sort_key,
                key_values=keys or None,
            )
            if sampled is None:
                raise TargetSampleUnavailable(
                    f"Could not read Iceberg snapshot sample from {table_name!r}"
                )
            return sampled

        if db_type == "pgvector":
            return read_pgvector_target_sample(
                dest,
                schema=schema,
                table_name=table_name,
                cols=cols,
                keys=keys,
                sort_key=sort_key,
                limit=limit,
            )

        if db_type == "sftp":
            from services.object_streaming import open_sftp_binary

            cfg = dict(dest)
            if table_name:
                cfg["table"] = table_name
            if schema and not str(cfg.get("database") or "").strip():
                cfg["database"] = schema
            opened = open_sftp_binary(cfg)
            if opened is False:
                return []
            if opened is None:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "sftp host, path, or GET unavailable"
                )
            stream, closer = opened
            try:
                return sample_artifact_records(
                    stream,
                    name=str(table_name or cfg.get("path") or ""),
                    limit=limit,
                    sort_key=sort_key or "",
                    keys=keys,
                    columns=None if cols == ["*"] else cols,
                )
            except UnmeasuredArtifact as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc
            finally:
                if closer is not None:
                    try:
                        closer()
                    except Exception:
                        pass

    except TargetSampleUnavailable:
        raise
    except Exception as exc:
        raise TargetSampleUnavailable(
            f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
        ) from exc
    raise TargetSampleUnavailable(
        f"No sample reader is wired for destination type {db_type!r} "
        f"(table {table_name!r}); refusing to treat as empty"
    )
