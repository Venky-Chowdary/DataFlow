"""PostgreSQL COPY CSV → Iceberg catalog snapshot (cross-engine bulk).

SQL cartesian among PostgreSQL / MySQL / SQL Server / Oracle is closed.
This path is the lakehouse identity bulk: one ``COPY (SELECT …) TO STDOUT``
CSV, one Arrow table, one Iceberg snapshot commit. Python never builds
dict rows. Dest COUNT is Parquet/ORC file footers via
``destination_row_count`` / ``iceberg_mor`` — never ``scan().count()``.

Empty dest is CoW snapshot append (or snapshot replace on overwrite).
That is **not** ``MERGE INTO``. Occupied dest whose footer COUNT already
equals the source snapshot is skip-complete (COUNT only). Occupied dest
with a different COUNT declines — leftover MERGE / upsert stays on the
row path. Filesystem CoW stays on the row path. Iceberg catalog commits
are snapshot-isolated, so this COPY is serial (no parallel writers).

Declines (row path keeps quarantine): transforms that change values,
jsonb/bytea/timestamptz/arrays, filesystem CoW, Glue/Nessie when the
catalog is not catalog-write, occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
    _quote,
    _table_ref as _pg_table_ref,
    source_column_types,
    source_table_shape,
)
from services.copy_pg_mysql import (
    _pg_connect,
    mapping_is_plain_carry,
    pg_type_is_load_safe,
)

logger = logging.getLogger(__name__)


def pg_iceberg_copy_enabled() -> bool:
    raw = (getenv_brand("PG_ICEBERG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def iceberg_copy_endpoint(
    dest_cfg: dict[str, Any],
    dest_table: str,
    dest_schema: str | None = None,
) -> dict[str, Any]:
    """Normalize a stream dest_cfg so catalog parse + dest COUNT share one shape."""
    extra = dict(dest_cfg.get("extra") or {})
    catalog_type = extra.get("catalog_type") or dest_cfg.get("catalog_type")
    warehouse = (
        dest_cfg.get("warehouse")
        or extra.get("warehouse")
        or dest_cfg.get("database")
        or ""
    )
    if catalog_type:
        extra.setdefault("catalog_type", catalog_type)
    if warehouse:
        extra.setdefault("warehouse", warehouse)
    schema = (
        dest_schema
        or dest_cfg.get("schema")
        or extra.get("namespace")
        or "default"
    )
    table = (dest_table or dest_cfg.get("table") or dest_cfg.get("table_name") or "").strip()
    return {
        **dest_cfg,
        "type": dest_cfg.get("type") or dest_cfg.get("format") or "iceberg",
        "table": table,
        "table_name": table,
        "schema": schema,
        "warehouse": warehouse,
        "connection_string": dest_cfg.get("connection_string") or "",
        "extra": extra,
    }


def _arrow_schema_for_iceberg(target_cols: list[str], iceberg_ddls: list[str]) -> Any:
    import pyarrow as pa
    from services.arrow_write import logical_to_arrow_type

    fields = []
    for name, ddl in zip(target_cols, iceberg_ddls):
        fields.append((name, logical_to_arrow_type(ddl or "string", pa, dialect="iceberg")))
    return pa.schema(fields)


def _copy_csv_sql(select_list: str, source_ref: str) -> str:
    return (
        f"COPY (SELECT {select_list} FROM {source_ref}) "  # nosec B608
        "TO STDOUT WITH (FORMAT csv, HEADER false, NULL '\\N')"
    )


def _arrow_from_csv(path: str, schema: Any) -> Any:
    import pyarrow as pa
    import pyarrow.csv as pacsv

    if os.path.getsize(path) == 0:
        return pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema],
            schema=schema,
        )
    read_opts = pacsv.ReadOptions(column_names=list(schema.names), encoding="utf8")
    parse_opts = pacsv.ParseOptions(delimiter=",", quote_char='"', double_quote=True)
    convert_opts = pacsv.ConvertOptions(
        column_types={name: schema.field(name).type for name in schema.names},
        null_values=["\\N"],
        strings_can_be_null=True,
        quoted_strings_can_be_null=False,
        true_values=["t", "true", "1"],
        false_values=["f", "false", "0"],
    )
    table = pacsv.read_csv(
        path,
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts,
    )
    return table.cast(schema)


def _iceberg_dest_count(endpoint: dict[str, Any]) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "iceberg",
        endpoint,
        schema=str(endpoint.get("schema") or "default"),
        table_name=str(endpoint.get("table") or endpoint.get("table_name") or ""),
    )
    if n is None:
        raise ValueError("Iceberg dest COUNT unmeasured (snapshot/file footers)")
    return int(n)


def _load_or_create_table(endpoint: dict[str, Any], arrow_schema: Any, *, create: bool) -> Any:
    from connectors.iceberg_catalog import ensure_namespace, load_catalog, parse_iceberg_catalog_config
    from pyiceberg.exceptions import NoSuchTableError

    parsed = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = parsed["namespace"] + (parsed["table_name"],)
    try:
        return catalog.load_table(identifier), True
    except NoSuchTableError:
        if not create:
            raise FastPathUnavailable(
                f"Iceberg table {'.'.join(identifier)} does not exist"
            ) from None
        ensure_namespace(catalog, parsed["namespace"])
        return catalog.create_table(identifier, schema=arrow_schema), False


def copy_postgres_to_iceberg(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    iceberg_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """COPY PG CSV into one Iceberg snapshot. Dest COUNT is the proof."""
    if not pairs or len(pairs) != len(iceberg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not pg_iceberg_copy_enabled():
        raise FastPathUnavailable("PostgreSQL→Iceberg COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(source_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or dest_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    endpoint = iceberg_copy_endpoint(dest_cfg, dest_table, dest_schema)
    from connectors.iceberg_writer import resolve_iceberg_write_path

    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    src_schema = source_schema or source_cfg.get("schema") or "public"
    source_ref = _pg_table_ref(src_schema, source_table)
    arrow_schema = _arrow_schema_for_iceberg(target_cols, iceberg_ddls)

    source_conn = _pg_connect(source_cfg)
    created_here = False
    tmp_path = ""
    try:
        source_conn.autocommit = False
        src_cur = source_conn.cursor()
        src_cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        live = source_column_types(src_cur, src_schema, source_table, source_cols)
        live_l = {k.lower(): v for k, v in live.items()}
        for col in source_cols:
            declared = live_l.get(col.lower()) or ""
            if not declared:
                raise FastPathUnavailable(f"source column {col!r} absent")
            if not pg_type_is_load_safe(declared):
                raise FastPathUnavailable(
                    f"source column {col!r} type {declared} is not Iceberg COPY-safe"
                )
        shape = source_table_shape(src_cur, src_schema, source_table, source_cols)
        src_cur.execute(f"SELECT COUNT(*) FROM {source_ref}")  # nosec B608
        source_count = int(src_cur.fetchone()[0])
        src_cur.execute("SELECT pg_export_snapshot()")
        snapshot_id = str(src_cur.fetchone()[0])

        tbl, existed = _load_or_create_table(
            endpoint, arrow_schema, create=True
        )
        created_here = not existed
        dest_count_before = 0 if created_here else _iceberg_dest_count(endpoint)
        dest_occupied = dest_count_before > 0

        if existed:
            live_names = [str(n) for n in tbl.schema().as_arrow().names]
            if {n.lower() for n in live_names} != {c.lower() for c in target_cols}:
                raise FastPathUnavailable(
                    "Iceberg dest columns do not match mapped COPY columns"
                )

        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                proof = f"dest_count:{dest_count_before}"
                return FastPathResult(
                    rows_copied=source_count,
                    source_rows=source_count,
                    source_checksum=proof,
                    target_rows=dest_count_before,
                    target_checksum=proof,
                    source_snapshot={
                        "pg_snapshot": snapshot_id,
                        "copy_workers": 1,
                        "copy_split": "skip",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "iceberg_write": "skip",
                        "source_pk": list(shape.primary_key or []),
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            raise FastPathUnavailable(
                "append into occupied Iceberg dest stays on the row path "
                "(leftover MERGE / upsert); identity COPY would duplicate"
            )

        select_list = ", ".join(_quote(c) for c in source_cols)
        copy_sql = _copy_csv_sql(select_list, source_ref)
        handle, tmp_path = tempfile.mkstemp(prefix="df_pg_iceberg_", suffix=".csv")
        os.close(handle)
        with open(tmp_path, "wb") as writer:
            src_cur.copy_expert(copy_sql, writer)
        pa_table = _arrow_from_csv(tmp_path, arrow_schema)
        if len(pa_table) != source_count:
            raise ValueError(
                "PG→Iceberg COPY refused: Arrow rows "
                f"{len(pa_table)} != source snapshot {source_count}"
            )
        if existed:
            live_names = [str(n) for n in tbl.schema().as_arrow().names]
            pa_table = pa_table.select(live_names)

        iceberg_write = "overwrite" if replace_destination and existed else "append"
        if iceberg_write == "overwrite":
            tbl.overwrite(pa_table)
        else:
            tbl.append(pa_table)

        dest_count = _iceberg_dest_count(endpoint)
        if dest_count != source_count:
            raise ValueError(
                "PG→Iceberg COPY refused: dest COUNT "
                f"{dest_count} != source snapshot {source_count}"
            )
        try:
            source_conn.commit()
        except Exception:
            logger.debug("PostgreSQL source commit skipped", exc_info=True)
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "pg_snapshot": snapshot_id,
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "iceberg_write": iceberg_write,
                "source_pk": list(shape.primary_key or []),
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

                parsed = parse_iceberg_catalog_config(endpoint)
                catalog = load_catalog(endpoint)
                catalog.drop_table(parsed["namespace"] + (parsed["table_name"],))
            except Exception:
                logger.debug("Iceberg dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.debug("Iceberg COPY tempfile unlink skipped", exc_info=True)
        try:
            source_conn.close()
        except Exception:
            logger.debug("PostgreSQL source close skipped", exc_info=True)
