"""Iceberg snapshot Parquet → PostgreSQL COPY FROM STDIN (cross-engine bulk).

The reverse of ``copy_pg_iceberg``. Source COUNT is Iceberg file footers
via ``destination_row_count`` / ``iceberg_mor`` — never ``scan().count()``.
Payload is current-snapshot data files read as Arrow (no
``scan().to_arrow()``). Each cell is encoded as PostgreSQL COPY text
into ``COPY … FROM STDIN``. Dest ``COUNT(*)`` must equal that footer
COUNT.

Empty dest COPYs the snapshot once. Occupied dest whose ``COUNT(*)``
already equals the source footer COUNT is skip-complete (COUNT only).
Occupied dest with a different COUNT declines — leftover MERGE / upsert
stays on the row path. Iceberg MoR (delete files) declines. Filesystem
CoW declines.

Declines (row path keeps quarantine): transforms that change values,
binary/uuid/timestamptz/list/map/struct, MoR snapshots, public proxy,
occupied dest with dest COUNT ≠ source.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable, _quote
from services.copy_mysql_pg import _pg_connect, _pg_create_sql, fast_copy_text_value
from services.copy_pg_iceberg import iceberg_copy_endpoint
from services.copy_pg_mysql import mapping_is_plain_carry

logger = logging.getLogger(__name__)

_READ_CHUNK = 1 << 20
_ARROW_BATCH = 8192

_UNSAFE_ICEBERG_TOKENS = (
    "binary",
    "uuid",
    "fixed",
    "list",
    "map",
    "struct",
    "timestamptz",
    "time",
    "timestamp_tz",
)


def iceberg_pg_copy_enabled() -> bool:
    raw = (getenv_brand("ICEBERG_PG_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def iceberg_type_is_copy_safe(declared: str) -> bool:
    raw = (declared or "").strip().lower()
    if not raw:
        return False
    if any(tok in raw for tok in _UNSAFE_ICEBERG_TOKENS):
        return False
    if "time zone" in raw and "without" not in raw:
        return False
    base = raw.split("(", 1)[0].strip().replace(" ", "")
    return base in {
        "string",
        "long",
        "int",
        "integer",
        "date",
        "boolean",
        "decimal",
        "float",
        "double",
        "timestamp",
        "timestamp_ntz",
        "varchar",
        "char",
        "character",
        "charactervarying",
        "text",
        "bigint",
        "smallint",
        "int2",
        "int4",
        "int8",
        "numeric",
        "real",
        "float4",
        "float8",
        "doubleprecision",
        "bool",
    }


def _pg_ident(name: str) -> str:
    return _quote(name)


class _ArrowCopyReader:
    """Single-thread file-like: encode Arrow batches as COPY text on read()."""

    def __init__(self, table: Any) -> None:
        self._batches = table.to_batches(max_chunksize=_ARROW_BATCH)
        self._idx = 0
        self._buf = b""
        self._done = False
        self._encode = fast_copy_text_value

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        want = _READ_CHUNK if size is None or size < 0 else max(int(size), 1)
        join = "\t".join
        encode = self._encode
        while not self._done and len(self._buf) < want:
            if self._idx >= len(self._batches):
                self._done = True
                break
            batch = self._batches[self._idx]
            self._idx += 1
            cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
            if not cols:
                continue
            rows = zip(*cols)
            payload = "\n".join(join(encode(v) for v in row) for row in rows)
            if payload:
                self._buf += (payload + "\n").encode("utf-8")
        out = self._buf[:want]
        self._buf = self._buf[want:]
        return out


def _iceberg_source_count(endpoint: dict[str, Any]) -> int:
    from services.dest_precount import destination_row_count

    n = destination_row_count(
        "iceberg",
        endpoint,
        schema=str(endpoint.get("schema") or "default"),
        table_name=str(endpoint.get("table") or endpoint.get("table_name") or ""),
    )
    if n is None:
        raise ValueError("Iceberg source COUNT unmeasured (snapshot/file footers)")
    return int(n)


def _arrow_from_iceberg_files(
    endpoint: dict[str, Any],
    source_cols: list[str],
) -> Any:
    """Current-snapshot data files as Arrow. Never ``scan().to_arrow()``."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config
    from pyiceberg.exceptions import NoSuchTableError
    from services.dest_precount import (
        _iceberg_catalog_snapshot,
        _iceberg_data_warehouse,
        _iceberg_local_path,
    )

    parsed = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = parsed["namespace"] + (parsed["table_name"],)
    try:
        tbl = catalog.load_table(identifier)
    except NoSuchTableError as exc:
        raise FastPathUnavailable(
            f"Iceberg source table {'.'.join(identifier)} does not exist"
        ) from exc

    live_names = [str(f.name) for f in tbl.schema().fields]
    live_l = {n.lower(): n for n in live_names}
    selected: list[str] = []
    for col in source_cols:
        name = live_l.get(col.lower())
        if not name:
            raise FastPathUnavailable(f"Iceberg source column {col!r} absent")
        field = next(f for f in tbl.schema().fields if str(f.name) == name)
        if not iceberg_type_is_copy_safe(str(field.type)):
            raise FastPathUnavailable(
                f"source column {col!r} type {field.type} is not Iceberg COPY-safe"
            )
        selected.append(name)

    snap = _iceberg_catalog_snapshot(endpoint)
    if snap is None:
        raise ValueError("Iceberg source snapshot unmeasured")
    uris, meta = snap
    if meta:
        raise FastPathUnavailable("Iceberg MoR source stays on the row path")

    warehouse = _iceberg_data_warehouse(endpoint, parsed)
    parts: list[Any] = []
    for uri in uris:
        local = _iceberg_local_path(str(uri), warehouse=warehouse)
        if local is None:
            raise FastPathUnavailable(
                f"Iceberg data file is not a local path: {uri}"
            )
        parts.append(pq.read_table(str(local), columns=selected))
    if not parts:
        empty = tbl.schema().as_arrow()
        arrays = [pa.array([], type=empty.field(name).type) for name in selected]
        return pa.Table.from_arrays(arrays, names=selected)
    table = pa.concat_tables(parts, promote_options="default")
    return table.select(selected)


def copy_iceberg_to_postgres(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
) -> FastPathResult:
    """COPY Iceberg snapshot files into PostgreSQL. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not iceberg_pg_copy_enabled():
        raise FastPathUnavailable("Iceberg→PostgreSQL COPY disabled")
    ok, reason = mapping_is_plain_carry(
        [{"source": s, "target": t, "transform": "none"} for s, t in pairs]
    )
    if not ok:
        raise FastPathUnavailable(reason)

    from connectors.iceberg_writer import resolve_iceberg_write_path
    from connectors.write_resilience import is_public_proxy_host
    from services.copy_fast_path import _table_ref as _pg_table_ref

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        source_cfg.get("connection_string") or source_cfg.get("host") or ""
    ):
        raise FastPathUnavailable("public proxy: Iceberg bulk copy not assumed")

    try:
        import pyarrow as pa  # noqa: F401
        import pyiceberg.catalog  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pyarrow/pyiceberg required for Iceberg COPY: {exc}") from exc

    endpoint = iceberg_copy_endpoint(source_cfg, source_table, source_schema)
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise FastPathUnavailable(str(exc)) from exc
    if write_path != "catalog":
        raise FastPathUnavailable("filesystem CoW stays on the row path")

    source_cols = [p[0] for p in pairs]
    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _pg_table_ref(dest_schema_n, dest_table)

    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    try:
        dst_cur = dest_conn.cursor()
        source_count = _iceberg_source_count(endpoint)

        dst_cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (dest_schema_n, dest_table),
        )
        exists = dst_cur.fetchone() is not None
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count:
                proof = f"dest_count:{dest_count_before}"
                return FastPathResult(
                    rows_copied=source_count,
                    source_rows=source_count,
                    source_checksum=proof,
                    target_rows=dest_count_before,
                    target_checksum=proof,
                    source_snapshot={
                        "copy_workers": 1,
                        "copy_split": "skip",
                        "copy_partitions": 1,
                        "partitions_skipped": 1,
                        "partitions_loaded": 0,
                        "shard_mode": "table",
                        "iceberg_read": "skip",
                    },
                    proof_scope="dest_count_equals_source_snapshot_count",
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied PostgreSQL dest stays on the row path "
                    "(Iceberg source has no PK-range skip on this path)"
                )
        else:
            dst_cur.execute(
                _pg_create_sql(dest_schema_n, dest_table, pairs, pg_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        pa_table = _arrow_from_iceberg_files(endpoint, source_cols)
        if len(pa_table) != source_count:
            raise ValueError(
                "Iceberg→PG COPY refused: Arrow rows "
                f"{len(pa_table)} != source footer COUNT {source_count}"
            )
        pa_table = pa_table.rename_columns(target_cols)
        col_list = ", ".join(_pg_ident(c) for c in target_cols)
        copy_sql = (
            f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
            "(FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        )
        dst_cur.copy_expert(copy_sql, _ArrowCopyReader(pa_table))
        dest_conn.commit()

        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            raise ValueError(
                "Iceberg→PG COPY refused: dest COUNT(*) "
                f"{dest_count} != source footer COUNT {source_count}"
            )
        proof = f"dest_count:{dest_count}"
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "copy_workers": 1,
                "copy_split": "serial",
                "copy_partitions": 1,
                "partitions_skipped": 0,
                "partitions_loaded": 1,
                "shard_mode": "table",
                "iceberg_read": "snapshot_parquet",
            },
            proof_scope="dest_count_equals_source_snapshot_count",
        )
    except Exception:
        if created_here:
            try:
                dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("PG dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("PostgreSQL dest close skipped", exc_info=True)
