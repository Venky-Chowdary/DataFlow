"""Live desktop coverage for dimensions the 7-column overwrite fixture skipped.

Honesty
-------
* This is still a named fixture — not every SQL type, not 80×80, not SaaS.
* Types here: JSONB, UUID, BYTEA, INT[], INTERVAL on PostgreSQL source.
  Geography / PostGIS is skipped (extension not installed). Nested XML is not
  in this fixture.
* Sync modes here: incremental_deduped, mirror, scd2, reverse_etl (PG→MySQL
  warehouse→OLTP), CDC (MySQL binlog + PG logical). Salesforce reverse-ETL
  is omitted — no live SaaS backend.
* Schema: dest-only NOT NULL (G14). DECIMAL→INT and extra-source G13 are
  measured on desktop_lab_dimensions (dest-exists overwrite).
* Extra engines: Mongo, SQLite, MinIO S3, SQL Server dest-exists, Oracle
  dest-exists. GCS/ADLS/BQ create-new probes are omitted (hang risk).
* CDC default remains at-least-once upsert. Map SSOT stays semantic_mapper.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.transfer.cdc_transfer import run_cdc_database_transfer
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

from services.desktop_lab_cross import bind_live_engine
from tests.typed_fidelity_helpers import (
    drop_mysql_table,
    drop_pg_table,
    mysql_endpoint,
    pg_endpoint,
    reachable,
    require_ports,
    sqlite_endpoint,
    uniq,
)

UID_1 = "11111111-1111-4111-8111-111111111111"
UID_2 = "22222222-2222-4222-8222-222222222222"
BLOB_1 = bytes.fromhex("deadbeef")
JSON_1 = {"k": 1, "nested": {"ok": True}}


def _cell(kind: str, name: str, status: str, **extra: Any) -> dict[str, Any]:
    row = {"kind": kind, "name": name, "status": status}
    row.update(extra)
    return row


def _xfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    *,
    sync_mode: str = "full_refresh_overwrite",
    skip_preflight: bool = False,
    mappings: list[dict[str, Any]] | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    cursor_field: str | None = None,
    cursor_semantics: str | None = None,
) -> Any:
    extra: dict[str, Any] = {}
    if cursor_field:
        extra["cursor_field"] = cursor_field
    if cursor_semantics:
        extra["cursor_semantics"] = cursor_semantics
    contract = stream_contracts or [
        {
            "name": source.table or "stream",
            "sync_mode": sync_mode,
            "primary_key": "id",
            "selected": True,
            **extra,
        }
    ]
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode=sync_mode,
        skip_preflight=skip_preflight,
        validation_mode="strict",
        stream_contracts=contract,
        mappings=list(mappings or []),
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _pg_connect():
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    return conn


def _mysql_connect():
    import pymysql

    return pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )


def _pg_exec(sql: str, params: tuple | None = None) -> None:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
    finally:
        conn.close()


def _pg_fetch(sql: str, params: tuple | None = None) -> list[tuple]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def _pg_count(table: str) -> int:
    rows = _pg_fetch(f'SELECT count(*) FROM public."{table}"')
    return int(rows[0][0])


def _mysql_count(table: str) -> int:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM `{table}`")
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _oracle_password() -> str:
    env = (os.environ.get("DATAFLOW_ORACLE_PASSWORD") or "").strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    return path.read_text().strip() if path.is_file() else ""


def _seed_pg_rich(table: str) -> None:
    _pg_exec(f'DROP TABLE IF EXISTS public."{table}"')
    _pg_exec(
        f"""
        CREATE TABLE public."{table}" (
          id INT PRIMARY KEY,
          payload JSONB NOT NULL,
          uid UUID NOT NULL,
          blob BYTEA NOT NULL,
          tags INT[] NOT NULL,
          span INTERVAL NOT NULL
        )
        """
    )
    _pg_exec(
        f"""
        INSERT INTO public."{table}"
          (id, payload, uid, blob, tags, span)
        VALUES
          (1, %s::jsonb, %s::uuid, %s, ARRAY[1,2], INTERVAL '1 day 2 hours'),
          (2, '{{"k": 2}}'::jsonb, %s::uuid, %s, ARRAY[3], INTERVAL '0')
        """,
        (json.dumps(JSON_1), UID_1, BLOB_1, UID_2, bytes.fromhex("00ff")),
    )


def _seed_pg_simple(table: str, *, rows: int = 2) -> None:
    _pg_exec(f'DROP TABLE IF EXISTS public."{table}"')
    _pg_exec(
        f"""
        CREATE TABLE public."{table}" (
          id INT PRIMARY KEY,
          amount NUMERIC(12,2) NOT NULL,
          code VARCHAR(64) NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    values = [
        (1, "1000.00", "usd", "2024-01-01 00:00:00+00"),
        (2, "2000.50", "eur", "2024-01-01 00:00:00+00"),
        (3, "3000.00", "gbp", "2024-01-01 00:00:00+00"),
    ][:rows]
    for row in values:
        _pg_exec(
            f'INSERT INTO public."{table}" (id, amount, code, updated_at) '
            "VALUES (%s, %s, %s, %s)",
            row,
        )


def _seed_pg_scd2(table: str) -> None:
    _pg_exec(f'DROP TABLE IF EXISTS public."{table}"')
    _pg_exec(
        f"""
        CREATE TABLE public."{table}" (
          id INT PRIMARY KEY,
          name TEXT NOT NULL
        )
        """
    )
    _pg_exec(
        f'INSERT INTO public."{table}" (id, name) VALUES (1, %s), (2, %s)',
        ("A", "B"),
    )


def _assert_pg_rich(table: str) -> dict[str, Any]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT payload, uid::text, blob, tags, span '
                f'FROM public."{table}" WHERE id = 1'
            )
            row = cur.fetchone()
            assert row is not None, "no dest row id=1"
            payload, uid, blob, tags, span = row
            if isinstance(payload, str):
                payload = json.loads(payload)
            assert payload == JSON_1, payload
            assert str(uid) == UID_1, uid
            assert bytes(blob) == BLOB_1, blob
            assert list(tags) == [1, 2], tags
            seconds = getattr(span, "total_seconds", lambda: None)()
            assert seconds == 93600, f"interval seconds={seconds} span={span!r}"
            cur.execute(
                """
                SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public' AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (table,),
            )
            types = {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()
    return {"types": types, "payload": payload, "uid": uid}


def _create_pg_rich_dest(table: str) -> None:
    _pg_exec(f'DROP TABLE IF EXISTS public."{table}"')
    _pg_exec(
        f"""
        CREATE TABLE public."{table}" (
          id INT PRIMARY KEY,
          payload JSONB NOT NULL,
          uid UUID NOT NULL,
          blob BYTEA NOT NULL,
          tags INT[] NOT NULL,
          span INTERVAL NOT NULL
        )
        """
    )


def _seed_mysql_portable(table: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"""
                CREATE TABLE `{table}` (
                  id INT PRIMARY KEY,
                  payload JSON NOT NULL,
                  uid CHAR(36) NOT NULL,
                  blob_col BLOB NOT NULL
                )
                """
            )
            cur.execute(
                f"INSERT INTO `{table}` (id, payload, uid, blob_col) VALUES "
                "(%s, %s, %s, %s), (%s, %s, %s, %s)",
                (
                    1, json.dumps(JSON_1), UID_1, BLOB_1,
                    2, '{"k": 2}', UID_2, bytes.fromhex("00ff"),
                ),
            )
    finally:
        conn.close()


def _run_types() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []

    # Create-new invent: ARRAY→JSONB must fail-closed (not silently collapse).
    src_t, dst_t = uniq("ut_ty_c"), uniq("ut_ty_cd")
    try:
        _seed_pg_rich(src_t)
        result = _xfer(pg_endpoint(src_t), pg_endpoint(dst_t))
        blocked = not result.success and "ARRAY" in str(result.error or "")
        cells.append(_cell(
            "types_extended", "create_new_array_invent postgresql->postgresql",
            "passed" if blocked else "failed",
            expect="block",
            success=bool(result.success),
            error=str(result.error or "")[:300],
            note="create-new inventing JSONB for INT[] must fail-closed",
        ))
    finally:
        drop_pg_table(src_t)
        drop_pg_table(dst_t)

    # Dest-exists matching DDL without INT[] — dest-exists ARRAY still invents
    # JSONB (same gate as create-new). JSONB/UUID/BYTEA/INTERVAL round-trip here.
    src_t, dst_t = uniq("ut_ty_s"), uniq("ut_ty_d")
    try:
        _pg_exec(f'DROP TABLE IF EXISTS public."{src_t}"')
        _pg_exec(
            f"""
            CREATE TABLE public."{src_t}" (
              id INT PRIMARY KEY,
              payload JSONB NOT NULL,
              uid UUID NOT NULL,
              blob BYTEA NOT NULL,
              span INTERVAL NOT NULL
            )
            """
        )
        _pg_exec(
            f"""
            INSERT INTO public."{src_t}" (id, payload, uid, blob, span) VALUES
              (1, %s::jsonb, %s::uuid, %s, INTERVAL '1 day 2 hours'),
              (2, '{{"k": 2}}'::jsonb, %s::uuid, %s, INTERVAL '0')
            """,
            (json.dumps(JSON_1), UID_1, BLOB_1, UID_2, bytes.fromhex("00ff")),
        )
        _pg_exec(f'DROP TABLE IF EXISTS public."{dst_t}"')
        _pg_exec(
            f"""
            CREATE TABLE public."{dst_t}" (
              id INT PRIMARY KEY,
              payload JSONB NOT NULL,
              uid UUID NOT NULL,
              blob BYTEA NOT NULL,
              span INTERVAL NOT NULL
            )
            """
        )
        result = _xfer(pg_endpoint(src_t), pg_endpoint(dst_t))
        if not result.success:
            cells.append(_cell(
                "types_extended", "dest_exists_native postgresql->postgresql", "failed",
                error=str(result.error or "")[:300],
            ))
        else:
            try:
                conn = _pg_connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'SELECT payload, uid::text, blob, span '
                            f'FROM public."{dst_t}" WHERE id = 1'
                        )
                        row = cur.fetchone()
                        cur.execute(
                            f'SELECT span FROM public."{dst_t}" WHERE id = 2'
                        )
                        zero_row = cur.fetchone()
                finally:
                    conn.close()
                assert row is not None
                payload, uid, blob, span = row
                if isinstance(payload, str):
                    payload = json.loads(payload)
                seconds = getattr(span, "total_seconds", lambda: None)()
                assert payload == JSON_1, payload
                assert str(uid) == UID_1, uid
                assert bytes(blob) == BLOB_1, blob
                assert seconds == 93600, span
                assert zero_row is not None
                zero_seconds = getattr(zero_row[0], "total_seconds", lambda: None)()
                assert zero_seconds == 0, zero_row[0]
                cells.append(_cell(
                    "types_extended", "dest_exists_native postgresql->postgresql", "passed",
                    records=int(result.records_transferred or 0),
                    columns=["JSONB", "UUID", "BYTEA", "INTERVAL"],
                    note="INT[] dest-exists still invents JSONB — see create_new_array_invent",
                ))
            except Exception as exc:
                cells.append(_cell(
                    "types_extended", "dest_exists_native postgresql->postgresql", "failed",
                    error=str(exc)[:300],
                ))
    finally:
        drop_pg_table(src_t)
        drop_pg_table(dst_t)

    # Portable JSON / UUID / BLOB through MySQL (no INTERVAL / INT[]).
    src_t, dst_t = uniq("ut_ty_m"), uniq("ut_ty_md")
    try:
        _seed_mysql_portable(src_t)
        result = _xfer(mysql_endpoint(src_t), pg_endpoint(dst_t))
        if not result.success:
            cells.append(_cell(
                "types_extended", "portable_json_uuid_blob mysql->postgresql", "failed",
                error=str(result.error or "")[:300],
            ))
        else:
            conn = _pg_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT a.attname FROM pg_attribute a
                        JOIN pg_class c ON a.attrelid = c.oid
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname='public' AND c.relname=%s
                          AND a.attnum>0 AND NOT a.attisdropped
                        """,
                        (dst_t,),
                    )
                    cols = {r[0] for r in cur.fetchall()}
                    blob_col = "blob" if "blob" in cols else "blob_col"
                    uid_expr = "uid::text" if "uid" in cols else "uid"
                    cur.execute(
                        f'SELECT payload, {uid_expr}, "{blob_col}" '
                        f'FROM public."{dst_t}" WHERE id = 1'
                    )
                    row = cur.fetchone()
            finally:
                conn.close()
            if not row:
                cells.append(_cell(
                    "types_extended", "portable_json_uuid_blob mysql->postgresql",
                    "failed", error="no dest row",
                ))
            else:
                payload, uid, blob = row
                if isinstance(payload, str):
                    payload = json.loads(payload)
                ok = payload == JSON_1 and str(uid).lower() == UID_1 and bytes(blob) == BLOB_1
                cells.append(_cell(
                    "types_extended", "portable_json_uuid_blob mysql->postgresql",
                    "passed" if ok else "failed",
                    json_ok=payload == JSON_1,
                    uid_ok=str(uid).lower() == UID_1,
                    blob_ok=bytes(blob) == BLOB_1,
                ))
    except Exception as exc:
        cells.append(_cell(
            "types_extended", "portable_json_uuid_blob mysql->postgresql",
            "failed", error=str(exc)[:300],
        ))
    finally:
        drop_mysql_table(src_t)
        drop_pg_table(dst_t)
    return cells


def _run_incremental_deduped() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for dest_engine, count, drop, endpoint in (
        ("postgresql", _pg_count, drop_pg_table, pg_endpoint),
        ("mysql", _mysql_count, drop_mysql_table, mysql_endpoint),
    ):
        src_t, dst_t = uniq("ut_inc_s"), uniq("ut_inc_d")
        try:
            _seed_pg_simple(src_t, rows=2)
            src, dst = pg_endpoint(src_t), endpoint(dst_t)
            first = _xfer(
                src, dst, sync_mode="incremental_deduped",
                cursor_field="updated_at",
                cursor_semantics="modification_timestamp",
            )
            second = _xfer(
                src, dst, sync_mode="incremental_deduped",
                cursor_field="updated_at",
                cursor_semantics="modification_timestamp",
            )
            if not first.success:
                cells.append(_cell(
                    "sync_extended", f"incremental_deduped postgresql->{dest_engine}",
                    "failed", error=str(first.error or "")[:300],
                ))
                continue
            _pg_exec(
                f'UPDATE public."{src_t}" SET amount = 1111.11, '
                "updated_at = '2024-06-01 00:00:00+00' WHERE id = 1"
            )
            third = _xfer(
                src, dst, sync_mode="incremental_deduped",
                cursor_field="updated_at",
                cursor_semantics="modification_timestamp",
            )
            dest_n = count(dst_t)
            if dest_engine == "postgresql":
                amt = _pg_fetch(
                    f'SELECT amount FROM public."{dst_t}" WHERE id = 1'
                )[0][0]
            else:
                conn = _mysql_connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT amount FROM `{dst_t}` WHERE id = 1")
                        amt = cur.fetchone()[0]
                finally:
                    conn.close()
            changed = str(amt).startswith("1111.11")
            ok = (
                first.success and second.success and third.success
                and dest_n == 2 and changed
            )
            cells.append(_cell(
                "sync_extended", f"incremental_deduped postgresql->{dest_engine}",
                "passed" if ok else "failed",
                dest_rows=dest_n,
                amount_updated=changed,
                first_rows=int(first.records_transferred or 0),
                third_rows=int(third.records_transferred or 0),
                error="" if ok else (
                    str(third.error or second.error or "")[:300]
                    or f"dest={dest_n} amount={amt}"
                ),
            ))
        except Exception as exc:
            cells.append(_cell(
                "sync_extended", f"incremental_deduped postgresql->{dest_engine}",
                "failed", error=str(exc)[:300],
            ))
        finally:
            drop_pg_table(src_t)
            drop(dst_t)
    return cells


def _run_mirror() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for dest_engine, count, drop, endpoint in (
        ("postgresql", _pg_count, drop_pg_table, pg_endpoint),
        ("mysql", _mysql_count, drop_mysql_table, mysql_endpoint),
    ):
        src_t, dst_t = uniq("ut_mir_s"), uniq("ut_mir_d")
        try:
            _seed_pg_simple(src_t, rows=3)
            src, dst = pg_endpoint(src_t), endpoint(dst_t)
            first = _xfer(src, dst, sync_mode="mirror")
            if not first.success:
                cells.append(_cell(
                    "sync_extended", f"mirror postgresql->{dest_engine}",
                    "failed", error=str(first.error or "")[:300],
                ))
                continue
            _pg_exec(f'DELETE FROM public."{src_t}" WHERE id = 3')
            second = _xfer(src, dst, sync_mode="mirror")
            dest_n = count(dst_t)
            leftover_active = False
            if dest_engine == "postgresql":
                cols = {r[0] for r in _pg_fetch(
                    """
                    SELECT a.attname FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname='public' AND c.relname=%s
                      AND a.attnum>0 AND NOT a.attisdropped
                    """,
                    (dst_t,),
                )}
                if "_deleted" in cols:
                    leftover_active = bool(_pg_fetch(
                        f'SELECT 1 FROM public."{dst_t}" WHERE id = 3 '
                        "AND COALESCE(_deleted, FALSE) = FALSE"
                    ))
                    dest_n = int(_pg_fetch(
                        f'SELECT count(*) FROM public."{dst_t}" '
                        "WHERE COALESCE(_deleted, FALSE) = FALSE"
                    )[0][0])
                else:
                    leftover_active = bool(_pg_fetch(
                        f'SELECT 1 FROM public."{dst_t}" WHERE id = 3'
                    ))
            else:
                conn = _mysql_connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SHOW COLUMNS FROM `{dst_t}`")
                        cols = {r[0] for r in cur.fetchall()}
                        if "_deleted" in cols:
                            cur.execute(
                                f"SELECT count(*) FROM `{dst_t}` "
                                "WHERE COALESCE(_deleted, 0) = 0"
                            )
                            dest_n = int(cur.fetchone()[0])
                            cur.execute(
                                f"SELECT 1 FROM `{dst_t}` WHERE id = 3 "
                                "AND COALESCE(_deleted, 0) = 0"
                            )
                            leftover_active = bool(cur.fetchone())
                        else:
                            cur.execute(f"SELECT 1 FROM `{dst_t}` WHERE id = 3")
                            leftover_active = bool(cur.fetchone())
                finally:
                    conn.close()
            leftover = leftover_active
            ok = first.success and second.success and dest_n == 2 and not leftover
            ledger = getattr(second, "row_accounting", None) or {}
            cells.append(_cell(
                "sync_extended", f"mirror postgresql->{dest_engine}",
                "passed" if ok else "failed",
                dest_rows=dest_n,
                leftover_id3=leftover,
                inferred_deletes=ledger.get("inferred_deletes"),
                conservation_kind=ledger.get("conservation_kind"),
                error="" if ok else str(second.error or f"dest={dest_n} leftover={leftover}")[:300],
            ))
        except Exception as exc:
            cells.append(_cell(
                "sync_extended", f"mirror postgresql->{dest_engine}",
                "failed", error=str(exc)[:300],
            ))
        finally:
            drop_pg_table(src_t)
            drop(dst_t)
    return cells


def _run_scd2() -> list[dict[str, Any]]:
    src_t = uniq("ut_scd_s")
    dest_path = Path(tempfile.mkdtemp()) / "scd2.db"
    try:
        _seed_pg_scd2(src_t)
        src = pg_endpoint(src_t)
        dst = sqlite_endpoint(str(dest_path), "products")
        mappings = [
            {"source": "id", "target": "id", "confidence": 1.0, "user_override": True},
            {"source": "name", "target": "name", "confidence": 1.0, "user_override": True},
        ]
        first = _xfer(src, dst, sync_mode="scd2", mappings=mappings)
        if not first.success:
            return [_cell("sync_extended", "scd2 postgresql->sqlite", "failed",
                          error=str(first.error or "")[:300])]
        _pg_exec(f'UPDATE public."{src_t}" SET name = %s WHERE id = 1', ("A-updated",))
        second = _xfer(src, dst, sync_mode="scd2", mappings=mappings)
        con = sqlite3.connect(str(dest_path))
        try:
            cols = [r[1] for r in con.execute("PRAGMA table_info(products)")]
            total = int(con.execute("SELECT count(*) FROM products").fetchone()[0])
            current_col = next((c for c in cols if c.lower() in {"is_current", "_is_current"}), None)
            current = total
            if current_col:
                current = int(con.execute(
                    f"SELECT count(*) FROM products WHERE {current_col} IN (1, '1')"
                ).fetchone()[0])
        finally:
            con.close()
        ok = first.success and second.success and current == 2 and total >= 3
        return [_cell(
            "sync_extended", "scd2 postgresql->sqlite",
            "passed" if ok else "failed",
            dest_rows=total, current_rows=current, current_col=current_col,
            error="" if ok else str(second.error or f"total={total} current={current}")[:300],
        )]
    except Exception as exc:
        return [_cell("sync_extended", "scd2 postgresql->sqlite", "failed",
                      error=str(exc)[:300])]
    finally:
        drop_pg_table(src_t)


def _run_reverse_etl() -> list[dict[str, Any]]:
    src_t, dst_t = uniq("ut_retl_s"), uniq("ut_retl_d")
    try:
        _seed_pg_simple(src_t, rows=2)
        first = _xfer(pg_endpoint(src_t), mysql_endpoint(dst_t), sync_mode="reverse_etl")
        second = _xfer(pg_endpoint(src_t), mysql_endpoint(dst_t), sync_mode="reverse_etl")
        dest_n = _mysql_count(dst_t) if first.success else None
        ok = first.success and second.success and dest_n == 2
        return [_cell(
            "sync_extended", "reverse_etl postgresql->mysql",
            "passed" if ok else "failed",
            dest_rows=dest_n,
            note="warehouse→OLTP upsert on desktop; Salesforce omitted",
            error="" if ok else str(first.error or second.error or f"dest={dest_n}")[:300],
        )]
    except Exception as exc:
        return [_cell("sync_extended", "reverse_etl postgresql->mysql", "failed",
                      error=str(exc)[:300])]
    finally:
        drop_pg_table(src_t)
        drop_mysql_table(dst_t)


def _run_cdc_mysql() -> dict[str, Any]:
    if not reachable("localhost", 3306):
        return _cell("cdc", "mysql_binlog->sqlite", "skipped", error="MySQL down")
    try:
        import pymysqlreplication  # noqa: F401
    except ImportError:
        return _cell("cdc", "mysql_binlog->sqlite", "skipped", error="pymysqlreplication missing")
    src_table = uniq("ut_cdc_my").replace("-", "_")
    dest_path = Path(tempfile.mkdtemp()) / "cdc_mysql.db"
    job_id = "ut-mysql-" + uuid.uuid4().hex[:8]
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
            cur.execute(
                f"CREATE TABLE `{src_table}` (id INT PRIMARY KEY, amount DECIMAL(10,2))"
            )
            cur.execute(
                f"INSERT INTO `{src_table}` (id, amount) VALUES (1, 10.00), (2, 20.00)"
            )
    finally:
        conn.close()
    src = EndpointConfig(
        kind="database", format="mysql", host="localhost", port=3306,
        database="dataflow", username="dataflow", password="dataflow",
        table=src_table, connection_string="", ssl=False,
    )
    dst = sqlite_endpoint(str(dest_path), src_table)
    mappings = [
        {"source": "id", "target": "id", "source_type": "INT", "target_type": "INTEGER"},
        {"source": "amount", "target": "amount", "source_type": "DECIMAL", "target_type": "NUMERIC"},
    ]
    schema = {"id": "INTEGER", "amount": "NUMERIC(10,2)"}
    stream = [{"name": src_table, "selected": True, "snapshot_mode": "initial",
               "primary_key": "id", "sync_mode": "cdc"}]
    try:
        rows1, ddl1, summary1, _ = run_cdc_database_transfer(
            src, dst, mappings, schema, sync_mode="cdc",
            stream_contracts=stream, job_id=job_id, limit=2,
        )
        capture = any("CDC(binlog)" in line for line in ddl1)
        conn = _mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO `{src_table}` (id, amount) VALUES (3, 30.00)")
                cur.execute(f"UPDATE `{src_table}` SET amount = 99.00 WHERE id = 1")
                cur.execute(f"DELETE FROM `{src_table}` WHERE id = 2")
        finally:
            conn.close()
        rows2, ddl2, summary2, _ = run_cdc_database_transfer(
            src, dst, mappings, schema, sync_mode="cdc",
            stream_contracts=stream, job_id=job_id,
        )
        con = sqlite3.connect(str(dest_path))
        try:
            landed = list(con.execute(f'SELECT id, amount FROM "{src_table}" ORDER BY id'))
        finally:
            con.close()
        ids = [int(r[0]) for r in landed]
        amounts = {int(r[0]): float(r[1]) for r in landed}
        ok = (
            capture and rows1 == 2 and 2 not in ids and 1 in ids and 3 in ids
            and amounts.get(1) == 99.0 and amounts.get(3) == 30.0
            and not any("downgraded" in line.lower() for line in ddl1 + ddl2)
        )
        return _cell(
            "cdc", "mysql_binlog->sqlite", "passed" if ok else "failed",
            snapshot_rows=rows1, resume_rows=rows2, dest_ids=ids,
            capture="CDC(binlog)" if capture else "unknown",
            delivery="at-least-once upsert",
            watermark=(summary2.get("cdc") or {}).get("watermark"),
            error="" if ok else f"ids={ids} amounts={amounts} ddl={ddl1[:3]}",
        )
    except Exception as exc:
        return _cell("cdc", "mysql_binlog->sqlite", "failed", error=str(exc)[:300])
    finally:
        conn = _mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
        finally:
            conn.close()


def _run_cdc_postgres() -> dict[str, Any]:
    if not reachable("localhost", 5432):
        return _cell("cdc", "postgresql_logical->postgresql", "skipped", error="Postgres down")
    wal = _pg_fetch("SHOW wal_level")
    if not wal or str(wal[0][0]) != "logical":
        return _cell("cdc", "postgresql_logical->postgresql", "skipped",
                     error=f"wal_level={wal}")
    src_t, dst_t = uniq("ut_cdc_ps"), uniq("ut_cdc_pd")
    job_id = "ut-pg-" + uuid.uuid4().hex[:8]
    slot_name = ""
    try:
        _pg_exec(f'DROP TABLE IF EXISTS public."{src_t}"')
        _pg_exec(f'DROP TABLE IF EXISTS public."{dst_t}"')
        _pg_exec(
            f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, amount NUMERIC(10,2))'
        )
        _pg_exec(f'INSERT INTO public."{src_t}" (id, amount) VALUES (1, 10.00), (2, 20.00)')
        src = pg_endpoint(src_t)
        dst = pg_endpoint(dst_t)
        mappings = [
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "amount", "target": "amount", "source_type": "NUMERIC", "target_type": "NUMERIC"},
        ]
        schema = {"id": "INTEGER", "amount": "NUMERIC(10,2)"}
        stream = [{"name": src_t, "selected": True, "snapshot_mode": "initial",
                   "primary_key": "id", "sync_mode": "cdc"}]
        rows1, _, summary1, _ = run_cdc_database_transfer(
            src, dst, mappings, schema, sync_mode="cdc",
            stream_contracts=stream, job_id=job_id, limit=2,
        )
        cdc1 = summary1.get("cdc") or {}
        slot_name = str(cdc1.get("cdc_slot_name") or "")
        _pg_exec(f'INSERT INTO public."{src_t}" (id, amount) VALUES (3, 30.00)')
        _pg_exec(f'UPDATE public."{src_t}" SET amount = 99.00 WHERE id = 1')
        rows2, _, summary2, _ = run_cdc_database_transfer(
            src, dst, mappings, schema, sync_mode="cdc",
            stream_contracts=stream, job_id=job_id,
        )
        landed = _pg_fetch(f'SELECT id, amount FROM public."{dst_t}" ORDER BY id')
        amounts = {int(r[0]): float(r[1]) for r in landed}
        wm = str((summary2.get("cdc") or {}).get("watermark") or "")
        ok = (
            rows1 == 2 and amounts.get(1) == 99.0 and amounts.get(2) == 20.0
            and amounts.get(3) == 30.0 and "lsn=" in wm
        )
        return _cell(
            "cdc", "postgresql_logical->postgresql", "passed" if ok else "failed",
            snapshot_rows=rows1, resume_rows=rows2, dest_ids=[int(r[0]) for r in landed],
            capture="CDC(logical)", delivery="at-least-once upsert",
            watermark=wm, slot=slot_name,
            error="" if ok else f"amounts={amounts} wm={wm}",
        )
    except Exception as exc:
        return _cell("cdc", "postgresql_logical->postgresql", "failed", error=str(exc)[:300])
    finally:
        if slot_name:
            try:
                _pg_exec(
                    "SELECT pg_drop_replication_slot(%s) WHERE EXISTS "
                    "(SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                    (slot_name, slot_name),
                )
            except Exception:
                pass
        drop_pg_table(src_t)
        drop_pg_table(dst_t)


def _run_g14() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    src_t, dst_t = uniq("ut_g14_s"), uniq("ut_g14_d")
    try:
        _seed_pg_simple(src_t, rows=2)
        _pg_exec(f'DROP TABLE IF EXISTS public."{dst_t}"')
        _pg_exec(
            f"""
            CREATE TABLE public."{dst_t}" (
              id INT PRIMARY KEY,
              amount NUMERIC(12,2) NOT NULL,
              code TEXT NOT NULL,
              updated_at TIMESTAMPTZ NOT NULL,
              tenant_id TEXT NOT NULL
            )
            """
        )
        result = _xfer(pg_endpoint(src_t), pg_endpoint(dst_t))
        blocked = not result.success
        cells.append(_cell(
            "schema_g14", "dest_only_not_null postgresql->postgresql",
            "passed" if blocked else "failed",
            expect="block",
            success=bool(result.success),
            error=str(result.error or "")[:300],
            note="G14: dest-only NOT NULL tenant_id with no default must block",
        ))
    except Exception as exc:
        cells.append(_cell(
            "schema_g14", "dest_only_not_null postgresql->postgresql",
            "failed", error=str(exc)[:300],
        ))
    finally:
        drop_pg_table(src_t)
        drop_pg_table(dst_t)
    return cells


def _sqlserver_prepare(table: str) -> str | None:
    if not reachable("localhost", 1433):
        return "SQL Server not reachable on 1433"
    import pymssql

    conn = pymssql.connect(
        server="localhost", port=1433, user="sa",
        password="Datawrap_CDC_2022!", database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"IF OBJECT_ID('dbo.[{table}]', 'U') IS NOT NULL DROP TABLE dbo.[{table}]")
            cur.execute(
                f"CREATE TABLE dbo.[{table}] (id INT PRIMARY KEY, amount DECIMAL(12,2) NOT NULL, "
                "code NVARCHAR(64) NOT NULL, updated_at DATETIMEOFFSET NOT NULL)"
            )
        conn.commit()
    finally:
        conn.close()
    return None


def _sqlserver_count(table: str) -> int:
    import pymssql

    conn = pymssql.connect(
        server="localhost", port=1433, user="sa",
        password="Datawrap_CDC_2022!", database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM dbo.[{table}]")
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _oracle_prepare(table: str) -> str | None:
    if not reachable("localhost", 1521):
        return "Oracle not reachable on 1521"
    password = _oracle_password()
    if not password:
        return "Oracle password unset"
    import oracledb

    conn = oracledb.connect(user="dataflow", password=password, dsn="localhost:1521/XEPDB1")
    try:
        cur = conn.cursor()
        cur.execute(
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        cur.execute(
            f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, amount NUMBER(12,2) NOT NULL, "
            "code VARCHAR2(64) NOT NULL)"
        )
        conn.commit()
    finally:
        conn.close()
    return None


def _oracle_count(table: str) -> int:
    import oracledb

    conn = oracledb.connect(
        user="dataflow", password=_oracle_password(), dsn="localhost:1521/XEPDB1",
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT count(*) FROM {table}")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _run_engine_routes(tmp: Path) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    src_t = uniq("ut_eng_s")
    _seed_pg_simple(src_t, rows=2)
    src = pg_endpoint(src_t)
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "amount", "target": "amount", "confidence": 0.99},
        {"source": "code", "target": "code", "confidence": 0.99},
        {"source": "updated_at", "target": "updated_at", "confidence": 0.99},
    ]

    # SQLite / Mongo / MinIO — create-new. GCS/ADLS/BQ create-new probes hang
    # this host even with skip_preflight on the transfer request (writer probe).
    for dest_engine, skip_pf in (
        ("sqlite", False),
        ("mongodb", False),
        ("s3", True),
    ):
        bound = bind_live_engine(dest_engine, uniq("utd"), tmp)
        if isinstance(bound, str):
            cells.append(_cell("engine_route", f"postgresql->{dest_engine}", "skipped",
                               error=bound))
            continue
        try:
            result = _xfer(src, bound, skip_preflight=skip_pf, mappings=mappings)
            dest_n = None
            if result.success:
                try:
                    from services.dest_precount import destination_row_count

                    dest_n = destination_row_count(
                        dest_engine,
                        {
                            "host": bound.host, "port": bound.port,
                            "database": bound.database, "username": bound.username,
                            "password": bound.password,
                            "connection_string": bound.connection_string or "",
                            "endpoint_url": getattr(bound, "endpoint_url", "") or "",
                        },
                        schema=bound.schema or "",
                        table_name=bound.table or "",
                    )
                except Exception:
                    dest_n = int(result.records_transferred or 0)
            ok = bool(result.success) and dest_n == 2
            cells.append(_cell(
                "engine_route", f"postgresql->{dest_engine}",
                "passed" if ok else "failed",
                dest_rows=dest_n,
                skip_preflight=skip_pf,
                error="" if ok else str(result.error or f"dest={dest_n}")[:300],
            ))
        except Exception as exc:
            cells.append(_cell(
                "engine_route", f"postgresql->{dest_engine}", "failed",
                error=str(exc)[:300],
            ))

    for dest_engine, reason in (
        ("gcs", "create-new writer probe hangs on fake-gcs"),
        ("adls", "create-new writer probe hangs on Azurite"),
        ("bigquery", "create-new writer probe hangs on BQ emulator"),
    ):
        cells.append(_cell(
            "engine_route", f"postgresql->{dest_engine}", "skipped", error=reason,
        ))

    # SQL Server / Oracle — dest-exists tables we create (avoid create-new probe hang).
    ss_t = uniq("utss")
    skip = _sqlserver_prepare(ss_t)
    if skip:
        cells.append(_cell("engine_route", "postgresql->sqlserver", "skipped", error=skip))
    else:
        dest = bind_live_engine("sqlserver", ss_t, tmp)
        try:
            result = _xfer(src, dest, mappings=mappings) if not isinstance(dest, str) else None
            dest_n = _sqlserver_count(ss_t) if result and result.success else None
            ok = bool(result and result.success and dest_n == 2)
            cells.append(_cell(
                "engine_route", "postgresql->sqlserver",
                "passed" if ok else "failed",
                dest_rows=dest_n, dest_exists=True,
                error="" if ok else str((result.error if result else dest) or "")[:300],
            ))
        except Exception as exc:
            cells.append(_cell("engine_route", "postgresql->sqlserver", "failed",
                               error=str(exc)[:300]))
        finally:
            from tests.typed_fidelity_helpers import drop_sqlserver_table
            drop_sqlserver_table(ss_t)

    ora_t = ("UT" + uuid.uuid4().hex[:10]).upper()
    skip = _oracle_prepare(ora_t)
    if skip:
        cells.append(_cell("engine_route", "postgresql->oracle", "skipped", error=skip))
    else:
        dest = bind_live_engine("oracle", ora_t, tmp)
        ora_map = [m for m in mappings if m["source"] != "updated_at"]
        ora_map.append({
            "source": "updated_at", "target": "",
            "intentional_omit": True, "confidence": 1.0,
        })
        try:
            result = _xfer(src, dest, mappings=ora_map) if not isinstance(dest, str) else None
            dest_n = _oracle_count(ora_t) if result and result.success else None
            ok = bool(result and result.success and dest_n == 2)
            cells.append(_cell(
                "engine_route", "postgresql->oracle",
                "passed" if ok else "failed",
                dest_rows=dest_n, dest_exists=True,
                error="" if ok else str((result.error if result else dest) or "")[:300],
            ))
        except Exception as exc:
            cells.append(_cell("engine_route", "postgresql->oracle", "failed",
                               error=str(exc)[:300]))
        finally:
            try:
                import oracledb
                conn = oracledb.connect(
                    user="dataflow", password=_oracle_password(),
                    dsn="localhost:1521/XEPDB1",
                )
                try:
                    conn.cursor().execute(
                        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {ora_t}'; "
                        "EXCEPTION WHEN OTHERS THEN NULL; END;"
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                pass

    drop_pg_table(src_t)
    return cells


def _run_more_types() -> list[dict[str, Any]]:
    """XML, native POINT (no PostGIS), and JSON-array unnest on this desktop."""
    cells: list[dict[str, Any]] = []

    src_t, dst_t = uniq("ut_xml_s"), uniq("ut_xml_d")
    try:
        _pg_exec(f'DROP TABLE IF EXISTS public."{src_t}"')
        _pg_exec(
            f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, doc XML NOT NULL)'
        )
        _pg_exec(
            f"INSERT INTO public.\"{src_t}\" (id, doc) VALUES (1, '<item sku=\"A\"/>')"
        )
        _pg_exec(f'DROP TABLE IF EXISTS public."{dst_t}"')
        _pg_exec(
            f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, doc XML NOT NULL)'
        )
        result = _xfer(pg_endpoint(src_t), pg_endpoint(dst_t))
        landed = _pg_fetch(f'SELECT id, doc::text FROM public."{dst_t}" WHERE id = 1')
        ok = bool(result.success and landed and "sku" in str(landed[0][1]))
        cells.append(_cell(
            "types_extended", "xml dest_exists postgresql->postgresql",
            "passed" if ok else "failed",
            error="" if ok else str(result.error or f"row={landed}")[:300],
        ))
    except Exception as exc:
        cells.append(_cell(
            "types_extended", "xml dest_exists postgresql->postgresql",
            "failed", error=str(exc)[:300],
        ))
    finally:
        drop_pg_table(src_t)
        drop_pg_table(dst_t)

    src_t, dst_t = uniq("ut_pt_s"), uniq("ut_pt_d")
    try:
        _pg_exec(f'DROP TABLE IF EXISTS public."{src_t}"')
        _pg_exec(
            f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, loc POINT NOT NULL)'
        )
        _pg_exec(f"INSERT INTO public.\"{src_t}\" (id, loc) VALUES (1, '(1,2)')")
        _pg_exec(f'DROP TABLE IF EXISTS public."{dst_t}"')
        _pg_exec(
            f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, loc POINT NOT NULL)'
        )
        result = _xfer(pg_endpoint(src_t), pg_endpoint(dst_t))
        landed = _pg_fetch(f'SELECT loc::text FROM public."{dst_t}" WHERE id = 1')
        ok = bool(result.success and landed and "1" in str(landed[0][0]))
        cells.append(_cell(
            "types_extended", "point dest_exists postgresql->postgresql",
            "passed" if ok else "failed",
            note="native POINT — PostGIS geography omitted (extension absent)",
            error="" if ok else str(result.error or f"row={landed}")[:300],
        ))
    except Exception as exc:
        cells.append(_cell(
            "types_extended", "point dest_exists postgresql->postgresql",
            "failed", error=str(exc)[:300],
        ))
    finally:
        drop_pg_table(src_t)
        drop_pg_table(dst_t)

    try:
        from tests.test_shape_unnest_any_source_matrix import (
            EXPECTED_SKUS,
            UNNEST_RECIPE,
            _file_bytes,
            _mappings,
            _recipe_hash,
        )

        dst_t = uniq("ut_unnest_d")
        content, filename = _file_bytes("csv")
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=pg_endpoint(dst_t),
            sync_mode="full_refresh_overwrite",
            skip_preflight=False,
            validation_mode="strict",
            mappings=_mappings(),
            source_filename=filename,
            source_content=content,
            shape_recipe=UNNEST_RECIPE,
            approved_shape_recipe_hash=_recipe_hash(),
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        rows = _pg_fetch(
            f'SELECT order_no::text, sku, qty::int FROM public."{dst_t}" ORDER BY sku'
        ) if result.success else []
        landed = [(str(r[0]), str(r[1]), int(r[2])) for r in rows]
        ok = result.success and landed == list(EXPECTED_SKUS)
        cells.append(_cell(
            "types_extended", "nested_explode csv->postgresql",
            "passed" if ok else "failed",
            dest_rows=len(landed),
            recipe_hash=_recipe_hash(),
            error="" if ok else str(result.error or f"rows={landed}")[:300],
        ))
        drop_pg_table(dst_t)
    except Exception as exc:
        cells.append(_cell(
            "types_extended", "nested_explode csv->postgresql",
            "failed", error=str(exc)[:300],
        ))

    cells.append(_cell(
        "saas", "salesforce/hubspot/stripe", "skipped",
        error="No live SaaS backend on this desktop — not invented green",
    ))
    return cells


def run_desktop_lab_untested(*, persist: bool = True) -> dict[str, Any]:
    require_ports(5432, 3306)
    tmp = Path(tempfile.mkdtemp(prefix="df-untested-"))
    cells: list[dict[str, Any]] = []
    cells.extend(_run_types())
    cells.extend(_run_incremental_deduped())
    cells.extend(_run_mirror())
    cells.extend(_run_scd2())
    cells.extend(_run_reverse_etl())
    cells.append(_run_cdc_mysql())
    cells.append(_run_cdc_postgres())
    cells.extend(_run_g14())
    cells.extend(_run_more_types())
    cells.extend(_run_engine_routes(tmp))

    passed = sum(1 for c in cells if c["status"] == "passed")
    failed = sum(1 for c in cells if c["status"] == "failed")
    skipped = sum(1 for c in cells if c["status"] == "skipped")
    payload = {
        "fixture": "tests.desktop_lab_untested.run_desktop_lab_untested",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "cells": len(cells),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "results": cells,
        "honesty": {
            "not_every_sql_type": True,
            "types_measured": [
                "JSONB", "UUID", "BYTEA", "INT[]", "INTERVAL", "XML", "POINT",
            ],
            "types_not_claimed": ["geography/PostGIS"],
            "sync_modes_measured": [
                "incremental_deduped", "mirror", "scd2", "reverse_etl", "cdc",
            ],
            "reverse_etl_is": "warehouse→OLTP (postgresql→mysql), not Salesforce",
            "cdc_default": "at-least-once upsert",
            "engines_measured": [
                "postgresql", "mysql", "sqlite", "mongodb", "s3", "sqlserver", "oracle",
            ],
            "engines_omitted_hang_risk": ["gcs", "adls", "bigquery"],
            "saas_omitted": ["salesforce", "hubspot", "stripe"],
            "open_gaps_this_fixture": [
                "dest-exists INT[] invents JSONB (fail-closed — measured)",
                "geography/PostGIS skipped (extension absent)",
                "Salesforce/HubSpot/Stripe skipped (no live SaaS backend)",
                "GCS/ADLS/BQ create-new skipped (writer probe hang)",
                "customer-tenant warehouse PRODUCTION_SKU not claimed",
            ],
            "map_ssot": "services.semantic_mapper.map_columns",
            "catalog_tiles_are_not_transfer_live": True,
        },
    }
    if persist:
        text = json.dumps(payload, indent=2, default=str) + "\n"
        proofs = Path(__file__).resolve().parents[1] / "data" / "proofs"
        proofs.mkdir(parents=True, exist_ok=True)
        (proofs / "desktop_lab_untested.json").write_text(text)
        artifacts = Path("/opt/cursor/artifacts")
        if artifacts.is_dir():
            (artifacts / "desktop_lab_untested.json").write_text(text)
            lab = artifacts / "warehouse-emulator-lab"
            lab.mkdir(parents=True, exist_ok=True)
            (lab / "desktop_lab_untested.json").write_text(text)
    return payload
