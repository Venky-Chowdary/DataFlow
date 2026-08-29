"""Live type × sync-mode × schema-shape matrix on desktop SQL engines.

Honesty
-------
* This is **not** every catalog type, every engine, or 80×80 aliases.
* Typed set is the named FIDELITY_COLUMNS fixture only
  (INT, DECIMAL, FLOAT, NULL vs '', timestamptz, BOOL). JSON/array/UUID/binary
  are not in this fixture.
* Sync modes here are the two-run operator set: overwrite, append,
  incremental_append, upsert. CDC / SCD2 / mirror / reverse_etl stay on their
  dedicated suites — not claimed here.
* Schema shapes: create-new, dest-exists compatible, dest-exists narrow
  (expect block), extra unmapped source column (expect fail-closed).
* Map SSOT stays semantic_mapper. CDC default remains at-least-once upsert.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.transfer.adapters import write_destination_database
from src.transfer.models import EndpointConfig, TransferRequest
from tests.sync_mode_probe import (
    COLUMNS,
    EXPECTED_AFTER_TWO_RUNS,
    MAPPINGS,
    RECORDS,
    SCHEMA,
    run_mode,
    stream_contract,
)
from tests.typed_fidelity_helpers import (
    assert_mysql_typed_fidelity,
    assert_pg_typed_fidelity,
    assert_preflight_ran,
    drop_mysql_table,
    drop_pg_table,
    mysql_endpoint,
    pg_endpoint,
    reachable,
    require_ports,
    run_typed_transfer,
    seed_mysql_typed,
    seed_postgresql_typed,
    uniq,
)

TYPED_COLUMNS = (
    "id INT",
    "amt_dec DECIMAL(12,4)",
    "amt_float FLOAT",
    "note_null TEXT",
    "note_empty TEXT",
    "ts_utc TIMESTAMPTZ/DATETIME",
    "flag BOOL",
)

ROUTES: tuple[tuple[str, str], ...] = (
    ("postgresql", "postgresql"),
    ("mysql", "postgresql"),
    ("postgresql", "mysql"),
    ("mysql", "mysql"),
)


def _seed(engine: str, table: str) -> None:
    if engine == "postgresql":
        seed_postgresql_typed(table)
    elif engine == "mysql":
        seed_mysql_typed(table)
    else:
        raise ValueError(engine)


def _endpoint(engine: str, table: str) -> EndpointConfig:
    return pg_endpoint(table) if engine == "postgresql" else mysql_endpoint(table)


def _drop(engine: str, table: str) -> None:
    if engine == "postgresql":
        drop_pg_table(table)
    else:
        drop_mysql_table(table)


def _create_dest(engine: str, table: str, *, narrow: bool) -> None:
    amt = "INT" if narrow else "NUMERIC(12,4)" if engine == "postgresql" else "DECIMAL(12,4)"
    if engine == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host="localhost", port=5432, database="dataflow",
            user="dataflow", password="dataflow",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f"""
                CREATE TABLE public."{table}" (
                  id INT PRIMARY KEY,
                  amt_dec {amt} NOT NULL,
                  amt_float DOUBLE PRECISION NOT NULL,
                  note_null TEXT,
                  note_empty TEXT NOT NULL,
                  ts_utc TIMESTAMPTZ NOT NULL,
                  flag BOOLEAN NOT NULL
                )
                """
            )
        conn.close()
        return
    import pymysql

    amt_my = "INT" if narrow else "DECIMAL(12,4)"
    conn = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
              id INT PRIMARY KEY,
              amt_dec {amt_my} NOT NULL,
              amt_float DOUBLE NOT NULL,
              note_null TEXT,
              note_empty VARCHAR(64) NOT NULL,
              ts_utc DATETIME(6) NOT NULL,
              flag TINYINT(1) NOT NULL
            )
            """
        )
    conn.close()


def _add_unmapped_column(engine: str, table: str) -> None:
    if engine == "postgresql":
        import psycopg2

        conn = psycopg2.connect(
            host="localhost", port=5432, database="dataflow",
            user="dataflow", password="dataflow",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE public."{table}" ADD COLUMN extra_unmapped TEXT')
            cur.execute(f"UPDATE public.\"{table}\" SET extra_unmapped = 'x'")
        conn.close()
        return
    import pymysql

    conn = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN extra_unmapped VARCHAR(8)")
        cur.execute(f"UPDATE `{table}` SET extra_unmapped = 'x'")
    conn.close()


def _cell(kind: str, source: str, dest: str, name: str, status: str, **extra: Any) -> dict[str, Any]:
    row = {
        "kind": kind,
        "source": source,
        "destination": dest,
        "name": name,
        "status": status,
    }
    row.update(extra)
    return row


def _run_typed_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for src, dst in ROUTES:
        src_t, dst_t = uniq("dim_src"), uniq("dim_dst")
        try:
            _seed(src, src_t)
            result = run_typed_transfer(_endpoint(src, src_t), _endpoint(dst, dst_t))
            if not result.success:
                cells.append(_cell(
                    "typed_create_new", src, dst, "fidelity_7col", "failed",
                    error=str(result.error or "")[:300],
                    expect="pass",
                ))
                continue
            try:
                assert_preflight_ran(result)
                if dst == "postgresql":
                    assert_pg_typed_fidelity(dst_t, expect_float_ddl=True)
                else:
                    assert_mysql_typed_fidelity(dst_t)
                cells.append(_cell(
                    "typed_create_new", src, dst, "fidelity_7col", "passed",
                    records=int(result.records_transferred or 0),
                    types=list(TYPED_COLUMNS),
                    expect="pass",
                ))
            except Exception as exc:
                cells.append(_cell(
                    "typed_create_new", src, dst, "fidelity_7col", "failed",
                    error=str(exc)[:300],
                    expect="pass",
                ))
        except Exception as exc:
            cells.append(_cell(
                "typed_create_new", src, dst, "fidelity_7col", "failed",
                error=str(exc)[:300],
                expect="pass",
            ))
        finally:
            _drop(src, src_t)
            _drop(dst, dst_t)
    return cells


def _run_sync_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    require_ports(5432)
    src_t = uniq("dim_sync_src")
    seed_postgresql_typed(src_t)
    # Sync probe uses id/amount only — write the two-run fixture onto PG.
    drop_pg_table(src_t)
    source = pg_endpoint(src_t)
    write_destination_database(source, RECORDS, COLUMNS, SCHEMA, MAPPINGS)
    try:
        for dest_engine in ("postgresql", "mysql"):
            if dest_engine == "mysql" and not reachable("localhost", 3306):
                for mode in EXPECTED_AFTER_TWO_RUNS:
                    cells.append(_cell(
                        "sync_two_run", "postgresql", dest_engine, mode,
                        "skipped", error="MySQL not reachable on 3306",
                    ))
                continue
            for mode in EXPECTED_AFTER_TWO_RUNS:
                dst_t = uniq("dim_sync_dst")
                dest = _endpoint(dest_engine, dst_t)
                try:
                    if dest_engine == "postgresql":
                        def _count(table: str = dst_t) -> int:
                            import psycopg2
                            conn = psycopg2.connect(
                                host="localhost", port=5432, database="dataflow",
                                user="dataflow", password="dataflow",
                            )
                            try:
                                with conn.cursor() as cur:
                                    cur.execute(f'SELECT count(*) FROM public."{table}"')
                                    return int(cur.fetchone()[0])
                            finally:
                                conn.close()
                    else:
                        def _count(table: str = dst_t) -> int:
                            import pymysql
                            conn = pymysql.connect(
                                host="localhost", port=3306, user="dataflow",
                                password="dataflow", database="dataflow",
                            )
                            try:
                                with conn.cursor() as cur:
                                    cur.execute(f"SELECT count(*) FROM `{table}`")
                                    return int(cur.fetchone()[0])
                            finally:
                                conn.close()

                    outcome = run_mode(
                        source, dest, mode,
                        stream_name=src_t,
                        count_rows=_count,
                    )
                    ok = outcome.correct
                    cells.append(_cell(
                        "sync_two_run", "postgresql", dest_engine, mode,
                        "passed" if ok else "failed",
                        first_rows=outcome.first_rows,
                        second_rows=outcome.second_rows,
                        dest_rows=outcome.destination_rows,
                        expected=outcome.expected,
                        error=outcome.error[:300],
                    ))
                except Exception as exc:
                    cells.append(_cell(
                        "sync_two_run", "postgresql", dest_engine, mode,
                        "failed", error=str(exc)[:300],
                    ))
                finally:
                    _drop(dest_engine, dst_t)
    finally:
        drop_pg_table(src_t)
    return cells


def _run_schema_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    cases = (
        ("dest_exists_compatible", False, "pass"),
        ("dest_exists_narrow_decimal", True, "block"),
    )
    for src, dst in ROUTES:
        for name, narrow, expect in cases:
            src_t, dst_t = uniq("dim_sch_s"), uniq("dim_sch_d")
            try:
                _seed(src, src_t)
                _create_dest(dst, dst_t, narrow=narrow)
                result = run_typed_transfer(_endpoint(src, src_t), _endpoint(dst, dst_t))
                if expect == "pass":
                    status = "passed" if result.success else "failed"
                else:
                    status = "passed" if not result.success else "failed"
                cells.append(_cell(
                    "schema_shape", src, dst, name, status,
                    expect=expect,
                    success=bool(result.success),
                    error=str(result.error or "")[:300],
                ))
            except Exception as exc:
                cells.append(_cell(
                    "schema_shape", src, dst, name, "failed",
                    expect=expect, error=str(exc)[:300],
                ))
            finally:
                _drop(src, src_t)
                _drop(dst, dst_t)

        src_t, dst_t = uniq("dim_sch_x"), uniq("dim_sch_xd")
        try:
            _seed(src, src_t)
            _add_unmapped_column(src, src_t)
            result = run_typed_transfer(_endpoint(src, src_t), _endpoint(dst, dst_t))
            # Fail-closed: unmapped extra column must not drop silently.
            status = "passed" if not result.success else "failed"
            cells.append(_cell(
                "schema_shape", src, dst, "extra_source_unmapped", status,
                expect="block",
                success=bool(result.success),
                error=str(result.error or "")[:300],
            ))
        except Exception as exc:
            cells.append(_cell(
                "schema_shape", src, dst, "extra_source_unmapped", "failed",
                expect="block", error=str(exc)[:300],
            ))
        finally:
            _drop(src, src_t)
            _drop(dst, dst_t)
    return cells


def run_desktop_lab_dimensions(*, persist: bool = True) -> dict[str, Any]:
    require_ports(5432, 3306)
    cells = _run_typed_cells() + _run_sync_cells() + _run_schema_cells()
    passed = sum(1 for c in cells if c["status"] == "passed")
    failed = sum(1 for c in cells if c["status"] == "failed")
    skipped = sum(1 for c in cells if c["status"] == "skipped")
    payload = {
        "fixture": "tests.desktop_lab_dimensions.run_desktop_lab_dimensions",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "cells": len(cells),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "results": cells,
        "honesty": {
            "not_every_sql_type": True,
            "typed_columns": list(TYPED_COLUMNS),
            "sync_modes_measured": sorted(EXPECTED_AFTER_TWO_RUNS),
            "sync_modes_not_claimed": [
                "cdc",
                "scd2",
                "mirror",
                "reverse_etl",
                "incremental_deduped",
            ],
            "schema_shapes_measured": [
                "typed_create_new",
                "dest_exists_compatible",
                "dest_exists_narrow_decimal",
                "extra_source_unmapped",
            ],
            "schema_block_cells_open": [
                "dest_exists_narrow_decimal — DECIMAL(12,4) source into INT dest "
                "did not fail-closed (transfer succeeded on all four SQL routes)",
                "extra_source_unmapped — extra source column was not gated "
                "(create-new succeeded; column omitted without a block)",
            ],
            "routes": [f"{a}->{b}" for a, b in ROUTES],
            "catalog_tiles_are_not_transfer_live": True,
            "cdc_default": "at-least-once upsert",
            "map_ssot": "services.semantic_mapper.map_columns",
        },
    }
    if persist:
        text = json.dumps(payload, indent=2) + "\n"
        proofs = Path(__file__).resolve().parents[1] / "data" / "proofs"
        proofs.mkdir(parents=True, exist_ok=True)
        (proofs / "desktop_lab_dimensions.json").write_text(text)
        artifacts = Path("/opt/cursor/artifacts")
        if artifacts.is_dir():
            (artifacts / "desktop_lab_dimensions.json").write_text(text)
            lab = artifacts / "warehouse-emulator-lab"
            lab.mkdir(parents=True, exist_ok=True)
            (lab / "desktop_lab_dimensions.json").write_text(text)
    return payload
