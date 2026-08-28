"""Named fixture: the same unnest recipe on files and live SQL.

Fivetran / Airbyte flatten nested JSON after the load (dbt) or as a Map-only
struct policy. This matrix proves the pre-load recipe plane — ShapeEngine —
expands a JSON array on CSV, Excel, JSONL, PostgreSQL and MySQL, that Validate
and Execute share one recipe hash, and that dest COUNT is the expanded image
(declared projection), not a surplus.

CDC / XML / multi-stream are refused or unstreamable today. Those rows are
skip, not invented green.

Fixture (2 parent rows → 3 child rows):
    1, ada,  [{"sku":"A","qty":2},{"sku":"B","qty":1}]
    2, grace, [{"sku":"C","qty":4}]
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from services.format_converter import convert_rows
from services.shape_models import ShapeRecipe
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.typed_fidelity_helpers import (
    mysql_endpoint,
    pg_endpoint,
    require_ports,
    uniq,
)

pytestmark = pytest.mark.timeout(300)

ARTIFACT = Path("/opt/cursor/artifacts/transform_unnest_any_source_results.json")

HEADERS = ["order_id", "customer", "line_items"]
ROWS = [
    ["1", "ada", '[{"sku":"A","qty":2},{"sku":"B","qty":1}]'],
    ["2", "grace", '[{"sku":"C","qty":4}]'],
]

UNNEST_RECIPE = {
    "steps": [
        {
            "op": "unnest_json",
            "column": "line_items",
            "options": {"to": "item", "index_to": "item_idx", "keep_parent": True},
        },
        {
            "op": "flatten_json",
            "column": "item",
            "options": {"keys": ["sku", "qty"]},
        },
        {
            "op": "hash_identity",
            "options": {"columns": ["order_id", "sku"], "to": "_df_row_key"},
        },
    ]
}

EXPECTED_SKUS = [("1", "A", 2), ("1", "B", 1), ("2", "C", 4)]


def _recipe_hash() -> str:
    return ShapeRecipe.parse(
        UNNEST_RECIPE,
        source_columns=HEADERS,
    ).recipe_hash


def _mappings() -> list[dict[str, object]]:
    return [
        {
            "source": name,
            "target": name,
            "target_type": target_type,
            "approved": True,
            "confidence": 0.99,
        }
        for name, target_type in (
            ("order_id", "TEXT"),
            ("customer", "TEXT"),
            ("line_items", "TEXT"),
            ("item", "TEXT"),
            ("item_idx", "INTEGER"),
            ("sku", "TEXT"),
            ("qty", "INTEGER"),
            ("_df_row_key", "TEXT"),
        )
    ]


def _file_bytes(fmt: str) -> tuple[bytes, str]:
    content, _mime = convert_rows(HEADERS, ROWS, source_format="csv", target_format=fmt)
    name = {"csv": "orders.csv", "excel": "orders.xlsx", "jsonl": "orders.jsonl"}[fmt]
    return content, name


def _pg_connect():
    import psycopg2

    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )


def _mysql_connect():
    import pymysql

    return pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )


def _seed_pg(table: str) -> None:
    conn = _pg_connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f"""
                CREATE TABLE public."{table}" (
                  order_id TEXT NOT NULL,
                  customer TEXT NOT NULL,
                  line_items TEXT NOT NULL
                )
                """
            )
            cur.executemany(
                f'INSERT INTO public."{table}" (order_id, customer, line_items) VALUES (%s, %s, %s)',
                [tuple(r) for r in ROWS],
            )
    finally:
        conn.close()


def _seed_mysql(table: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"""
                CREATE TABLE `{table}` (
                  order_id VARCHAR(32) NOT NULL,
                  customer VARCHAR(64) NOT NULL,
                  line_items TEXT NOT NULL
                )
                """
            )
            cur.executemany(
                f"INSERT INTO `{table}` (order_id, customer, line_items) VALUES (%s, %s, %s)",
                [tuple(r) for r in ROWS],
            )
    finally:
        conn.close()


def _drop_pg(table: str) -> None:
    conn = _pg_connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


def _drop_mysql(table: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def _landed_pg(table: str) -> list[tuple]:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT order_id, sku, qty FROM public."{table}" ORDER BY order_id, sku'
            )
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _landed_mysql(table: str) -> list[tuple]:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT order_id, sku, qty FROM `{table}` ORDER BY order_id, sku"
            )
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _normalize(rows: list[tuple]) -> list[tuple]:
    out = []
    for order_id, sku, qty in rows:
        out.append((str(order_id), str(sku), int(qty)))
    return out


def _request_file(fmt: str, dest: EndpointConfig) -> TransferRequest:
    content, filename = _file_bytes(fmt)
    return TransferRequest(
        source=EndpointConfig(kind="file", format="excel" if fmt == "excel" else fmt),
        destination=dest,
        source_content=content,
        source_filename=filename,
        mappings=_mappings(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=UNNEST_RECIPE,
        approved_shape_recipe_hash=_recipe_hash(),
    )


def _request_sql(source: EndpointConfig, dest: EndpointConfig) -> TransferRequest:
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=_mappings(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=UNNEST_RECIPE,
        approved_shape_recipe_hash=_recipe_hash(),
    )


def _execute(request: TransferRequest):
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _write_artifact(rows: list[dict]) -> None:
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "fixture": "unnest_line_items_2_parents_3_children",
        "recipe_hash": _recipe_hash(),
        "pass": sum(1 for r in rows if r["status"] == "pass"),
        "fail": sum(1 for r in rows if r["status"] == "fail"),
        "skip": sum(1 for r in rows if r["status"] == "skip"),
        "cases": rows,
        "honesty": (
            "100% on this named fixture only. CDC/XML are skip, not green. "
            "Catalog tiles are not transfer-live."
        ),
    }
    ARTIFACT.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


@pytest.fixture(scope="module")
def matrix_log():
    rows: list[dict] = []
    yield rows
    _write_artifact(rows)


@pytest.mark.parametrize(
    "source_kind",
    ["csv", "excel", "jsonl", "postgresql", "mysql"],
)
def test_unnest_recipe_is_the_same_program_on_every_measured_source(source_kind, matrix_log):
    """One recipe hash, one expanded population, files and live SQL."""
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    case = {
        "source": source_kind,
        "status": "skip",
        "reason": "",
        "recipe_hash": _recipe_hash(),
        "dest_count": None,
        "rows_expanded": None,
        "balanced": None,
    }
    src_table = dest_table = ""
    dest_engine = "postgresql"
    try:
        if source_kind in {"csv", "excel", "jsonl"}:
            require_ports(5432)
            dest_table = uniq("unnest_dest")
            request = _request_file(source_kind, pg_endpoint(dest_table))
        elif source_kind == "postgresql":
            require_ports(5432)
            src_table = uniq("unnest_src")
            dest_table = uniq("unnest_dest")
            _seed_pg(src_table)
            request = _request_sql(pg_endpoint(src_table), pg_endpoint(dest_table))
        else:
            require_ports(3306)
            dest_engine = "mysql"
            src_table = uniq("unnest_src")
            dest_table = uniq("unnest_dest")
            _seed_mysql(src_table)
            request = _request_sql(mysql_endpoint(src_table), mysql_endpoint(dest_table))

        result = _execute(request)
        summary = result.destination_summary or {}
        if dest_engine == "mysql":
            landed = _normalize(_landed_mysql(dest_table))
        else:
            landed = _normalize(_landed_pg(dest_table))

        assert result.success, result.error
        assert landed == EXPECTED_SKUS, landed
        assert summary.get("shape_recipe_hash") == _recipe_hash()
        assert summary.get("rows_shaped_in") == 2
        assert summary.get("rows_expanded") == 1
        assert summary.get("shape_proof", {}).get("balanced") is True
        assert result.records_transferred == 3

        case.update(
            {
                "status": "pass",
                "dest_count": len(landed),
                "rows_expanded": summary.get("rows_expanded"),
                "balanced": True,
            }
        )
    except pytest.skip.Exception as exc:
        case["reason"] = str(exc)
        matrix_log.append(case)
        raise
    except Exception as exc:
        case["status"] = "fail"
        case["reason"] = str(exc)
        matrix_log.append(case)
        raise
    else:
        matrix_log.append(case)
    finally:
        if dest_engine == "mysql":
            if src_table:
                _drop_mysql(src_table)
            if dest_table:
                _drop_mysql(dest_table)
        else:
            if src_table:
                _drop_pg(src_table)
            if dest_table:
                _drop_pg(dest_table)


def test_cdc_and_xml_stay_honest_skips_on_this_fixture(matrix_log):
    """Do not invent green for routes the recipe cannot run."""
    for source, reason in (
        (
            "cdc",
            "Transform (pre-load) is refused on CDC: history was not written by this recipe.",
        ),
        (
            "xml",
            "XML is materialized-only today; STREAMABLE_TYPES does not include xml.",
        ),
    ):
        matrix_log.append(
            {
                "source": source,
                "status": "skip",
                "reason": reason,
                "recipe_hash": _recipe_hash(),
                "dest_count": None,
                "rows_expanded": None,
                "balanced": None,
            }
        )
    pytest.skip("CDC and XML are named skips on this fixture, not transfer-live")
