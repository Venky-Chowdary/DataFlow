"""Case A Studio Validate — live PostgreSQL dest INT, not a mocked INTEGER probe.

Execute already rounds 22.6/21.4/22.0 into an existing INT column (SUM=66).
The leftover was Validate: the last browser run blocked on ``schema_drift``
with ``source_changed:false``. Unit tests mock dest introspect as INTEGER,
so they cannot see live ``integer``/``INT4`` catalog spelling or append-mode
dest-contract grading.

This file calls the same ``run_plan_preflight`` Transfer Studio uses, against
a real existing INT table. Append is required: overwrite recreates PG DDL and
would skip the dest-carrier question the operator actually hits.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.shape_models import ShapeRecipe
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from tests.typed_fidelity_helpers import pg_endpoint, require_ports, uniq

CASE_A_SOURCE = (
    (1, Decimal("22.6")),
    (2, Decimal("21.4")),
    (3, Decimal("22.0")),
)
ROUNDED_SUM = 66
TRUNCATED_SUM = 65
ROUND_RECIPE = {
    "steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 0}}]
}


def _pg():
    import psycopg2

    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )


def _seed(src: str, dst: str) -> None:
    conn = _pg()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in (src, dst):
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f"""
                CREATE TABLE public."{src}" (
                  id INT PRIMARY KEY,
                  arr_time NUMERIC(12,1) NOT NULL
                )
                """
            )
            cur.executemany(
                f'INSERT INTO public."{src}" (id, arr_time) VALUES (%s, %s)',
                list(CASE_A_SOURCE),
            )
            cur.execute(
                f"""
                CREATE TABLE public."{dst}" (
                  id INT PRIMARY KEY,
                  arr_time INT NOT NULL
                )
                """
            )
    finally:
        conn.close()


def _drop(*tables: str) -> None:
    conn = _pg()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


def _sum(table: str) -> tuple[int, list[int]]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT arr_time FROM public."{table}" ORDER BY id')
            values = [int(r[0]) for r in cur.fetchall()]
            cur.execute(f'SELECT COALESCE(SUM(arr_time), 0) FROM public."{table}"')
            return int(cur.fetchone()[0]), values
    finally:
        conn.close()


def _pg_endpoint_dict(table: str) -> dict:
    return {
        "kind": "database",
        "format": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
        "table": table,
    }


def _findings(result: dict) -> str:
    parts = [str(b.get("message") or "") for b in result.get("blockers") or []]
    parts += [str(g.get("id") or "") + ":" + str(g.get("message") or "") for g in result.get("gates") or []]
    return " | ".join(parts)


@pytest.fixture
def isolated_plans(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json"
    )
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def test_case_a_plan_validate_append_existing_int_is_not_schema_drift(
    isolated_plans,
) -> None:
    """Studio Validate (append into live INT) must pass after round_number."""
    require_ports(5432)
    from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
    from services.transfer_plan_store import create_plan

    src = uniq("case_a_pf_src")
    dst = uniq("case_a_pf_dst")
    _seed(src, dst)
    try:
        plan = create_plan(
            {
                "name": "case-a-validate",
                "source": _pg_endpoint_dict(src),
                "destination": {**_pg_endpoint_dict(dst), "table_exists": True},
                "source_columns": ["id", "arr_time"],
                "source_schema": {"id": "INTEGER", "arr_time": "DECIMAL(12,9)"},
                "target_columns": ["id", "arr_time"],
                "target_schema": {"id": "INT", "arr_time": "INT"},
                "row_count_estimate": 3,
                "sample_rows": [
                    {"id": 1, "arr_time": "22.6"},
                    {"id": 2, "arr_time": "21.4"},
                    {"id": 3, "arr_time": "22.0"},
                ],
                "policies": {
                    "validation_mode": "strict",
                    "sync_mode": "full_refresh_append",
                    "schema_policy": "manual_review",
                },
            }
        )
        sync_plan_mappings(
            plan.id,
            [
                {
                    "source": "id",
                    "target": "id",
                    "source_type": "INTEGER",
                    "target_type": "INT",
                    "confidence": 0.99,
                    "approved": True,
                },
                {
                    "source": "arr_time",
                    "target": "arr_time",
                    # Hostile: Map still holding the declared decimal, as the
                    # last browser run did before restamp/ceiling.
                    "source_type": "DECIMAL(12,9)",
                    "target_type": "INT",
                    "confidence": 0.99,
                    "approved": True,
                },
            ],
        )
        result = run_plan_preflight(plan.id, shape_recipe=ROUND_RECIPE)
        text = _findings(result).casefold()
        blockers = result.get("blockers") or []
        assert not any(
            str(b.get("id") or b.get("gate") or "").casefold() == "schema_drift"
            or "schema drift" in str(b.get("message") or "").casefold()
            or "narrow_type" in str(b).casefold()
            for b in blockers
        ), (blockers, text)
        assert "invalid integer" not in text, text
        assert result.get("passed") is True, (text, result.get("gates"))
        image = result.get("transform_image") or {}
        assert image.get("recipe_hash")
        assert image.get("sample_rows_out") == 3
    finally:
        _drop(src, dst)


def test_case_a_plan_validate_then_execute_sum_is_rounded(isolated_plans) -> None:
    require_ports(5432)
    from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
    from services.transfer_plan_store import create_plan

    src = uniq("case_a_px_src")
    dst = uniq("case_a_px_dst")
    _seed(src, dst)
    recipe_hash = ShapeRecipe.parse(
        ROUND_RECIPE, source_columns=["id", "arr_time"]
    ).recipe_hash
    try:
        plan = create_plan(
            {
                "name": "case-a-validate-execute",
                "source": _pg_endpoint_dict(src),
                "destination": {**_pg_endpoint_dict(dst), "table_exists": True},
                "source_columns": ["id", "arr_time"],
                "source_schema": {"id": "INTEGER", "arr_time": "DECIMAL(12,9)"},
                "target_columns": ["id", "arr_time"],
                "target_schema": {"id": "INT", "arr_time": "INT"},
                "row_count_estimate": 3,
                "sample_rows": [
                    {"id": 1, "arr_time": "22.6"},
                    {"id": 2, "arr_time": "21.4"},
                    {"id": 3, "arr_time": "22.0"},
                ],
                "policies": {
                    "validation_mode": "strict",
                    "sync_mode": "full_refresh_overwrite",
                    "schema_policy": "manual_review",
                },
            }
        )
        sync_plan_mappings(
            plan.id,
            [
                {
                    "source": "id",
                    "target": "id",
                    "target_type": "INT",
                    "confidence": 0.99,
                    "approved": True,
                },
                {
                    "source": "arr_time",
                    "target": "arr_time",
                    "source_type": "DECIMAL(12,9)",
                    "target_type": "INT",
                    "confidence": 0.99,
                    "approved": True,
                },
            ],
        )
        pf = run_plan_preflight(plan.id, shape_recipe=ROUND_RECIPE)
        assert pf.get("passed") is True, _findings(pf)

        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=pg_endpoint(src),
                destination=pg_endpoint(dst),
                mappings=[
                    {
                        "source": "id",
                        "target": "id",
                        "target_type": "INT",
                        "approved": True,
                        "confidence": 0.99,
                    },
                    {
                        "source": "arr_time",
                        "target": "arr_time",
                        "target_type": "INT",
                        "approved": True,
                        "confidence": 0.99,
                    },
                ],
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                shape_recipe=ROUND_RECIPE,
                approved_shape_recipe_hash=recipe_hash,
            ),
            "case_a_pf_exec",
        )
        assert result.success, result.error
        assert result.records_transferred == 3, result.error
        total, values = _sum(dst)
        assert values == [23, 21, 22], values
        assert total == ROUNDED_SUM
        assert total != TRUNCATED_SUM
    finally:
        _drop(src, dst)
