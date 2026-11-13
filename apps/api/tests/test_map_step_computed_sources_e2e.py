"""Map-step computed sources: an expression or a constant feeding a destination column.

The Map step offers "CONCAT(first_name, ' ', last_name) → full_name" and
"constant 'mysql' → source_system" by adding a ``derive_column`` /
``constant_column`` step to the Transform recipe — the one expression engine
(``services.shape_expr``). These tests drive that recipe exactly as Map writes
it, against an EXISTING destination table whose columns are named differently
from the source (order_id → id), and prove:

* Execute writes the computed values and Gate-8 verifies the recipe
  (``transform_applied.status == "verified"``), on a first run and a second.
* Validate refuses an expression naming a column the source does not have,
  naming it, before a row moves.
* Validate type-checks the computed column against the destination: a text
  constant into a BIGINT column is a fidelity blocker, not a silent cast.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

#: Exactly what the Map step's "Add computed column" writes into the recipe.
COMPUTED = {
    "steps": [
        {
            "op": "derive_column",
            "options": {"to": "full_name", "expression": "concat([first_name], ' ', [last_name])"},
        },
        {"op": "constant_column", "options": {"to": "source_system", "value": "mysql"}},
    ]
}


def _seed(dst: str) -> str:
    src = f"mapc_src_{uuid.uuid4().hex[:8]}"
    h.execute(
        "postgresql",
        f'CREATE TABLE public."{src}" (order_id integer PRIMARY KEY, '  # nosec B608
        "first_name text, last_name text, total_balance numeric(12,2))",
    )
    h.execute(
        "postgresql",
        f"INSERT INTO public.\"{src}\" VALUES (1, 'Ada', 'Lovelace', 10.50), "  # nosec B608
        "(2, 'Grace', 'Hopper', 7.25), (3, 'Zoë', '山田', 0)",
    )
    # The destination already exists, with its own names and constraints.
    h.execute(
        "postgresql",
        f'CREATE TABLE public."{dst}" (id bigint PRIMARY KEY, full_name text NOT NULL, '  # nosec B608
        "source_system varchar(20) NOT NULL, amount numeric(12,2), "
        "loaded_at timestamp DEFAULT now())",
    )
    return src


def _mappings() -> list[dict]:
    rows = [
        ("order_id", "id", "BIGINT"),
        ("full_name", "full_name", "TEXT"),
        ("source_system", "source_system", "VARCHAR(20)"),
        ("total_balance", "amount", "DECIMAL(12,2)"),
    ]
    # As Map sends them: approved rows carry user_override (an operator remap,
    # not an engine guess), and the two name parts the computed full_name
    # replaces are declared omits with a reason (G16), never silent drops.
    return [
        {"source": s, "target": t, "target_type": tt, "approved": True, "confidence": 0.99,
         "user_override": True}
        for s, t, tt in rows
    ] + [
        {"source": s, "target": "", "transform": "omit", "intentional_omit": True,
         "omit_reason": "dropped_redundant", "omit_reason_text": "Carried in full_name",
         "confidence": 1.0}
        for s in ("first_name", "last_name")
    ]


def test_computed_sources_write_into_an_existing_table_and_gate8_verifies():
    h.dest_config("postgresql")  # skips when PostgreSQL is absent
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest
    from tests.typed_fidelity_helpers import pg_endpoint

    dst = f"mapc_dst_{uuid.uuid4().hex[:8]}"
    src = _seed(dst)
    try:
        # Append into the table that already exists (an overwrite would empty
        # loaded_at, which nothing writes — Execute rightly refuses that).
        for _run in (1,):
            request = TransferRequest(
                source=pg_endpoint(src),
                destination=pg_endpoint(dst),
                mappings=_mappings(),
                sync_mode="full_refresh_append",
                validation_mode="strict",
                shape_recipe=COMPUTED,
                approved_shape_recipe_hash=ShapeRecipe.parse(COMPUTED).recipe_hash,
                compliance_acknowledged=True,
                acknowledgment_actor="qa@example.com",
                acknowledgment_reason="Customer names are in scope for this migration",
            )
            result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
            assert result.success, result.error
            proof = (result.reconciliation or {}).get("transform_applied") or {}
            assert proof.get("status") == "verified", proof
            assert result.records_transferred == 3
            rows = h.query(
                "postgresql",
                f'SELECT id, full_name, source_system, amount, loaded_at IS NOT NULL '  # nosec B608
                f'FROM public."{dst}" ORDER BY id',
            )
            assert [(r[0], r[1], r[2], str(r[3]), r[4]) for r in rows] == [
                (1, "Ada Lovelace", "mysql", "10.50", True),
                (2, "Grace Hopper", "mysql", "7.25", True),
                (3, "Zoë 山田", "mysql", "0.00", True),
            ]
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_run_start_time_is_one_recorded_value_and_gate8_verifies_it():
    """A ``run_timestamp`` column: every row gets the run's start, the run
    records it, and Gate-8 recomputes the expected rows with that value."""
    h.dest_config("postgresql")  # skips when PostgreSQL is absent
    from services.shape_models import ShapeRecipe
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest
    from tests.typed_fidelity_helpers import pg_endpoint

    recipe = {"steps": COMPUTED["steps"] + [{"op": "run_timestamp", "options": {"to": "stamped_at"}}]}
    dst = f"mapc_dst_{uuid.uuid4().hex[:8]}"
    src = _seed(dst)
    h.execute("postgresql", f'ALTER TABLE public."{dst}" ADD COLUMN stamped_at timestamptz')  # nosec B608
    try:
        mappings = _mappings() + [{
            "source": "stamped_at", "target": "stamped_at", "target_type": "TIMESTAMPTZ",
            "approved": True, "confidence": 0.99, "user_override": True,
        }]
        request = TransferRequest(
            source=pg_endpoint(src),
            destination=pg_endpoint(dst),
            mappings=mappings,
            sync_mode="full_refresh_append",
            validation_mode="strict",
            shape_recipe=recipe,
            approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
            compliance_acknowledged=True,
            acknowledgment_actor="qa@example.com",
            acknowledgment_reason="Customer names are in scope for this migration",
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success, result.error
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "verified", proof
        clock = (result.destination_summary or {}).get("shape_run_clock") or {}
        assert clock.get("source") == "run", sorted(result.destination_summary or {})
        stamps = h.query(
            "postgresql",
            f'SELECT DISTINCT stamped_at FROM public."{dst}"',  # nosec B608
        )
        assert len(stamps) == 1, stamps
        from services.run_clock import as_instant

        assert stamps[0][0] == as_instant(clock["started_at"])
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _validate(client, cfg: dict, src: str, dst: str, recipe: dict, mappings: list[dict]):
    return client.post(
        "/api/v1/preflight/run",
        json={
            "columns": ["order_id", "first_name", "last_name", "total_balance"],
            "column_types": {
                "order_id": "INTEGER", "first_name": "VARCHAR", "last_name": "VARCHAR",
                "total_balance": "DECIMAL(12,2)",
            },
            "row_count": 3,
            "sample_rows": [
                {"order_id": 1, "first_name": "Ada", "last_name": "Lovelace", "total_balance": "10.50"},
                {"order_id": 2, "first_name": "Grace", "last_name": "Hopper", "total_balance": "7.25"},
            ],
            "source_kind": "database",
            "source_type": "postgresql",
            "dest_kind": "database",
            "dest_type": "postgresql",
            "dest_host": cfg["host"],
            "dest_port": cfg["port"],
            "dest_database": cfg["database"],
            "dest_username": cfg["username"],
            "dest_password": cfg["password"],
            "dest_schema": "public",
            "dest_table": dst,
            "sync_mode": "full_refresh_append",
            "mappings": mappings,
            "shape_recipe": recipe,
        },
    )


def test_validate_refuses_an_expression_naming_a_missing_column(client):
    cfg = h.dest_config("postgresql")
    dst = f"mapc_dst_{uuid.uuid4().hex[:8]}"
    src = _seed(dst)
    try:
        bad = {"steps": [{"op": "derive_column", "options": {
            "to": "full_name", "expression": "concat([first_name], ' ', [surname])"}}]}
        r = _validate(client, cfg, src, dst, bad, _mappings()[:2])
        assert r.status_code == 400, r.text
        assert "surname" in r.text
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_validate_type_checks_a_computed_column_against_the_destination(client):
    cfg = h.dest_config("postgresql")
    dst = f"mapc_dst_{uuid.uuid4().hex[:8]}"
    src = _seed(dst)
    try:
        ok = _validate(client, cfg, src, dst, COMPUTED, _mappings())
        assert ok.status_code == 200, ok.text
        image = ok.json().get("transform_image") or {}
        assert "full_name" in (image.get("columns") or []), image

        # A text constant aimed at the BIGINT key: Validate must not pass it.
        wrong = {"steps": [{"op": "constant_column", "options": {"to": "ref", "value": "not-a-number"}}]}
        mappings = [{"source": "ref", "target": "id", "target_type": "BIGINT", "approved": True,
                     "confidence": 0.99}] + _mappings()[1:]
        bad = _validate(client, cfg, src, dst, wrong, mappings)
        assert bad.status_code == 200, bad.text
        body = bad.json()
        assert body.get("passed") is False or body.get("can_execute") is False, body.get("blockers")
        assert any("ref" in str(b) or "id" in str(b) for b in (body.get("blockers") or [])), body.get("blockers")
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_validate_types_run_start_time_as_an_instant(client):
    cfg = h.dest_config("postgresql")
    dst = f"mapc_dst_{uuid.uuid4().hex[:8]}"
    src = _seed(dst)
    h.execute("postgresql", f'ALTER TABLE public."{dst}" ADD COLUMN stamped_at timestamptz')  # nosec B608
    try:
        recipe = {"steps": COMPUTED["steps"] + [{"op": "run_timestamp", "options": {"to": "stamped_at"}}]}
        mappings = _mappings() + [{"source": "stamped_at", "target": "stamped_at", "target_type": "TIMESTAMPTZ",
                                   "approved": True, "confidence": 0.99, "user_override": True}]
        res = _validate(client, cfg, src, dst, recipe, mappings)
        assert res.status_code == 200, res.text
        image = res.json().get("transform_image") or {}
        assert "stamped_at" in (image.get("columns") or []), image
        blockers = [str(b) for b in (res.json().get("blockers") or [])]
        assert not any("stamped_at" in b for b in blockers), blockers
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)
