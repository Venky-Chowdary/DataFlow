"""The wizard's Validate is the plan-scoped call, so that is where the recipe lands.

`POST /api/v1/preflight/run` got the transformed-image fix first, but Transfer
Studio validates through `POST /api/v1/transfer/plans/{id}/preflight`. Until this
path shaped the image too, an operator with an approved `strip_characters` step
still watched Validate block on the control character that step removes — the fix
was wired to an endpoint the UI never calls.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.shape_preflight import ShapePreflightRefused
from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
from services.transfer_plan_store import create_plan

_STRIP_CONTROLS = {
    "steps": [
        {
            "op": "strip_characters",
            "column": "name",
            "options": {"characters": "non_printable"},
        }
    ]
}


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json"
    )
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def _make_plan() -> str:
    plan = create_plan(
        {
            "name": "csv-pg",
            "source": {"kind": "file", "format": "csv"},
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": "dst",
                "table": "people",
            },
            "source_columns": ["name", "arr_time"],
            "source_schema": {"name": "VARCHAR(64)", "arr_time": "DECIMAL(12,9)"},
            "target_columns": ["name", "arr_time"],
            "target_schema": {"name": "VARCHAR(64)", "arr_time": "INTEGER"},
            "sample_rows": [
                {"name": "AC\u0001ME Corp", "arr_time": "22.433332"},
                {"name": "Globex", "arr_time": "21.833334"},
            ],
            "policies": {"validation_mode": "strict"},
        }
    )
    sync_plan_mappings(
        plan.id,
        [
            {"source": "name", "target": "name", "confidence": 0.99},
            {"source": "arr_time", "target": "arr_time", "confidence": 0.99},
        ],
    )
    return plan.id


def _run(plan_id: str, **kwargs) -> tuple[dict, dict]:
    """Return (what the gates were asked, what the caller was told)."""
    captured: dict = {}

    def fake_run_file_preflight(**kw):
        captured.update(kw)
        return {"passed": True, "gates": [], "blockers": []}

    with patch("services.transfer_plan_service._preflight") as mock_pf, patch(
        "services.transfer_plan_service.read_source_database",
        side_effect=Exception("skip"),
    ):
        mock_pf.return_value = (
            lambda pf, *_a, **_k: pf,
            lambda mode: 0.85,
            lambda **_k: {
                "connected": True,
                "table_exists": True,
                "can_create_table": True,
                "db_type": "postgresql",
                "column_types": {"name": "VARCHAR(64)", "arr_time": "INTEGER"},
                "message": "ok",
            },
            fake_run_file_preflight,
            lambda **_k: [],
        )
        result = run_plan_preflight(plan_id, **kwargs)
    return captured, result


def test_without_a_recipe_the_gates_still_see_the_raw_source() -> None:
    captured, result = _run(_make_plan())
    assert captured["sample_rows"][0]["name"] == "AC\u0001ME Corp"
    assert captured["column_types"]["arr_time"] == "DECIMAL(12,9)"
    assert "transform_image" not in result


def test_the_approved_recipe_shapes_the_image_the_gates_judge() -> None:
    captured, result = _run(_make_plan(), shape_recipe=_STRIP_CONTROLS)

    # The control character the step removes never reaches a gate.
    assert captured["sample_rows"][0]["name"] == "ACME Corp"
    assert all("\u0001" not in str(row["name"]) for row in captured["sample_rows"])

    image = result["transform_image"]
    assert image["recipe_hash"]
    assert image["sample_rows_in"] == 2
    assert image["sample_rows_out"] == 2
    assert image["sample_rows_removed"] == 0
    assert any("applied before the gates" in w for w in result["warnings"])


def test_a_row_the_recipe_removes_is_not_scored_and_is_counted() -> None:
    recipe = {
        "steps": [
            {"op": "filter_rows", "options": {"condition": "[name] <> 'Globex'"}}
        ]
    }
    captured, result = _run(_make_plan(), shape_recipe=recipe)
    assert [row["name"] for row in captured["sample_rows"]] == ["AC\u0001ME Corp"]
    assert result["transform_image"]["sample_rows_removed"] == 1
    assert any("removed by transform" in w for w in result["warnings"])


def test_a_recipe_that_cannot_run_refuses_instead_of_passing() -> None:
    """Fail closed: Validate never scores a program Execute would abort on."""
    with pytest.raises(ShapePreflightRefused):
        _run(
            _make_plan(),
            shape_recipe={
                "steps": [
                    {
                        "op": "strip_characters",
                        "column": "no_such_column",
                        "options": {"characters": "non_printable"},
                    }
                ]
            },
        )


def test_the_drift_contract_still_reads_the_declared_source() -> None:
    """An approved recipe is not source drift.

    The mapping revision fingerprints the *declared* source. Handing the
    transformed image to the drift detector made the operator's own retyped
    column read as "source schema changed since last mapping revision", which
    blocked Validate on the very transform it had just approved.
    """
    captured, _ = _run(
        _make_plan(),
        shape_recipe={
            "steps": [
                {"op": "round_number", "column": "arr_time", "options": {"places": 0}}
            ]
        },
    )
    # Gates judge the transformed image…
    assert captured["column_types"]["arr_time"] != "DECIMAL(12,9)"
    # …while drift compares the declared source the revision was signed on.
    assert captured["declared_source_columns"] == ["name", "arr_time"]
    assert captured["declared_source_schema"]["arr_time"] == "DECIMAL(12,9)"


def test_an_empty_recipe_is_not_an_identity() -> None:
    """A step-less Transform must leave Validate exactly as it was."""
    captured, result = _run(_make_plan(), shape_recipe={"steps": []})
    assert captured["sample_rows"][0]["name"] == "AC\u0001ME Corp"
    assert "transform_image" not in result
