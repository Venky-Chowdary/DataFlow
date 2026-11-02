"""D243 — a pre-load recipe on a history-merging route (cdc / mirror) passed
preflight green (safe_to_start=true) and execute then refused it with
"Transform (pre-load) is not applied on the cdc route" — 0 rows on a new,
empty destination. Preflight must refuse the same recipe, with the same text.
"""
from __future__ import annotations

import pytest

_RECIPE = {
    "steps": [
        {"op": "datetime", "column": "ts", "enabled": True},
        {"op": "assume_timezone", "column": "ts", "tz": "UTC"},
    ]
}


def _preflight(sync_mode, shape_recipe):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["id", "ts"],
        column_types={"id": "INTEGER", "ts": "TEXT"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "target_type": "INTEGER"},
            {"source": "ts", "target": "ts", "confidence": 0.99, "target_type": "TEXT"},
        ],
        destination_connected=True,
        sample_rows=[{"id": 1, "ts": "2024-03-10T02:30:00"}, {"id": 2, "ts": None}],
        destination_column_types={"id": "INTEGER", "ts": "TEXT"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
        sync_mode=sync_mode,
        shape_recipe=shape_recipe,
    )


def _shape_blockers(result):
    return [b for b in result.get("blockers") or [] if b.get("id") == "g_shape_route"]


@pytest.mark.parametrize("sync_mode", ["cdc", "mirror", "full_refresh_mirror"])
def test_recipe_on_history_route_blocks_preflight_with_execute_text(sync_mode):
    from services.multi_stream_plan import history_shape_refusal
    from services.sync_cursor import normalize_sync_mode

    result = _preflight(sync_mode, _RECIPE)
    assert result["passed"] is False
    blockers = _shape_blockers(result)
    assert blockers, result.get("blockers")
    assert blockers[0]["message"] == history_shape_refusal(normalize_sync_mode(sync_mode))


@pytest.mark.parametrize("sync_mode", ["full_refresh_append", "incremental_append"])
def test_recipe_on_read_side_route_is_not_refused(sync_mode):
    assert not _shape_blockers(_preflight(sync_mode, _RECIPE))


def test_disabled_steps_do_not_refuse_cdc():
    recipe = {"steps": [dict(s, enabled=False) for s in _RECIPE["steps"]]}
    assert not _shape_blockers(_preflight("cdc", recipe))


def test_execute_refusal_text_is_the_shared_helper():
    from services.multi_stream_plan import history_shape_refusal
    from src.transfer.engine_shape import _shape_stream_refusal

    class _R:
        recipe = None

    assert _shape_stream_refusal(
        _R(), effective_sync="cdc", multi_stream=False, cursor_field="", key_columns=()
    ) == history_shape_refusal("cdc")
