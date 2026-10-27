"""C19 — schema drift: additive drift must be propagatable, the diff must
run in the direction of the load, and a removed column is surfaced.

- D20 (HIGH): the tool contract says "Accept additive schema drift …
  Hard-breaking changes still block" but ``schema_policy`` rejected every
  propagate policy, so ``ALTER TABLE ADD COLUMN`` could never be approved.
  propagate_columns is now reachable by explicit request; propagate_all
  stays unreachable from chat (it silently rewrites dest types).
- D43: ``diff_schemas`` judged type changes destination→source, calling
  DataFlow's own widening and documented carriers "breaking". Type items
  are now re-evaluated source→destination.
- D21-class: a removed source column now names itself in the drift issues
  instead of passing as a generic "schema changed" note.
"""

import pytest


def test_propagate_columns_reachable_in_tool_enums():
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    found = 0
    for tool in TOOL_DEFINITIONS:
        props = (tool.get("input_schema") or {}).get("properties") or {}
        enum = (props.get("schema_policy") or {}).get("enum")
        if enum:
            found += 1
            assert "propagate_columns" in enum, tool["name"]
    assert found >= 4  # plan_transfer, plan_transfer_route, start_transfer, update_schedule…


def test_propagate_columns_survives_posture_parse():
    from src.ai.copilot.tools import resolve_transfer_bind_kwargs

    out = resolve_transfer_bind_kwargs(schema_policy="propagate_columns")
    assert out["schema_policy"] == "propagate_columns"


def test_spoken_propagate_phrases_resolve():
    from src.ai.copilot.tools import parse_transfer_schema_posture

    assert parse_transfer_schema_posture(
        "accept additive drift and add new columns automatically"
    ) == {"schema_policy": "propagate_columns"}
    assert parse_transfer_schema_posture(
        "propagate columns for new fields"
    ) == {"schema_policy": "propagate_columns"}
    # Vague phrasing must never upgrade the posture.
    assert "schema_policy" not in parse_transfer_schema_posture("just make it work")


def test_propagate_columns_enables_backfill():
    from services.batch_progress import effective_backfill_new_fields

    assert effective_backfill_new_fields(schema_policy="propagate_columns")
    assert not effective_backfill_new_fields(schema_policy="manual_review")


def test_removed_source_column_surfaced_by_name():
    from services.schema_drift import detect_schema_drift

    drift = detect_schema_drift(
        source_columns=["id", "name"],
        source_schema={"id": "INT4", "name": "TEXT"},
        target_columns=["id", "name", "extra"],
        target_schema={"id": "INT4", "name": "TEXT", "extra": "TEXT"},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
            {"source": "extra", "target": "extra"},
        ],
        previous_source_columns=["id", "name", "extra"],
        previous_source_schema={"id": "INT4", "name": "TEXT", "extra": "TEXT"},
        schema_policy="manual_review",
    )
    text = " ".join(drift.get("issues") or [])
    assert "extra" in text, text
    assert "removed" in text.lower() or "renamed" in text.lower()


def _fake_introspect(schema_map, columns, type_):
    class _Res:
        def __init__(self):
            self.success = True
            self.output = {
                "connector_name": "fake",
                "schema_map": dict(schema_map),
                "columns": columns,
                "type": type_,
            }
    return _Res()


def test_diff_schemas_load_direction(monkeypatch):
    """Dest widened INT4→BIGINT is compatible, not breaking (D43)."""
    from src.ai.copilot import schema_tools

    src_cols = [{"name": "id", "nullable": False}]
    dst_cols = [{"name": "id", "nullable": False}]
    calls = iter([
        _fake_introspect({"id": "INT4"}, src_cols, "postgresql"),
        _fake_introspect({"id": "BIGINT"}, dst_cols, "postgresql"),
    ])
    monkeypatch.setattr(schema_tools, "introspect_connector_schema", lambda *a, **k: next(calls))
    res = schema_tools.diff_schemas(
        source_connector_name="s", source_table="t",
        dest_connector_name="d", dest_table="t",
    )
    assert res.success
    body = res.output
    assert body["severity"] != "breaking"
    assert not [b for b in body["breaking"] if b.get("column") == "id"]
    assert any(w["column"] == "id" for w in body["dest_carries_source"])
    # Nothing to add: severity must agree with the empty additive list.
    assert body["additive"] == []
    assert body["severity"] == "compatible"


def test_diff_schemas_dest_narrower_is_breaking(monkeypatch):
    """Source BIGINT → dest INT4 loses range: that direction IS breaking."""
    from src.ai.copilot import schema_tools

    calls = iter([
        _fake_introspect({"id": "BIGINT"}, [{"name": "id"}], "postgresql"),
        _fake_introspect({"id": "INT4"}, [{"name": "id"}], "postgresql"),
    ])
    monkeypatch.setattr(schema_tools, "introspect_connector_schema", lambda *a, **k: next(calls))
    res = schema_tools.diff_schemas(
        source_connector_name="s", source_table="t",
        dest_connector_name="d", dest_table="t",
    )
    assert res.success
    body = res.output
    assert body["severity"] == "breaking"
    assert any(b.get("column") == "id" for b in body["breaking"])
