"""Shape's design-time surface: profile, suggest, preview, and refuse.

The suggestions are the product claim — a recipe an operator accepts rather than
authors — so each one is pinned to the evidence that produced it. The narrowing
decimal case is the same failure that aborted a 1M-row Snowflake load at row 431:
here it must appear as a *blocking* suggestion carrying the exact scale the
carrier allows and the number of sampled rows that exceed it, before the run.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.rbac import Permission, _required_permission  # noqa: E402
from services.shape_suggest import profile_columns, suggest_steps  # noqa: E402
from src.routers.shape_router import MAX_PREVIEW_ROWS, router  # noqa: E402


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _suggestion(suggestions: list[dict], op: str, column: str) -> dict | None:
    for item in suggestions:
        if item["step"]["op"] == op and item["step"]["column"] == column:
            return item
    return None


# ---------------------------------------------------------------------------
# Profiling and suggestions
# ---------------------------------------------------------------------------


def test_a_narrowing_decimal_is_a_blocking_suggestion_with_the_carriers_scale():
    rows = [{"arr_time": "1.5"}, {"arr_time": "1.123456789"}, {"arr_time": "2.987654321"}]
    profiles = profile_columns(rows)
    suggestions = suggest_steps(profiles, target_schema={"arr_time": "NUMBER(11,8)"})

    found = _suggestion(suggestions, "round_number", "arr_time")
    assert found is not None
    assert found["severity"] == "blocking"
    assert found["step"]["options"] == {"places": 8}
    # Two of the three values carry scale 9 — counted, not estimated.
    assert found["rows_affected"] == 2
    assert suggestions[0] is found  # blocking sorts before hygiene


def test_a_decimal_that_already_fits_is_not_suggested():
    rows = [{"amount": "1.50"}, {"amount": "2.25"}]
    suggestions = suggest_steps(
        profile_columns(rows), target_schema={"amount": "NUMERIC(12,4)"}
    )
    assert _suggestion(suggestions, "round_number", "amount") is None


def test_informal_yes_no_is_not_profiled_as_boolean():
    informal = profile_columns([{"flag": "yes"}, {"flag": "no"}])[0]
    assert informal.logical_type != "boolean"
    canonical = profile_columns([{"flag": "true"}, {"flag": "false"}])[0]
    assert canonical.logical_type == "boolean"


def test_whitespace_and_sentinels_are_found_and_counted():
    rows = [
        {"city": " Paris "},
        {"city": "New  York"},
        {"city": "N/A"},
        {"city": "Berlin"},
    ]
    profiles = profile_columns(rows)
    suggestions = suggest_steps(profiles)

    trim = _suggestion(suggestions, "trim", "city")
    collapse = _suggestion(suggestions, "collapse_whitespace", "city")
    nullify = _suggestion(suggestions, "null_if", "city")
    assert trim is not None and trim["rows_affected"] == 1
    assert collapse is not None and collapse["rows_affected"] == 1
    assert nullify is not None and nullify["step"]["options"] == {"values": ["N/A"]}


def test_an_ambiguous_date_order_is_a_decision_not_a_guess():
    rows = [{"d": "01/02/2024"}, {"d": "03/04/2024"}]
    profiles = profile_columns(rows)
    found = _suggestion(suggest_steps(profiles), "parse_date", "d")
    assert found is not None
    assert found["severity"] == "decision"
    assert profiles[0].ambiguous_date_order is True


def test_an_unambiguous_date_names_the_only_format_that_fits():
    rows = [{"d": "2024-02-29"}, {"d": "2023-12-31"}]
    profiles = profile_columns(rows)
    found = _suggestion(suggest_steps(profiles), "parse_date", "d")
    assert found is not None
    assert found["severity"] == "hygiene"
    assert found["step"]["options"] == {"format": "%Y-%m-%d"}


def test_a_human_written_number_is_offered_a_parse_step():
    rows = [{"amount": "1,234.50"}, {"amount": "(9.99)"}, {"amount": "$45"}]
    profiles = profile_columns(rows)
    found = _suggestion(suggest_steps(profiles), "parse_number", "amount")
    assert found is not None
    assert found["rows_affected"] == 3
    assert profiles[0].logical_type == "decimal"


def test_a_plain_decimal_column_is_not_told_to_parse_itself():
    profiles = profile_columns([{"amount": "1.50"}, {"amount": "-2"}])
    assert _suggestion(suggest_steps(profiles), "parse_number", "amount") is None


def test_auto_ambiguous_dot_group_is_not_already_numeric():
    """Decimal('1.234') succeeds; the write path refuses. Do not invent decimal."""
    from services.transform_engine import decimal_wire_value

    assert decimal_wire_value("1.234") is None
    assert decimal_wire_value("1.000") is None
    profiles = profile_columns([{"amount": "1.234"}, {"amount": "1.000"}])
    assert profiles[0].numeric_like == 0
    assert profiles[0].logical_type == "text"
    assert profiles[0].needs_parse_number == 2
    # parse_number cannot bind these under Auto — do not offer a dead CTA.
    assert _suggestion(suggest_steps(profiles), "parse_number", "amount") is None


def test_bindable_scale_longer_than_three_stays_plain_decimal():
    profiles = profile_columns([{"amount": "1.2345"}, {"amount": "1.23"}])
    assert profiles[0].numeric_like == 2
    assert profiles[0].logical_type == "decimal"
    assert _suggestion(suggest_steps(profiles), "parse_number", "amount") is None


def test_blanks_and_declared_columns_survive_a_column_missing_from_every_row():
    profiles = profile_columns([{"a": 1}], columns=["a", "b"])
    by_name = {p.name: p for p in profiles}
    assert by_name["b"].rows == 1
    assert by_name["b"].blanks == 1
    assert by_name["b"].logical_type == "empty"


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------


def test_the_catalog_states_the_operations_the_engine_will_accept(client):
    body = client.get("/api/v1/shape/catalog").json()
    ops = {op["op"] for op in body["operations"]}
    assert {"trim", "round_number", "derive_column", "filter_rows"} <= ops
    assert "join" not in ops
    assert "join" in body["post_load_only"]["operations"]
    assert {p["value"] for p in body["error_policies"]} == {"refuse", "divert", "null"}
    assert body["max_steps"] > 0


def test_catalog_does_not_advertise_yn_boolean_or_iso_only_dates():
    """Operators used to be told parse_boolean accepts Y/N; Execute refuses it."""
    from services.shape_expr import describe_functions
    from services.shape_models import describe_catalog

    ops = {row["op"]: row["summary"] for row in describe_catalog()}
    assert "Y/N" not in ops["parse_boolean"]
    assert "yes" in ops["parse_boolean"]
    assert "refuse" in ops["parse_boolean"]
    assert "write-path" in ops["parse_date"]
    fns = {row["name"]: row["summary"] for row in describe_functions()}
    assert "Y/N" not in fns["to_boolean"]
    assert "refuse" in fns["to_boolean"]
    assert "write-path" in fns["to_date"]


def test_a_preview_reports_the_value_it_changed_and_the_cell_it_changed_it_in(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"city": " Paris "}, {"city": "Berlin"}],
            "source_columns": ["city"],
            "recipe": {"steps": [{"op": "trim", "column": "city"}]},
        },
    ).json()

    assert body["after"] == [{"city": "Paris"}, {"city": "Berlin"}]
    assert body["effect"]["cells_changed"] == 1
    assert body["effect"]["rows_out"] == 2
    assert body["changed_cells"] == [{"row": 0, "column": "city", "kind": "changed"}]
    assert body["recipe"]["recipe_hash"]


def test_a_preview_keeps_a_decimal_exact_on_the_wire(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"n": "1.0050"}],
            "source_columns": ["n"],
            "recipe": {"steps": [{"op": "round_number", "column": "n", "options": {"places": 2}}]},
        },
    ).json()
    # 1.01, not 1.0 — the value is carried as text, never through a float.
    assert body["after"] == [{"n": "1.01"}]
    assert Decimal(body["after"][0]["n"]) == Decimal("1.01")


def test_a_preview_reports_the_carriers_map_must_decide_from(client):
    """A rounded decimal is no longer a decimal, and Map has to be told so.

    The carrier of a column the recipe wrote is re-read from the transformed
    values; a column no step touched keeps the type the catalog declared, so a
    12-row sample cannot demote an introspected DECIMAL(12,2) to DECIMAL(4,1).
    """
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"arr_time": "22.43", "code": "AA"}, {"arr_time": "21.05", "code": "BB"}],
            "source_columns": ["arr_time", "code"],
            "column_types": {"arr_time": "DECIMAL(12,8)", "code": "VARCHAR(64)"},
            "recipe": {
                "steps": [
                    {"op": "round_number", "column": "arr_time", "options": {"places": 0}}
                ]
            },
        },
    ).json()

    assert body["after"] == [{"arr_time": "22", "code": "AA"}, {"arr_time": "21", "code": "BB"}]
    # Written by the recipe → re-read from what it produced.
    assert body["retyped_columns"] == {"arr_time": body["column_types"]["arr_time"]}
    assert "INT" in body["column_types"]["arr_time"].upper()
    # Untouched → declared truth survives the sample.
    assert body["column_types"]["code"] == "VARCHAR(64)"


def test_a_preview_without_declared_types_claims_no_retyping(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"city": " Paris "}],
            "source_columns": ["city"],
            "recipe": {"steps": [{"op": "trim", "column": "city"}]},
        },
    ).json()
    # With nothing declared there is no carrier to have changed, so the preview
    # reads one for Map without claiming the transform retyped anything.
    assert set(body["column_types"]) == {"city"}
    assert body["retyped_columns"] == {}


def test_a_filtered_row_is_reported_as_shaped_out_not_as_a_finding(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"status": "ok"}, {"status": "void"}],
            "source_columns": ["status"],
            "recipe": {
                "steps": [
                    {
                        "op": "filter_rows",
                        "options": {"condition": "[status] <> 'void'"},
                    }
                ]
            },
        },
    ).json()
    assert body["effect"]["rows_shaped_out"] == 1
    assert body["effect"]["rows_diverted"] == 0
    assert body["effect"]["balanced"] is True
    assert body["after"] == [{"status": "ok"}]
    # Row counts moved, so cell highlighting is withheld rather than misaligned.
    assert body["changed_cells"] == []


def test_a_refusing_row_stops_the_preview_and_names_the_row(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={
            "sample_rows": [{"n": "12"}, {"n": "not a number"}],
            "source_columns": ["n"],
            "recipe": {"steps": [{"op": "parse_number", "column": "n"}]},
        },
    ).json()
    refusal = body["refusal"]
    assert refusal is not None
    assert refusal["row"] == 2  # rows are numbered as the operator counts them
    assert refusal["column"] == "n"
    assert refusal["op"] == "parse_number"


def test_a_global_operation_is_refused_with_the_place_that_does_it(client):
    response = client.post(
        "/api/v1/shape/validate",
        json={"recipe": {"steps": [{"op": "dedupe", "column": "id"}]}, "source_columns": ["id"]},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "dedupe" in detail
    assert "Transforms" in detail or "post-load" in detail


def test_a_step_on_a_column_dropped_earlier_is_refused_at_design_time(client):
    response = client.post(
        "/api/v1/shape/validate",
        json={
            "recipe": {
                "steps": [
                    {"op": "drop_column", "column": "city"},
                    {"op": "trim", "column": "city"},
                ]
            },
            "source_columns": ["city", "id"],
        },
    )
    assert response.status_code == 400
    assert "city" in response.json()["detail"]


def test_validate_returns_the_identity_execute_will_be_held_to(client):
    payload = {
        "recipe": {"steps": [{"op": "trim", "column": "city"}]},
        "source_columns": ["city"],
    }
    first = client.post("/api/v1/shape/validate", json=payload).json()
    spaced = {
        "recipe": {"steps": [{"op": "trim", "column": "city", "label": "tidy city"}]},
        "source_columns": ["city"],
    }
    second = client.post("/api/v1/shape/validate", json=spaced).json()

    assert first["recipe_hash"] == second["recipe_hash"]  # a label is not meaning
    assert first["output_columns"] == ["city"]
    assert first["has_active_step"] is False


def test_an_expression_is_checked_as_it_is_typed(client):
    good = client.post(
        "/api/v1/shape/expression",
        json={"expression": "[qty] * [price]", "source_columns": ["qty", "price"]},
    ).json()
    assert good == {"valid": True, "columns": ["price", "qty"]}

    bad = client.post(
        "/api/v1/shape/expression",
        json={"expression": "[qty] * [nope]", "source_columns": ["qty", "price"]},
    ).json()
    assert bad["valid"] is False
    assert "nope" in bad["error"]


def test_a_preview_refuses_more_rows_than_it_will_hold(client):
    rows = [{"n": 1}] * (MAX_PREVIEW_ROWS + 1)
    response = client.post(
        "/api/v1/shape/preview", json={"sample_rows": rows, "recipe": {"steps": []}}
    )
    assert response.status_code == 400
    assert str(MAX_PREVIEW_ROWS) in response.json()["detail"]


def test_an_empty_recipe_returns_the_rows_unchanged(client):
    body = client.post(
        "/api/v1/shape/preview",
        json={"sample_rows": [{"a": "1"}], "recipe": {"steps": []}},
    ).json()
    assert body["before"] == body["after"] == [{"a": "1"}]
    assert body["recipe"]["has_active_step"] is False
    assert body["effect"]["cells_changed"] == 0


# ---------------------------------------------------------------------------
# Authority
# ---------------------------------------------------------------------------


def test_reading_the_vocabulary_is_a_read_and_designing_a_recipe_is_planning():
    assert _required_permission("GET", "/api/v1/shape/catalog") == Permission.JOB_READ
    # Profiling describes rows the caller sent and mints no identity, so a viewer
    # told it may inspect the step actually sees the findings.
    assert _required_permission("POST", "/api/v1/shape/profile") == Permission.JOB_READ
    assert _required_permission("POST", "/api/v1/shape/preview") == Permission.JOB_PLAN
    assert _required_permission("POST", "/api/v1/shape/validate") == Permission.JOB_PLAN
