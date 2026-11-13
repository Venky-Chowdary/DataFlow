"""Mapping-as-code (export/import) and workspace mapping templates.

A mapping is a reviewable artefact: export it as YAML or JSON with a schema
version and a checksum, keep it in Git, import it into another route. A
template is the same document saved in the workspace and re-bound onto a
table whose column names differ (fuzzy, by the auto-map tiers).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))
_API = _TESTS.parent
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.mapping_document import (  # noqa: E402
    DOCUMENT_API_VERSION,
    MappingDocumentError,
    build_mapping_document,
    dump_mapping_document,
    load_mapping_document,
)
from services.mapping_templates import rebind_template_pairs  # noqa: E402
from tenancy_world import tenancy_world  # noqa: E402,F401

BASE = "/api/v1/transfer/mapping"

MAPPINGS = [
    {"source": "order_id", "target": "legacy_ref", "target_type": "BIGINT", "transform": "none"},
    {"source": "full_name", "target": "full_name", "target_type": "TEXT"},
    {"source": "first_name", "target": "", "transform": "omit", "intentional_omit": True,
     "omit_reason": "dropped_redundant", "omit_reason_text": "Carried in full_name"},
]
COMPUTED = [
    {"op": "derive_column", "options": {"to": "full_name", "expression": "concat(first_name, ' ', last_name)"}},
    {"op": "constant_column", "options": {"to": "source_system", "value": "mysql"}},
]


def _doc(**extra):
    return build_mapping_document(
        mappings=MAPPINGS, computed=COMPUTED, name="orders → customers",
        source={"table": "orders", "type": "mysql"},
        destination={"table": "customers", "type": "postgresql"}, **extra,
    )


@pytest.mark.parametrize("fmt", ["yaml", "json"])
def test_export_import_round_trips_exactly(fmt):
    doc = _doc()
    text = dump_mapping_document(doc, fmt=fmt)
    back = load_mapping_document(text)
    assert back["apiVersion"] == DOCUMENT_API_VERSION
    assert back["checksum"] == doc["checksum"]
    assert back["spec"]["mappings"] == doc["spec"]["mappings"]
    assert back["spec"]["computed"] == COMPUTED
    # Re-exporting the import gives the same bytes: a stable artefact for Git.
    assert dump_mapping_document(back, fmt=fmt) == text


def test_checksum_is_over_the_content_not_the_timestamp():
    a, b = _doc(), _doc()
    assert a["checksum"] == b["checksum"]
    assert a["checksum"].startswith("sha256:")


def test_a_hand_edit_without_a_new_checksum_is_refused():
    text = dump_mapping_document(_doc(), fmt="yaml").replace("legacy_ref", "legacy_id")
    with pytest.raises(MappingDocumentError, match="checksum"):
        load_mapping_document(text)


def test_an_unknown_version_is_refused_with_the_supported_one_named():
    text = dump_mapping_document(_doc(), fmt="json").replace(DOCUMENT_API_VERSION, "datawrap.io/mapping/v9")
    with pytest.raises(MappingDocumentError, match="v1"):
        load_mapping_document(text)


@pytest.mark.parametrize(
    "bad",
    ["", "{not json", "- just\n- a list\n", "apiVersion: x\n" * 1],
)
def test_malformed_documents_are_refused_not_crashed(bad):
    with pytest.raises(MappingDocumentError):
        load_mapping_document(bad)


def test_yaml_is_loaded_safely():
    evil = "!!python/object/apply:os.system ['echo pwned']\n"
    with pytest.raises(MappingDocumentError):
        load_mapping_document(evil)


def test_a_computed_step_must_parse_in_the_shape_engine():
    with pytest.raises(MappingDocumentError, match="computed"):
        build_mapping_document(
            mappings=MAPPINGS,
            computed=[{"op": "derive_column", "options": {"to": "x", "expression": "now()"}}],
        )


def test_template_rebinds_onto_a_table_with_different_names():
    pairs = [("order_id", "legacy_ref"), ("first_name", "given_name"), ("customer_email", "email_address")]
    out = rebind_template_pairs(
        pairs,
        source_columns=["OrderID", "firstName", "email"],
        destination_columns=["legacy_ref", "given_name", "contact_email"],
    )
    got = {s["source"]: s["target"] for s in out["suggestions"]}
    assert got == {"OrderID": "legacy_ref", "firstName": "given_name", "email": "contact_email"}
    assert all(s["kind"] == "template" for s in out["suggestions"])
    assert out["unbound"] == []


def test_template_reports_what_it_cannot_rebind():
    out = rebind_template_pairs(
        [("order_id", "legacy_ref"), ("tax_code", "tax_ref")],
        source_columns=["order_id"],
        destination_columns=["legacy_ref", "tax_ref"],
    )
    assert [s["source"] for s in out["suggestions"]] == ["order_id"]
    assert out["unbound"] == [{"source": "tax_code", "target": "tax_ref", "missing": "source"}]


# --------------------------------------------------------------------------- routes


def test_export_and_import_routes_round_trip(tenancy_world):
    a = tenancy_world.a
    r = a.post(f"{BASE}/export", json={"mappings": MAPPINGS, "computed": COMPUTED, "format": "yaml",
                                        "name": "orders", "destination": {"table": "customers"}})
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("application/yaml")
    assert "attachment" in r.headers.get("content-disposition", "")
    imported = a.post(f"{BASE}/import", json={"document": r.text})
    assert imported.status_code == 200, imported.text
    assert imported.json()["mappings"][0]["target"] == "legacy_ref"
    tampered = a.post(f"{BASE}/import", json={"document": r.text.replace("legacy_ref", "x_ref")})
    assert tampered.status_code == 422
    assert "checksum" in tampered.json()["detail"]


def test_templates_are_workspace_scoped(tenancy_world):
    w = tenancy_world
    made = w.a.post(f"{BASE}/templates", json={"name": "Orders onto customers", "mappings": MAPPINGS,
                                                "computed": COMPUTED})
    assert made.status_code == 201, made.text
    tid = made.json()["id"]
    assert [t["id"] for t in w.a.get(f"{BASE}/templates").json()["templates"]] == [tid]
    assert w.b.get(f"{BASE}/templates").json()["templates"] == []
    assert w.b.get(f"{BASE}/templates/{tid}").status_code == 404
    assert w.b.post(f"{BASE}/templates/{tid}/apply", json={"source_columns": ["order_id"],
                                                            "destination_columns": ["legacy_ref"]}).status_code == 404
    assert w.b.delete(f"{BASE}/templates/{tid}").status_code == 404
    applied = w.a.post(f"{BASE}/templates/{tid}/apply", json={
        "source_columns": ["OrderId", "full_name"], "destination_columns": ["legacy_ref", "full_name"]})
    assert applied.status_code == 200, applied.text
    assert {s["source"]: s["target"] for s in applied.json()["suggestions"]} == {
        "OrderId": "legacy_ref", "full_name": "full_name"}
    assert applied.json()["computed"] == COMPUTED
    assert w.a.delete(f"{BASE}/templates/{tid}").status_code == 200
    assert w.a.get(f"{BASE}/templates/{tid}").status_code == 404


def test_viewer_can_list_but_not_save_templates(tenancy_world):
    viewer = tenancy_world.member("viewer-tpl@tenancy.example.com", tenancy_world.workspace_a, "viewer")
    assert viewer.get(f"{BASE}/templates").status_code == 200
    assert viewer.post(f"{BASE}/templates", json={"name": "x", "mappings": MAPPINGS}).status_code == 403


# --------------------------------------------------------------------------- H1: resource bombs


def _billion_laughs(levels: int = 9) -> str:
    lines = ['a0: &a0 ["lol","lol","lol","lol","lol","lol","lol","lol","lol"]']
    for i in range(1, levels):
        prev = f"*a{i - 1}"
        lines.append(f"a{i}: &a{i} [{', '.join([prev] * 9)}]")
    return "\n".join(lines) + f"\napiVersion: datawrap.io/mapping/v1\nkind: ColumnMapping\nspec: {{mappings: [{{source: x, target: *a{levels - 1}}}]}}\n"


def test_a_yaml_alias_bomb_is_refused_fast_without_expanding():
    import time

    doc = _billion_laughs()
    assert len(doc) < 1200
    started = time.perf_counter()
    with pytest.raises(MappingDocumentError, match="alias|anchor"):
        load_mapping_document(doc)
    assert time.perf_counter() - started < 1.0


def test_any_yaml_anchor_is_refused():
    doc = "x: &a 1\ny: *a\napiVersion: datawrap.io/mapping/v1\n"
    with pytest.raises(MappingDocumentError, match="alias|anchor"):
        load_mapping_document(doc)


@pytest.mark.parametrize(
    "text",
    ["[" * 5000, "{" + "a: {" * 3000, "[" * 100000, '{"a":' * 100000],
    ids=["yaml_flow_5000", "yaml_map_3000", "json_list_100000", "json_object_100000"],
)
def test_deep_nesting_is_a_clean_refusal_not_a_recursion_error(text):
    with pytest.raises(MappingDocumentError, match="nest"):
        load_mapping_document(text)


def test_a_document_with_too_many_nodes_is_refused():
    body = json.dumps({"apiVersion": DOCUMENT_API_VERSION, "kind": "ColumnMapping",
                       "spec": {"mappings": [], "pad": [0] * 300_000}})
    with pytest.raises(MappingDocumentError, match="too many"):
        load_mapping_document(body)


def test_non_text_mapping_fields_are_refused_not_stringified():
    deep = {"x": 1}
    for _ in range(2000):
        deep = {"x": deep}
    with pytest.raises(MappingDocumentError):
        build_mapping_document(mappings=[{"source": "a", "target": deep}])


def test_template_create_refuses_a_bomb_shaped_body(tenancy_world):
    deep: dict = {"x": 1}
    for _ in range(60):
        deep = {"x": deep}
    r = tenancy_world.a.post(f"{BASE}/templates", json={
        "name": "bomb", "mappings": [{"source": "a", "target": "b", "target_type": deep}],
    })
    assert r.status_code == 422, r.text
    big = {"op": "constant_column", "options": {"to": "c", "value": "x" * 600_000}}
    r = tenancy_world.a.post(f"{BASE}/templates", json={"name": "big", "mappings": MAPPINGS, "computed": [big]})
    assert r.status_code in (413, 422), r.text
