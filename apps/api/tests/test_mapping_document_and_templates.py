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


def test_template_binds_each_live_source_once():
    # A hand-edited template may name one source twice; the editor holds one
    # target per source row, so the second pair is reported, not suggested.
    out = rebind_template_pairs(
        [("order_id", "legacy_ref"), ("order_id", "order_ref")],
        source_columns=["order_id"],
        destination_columns=["legacy_ref", "order_ref"],
    )
    assert [(s["source"], s["target"]) for s in out["suggestions"]] == [("order_id", "legacy_ref")]
    assert out["unbound"] == [{"source": "order_id", "target": "order_ref", "missing": "source"}]


def test_template_never_binds_an_engine_filled_column():
    out = rebind_template_pairs(
        [("order_id", "legacy_ref"), ("total", "total_cents")],
        source_columns=["order_id", "total"],
        destination_columns=["legacy_ref", "total_cents", "total_cents_v2"],
        unwritable_columns=["total_cents"],
    )
    got = [(s["source"], s["target"]) for s in out["suggestions"]]
    assert ("total", "total_cents") not in got
    assert all(s["target"] != "total_cents" for s in out["suggestions"])
    assert {"source": "total", "target": "total_cents", "missing": "target_unwritable"} in out["unbound"]


def test_template_apply_route_skips_unwritable_columns(tenancy_world):
    w = tenancy_world
    tid = w.a.post(f"{BASE}/templates", json={"name": "gen", "mappings": MAPPINGS}).json()["id"]
    body = {
        "source_columns": [m["source"] for m in MAPPINGS],
        "destination_columns": [m["target"] for m in MAPPINGS],
        "unwritable_columns": [MAPPINGS[0]["target"]],
    }
    out = w.a.post(f"{BASE}/templates/{tid}/apply", json=body).json()
    assert MAPPINGS[0]["target"] not in {s["target"] for s in out["suggestions"]}
    assert any(u["missing"] == "target_unwritable" for u in out["unbound"])


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
    # No checksum: accepted, and the warning says what a checksum would have
    # shown (unchanged since export), not that anything was "verified".
    bare = "\n".join(line for line in r.text.splitlines() if not line.startswith("checksum:"))
    hand = a.post(f"{BASE}/import", json={"document": bare}).json()
    assert hand["checksum_verified"] is False
    assert "unchanged since export" in hand["warnings"][0]
    assert "verified" not in " ".join(hand["warnings"]).lower()


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


# --------------------------------------------------------------------------- M5: scope and cap


def test_template_listing_reads_only_the_active_workspace(tenancy_world):
    w = tenancy_world
    a_tpl = w.a.post(f"{BASE}/templates", json={"name": "A only", "mappings": MAPPINGS}).json()["id"]
    b_tpl = w.b.post(f"{BASE}/templates", json={"name": "B only", "mappings": MAPPINGS}).json()["id"]
    # A platform administrator acting in workspace B sees B's templates, not every tenant's.
    listed = w.admin.get(f"{BASE}/templates", headers={"X-Workspace-Id": w.workspace_b}).json()["templates"]
    assert [t["id"] for t in listed] == [b_tpl]
    assert [t["id"] for t in w.a.get(f"{BASE}/templates").json()["templates"]] == [a_tpl]


def test_template_list_reads_one_partition(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    from services import mapping_templates

    seen: list = []
    real = mapping_templates._DOC.load

    def spy(*args, **kwargs):
        seen.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(mapping_templates._DOC, "load", spy)
    mapping_templates.list_templates(workspace_id="ws-a")
    assert seen and all(k.get("part") == "ws-a" for k in seen), seen


def test_the_per_workspace_template_cap_holds_under_concurrent_creates(tmp_path, monkeypatch):
    import threading

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    from services import mapping_templates

    monkeypatch.setattr(mapping_templates, "MAX_TEMPLATES_PER_WORKSPACE", 3)
    doc = build_mapping_document(mappings=MAPPINGS)
    results: list[str] = []

    def make(i: int) -> None:
        try:
            mapping_templates.create_template(name=f"t{i}", description="", document=doc, workspace_id="ws-a")
            results.append("ok")
        except mapping_templates.TemplateLimitReached:
            results.append("full")

    threads = [threading.Thread(target=make, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count("ok") == 3, results
    assert len(mapping_templates.list_templates(workspace_id="ws-a")) == 3


# --------------------------------------------------------------------------- delete authority


def test_only_the_creator_or_a_workspace_admin_deletes_a_template(tenancy_world):
    w = tenancy_world
    tid = w.a.post(f"{BASE}/templates", json={"name": "mine", "mappings": MAPPINGS}).json()["id"]
    other_editor = w.member("editor-2@tenancy.example.com", w.workspace_a, "editor")
    refused = other_editor.delete(f"{BASE}/templates/{tid}")
    assert refused.status_code == 403, refused.text
    assert w.a.get(f"{BASE}/templates/{tid}").status_code == 200
    admin_a = w.member("admin-ws-a@tenancy.example.com", w.workspace_a, "admin")
    assert admin_a.delete(f"{BASE}/templates/{tid}").status_code == 200
    mine = w.a.post(f"{BASE}/templates", json={"name": "mine 2", "mappings": MAPPINGS}).json()["id"]
    assert w.a.delete(f"{BASE}/templates/{mine}").status_code == 200
