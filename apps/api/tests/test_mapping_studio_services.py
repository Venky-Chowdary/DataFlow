"""Map-step remapping services: destination catalog, type grades, name matching, memory.

The Map step lets an operator point ``order_id`` / ``first_name`` at *other*
columns of an existing destination table. These tests pin the rules it relies
on to the ones Validate enforces, so the picker and Validate cannot disagree.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.destination_requirements_gate import build_destination_requirements_gate  # noqa: E402
from services.mapping_dest_catalog import (  # noqa: E402
    add_column_decision,
    build_destination_column_catalog,
    safe_catalog_message,
)
from services.mapping_name_match import (  # noqa: E402
    normalize_name,
    score_pair,
    suggest_assignments,
)
from services.mapping_type_compat import grade_type_pair, grade_type_pairs  # noqa: E402

_INFO = {
    "connected": True,
    "table_exists": True,
    "columns": ["id", "given_name", "family_name", "full_name", "amount", "created_at", "tenant_id"],
    "schema": {
        "id": "BIGINT",
        "given_name": "VARCHAR(40)",
        "family_name": "VARCHAR(60)",
        "full_name": "VARCHAR(120)",
        "amount": "DECIMAL(12,2)",
        "created_at": "TIMESTAMP",
        "tenant_id": "BIGINT",
    },
    "schema_nullability": {
        "id": False, "given_name": False, "family_name": True, "full_name": True,
        "amount": False, "created_at": True, "tenant_id": False,
    },
    "schema_defaults": {"amount": "0", "created_at": "current_timestamp"},
    "identity_columns": ["id"],
    "generated_columns": [],
    "primary_key_columns": ["id"],
    "message": "connected",
}


# --------------------------------------------------------------------------- catalog


def test_catalog_lists_every_destination_column_with_type_nullability_default_and_key():
    cat = build_destination_column_catalog(_INFO, dest_type="postgresql")
    by = {c["name"]: c for c in cat["columns"]}
    assert [c["name"] for c in cat["columns"]] == _INFO["columns"]
    assert by["id"]["primary_key"] and by["id"]["identity"]
    assert by["given_name"]["type"] == "VARCHAR(40)" and by["given_name"]["nullable"] is False
    assert by["amount"]["default"] == "0" and by["amount"]["has_default"]
    assert by["family_name"]["nullable"] is True and by["family_name"]["default"] is None
    assert cat["nullability_measured"] is True
    assert cat["table_exists"] is True


def test_required_is_exactly_what_g14_blocks_on():
    """NOT NULL, no default, not identity/generated — the G14 predicate, not a copy."""
    cat = build_destination_column_catalog(_INFO, dest_type="postgresql")
    required = sorted(c["name"] for c in cat["columns"] if c["required"])
    assert required == ["given_name", "tenant_id"]
    gate = build_destination_requirements_gate(
        destination_table_exists=True,
        column_nullability=_INFO["schema_nullability"],
        column_defaults=_INFO["schema_defaults"],
        identity_columns=_INFO["identity_columns"],
        generated_columns=[],
        mappings=[],
        dest_columns=_INFO["columns"],
    )
    assert gate["status"] == "block"
    assert sorted(gate["details"]["unfilled_required_columns"]) == required


def test_unmeasured_nullability_is_none_never_optional():
    info = {**_INFO, "schema_nullability": {"id": False}}
    cat = build_destination_column_catalog(info, dest_type="postgresql")
    by = {c["name"]: c for c in cat["columns"]}
    assert cat["nullability_measured"] is False
    assert by["tenant_id"]["nullable"] is None and by["tenant_id"]["required"] is None


@pytest.mark.parametrize(
    ("dest_type", "exists", "policy", "allowed"),
    [
        ("postgresql", False, "manual_review", True),
        ("postgresql", True, "manual_review", True),
        ("postgresql", True, "pause_on_change", False),
        ("postgresql", True, "type_locked", False),
        ("postgresql", None, "", False),
        ("mongodb", True, "pause_on_change", True),
    ],
)
def test_new_column_is_offered_only_when_validate_would_accept_it(dest_type, exists, policy, allowed):
    ok, reason = add_column_decision(dest_type=dest_type, table_exists=exists, schema_policy=policy)
    assert ok is allowed
    assert reason


def test_a_failed_schema_probe_never_promises_a_new_table():
    """introspect keeps table_exists=False from its object list when the schema
    read itself failed; that is unknown, not create-new."""
    cat = build_destination_column_catalog(
        {"connected": True, "table_exists": False, "columns": [],
         "message": "connected · schema probe: permission denied for table orders"},
        dest_type="postgresql",
    )
    assert cat["table_exists"] is None and cat["can_add_columns"] is False
    down = build_destination_column_catalog(
        {"connected": False, "table_exists": False, "columns": []}, dest_type="postgresql"
    )
    assert down["table_exists"] is None and down["can_add_columns"] is False


def test_catalog_messages_carry_no_secret_or_filesystem_path():
    text = safe_catalog_message(
        "cannot open /srv/customer/secret/prod.sqlite password=hunter2 "
        "postgresql://admin:s3cr3t@db.internal:5432/app"
    )
    assert "hunter2" not in text and "s3cr3t" not in text
    assert "/srv/customer" not in text and "<path>" in text


# --------------------------------------------------------------------------- type grades


@pytest.mark.parametrize(
    ("src", "tgt", "verdict", "kind"),
    [
        ("INTEGER", "BIGINT", "safe", "safe"),
        ("VARCHAR(40)", "VARCHAR(40)", "safe", "safe"),
        ("BIGINT", "INTEGER", "lossy", "narrowing"),
        ("VARCHAR(255)", "VARCHAR(50)", "lossy", "narrowing"),
        ("DECIMAL(12,2)", "DECIMAL(10,2)", "lossy", "precision"),
        ("TIMESTAMP(6)", "TIMESTAMP(3)", "lossy", "precision"),
        ("TIMESTAMPTZ", "TIMESTAMP", "lossy", "timezone"),
        ("VARCHAR", "INTEGER", "lossy", "domain"),
        ("BLOB", "INTEGER", "incompatible", "incompatible"),
        ("INTEGER", "", "unknown", "unknown"),
    ],
)
def test_type_grade_kinds(src, tgt, verdict, kind):
    grade = grade_type_pair(src, tgt, dest_db="postgresql")
    assert grade["verdict"] == verdict, grade
    assert kind in {r["kind"] for r in grade["reasons"]}, grade


def test_type_grade_agrees_with_validate_fidelity_verdict():
    """Lossy here ⇔ mapping_fidelity (G4's verdict) says lossy/cast for the same pair."""
    from services.mapping_proof import mapping_fidelity

    pairs = [
        ("INTEGER", "BIGINT"), ("BIGINT", "INTEGER"), ("VARCHAR(50)", "TEXT"),
        ("DECIMAL(12,2)", "DECIMAL(10,2)"), ("DATE", "TIMESTAMP"), ("DOUBLE", "REAL"),
    ]
    for src, tgt in pairs:
        fid = mapping_fidelity(
            {"source_type": src, "target_type": tgt, "transform": "none"},
            declared_source_type=src, declared_target_type=tgt,
            destination_db_type="postgresql", dest_table_exists=True,
        )
        ours = grade_type_pair(src, tgt, dest_db="postgresql")["verdict"]
        assert (ours == "lossy") == (fid["verdict"] in {"lossy_cast", "cast"}), (src, tgt, fid)


def test_type_grades_dedupe_identical_pairs():
    rows = grade_type_pairs(
        [{"source_type": "BIGINT", "target_type": "INTEGER"}] * 3, dest_db="postgresql"
    )
    assert len(rows) == 3 and rows[0] is rows[1] is rows[2]


# --------------------------------------------------------------------------- name matching


@pytest.mark.parametrize(
    ("name", "norm"),
    [("OrderID", "order_id"), ("firstName", "first_name"), ("First Name", "first_name"),
     ("total-balance", "total_balance"), ("HTTPStatusCode", "http_status_code")],
)
def test_normalize_name_folds_snake_camel_space(name, norm):
    assert normalize_name(name) == norm


@pytest.mark.parametrize(
    ("src", "tgt", "kind"),
    [
        ("order_id", "order_id", "exact"),
        ("ORDER_ID", "order_id", "case_insensitive"),
        ("firstName", "first_name", "normalized"),
        ("first_name", "given_name", "synonym"),
        ("last_name", "family_name", "synonym"),
        ("last_name", "surname", "synonym"),
        ("total", "amount", "synonym"),
        ("order_id", "id", "key_suffix"),
    ],
)
def test_score_pair_tiers(src, tgt, kind):
    hit = score_pair(src, tgt)
    assert hit is not None and hit[1] == kind, hit


@pytest.mark.parametrize(
    ("src", "tgt"),
    [("customer_email", "email_address"), ("order_amt", "total_amount"), ("order_status", "status")],
)
def test_concept_tier_matches_the_same_head_concept(src, tgt):
    hit = score_pair(src, tgt)
    assert hit is not None and hit[1] in {"concept", "synonym"}, hit
    assert hit[0] >= 0.7


@pytest.mark.parametrize(
    ("src", "tgt"),
    [("start_date", "end_date"), ("first_name", "last_name"), ("customer_id", "order_id"),
     ("billing_address", "shipping_address"),
     # review H2: family members are not each other's synonyms
     ("is_active", "is_deleted"), ("height", "width"), ("subtotal", "total"),
     ("state", "status"), ("revenue", "fee"),
     # contrastive qualifiers differ even from the plain name
     ("prev_status", "status"), ("max_qty", "qty"), ("min_qty", "quantity"),
     ("old_price", "unit_price"), ("new_email", "email")],
)
def test_concept_tier_refuses_false_friends(src, tgt):
    hit = score_pair(src, tgt)
    assert hit is None or hit[0] < 0.7, hit


def test_learned_pair_outranks_synonym_but_not_a_normalized_match():
    learned = {"cust_fname": {"given_name": 3}}
    conf, kind, reason = score_pair("cust_fname", "given_name", learned=learned)
    assert kind == "learned" and "3 times" in reason
    assert conf > score_pair("first_name", "given_name")[0]
    assert conf < score_pair("firstName", "first_name")[0]


def test_key_suffix_is_stronger_when_entity_names_the_table():
    plain = score_pair("order_id", "id")[0]
    entity = score_pair("order_id", "id", entities={"order"})[0]
    assert entity > plain


@pytest.mark.parametrize("src", ["customer_id", "product_id", "parent_id"])
def test_a_foreign_key_never_preselects_onto_another_tables_id(src):
    """Review H2: customer_id → orders.id is a foreign key into the PK."""
    out = suggest_assignments([src], ["id"], source_table="orders", dest_table="orders")
    assert out == [] or (out[0]["selected"] is False and out[0]["confidence"] < 0.7), out


def test_the_tables_own_key_preselects_onto_its_id():
    out = suggest_assignments(["order_id"], ["id"], source_table="legacy", dest_table="orders")
    assert out[0]["target"] == "id" and out[0]["selected"] is True


def test_assignment_is_one_to_one_and_best_first():
    sources = ["order_id", "name", "total_balance", "first_name", "last_name"]
    targets = ["id", "given_name", "surname", "full_name", "amount", "first_name"]
    out = suggest_assignments(sources, targets, source_table="orders", dest_table="orders")
    pairs = {s["source"]: s["target"] for s in out}
    assert pairs["first_name"] == "first_name"  # exact beats the given_name synonym
    assert pairs["last_name"] == "surname"
    assert pairs["order_id"] == "id"
    assert pairs["name"] == "full_name"
    assert len(set(pairs.values())) == len(pairs)
    exact = next(s for s in out if s["source"] == "first_name")
    assert exact["confidence"] == 1.0 and exact["selected"]
    assert any(a["target"] == "given_name" for a in exact["alternatives"])


def test_remaining_mode_never_steals_a_target_the_operator_chose():
    out = suggest_assignments(
        ["first_name", "fname"], ["first_name", "given_name"],
        locked_sources=["fname"], locked_targets=["first_name"],
    )
    assert [(s["source"], s["target"]) for s in out] == [("first_name", "given_name")]


def test_assignment_scales_to_wide_tables():
    import time

    sources = [f"col_{i}_value" for i in range(1500)]
    targets = [f"Col{i}Value" for i in range(1500)]
    started = time.perf_counter()
    out = suggest_assignments(sources, targets)
    assert time.perf_counter() - started < 10
    assert len(out) == 1500 and all(s["kind"] == "normalized" for s in out)


# --------------------------------------------------------------------------- memory


@pytest.fixture
def memory(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    from services import mapping_memory

    return mapping_memory


def test_memory_counts_confirmations_per_workspace_and_isolates_tenants(memory):
    memory.record_confirmed([{"source": "cust_fname", "target": "given_name"}], workspace_id="ws-a")
    memory.record_confirmed([{"source": "CustFname", "target": "GivenName"}], workspace_id="ws-a")
    assert memory.learned_index(workspace_id="ws-a") == {"cust_fname": {"given_name": 2}}
    assert memory.learned_index(workspace_id="ws-b") == {}
    assert memory.list_learned(workspace_id="ws-b") == []
    assert memory.forget_all(workspace_id="ws-a") == 1
    assert memory.learned_index(workspace_id="ws-a") == {}


def test_memory_feeds_the_learned_tier(memory):
    memory.record_confirmed([{"source": "cust_fname", "target": "given_name"}], workspace_id="ws-a")
    out = suggest_assignments(
        ["cust_fname"], ["given_name", "first_name"],
        learned=memory.learned_index(workspace_id="ws-a"),
    )
    assert out[0]["target"] == "given_name" and out[0]["kind"] == "learned"


def test_the_reported_orders_table_maps_onto_other_destination_columns():
    """MySQL orders (order_id, name, total_balance, first_name, last_name) onto an
    existing table whose columns are named differently — the case that started this."""
    out = suggest_assignments(
        ["order_id", "name", "total_balance", "first_name", "last_name"],
        ["id", "given_name", "surname", "full_name", "amount", "created_at", "tenant_id"],
        source_table="orders",
        dest_table="customers",
    )
    pairs = {s["source"]: (s["target"], s["kind"]) for s in out}
    assert pairs == {
        "order_id": ("id", "key_suffix"),
        "name": ("full_name", "synonym"),
        "total_balance": ("amount", "concept"),
        "first_name": ("given_name", "synonym"),
        "last_name": ("surname", "synonym"),
    }
    # order_id → customers.id is suggested but not pre-selected: the entity
    # (order) does not name the destination table (review H2).
    assert {s["source"] for s in out if s["selected"]} == {"name", "total_balance", "first_name", "last_name"}


@pytest.mark.parametrize(
    ("src", "tgt"),
    [("created_at", "updated_at"), ("work_email", "personal_email"), ("order_status", "payment_status")],
)
def test_qualified_siblings_in_one_synonym_group_are_not_synonyms(src, tgt):
    hit = score_pair(src, tgt)
    assert hit is None or hit[1] not in {"synonym", "concept"}, hit
