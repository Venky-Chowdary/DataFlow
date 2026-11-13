"""Document model plumbing: request, plan revision, Decision Artifact, refusal.

No behaviour changes for a request without a model: the legacy Decision
Artifact hashes are pinned below to the values the code produced before
``document_model_hash`` existed.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from services.document_model import DocumentModelError, model_hash

_GOLDEN = Path(__file__).resolve().parent / "fixtures" / "decision_artifact_v1_golden.json"
# Computed on origin/feature/Cleanup 8d715d902, before this change.
_GOLDEN_CONTENT_HASH = "1e49e084e59f492a3b375bf7c16e372051b32aa39e90b7d0a7b3e458377ffdc4"
_GOLDEN_JSON_SHA256 = "2d87daa2d11a5ec8942b038914c2e206c1e684891fbc2b936a859ee2f31950b4"


def _model(**over) -> dict:
    model = {
        "collection": "customers",
        "root": {
            "source": "public.customers",
            "key": ["customer_id"],
            "fields": [{"source": "name", "path": "name"}],
        },
        "relationships": [
            {
                "id": "orders",
                "parent": "root",
                "source": "public.orders",
                "join": [{"parent": "customer_id", "child": "customer_id"}],
                "rule": "embed_array",
                "path": "orders",
                "child_key": ["order_id"],
            }
        ],
    }
    model.update(over)
    return model


# --- TransferRequest -----------------------------------------------------------


def _request(**kw):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="customers"),
        destination=EndpointConfig(kind="database", format="mongodb", collection="customers"),
        **kw,
    )


def test_request_without_a_model_is_unchanged():
    from src.transfer.models import transfer_request_from_dict, transfer_request_to_dict

    request = _request()
    assert request.document_model is None
    assert request.approved_document_model_hash == ""
    wire = transfer_request_to_dict(request)
    assert wire["document_model"] is None
    assert wire["approved_document_model_hash"] == ""
    revived = transfer_request_from_dict(wire)
    assert revived.document_model is None
    assert revived.approved_document_model_hash == ""


def test_request_serializes_the_model_next_to_the_shape_recipe():
    from src.transfer.models import transfer_request_to_dict

    keys = list(transfer_request_to_dict(_request()))
    at = keys.index("approved_shape_recipe_hash")
    assert keys[at + 1 : at + 3] == ["document_model", "approved_document_model_hash"]


def test_request_round_trips_the_model_and_its_approval():
    from src.transfer.models import transfer_request_from_dict, transfer_request_to_dict

    model = _model()
    request = _request(document_model=model, approved_document_model_hash=model_hash(model))
    wire = json.loads(json.dumps(transfer_request_to_dict(request)))
    revived = transfer_request_from_dict(wire)
    assert revived.document_model == model
    assert revived.approved_document_model_hash == model_hash(model)
    # The stored copy is not the caller's dict: editing one does not edit the other.
    assert transfer_request_to_dict(request)["document_model"] is not model


def test_legacy_request_dict_reads_as_no_model():
    from src.transfer.models import transfer_request_from_dict, transfer_request_to_dict

    wire = transfer_request_to_dict(_request())
    wire.pop("document_model")
    wire.pop("approved_document_model_hash")
    revived = transfer_request_from_dict(wire)
    assert revived.document_model is None
    assert revived.approved_document_model_hash == ""


def test_a_stored_model_that_is_not_an_object_is_kept_so_execute_refuses_it():
    from services.execute_document_route import approved_model_refusal
    from src.transfer.models import transfer_request_from_dict, transfer_request_to_dict

    wire = transfer_request_to_dict(_request())
    wire["document_model"] = ["not", "a", "model"]
    revived = transfer_request_from_dict(wire)
    assert revived.document_model == ["not", "a", "model"]
    assert approved_model_refusal(revived.document_model, "abc")


# --- PlanRevision and the plan store -------------------------------------------


@pytest.fixture
def plan_store(tmp_path, monkeypatch):
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    from services import transfer_plan_store

    return transfer_plan_store


def _plan(store):
    return store.create_plan(
        {
            "source_columns": ["customer_id", "name"],
            "source_schema": {"customer_id": "INTEGER", "name": "TEXT"},
            "target_columns": ["customer_id", "name"],
            "target_schema": {"customer_id": "int", "name": "string"},
        }
    )


_MAPPINGS = [{"source": "name", "target": "name"}]


def test_plan_revision_defaults_and_legacy_rows():
    from services.transfer_plan_store import PlanRevision

    legacy = {
        "version": 1,
        "mappings": [],
        "mapping_hash": "m",
        "source_schema_hash": "s",
        "target_schema_hash": "t",
    }
    rev = PlanRevision.from_dict(legacy)
    assert rev.document_model == {}
    assert rev.document_model_hash == ""
    as_dict = rev.to_dict()
    assert as_dict["document_model"] == {}
    assert as_dict["document_model_hash"] == ""


def test_plan_revision_from_dict_is_tolerant():
    from services.transfer_plan_store import PlanRevision

    rev = PlanRevision.from_dict(
        {"version": 2, "document_model": "garbage", "document_model_hash": None}
    )
    assert rev.document_model == {}
    assert rev.document_model_hash == ""
    model = _model()
    rev = PlanRevision.from_dict(
        {"version": 3, "document_model": model, "document_model_hash": model_hash(model)}
    )
    assert rev.document_model == model
    assert rev.document_model_hash == model_hash(model)
    assert PlanRevision.from_dict(rev.to_dict()) == rev


def test_sync_ui_mappings_stores_each_model_edit_as_a_new_revision(plan_store):
    plan = _plan(plan_store)
    first = _model()
    plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model=first)
    edited = _model()
    edited["relationships"][0]["path"] = "purchases"
    updated = plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model=edited)

    reloaded = plan_store.get_plan(plan.id)
    assert [r.version for r in reloaded.revisions] == [1, 2]
    v1, v2 = reloaded.revisions
    assert v1.document_model == first
    assert v1.document_model_hash == model_hash(first)
    assert v2.document_model == edited
    assert v2.document_model_hash == model_hash(edited)
    assert v1.document_model_hash != v2.document_model_hash
    assert updated.active_revision().document_model_hash == model_hash(edited)


def test_a_mapping_edit_without_a_model_keeps_the_current_model(plan_store):
    plan = _plan(plan_store)
    model = _model()
    plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model=model)
    plan_store.sync_ui_mappings(plan.id, _MAPPINGS + [{"source": "customer_id", "target": "id"}])
    rev = plan_store.get_plan(plan.id).active_revision()
    assert rev.version == 2
    assert rev.document_model == model
    assert rev.document_model_hash == model_hash(model)


def test_an_empty_model_clears_it(plan_store):
    plan = _plan(plan_store)
    plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model=_model())
    plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model={})
    rev = plan_store.get_plan(plan.id).active_revision()
    assert rev.document_model == {}
    assert rev.document_model_hash == ""


def test_plans_without_a_model_store_none(plan_store):
    plan = _plan(plan_store)
    plan_store.sync_ui_mappings(plan.id, _MAPPINGS)
    rev = plan_store.get_plan(plan.id).active_revision()
    assert rev.document_model == {}
    assert rev.document_model_hash == ""


def test_a_malformed_model_is_refused_and_no_revision_is_written(plan_store):
    plan = _plan(plan_store)
    with pytest.raises(DocumentModelError) as exc:
        plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model={"collection": "c", "colour": 1})
    assert exc.value.code == "unknown_key"
    assert plan_store.get_plan(plan.id).revisions == []


def test_add_mapping_revision_accepts_and_carries_the_model(plan_store):
    plan = _plan(plan_store)
    model = _model()
    plan_store.add_mapping_revision(plan.id, {"mappings": _MAPPINGS}, document_model=model)
    plan_store.add_mapping_revision(plan.id, {"mappings": _MAPPINGS})
    revs = plan_store.get_plan(plan.id).revisions
    assert [r.document_model_hash for r in revs] == [model_hash(model)] * 2
    with pytest.raises(DocumentModelError):
        plan_store.add_mapping_revision(plan.id, {"mappings": _MAPPINGS}, document_model={"x": 1})
    assert len(plan_store.get_plan(plan.id).revisions) == 2


# --- Decision Artifact -----------------------------------------------------------


def _golden_raw() -> dict:
    from services.decision_kernel import compute_content_hash

    raw = json.loads(_GOLDEN.read_text(encoding="utf-8"))
    raw["content_hash"] = compute_content_hash(raw)
    return raw


def test_legacy_artifact_hash_and_bytes_are_unchanged():
    from services.decision_kernel import compute_content_hash, decision_artifact_from_dict

    raw = _golden_raw()
    assert raw["content_hash"] == _GOLDEN_CONTENT_HASH
    art = decision_artifact_from_dict(raw)
    assert art.document_model_hash == ""
    as_dict = art.to_dict()
    assert "document_model_hash" not in as_dict
    assert compute_content_hash(as_dict) == _GOLDEN_CONTENT_HASH
    assert hashlib.sha256(art.to_json().encode("utf-8")).hexdigest() == _GOLDEN_JSON_SHA256


def _built(**kw):
    from services.decision_kernel import DdlPlan, build_decision_artifact

    return build_decision_artifact(
        tenant_id="t1",
        route_id="r1",
        source_fingerprint="sf",
        dest_fingerprint="df",
        source_columns=[],
        dest_columns=[],
        mappings=[],
        ddl=DdlPlan(ddl_identity_hash="d", column_ddl={}, dialect="mongodb"),
        artifact_id="da_fixed",
        created_at="2026-01-01T00:00:00+00:00",
        **kw,
    )


def test_artifact_without_a_model_is_built_exactly_as_before():
    plain = _built()
    explicit_empty = _built(document_model_hash="")
    assert "document_model_hash" not in plain.to_dict()
    assert plain.content_hash == explicit_empty.content_hash
    assert plain.to_json() == explicit_empty.to_json()


def test_artifact_carries_the_model_hash_into_its_content_hash():
    from services.decision_kernel import decision_artifact_from_dict

    digest = model_hash(_model())
    art = _built(document_model_hash=digest)
    assert art.document_model_hash == digest
    assert art.to_dict()["document_model_hash"] == digest
    assert art.content_hash != _built().content_hash
    revived = decision_artifact_from_dict(json.loads(art.to_json()))
    assert revived == art


def test_artifact_model_hash_cannot_be_swapped_or_dropped():
    from services.decision_kernel import decision_artifact_from_dict

    art = _built(document_model_hash=model_hash(_model()))
    swapped = art.to_dict()
    swapped["document_model_hash"] = "0" * 16
    with pytest.raises(ValueError, match="content_hash mismatch"):
        decision_artifact_from_dict(swapped)
    dropped = art.to_dict()
    dropped.pop("document_model_hash")
    with pytest.raises(ValueError, match="content_hash mismatch"):
        decision_artifact_from_dict(dropped)


# --- approved_model_refusal ------------------------------------------------------


def test_no_model_and_no_approval_is_today_behaviour():
    from services.execute_document_route import approved_model_refusal

    assert approved_model_refusal(None, "") == ""
    assert approved_model_refusal({}, "") == ""
    assert approved_model_refusal(None, "   ") == ""


def test_the_approved_model_runs():
    from services.execute_document_route import approved_model_refusal

    model = _model()
    assert approved_model_refusal(model, model_hash(model)) == ""
    assert approved_model_refusal(model, f"  {model_hash(model)}  ") == ""
    relabeled = _model(label="Customer 360")
    assert approved_model_refusal(relabeled, model_hash(model)) == ""


def test_a_changed_model_is_refused():
    from services.execute_document_route import approved_model_refusal

    approved = model_hash(_model())
    changed = _model()
    changed["relationships"][0]["path"] = "purchases"
    reason = approved_model_refusal(changed, approved)
    assert reason
    assert model_hash(changed) in reason and approved in reason
    assert "re-validate" in reason


def test_a_model_that_was_never_approved_is_refused():
    from services.execute_document_route import approved_model_refusal

    reason = approved_model_refusal(_model(), "")
    assert reason
    assert "Validate" in reason


def test_an_approval_with_no_model_is_refused():
    from services.execute_document_route import approved_model_refusal

    reason = approved_model_refusal(None, "abcd1234abcd1234")
    assert reason
    assert "abcd1234abcd1234" in reason
    assert approved_model_refusal({}, "abcd1234abcd1234") == reason


def test_a_malformed_model_is_refused_with_its_parse_error():
    from services.execute_document_route import approved_model_refusal

    reason = approved_model_refusal({"collection": "c", "colour": "red"}, "abcd1234abcd1234")
    assert "'colour'" in reason


def test_refusals_speak_datawrap():
    from services.execute_document_route import approved_model_refusal

    reasons = [
        approved_model_refusal(_model(), ""),
        approved_model_refusal(_model(), "ffff"),
        approved_model_refusal(None, "ffff"),
    ]
    assert all(r and "DataFlow" not in r for r in reasons)


def test_request_level_refusal_reads_the_request():
    from services.execute_document_route import request_document_model_refusal

    model = _model()
    assert request_document_model_refusal(_request()) == ""
    assert request_document_model_refusal(
        _request(document_model=model, approved_document_model_hash=model_hash(model))
    ) == ""
    assert request_document_model_refusal(_request(document_model=model))


# --- review of PR 417: parse strictness ------------------------------------------


def test_a_model_with_a_null_rule_is_never_approved():
    # M1: a null rule parsed as "" and hashed, so its own approval passed.
    from services.execute_document_route import approved_model_refusal

    model = _model()
    model["relationships"][0]["rule"] = None
    reason = approved_model_refusal(model, "abcd1234abcd1234")
    assert reason.startswith("the document model cannot be read")
    assert "rule" in reason


class _Hostile(dict):
    """A mapping that fails when read, standing in for any unexpected input."""

    def get(self, *_a, **_k):
        raise RuntimeError("boom")

    def items(self):
        raise RuntimeError("boom")

    def __iter__(self):
        raise RuntimeError("boom")


class _BadStr:
    def __str__(self):
        raise RuntimeError("boom")


@pytest.mark.parametrize(
    "model, approved",
    [
        (_model(collection="c\ud800"), "abcd1234abcd1234"),
        (_model(collection="c\ud800"), ""),
        ({"collection": "c", "root": {"source": "t", "key": ["k\ud800"]}}, "x"),
        (_Hostile(collection="c"), "abcd1234abcd1234"),
        (_model(), _BadStr()),
        (_model(), "abc\ud800"),
        (object(), "abcd1234abcd1234"),
    ],
)
def test_the_approval_check_never_raises(model, approved):
    # M2: a lone surrogate crashed model_hash, and so the approval check, with
    # UnicodeEncodeError. Execute must get a refusal, never an exception.
    from services.execute_document_route import approved_model_refusal

    reason = approved_model_refusal(model, approved)
    assert reason
    reason.encode("utf-8")
    assert "DataFlow" not in reason


def test_a_model_that_is_not_utf8_is_refused_before_a_revision_is_written(plan_store):
    # M2: sync_ui_mappings raised UnicodeEncodeError from model_hash.
    plan = _plan(plan_store)
    bad = _model(collection="c\ud800")
    with pytest.raises(DocumentModelError) as exc:
        plan_store.sync_ui_mappings(plan.id, _MAPPINGS, document_model=bad)
    assert exc.value.code == "bad_value"
    assert plan_store.get_plan(plan.id).revisions == []
