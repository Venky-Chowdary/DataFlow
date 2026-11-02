"""Autopilot: delegated authority for unattended runs, and the approval inbox.

A scheduled run has nobody at the keyboard, so every gate that wants an operator
signature refused it — correctly, and then identically on every later beat. The
schedule never entered a state that said a human owed it a decision, so a real
Excel to Snowflake job simply failed nightly forever.

Two mechanisms fix that, and both are only worth having if they fail closed:

* a **standing authorization** — a named human's advance signature, hash-bound to
  the exact plan they signed, scoped, time-boxed and revocable;
* an **approval request** — one durable finding with its refusal code, corrective
  action and binding, which suppresses the cadence until it is decided.

What these tests defend is mostly what the feature must *refuse*: a signature must
not survive the plan changing, must not cover a risk class nobody may delegate,
must not outlive its expiry, and an approval must never be offered for a finding
that a signature cannot clear.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import services.schedule_store as store
from services.schedule_approvals import (
    STATUS_OPEN,
    STATUS_REJECTED,
    ApprovalRequired,
    approve_request,
    build_approval_request,
    open_approval_request,
    open_approvals,
    record_authorization_use,
    reject_request,
    revoke_standing_authorization,
    set_standing_authorization,
)
from services.schedule_runner import authorization_for
from services.standing_authorization import (
    CODE_BINDING_CHANGED,
    CODE_EXHAUSTED,
    CODE_EXPIRED,
    CODE_NO_AUTHORIZATION,
    CODE_REVOKED,
    MAX_GRANT_DAYS,
    SCOPE_COMPLIANCE,
    SCOPE_FK_RISK,
    SCOPE_NET_ADDITIVE_DRIFT,
    SCOPE_SCHEMA_DRIFT,
    AuthorizationRefused,
    StandingAuthorization,
    binding_from_schedule,
    delegable_scopes_for,
    evaluate_authorization,
    grant_authorization,
    rebind_authorization,
    record_use,
    revoke_authorization,
    scopes_for_drift_kinds,
)

ACTOR = "dana.architect@example.com"
REASON = "Nightly finance load is signed off for the current mapping."


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    yield


@pytest.fixture
def sched(temp_store):
    return store.create_schedule({
        "name": "Excel to Snowflake",
        "source_connector_id": "src-xlsx",
        "source_table": "revenue",
        "dest_connector_id": "dst-snow",
        "dest_table": "FINANCE.REVENUE",
        "interval": "daily",
        "mappings": [{"source": "amount", "target": "AMOUNT"}],
    })


def _binding(**overrides: Any) -> dict[str, Any]:
    from services.standing_authorization import compute_binding

    base: dict[str, Any] = {
        "mappings": [{"source": "amount", "target": "AMOUNT"}],
        "schedule_id": "sched-1",
        "workspace_id": "ws-1",
        "source_table": "revenue",
        "dest_table": "FINANCE.REVENUE",
        "source_schema_fingerprint": "fp-source-a",
        "sync_mode": "full_refresh_overwrite",
        "schema_policy": "manual_review",
        "validation_mode": "strict",
    }
    base.update(overrides)
    return compute_binding(**base)


def _grant(**overrides: Any) -> StandingAuthorization:
    kwargs: dict[str, Any] = {
        "actor": ACTOR,
        "reason": REASON,
        "scopes": [SCOPE_SCHEMA_DRIFT],
        "binding": _binding(),
        "acknowledgments": {"schema_drift": True},
    }
    kwargs.update(overrides)
    return grant_authorization(**kwargs)


# --- what a grant may be, and may not ---------------------------------------


def test_a_grant_records_the_named_human_and_expires():
    grant = _grant()
    assert grant.actor == ACTOR
    assert grant.reason == REASON
    assert grant.scopes == (SCOPE_SCHEMA_DRIFT,)
    assert grant.expires_at and not grant.expired()
    assert grant.binding["binding_hash"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"actor": "d"},
        {"reason": "ok"},
        {"scopes": []},
        {"scopes": ["narrow_type"]},
        {"scopes": ["approve_everything"]},
        {"expires_in_days": 0},
        {"expires_in_days": MAX_GRANT_DAYS + 1},
        {"binding": {}},
    ],
)
def test_a_grant_is_refused_when_it_would_not_be_accountable(overrides):
    with pytest.raises(AuthorizationRefused):
        _grant(**overrides)


def test_a_scope_cannot_replay_an_attestation_nobody_made():
    """Delegating "compliance is accepted" without accepting it is a forgery."""
    with pytest.raises(AuthorizationRefused):
        _grant(scopes=[SCOPE_COMPLIANCE], acknowledgments={"schema_drift": True})


def test_a_grant_replays_only_the_attestations_it_holds():
    grant = _grant(
        scopes=[SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True, "compliance": True, "fk_risk": True},
    )
    signed = grant.signed()
    assert signed.schema_drift
    # Signed for, but out of scope: authority is the intersection, never the union.
    assert not signed.compliance
    assert not signed.fk_risk
    assert signed.actor == ACTOR


# --- binding: the signature is for one plan ---------------------------------


def test_an_unchanged_plan_replays_the_signature():
    binding = _binding()
    decision = evaluate_authorization(_grant(binding=binding), binding_now=binding)
    assert decision.applies
    assert decision.allows(SCOPE_SCHEMA_DRIFT)
    assert decision.acknowledgments.schema_drift


@pytest.mark.parametrize(
    "moved",
    [
        {"mappings": [{"source": "amount", "target": "TOTAL"}]},
        {"source_schema_fingerprint": "fp-source-b"},
        {"schedule_id": "sched-2"},
        {"workspace_id": "ws-2"},
        {"dest_table": "FINANCE.REVENUE_V2"},
        {"source_table": "revenue_v2"},
        {"sync_mode": "incremental"},
        {"schema_policy": "propagate_all"},
        {"validation_mode": "lenient"},
        {"primary_key": "id"},
        {"cursor_column": "updated_at"},
        {"contract_id": "contract-9"},
        {"require_signed_contract": True},
        {"backfill_new_fields": True},
        {"source_query": "select * from revenue where region = 'EU'"},
        {"stream_contracts": [{"stream": "revenue", "primary_key": ["id"]}]},
    ],
)
def test_a_signature_does_not_carry_to_a_plan_nobody_looked_at(moved):
    grant = _grant(binding=_binding())
    decision = evaluate_authorization(grant, binding_now=_binding(**moved))
    assert not decision.applies
    assert decision.code == CODE_BINDING_CHANGED
    assert decision.differences
    assert not decision.acknowledgments.any_claimed


def test_no_grant_is_not_an_error_it_is_simply_no_authority():
    decision = evaluate_authorization(None, binding_now=_binding())
    assert not decision.applies
    assert decision.code == CODE_NO_AUTHORIZATION
    assert not decision.has_grant
    assert not decision.acknowledgments.any_claimed


def test_a_record_with_no_named_actor_is_no_authority():
    assert StandingAuthorization.from_dict({"scopes": [SCOPE_SCHEMA_DRIFT]}) is None


def test_an_expired_grant_stops_applying():
    binding = _binding()
    grant = _grant(binding=binding, expires_in_days=1)
    later = datetime.now(timezone.utc) + timedelta(days=2)
    decision = evaluate_authorization(grant, binding_now=binding, now=later)
    assert not decision.applies
    assert decision.code == CODE_EXPIRED


def test_a_grant_with_an_unreadable_expiry_fails_closed():
    binding = _binding()
    broken = StandingAuthorization.from_dict({
        **_grant(binding=binding).to_dict(),
        "expires_at": "whenever",
    })
    assert broken is not None
    assert broken.expired()
    assert not evaluate_authorization(broken, binding_now=binding).applies


def test_a_revoked_grant_is_kept_and_permanently_unusable():
    binding = _binding()
    revoked = revoke_authorization(_grant(binding=binding), actor="admin", reason="offboarded")
    decision = evaluate_authorization(revoked, binding_now=binding)
    assert not decision.applies
    assert decision.code == CODE_REVOKED
    # The record survives: revocation is evidence, not a delete.
    assert revoked.actor == ACTOR and revoked.revoked_by == "admin"


def test_an_approve_once_grant_stops_after_the_run_it_authorized():
    binding = _binding()
    grant = _grant(binding=binding, max_uses=1)
    assert evaluate_authorization(grant, binding_now=binding).applies
    used = record_use(grant)
    assert used.uses == 1 and used.last_used_at
    decision = evaluate_authorization(used, binding_now=binding)
    assert not decision.applies
    assert decision.code == CODE_EXHAUSTED


# --- re-binding: only across a change the granter authorized ----------------


def test_a_net_additive_grant_carries_itself_across_the_shape_it_authorized():
    """Otherwise the grant that permitted the advance would invalidate itself."""
    grant = _grant(
        binding=_binding(),
        scopes=[SCOPE_NET_ADDITIVE_DRIFT, SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True},
    )
    advanced = _binding(source_schema_fingerprint="fp-source-b")
    rebound = rebind_authorization(grant, binding_now=advanced)
    assert rebound.binding["binding_hash"] == advanced["binding_hash"]
    assert rebound.rebinds and rebound.rebinds[-1]["from"] == "fp-source-a"
    assert rebound.actor == ACTOR and rebound.expires_at == grant.expires_at
    assert evaluate_authorization(rebound, binding_now=advanced).applies


def test_re_binding_is_refused_without_the_scope_that_earned_it():
    with pytest.raises(AuthorizationRefused):
        rebind_authorization(
            _grant(binding=_binding()),
            binding_now=_binding(source_schema_fingerprint="fp-source-b"),
        )


def test_re_binding_never_moves_anything_but_the_source_shape():
    grant = _grant(
        binding=_binding(),
        scopes=[SCOPE_NET_ADDITIVE_DRIFT, SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True},
    )
    with pytest.raises(AuthorizationRefused):
        rebind_authorization(grant, binding_now=_binding(sync_mode="incremental"))


# --- which findings a signature can even clear ------------------------------


@pytest.mark.parametrize(
    "kinds",
    [
        [],
        ["narrow_type"],
        ["type_change"],
        ["add_not_null"],
        ["nullability_tighten"],
        ["primary_key_change"],
        ["primary_key_removed"],
        ["cursor_removed"],
        ["drop", "narrow_type"],
        ["something_the_model_does_not_classify"],
    ],
)
def test_hard_breaking_and_unknown_drift_is_never_delegable(kinds):
    assert scopes_for_drift_kinds(kinds) == ()


def test_a_mapped_drop_or_rename_is_the_only_delegable_drift():
    assert scopes_for_drift_kinds(["drop", "rename"]) == (
        SCOPE_NET_ADDITIVE_DRIFT,
        SCOPE_SCHEMA_DRIFT,
    )


@pytest.mark.parametrize(
    ("finding", "expected"),
    [
        ("Source schema drift: column region added", (SCOPE_SCHEMA_DRIFT,)),
        ("PII detected in column email", (SCOPE_COMPLIANCE,)),
        ("Foreign key would be left with orphan rows", (SCOPE_FK_RISK,)),
        ("Mapping confidence below floor for column amount", ()),
        ("Lossy conversion DECIMAL(18,4) to FLOAT", ()),
        ("Narrowing type change on column amount", ()),
        ("Unsupported conversion from GEOGRAPHY", ()),
        ("Primary key missing for incremental_deduped", ()),
        ("add_not_null would tighten nullability", ()),
        ("", ()),
        # A finding that names a drift *and* a lossy path is not approvable: the
        # lossy half would refuse the very next run.
        ("Source schema drift with lossy conversion on amount", ()),
    ],
)
def test_only_findings_an_attestation_clears_are_offered_for_approval(finding, expected):
    assert delegable_scopes_for(finding) == expected


# --- the inbox: durable state a human can act on ----------------------------


def _finding(sch, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": "source_drift",
        "code": "SOURCE_SCHEMA_DRIFT",
        "finding": "Source schema drift: column region was renamed",
        "corrective_action": "Confirm the mapping, then accept the new shape.",
        "binding": binding_from_schedule(sch),
        "requested_scopes": [SCOPE_NET_ADDITIVE_DRIFT, SCOPE_SCHEMA_DRIFT],
    }
    payload.update(overrides)
    return build_approval_request(**payload)


def test_a_refused_run_parks_the_schedule_and_suppresses_the_cadence(sched):
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    store.update_schedule(sched.id, {"next_run_at": past})
    assert sched.id in {s.id for s in store.due_schedules()}

    parked = open_approval_request(sched.id, _finding(sched))
    assert parked is not None
    assert parked.last_status == "needs_approval"
    assert parked.approval_request["status"] == STATUS_OPEN
    assert parked.retry_at is None and parked.retry_attempt == 0
    # The whole point: the same refusal is not replayed on the next beat.
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_the_same_finding_is_counted_not_queued(sched):
    open_approval_request(sched.id, _finding(sched))
    again = open_approval_request(sched.id, _finding(sched))
    assert again is not None
    assert again.approval_request["occurrences"] == 2
    assert again.approval_request["last_seen_at"]


def test_the_same_finding_on_a_changed_plan_is_a_new_decision(sched):
    first = open_approval_request(sched.id, _finding(sched))
    assert first is not None
    moved = store.update_schedule(sched.id, {"source_schema_fingerprint": "fp-moved"})
    assert moved is not None
    second = open_approval_request(sched.id, _finding(moved))
    assert second is not None
    assert second.approval_request["id"] != first.approval_request["id"]
    assert second.approval_request["occurrences"] == 1


def test_a_finding_no_signature_can_clear_is_not_offered_as_approvable(sched):
    request = _finding(
        sched,
        code="RUN_REFUSED",
        finding="Lossy conversion DECIMAL(18,4) to FLOAT on column amount",
        requested_scopes=(),
    )
    assert request["approvable"] is False
    parked = open_approval_request(sched.id, request)
    assert parked is not None
    with pytest.raises(AuthorizationRefused):
        approve_request(
            sched.id,
            request["id"],
            actor=ACTOR,
            reason=REASON,
            grant_standing=True,
        )
    # Nothing was written: the schedule is still parked on the same finding.
    still = store.get_schedule(sched.id)
    assert still is not None
    assert still.approval_request["status"] == STATUS_OPEN
    assert not still.standing_authorization


def test_approving_once_re_arms_the_run_without_standing_authority(sched):
    request = _finding(sched)
    open_approval_request(sched.id, request)
    result = approve_request(
        sched.id,
        request["id"],
        actor=ACTOR,
        reason=REASON,
        acknowledgments={"schema_drift": True},
    )
    grant = result["authorization"]
    assert grant["max_uses"] == 1, "approve-once must not authorize every later run"
    updated = result["schedule"]
    assert updated.approval_request["status"] == "approved"
    assert updated.approval_request["resolved_by"] == ACTOR
    assert updated.last_status != "needs_approval"
    # Re-armed: the decision takes effect now, not a cadence away.
    assert sched.id in {s.id for s in store.due_schedules()}


def test_approving_and_authorizing_covers_later_identical_runs(sched):
    request = _finding(sched)
    open_approval_request(sched.id, request)
    result = approve_request(
        sched.id,
        request["id"],
        actor=ACTOR,
        reason=REASON,
        acknowledgments={"schema_drift": True},
        grant_standing=True,
        expires_in_days=14,
    )
    assert result["authorization"]["max_uses"] == 0
    decision = authorization_for(result["schedule"])
    assert decision.applies
    assert decision.allows(SCOPE_NET_ADDITIVE_DRIFT)


def test_an_approval_is_refused_once_the_plan_moved_under_it(sched):
    """The operator would be signing for something they were never shown."""
    request = _finding(sched)
    open_approval_request(sched.id, request)
    store.update_schedule(sched.id, {"mappings": [{"source": "amount", "target": "NET"}]})
    with pytest.raises(AuthorizationRefused) as excinfo:
        approve_request(
            sched.id,
            request["id"],
            actor=ACTOR,
            reason=REASON,
            acknowledgments={"schema_drift": True},
        )
    assert CODE_BINDING_CHANGED in str(excinfo.value)
    parked = store.get_schedule(sched.id)
    assert parked is not None
    assert parked.approval_request["status"] == STATUS_OPEN


def test_an_already_resolved_request_cannot_be_resolved_twice(sched):
    request = _finding(sched)
    open_approval_request(sched.id, request)
    reject_request(sched.id, request["id"], actor=ACTOR, reason="Upstream owner will fix it.")
    with pytest.raises(LookupError):
        approve_request(sched.id, request["id"], actor=ACTOR, reason=REASON)


def test_rejecting_pauses_the_schedule_rather_than_letting_it_re_refuse(sched):
    request = _finding(sched)
    open_approval_request(sched.id, request)
    result = reject_request(
        sched.id, request["id"], actor=ACTOR, reason="The upstream change is wrong."
    )
    updated = result["schedule"]
    assert updated.approval_request["status"] == STATUS_REJECTED
    assert updated.enabled is False


def test_the_inbox_lists_only_open_findings_in_the_caller_workspace(sched):
    open_approval_request(sched.id, _finding(sched))
    assert [r["schedule_id"] for r in open_approvals()] == [sched.id]
    assert open_approvals("ws-other") == []
    resolved = _finding(sched)
    reject_request(sched.id, resolved["id"], actor=ACTOR, reason="Not this change.")
    assert open_approvals() == []


# --- persistence, use counting and revocation -------------------------------


def test_authority_is_granted_against_the_live_plan_and_survives_reload(sched):
    set_standing_authorization(
        sched.id,
        actor=ACTOR,
        reason=REASON,
        scopes=[SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True},
    )
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    assert authorization_for(reloaded).applies


def test_use_is_counted_so_an_approve_once_decision_is_single_use(sched):
    request = _finding(sched)
    open_approval_request(sched.id, request)
    approve_request(
        sched.id,
        request["id"],
        actor=ACTOR,
        reason=REASON,
        acknowledgments={"schema_drift": True},
    )
    record_authorization_use(sched.id)
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    assert reloaded.standing_authorization["uses"] == 1
    decision = authorization_for(reloaded)
    assert not decision.applies
    assert decision.code == CODE_EXHAUSTED


def test_revoking_leaves_the_record_and_removes_the_authority(sched):
    set_standing_authorization(
        sched.id,
        actor=ACTOR,
        reason=REASON,
        scopes=[SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True},
    )
    revoke_standing_authorization(sched.id, actor="admin@example.com", reason="rotation")
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    assert reloaded.standing_authorization["revoked_by"] == "admin@example.com"
    assert authorization_for(reloaded).code == CODE_REVOKED


def test_a_schedule_with_no_authorization_behaves_exactly_as_before(sched):
    """Backward compatibility is the acceptance criterion, not a nice-to-have."""
    decision = authorization_for(sched)
    assert not decision.applies
    assert not decision.acknowledgments.any_claimed


def test_a_corrupted_authorization_record_is_no_authority_not_an_outage(sched):
    store.update_schedule(sched.id, {"standing_authorization": {"actor": ACTOR, "scopes": "oops"}})
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    decision = authorization_for(reloaded)
    assert not decision.applies


# --- the run path -----------------------------------------------------------


def test_a_scheduled_request_carries_attestations_only_under_a_valid_grant(sched):
    import services.schedule_runner as runner

    src = {
        "_id": "src-xlsx",
        "id": "src-xlsx",
        "type": "postgresql",
        "host": "h",
        "port": 5432,
        "database": "db",
        "schema": "public",
        "username": "u",
        "password": "p",
    }
    dst = {**src, "_id": "dst-snow", "id": "dst-snow"}

    unattested = runner.build_schedule_request(sched, src, dst)
    assert unattested.schema_drift_acknowledged is False
    assert unattested.acknowledgment_actor == ""

    set_standing_authorization(
        sched.id,
        actor=ACTOR,
        reason=REASON,
        scopes=[SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True},
    )
    authorized = store.get_schedule(sched.id)
    assert authorized is not None
    request = runner.build_schedule_request(authorized, src, dst)
    assert request.schema_drift_acknowledged is True
    assert request.acknowledgment_actor == ACTOR
    assert request.acknowledgment_reason == REASON
    # Out of scope stays out of scope, even for an authorized run.
    assert request.compliance_acknowledged is False
    assert request.fk_risk_acknowledged is False


def test_a_deterministic_refusal_before_any_row_moves_becomes_a_finding(sched, monkeypatch):
    import services.schedule_runner as runner

    monkeypatch.setattr(runner, "_resolve_connector", lambda *_a, **_k: object())
    monkeypatch.setattr(runner, "_endpoint_from_connector", lambda *_a, **_k: object())

    def _refuse(*_a: Any, **_k: Any):
        raise ApprovalRequired(
            "Source schema drift: column region was renamed",
            kind="source_drift",
            code="SOURCE_SCHEMA_DRIFT",
            corrective_action="Confirm the mapping, then accept the new shape.",
            scopes=(SCOPE_NET_ADDITIVE_DRIFT, SCOPE_SCHEMA_DRIFT),
            evidence={"summary": "column region was renamed"},
        )

    monkeypatch.setattr(runner, "build_schedule_request", _refuse)

    assert runner._dispatch_transfer(sched.id) is None
    parked = store.get_schedule(sched.id)
    assert parked is not None
    request = parked.approval_request
    assert request["status"] == STATUS_OPEN
    assert request["code"] == "SOURCE_SCHEMA_DRIFT"
    assert request["approvable"] is True
    assert request["corrective_action"]
    assert request["evidence"]["summary"] == "column region was renamed"
    # A refused run is still recorded as a failed run — nothing is dressed up.
    assert parked.run_history and parked.run_history[-1]["status"] == "failed"
    assert parked.last_status == "needs_approval"


def test_authority_is_not_spent_when_no_run_ever_started(sched, monkeypatch):
    """A single-use approval must survive a dispatch that never produced a job.

    Counting the use before the job exists would burn the one run a human
    approved on a submission that failed, and the schedule would then park again
    on a finding nobody could clear without re-approving.
    """
    import src.transfer.engine as engine_module
    from types import SimpleNamespace

    import services.schedule_runner as runner

    set_standing_authorization(
        sched.id,
        actor=ACTOR,
        reason=REASON,
        scopes=[SCOPE_SCHEMA_DRIFT],
        acknowledgments={"schema_drift": True},
    )
    monkeypatch.setattr(runner, "_resolve_connector", lambda *_a, **_k: {"id": "c"})
    monkeypatch.setattr(
        runner,
        "build_schedule_request",
        lambda *_a, **_k: SimpleNamespace(acknowledgment_actor=ACTOR),
    )
    monkeypatch.setattr(runner, "_guard_source_schema_drift", lambda *_a, **_k: False)

    class _UnavailableEngine:
        def _create_pending_job(self, _request: Any) -> str:
            raise RuntimeError("job store unavailable")

    monkeypatch.setattr(engine_module, "get_transfer_engine", lambda: _UnavailableEngine())

    with pytest.raises(RuntimeError):
        runner._dispatch_transfer(sched.id)

    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    assert reloaded.standing_authorization["uses"] == 0
    assert authorization_for(reloaded).applies


def test_a_refusal_no_approval_can_clear_parks_without_offering_a_signature(
    sched, monkeypatch
):
    import services.schedule_runner as runner

    monkeypatch.setattr(runner, "_resolve_connector", lambda *_a, **_k: object())
    monkeypatch.setattr(runner, "_endpoint_from_connector", lambda *_a, **_k: object())

    def _refuse(*_a: Any, **_k: Any):
        raise ValueError("Lossy conversion DECIMAL(18,4) to FLOAT on column amount")

    monkeypatch.setattr(runner, "build_schedule_request", _refuse)

    assert runner._dispatch_transfer(sched.id) is None
    parked = store.get_schedule(sched.id)
    assert parked is not None
    assert parked.approval_request["approvable"] is False
    assert parked.approval_request["corrective_action"]
