"""P0-7 — organizations + plan_entitlements + entitlements.can().

Free org cannot exceed its workspace limit; limits come from the plan row
(data), not code.
"""

from __future__ import annotations

import pytest

from services import entitlements, org_store


@pytest.fixture()
def org():
    return org_store.create_organization(
        name="Ada's org", type="individual", created_by="Ada@Example.com"
    )


def test_create_individual_org_defaults(org):
    assert org["type"] == "individual"
    assert org["plan_tier"] == "free"
    assert org["billing_customer_id"] is None
    assert org["status"] == "active"
    assert org["created_by"] == "ada@example.com"  # normalized
    assert org["id"].startswith("org_")


def test_free_org_cannot_exceed_workspace_limit(org):
    assert entitlements.can(org, entitlements.WORKSPACES, count=0) is True
    assert entitlements.can(org, entitlements.WORKSPACES, count=1) is False
    assert entitlements.limit_for(org, entitlements.WORKSPACES) == 1
    with pytest.raises(entitlements.EntitlementDenied):
        entitlements.enforce(org, entitlements.WORKSPACES, count=1)


def test_free_plan_limits_are_data(org):
    plan = org_store.get_plan("free")
    assert plan["max_workspaces"] == 1
    assert plan["max_members"] == 1
    assert plan["max_connections"] == 3
    assert plan["min_schedule_minutes"] == 60
    assert plan["monthly_row_cap"] == 500_000
    assert plan["sso"] is False and plan["byok"] is False


def test_schedule_and_flags(org):
    assert entitlements.can(org, entitlements.SCHEDULE_MINUTES, value=60) is True
    assert entitlements.can(org, entitlements.SCHEDULE_MINUTES, value=15) is False
    assert entitlements.can(org, entitlements.SSO) is False
    assert entitlements.can(org, entitlements.BYOK) is False


def test_plan_change_unlocks(org):
    org_store.upsert_plan({**org_store.FREE_PLAN, "plan_tier": "team",
                           "max_workspaces": 5, "sso": True})
    org_store.update_organization(org["id"], plan_tier="team")
    assert entitlements.can(org["id"], entitlements.WORKSPACES, count=3) is True
    assert entitlements.can(org["id"], entitlements.SSO) is True
    assert entitlements.summary(org["id"])["plan_tier"] == "team"


def test_organizations_for_user(org):
    orgs = org_store.organizations_for_user("ada@example.com")
    assert org["id"] in [o["id"] for o in orgs]
    assert org_store.organizations_for_user("other@example.com") == []


def test_unknown_org_and_plan_rejected():
    assert org_store.get_organization("org_missing") is None
    assert entitlements.can("org_missing", entitlements.WORKSPACES, count=0) is False
    with pytest.raises(ValueError):
        org_store.create_organization(name="x", type="bogus", created_by="a@b.c")
    with pytest.raises(ValueError):
        org_store.create_organization(name="x", created_by="a@b.c", plan_tier="nope")
