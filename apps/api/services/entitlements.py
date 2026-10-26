"""Plan entitlement checks — the ONE place limits are enforced (P0-7).

``can(org, feature, ...)`` is the seam every limit goes through; the limits
themselves are data in ``plan_entitlements`` (see ``org_store.FREE_PLAN``).
Callers pass the current count/value; the service compares against the
plan row. ``enforce`` raises :class:`EntitlementDenied` for API surfaces to
map to 403.
"""

from __future__ import annotations

from typing import Any

from services import org_store


class EntitlementDenied(Exception):
    """Plan limit reached. ``code`` maps into the error envelope."""

    def __init__(self, feature: str, plan_tier: str, message: str) -> None:
        super().__init__(message)
        self.feature = feature
        self.plan_tier = plan_tier
        self.code = "plan_limit_reached"


# Feature names (stable API surface — map to plan_entitlements columns).
WORKSPACES = "workspaces"
MEMBERS = "members"
CONNECTIONS = "connections"
SCHEDULE_MINUTES = "schedule_minutes"
MONTHLY_ROWS = "monthly_rows"
SSO = "sso"
BYOK = "byok"

_NUMERIC_FEATURES = {
    WORKSPACES: "max_workspaces",
    MEMBERS: "max_members",
    CONNECTIONS: "max_connections",
    MONTHLY_ROWS: "monthly_row_cap",
}


def _resolve_org(org: str | dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(org, dict):
        return org
    return org_store.get_organization(org)


def _plan(org: dict[str, Any]) -> dict[str, Any]:
    plan = org_store.get_plan(str(org.get("plan_tier") or "free"))
    return plan or dict(org_store.FREE_PLAN)


def can(org: str | dict[str, Any], feature: str, *, count: int | None = None,
        value: int | float | None = None) -> bool:
    """Is ``feature`` allowed for this org's plan?

    - count-based features (workspaces/members/connections/monthly_rows):
      pass ``count`` = how many already exist → allowed when count < max.
    - ``schedule_minutes``: pass ``value`` = requested minutes → allowed
      when >= the plan's min_schedule_minutes.
    - flag features (sso/byok): plan boolean.
    """
    doc = _resolve_org(org)
    if doc is None:
        return False
    plan = _plan(doc)

    if feature in _NUMERIC_FEATURES:
        limit = plan.get(_NUMERIC_FEATURES[feature])
        if limit is None:  # unlimited
            return True
        if count is None:
            return True  # "does the plan carry the capability" form
        return count < int(limit)
    if feature == SCHEDULE_MINUTES:
        minimum = int(plan.get("min_schedule_minutes") or 0)
        if value is None:
            return True
        return float(value) >= minimum
    if feature in (SSO, BYOK):
        return bool(plan.get(feature))
    return False


def limit_for(org: str | dict[str, Any], feature: str) -> int | None:
    """The numeric cap for a count-based feature (None = unlimited)."""
    doc = _resolve_org(org)
    if doc is None:
        return None
    column = _NUMERIC_FEATURES.get(feature)
    if column is None:
        return None
    limit = _plan(doc).get(column)
    return int(limit) if limit is not None else None


def enforce(org: str | dict[str, Any], feature: str, *, count: int | None = None,
            value: int | float | None = None) -> None:
    """Raise EntitlementDenied when ``can`` is false."""
    if can(org, feature, count=count, value=value):
        return
    doc = _resolve_org(org) or {}
    tier = str(doc.get("plan_tier") or "free")
    cap = limit_for(doc, feature) if feature in _NUMERIC_FEATURES else None
    detail = f"plan {tier!r} does not allow {feature}"
    if cap is not None:
        detail = f"plan {tier!r} allows at most {cap} {feature}"
    raise EntitlementDenied(feature, tier, detail)


def summary(org: str | dict[str, Any]) -> dict[str, Any]:
    """Plan + limits for /auth/me and admin surfaces."""
    doc = _resolve_org(org)
    if doc is None:
        return {}
    plan = _plan(doc)
    return {
        "org_id": doc.get("id"),
        "org_name": doc.get("name"),
        "org_type": doc.get("type"),
        "plan_tier": doc.get("plan_tier"),
        "entitlements": plan,
    }
