"""Organizations and plan entitlements (P0-7).

The org is the billing/plan boundary the signup flow creates: one org per
verified signup (``type=individual``), more later for teams/enterprise.
Plan limits live in ``plan_entitlements`` as *data* — nothing about the
free tier is hard-coded in checks.

Mongo collections ``organizations`` + ``plan_entitlements`` when the
metadata database is configured; JSON files under data_dir() otherwise —
same dual-backend pattern as team_store.
"""

from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime
from typing import Any

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir
from services.user_store import normalize_email

ORGANIZATIONS_COLLECTION = "organizations"
PLAN_ENTITLEMENTS_COLLECTION = "plan_entitlements"

ORG_TYPES = ("individual", "team", "enterprise")
ORG_STATUSES = ("active", "suspended")

_ORGS_FILE = "organizations.json"
_PLANS_FILE = "plan_entitlements.json"
_LOCK = threading.Lock()

# The free plan as data — the only plan that exists until billing lands.
# Numbers are the product decision from the handover: 1 workspace, 1 member,
# 3 connections, no schedule faster than hourly, a monthly row cap, no
# SSO/BYOK. Change the data, not the checks.
FREE_PLAN: dict[str, Any] = {
    "plan_tier": "free",
    "max_workspaces": 1,
    "max_members": 1,
    "max_connections": 3,
    "min_schedule_minutes": 60,
    "monthly_row_cap": 500_000,
    "sso": False,
    "byok": False,
}

_PREPARED_DATABASES: set[str] = set()


def _database():
    db = mongo_database()
    if db is None:
        return None
    if db.name not in _PREPARED_DATABASES:
        db[ORGANIZATIONS_COLLECTION].create_index([("created_by", 1)])
        db[ORGANIZATIONS_COLLECTION].create_index([("type", 1)])
        _PREPARED_DATABASES.add(db.name)
    return db


def _orgs_path():
    return data_dir() / _ORGS_FILE


def _plans_path():
    return data_dir() / _PLANS_FILE


def _read_orgs() -> list[dict[str, Any]]:
    db = _database()
    if db is not None:
        return [dict(r) for r in db[ORGANIZATIONS_COLLECTION].find({}, {"_id": False})]
    return [o for o in load_json_doc(_orgs_path(), {"organizations": []}).get("organizations", [])
            if isinstance(o, dict)]


def _write_org(doc: dict[str, Any]) -> None:
    db = _database()
    if db is not None:
        db[ORGANIZATIONS_COLLECTION].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return
    with json_doc_transaction(_orgs_path(), {"organizations": []}) as data:
        rows = [o for o in data.get("organizations", []) if o.get("id") != doc.get("id")]
        rows.append({k: v for k, v in doc.items() if k != "_id"})
        data["organizations"] = rows


def create_organization(
    *,
    name: str,
    type: str = "individual",
    created_by: str,
    plan_tier: str = "free",
    org_id: str | None = None,
) -> dict[str, Any]:
    """Create one org. ``type``/``plan_tier`` validated against known values."""
    if type not in ORG_TYPES:
        raise ValueError(f"org type must be one of {ORG_TYPES}")
    if get_plan(plan_tier) is None:
        raise ValueError(f"unknown plan_tier {plan_tier!r}")
    oid = org_id or f"org_{uuid.uuid4().hex[:16]}"
    doc: dict[str, Any] = {
        "_id": oid,
        "id": oid,
        "name": (name or "").strip() or "Personal workspace",
        "type": type,
        "plan_tier": plan_tier,
        "billing_customer_id": None,
        "status": "active",
        "created_by": normalize_email(created_by),
        "created_at": datetime.now(UTC).isoformat(),
    }
    _write_org(doc)
    return {k: v for k, v in doc.items() if k != "_id"}


def get_organization(org_id: str) -> dict[str, Any] | None:
    db = _database()
    if db is not None:
        row = db[ORGANIZATIONS_COLLECTION].find_one({"_id": org_id}, {"_id": False})
        return dict(row) if row else None
    for o in _read_orgs():
        if o.get("id") == org_id:
            return o
    return None


def organizations_for_user(email: str) -> list[dict[str, Any]]:
    email_n = normalize_email(email)
    db = _database()
    if db is not None:
        return [dict(r) for r in db[ORGANIZATIONS_COLLECTION].find({"created_by": email_n}, {"_id": False})]
    return [o for o in _read_orgs() if o.get("created_by") == email_n]


def rekey_creator(old_email: str, new_email: str) -> int:
    """Repoint ``created_by`` on every org the account owns (email change)."""
    old_n = normalize_email(old_email)
    new_n = normalize_email(new_email)
    moved = 0
    for o in _read_orgs():
        if o.get("created_by") == old_n:
            o["created_by"] = new_n
            _write_org(o)
            moved += 1
    return moved


def update_organization(org_id: str, **fields: Any) -> dict[str, Any] | None:
    """Patch allowed fields (name, plan_tier, billing_customer_id, status,
    onboarding_profile)."""
    allowed = {"name", "plan_tier", "billing_customer_id", "status",
               "onboarding_profile"}
    db = _database()
    if db is not None:
        current = db[ORGANIZATIONS_COLLECTION].find_one({"_id": org_id})
        if not current:
            return None
        for k in allowed:
            if k in fields:
                current[k] = fields[k]
        db[ORGANIZATIONS_COLLECTION].replace_one({"_id": org_id}, current)
        return {k: v for k, v in current.items() if k != "_id"}
    with json_doc_transaction(_orgs_path(), {"organizations": []}) as data:
        for o in data.get("organizations", []):
            if o.get("id") == org_id:
                for k in allowed:
                    if k in fields:
                        o[k] = fields[k]
                return dict(o)
    return None


# ── Plan entitlements ────────────────────────────────────────────────────────


def _seed_plans() -> None:
    db = _database()
    if db is not None:
        if db[PLAN_ENTITLEMENTS_COLLECTION].find_one({"_id": "free"}) is None:
            db[PLAN_ENTITLEMENTS_COLLECTION].replace_one({"_id": "free"}, {"_id": "free", **FREE_PLAN}, upsert=True)
        return
    with json_doc_transaction(_plans_path(), {"plans": {}}) as data:
        plans = data.setdefault("plans", {})
        plans.setdefault("free", dict(FREE_PLAN))


def get_plan(plan_tier: str) -> dict[str, Any] | None:
    _seed_plans()
    db = _database()
    if db is not None:
        row = db[PLAN_ENTITLEMENTS_COLLECTION].find_one({"_id": plan_tier}, {"_id": False})
        return dict(row) if row else None
    plans = load_json_doc(_plans_path(), {"plans": {}}).get("plans", {})
    plan = plans.get(plan_tier)
    return dict(plan) if isinstance(plan, dict) else None


def upsert_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Admin path: write a whole plan row (validated keys only)."""
    keys = set(FREE_PLAN)
    doc = {k: plan[k] for k in keys if k in plan}
    if "plan_tier" not in doc:
        raise ValueError("plan_tier is required")
    db = _database()
    if db is not None:
        db[PLAN_ENTITLEMENTS_COLLECTION].replace_one(
            {"_id": doc["plan_tier"]}, {"_id": doc["plan_tier"], **doc}, upsert=True)
        return doc
    with json_doc_transaction(_plans_path(), {"plans": {}}) as data:
        data.setdefault("plans", {})[doc["plan_tier"]] = doc
    return doc
