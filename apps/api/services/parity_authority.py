"""Parity Studio: who may sign which sign-off role.

A sign-off role (``engineering``, ``data_owner``, ``compliance``) is only proof
when it is bound to authority the deployment already models — never a label a
person picks from a dropdown. Datawrap's authority is the caller's gate role in
the job's workspace (:func:`services.effective_role.resolve_effective_role`:
the workspace membership role, or the platform role of an administrator), so
each sign-off role names the gate roles that may sign it, and optionally the
people (email) who may.

The mapping is explicit and configurable with ``DATAFLOW_PARITY_SIGNOFF_AUTHORITY``
(or ``DATAWRAP_…``), a JSON object::

    {"engineering": {"roles": ["operator", "editor", "admin"]},
     "data_owner":  {"roles": ["editor", "admin"], "members": ["owner@example.com"]},
     "compliance":  {"roles": ["admin"]}}

``roles`` are gate roles (viewer / operator / editor / admin); ``members``, when
given, further restricts the role to those people. A sign-off role missing from
the mapping cannot be signed. A mapping that does not parse refuses every
signature (fail closed) and says why. The route itself needs ``job.manage``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

ENV = "PARITY_SIGNOFF_AUTHORITY"
GATE_ROLES = ("viewer", "operator", "editor", "admin")

DEFAULT_SIGNOFF_AUTHORITY: dict[str, dict[str, list[str]]] = {
    # Runs and repairs pipelines in the workspace (holds job.manage).
    "engineering": {"roles": ["operator", "editor", "admin"]},
    # Owns the workspace's data and its connectors (editor or admin).
    "data_owner": {"roles": ["editor", "admin"]},
    # Administers the workspace: membership, settings, audit.
    "compliance": {"roles": ["admin"]},
}


class AuthorityConfigError(ValueError):
    """The configured sign-off authority does not parse."""


def _normalize(raw: Any) -> dict[str, dict[str, list[str]]]:
    if not isinstance(raw, dict) or not raw:
        raise AuthorityConfigError("it must be a non-empty JSON object of sign-off role -> rule")
    out: dict[str, dict[str, list[str]]] = {}
    for role, rule in raw.items():
        if not isinstance(rule, dict):
            raise AuthorityConfigError(f"the rule for {role!r} must be an object")
        roles = [str(r).strip().lower() for r in rule.get("roles") or []]
        unknown = [r for r in roles if r not in GATE_ROLES]
        if unknown or not roles:
            raise AuthorityConfigError(
                f"{role!r} needs \"roles\" from {', '.join(GATE_ROLES)} (got {roles or 'none'})"
            )
        members = [str(m).strip().lower() for m in rule.get("members") or [] if str(m).strip()]
        out[str(role).strip().lower()] = {"roles": roles, "members": members}
    return out


def signoff_authority() -> dict[str, dict[str, list[str]]]:
    """The effective mapping (defaults unless configured). Raises on a bad configuration."""
    raw = (getenv_brand(ENV, "") or "").strip()
    if not raw:
        return {k: {"roles": list(v["roles"]), "members": []} for k, v in DEFAULT_SIGNOFF_AUTHORITY.items()}
    try:
        return _normalize(json.loads(raw))
    except (json.JSONDecodeError, AuthorityConfigError) as exc:
        raise AuthorityConfigError(f"DATAFLOW_{ENV} is invalid: {exc}") from exc


def signing_refusal(signoff_role: str, *, signer_role: str, signer: str) -> str | None:
    """Why ``signer`` (gate role ``signer_role`` in the job's workspace) may not sign, or None."""
    try:
        mapping = signoff_authority()
    except AuthorityConfigError as exc:
        logger.error("parity sign-off refused: %s", exc)
        return f"Sign-off authority is misconfigured, so nobody can sign ({exc})."
    rule = mapping.get(str(signoff_role or "").lower())
    if rule is None:
        return f"No authority is configured for the {signoff_role} role, so it cannot be signed."
    role = str(signer_role or "viewer").lower()
    if role not in rule["roles"]:
        return (
            f"The {signoff_role} sign-off needs a workspace {' or '.join(rule['roles'])}; "
            f"you are {role} in this job's workspace."
        )
    if rule["members"] and str(signer or "").strip().lower() not in rule["members"]:
        return f"The {signoff_role} sign-off is limited to its named approvers, and you are not one of them."
    return None


def authority_note(signoff_role: str, signer_role: str) -> str:
    """What made this signature authoritative, as recorded on the approval."""
    rule = (signoff_authority().get(signoff_role) or {})
    named = " (named approver)" if rule.get("members") else ""
    return f"workspace {signer_role}{named}"
