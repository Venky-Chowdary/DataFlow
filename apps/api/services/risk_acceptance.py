"""One way in for an operator's ``risk_acceptance`` on every route (QA r12 D2).

MCP ``plan_transfer`` / ``start_transfer`` / ``rerun_job`` / ``retry_job`` and
REST ``POST /transfer/run`` (form field), ``POST /connectors/jobs/{id}/retry``
and ``/rerun`` (JSON body) all accept the same object::

    {approved_by, reason, execution_policy, on_cast_failure?, columns?}

and sign it the same way: ``_sign_required_risk_contracts`` mints a
continue-policy Migration Risk Contract on each mapping a gate already
requires, so the rows that fail to cast are quarantined and the valid rows
load. An acceptance that is malformed, or that signs nothing, is refused
(``ValueError``) rather than silently ignored.
"""

from __future__ import annotations

import json
from typing import Any

#: Where the operator passes the acceptance, by surface. Quoted by the
#: cast-failure remediation so the message names a field that exists.
RISK_ACCEPTANCE_SURFACES = (
    "MCP plan_transfer / start_transfer / rerun_job / retry_job (risk_acceptance), "
    "REST POST /api/v1/transfer/run (form field risk_acceptance, JSON-encoded), or "
    "REST POST /api/v1/connectors/jobs/{job_id}/retry or /rerun "
    '(JSON body {"risk_acceptance": {...}})'
)


def parse_risk_acceptance_form(raw: str | None) -> dict[str, Any] | None:
    """Decode the JSON-encoded ``risk_acceptance`` form field. Blank → ``None``."""
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        value = json.loads(text)
    except ValueError as exc:
        raise ValueError(
            "risk_acceptance must be a JSON object "
            '{"approved_by", "reason", "execution_policy", ...}; it is not valid JSON.'
        ) from exc
    return validate_risk_acceptance(value)


def validate_risk_acceptance(value: Any) -> dict[str, Any] | None:
    """Check the shape with the same rules MCP and preflight sign under.

    Returns the acceptance (``None`` when empty). Raises ``ValueError`` naming
    the missing piece; nothing is signed on a refusal.
    """
    if value is None or value == {}:
        return None
    if not isinstance(value, dict):
        raise ValueError(
            "risk_acceptance must be an object {approved_by, reason, execution_policy, ...}."
        )
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    # Signing no mappings runs every field check and signs nothing.
    _sign_required_risk_contracts([], value, table="")
    return dict(value)


def sign_request_risk_acceptance(
    xfer_req: Any,
    acceptance: dict[str, Any],
    *,
    no_mappings_hint: str,
) -> list[str]:
    """Sign ``acceptance`` onto ``xfer_req.mappings``; return the columns signed.

    The same signing MCP's ``rerun_job`` performs on Confirm (D170). Raises
    ``ValueError`` when the request carries no mappings or nothing was signed.
    """
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    acceptance = validate_risk_acceptance(acceptance) or {}
    if not acceptance:
        return []
    mappings = [dict(m) for m in (xfer_req.mappings or []) if isinstance(m, dict)]
    if not mappings:
        raise ValueError(no_mappings_hint)
    named = {
        str(c).strip() for c in (acceptance.get("columns") or []) if str(c or "").strip()
    }
    table = str(
        getattr(xfer_req.destination, "table", "")
        or getattr(xfer_req.destination, "collection", "")
        or ""
    )
    signed = _sign_required_risk_contracts(
        mappings, acceptance, table=table, force_columns=named or None
    )
    gained = [
        str(new.get("source") or new.get("target") or "")
        for old, new in zip(mappings, signed)
        if new.get("risk_contract") and not old.get("risk_contract")
    ]
    if not gained:
        raise ValueError(
            "risk_acceptance signed nothing: no mapped column needs a Risk "
            "Contract (name the columns that fail in risk_acceptance.columns)."
        )
    xfer_req.mappings = signed
    return gained
