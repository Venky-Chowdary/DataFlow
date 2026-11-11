"""Seal Pilot's short-lived state before it touches disk or a shared collection.

The result store spills sampled rows and the ack ledger spills staged
connector payloads (passwords, connection strings) so that a second worker or
a restarted API can serve the follow-up. Both wrote them in plaintext (audit
H5, L4). They are now sealed with the same vault that protects saved connector
credentials (``services.secret_vault``) — no second key, no second scheme —
and each record carries who staged it and in which workspace, which is what a
read is checked against.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from services.value_serializer import json_default
from services.workspace_filter import bound_allows, current_actor, current_workspace_stamp

_log = logging.getLogger(__name__)
_LABEL = "pilot-state"


def seal(value: Any) -> str | None:
    """Ciphertext for ``value``, or None when the vault cannot seal it.

    None means "do not persist this": the caller keeps the value in memory
    only, which is the safe failure for a production vault with no key.
    """
    from services.secret_vault import encrypt_secret

    try:
        # Wrapped in an object: the vault passes strings that look like a
        # placeholder ("[...]", "****") through untouched.
        return encrypt_secret(json.dumps({"v": value}, default=json_default), label=_LABEL)
    except Exception as exc:  # noqa: BLE001 - never fall back to plaintext on disk
        _log.warning("pilot state could not be sealed (%s); keeping it in memory only", type(exc).__name__)
        return None


def unseal(token: Any) -> Any:
    """The value sealed by :func:`seal`, or None when it cannot be opened."""
    from services.secret_vault import decrypt_secret

    if not isinstance(token, str) or not token:
        return None
    try:
        return json.loads(decrypt_secret(token)).get("v")
    except Exception as exc:  # noqa: BLE001 - a record sealed under another key is simply gone
        _log.warning("pilot state could not be unsealed (%s)", type(exc).__name__)
        return None


def owner_stamp() -> dict[str, str]:
    """Who is staging this record and in which workspace (from the bound request)."""
    return {
        "workspace_id": current_workspace_stamp(for_write=False) or "",
        "actor": current_actor() or "",
    }


def readable_by_caller(doc: dict[str, Any], *, same_actor: bool) -> bool:
    """Whether the bound caller may read a record staged with :func:`owner_stamp`.

    The record's workspace must be one the caller may read. With
    ``same_actor`` the record is private to the principal that staged it. A
    caller with no bound identity (system scope, auth switched off) is not
    held to a stamp nobody could have set.
    """
    workspace = str(doc.get("workspace_id") or "")
    # A record staged by a request that acted in no workspace carries no
    # workspace to check; it is still private to its actor below.
    if workspace and not bound_allows(workspace):
        return False
    if not same_actor:
        return True
    owner, caller = str(doc.get("actor") or ""), current_actor() or ""
    return not owner or not caller or owner == caller
