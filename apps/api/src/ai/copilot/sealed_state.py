"""Seal Pilot's short-lived state before it touches disk or a shared collection.

The result store spills sampled rows and the ack ledger spills staged
connector payloads (passwords, connection strings) so that a second worker or
a restarted API can serve the follow-up. Both wrote them in plaintext (audit
H5, L4). They are sealed with a local Fernet data key derived (HKDF, its own
purpose label) from the key that protects saved connector credentials — never
handed to the configured vault backend: with ``SECRETS_BACKEND=aws_secretsmanager``
that backend turned every sampled row set into a permanent AWS secret (review
H4). With no local key, state is kept in memory only. Each record carries who
staged it and in which workspace, which is what a read is checked against.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any

from services.value_serializer import json_default
from services.workspace_filter import (
    bound_allows,
    current_actor,
    current_workspace_filter,
    current_workspace_stamp,
)

_log = logging.getLogger(__name__)
_PREFIX = "pilot:v1:"
_KEY_INFO = b"datawrap-pilot-state-v1"


def _data_key() -> Any:
    """Fernet over a key derived for Pilot state only; raises when there is none."""
    from cryptography.fernet import Fernet

    from services.secret_vault import _fernet_key_material, _hkdf_32

    root = base64.urlsafe_b64decode(_fernet_key_material())
    return Fernet(base64.urlsafe_b64encode(_hkdf_32(root, info=_KEY_INFO)))


def seal(value: Any) -> str | None:
    """Ciphertext for ``value``, or None when the vault cannot seal it.

    None means "do not persist this": the caller keeps the value in memory
    only, which is the safe failure for a production vault with no key.
    """
    try:
        plain = json.dumps({"v": value}, default=json_default).encode("utf-8")
        return _PREFIX + _data_key().encrypt(plain).decode("ascii")
    except Exception as exc:  # noqa: BLE001 - never fall back to plaintext on disk
        _log.warning("pilot state could not be sealed (%s); keeping it in memory only", type(exc).__name__)
        return None


def unseal(token: Any) -> Any:
    """The value sealed by :func:`seal`, or None when it cannot be opened."""
    if not isinstance(token, str) or not token.startswith(_PREFIX):
        # Including state sealed by an earlier release through the vault: it is
        # simply gone, like state sealed under a rotated key.
        return None
    try:
        plain = _data_key().decrypt(token[len(_PREFIX):].encode("ascii"))
        return json.loads(plain.decode("utf-8")).get("v")
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
    if owner and caller:
        return owner == caller
    # Fail closed when either side has no actor (review M7). Only a process
    # with no bound identity at all (auth off, a script) reads a record that
    # nobody's identity could have stamped.
    return not owner and not caller and current_workspace_filter() is None
