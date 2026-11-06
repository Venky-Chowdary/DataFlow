"""Per-workspace HMAC keys for ``hash_pii`` (QA enterprise #19).

``hash_pii`` used to need ``DATAFLOW_PII_HASH_KEY`` on the API service and
refused with an error that named that variable and Railway — on production no
one had set it, so the transform could not be used at all, and the message
exposed infrastructure to a tenant.

Each workspace now has its own key, generated on first use and stored
encrypted (``services.secret_vault``, ``enc:v1``) in the control-plane database,
the same durable home round 7 gave AI keys (``integrations_persistence``); the
JSON file under the data directory is the store when no database is configured.
The same workspace always gets the same digest for the same value; two
workspaces get different digests, so hashed columns cannot be joined across
tenants.

Rotation adds a new active version and keeps the old ones (marked retired), so
an operator can tell which key produced historic digests. Rotating changes
every future digest, which is why it is an explicit admin action. A workspace
admin may also set the key (``set_workspace_key``) — to keep the digests
another system already produced, or to recover a key that can no longer be
read — from Settings → Security or ``PUT /api/v1/workspace/pii-hash-key``.
The key material goes through the vault and is never returned.

An operator who pins ``DATAFLOW_PII_HASH_KEY`` keeps exactly the digests they
had: the pinned key wins (``transform_engine.pii_hash_key``).
"""

from __future__ import annotations

import logging
import math
import re
import secrets
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "workspace_pii_hash_keys"
#: Key id for work that runs outside any workspace (system jobs, auth-off).
DEPLOYMENT_SCOPE = "__deployment__"
_LOCK = threading.Lock()


class PiiHashKeyUnavailable(RuntimeError):
    """The workspace key could not be read or created. Message is user-safe."""


class PiiHashKeyRejected(ValueError):
    """A key an admin tried to set is not acceptable. Message is user-safe."""


#: Shortest key an admin may set: 32 characters (the generated key is 64 hex).
MIN_KEY_LENGTH = 32
MAX_KEY_LENGTH = 512

#: What a tenant is told when ``hash_pii`` cannot get its key. Validate raises
#: it as a blocker and the write path refuses with it, so both say the same
#: thing (D8). It names no environment variable, path, host or vendor — only
#: the action an operator can take (QA browser 2 / security 13).
KEY_UNAVAILABLE_MESSAGE = (
    "hash_pii could not load this workspace's hashing key, so no row was hashed. "
    "A workspace admin can set or rotate the hashing key in Settings → Security, "
    "then re-run Validate. Use mask_pii or redact if a keyed digest is not required."
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(COLLECTION)
    except Exception:  # noqa: BLE001 - no database = file store
        return None


def _file_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "pii_hash_keys.json"


def _scope_id(workspace_id: str) -> str:
    return str(workspace_id or "").strip() or DEPLOYMENT_SCOPE


def _load(scope: str) -> dict[str, Any] | None:
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": scope})
        return dict(doc) if isinstance(doc, dict) else None
    from services.metadata_backend import load_json_doc

    data = load_json_doc(_file_path(), {"keys": {}})
    doc = (data.get("keys") or {}).get(scope)
    return dict(doc) if isinstance(doc, dict) else None


def _insert_if_absent(scope: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Store ``doc`` unless another writer got there first; return the winner."""
    coll = _collection()
    if coll is not None:
        from pymongo.errors import DuplicateKeyError

        try:
            coll.insert_one({"_id": scope, **doc})
        except DuplicateKeyError:
            pass
        return dict(coll.find_one({"_id": scope}) or doc)
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"keys": {}}) as data:
        keys = data.setdefault("keys", {})
        if scope not in keys:
            keys[scope] = doc
        return dict(keys[scope])


def _replace(scope: str, doc: dict[str, Any], *, expected_version: int) -> bool:
    coll = _collection()
    if coll is not None:
        result = coll.replace_one(
            {"_id": scope, "active_version": expected_version}, {"_id": scope, **doc}
        )
        return result.matched_count == 1
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"keys": {}}) as data:
        keys = data.setdefault("keys", {})
        current = keys.get(scope) or {}
        if int(current.get("active_version") or 0) != expected_version:
            return False
        keys[scope] = doc
        return True


def _new_version(version: int, actor: str, secret: str | None = None) -> dict[str, Any]:
    from services.secret_vault import encrypt_secret

    return {
        "version": version,
        "key_enc": encrypt_secret(secret or secrets.token_hex(32), label="pii_hash_key"),
        "created_at": _now(),
        "created_by": actor,
        "retired_at": None,
    }


def _active_secret(doc: dict[str, Any]) -> str:
    from services.secret_vault import decrypt_secret

    active = int(doc.get("active_version") or 0)
    for version in doc.get("versions") or []:
        if int(version.get("version") or 0) == active:
            return decrypt_secret(str(version.get("key_enc") or ""))
    return ""


_UNAVAILABLE = KEY_UNAVAILABLE_MESSAGE


def workspace_key(workspace_id: str) -> str:
    """The active key for ``workspace_id``, created on first use."""
    scope = _scope_id(workspace_id)
    try:
        doc = _load(scope)
        if doc is None:
            with _LOCK:
                doc = _load(scope) or _insert_if_absent(
                    scope,
                    {
                        "workspace_id": "" if scope == DEPLOYMENT_SCOPE else scope,
                        "active_version": 1,
                        "versions": [_new_version(1, "system")],
                        "created_at": _now(),
                    },
                )
        secret = _active_secret(doc)
    except Exception as exc:  # noqa: BLE001 - every failure is the same answer
        logger.error("pii hash key unavailable for %s: %s", scope, type(exc).__name__)
        raise PiiHashKeyUnavailable(_UNAVAILABLE) from exc
    if not secret:
        raise PiiHashKeyUnavailable(_UNAVAILABLE)
    return secret


def _add_version(workspace_id: str, *, actor: str, secret: str | None) -> dict[str, Any]:
    """Make a new version active (generated, or the admin's ``secret``)."""
    scope = _scope_id(workspace_id)
    if _load(scope) is None:
        if secret is not None:
            # First key for this workspace is the admin's own: no generated v1.
            doc = _insert_if_absent(scope, {
                "workspace_id": "" if scope == DEPLOYMENT_SCOPE else scope,
                "active_version": 1,
                "versions": [_new_version(1, actor, secret)],
                "created_at": _now(),
                "set_by_admin": True,
            })
            if _active_secret(doc) == secret:
                return key_status(workspace_id)
        else:
            workspace_key(workspace_id)  # make sure version 1 exists
    for _attempt in range(3):
        doc = _load(scope) or {}
        current = int(doc.get("active_version") or 1)
        versions = list(doc.get("versions") or [])
        for version in versions:
            if int(version.get("version") or 0) == current:
                version["retired_at"] = _now()
        versions.append(_new_version(current + 1, actor, secret))
        updated = {k: v for k, v in doc.items() if k != "_id"}
        updated.update({"active_version": current + 1, "versions": versions, "rotated_at": _now()})
        if secret is not None:
            updated["set_by_admin"] = True
        if _replace(scope, updated, expected_version=current):
            return key_status(workspace_id)
    raise PiiHashKeyUnavailable(_UNAVAILABLE)


def rotate_workspace_key(workspace_id: str, *, actor: str) -> dict[str, Any]:
    """Start a new generated key version; earlier versions stay, marked retired."""
    return _add_version(workspace_id, actor=actor, secret=None)


#: What a key an admin sets must carry: about 128 bits. The rule, in the words
#: Settings → Security shows (apps/web lib/piiHashKeyApi.ts PII_HASH_KEY_RULE).
KEY_RULE = (
    "Use at least 128 random bits: 32 or more hex characters, base64 of 16 or more "
    "random bytes (24+ characters), or 32+ characters of random text — no repeated "
    "or sequential runs, no spaces at the ends."
)
_HEX = re.compile(r"[0-9a-fA-F]+")
_B64 = re.compile(r"[A-Za-z0-9+/_-]+={0,2}")


def _sequential_run(text: str, length: int = 6) -> bool:
    run = 1
    for a, b in zip(text, text[1:]):
        run = run + 1 if abs(ord(b) - ord(a)) == 1 else 1
        if run >= length:
            return True
    return False


def _patterned(text: str) -> bool:
    """Repeated units, low 4-gram variety or ascending/descending runs."""
    if (text + text).find(text, 1) < len(text):
        return True  # the whole key is one unit repeated
    grams = [text[i:i + 4] for i in range(len(text) - 3)]
    if grams and len(set(grams)) < 0.75 * len(grams):
        return True
    return _sequential_run(text) or bool(re.search(r"(.)\1{4}", text))


def _shannon_bits(text: str) -> float:
    counts: dict[str, int] = {}
    for ch in text:
        counts[ch] = counts.get(ch, 0) + 1
    n = len(text)
    return -n * sum((c / n) * math.log2(c / n) for c in counts.values()) if n else 0.0


def key_strength_problem(text: str) -> str:
    """Why ``text`` is not an acceptable hash_pii key, or ``""`` (QA review #6).

    A weak HMAC key lets a dictionary recover the hashed PII, so the bar is the
    generated key's: about 128 bits. Encoded random bytes (hex, base64) count
    their encoding's bits; other text counts its Shannon estimate. Repeated or
    sequential patterns are refused whatever their length.
    """
    if text != text.strip() or not text:
        return "A hashing key cannot be empty or start or end with spaces. " + KEY_RULE
    if len(text) > MAX_KEY_LENGTH:
        return f"A hashing key is at most {MAX_KEY_LENGTH} characters."
    if _HEX.fullmatch(text):
        bits = 4 * len(text)
    elif _B64.fullmatch(text):
        bits = 6 * len(text.rstrip("="))
        if len(text) < 24:
            bits = 0
    else:
        bits = int(_shannon_bits(text)) if len(text) >= MIN_KEY_LENGTH else 0
    if bits < 128 or _patterned(text):
        return "That hashing key is too guessable. " + KEY_RULE + " Or press Rotate to have Datawrap generate one."
    return ""


def set_workspace_key(workspace_id: str, secret: str, *, actor: str) -> dict[str, Any]:
    """Make ``secret`` the workspace's active hash_pii key (a new version).

    For an admin who must keep digests another system produced, or whose
    stored key can no longer be read. Earlier versions stay, marked retired.
    The key is stored through the vault and never returned or logged.
    """
    text = str(secret or "")
    problem = key_strength_problem(text)
    if problem:
        raise PiiHashKeyRejected(problem)
    try:
        return _add_version(workspace_id, actor=actor, secret=text)
    except PiiHashKeyUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - the vault refused: same answer
        logger.error("pii hash key set failed: %s", type(exc).__name__)
        raise PiiHashKeyUnavailable(_UNAVAILABLE) from exc


def key_status(workspace_id: str) -> dict[str, Any]:
    """Public facts about the key: versions and dates, never key material."""
    doc = _load(_scope_id(workspace_id)) or {}
    return {
        "workspace_id": str(workspace_id or ""),
        "configured": bool(doc),
        # The deployment pins one key for every workspace: setting or rotating
        # the workspace key would not change any digest, so the UI says so.
        "managed_by_deployment": bool(pinned_key()),
        "set_by_admin": bool(doc.get("set_by_admin")),
        "active_version": int(doc.get("active_version") or 0),
        "versions": [
            {k: v.get(k) for k in ("version", "created_at", "created_by", "retired_at")}
            for v in doc.get("versions") or []
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# One key per job (QA review #7)
# ─────────────────────────────────────────────────────────────────────────────
#
# ``transform_engine`` resolved the key for every value, so a rotation while a
# job ran gave one job two digests for the same value, and nothing recorded
# which key a job used. A job now snapshots the key and its version once
# (``job_key_scope``); every digest in the job uses it, the job records
# ``pii_hash_key_version``, the route's load history keeps it, and Validate
# warns when the workspace key changed since this route's last run (upserts
# keyed on a hashed column would stop matching earlier rows).

_JOB_KEY: ContextVar[dict[str, str] | None] = ContextVar("pii_hash_job_key", default=None)
DEPLOYMENT_VERSION = "deployment"


def pinned_key() -> str:
    """The operator-pinned key that every workspace uses, or ``""``."""
    from services.brand_env import getenv_brand

    return getenv_brand("PII_HASH_KEY") or getenv_brand("SECRET") or ""


def active_key(workspace_id: str) -> tuple[str, str]:
    """``(secret, version)`` in force now: the pinned key, else the workspace's."""
    pinned = pinned_key()
    if pinned:
        return pinned, DEPLOYMENT_VERSION
    secret = workspace_key(workspace_id)
    doc = _load(_scope_id(workspace_id)) or {}
    return secret, str(int(doc.get("active_version") or 1))


def current_version(workspace_id: str) -> str:
    """The version a run would hash with now, ``""`` when no key is readable."""
    try:
        return active_key(workspace_id)[1]
    except PiiHashKeyUnavailable:
        return ""


def snapshot_secret(workspace_id: str) -> str | None:
    """The key this job snapshotted for ``workspace_id``, or ``None`` outside a job."""
    snap = _JOB_KEY.get()
    if snap is None or snap["scope"] != _scope_id(workspace_id):
        return None
    return snap["secret"]


def snapshot_version() -> str:
    snap = _JOB_KEY.get()
    return snap["version"] if snap else ""


@contextmanager
def job_key_scope(workspace_id: str, *, uses_hash_pii: bool):
    """Hold one key for the whole job; yield its version (``""`` when unused)."""
    if not uses_hash_pii:
        yield ""
        return
    try:
        secret, version = active_key(workspace_id)
    except PiiHashKeyUnavailable:
        secret, version = "", ""  # the transform still fails closed per value
    token = _JOB_KEY.set({"scope": _scope_id(workspace_id), "secret": secret, "version": version})
    try:
        yield version
    finally:
        _JOB_KEY.reset(token)


@contextmanager
def transfer_job_scope(request: Any, job_id: str, *, store: Any):
    """``job_key_scope`` for a transfer, stamping ``pii_hash_key_version`` on the job."""
    import json

    try:
        from src.transfer.models import transfer_request_to_dict

        uses = '"hash_pii"' in json.dumps(transfer_request_to_dict(request), default=str)
    except Exception:  # noqa: BLE001 - unreadable request: resolve per value as before
        uses = False
    workspace_id = str(getattr(request, "workspace_id", "") or "")
    with job_key_scope(workspace_id, uses_hash_pii=uses) as version:
        if version:
            try:
                store().update_job_fields(
                    job_id, {"pii_hash_key_version": version}, touch_updated_at=False
                )
            except Exception:  # noqa: BLE001 - the stamp is evidence, not the run
                logger.warning("could not stamp pii_hash_key_version on %s", job_id, exc_info=True)
        yield version


def key_change_warning(workspace_id: str, last_version: str) -> dict[str, Any] | None:
    """The Validate warning when the key changed since this route's last run."""
    now = current_version(workspace_id)
    if not last_version or not now or str(last_version) == now:
        return None
    return {
        "id": "pii_hash_key_changed",
        "message": (
            f"hash_pii digests will differ from this route's last run: it used key "
            f"version {last_version}, the workspace key is now version {now}. Rows "
            "upserted or joined on a hashed column will not match the earlier rows."
        ),
        "details": {"last_version": str(last_version), "current_version": now},
    }
