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


def _active_doc(workspace_id: str) -> dict[str, Any]:
    """The workspace's key document, created on first use (one read)."""
    scope = _scope_id(workspace_id)
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
    return doc


def _workspace_key_and_version(workspace_id: str) -> tuple[str, str]:
    """``(secret, version)`` from ONE read of the key document (review S1)."""
    scope = _scope_id(workspace_id)
    try:
        doc = _active_doc(workspace_id)
        secret = _active_secret(doc)
    except Exception as exc:  # noqa: BLE001 - every failure is the same answer
        logger.error("pii hash key unavailable for %s: %s", scope, type(exc).__name__)
        raise PiiHashKeyUnavailable(_UNAVAILABLE) from exc
    if not secret:
        raise PiiHashKeyUnavailable(_UNAVAILABLE)
    return secret, str(int(doc.get("active_version") or 1))


def workspace_key(workspace_id: str) -> str:
    """The active key for ``workspace_id``, created on first use."""
    return _workspace_key_and_version(workspace_id)[0]


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
    "Paste random key material: 32 or more hex characters, or base64 of 16 or more "
    "random bytes (for example the output of openssl rand -hex 32). Passwords, "
    "phrases and hashes of them are refused; press Rotate to have Datawrap "
    "generate a key."
)
_HEX = re.compile(r"(?:[0-9a-fA-F]{2})+")
_B64 = re.compile(r"[A-Za-z0-9+/]+={0,2}|[A-Za-z0-9_-]+={0,2}")
#: Decoded bytes a key must carry: 16 random bytes are 128 bits.
MIN_KEY_BYTES = 16


def _decoded_key_bytes(text: str) -> bytes | None:
    """The bytes a hex or base64 key encodes, or ``None`` when it is neither."""
    import base64
    import binascii

    if _HEX.fullmatch(text):
        return bytes.fromhex(text)
    if not _B64.fullmatch(text):
        return None
    padded = text + "=" * (-len(text) % 4)
    try:
        if "-" in text or "_" in text:
            return base64.urlsafe_b64decode(padded)
        return base64.b64decode(padded, validate=True)
    except (binascii.Error, ValueError):
        return None


def _looks_like_text(data: bytes) -> bool:
    """Printable text hiding inside an encoding (hex of "password12345678")."""
    printable = sum(1 for b in data if 32 <= b <= 126 or b in (9, 10, 13))
    # Random bytes are all printable with probability (97/256)^n: demand every
    # byte for a short key, nine in ten for a long one.
    return printable == len(data) if len(data) < 32 else printable >= 0.9 * len(data)


def _byte_pattern(data: bytes) -> bool:
    """Repeated or stepped bytes (00 00 …, 00 01 02 …, ab ab …): not random."""
    if len(set(data)) < 0.6 * min(len(data), 64):
        return True
    steps = [(b - a) % 256 for a, b in zip(data, data[1:])]
    if not steps:
        return True
    most = max(steps.count(step) for step in set(steps))
    return most > len(steps) // 2


def _typed_run(text: str, length: int = 6) -> bool:
    """An ascending/descending character run ("abcdef", "543210") in base64 text.

    Random base64 shows one with probability about 27 * (2/64)^5 per key; a
    typed key shows one routinely. Hex is not judged this way (16 symbols make
    such runs common in random hex).
    """
    run = 1
    for a, b in zip(text, text[1:]):
        run = run + 1 if abs(ord(b) - ord(a)) == 1 else 1
        if run >= length:
            return True
    return False


def key_strength_problem(text: str) -> str:
    """Why ``text`` is not an acceptable hash_pii key, or ``""`` (review B1).

    A weak HMAC key lets a dictionary recover the hashed PII. Only encoded
    random bytes are accepted: hex or base64 of at least 16 bytes that are not
    printable text, not a repeating or stepped pattern, and not the MD5 /
    SHA-1 / SHA-256 digest of a common password. Free text — passwords,
    passphrases, English — is refused outright (no estimator credits it); an
    admin without random material presses Rotate.
    """
    from services.common_password_digests import common_password_digests, spells_common_word

    refused = "That hashing key is not random key material. " + KEY_RULE
    if not text or text != text.strip():
        return "A hashing key cannot be empty or start or end with spaces. " + KEY_RULE
    if len(text) > MAX_KEY_LENGTH:
        return f"A hashing key is at most {MAX_KEY_LENGTH} characters."
    data = _decoded_key_bytes(text)
    if data is None or len(data) < MIN_KEY_BYTES:
        return refused
    if _looks_like_text(data) or _byte_pattern(data):
        return refused
    if not _HEX.fullmatch(text) and (spells_common_word(text) or _typed_run(text)):
        return refused  # a phrase or keyboard run that happens to be valid base64
    if data in common_password_digests():
        return "That hashing key is the digest of a common password. " + KEY_RULE
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

DEPLOYMENT_VERSION = "deployment"


def pinned_key() -> str:
    """The operator-pinned key that every workspace uses, or ``""``."""
    from services.brand_env import getenv_brand

    return getenv_brand("PII_HASH_KEY") or getenv_brand("SECRET") or ""


def pinned_version(pinned: str) -> str:
    """A version id for the pinned key that changes when the key does (review S2)."""
    import hashlib
    import hmac

    return f"{DEPLOYMENT_VERSION}:{hmac.new(pinned.encode(), b'version-id', hashlib.sha256).hexdigest()[:8]}"


def active_key(workspace_id: str) -> tuple[str, str]:
    """``(secret, version)`` in force now: the pinned key, else the workspace's."""
    pinned = pinned_key()
    if pinned:
        return pinned, pinned_version(pinned)
    return _workspace_key_and_version(workspace_id)


def secret_for_version(workspace_id: str, version: str) -> str:
    """The secret of a recorded key ``version`` (retired versions are kept), or ``""``.

    A replay of a job's quarantine digests with the parent job's key (review
    B2). A pinned deployment key is reproducible only while it is unchanged.
    """
    text = str(version or "")
    pinned = pinned_key()
    if text.startswith(DEPLOYMENT_VERSION):
        return pinned if pinned and pinned_version(pinned) == text else ""
    if pinned:
        return ""
    try:
        want = int(text)
    except ValueError:
        return ""
    from services.secret_vault import decrypt_secret

    doc = _load(_scope_id(workspace_id)) or {}
    for entry in doc.get("versions") or []:
        if int(entry.get("version") or 0) == want:
            try:
                return decrypt_secret(str(entry.get("key_enc") or ""))
            except Exception:  # noqa: BLE001 - unreadable: fail closed
                return ""
    return ""


def current_version(workspace_id: str) -> str:
    """The version a run would hash with now, ``""`` when no key is readable."""
    try:
        return active_key(workspace_id)[1]
    except PiiHashKeyUnavailable:
        return ""


class _JobKey:
    """One job's key, resolved lazily on its first digest (review B3).

    Every spelling of the transform (hash_pii, sha256, HASH_PII, a chain, a
    recipe step) ends in ``transform_engine.pii_hash_key``; resolving there,
    instead of guessing from the request's text, snapshots exactly the jobs
    that hash. Shared by reference with worker threads (context copies).
    """

    def __init__(self, workspace_id: str, *, version: str | None, on_resolve: Any) -> None:
        self.workspace_id = workspace_id
        self.scope = _scope_id(workspace_id)
        self.pin = version or None
        self.on_resolve = on_resolve
        self.lock = threading.Lock()
        self.resolved = False
        self.secret = ""
        self.version = ""
        self.checked_at = 0.0
        self.history: list[str] = []

    def _adopt(self, secret: str, version: str) -> None:
        self.secret, self.version = secret, version
        if version and version not in self.history:
            self.history.append(version)
            if self.on_resolve is not None:
                self.on_resolve(version, list(self.history))

    def resolve(self) -> str:
        if self.resolved:
            return self.secret
        with self.lock:
            if not self.resolved:
                import time as _time

                if self.pin:
                    secret, version = secret_for_version(self.workspace_id, self.pin), self.pin
                else:
                    try:
                        secret, version = active_key(self.workspace_id)
                    except PiiHashKeyUnavailable:
                        secret, version = "", ""  # the transform fails closed per value
                self._adopt(secret, version)
                self.checked_at = _time.monotonic()
                self.resolved = True
        return self.secret

    def renew(self, max_age_s: float) -> None:
        """Adopt a rotated key at a batch boundary (review S6); pinned jobs never move."""
        import time as _time

        if not self.resolved or self.pin or _time.monotonic() - self.checked_at < max_age_s:
            return
        with self.lock:
            self.checked_at = _time.monotonic()
            try:
                secret, version = active_key(self.workspace_id)
            except PiiHashKeyUnavailable:
                return
            if version != self.version:
                self._adopt(secret, version)


_JOB_KEY: ContextVar[_JobKey | None] = ContextVar("pii_hash_job_key", default=None)


def snapshot_secret(workspace_id: str) -> str | None:
    """The key this job holds for ``workspace_id``, or ``None`` outside a job."""
    snap = _JOB_KEY.get()
    if snap is None or snap.scope != _scope_id(workspace_id):
        return None
    return snap.resolve()


def snapshot_version() -> str:
    snap = _JOB_KEY.get()
    return snap.version if snap is not None and snap.resolved else ""


def renew_job_key_at_boundary(max_age_s: float = 60.0) -> None:
    """A long job (continuous CDC) adopts a rotated key between batches."""
    snap = _JOB_KEY.get()
    if snap is not None:
        snap.renew(max_age_s)


@contextmanager
def job_key_scope(workspace_id: str, *, version: str | None = None, on_resolve: Any = None):
    """One key for the whole job, resolved on its first digest; pinned to ``version`` if given."""
    snap = _JobKey(workspace_id, version=version, on_resolve=on_resolve)
    token = _JOB_KEY.set(snap)
    try:
        yield snap
    finally:
        _JOB_KEY.reset(token)


@contextmanager
def transfer_job_scope(request: Any, job_id: str, *, store: Any, version: str | None = None):
    """``job_key_scope`` for a job, stamping ``pii_hash_key_version`` when it first hashes."""

    def stamp(current: str, history: list[str]) -> None:
        try:
            store().update_job_fields(
                job_id,
                {"pii_hash_key_version": current, "pii_hash_key_versions": history},
                touch_updated_at=False,
            )
        except Exception:  # noqa: BLE001 - the stamp is evidence, not the run
            logger.warning("could not stamp pii_hash_key_version on %s", job_id, exc_info=True)

    workspace_id = str(getattr(request, "workspace_id", "") or "")
    with job_key_scope(workspace_id, version=version, on_resolve=stamp) as snap:
        yield snap


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
