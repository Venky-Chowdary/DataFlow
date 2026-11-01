"""The audit chain's own signing key, named on every record by a key id (D221).

Before D221 every chain record was HMAC-ed with the login/JWT secret. Rotating
the login secret (the routine response to a suspected token leak) therefore
turned every older record into one that reads as "altered", and the audit log
was only as safe as the most widely held secret on the platform.

Now:

* ``DATAWRAP_AUDIT_HMAC_KEY`` / ``DATAFLOW_AUDIT_HMAC_KEY`` is the chain key.
  Set the same value on every process that appends (API, worker, scheduler,
  CDC worker).
* Each record carries ``key_id``: a fingerprint of the key that signed it (a
  truncated SHA-256 under a fixed label, never the key). Verification picks
  the key by that id, so a rotation leaves earlier records verifiable as long
  as the old key stays listed in ``..._AUDIT_HMAC_RETIRED_KEYS``
  (comma-separated).
* Records written before D221 carry no ``key_id``. They verify under the
  legacy key — the login secret, read exactly as before — or under any key in
  the retired list (which is where a rotated-away login secret goes).
* **Unset key, control-plane database configured** (QA r12 #3): the key is
  generated once and kept, encrypted, in the database's ``audit_chain_keys``
  keyring (``services.audit_chain_keystore``). Every process on every host
  signs with it, and it survives deploys. Each key ever used stays in the
  keyring by ``key_id``; :func:`rotate_signing_key` starts a new one. A key
  file left by the scheme below is imported on first use.
* **Unset key, no database:** the login secret is *not* used. The process
  generates a random key once and keeps it at ``<data dir>/audit_chain.key``
  (mode 0600), so processes sharing that data volume share it. Processes on
  other hosts generate their own, and verification then reports their records
  as ``unverifiable_key_lost`` — never as tampering. A warning is logged once
  per process.

Verification looks a record's ``key_id`` up in: the configured key, the
retired keys, the persisted keyring, this node's key file. A record whose key
is in none of them cannot be checked; it is reported as
``unverifiable_key_lost``, separately from tampering, and the report is
``verified_with_exceptions``, never ``verified``. If the lost key turns up (an
old data volume, a copied ``audit_chain.key``), the owner adds its contents to
``DATAFLOW_AUDIT_HMAC_RETIRED_KEYS`` and those records verify again.
"""

from __future__ import annotations

import hashlib
import hmac as hmac_mod
import json
import logging
import os
import secrets
import threading
from pathlib import Path
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

KEY_ENV = "AUDIT_HMAC_KEY"
RETIRED_ENV = "AUDIT_HMAC_RETIRED_KEYS"
GENERATED_KEY_FILE = "audit_chain.key"
#: Shown for records that carry no ``key_id`` (written before D221).
LEGACY_KEY_LABEL = "legacy (no key_id)"

_KEY_ID_LABEL = b"datawrap-audit-chain-key-id\x00"
_lock = threading.Lock()
_generated: dict[str, bytes] = {}
_warned: set[str] = set()


def hmac_hex(payload: dict[str, Any], secret: bytes) -> str:
    """HMAC-SHA256 over the canonical JSON of ``payload`` minus ``_id``/``event_hash``."""
    body = {k: v for k, v in payload.items() if k not in ("_id", "event_hash")}
    canon = json.dumps(body, sort_keys=True, separators=(",", ":"), default=json_default)
    return hmac_mod.new(secret, canon.encode("utf-8"), hashlib.sha256).hexdigest()


def key_id_for(secret: bytes) -> str:
    """Public fingerprint of a key. Identifies it; reveals nothing usable."""
    return "ak_" + hashlib.sha256(_KEY_ID_LABEL + secret).hexdigest()[:16]


def _env(name: str) -> str:
    from services.brand_env import getenv_brand

    return (getenv_brand(name, "") or "").strip()


def legacy_login_secret() -> bytes:
    """The key records written before D221 were signed with (the login secret)."""
    try:
        from services.auth_service import _token_secret

        raw = _token_secret()
        return raw if isinstance(raw, bytes) else str(raw or "").encode("utf-8")
    except Exception:  # noqa: BLE001 - same fallback the pre-D221 writer used
        return (_env("AUTH_SECRET") or "dev-only-not-for-production").encode("utf-8")


def retired_keys() -> list[bytes]:
    raw = _env(RETIRED_ENV)
    return [part.strip().encode("utf-8") for part in raw.split(",") if part.strip()]


def _generated_key_path(create: bool) -> Path:
    from services.platform_config import data_dir, data_dir_path

    return (data_dir() if create else data_dir_path()) / GENERATED_KEY_FILE


def _read_generated(path: Path) -> bytes | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return text.encode("utf-8") if text else None


def _generated_key(*, create: bool) -> bytes | None:
    """The node-local key used when no chain key is configured.

    Created atomically (write a temp file, then ``link`` it into place, which
    fails if another process won), so processes sharing a volume agree.
    """
    path = _generated_key_path(create)
    cached = _generated.get(str(path))
    if cached is not None and path.exists():
        return cached
    key = _read_generated(path)
    if key is None and create:
        with _lock:
            key = _read_generated(path)
            if key is None:
                tmp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}")
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as fh:
                        fh.write(secrets.token_urlsafe(48))
                    try:
                        os.link(tmp, path)
                    except FileExistsError:
                        pass
                finally:
                    tmp.unlink(missing_ok=True)
                key = _read_generated(path)
    if key is not None:
        _generated[str(path)] = key
    return key


def _persisted_current() -> tuple[str, bytes] | None:
    """The shared keyring's current key, or ``None`` when there is no database."""
    from services import audit_chain_keystore as store

    try:
        local = _generated_key(create=False)
    except OSError:
        local = None
    try:
        return store.current_key(local)
    except store.KeystoreUnavailable as exc:
        logger.warning("audit chain keyring unavailable, using the node-local key: %s", exc)
        return None


def signing_key() -> tuple[str, bytes, str]:
    """``(key_id, key, source)`` for new records.

    ``source`` is ``env``, ``persisted`` (the shared keyring) or ``generated_local``.
    """
    configured = _env(KEY_ENV)
    if configured:
        key = configured.encode("utf-8")
        return key_id_for(key), key, "env"
    persisted = _persisted_current()
    if persisted is not None:
        return persisted[0], persisted[1], "persisted"
    try:
        key = _generated_key(create=True)
    except OSError as exc:  # read-only data dir: never fail the audit write over it
        key = None
        logger.warning("audit chain key file could not be written: %s", exc)
    if key is None:
        # Process-local last resort; its records verify only in this process
        # and show elsewhere as unverifiable — still never the login secret.
        key = _generated.setdefault("<process>", secrets.token_urlsafe(48).encode("utf-8"))
    path = str(_generated_key_path(create=False))
    if path not in _warned:
        _warned.add(path)
        logger.warning(
            "DATAFLOW_AUDIT_HMAC_KEY is not set and no control-plane database holds the "
            "audit keyring: the audit chain is signed with a key generated on this node "
            "(%s). Set the same DATAFLOW_AUDIT_HMAC_KEY on every process that writes "
            "audit records so their records verify everywhere.",
            path,
        )
    return key_id_for(key), key, "generated_local"


def rotate_signing_key(*, actor: str) -> dict[str, Any]:
    """Start a new chain key in the shared keyring; the old one keeps verifying.

    With a configured key the configured key keeps signing: rotate it by
    setting a new ``..._AUDIT_HMAC_KEY`` and moving the old value to
    ``..._AUDIT_HMAC_RETIRED_KEYS``. The result says which applies.
    """
    from services import audit_chain_keystore as store

    try:
        local = _generated_key(create=False)
    except OSError:
        local = None
    out = store.rotate(actor=actor, local_key=local)
    out["signing_unchanged_configured_key"] = bool(_env(KEY_ENV))
    return out


def keyring(*, missing: str | None = None) -> dict[str, bytes]:
    """Every key this deployment can verify a keyed record with, by key id.

    ``missing``: a key id the caller looked for and did not find; the shared
    keyring is then re-read (rate-limited) in case another process rotated.
    """
    from services import audit_chain_keystore as store

    ring: dict[str, bytes] = {}
    configured = _env(KEY_ENV)
    if configured:
        ring[key_id_for(configured.encode("utf-8"))] = configured.encode("utf-8")
    for kid, key in store.all_keys(missing=missing).items():
        ring.setdefault(kid, key)
    generated = _generated_key(create=False)
    if generated:
        ring.setdefault(key_id_for(generated), generated)
    if "<process>" in _generated:
        ring.setdefault(key_id_for(_generated["<process>"]), _generated["<process>"])
    for key in retired_keys():
        ring.setdefault(key_id_for(key), key)
    return ring


def key_for(key_id: str) -> bytes | None:
    """The key named ``key_id``, re-reading the shared keyring once on a miss."""
    key = keyring().get(key_id)
    if key is None:
        key = keyring(missing=key_id).get(key_id)
    return key


def legacy_keys() -> list[bytes]:
    """Candidate keys for records with no ``key_id``: the login secret, then retired keys."""
    out: list[bytes] = []
    for key in [legacy_login_secret(), *retired_keys()]:
        if key and key not in out:
            out.append(key)
    return out


def check_signature(record: dict[str, Any], hmac_of: Any = None) -> tuple[str, str | None]:
    """Verify one signed record.

    Returns ``(verdict, key_id)``: ``ok``; ``mismatch`` (the key is held and
    the HMAC disagrees: the content changed); or ``unknown_key`` (the record
    names a key this deployment does not hold, so it cannot be checked — a
    lost key, not evidence of tampering). Records with no ``key_id`` are tried
    against every legacy key.
    """
    compute = hmac_of or hmac_hex
    stored = str(record.get("event_hash") or "")
    kid = record.get("key_id")
    if kid:
        key = key_for(str(kid))
        if key is None:
            return "unknown_key", str(kid)
        return ("ok" if compute(record, key) == stored else "mismatch"), str(kid)
    for key in legacy_keys():
        if compute(record, key) == stored:
            return "ok", None
    return "mismatch", None


def sign(payload: dict[str, Any], *, field: str) -> dict[str, Any]:
    """Stamp ``key_id`` and an HMAC (in ``field``) on a chain side record."""
    kid, key, _source = signing_key()
    out = {k: v for k, v in payload.items() if k != field}
    out["key_id"] = kid
    out[field] = hmac_hex({k: v for k, v in out.items() if k != field}, key)
    return out


def side_record_keys(record: dict[str, Any]) -> list[bytes]:
    """Keys a side record (checkpoint, manifest) may have been signed with.

    A record with a ``key_id`` names its key; an older one (no ``key_id``) was
    signed with the login secret, or with a secret since moved to the retired list.
    """
    kid = record.get("key_id")
    if kid:
        key = key_for(str(kid))
        return [key] if key is not None else []
    return legacy_keys()


def verify_side_record(record: dict[str, Any], *, field: str) -> bytes | None:
    """The key that verifies a signed side record, or ``None`` if none does."""
    claimed = str(record.get(field) or "")
    if not claimed:
        return None
    body = {k: v for k, v in record.items() if k not in (field, "_id")}
    for key in side_record_keys(record):
        if hmac_hex(body, key) == claimed:
            return key
    return None


def describe() -> dict[str, Any]:
    """What a verification report says about keys. Ids only, never key material.

    Never names server variables either: this is read by workspace admins in
    Settings (A1); the variables are described in this module's docstring.
    """
    configured = bool(_env(KEY_ENV))
    current = None
    source = "unset"
    if configured:
        current = key_id_for(_env(KEY_ENV).encode("utf-8"))
        source = "configured"
    else:
        persisted = _persisted_current()
        if persisted is not None:
            current = persisted[0]
            source = "persisted"
        else:
            generated = _generated_key(create=False) or _generated.get("<process>")
            if generated:
                current = key_id_for(generated)
                source = "generated_local"
    from services import audit_chain_keystore as store

    return {
        "current_key_id": current,
        "source": source,
        "known_key_ids": sorted(keyring()),
        "persisted_key_count": len(store.all_keys()),
        "retired_key_count": len(retired_keys()),
        "legacy_records_verify_under": "login secret, then retired keys",
    }
