"""Which free-form config keys hold credentials, and how they are masked and sealed.

One rule for every ``extra`` map a connector or a transfer plan carries (SEC-3,
SEC-4). A key is a secret when its name says so (``pass``, ``secret``,
``token``, ``key``, ``credential``, ``auth``, ``private``, ``bearer``, a SAS or
a service account), unless it is one of the configuration names that only
happen to contain those words: key columns (``primary_key``, ``sort_key``),
modes and types (``auth_type``), names, ids, paths and URLs (``token_url``,
``aws_access_key_id``, ``private_key_file``). Nested maps are walked, so
``headers.Authorization`` is a secret too.

API responses replace a secret value with ``****``; at rest it is encrypted
with the same vault, and the same tenant key, as a connector's top-level
password. A map or list under a secret name is sealed as JSON.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

MASK = "****"

_SECRET_WORD = re.compile(
    r"(pass|pwd|secret|token|key|credential|auth|private|bearer|service_account|signature|cookie|jwt|"
    r"(^|_)sas(_|$))"
)
#: Configuration names that contain a secret word but hold no secret.
_NON_SECRET_SUFFIXES = (
    "_type", "_mode", "_method", "_mechanism", "_kind", "_source", "_database", "_db",
    "_url", "_uri", "_endpoint", "_host", "_hosts", "_port", "_path", "_file", "_filename",
    "_dir", "_id", "_ids", "_name", "_region", "_scope", "_scopes", "_header", "_param",
    "_column", "_columns", "_field", "_fields", "_schema", "_prefix", "_pattern",
    "_separator", "_format", "_location", "_enabled", "_required", "_version",
    "_algorithm", "_ttl", "_expiry", "_expires_in", "_policy", "_strategy", "_tokenizer",
    "_authority",
)
_NON_SECRET_NAMES = frozenset({
    "keyspace", "key_space", "tokenizer", "authority", "authentication", "authenticator",
    "host_key", "known_hosts", "keys_only", "key", "keys",
})
#: Key-column vocabulary: ``primary_key``, ``upsert_keys``, ``dynamo_key_schema``.
_KEY_COLUMN = re.compile(
    r"(^|_)(primary|foreign|unique|upsert|partition|sort|row|shard|sharding|cluster|clustering|"
    r"distribution|dist|merge|business|natural|surrogate|dedupe|dedup|hash|range|cursor|"
    r"idempotency|join|lookup|dynamo|redis|routing|group|order|index|message|record|object|s3)_?keys?($|_)"
)
_ENC_JSON = "__enc_json__"
#: Connection strings: shown with their userinfo masked, kept when sent back masked.
DSN_KEYS = frozenset({"connection_string", "dsn", "odbc_connect", "uri"})
_ENC_PREFIXES = ("enc:v1:", "enc:v0:", "sm:", "byok:")


def _snake(name: Any) -> str:
    text = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", str(name or ""))
    return text.replace("-", "_").replace(".", "_").strip().lower()


def is_secret_field_name(name: Any) -> bool:
    """Whether a config key holds a credential (see the module docstring)."""
    low = _snake(name)
    if not low or not _SECRET_WORD.search(low):
        return False
    if low in _NON_SECRET_NAMES or _KEY_COLUMN.search(low):
        return False
    return not low.endswith(_NON_SECRET_SUFFIXES)


def _has_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    return isinstance(value, (dict, list, tuple)) and bool(value)


def is_masked(value: Any) -> bool:
    """A placeholder a client sends back for a secret it was never shown."""
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip()
        return not text or "****" in text or "<redacted>" in text.lower()
    return False


def mask_secret_tree(value: Any, *, mask_url: Callable[[str], str] | None = None) -> Any:
    """A copy of ``value`` with every secret-named value replaced by ``****``.

    ``mask_url`` masks userinfo in connection strings and URLs that stay shown.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            low = _snake(key)
            if is_secret_field_name(key) and _has_value(item):
                out[key] = MASK
            elif isinstance(item, (dict, list)):
                out[key] = mask_secret_tree(item, mask_url=mask_url)
            elif (
                mask_url is not None and isinstance(item, str) and item
                and (low in {"connection_string", "dsn", "url", "uri", "odbc_connect"}
                     or low.endswith(("_url", "_uri", "_dsn", "_connection_string")))
            ):
                out[key] = mask_url(item)
            else:
                out[key] = item
        return out
    if isinstance(value, list):
        return [mask_secret_tree(item, mask_url=mask_url) for item in value]
    return value


def _is_sealed(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith(_ENC_PREFIXES)
    return isinstance(value, dict) and set(value) == {_ENC_JSON}


def has_plaintext_secrets(value: Any) -> bool:
    """Whether a stored tree still holds a secret-named value in plain text."""
    if isinstance(value, dict):
        for key, item in value.items():
            if is_secret_field_name(key) and _has_value(item):
                if not _is_sealed(item) and not is_masked(item):
                    return True
            elif isinstance(item, (dict, list)) and has_plaintext_secrets(item):
                return True
        return False
    if isinstance(value, list):
        return any(has_plaintext_secrets(item) for item in value)
    return False


def seal_secret_tree(value: Any, encrypt: Callable[[str], str], *, also: frozenset[str] = frozenset()) -> Any:
    """A copy of ``value`` with every secret-named value encrypted by ``encrypt``.

    ``also`` names more keys to seal (a DSN that may carry a password).
    Already-sealed values and placeholders are kept as they are.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            secret = is_secret_field_name(key) or _snake(key) in also
            if secret and _has_value(item) and not _is_sealed(item) and not is_masked(item):
                if isinstance(item, str):
                    out[key] = encrypt(item)
                else:
                    out[key] = {_ENC_JSON: encrypt(json.dumps(item, sort_keys=True, default=str))}
            elif isinstance(item, (dict, list)) and not _is_sealed(item):
                out[key] = seal_secret_tree(item, encrypt, also=also)
            else:
                out[key] = item
        return out
    if isinstance(value, list):
        return [seal_secret_tree(item, encrypt, also=also) for item in value]
    return value


def open_secret_tree(value: Any, decrypt: Callable[[str], str]) -> Any:
    """Inverse of :func:`seal_secret_tree`; plaintext (legacy) values pass through."""
    if isinstance(value, dict):
        if _is_sealed(value):
            return json.loads(decrypt(str(value[_ENC_JSON])))
        return {key: open_secret_tree(item, decrypt) for key, item in value.items()}
    if isinstance(value, list):
        return [open_secret_tree(item, decrypt) for item in value]
    if isinstance(value, str) and value.startswith(_ENC_PREFIXES):
        return decrypt(value)
    return value


def keep_stored_secrets(new: Any, stored: Any, *, also: frozenset[str] = DSN_KEYS) -> Any:
    """``new`` with every masked secret placeholder replaced by the stored value.

    An edit form sends ``****`` back for a secret it was shown masked; that
    must never overwrite the saved credential. Keys ``new`` drops stay dropped.
    """
    if not isinstance(new, dict) or not isinstance(stored, dict):
        return new
    out: dict[str, Any] = {}
    for key, item in new.items():
        old = stored.get(key)
        if (is_secret_field_name(key) or _snake(key) in also) and is_masked(item) and _has_value(old):
            out[key] = old
        elif isinstance(item, dict) and isinstance(old, dict) and not _is_sealed(old):
            out[key] = keep_stored_secrets(item, old, also=also)
        else:
            out[key] = item
    return out


def public_plan_dict(plan: Any) -> dict[str, Any]:
    """A transfer plan's ``to_dict()`` with its endpoint credentials masked (SEC-4).

    Pure (no store read), so an async route may call it on the event loop.
    """
    from services.secret_config import mask_secrets_in_text

    d = plan.to_dict()
    for side in ("source", "destination"):
        if isinstance(d.get(side), dict):
            d[side] = mask_secret_tree(d[side], mask_url=mask_secrets_in_text)
    return d
