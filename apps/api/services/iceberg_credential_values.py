"""Which Iceberg credential properties count as the table's own (follow-up review 2, H1).

pyiceberg reads a credential by truthiness (``if token := props.get(...)``,
``get_first_property_value``): ``0``, ``False``, ``[]`` or ``{}`` is no
credential, and pyarrow / boto3 then fall back to the API host's identity. The
guards used to count ``str(value).strip()``, which is ``"0"`` / ``"False"`` /
``"[]"`` for those, so a tenant's connector settings (``extra`` is a
``dict[str, Any]``) or its REST catalog's JSON (``/v1/config``, a table's
config and storage credentials) passed every "has its own key" check with a
credential pyiceberg would never send.

A credential counts only as a non-empty string after strip
(:func:`credential_text`). A credential property whose value is not text (and
not ``None``, JSON ``null``, which is no value) is refused outright
(:func:`non_text_credential_refusal`), whatever the operator opt-ins say: it is
malformed, and pyiceberg would read it as absent. The message names the
property and never the value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: Suffixes of the AWS key properties pyiceberg reads (``s3.``, ``client.``, ``glue.``, ...).
_AWS_SUFFIXES = (".access-key-id", ".secret-access-key", ".session-token")
#: Google Cloud and Azure credential properties pyiceberg's PyArrow FileIO reads.
_CLOUD_KEYS = frozenset(
    {
        "gcs.oauth2.token",
        "adls.sas-token",
        "adls.account-key",
        "adls.client-id",
        "adls.client-secret",
        "adls.tenant-id",
    }
)
#: Properties that name a credential source on the API host (a profile, a botocore session).
_CHAIN_SUFFIXES = (".profile-name",)
_CHAIN_KEYS = frozenset({"botocore_session", "botocore-session", "profile-name", "profile_name"})


def is_credential_property(key: Any) -> bool:
    """True for a property that carries a credential (or names a host credential source)."""
    name = str(key).strip().lower()
    return (
        name in _CLOUD_KEYS
        or name in _CHAIN_KEYS
        or name.endswith(_AWS_SUFFIXES)
        or name.endswith(_CHAIN_SUFFIXES)
    )


def credential_text(props: Mapping[str, Any], *keys: str) -> str:
    """The first of ``keys`` whose value is a non-empty string (stripped), else ``""``."""
    for key in keys:
        value = props.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _shown(key: Any) -> str:
    return "".join(ch for ch in str(key) if ch.isprintable())[:80]


def non_text_credential(props: Any) -> str:
    """The first credential property whose value is neither text nor null, else ``""``."""
    if not isinstance(props, Mapping):
        return ""
    for key, value in props.items():
        if value is not None and not isinstance(value, str) and is_credential_property(key):
            return _shown(key)
    return ""


def non_text_credential_refusal(props: Any) -> str:
    """Why ``props`` carry a credential Datawrap cannot use, else ``""``."""
    key = non_text_credential(props)
    if not key:
        return ""
    return (
        f"Iceberg property {key} is not text. A credential must be a non-empty string; "
        "pyiceberg would read this value as no credential and fall back to the platform's own "
        "identity, which Datawrap does not allow. Set the property to the credential's text, "
        "or remove it."
    )
