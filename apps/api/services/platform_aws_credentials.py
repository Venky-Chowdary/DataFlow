"""Whether a connector may sign with the platform's own AWS credentials (follow-up review 1, H2).

The API process has an AWS credential chain of its own (environment keys, a
profile, an instance or IRSA role) for the platform's buckets and services. A
tenant's connector must never borrow it: requests signed with it act in the
platform's account, toward an endpoint the tenant chose, and their answers
(an error, a listing, a row count) come back to the tenant.

Off (the default): no tenant-controlled setting makes a client sign with that
chain. S3 and DynamoDB connectors sign with their own keys
(``aws_common.boto3_client``); an Iceberg catalog refuses SigV4 signing, a
Glue catalog and an S3 FileIO without the connector's own keys, and any
profile or botocore-session property (:func:`iceberg_credentials_refusal`).

``DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS=1`` is for a self-hosted,
single-tenant deployment that grants its connectors an IAM / IRSA role instead
of keys: connectors may then use the instance role. The API warns about it at
every start and the security-settings snapshot records it as
``platform_aws_chain: "allowed"`` (a key the audit redaction leaves readable).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.brand_env import getenv_brand

FLAG = "DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS"
_ON = frozenset({"1", "true", "yes", "on"})

#: The two remedies every refusal names. Never followed by a value from the config.
REMEDIES = (
    "Enter this connector's own AWS access key and secret key, or ask the operator to set "
    f"{FLAG}=1 (for a single-tenant deployment whose connectors use the instance's IAM role)."
)
#: Iceberg / pyiceberg properties that name a credential source on the API host.
_CHAIN_KEY_SUFFIXES = (".profile-name",)
_CHAIN_KEYS = frozenset({"botocore_session", "botocore-session", "profile-name", "profile_name"})
_TRUE = frozenset({"1", "true", "yes", "on"})


def platform_aws_credentials_allowed() -> bool:
    """True only when the operator set ``DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS`` on."""
    return str(getenv_brand("ALLOW_PLATFORM_AWS_CREDENTIALS", "") or "").strip().lower() in _ON


def platform_aws_settings() -> dict[str, str]:
    """The entry of the startup security-settings snapshot."""
    return {"platform_aws_chain": "allowed" if platform_aws_credentials_allowed() else "refused"}


def warn_if_platform_aws_credentials_allowed(logger: Any) -> None:
    """Startup: say once that connectors may sign with this process's AWS credentials."""
    if not platform_aws_credentials_allowed():
        return
    logger.warning(
        "%s is on: S3, DynamoDB and Iceberg connectors without their own keys sign with this "
        "API process's AWS credentials (its IAM role), in every workspace. Use it only for a "
        "single-tenant deployment.",
        FLAG,
    )


def platform_credentials_message(what: str) -> str:
    """``<what> would sign with the platform's AWS credentials`` plus the remedies."""
    return (
        f"{what}: Datawrap does not sign a workspace's requests with the platform's own AWS "
        f"credentials. {REMEDIES}"
    )


def _text(props: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = props.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _has_keys(props: Mapping[str, Any], prefix: str) -> bool:
    access = _text(props, f"{prefix}.access-key-id", "client.access-key-id")
    secret = _text(props, f"{prefix}.secret-access-key", "client.secret-access-key")
    return bool(access and secret)


def chain_property(props: Mapping[str, Any]) -> str:
    """The first property naming a profile or botocore session on the API host, else ``""``."""
    for key in props:
        name = str(key).strip().lower()
        if name in _CHAIN_KEYS or name.endswith(_CHAIN_KEY_SUFFIXES):
            if props[key] not in (None, "", False):
                return str(key)
    return ""


def s3_fileio_refusal(props: Mapping[str, Any]) -> str:
    """Why an Iceberg FileIO with ``props`` would sign S3 with the platform chain, else ``""``.

    Explicit keys (``s3.*`` or ``client.*``, as pyiceberg reads them, a vended
    credential included) or ``s3.anonymous`` sign with no platform credential.
    """
    if platform_aws_credentials_allowed():
        return ""
    named = chain_property(props)
    if named:
        return platform_credentials_message(f"Iceberg property {named} names a credential on the API host")
    if _has_keys(props, "s3") or _text(props, "s3.anonymous").lower() in _TRUE:
        return ""
    return platform_credentials_message("This Iceberg table's S3 files have no access key")


def iceberg_credentials_refusal(catalog_type: str, props: Mapping[str, Any]) -> str:
    """Why an Iceberg catalog built from ``props`` would sign with the platform chain, else ``""``.

    REST SigV4 (``rest.sigv4-enabled``) signs with ``client.*`` keys or the
    process chain; Glue with ``glue.*`` / ``client.*`` keys or the chain. The
    S3 FileIO is judged when it is built (:func:`s3_fileio_refusal`), since a
    REST catalog may vend its keys with the table.
    """
    if platform_aws_credentials_allowed():
        return ""
    named = chain_property(props)
    if named:
        return platform_credentials_message(f"Iceberg property {named} names a credential on the API host")
    kind = str(catalog_type or "").strip().lower()
    if kind == "rest" and _text(props, "rest.sigv4-enabled").lower() in _TRUE:
        if not _has_keys(props, "client"):
            return platform_credentials_message(
                "Iceberg REST SigV4 signing needs client.access-key-id and client.secret-access-key"
            )
    if kind == "glue" and not _has_keys(props, "glue"):
        return platform_credentials_message(
            "An Iceberg Glue catalog needs glue.access-key-id and glue.secret-access-key"
        )
    return ""


_ICEBERG_TYPES = frozenset({"iceberg", "apache_iceberg"})


def iceberg_connector_refusal(data: Mapping[str, Any]) -> str:
    """At save: an Iceberg connector whose catalog settings would sign with the platform chain.

    Reads the settings the catalog factory reads (dotted keys at the top level
    and in ``extra``) without building anything; the factory checks again,
    with a REST catalog's own overrides, when it builds the catalog.
    """
    if platform_aws_credentials_allowed():
        return ""
    if str(data.get("type") or data.get("format") or "").strip().lower() not in _ICEBERG_TYPES:
        return ""
    raw_extra = data.get("extra")
    extra = dict(raw_extra) if isinstance(raw_extra, Mapping) else {}
    props = {key: value for key, value in data.items() if isinstance(key, str) and "." in key}
    props.update(extra)
    from connectors.iceberg_catalog import _infer_catalog_type

    catalog = _infer_catalog_type(
        str(data.get("connection_string") or ""),
        str(data.get("region") or ""),
        str(data.get("warehouse") or data.get("database") or ""),
        extra,
    )
    return iceberg_credentials_refusal(catalog, props)
