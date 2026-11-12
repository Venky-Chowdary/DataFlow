"""What a connector's region verification is bound to (security review round 2, H4).

A platform administrator's region stamp speaks for a connector whose region no
hostname proves (an IP address, a custom domain, an S3-compatible store,
BigQuery, ADLS, a SaaS account). It used to be bound to the connector's type
and hosts only, so it survived edits and per-request overrides that move the
data: an Oracle descriptor, the ADLS storage account, the bucket behind a
bucket-agnostic endpoint, the BigQuery dataset or project, a port on a shared
TCP proxy, the API key of a SaaS account.

Now the stamp is bound to every field the driver addresses with, per driver
family:

- always: ``host``, ``port``, ``database`` (database, service, bucket or
  container), ``endpoint_url``, ``connection_string``, and the ``extra`` keys
  that name an account, project, dataset, bucket, region or endpoint;
- BigQuery: ``schema`` (the dataset fixes the location);
- drivers that dial no host of their own (cloud storage, SaaS APIs): the
  credential identity, ``username`` (a storage account or access key),
  ``api_key`` and ``service_account``. The values are only ever fed to a keyed
  hash (HKDF subkey of the vault key), never stored.

Any change to those fields clears the stamp, an address field residency
cannot read counts as moved, and a run whose resolved destination differs
from the stored connector in any of them is not covered by the stamp.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

#: Fields every driver addresses with.
_ADDRESS_FIELDS = ("host", "port", "database", "endpoint_url", "connection_string")
#: Per driver family, further fields that choose where data lands.
_FAMILY_FIELDS: dict[str, tuple[str, ...]] = {"bigquery": ("schema",)}
#: ``extra`` keys that name where data lands.
_ADDRESS_EXTRA_KEYS = frozenset({
    "account", "account_name", "account_url", "storage_account", "project", "project_id",
    "dataset", "dataset_id", "location", "bucket", "container", "endpoint", "endpoint_url",
    "host", "hosts", "server", "port", "dsn", "service", "service_name", "sid", "region",
    "aws_region", "url", "base_url", "instance_url", "domain", "subdomain", "site",
    "http_path", "catalog", "workspace_url", "cluster", "connection_string", "uri",
    "tenant", "tenant_id", "org", "org_id", "portal_id", "odbc_connect", "database",
})
#: Credentials that choose the account for a driver that dials no host of its own.
_IDENTITY_FIELDS = ("username", "api_key", "service_account")
#: SQL warehouses address by host/account, not by login: no credential identity.
_HOST_ADDRESSED_WAREHOUSES = frozenset({"snowflake", "redshift", "generic_sql", "databricks"})
#: Address fields shown in the audit event and the stamp (never a credential or DSN).
_SHOWN_FIELDS = ("port", "database", "schema")

_SUBKEY_INFO = b"datawrap-region-verification/v1"


def _driver_of(target: Any) -> str:
    from services.data_residency import _driver, _norm

    return _driver(_norm(getattr(target, "type", "") or getattr(target, "format", "")))


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    text = str(value).strip()
    return "" if text in ("0", "None") else text


def _field(target: Any, name: str) -> Any:
    from services.data_residency import _value

    return _value(target, name)


def address_fields(target: Any) -> dict[str, str]:
    """Every addressing field of a connector or resolved endpoint, normalised."""
    from services.data_residency import _dials_host, _norm

    driver = _driver_of(target)
    names = list(_ADDRESS_FIELDS) + list(_FAMILY_FIELDS.get(driver, ()))
    fmt = _norm(getattr(target, "type", "") or getattr(target, "format", ""))
    if not _dials_host(fmt) and driver not in _HOST_ADDRESSED_WAREHOUSES:
        names += list(_IDENTITY_FIELDS)
    fields = {name: _text(_field(target, name)) for name in names}
    fields["host"] = fields["host"].lower().rstrip(".")
    extra = getattr(target, "extra", None)
    if isinstance(extra, dict):
        for key in sorted(_ADDRESS_EXTRA_KEYS & set(extra)):
            fields[f"extra.{key}"] = _text(extra[key])
    return {name: value for name, value in fields.items() if value}


def address_unreadable(target: Any) -> bool:
    """Whether an address field residency judges cannot be read (counts as moved)."""
    from services.data_residency import _addresses

    return _addresses(target)[1]


def verification_address(target: Any) -> str:
    """A keyed hash of the connector's driver family and every addressing field.

    Keyed (an HKDF subkey of the vault key), so the stored value is no offline
    oracle for an API key or a password inside a connection string.
    """
    from services.secret_vault import derive_subkey

    key = [driver_family_key(target), sorted(address_fields(target).items())]
    payload = json.dumps(key, sort_keys=True).encode("utf-8")
    return hmac.new(derive_subkey(_SUBKEY_INFO), payload, hashlib.sha256).hexdigest()[:32]


def driver_family_key(target: Any) -> str:
    from services.data_residency import _norm

    return _norm(getattr(target, "type", "") or getattr(target, "format", ""))


def resolved_address(target: Any) -> dict[str, Any]:
    """What the stamp was verified for, readable and credential-free (for the audit trail)."""
    from services.data_residency import _addresses

    hosts, _unreadable = _addresses(target)
    shown: dict[str, Any] = {"type": driver_family_key(target), "hosts": sorted(set(hosts))}
    fields = address_fields(target)
    for name in _SHOWN_FIELDS:
        if fields.get(name):
            shown[name] = fields[name]
    shown.update({
        name: value for name, value in fields.items()
        if name.startswith("extra.") and name[6:] not in ("connection_string", "odbc_connect", "uri", "url")
    })
    return shown


def overridden_by_request(endpoint: Any, connector: Any, workspace_id: str | None) -> bool:
    """Whether the resolved run endpoint addresses differently from the stored connector.

    Both are resolved by the same resolver, so a request that restates the
    connector's own values is not an override; any address field it changes is.
    """
    from services.data_residency import _resolved
    from src.transfer.models import EndpointConfig

    bare = EndpointConfig.from_dict(
        str(getattr(endpoint, "kind", "") or "database"),
        {"format": getattr(connector, "type", ""), "connector_id": getattr(connector, "id", "")},
    )
    resolved = _resolved(bare, workspace_id)
    return address_fields(endpoint) != address_fields(resolved)


__all__ = [
    "address_fields",
    "address_unreadable",
    "overridden_by_request",
    "resolved_address",
    "verification_address",
]
