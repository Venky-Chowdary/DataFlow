"""Whether an Iceberg FileIO may open Google Cloud or Azure storage with the platform's own identity (follow-up review 1).

pyiceberg's ``PyArrowFileIO`` opens ``gs://`` / ``gcs://`` with pyarrow's
``GcsFileSystem`` and ``abfs[s]://`` / ``wasb[s]://`` with its
``AzureFileSystem``. Given no credential, pyarrow's C++ clients use the API
host's: Google Application Default Credentials and Azure
``DefaultAzureCredential`` (environment, workload or managed identity, CLI).
A table's location and configuration come from the tenant's catalog, so its
files would be read and written as the platform, and ``gcs.service.host`` or
an ``adls.*-storage-authority`` would send the platform's token to the
tenant's host.

Off (the default): a FileIO opens GCS only with ``gcs.oauth2.token`` and
Azure only with ``adls.sas-token``, ``adls.account-key`` or
``adls.client-id`` / ``adls.client-secret`` / ``adls.tenant-id`` (as a REST
catalog vends them with a table), checked where the filesystem is built
(``connectors.iceberg_pyarrow_io``); a non-REST connector whose warehouse is on
GCS or Azure is refused at save, since its settings cannot carry those.

``DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS=1`` is for a self-hosted,
single-tenant deployment whose connectors use the instance's Google or Azure
identity. The API warns at every start and the security-settings snapshot
records ``platform_gcp_azure_chain`` (a key the audit redaction leaves
readable). An endpoint a property names never gets that identity, on or off.
pyiceberg's ``FsspecFileIO`` would use gcsfs / adlfs, which are not installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.brand_env import getenv_brand
from services.iceberg_credential_values import credential_text, non_text_credential_refusal

FLAG = "DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS"
_ON = frozenset({"1", "true", "yes", "on"})
GCS_TOKEN = "gcs.oauth2.token"
_GCS_ENDPOINTS = ("gcs.service.host",)
_ADLS_OWN = ("adls.sas-token", "adls.account-key")
_ADLS_CLIENT = ("adls.client-id", "adls.client-secret", "adls.tenant-id")
_ADLS_ENDPOINTS = (
    "adls.blob-storage-authority",
    "adls.dfs-storage-authority",
    "adls.blob-storage-scheme",
    "adls.dfs-storage-scheme",
)
_ADLS_NEEDS = "adls.sas-token, adls.account-key, or adls.client-id with adls.client-secret and adls.tenant-id"
STORAGE_SCHEMES = frozenset({"gs", "gcs", "abfs", "abfss", "wasb", "wasbs"})
_ICEBERG_TYPES = frozenset({"iceberg", "apache_iceberg"})


def platform_gcp_azure_credentials_allowed() -> bool:
    """True only when the operator set ``DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS`` on."""
    return str(getenv_brand("ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS", "") or "").strip().lower() in _ON


def platform_gcp_azure_settings() -> dict[str, str]:
    """The entry of the startup security-settings snapshot."""
    return {"platform_gcp_azure_chain": "allowed" if platform_gcp_azure_credentials_allowed() else "refused"}


def warn_if_platform_gcp_azure_credentials_allowed(logger: Any) -> None:
    """Startup: say once that Iceberg tables may be opened with this host's Google or Azure identity."""
    if not platform_gcp_azure_credentials_allowed():
        return
    logger.warning(
        "%s is on: Iceberg tables on Google Cloud Storage or Azure storage without their own "
        "credential are opened with this API host's Google or Azure identity, in every workspace. "
        "Use it only for a single-tenant deployment.",
        FLAG,
    )


def _message(what: str, needs: str) -> str:
    return (
        f"{what}: Datawrap does not open a workspace's files with the platform's own Google Cloud "
        f"or Azure credentials. Use a catalog that supplies the table's own {needs}, or ask the "
        f"operator to set {FLAG}=1 (for a single-tenant deployment whose connectors use the "
        "instance's Google or Azure identity)."
    )


def _endpoint_message(prop: str, needs: str) -> str:
    return (
        f"Iceberg property {prop} sends this table's storage requests to another endpoint, and the "
        f"table has no {needs} of its own: Datawrap never sends the platform's Google Cloud or Azure "
        "credentials to an endpoint a catalog names. Supply the table's own credential, or remove "
        "the property."
    )


def _set(props: Mapping[str, Any], key: str) -> bool:
    """An endpoint property is present unless null or blank (any other value is judged as set)."""
    value = props.get(key)
    return value is not None and bool(str(value).strip())


def _own(props: Mapping[str, Any], key: str) -> bool:
    """A credential counts only as non-empty text: pyiceberg reads ``0`` / ``[]`` as none (follow-up review 2, H1)."""
    return bool(credential_text(props, key))


def _refusal(props: Mapping[str, Any], own: bool, endpoints: tuple[str, ...], what: str, needs: str) -> str:
    malformed = non_text_credential_refusal(props)
    if malformed:
        return malformed
    if own:
        return ""
    for prop in endpoints:
        if _set(props, prop):
            return _endpoint_message(prop, needs)
    return "" if platform_gcp_azure_credentials_allowed() else _message(what, needs)


def gcs_fileio_refusal(props: Mapping[str, Any]) -> str:
    """Why pyarrow's GCS filesystem from ``props`` would use the platform's identity, else ``""``."""
    return _refusal(
        props,
        _own(props, GCS_TOKEN),
        _GCS_ENDPOINTS,
        "This Iceberg table's Google Cloud Storage files have no access token",
        GCS_TOKEN,
    )


def azure_fileio_refusal(props: Mapping[str, Any]) -> str:
    """Why pyarrow's Azure filesystem from ``props`` would use the platform's identity, else ``""``."""
    own = any(_own(props, key) for key in _ADLS_OWN) or all(_own(props, key) for key in _ADLS_CLIENT)
    return _refusal(
        props,
        own,
        _ADLS_ENDPOINTS,
        "This Iceberg table's Azure storage files have no credential",
        _ADLS_NEEDS,
    )


def iceberg_gcp_azure_connector_refusal(data: Mapping[str, Any]) -> str:
    """At save: a non-REST Iceberg catalog whose warehouse is on GCS or Azure, else ``""``.

    The connector's settings forward only ``s3.*`` / ``client.*`` storage
    properties, so such a catalog could open its files only with the
    platform's identity; a REST catalog may vend the table's own credential.
    """
    if platform_gcp_azure_credentials_allowed():
        return ""
    if str(data.get("type") or data.get("format") or "").strip().lower() not in _ICEBERG_TYPES:
        return ""
    raw_extra = data.get("extra")
    extra = dict(raw_extra) if isinstance(raw_extra, Mapping) else {}
    from connectors.iceberg_catalog import _infer_catalog_type

    warehouse = str(data.get("warehouse") or data.get("database") or "")
    catalog = _infer_catalog_type(
        str(data.get("connection_string") or ""), str(data.get("region") or ""), warehouse, extra
    )
    if catalog == "rest":
        return ""
    for value in (warehouse, data.get("connection_string"), extra.get("warehouse")):
        scheme, sep, _rest = str(value or "").strip().lower().partition("://")
        if sep and scheme in STORAGE_SCHEMES:
            return _message(
                f"This Iceberg connector's warehouse is on {scheme}:// storage, which a {catalog} "
                "catalog cannot give a credential",
                f"{GCS_TOKEN} or {_ADLS_NEEDS}",
            )
    return ""
