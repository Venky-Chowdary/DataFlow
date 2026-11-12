"""BigQuery and GCS clients never use the API host's Google identity (follow-up review 2, BQ).

``connectors.bigquery_conn.get_client`` built ``bigquery.Client`` without
credentials for a non-emulator connector that had no key: google-cloud-core
then calls ``google.auth.default()``, the API host's Application Default
Credentials (a key file in the environment, a gcloud user, the GCE / GKE
metadata identity), so a workspace's queries ran as the platform. And any
credential reference that was not JSON was read with
``service_account.Credentials.from_service_account_file`` from any path on the
API host (``bigquery_conn``; ``connectors.gcs_common._credentials``, where the
``password`` field counts too), so a workspace could use any key the host holds.

Off (the default), as for Iceberg's GCS / Azure FileIO
(``services.platform_gcp_azure_credentials``):

* a BigQuery client for a non-emulator endpoint needs the connector's own
  service-account key (JSON in ``service_account`` or ``connection_string``);
* a key *file* is read only from inside the areas a connector may open: an
  upload the workspace owns, its own export folder, or a directory the operator
  lists in ``DATAFLOW_SOURCE_PATH_ROOTS`` (``local_file_policy``), symlinks and
  ``..`` resolved first.

``DATAFLOW_ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS=1`` (a single-tenant
deployment) allows both. Emulators keep anonymous credentials, and GCS without
a credential stays anonymous. Checked at save
(``egress_guard.connector_save_refusal``) and where each client is built;
messages name the setting and never the path.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from services.platform_gcp_azure_credentials import FLAG, platform_gcp_azure_credentials_allowed

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "host.docker.internal"})


def _remedy() -> str:
    return (
        f"or ask the operator to set {FLAG}=1 (for a single-tenant deployment whose connectors "
        "use the instance's Google identity)."
    )


def ambient_message(what: str) -> str:
    return (
        f"{what} has no service-account key of its own: Datawrap does not run a workspace's "
        "requests with the platform's own Google credentials. Paste the service account's JSON "
        f"key into the connector, {_remedy()}"
    )


def key_file_message(what: str) -> str:
    return (
        f"{what} names a key file outside the folders connectors may open on this server: "
        "Datawrap does not read a service-account key from the platform's own files. Paste the "
        "key's JSON into the connector, upload the key file in this workspace and use that "
        "upload, ask the operator to list its folder in DATAFLOW_SOURCE_PATH_ROOTS, "
        f"{_remedy()}"
    )


def key_file_allowed(path: str) -> bool:
    """A key file inside an area a connector may open, or the operator opted in."""
    if platform_gcp_azure_credentials_allowed():
        return True
    from services.brand_env import getenv_brand_str
    from services.export_paths import connector_may_open_export_path, exports_dir
    from services.local_file_policy import connector_may_open_upload_path, is_within
    from services.platform_config import upload_dir_path

    try:
        resolved = Path(path).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return False
    if is_within(resolved, upload_dir_path().expanduser().resolve(strict=False)):
        return connector_may_open_upload_path(resolved)
    if is_within(resolved, exports_dir(create=False).expanduser().resolve(strict=False)):
        return connector_may_open_export_path(resolved)
    roots = [raw.strip() for raw in getenv_brand_str("SOURCE_PATH_ROOTS", "").split(os.pathsep) if raw.strip()]
    return any(is_within(resolved, Path(root).expanduser().resolve(strict=False)) for root in roots)


def key_file_refusal(path: str, what: str) -> str:
    return "" if key_file_allowed(path) else key_file_message(what)


def bigquery_credentials_refusal(creds_ref: str, *, is_local: bool) -> str:
    """Why ``get_client`` would use the host's identity or a host key file, else ``""``."""
    if is_local:
        return ""
    ref = (creds_ref or "").strip()
    if not ref:
        if platform_gcp_azure_credentials_allowed():
            return ""
        return ambient_message("This BigQuery connector")
    if ref.startswith("{"):
        return ""
    return key_file_refusal(ref, "This BigQuery connector's credential")


def gcs_reads_key_file(creds_ref: str) -> bool:
    """``gcs_common._credentials`` reads ``creds_ref`` as a key file path."""
    ref = (creds_ref or "").strip()
    return bool(ref) and not ref.startswith("{") and (ref.endswith(".json") or "/" in ref)


def gcs_credentials_refusal(creds_ref: str) -> str:
    if not gcs_reads_key_file(creds_ref):
        return ""
    return key_file_refusal(creds_ref.strip(), "This Google Cloud Storage connector's credential")


def _text(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    return value.strip() if isinstance(value, str) else ""


def google_connector_refusal(data: Any) -> str:
    """At save: a BigQuery / GCS connector that would use the host's identity or key files."""
    if not isinstance(data, Mapping):
        return ""
    from src.transfer.connector_capabilities import resolve_driver_type

    kind = str(resolve_driver_type(str(data.get("type") or data.get("format") or "").strip()) or "").lower()
    if kind == "bigquery":
        ref = _text(data, "service_account") or _text(data, "connection_string") or _text(data, "credentials_path")
        host = _text(data, "host")
        is_local = host in _LOCAL_HOSTS or ref.startswith(("http://", "https://"))
        return bigquery_credentials_refusal(ref, is_local=is_local)
    if kind == "gcs":
        ref = _text(data, "service_account") or _text(data, "connection_string") or _text(data, "password")
        if ref.startswith(("http://", "https://")):
            return ""
        return gcs_credentials_refusal(ref)
    return ""
