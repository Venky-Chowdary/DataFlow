"""Hosts an Iceberg catalog server may not choose for the API (follow-up review 2, Iceberg REST hosts).

A tenant runs its own REST catalog, so ``/v1/config`` (``defaults`` and
``overrides``) and each table response's ``config`` are the tenant's to write.
pyiceberg reads hosts from those merged properties:

* ``oauth2-server-uri``: the legacy OAuth2 manager POSTs the connector's
  ``credential`` (client id and secret) there while the catalog is built. The
  connector cannot name an OAuth2 server (only ``rest.*``, the token /
  credential and storage properties are forwarded), so a server-supplied one
  must be on the catalog's own origin (scheme, host and port of the
  connector's URI): another host would receive the secret, or be a host the
  egress policy refuses.
* the FileIO's storage hosts (:data:`STORAGE_HOSTS`): pyarrow's S3, GCS,
  Azure and HDFS filesystems dial them. A value the connector did not set
  itself passes the egress check the connector's own address passes at save
  (``egress_guard.new_point_refusal``): cloud metadata and link-local always
  refused, private / loopback / CGNAT refused under
  ``DATAFLOW_EGRESS_ENFORCE_NEW`` and audited otherwise.

Messages name the property and its source, never the value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

OAUTH2_SERVER_URI = "oauth2-server-uri"
#: FileIO properties whose value is a host (or URL) a filesystem dials.
STORAGE_HOSTS = (
    "s3.endpoint", "s3.proxy-uri", "s3.signer.uri", "gcs.service.host", "adls.account-host",
    "adls.blob-storage-authority", "adls.dfs-storage-authority", "hdfs.host", "hf.endpoint",
)
_DEFAULT_PORTS = {"http": 80, "https": 443}
_POINT = "iceberg_catalog_server"


def _message(prop: str, source: str, does: str) -> str:
    return (
        f"Iceberg property {prop} {source} would {does}. Datawrap does not accept that from a "
        "catalog server. Remove it from the server's configuration, or point it at the catalog's "
        "own address or a public storage endpoint."
    )


def _origin(url: Any) -> tuple[str, str, int] | None:
    try:
        parts = urlsplit(str(url or "").strip())
        port = parts.port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if not scheme or not parts.hostname:
        return None
    return scheme, parts.hostname.lower().rstrip("."), port or _DEFAULT_PORTS.get(scheme, 0)


def _supplied(own: Mapping[str, Any], props: Mapping[str, Any], key: str) -> bool:
    """``props`` holds ``key`` with a value the connector did not set itself."""
    if key not in props or not str(props.get(key) or "").strip():
        return False
    return key not in own or props[key] != own[key]


def check_server_oauth_uri(own: Mapping[str, Any], merged: Mapping[str, Any], source: str) -> None:
    """The connector's credential is posted only to the catalog's own origin."""
    if not _supplied(own, merged, OAUTH2_SERVER_URI):
        return
    target = _origin(merged[OAUTH2_SERVER_URI])
    if target is None or target != _origin(own.get("uri")):
        raise ValueError(_message(
            OAUTH2_SERVER_URI, source,
            "send this connector's OAuth2 credential to a host other than the catalog's own address",
        ))


def check_storage_hosts(own: Mapping[str, Any], props: Mapping[str, Any], source: str) -> None:
    """Every storage host the server supplied passes the connector egress check."""
    from services.egress_guard import current_egress_workspace, new_point_refusal

    for prop in STORAGE_HOSTS:
        if not _supplied(own, props, prop):
            continue
        text = str(props[prop]).strip()
        target = text if "://" in text else f"http://{text}"
        if new_point_refusal(_POINT, target, workspace_id=current_egress_workspace()):
            raise ValueError(_message(
                prop, source,
                "make Datawrap dial an address the egress policy refuses (cloud metadata, "
                "link-local, or a private or loopback network)",
            ))
