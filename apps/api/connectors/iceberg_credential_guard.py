"""Iceberg catalogs and FileIO that never sign with the platform's AWS credentials.

Follow-up review 1, H2. A tenant's Iceberg settings reached three signers that
fall back to the API process's AWS credential chain: REST SigV4
(``rest.sigv4-enabled``), the Glue client, and pyarrow's S3 filesystem. A
REST catalog's ``/v1/config`` overrides and the config it returns with a
table are merged into those properties after any check of the connector's
own settings, so the check runs where each signer is built:

* :func:`check_catalog_properties` before a catalog is constructed;
* :func:`guarded_catalog` wraps the pyiceberg catalog class: REST's SigV4
  adapter and Glue's client are refused without the connector's keys, and a
  FileIO the catalog builds from properties with no keys (a table's vended
  keys count) is ``iceberg_pyarrow_io.KeyedS3PyArrowFileIO``, whatever
  ``py-io-impl`` the catalog named, which refuses to open S3;
* :func:`guarded_file_io` for a FileIO built outside a catalog.

``DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS`` on (a single-tenant deployment)
turns all three off (``services.platform_aws_credentials``).

pyarrow opens Google Cloud and Azure storage with the API host's identity
too: a FileIO that would be pyiceberg's plain ``PyArrowFileIO`` (named, or
inferred from a location) is ``LocalUriPyArrowFileIO``, which refuses that
(``services.platform_gcp_azure_credentials``), and so is the one a catalog
builds from its own properties (a SQL catalog's metadata file).

The REST server's ``/v1/config`` may not set ``ssl`` (the session's client
certificate and verification) or change ``uri`` (follow-up review 2, M1).

A FileIO's properties, the REST server's ``/v1/config`` answer, each part of a
table response (metadata properties, config, every storage credential, checked
separately so a blank in one cannot hide a class another names) and the
metadata a commit returns are also refused when a property names a class
Datawrap does not ship (``services.iceberg_class_properties``), whatever that
setting says; ``load_catalog`` checks the connector's own settings.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from services.iceberg_class_properties import (
    COMMIT,
    FILE_IO,
    SERVER_CONFIG,
    STORAGE_CREDENTIAL,
    TABLE_CONFIG,
    TABLE_METADATA,
    check_class_properties,
)
from services.platform_aws_credentials import iceberg_credentials_refusal, s3_fileio_refusal

KEYED_S3_FILE_IO = "connectors.iceberg_pyarrow_io.KeyedS3PyArrowFileIO"
LOCAL_URI_FILE_IO = "connectors.iceberg_pyarrow_io.LocalUriPyArrowFileIO"
_PY_IO_IMPL = "py-io-impl"
_UNGUARDED_FILE_IO = (None, "", "pyiceberg.io.pyarrow.PyArrowFileIO")


def check_catalog_properties(catalog_type: str, properties: dict[str, Any]) -> None:
    """Raise ``ValueError`` when the catalog would sign with the platform's AWS chain."""
    refusal = iceberg_credentials_refusal(catalog_type, properties)
    if refusal:
        raise ValueError(refusal)


def file_io_properties(properties: Any) -> dict[str, Any]:
    """``properties`` with the keyed S3 FileIO when S3 would be opened with the platform chain.

    A FileIO's properties are fixed once it is built, so the check is made on
    them here; the keyed IO refuses only when S3 is opened, and a local
    warehouse still works. Any other PyArrow FileIO is the local-URI one,
    which refuses GCS / Azure without the table's own credential.
    """
    check_class_properties(properties, FILE_IO)
    merged = dict(properties or {})
    if s3_fileio_refusal(merged):
        merged[_PY_IO_IMPL] = KEYED_S3_FILE_IO
    elif merged.get(_PY_IO_IMPL) in _UNGUARDED_FILE_IO:
        merged[_PY_IO_IMPL] = LOCAL_URI_FILE_IO
    return merged


def _same_uri(left: Any, right: Any) -> bool:
    return str(left or "").strip().rstrip("/") == str(right or "").strip().rstrip("/")


def check_server_session_properties(own: dict[str, Any], merged: dict[str, Any]) -> None:
    """The REST server may not set the session's TLS or move the catalog (follow-up review 2, M1).

    ``RestCatalog._create_session`` runs after ``_fetch_config`` and reads
    ``ssl``: ``ssl.client.cert`` / ``key`` become the client certificate (a
    path on the API host) and ``ssl.cabundle`` the verification setting
    (``false`` turns it off). ``overrides.uri`` sends every later request to
    another host. The connector's settings never forward ``ssl`` (only
    ``rest.*``, the token / credential and storage properties), so any ``ssl``
    after the merge came from the server.
    """
    for key in merged:
        if str(key).strip().lower() == "ssl" and (key not in own or merged[key] != own[key]):
            raise ValueError(_server_session_message(key, "set this connection's TLS settings"))
    if "uri" in merged and not _same_uri(merged.get("uri"), own.get("uri")):
        raise ValueError(_server_session_message("uri", "move this catalog to another address"))


def _server_session_message(key: Any, does: str) -> str:
    shown = "".join(ch for ch in str(key) if ch.isprintable())[:40]
    return (
        f"Iceberg property {shown} {SERVER_CONFIG} would {does} (a client certificate or CA "
        "file on this server, certificate verification, or the catalog's URI). Datawrap does not "
        "accept those from a catalog server. Remove it from the server's defaults and overrides."
    )


def check_table_response(table_response: Any) -> None:
    """Each source of a REST table response, separately (follow-up review 2, H2).

    pyiceberg hands the FileIO ``{**metadata.properties, **config,
    **credential config}``, so a blank in the config or a storage credential
    hid a class the metadata names from the merged check, while the table keeps
    the raw metadata and ``Table.location_provider()`` imports that class.
    """
    check_class_properties(table_response.metadata.properties, TABLE_METADATA)
    check_class_properties(table_response.config, TABLE_CONFIG)
    for credential in getattr(table_response, "storage_credentials", None) or ():
        check_class_properties(credential.config, STORAGE_CREDENTIAL)


def guarded_file_io(properties: Any, location: str | None = None) -> Any:
    from pyiceberg.io import load_file_io

    return load_file_io(file_io_properties(properties), location=location)


@lru_cache(maxsize=None)
def guarded_catalog(base: type) -> type:
    """``base`` (a pyiceberg catalog class) with each AWS signer checked where it is built."""

    class Guarded(base):  # type: ignore[misc, valid-type]
        def __init__(self, name: str, *args: Any, **properties: Any) -> None:
            if base.__name__ == "GlueCatalog" and not args and properties.get("client") is None:
                check_catalog_properties("glue", properties)
            super().__init__(name, *args, **properties)
            # pyiceberg also builds a FileIO from these alone (SqlCatalog's metadata file).
            self.properties.update(file_io_properties(self.properties))

        def _fetch_config(self) -> None:  # REST: the server's defaults and overrides
            own = dict(self.properties)
            super()._fetch_config()
            check_class_properties(self.properties, SERVER_CONFIG)
            # Before _create_session reads them (follow-up review 2, M1).
            check_server_session_properties(own, self.properties)

        def _init_sigv4(self, session: Any) -> None:  # REST: after the server's overrides
            check_catalog_properties("rest", {**self.properties, "rest.sigv4-enabled": "true"})
            super()._init_sigv4(session)

        def _load_file_io(self, properties: Any = None, location: str | None = None) -> Any:
            merged = {**self.properties, **(properties or {})}  # what pyiceberg hands the FileIO
            return super()._load_file_io(file_io_properties(merged), location)

        def commit_table(self, *args: Any, **kwargs: Any) -> Any:
            response = super().commit_table(*args, **kwargs)
            # The next write loads the location provider from these properties
            # (the raw metadata: a commit response carries no config to merge).
            check_class_properties(response.metadata.properties, COMMIT)
            return response

        if hasattr(base, "_response_to_table"):  # REST: load, create, register, refresh

            def _response_to_table(self, identifier: Any, table_response: Any) -> Any:
                check_table_response(table_response)
                return super()._response_to_table(identifier, table_response)

            def _response_to_staged_table(self, identifier: Any, table_response: Any) -> Any:
                check_table_response(table_response)
                return super()._response_to_staged_table(identifier, table_response)

    Guarded.__name__ = Guarded.__qualname__ = f"Keyed{base.__name__}"
    return Guarded
