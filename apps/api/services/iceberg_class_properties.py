"""Iceberg properties that name a Python class: only the FileIO classes Datawrap ships (follow-up review 1).

pyiceberg imports and instantiates whatever class some properties name by
dotted path: the FileIO (``py-io-impl``), the catalog (``py-catalog-impl``),
the S3 retry strategy (``s3.retry-strategy-impl``), the location provider
(``write.py-location-provider.impl``, a table property) and a REST catalog's
auth manager (``auth`` = ``{"type": "custom", "impl": ...}``, or any
``type`` pyiceberg has not registered). A tenant runs its own REST catalog
server, so those properties can come from the connector's settings, from the
server's ``/v1/config`` defaults and overrides, from a table's load response
(its config, metadata properties and storage credentials, each checked on its
own: follow-up review 2, H2) and from the metadata a commit returns. pyiceberg 0.12's S3 signer (``s3.signer``) is
looked up in a fixed registry, not imported.

``py-io-impl`` may name only a FileIO in :data:`ALLOWED_FILE_IO`. Any other
class-name property (:func:`names_a_class`) is refused, and so is a registered
auth manager that signs with the API host's own credentials (``google``:
Application Default Credentials or a credentials file on the host; ``entra``:
``DefaultAzureCredential``), which would send the platform's bearer token to
the tenant's catalog server (follow-up review 1, secfu20). The message names the
property and where it came from, and repeats no value. The operator's
platform-AWS-credentials opt-in does not change this. Where each source is
checked: ``connectors.iceberg_credential_guard`` and ``iceberg_catalog.load_catalog``
(build and use), ``egress_guard.connector_save_refusal`` (save).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PY_IO_IMPL = "py-io-impl"
#: The FileIO classes Datawrap ships and uses: pyiceberg's ``PyArrowFileIO``
#: (the guard substitutes its local-URI subclass) and Datawrap's local-URI and
#: keyed S3 subclasses of it (``connectors.iceberg_pyarrow_io``), which refuse
#: GCS / Azure / S3 without the table's own credential. Not
#: ``pyiceberg.io.fsspec.FsspecFileIO`` (follow-up review 2, L1): its gs / gcs /
#: abfs / abfss / hf filesystems (gcsfs, adlfs, huggingface_hub) fall back to the
#: API host's identity and are not guarded; Datawrap never selects it.
ALLOWED_FILE_IO = frozenset(
    {
        "pyiceberg.io.pyarrow.PyArrowFileIO",
        "connectors.iceberg_pyarrow_io.LocalUriPyArrowFileIO",
        "connectors.iceberg_pyarrow_io.KeyedS3PyArrowFileIO",
    }
)
#: pyiceberg's registered REST auth managers that use only their configuration's
#: secret; any other ``auth.type`` is imported by path, or (``google``, ``entra``)
#: signs with the API host's own cloud credentials.
_AUTH_TYPES = frozenset({"noop", "basic", "legacyoauth2", "oauth2"})
_HOST_CREDENTIAL_AUTH_TYPES = frozenset({"google", "entra"})
_CLASS_SUFFIXES = ("-impl", ".impl", "_impl", "credentials-provider", "credential-provider")

SETTINGS = "in this connector's Iceberg settings"
SERVER_CONFIG = "in the REST catalog server's configuration"
FILE_IO = "in an Iceberg FileIO's properties (the connector's settings, or a table's configuration from its catalog)"
COMMIT = "in the table metadata the catalog returned for a commit"
TABLE_METADATA = "in the table metadata the catalog returned"
TABLE_CONFIG = "in the table configuration the catalog returned"
STORAGE_CREDENTIAL = "in a storage credential the catalog returned with a table"

_ICEBERG_TYPES = frozenset({"iceberg", "apache_iceberg"})


def names_a_class(key: Any) -> bool:
    """True for a property pyiceberg (or a later pyiceberg) reads as a class's dotted path."""
    name = str(key).strip().lower()
    return name == "impl" or name.endswith(_CLASS_SUFFIXES)


def _shown(key: Any) -> str:
    return "".join(ch for ch in str(key) if ch.isprintable())[:80]


def _message(key: Any, where: str) -> str:
    return (
        f"Iceberg property {_shown(key)} {where} names a Python class for pyiceberg to load. "
        "Datawrap loads only the FileIO classes it ships, and no other class an Iceberg "
        "catalog's settings name. Remove the property."
    )


def class_property_refusal(props: Any, where: str) -> str:
    """Why ``props`` would make pyiceberg import a class Datawrap does not ship, else ``""``."""
    if not isinstance(props, Mapping):
        return ""
    for key, value in props.items():
        name = str(key).strip().lower()
        if name == PY_IO_IMPL:
            if value not in (None, "") and not (isinstance(value, str) and value in ALLOWED_FILE_IO):
                return (
                    f"Iceberg property {_shown(key)} {where} names a FileIO class Datawrap does not "
                    f"ship. Remove it, or name one of: {', '.join(sorted(ALLOWED_FILE_IO))}."
                )
        elif name == "auth" and isinstance(value, Mapping):
            auth_type = value.get("type")
            if value.get("impl") not in (None, ""):
                return _message(f"{_shown(key)}.impl", where)
            if isinstance(auth_type, str) and auth_type in _HOST_CREDENTIAL_AUTH_TYPES:
                return (
                    f"Iceberg property {_shown(key)}.type {where} selects an auth manager that signs "
                    "with the API host's own cloud credentials. Datawrap does not send the platform's "
                    "credentials to a workspace's catalog. Authenticate with this connector's own "
                    "token or credential instead."
                )
            if auth_type is not None and not (isinstance(auth_type, str) and auth_type in _AUTH_TYPES):
                return _message(f"{_shown(key)}.type", where)
        elif names_a_class(key) and value not in (None, ""):
            return _message(key, where)
    return ""


def check_class_properties(props: Any, where: str) -> None:
    """Raise ``ValueError`` when ``props`` name a class Datawrap does not ship."""
    refusal = class_property_refusal(props, where)
    if refusal:
        raise ValueError(refusal)


def connector_class_refusal(endpoint: Any) -> str:
    """An Iceberg connector's settings (its top-level keys and ``extra``) naming a class, else ``""``."""
    top = endpoint if isinstance(endpoint, Mapping) else {}
    extra = top.get("extra") if top else getattr(endpoint, "extra", None)
    return class_property_refusal(top, SETTINGS) or class_property_refusal(extra, SETTINGS)


def iceberg_connector_class_refusal(data: Mapping[str, Any]) -> str:
    """At save: :func:`connector_class_refusal` for an Iceberg connector, else ``""``."""
    if str(data.get("type") or data.get("format") or "").strip().lower() not in _ICEBERG_TYPES:
        return ""
    return connector_class_refusal(data)
