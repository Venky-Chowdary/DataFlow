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
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from services.platform_aws_credentials import iceberg_credentials_refusal, s3_fileio_refusal

KEYED_S3_FILE_IO = "connectors.iceberg_pyarrow_io.KeyedS3PyArrowFileIO"
_PY_IO_IMPL = "py-io-impl"


def check_catalog_properties(catalog_type: str, properties: dict[str, Any]) -> None:
    """Raise ``ValueError`` when the catalog would sign with the platform's AWS chain."""
    refusal = iceberg_credentials_refusal(catalog_type, properties)
    if refusal:
        raise ValueError(refusal)


def file_io_properties(properties: Any) -> dict[str, Any]:
    """``properties`` with the keyed S3 FileIO when S3 would be opened with the platform chain.

    A FileIO's properties are fixed once it is built, so the check is made on
    them here; the keyed IO refuses only when S3 is opened, and a local
    warehouse still works.
    """
    merged = dict(properties or {})
    if s3_fileio_refusal(merged):
        merged[_PY_IO_IMPL] = KEYED_S3_FILE_IO
    return merged


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

        def _init_sigv4(self, session: Any) -> None:  # REST: after the server's overrides
            check_catalog_properties("rest", {**self.properties, "rest.sigv4-enabled": "true"})
            super()._init_sigv4(session)

        def _load_file_io(self, properties: Any = None, location: str | None = None) -> Any:
            return super()._load_file_io(file_io_properties(properties), location)

    Guarded.__name__ = Guarded.__qualname__ = f"Keyed{base.__name__}"
    return Guarded
