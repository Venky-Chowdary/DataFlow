"""Azure Blob Storage / ADLS Gen2 object reader — stream payloads to disk."""

from __future__ import annotations

import itertools
from typing import Any

from connectors.object_store_common import ReadBatch, read_object_from_store


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    return read_object_from_store(
        "adls", cfg, bucket, key, offset=offset, limit=limit, known_total_rows=known_total_rows
    )


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    # Resolved per call, like every other ADLS entry point: a module-level
    # binding freezes whichever credential factory happened to be installed when
    # this module was first imported.
    from connectors.adls_common import blob_service_client

    client = blob_service_client(cfg)
    container = client.get_container_client(bucket)
    # Azure names this filter ``name_starts_with``; ``prefix`` is a hard error
    # rather than a no-op, which took Gate-8 read-back on ADLS down to an
    # unverified warning. The parameter stays ``prefix`` here so the object-store
    # readers keep one shared signature.
    return [
        b.name
        for b in itertools.islice(container.list_blobs(name_starts_with=prefix or ""), 2000)
    ]
