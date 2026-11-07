"""Object-store source paths as operators type them.

Studio's Cloud-storage field is labelled "Object path / prefix" with the
placeholder ``s3://bucket/path/orders.jsonl``. Every reader takes a bucket
(``database``) plus a key, so ``s3://bucket/key`` used to be read as the
literal key ``s3://bucket/key`` -- "no object" -- on every route (QA demo,
557f556c). The URL is split once, where an endpoint is built.
"""

from __future__ import annotations

import re

_OBJECT_URL = re.compile(r"^(?:s3|s3a|s3n|gs|gcs)://([^/]+)/?(.*)$", re.IGNORECASE)


def split_object_url(raw: str) -> tuple[str, str] | None:
    """``(bucket, key)`` for ``s3://bucket/key`` (or gs://...), else None."""
    text = str(raw or "").strip()
    match = _OBJECT_URL.match(text)
    if not match:
        return None
    return match.group(1), match.group(2)


def normalize_object_source(database: str, table: str) -> tuple[str, str]:
    """Bucket and key for an endpoint whose table may be an object URL.

    The URL's bucket fills an empty bucket. A URL naming a different bucket
    than the endpoint's is left as typed, so the read reports it missing
    rather than silently reading another bucket under this connector.
    """
    split = split_object_url(table)
    if split is None:
        return database, table
    bucket, key = split
    current = str(database or "").strip()
    if current and current != bucket:
        return database, table
    return bucket, key


def source_pattern_sample_key(endpoint, cfg: dict, key: str, family) -> str:
    """The object a source prefix/glob is profiled from, or "" for a plain key.

    A source prefix (``sales/2026-10/``) is a set of files: Execute loads each
    one (services.file_pickup_load). Studio's preview turned it into the
    *destination* folder key ``sales/2026-10/export.json`` and reported "no
    object". The source is profiled from its first file in load order; a
    destination keeps its folder meaning.
    """
    purpose = str((getattr(endpoint, "extra", None) or {}).get("introspect_purpose") or "")
    if purpose != "source":
        return ""
    from services.file_pickup import is_file_pattern, list_remote_files, ordered_files

    if not is_file_pattern(key):
        return ""
    store = family(str(cfg.get("type") or getattr(endpoint, "format", "") or ""))
    if store not in {"s3", "gcs", "adls"}:
        return ""
    bucket = str(cfg.get("database") or getattr(endpoint, "database", "") or "")
    try:
        listed = ordered_files(list_remote_files(store, cfg, bucket, key))
    except Exception:
        return ""  # the read reports the listing error
    return listed[0].name if listed else ""
