"""Re-read an API source for the Gate-8 transform proof (D269).

The proof re-reads the source raw and re-applies the approved recipe. Its
reader built a SQLAlchemy engine for every non-Mongo source, so a Stripe /
HubSpot / SDK-manifest source failed with "SQLAlchemy dialect/driver for
'stripe' is not available" and the proof was reported unverifiable on a
correct load. These sources are re-read here through the engine's own batch
reader (``stream._read_batch``) — the reader the write pass used — paged by
SDK state for manifest connectors and by offset for registry object readers.
"""

from __future__ import annotations

from typing import Any, Iterator

_PAGE = 1000


def _sdk_source(src_type: str) -> bool:
    from connectors.sdk import get_descriptor

    descriptor = get_descriptor(src_type)
    return bool(descriptor and "source" in descriptor.roles and descriptor.catalog_ids)


def is_api_source(src_type: str) -> bool:
    """True for sources read by an API/object reader rather than SQL."""
    from .connector_dispatch import REGISTRY_OBJECT_READERS

    kind = (src_type or "").strip().lower()
    return kind in REGISTRY_OBJECT_READERS or _sdk_source(kind)


#: File objects (CSV/JSONL/Parquet/XLSX…) the engine's batch reader pages by
#: row offset (``batch_readers._read_batch_unscoped``). D327: the proof built
#: a SQLAlchemy engine for them ("SQLAlchemy dialect/driver for 'sftp' is not
#: available") and never proved a file source's transforms.
_OFFSET_FILE_READERS = frozenset({"sftp", "s3", "gcs", "adls"})

#: Sources paged by an opaque continuation token: the offset loop below would
#: re-read page one forever, so their transforms are not re-computed.
_NO_PROOF_REREAD = frozenset({"dynamodb", "elasticsearch", "redis", "kafka", "qdrant"})


def is_reader_source(src_type: str) -> bool:
    """True when the proof re-reads this source through the write's own reader."""
    kind = (src_type or "").strip().lower()
    return kind in _OFFSET_FILE_READERS or is_api_source(kind)


def proof_unavailable_reason(source_endpoint: Any, src_type: str) -> str | None:
    """Plain-words reason the proof cannot re-read this source, or ``None``."""
    kind = (src_type or "").strip().lower()
    if getattr(source_endpoint, "kind", "") != "database":
        what = "uploaded or local file"
    elif kind in _NO_PROOF_REREAD:
        what = f"{kind} document or stream"
    else:
        return None
    return (
        f"transform proof not available for {what} sources — the recipe "
        "output was not recomputed from the source, so the transforms are "
        "not proven"
    )


def iter_api_source_batches(
    *,
    source_endpoint: Any,
    src_type: str,
    src_cfg: dict[str, Any],
    read_cols: list[str],
    page: int = _PAGE,
) -> Iterator[list[dict[str, Any]]]:
    """Yield the whole source, projected to ``read_cols``, one page at a time."""
    from .stream import _read_batch, _source_name, _unwrap_read

    table = _source_name(source_endpoint)
    database = str(getattr(source_endpoint, "database", "") or src_cfg.get("database") or "")
    sdk = _sdk_source(src_type)
    offset = 0
    state: Any = None
    seen_states: set[str] = set()
    while True:
        batch, _ = _unwrap_read(
            _read_batch(
                src_type,
                src_cfg,
                table,
                None,
                offset,
                page,
                database=database,
                sdk_state=state if sdk else None,
            )
        )
        headers = list(getattr(batch, "headers", None) or [])
        rows = list(getattr(batch, "rows", None) or [])
        if rows:
            yield [
                {c: rec.get(c) for c in read_cols}
                for rec in (dict(zip(headers, row)) for row in rows)
            ]
        meta = batch.meta if isinstance(getattr(batch, "meta", None), dict) else {}
        raw = int(getattr(batch, "raw_page_rows", None) or len(rows))
        if sdk:
            nxt = meta.get("sdk_state")
            if meta.get("sdk_done") or not nxt or str(nxt) in seen_states:
                return
            seen_states.add(str(nxt))
            state = nxt
            continue
        if raw == 0 or meta.get("has_more") is False or raw < page:
            return
        offset += raw
