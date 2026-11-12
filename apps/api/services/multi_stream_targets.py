"""Which destination object one stream of a multi-stream run lands in.

A multi-table run writes each selected stream to the object named after that
stream — the rule SQL tables follow, where ``destination.table`` names the
first stream and every stream lands in its own table. Writers and verifiers
resolve the object name as ``table or collection`` (``resolve_dest_table``),
while MongoDB readers and the Studio prefer ``collection``. Remapping only one
of the two fields left the other naming the request's single object: a
MongoDB destination with ``table`` set, or any destination with both fields
set, wrote every stream into that one object and each stream overwrote the
one before it. Every name field the request set is therefore remapped.
"""

from __future__ import annotations

from typing import Any


def bind_endpoint_to_stream(
    endpoint: Any,
    stream_name: str,
    *,
    table: str | None = None,
    collection: str | None = None,
) -> None:
    """Point ``endpoint`` at ``stream_name`` in every field that names its object.

    ``table`` / ``collection`` are the request's own values (the caller keeps
    them to restore the endpoint afterwards); they default to the endpoint's
    current fields. A collection-addressed endpoint (MongoDB, or one whose
    request named a collection) gets the stream as its collection; ``table``
    is set as well whenever the request set it, or when nothing else would
    name the stream's object (an unset ``table`` falls back to ``database`` —
    an index, topic or key space shared by every stream — in the resolver).
    """
    table = getattr(endpoint, "table", None) if table is None else table
    collection = getattr(endpoint, "collection", None) if collection is None else collection
    by_collection = str(getattr(endpoint, "format", "") or "").lower() == "mongodb" or bool(
        collection
    )
    if by_collection:
        endpoint.collection = stream_name
    if table or not by_collection:
        endpoint.table = stream_name
