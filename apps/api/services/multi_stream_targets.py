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


def file_export_refusal(request: Any) -> str | None:
    """Why a multi-stream run cannot go to this file export, or ``None``.

    A file export is the buffered single-table route: it reads the request's
    one source table and writes one file, so a run that selected several
    streams exported only the first and reported success (the other streams
    were never read; Gate-8 checked the one file against the one table). That
    route cannot hold several streams, so the run is refused before any read
    or write. A SQLite export is a database in a file and lands one table per
    stream, so it is not refused.
    """
    from services.sync_cursor import resolve_selected_sync_contracts
    from src.transfer.endpoint_kind import canonical_destination

    destination = getattr(request, "destination", None)
    kind, _fmt = canonical_destination(
        str(getattr(destination, "kind", "") or ""), str(getattr(destination, "format", "") or "")
    )
    if kind != "file_export":
        return None
    selected = resolve_selected_sync_contracts(getattr(request, "stream_contracts", None))
    if len(selected) < 2:
        return None
    names = ", ".join((c.name or "").strip() or "stream" for c in selected)
    return (
        f"A file export writes one file, and this run selects {len(selected)} streams "
        f"({names}): only the first would be exported. Run one export per stream, or "
        "choose a destination that keeps one object per stream (a database, a SQLite "
        "export, or an S3 / GCS / ADLS object store)."
    )
