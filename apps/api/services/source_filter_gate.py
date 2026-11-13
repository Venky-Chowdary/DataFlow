"""Whether a declared ``source_filter`` can be honoured on this run.

A source filter is how an operator keeps rows out of a destination (a tenant,
a region, a PII subset). A filter that is declared and then not applied is
over-delivery with a green result, so every surface asks the same question
before anything is read or written: the preflight gate that Validate / Plan
show, and Execute's own refusal (a programmatic run may skip preflight).

Two ways a filter cannot be honoured:

* its shape is not one :mod:`services.row_filter` can evaluate;
* the route does not apply row filters. ``mirror`` and ``scd2`` stage the
  whole source and merge it against the history stored at the destination: a
  filtered stage would read every row the filter leaves out as deleted at the
  source. They refuse a filter the way they refuse a pre-load transform recipe
  (:mod:`services.history_route_recipe`), rather than ignore it.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

GATE_ID = "g_source_filter"
#: Sync modes whose streaming route stages the whole source and never applies
#: ``source_filter``.
UNFILTERED_SYNCS = frozenset({"full_refresh_mirror", "mirror", "scd2"})


def filter_shape_refusal(source_filter: Any) -> str:
    """Why this filter spec cannot be evaluated, or ``""``."""
    if not source_filter:
        return ""
    from services.row_filter import canonical_filter_spec

    try:
        canonical_filter_spec(source_filter)  # type: ignore[arg-type]
    except ValueError as exc:
        return (
            f"source_filter is refused: {exc} Nothing was read or written. A "
            "filter that cannot be evaluated is never treated as 'keep every row'."
        )
    return ""


def _sync_modes(sync_mode: str | None, stream_contracts: Iterable[Any] | None) -> set[str]:
    from services.sync_cursor import resolve_effective_sync_mode

    selected = [
        c for c in (stream_contracts or [])
        if isinstance(c, Mapping) and c.get("selected", True)
    ]
    if not selected:
        return {resolve_effective_sync_mode(sync_mode).lower()}
    return {
        resolve_effective_sync_mode(sync_mode, str(c.get("sync_mode") or "")).lower()
        for c in selected
    }


def filter_route_refusal(
    source_filter: Any, sync_mode: str | None, stream_contracts: Iterable[Any] | None = None
) -> str:
    """Why this route cannot apply a (valid) filter, or ``""``."""
    if not source_filter:
        return ""
    hit = sorted(_sync_modes(sync_mode, stream_contracts) & UNFILTERED_SYNCS)
    if not hit:
        return ""
    return (
        f"source_filter is not applied on the {hit[0]} route: it stages the whole "
        "source and merges it against the history stored on the destination, so "
        "a filtered read would treat every row the filter leaves out as deleted "
        "at the source. Nothing was read or written. Remove the filter for this "
        "sync mode (filter in a source view instead), or use full refresh / "
        "incremental append / upsert, which filter on the read."
    )


def source_filter_refusal(
    source_filter: Any, sync_mode: str | None, stream_contracts: Iterable[Any] | None = None
) -> str:
    """Execute's refusal for this filter on this route, or ``""``."""
    return filter_shape_refusal(source_filter) or filter_route_refusal(
        source_filter, sync_mode, stream_contracts
    )


def source_filter_gate(source_filter: Any, sync_mode: str | None) -> dict[str, Any] | None:
    """Preflight gate: the same refusal text Execute raises."""
    refusal = source_filter_refusal(source_filter, sync_mode)
    if not refusal:
        return None
    return {
        "id": GATE_ID,
        "status": "block",
        "message": refusal,
        "duration_ms": 0,
        "details": {"sync_mode": sync_mode, "kind": "source_filter"},
    }


def route_request_gate(
    shape_recipe: Any, sync_mode: str | None, source_filter: Any = None
) -> dict[str, Any] | None:
    """The route gate preflight shows: a refused filter, else a refused recipe."""
    from services.multi_stream_plan import history_shape_route_gate

    return source_filter_gate(source_filter, sync_mode) or history_shape_route_gate(
        shape_recipe, sync_mode or ""
    )


def request_refusal(request: Any) -> str:
    """Why Execute must not start this request, or ``""`` (checked before any write).

    One ordered list of the up-front refusals: a local-disk destination this
    deployment does not allow (D276), an unsafe destination identifier, a
    mapped target name the destination would rename or cut (G11), and a source
    filter that cannot be honoured.
    """
    from connectors.destination_naming import target_names_refusal
    from connectors.sql_identifiers import unsafe_destination_identifier_reason
    from services.local_file_policy import local_destination_refusal
    from services.target_name_legacy import (
        adopt_for_request,
        legacy_fields_refusal,
        selected_streams,
        streams_refusal,
    )

    dest = request.destination
    # G11: an existing table keeps the columns an earlier run named; the
    # mapping is retargeted before any read, write or Gate-8 digest.
    adopt_for_request(request)
    return (
        local_destination_refusal(dest)
        or unsafe_destination_identifier_reason(
            str(dest.format or ""), str(dest.table or dest.collection or "")
        )
        or (
            # Multi-stream: each stream against its own object and Map rows.
            streams_refusal(request)
            if selected_streams(request)
            else target_names_refusal(str(dest.format or ""), getattr(request, "mappings", None))
            or legacy_fields_refusal(request)
        )
        or source_filter_refusal(
            getattr(request, "source_filter", None),
            getattr(request, "sync_mode", None),
            getattr(request, "stream_contracts", None),
        )
        or ""
    )
