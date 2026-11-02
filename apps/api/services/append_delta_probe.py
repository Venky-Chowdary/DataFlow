"""Validate's append-collision probe over every key the incremental read delivers.

QA round 11 (M3): an incremental append whose delta re-delivered 5 updated
keys passed Validate — the probe judged only the preview rows, which did not
contain them — and the writer then refused the batch at Run, so the 10 new
rows did not load either. A refusal discovered per writer batch can also come
after an earlier batch committed.

Here Validate (and Execute's own preflight) walks the delta itself, projected
to the identity columns and the cursor, and probes every key against the
destination before the first write. The walk is the same bounded table walk
the population-fit scan uses; when it cannot run (a file or callable source,
CDC) the caller's sample rows are probed as before and the writer's per-batch
guard still refuses an enforced-key collision.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Most delta rows walked for keys. Past it the probe judges what it read and
#: the writer's per-batch guard covers the rest.
MAX_DELTA_KEY_ROWS = 200_000


def _delta_key_rows(*, pairs: list[tuple[str, str]], cursor_column: str, walk_kw: dict[str, Any]) -> list[dict[str, Any]] | None:
    from services.preflight_population import _iter_table_population_for_preflight

    columns = list(dict.fromkeys([s for s, _t in pairs] + ([cursor_column] if cursor_column else [])))
    mappings = [m for m in walk_kw.pop("mappings") or [] if str(m.get("source") or "") in columns]
    if not mappings:
        return None
    rows = _iter_table_population_for_preflight(mappings=mappings, source_columns=columns, **walk_kw)
    if rows is None:
        return None
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(dict(row))
        if len(out) >= MAX_DELTA_KEY_ROWS:
            break
    return out


def probe_append_key_collisions_over_delta(
    *,
    source_kind: str = "",
    source_format: str = "",
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_filter: dict[str, Any] | None = None,
    column_types: dict[str, str] | None = None,
    read_scope: Any = None,
    probe: Any = None,
    **probe_kw: Any,
):
    """``probe_append_key_collisions`` fed the whole delta when it can be walked."""
    from services.destination_key_collision_probe import (
        _identity_pairs,
        probe_append_key_collisions,
        sync_mode_appends_without_key_resolution,
    )
    from services.sync_cursor import incremental_read_narrows

    sync_mode = str(probe_kw.get("sync_mode") or "")
    if (
        sync_mode_appends_without_key_resolution(sync_mode)
        and probe_kw.get("destination_table_exists") is True
        and str(source_kind or "").lower() in {"database", "cloud"}
    ):
        try:
            pairs = _identity_pairs(
                mappings=probe_kw.get("mappings"), source_columns=probe_kw.get("source_columns"),
                dest_kind=str(probe_kw.get("dest_kind") or ""),
                validation_mode=str(probe_kw.get("validation_mode") or ""),
                destination_pk_columns=probe_kw.get("destination_pk_columns"),
                contract_primary_key=probe_kw.get("contract_primary_key"),
                stream_contracts=probe_kw.get("stream_contracts"),
                source_table=str(probe_kw.get("source_table") or ""),
                destination_table=str(probe_kw.get("destination_table") or ""),
            )
            rows = _delta_key_rows(
                pairs=pairs,
                cursor_column=str(probe_kw.get("incremental_cursor_column") or ""),
                walk_kw=dict(
                    source_kind=source_kind, source_format=source_format,
                    source_connector_id=source_connector_id, source_config=source_config,
                    source_table=str(probe_kw.get("source_table") or ""),
                    mappings=list(probe_kw.get("mappings") or []),
                    column_types=dict(column_types or {}), dest_types={},
                    dest_db=str(probe_kw.get("destination_db_type") or ""), sync_mode=sync_mode,
                    read_scope=read_scope if incremental_read_narrows(sync_mode) else None,
                    source_filter=source_filter or None,
                ),
            ) if pairs else None
        except Exception as exc:  # noqa: BLE001 - the sample probe still runs
            logger.warning("append delta key walk failed; probing the sample: %s", exc)
            rows = None
        if rows is not None:
            probe_kw["sample_rows"] = rows
    return (probe or probe_append_key_collisions)(**probe_kw)
