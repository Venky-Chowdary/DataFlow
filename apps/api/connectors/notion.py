"""Notion REST API source connector.

Notion serves reads only on POST endpoints: ``POST /search`` (pages,
databases) and ``POST /databases/{id}/query`` (database rows), with
``start_cursor`` in the request body. The SDK manifest
(``connectors/sdk/declarative/manifests/notion.json``) encodes that contract
and is what the transfer engine and ``sample_connector_object`` read through.
"""

from __future__ import annotations

from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch


def test_notion(
    *,
    connector_type: str = "notion",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Notion connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    """Read up to ``limit`` rows of a Notion stream (pages, databases, database_rows).

    Same reader as the engine and the sample: the generic REST reader built
    ``GET <base>/<object>?page_size=N``, a path Notion does not serve (HTTP
    400), so introspect / plan / start could not read what sample could
    (D311). Pages follow the manifest's body cursor until ``offset + limit``
    rows or the stream ends.
    """
    from connectors.sdk.transfer_bridge import read_object as sdk_read_object

    resolved_cfg = {**cfg, "type": "notion"}
    stream = (object or str(cfg.get("table") or "")).strip()
    wanted = max(0, int(offset or 0)) + max(0, int(limit or 0))
    headers: list[str] = []
    records: list[dict[str, str]] = []
    meta: dict[str, Any] = {}
    state: str | None = None
    while len(records) < wanted:
        page = sdk_read_object(
            cfg=resolved_cfg, object=stream, limit=wanted - len(records), sdk_state=state
        )
        meta = dict(page.meta or {})
        for name in page.headers:
            if name not in headers:
                headers.append(name)
        records.extend(dict(zip(page.headers, row)) for row in page.rows)
        state = str(meta.get("sdk_state") or "") or None
        if meta.get("sdk_done") or not page.rows or state is None:
            break
    kept = records[int(offset or 0):wanted]
    return ReadBatch(
        headers=headers,
        rows=[[record.get(name) for name in headers] for record in kept],
        offset=offset,
        total_rows=None,
        meta={"native_types": meta.get("native_types") or {}, "sdk_stream": stream},
        raw_page_rows=len(kept),
    )
