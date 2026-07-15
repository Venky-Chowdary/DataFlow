"""Elasticsearch index reader — search_after pagination for million-row indexes."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import cell_to_string


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int = 0


def _client(cfg: dict[str, Any]):
    from elasticsearch import Elasticsearch

    if cfg.get("connection_string"):
        url = cfg["connection_string"]
    else:
        scheme = "https" if cfg.get("ssl") or int(cfg.get("port") or 9200) == 443 else "http"
        url = f"{scheme}://{cfg.get('host') or 'localhost'}:{cfg.get('port') or 9200}"
    kwargs: dict[str, Any] = {"hosts": [url], "request_timeout": 60}
    if cfg.get("username") and cfg.get("password"):
        kwargs["basic_auth"] = (cfg["username"], cfg["password"])
    elif cfg.get("api_key"):
        api_key = cfg["api_key"].strip()
        if ":" in api_key:
            key_id, key_value = api_key.split(":", 1)
            kwargs["api_key"] = (key_id, key_value)
        else:
            kwargs["api_key"] = api_key
    return Elasticsearch(**kwargs)


def _cell(value: Any) -> str:
    return cell_to_string(value)


def read_index_batch(
    *,
    cfg: dict[str, Any],
    index: str,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
    search_after: list | None = None,
) -> tuple[ReadBatch, list | None]:
    del offset  # search_after replaces offset for scale
    client = _client(cfg)
    try:
        if known_total_rows is not None:
            total = known_total_rows
        else:
            count_resp = client.count(index=index)
            total = int(count_resp.get("count", 0))

        body: dict[str, Any] = {
            "size": min(limit, 10000),
            "query": {"match_all": {}},
            "sort": ["_doc"],
        }
        if search_after:
            body["search_after"] = search_after

        resp = client.search(index=index, body=body)
        hits = resp.get("hits", {}).get("hits") or []
        records = [h.get("_source") or {} for h in hits]
        if columns:
            headers = columns
        else:
            keys: set[str] = set()
            for rec in records:
                keys.update(rec.keys())
            headers = sorted(keys)
        rows = [[_cell(r.get(h)) for h in headers] for r in records]
        next_after = hits[-1].get("sort") if hits else None
        batch = ReadBatch(headers=headers, rows=rows, offset=0, total_rows=total)
        return batch, next_after
    finally:
        client.close()
