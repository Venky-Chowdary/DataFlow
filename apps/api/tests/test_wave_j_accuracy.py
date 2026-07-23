"""Wave J accuracy: REST pagination, ES/Neo4j identity, JSON/XML multi-collection, ORC Arrow."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_rest_offset_pagination_continues_without_cursor(monkeypatch):
    from connectors import rest_api as ra

    pages = {
        0: [{"id": i} for i in range(100)],
        100: [{"id": i} for i in range(100, 150)],
    }

    def fake_read_page(cfg, pagination, next_url=None):
        start = int(pagination.get("offset") or pagination.get(cfg.get("offset_param", "offset")) or 0)
        # resolve uses offset_param default "offset"
        for k, v in pagination.items():
            if k != "limit" and k != cfg.get("limit_param", "limit"):
                start = int(v)
                break
        records = pages.get(start, [])
        return records, None, False  # no cursor — old bug stopped here

    monkeypatch.setattr(ra, "_read_page", fake_read_page)
    monkeypatch.setattr(
        ra,
        "_resolve_config",
        lambda cfg: {
            **cfg,
            "pagination_type": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_param": "page",
            "cursor_param": "cursor",
            "data_path": "",
        },
    )
    batch = ra.read_object(cfg={"host": "http://example"}, limit=150, offset=0)
    assert len(batch.rows) == 150


def test_json_refuses_multiple_sibling_collections():
    from services.json_tabular import extract_json_records

    with pytest.raises(ValueError, match="multiple array-of-object"):
        extract_json_records({
            "orders": [{"id": 1}],
            "refunds": [{"id": 2}],
        })

    rows = extract_json_records(
        {"orders": [{"id": 1}], "refunds": [{"id": 2}]},
        records_path="orders",
    )
    assert rows == [{"id": 1}]


def test_json_preferred_wrapper_still_works():
    from services.json_tabular import extract_json_records

    rows = extract_json_records({"countries": [{"name": "India"}], "count": 2})
    assert rows[0]["name"] == "India"
    rows = extract_json_records({"data": [{"a": 1}], "items": [{"b": 2}]})
    assert rows == [{"a": 1}]  # data ranks above items


def test_xml_refuses_multiple_sibling_collections():
    from services.file_parser import FileParser

    # Mimic xmltodict shape without requiring a working pyexpat build.
    root = {
        "root": {
            "orders": {"order": [{"id": "1"}]},
            "refunds": {"refund": [{"id": "2"}]},
        }
    }
    # xmltodict often collapses single children; force list-of-dict collections:
    root = {
        "root": {
            "orders": [{"id": "1"}, {"id": "2"}],
            "refunds": [{"id": "9"}],
        }
    }
    records, path, ambiguity = FileParser._extract_xml_records(root)
    assert records is None
    assert ambiguity and "multiple" in ambiguity.lower()


def test_elasticsearch_emits_document_id():
    from connectors.elasticsearch_reader import read_index_batch

    mock_client = MagicMock()
    mock_client.count.return_value = {"count": 1}
    mock_client.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_id": "doc-1",
                    "_index": "orders",
                    "_source": {"amount": 10},
                    "sort": [1],
                }
            ]
        }
    }
    with patch("connectors.elasticsearch_reader._client", return_value=mock_client):
        batch, _ = read_index_batch(cfg={}, index="orders", limit=10)
    assert "_id" in batch.headers
    assert batch.headers[0] == "_id"
    id_idx = batch.headers.index("_id")
    assert batch.rows[0][id_idx] == "doc-1"


def test_neo4j_extract_preserves_element_id():
    from connectors.neo4j import _extract_rows

    body = {
        "results": [
            {
                "columns": ["_neo4j_element_id", "_neo4j_labels", "props"],
                "data": [
                    {"row": ["4:abc:1", ["Person"], {"name": "Ada"}]},
                ],
            }
        ]
    }
    headers, rows = _extract_rows(body)
    assert "_neo4j_element_id" in headers
    assert "name" in headers
    eid = headers.index("_neo4j_element_id")
    name = headers.index("name")
    assert rows[0][eid] == "4:abc:1"
    assert rows[0][name] == "Ada"


def test_orc_parse_uses_arrow_schema_path(monkeypatch):
    """ORC path must prefer Arrow schema + full num_rows (not pandas head length)."""
    import sys
    import types

    pytest.importorskip("pyarrow")
    from services.file_parser import FileParser

    class FakeTable:
        schema = object()
        column_names = ["amt", "id"]
        num_rows = 50

        def slice(self, offset, length):
            return self

        def to_pylist(self):
            return [{"amt": 1.0, "id": 1}]

    fake_orc = types.ModuleType("pyarrow.orc")
    fake_orc.read_table = lambda buf: FakeTable()  # type: ignore[attr-defined]

    monkeypatch.setattr(
        "services.arrow_schema.schema_from_arrow",
        lambda s: {"amt": "FLOAT", "id": "INTEGER"},
    )
    monkeypatch.setattr(
        "services.arrow_schema.columns_from_arrow_schema",
        lambda s: [
            {"name": "amt", "inferred_type": "FLOAT", "source": "arrow_schema"},
            {"name": "id", "inferred_type": "INTEGER", "source": "arrow_schema"},
        ],
    )
    monkeypatch.setitem(sys.modules, "pyarrow.orc", fake_orc)

    result = FileParser.parse_orc(b"fake", max_rows=1)
    assert result.success is False
    assert result.row_count == 50
    assert "streaming" in (result.error or "").lower()
    assert result.schema_map == {"amt": "FLOAT", "id": "INTEGER"}
    assert result.data == []


def test_object_version_token_in_cache_key():
    from connectors.object_store_common import read_object_from_store

    calls: list[str] = []

    def fake_download(cache_key, downloader, force=False):
        calls.append(cache_key)
        return Path("/tmp/unused")

    with patch("connectors.object_store_common.download_object", side_effect=fake_download), patch(
        "connectors.object_store_common._object_version_token",
        return_value="etag123",
    ), patch(
        "connectors.object_store_common.read_rows_from_spill",
        return_value=(["a"], [["1"]], 1),
    ):
        read_object_from_store("s3", {}, "b", "k.csv")
    assert calls and "etag123" in calls[0]
