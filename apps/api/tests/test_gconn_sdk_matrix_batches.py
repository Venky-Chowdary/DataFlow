from __future__ import annotations

import pytest

import connectors.sdk as sdk
from connectors.sdk import RecordBatch, StreamSchema
from connectors.sdk.declarative.errors import ConnectorError


class _MultiBatchSource:
    def __init__(self, _config: dict) -> None:
        pass

    def read(self, stream: str, **_kwargs):
        schema = StreamSchema(name=stream, properties={"id": "integer"})
        yield RecordBatch(
            stream=stream,
            records=[{"id": 1}],
            schema=schema,
            state={"page_token": "page-2", "pages_done": 1},
        )
        yield RecordBatch(
            stream=stream,
            records=[{"id": 2}],
            schema=schema,
            state={"page_token": None, "pages_done": 2},
        )


def test_sdk_read_as_matrix_consumes_batches_until_limit(monkeypatch) -> None:
    monkeypatch.setattr(sdk, "get_sdk_connector", lambda _name: _MultiBatchSource)

    headers, rows, schema, state = sdk.sdk_read_as_matrix(
        "synthetic_multi_batch", {}, "items", limit=2
    )

    assert headers == ["id"]
    assert rows == [["1"], ["2"]]
    assert schema == {"id": "integer"}
    assert state == {"page_token": None, "pages_done": 2}


def test_sdk_read_as_matrix_fails_closed_when_more_rows_remain(monkeypatch) -> None:
    monkeypatch.setattr(sdk, "get_sdk_connector", lambda _name: _MultiBatchSource)

    with pytest.raises(ConnectorError, match="limit"):
        sdk.sdk_read_as_matrix("synthetic_multi_batch", {}, "items", limit=1)
