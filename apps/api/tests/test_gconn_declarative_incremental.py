from __future__ import annotations

import json
import logging
from typing import Any

import pytest

from connectors.sdk import RecordBatch
from connectors.sdk.declarative.incremental import (
    StreamState,
    advance_stream_state,
    cursor_for_request,
    run_sync,
)


def test_stream_state_is_json_serializable_and_advances_to_page_maximum() -> None:
    state = advance_stream_state(
        None,
        [
            {"updated_at": "2025-01-01T00:00:02Z"},
            {"updated_at": "2025-01-01T00:00:10Z"},
            {"updated_at": "2025-01-01T00:00:03Z"},
        ],
        cursor_field="updated_at",
        cursor_format="iso8601",
        page_token="next-page",
    )

    assert state.cursor == "2025-01-01T00:00:10Z"
    assert state.page_token == "next-page"
    assert state.pages_done == 1
    assert json.loads(json.dumps(state.to_dict())) == {
        "cursor": "2025-01-01T00:00:10Z",
        "page_token": "next-page",
        "pages_done": 1,
    }


def test_regressing_cursor_does_not_move_state_backwards(caplog) -> None:
    previous = StreamState(
        cursor="2025-01-02T00:00:00Z",
        page_token="old-page",
        pages_done=4,
    )
    with caplog.at_level(logging.WARNING):
        state = advance_stream_state(
            previous,
            [{"updated_at": "2025-01-01T00:00:00Z"}],
            cursor_field="updated_at",
            cursor_format="iso8601",
            page_token="next-page",
            stream="accounts",
        )

    assert state.cursor == previous.cursor
    assert state.page_token == "next-page"
    assert state.pages_done == 5
    assert "regressing cursor" in caplog.text
    assert previous.cursor not in caplog.text


def test_incremental_state_reads_nested_cursor_fields() -> None:
    state = advance_stream_state(
        None,
        [{"id": "1001", "fields": {"updated": "2026-01-02T03:04:05Z"}}],
        cursor_field="fields.updated",
        cursor_format="iso8601",
    )

    assert state.cursor == "2026-01-02T03:04:05Z"


@pytest.mark.parametrize(
    ("cursor_format", "cursor", "lookback_s", "expected"),
    [
        (
            "iso8601",
            "2025-01-01T00:00:10Z",
            5,
            "2025-01-01T00:00:05Z",
        ),
        ("epoch_s", 1_700_000_010, 5, 1_700_000_005),
        ("epoch_ms", 1_700_000_010_000, 5, 1_700_000_005_000),
        ("epoch_s", "1700000010.5", 0.25, "1700000010.25"),
    ],
)
def test_cursor_lookback_is_subtracted_per_format(
    cursor_format: str,
    cursor: Any,
    lookback_s: float,
    expected: Any,
) -> None:
    state = StreamState(cursor=cursor)

    assert (
        cursor_for_request(
            state,
            cursor_format=cursor_format,
            lookback_s=lookback_s,
        )
        == expected
    )


def test_run_sync_saves_only_after_successful_durable_write_and_resumes() -> None:
    first_page_state = {
        "cursor": "2025-01-01T00:00:10Z",
        "page_token": "next",
        "pages_done": 1,
    }

    class Source:
        def __init__(self) -> None:
            self.read_states: list[Any] = []

        def read(self, stream: str, *, state: Any = None):
            self.read_states.append(state)
            yield RecordBatch(
                stream=stream,
                records=[{"id": 1}],
                state=first_page_state,
            )

    source = Source()
    saved: list[tuple[str, dict[str, Any]]] = []
    attempted: list[list[dict[str, Any]]] = []

    def load_state(_stream: str) -> dict[str, Any] | None:
        return saved[-1][1] if saved else None

    def save_state(stream: str, state: dict[str, Any]) -> None:
        saved.append((stream, state))

    def failing_write(records: list[dict[str, Any]]) -> None:
        attempted.append(records)
        raise RuntimeError("destination write failed")

    with pytest.raises(RuntimeError, match="destination write failed"):
        run_sync(source, "users", failing_write, load_state, save_state)

    assert saved == []
    assert source.read_states == [None]

    def successful_write(records: list[dict[str, Any]]) -> None:
        attempted.append(records)

    assert run_sync(source, "users", successful_write, load_state, save_state) == 1
    assert source.read_states == [None, None]
    assert attempted == [[{"id": 1}], [{"id": 1}]]
    assert saved == [("users", first_page_state)]

    assert run_sync(source, "users", successful_write, load_state, save_state) == 1
    assert source.read_states[-1] == first_page_state


def test_invalid_cursor_format_and_negative_lookback_fail() -> None:
    with pytest.raises(ValueError, match="cursor_format"):
        cursor_for_request(StreamState(), cursor_format="unknown")
    with pytest.raises(ValueError, match="lookback_s"):
        cursor_for_request(
            StreamState(cursor="2025-01-01T00:00:00Z"),
            cursor_format="iso8601",
            lookback_s=-1,
        )
    with pytest.raises(ValueError, match="pages_done"):
        StreamState(pages_done=True)
