from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from connectors.sdk import RecordBatch

logger = logging.getLogger(__name__)

CursorFormat = Literal["iso8601", "epoch_s", "epoch_ms"]


@dataclass(frozen=True)
class StreamState:
    cursor: Any = None
    page_token: Any = None
    pages_done: int = 0

    def __post_init__(self) -> None:
        if (
            isinstance(self.pages_done, bool)
            or not isinstance(self.pages_done, int)
            or self.pages_done < 0
        ):
            raise ValueError("pages_done must be a non-negative integer")

    def to_dict(self) -> dict[str, Any]:
        state = {
            "cursor": self.cursor,
            "page_token": self.page_token,
            "pages_done": self.pages_done,
        }
        try:
            json.dumps(state, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("StreamState must be JSON serializable") from exc
        return state

    @classmethod
    def from_dict(cls, state: Mapping[str, Any] | StreamState | None) -> StreamState:
        if state is None:
            return cls()
        if isinstance(state, cls):
            return state
        if not isinstance(state, Mapping):
            raise ValueError("stream state must be an object")
        pages_done = state.get("pages_done", 0)
        if isinstance(pages_done, bool) or not isinstance(pages_done, int) or pages_done < 0:
            raise ValueError("pages_done must be a non-negative integer")
        return cls(
            cursor=state.get("cursor"),
            page_token=state.get("page_token"),
            pages_done=pages_done,
        )


def _parse_cursor(value: Any, cursor_format: str) -> datetime | Decimal:
    if cursor_format == "iso8601":
        try:
            parsed = (
                value
                if isinstance(value, datetime)
                else datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
            )
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (OverflowError, TypeError, ValueError) as exc:
            raise ValueError("cursor is not a valid ISO-8601 timestamp") from exc
    if cursor_format in {"epoch_s", "epoch_ms"}:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"cursor is not a valid {cursor_format} value") from exc
        if not parsed.is_finite():
            raise ValueError(f"cursor is not a finite {cursor_format} value")
        return parsed
    raise ValueError(f"unsupported cursor_format {cursor_format!r}")


def _json_cursor_value(value: Any, cursor_format: str) -> Any:
    parsed = _parse_cursor(value, cursor_format)
    if isinstance(parsed, datetime):
        return parsed.isoformat().replace("+00:00", "Z")
    if parsed == parsed.to_integral_value():
        return int(parsed)
    return format(parsed, "f")


def _decimal_request_value(value: Decimal, original: Any) -> Any:
    if value == value.to_integral_value():
        if isinstance(original, int) and not isinstance(original, bool):
            return int(value)
        if isinstance(original, float):
            return float(value)
    if isinstance(original, float):
        return float(value)
    return format(value, "f")


def cursor_for_request(
    state: StreamState | Mapping[str, Any] | None,
    *,
    cursor_format: CursorFormat | str,
    lookback_s: int | float | Decimal = 0,
) -> Any:
    """Return the inclusive lower-bound cursor, subtracting the configured lookback."""
    current_state = StreamState.from_dict(state)
    if cursor_format not in {"iso8601", "epoch_s", "epoch_ms"}:
        raise ValueError(f"unsupported cursor_format {cursor_format!r}")
    if isinstance(lookback_s, bool):
        raise ValueError("lookback_s must be a non-negative finite number")
    try:
        lookback = Decimal(str(lookback_s))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("lookback_s must be a non-negative finite number") from exc
    if not lookback.is_finite() or lookback < 0:
        raise ValueError("lookback_s must be a non-negative finite number")
    if current_state.cursor is None:
        return None

    parsed = _parse_cursor(current_state.cursor, str(cursor_format))
    if lookback == 0:
        return current_state.cursor
    if isinstance(parsed, datetime):
        return (parsed - timedelta(seconds=float(lookback))).isoformat().replace(
            "+00:00", "Z"
        )
    if cursor_format == "epoch_ms":
        return _decimal_request_value(parsed - lookback * 1000, current_state.cursor)
    return _decimal_request_value(parsed - lookback, current_state.cursor)


def advance_stream_state(
    state: StreamState | Mapping[str, Any] | None,
    records: Iterable[Mapping[str, Any]],
    *,
    cursor_field: str,
    cursor_format: CursorFormat | str,
    page_token: Any = None,
    stream: str = "",
) -> StreamState:
    """Checkpoint the maximum cursor seen on a page, ignoring regressions.

    Cursor boundaries are inclusive; overlap may intentionally re-read records.
    This provides at-least-once rather than exactly-once delivery, so destinations
    should use an idempotent upsert or equivalent deduplication strategy.
    """
    current_state = StreamState.from_dict(state)
    if cursor_format not in {"iso8601", "epoch_s", "epoch_ms"}:
        raise ValueError(f"unsupported cursor_format {cursor_format!r}")
    if not cursor_field:
        raise ValueError("cursor_field must not be empty")
    best_value = current_state.cursor
    best_parsed = (
        _parse_cursor(best_value, str(cursor_format))
        if best_value is not None
        else None
    )

    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("incremental records must be objects")
        if cursor_field not in record or record[cursor_field] is None:
            continue
        value = record[cursor_field]
        parsed = _parse_cursor(value, str(cursor_format))
        if best_parsed is not None and parsed < best_parsed:
            logger.warning(
                "Ignoring regressing cursor for stream=%s field=%s",
                stream,
                cursor_field,
            )
            continue
        if best_parsed is None or parsed > best_parsed:
            best_parsed = parsed
            best_value = _json_cursor_value(value, str(cursor_format))

    return StreamState(
        cursor=best_value,
        page_token=page_token,
        pages_done=current_state.pages_done + 1,
    )


def run_sync(
    source: Any,
    stream: str,
    write_fn: Callable[[list[dict[str, Any]]], Any],
    load_state: Callable[[str], Mapping[str, Any] | StreamState | None],
    save_state: Callable[[str, dict[str, Any]], Any],
) -> int:
    """Write each page before saving its checkpoint; resume is at-least-once.

    A write exception propagates without persisting that page's state. If saving
    state fails after a successful write, the page can be replayed, so writes
    should be idempotent.
    """
    state = load_state(stream)
    if isinstance(state, StreamState):
        state = state.to_dict()
    elif state is not None:
        state = dict(state)

    records_written = 0
    batches = source.read(stream, state=state)
    for batch in batches:
        if not isinstance(batch, RecordBatch):
            raise TypeError("source.read must yield RecordBatch instances")
        write_fn(batch.records)
        batch_state = dict(batch.state or {})
        json.dumps(batch_state, allow_nan=False)
        save_state(stream, batch_state)
        state = batch_state
        records_written += len(batch.records)
    return records_written
