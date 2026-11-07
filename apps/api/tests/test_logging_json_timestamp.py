"""Logging timestamp must be valid on Windows (no strftime %03d)."""

from __future__ import annotations

import logging


def test_json_formatter_timestamp_is_iso_utc_ms():
    from services.logging_config import JsonFormatter

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    line = JsonFormatter().format(record)
    assert '"ts":' in line
    # Must not raise; must look like 2024-01-01T00:00:00.123Z
    import json

    payload = json.loads(line)
    ts = payload["ts"]
    assert ts.endswith("Z")
    assert "." in ts
    assert len(ts) >= 24
