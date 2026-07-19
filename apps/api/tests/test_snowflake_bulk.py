"""Snowflake writer bulk load threshold."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_writer import COPY_THRESHOLD  # noqa: E402


def test_snowflake_copy_threshold_clears_stream_batches():
    """Default must be low enough that normal stream chunks use COPY INTO."""
    assert COPY_THRESHOLD <= 200
    assert COPY_THRESHOLD >= 50
