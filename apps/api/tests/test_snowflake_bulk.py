"""Snowflake writer bulk load threshold."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.snowflake_writer import COPY_THRESHOLD  # noqa: E402


def test_snowflake_copy_threshold_production_scale():
    assert COPY_THRESHOLD >= 2000
