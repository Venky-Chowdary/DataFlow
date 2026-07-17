"""Platform readiness — all transfer-live drivers must import."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from connectors.writer_common import (
    transform_error_policy_for_validation_mode,  # noqa: E402
)
from transfer.readiness import platform_readiness_report  # noqa: E402


def test_all_transfer_live_drivers_ready():
    report = platform_readiness_report()
    assert report["drivers_failed"] == 0, report["failed_drivers"]
    assert report["ready"] is True


def test_strict_validation_fails_on_bad_cells():
    assert transform_error_policy_for_validation_mode("strict") == "fail"
    assert transform_error_policy_for_validation_mode("maximum") == "fail"
    assert transform_error_policy_for_validation_mode("balanced") == "quarantine"
