"""Engine read → confirm → write must not leave Extract active during preflight."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.job_phases import (  # noqa: E402
    PHASE_ORDER,
    advance_phase,
    initial_phases,
    phase_from_engine_label,
)


def test_phase_order_is_extract_then_preflight_then_load():
    assert PHASE_ORDER.index("extract") < PHASE_ORDER.index("preflight")
    assert PHASE_ORDER.index("preflight") < PHASE_ORDER.index("load")


def test_reading_then_preflight_finalizes_extract():
    phases = initial_phases()
    phases = advance_phase(
        phases, phase_from_engine_label("reading"), status="active", message="read"
    )
    phases = advance_phase(
        phases,
        phase_from_engine_label("preflight"),
        status="active",
        message="confirm",
    )
    by_name = {p["name"]: p for p in phases}
    assert by_name["extract"]["status"] == "done"
    assert by_name["preflight"]["status"] == "active"
    assert by_name["load"]["status"] == "pending"
    assert by_name["extract"].get("elapsed_ms") is not None
