"""A transform refusal is not a capacity problem."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.error_handling import humanize_transfer_failure


def test_transform_step_does_not_mention_capacity():
    human = humanize_transfer_failure(
        ValueError(
            "transform step 2 (round_number) on column 'amount': "
            "expected a number, got an empty value"
        )
    )
    assert human["code"] == "shape_refused_row"
    assert "address capacity" not in human["message"].lower()
    assert "capacity" not in human["message"].lower()
