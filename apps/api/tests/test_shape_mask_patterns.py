"""mask_pii pattern option — shape-keeping masks (matrix row 20).

last4 / first4 / email are refused when unknown so a typo can never
silently degrade to reveal-all or stars-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from services.shape_engine import ShapeRowError, shape_records
from services.shape_models import ShapeRecipe


def _run(pattern: str, value: str):
    plan = ShapeRecipe.parse(
        {
            "steps": [
                {"op": "mask_pii", "column": "v", "options": {"pattern": pattern}}
            ]
        },
        source_columns=["v"],
    )
    return shape_records(plan, [{"v": value}])


def test_last4_keeps_the_tail():
    shaped, _ = _run("last4", "4111111111111111")
    assert shaped[0]["v"] == "************1111"


def test_first4_keeps_the_head():
    shaped, _ = _run("first4", "4111111111111111")
    assert shaped[0]["v"] == "4111************"


def test_email_keeps_domain_and_first_local_char():
    shaped, _ = _run("email", "venkat@example.com")
    assert shaped[0]["v"] == "v***@example.com"


def test_unknown_pattern_refuses():
    with pytest.raises(ShapeRowError, match="not one of"):
        _run("bogus", "x@y.z")


def test_email_pattern_refuses_a_non_address():
    with pytest.raises(ShapeRowError, match="domain"):
        _run("email", "not-an-email")
