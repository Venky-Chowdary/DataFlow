"""D372 and D373: plan and execute use the same fidelity and reversibility rules.

datetime2(7) into a 6-digit carrier is a risk when the 7th digit is present
or unmeasured, and not a value loss when every measured digit fits.
A schedule that requires reversibility is refused at create when a transform
cannot be undone.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.decision_kernel.type_invent import (  # noqa: E402
    temporal_precision_would_narrow,
)
from services.reversibility_schedule import reversibility_refusal  # noqa: E402


def test_datetime2_7_into_6_digits_is_flagged_until_values_fit() -> None:
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(7)", "DATETIME(6)", dest_db="mysql"
    )
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(7)", "TIMESTAMP(6)", dest_db="postgresql",
        population=["2026-01-01 00:00:00.1234567"],
    )
    # The 7th digit is 0: the profiler says this value is not lossy.
    assert not temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(7)", "TIMESTAMP(6)", dest_db="postgresql",
        population=["2026-01-01 00:00:00.1234560"],
    )


def test_schedule_reversibility_names_omit_and_mask() -> None:
    message = reversibility_refusal(
        [
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "note", "target": "note", "transform": "mask_pii"},
            {"source": "long_txt", "target": "long_txt", "transform": "omit"},
        ],
        table="q3_rev_out",
    )
    assert "must be reversible" in message
    assert "q3_rev_out.note" in message
    assert "q3_rev_out.long_txt" in message
    assert reversibility_refusal(
        [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"}],
        table="q3_rev_out",
    ) == ""
