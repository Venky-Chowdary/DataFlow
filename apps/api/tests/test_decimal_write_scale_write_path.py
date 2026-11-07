"""Write-fit scale is not create-new money scale.

``fits_decimal`` reused ``cell_int_digits_and_scale``, which keeps
``2000.00`` as scale 2 so Map invents DECIMAL(*,2). Snowflake NUMBER(38,0)
stores that cell as 2000 — the write scan invented overflow. Significant
cents still fail scale 0. Observe polarity is unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import (  # noqa: E402
    decimal_int_digits_and_scale,
    fits_decimal,
)
from services.decimal_observe import (  # noqa: E402
    cell_int_digits_and_scale,
    write_int_digits_and_scale,
)


def test_write_scale_strips_trailing_zeros_observe_keeps_money():
    assert write_int_digits_and_scale("2000.00") == (4, 0)
    assert decimal_int_digits_and_scale("2000.00") == (4, 0)
    assert cell_int_digits_and_scale("2000.00") == (4, 2)
    assert write_int_digits_and_scale("2000.10") == (4, 1)
    assert cell_int_digits_and_scale("2000.10") == (4, 2)
    assert write_int_digits_and_scale("2000.1") == (4, 1)
    assert cell_int_digits_and_scale("2000.1") == (4, 1)


def test_fits_zero_scale_number_accepts_integral_padding():
    assert fits_decimal("2000.00", 38, 0, dest_db="snowflake") is True
    assert fits_decimal("1000", 38, 0, dest_db="snowflake") is True
    assert fits_decimal("1e3", 38, 0, dest_db="snowflake") is True
    assert fits_decimal("2000.10", 38, 0, dest_db="snowflake") is False
    assert fits_decimal("$1,234.56", 38, 0, dest_db="snowflake") is False
    assert fits_decimal("€1.234,56", 38, 0, dest_db="snowflake") is False
