"""Source-row spool reread uses json_loads_exact, not stdlib float().

A JSON number with extra fraction digits on the spill file used to collapse
to IEEE on iter_rows. Decimal cells still dump as exact text via json_default.
DF_MISSING still survives. IEEE-exact 1.5 stays float.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.source_row_spool import SourceRowSpool  # noqa: E402
from services.value_serializer import DF_MISSING_SENTINEL  # noqa: E402

LONG = "1.234567890123456789"
IEEE_COLLAPSED = json.loads(f"[{LONG}]")[0]


def test_spool_reread_keeps_json_number_fraction():
    spool = SourceRowSpool(spill_max_size=10_000)
    try:
        spool._spool.write(f'[{LONG}, 1.5, 1, "ok"]\n'.encode("utf-8"))
        spool.row_count = 1
        row = next(spool.iter_rows())
        assert row[0] == Decimal(LONG)
        assert row[0] != IEEE_COLLAPSED
        assert row[1] == 1.5
        assert isinstance(row[1], float)
        assert row[2] == 1
        assert row[3] == "ok"
    finally:
        spool.close()


def test_spool_decimal_cell_dumps_as_exact_text():
    spool = SourceRowSpool(spill_max_size=10_000)
    try:
        spool.ingest_matrix(["amt"], [[Decimal(LONG)]])
        row = next(spool.iter_rows())
        assert row[0] == LONG
        assert row[0] != str(IEEE_COLLAPSED)
    finally:
        spool.close()


def test_spool_missing_and_empty_still_round_trip():
    spool = SourceRowSpool(spill_max_size=10_000)
    try:
        spool.ingest_matrix(
            ["id", "note"],
            [["1", DF_MISSING_SENTINEL], ["2", ""]],
        )
        rows = list(spool.iter_rows())
        assert rows[0][1] == DF_MISSING_SENTINEL
        assert rows[1][1] == ""
    finally:
        spool.close()
