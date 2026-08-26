"""Singer / Airbyte SDK protocol lines use json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 in RECORD cells before
the writer saw them. IEEE-exact 1.5 stays float. Invalid lines stay None.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sdk import load_sdk_protocol_message  # noqa: E402

LONG = "1.234567890123456789"


def test_sdk_record_keeps_long_fraction():
    line = (
        '{"type":"RECORD","stream":"orders","record":'
        f'{{"amt": {LONG}, "n": 1.5, "id": 1}}}}'
    )
    msg = load_sdk_protocol_message(line)
    assert msg is not None
    rec = msg["record"]
    assert rec["amt"] == Decimal(LONG)
    assert rec["amt"] != json.loads(line)["record"]["amt"]
    assert rec["n"] == 1.5
    assert rec["id"] == 1


def test_sdk_invalid_line_is_none():
    assert load_sdk_protocol_message("{not-json}") is None
    assert load_sdk_protocol_message("42") is None
    assert load_sdk_protocol_message("") is None
    schema = load_sdk_protocol_message(
        '{"type":"SCHEMA","stream":"orders","schema":{"properties":{"amt":{"type":"number"}}}}'
    )
    assert schema is not None
    assert schema["type"] == "SCHEMA"
