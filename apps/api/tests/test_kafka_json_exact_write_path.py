"""Kafka / Debezium JSON payloads use json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 in after/before cells
and in plain Kafka JSON. IEEE-exact 1.5 stays float. Schema documents
still parse with stdlib (not cell data).
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.confluent_schema_registry import decode_kafka_value  # noqa: E402
from connectors.kafka_debezium_bridge import parse_debezium_envelope  # noqa: E402

LONG = "1.234567890123456789"


def test_decode_kafka_value_keeps_long_fraction():
    raw = f'{{"amt": {LONG}, "n": 1.5}}'
    doc = decode_kafka_value(raw)
    assert doc["amt"] == Decimal(LONG)
    assert doc["amt"] != json.loads(raw)["amt"]
    assert doc["n"] == 1.5
    assert decode_kafka_value(raw.encode("utf-8")) == doc


def test_debezium_envelope_after_keeps_long_fraction():
    payload = (
        '{"op":"c","source":{"table":"orders","lsn":"0/1"},'
        '"after":{"amt": ' + LONG + ', "n": 1.5}}'
    )
    change = parse_debezium_envelope(payload)
    assert change is not None
    assert change.after["amt"] == Decimal(LONG)
    assert change.after["n"] == 1.5
    assert change.table == "orders"
