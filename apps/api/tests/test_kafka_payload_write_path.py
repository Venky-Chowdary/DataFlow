"""Kafka JSON produce binds reader-null via to_json_value, not the token.

The produce loop only dropped Missing. After extract emits
SQL_NULL_SENTINEL, that spelling was a JSON string. 0 / false stay.
Missing still omits the key.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.kafka_writer import kafka_json_payload  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
    json_default,
)


def test_kafka_payload_reader_null_is_json_null():
    cols = ["id", "note"]
    types = {"id": "string", "note": "string"}
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__"):
        payload = kafka_json_payload(("1", wire), cols, types)
        assert payload["id"] == "1"
        assert payload["note"] is None
        dumped = json.dumps(payload, default=json_default)
        assert SQL_NULL_SENTINEL not in dumped
        assert '"note": null' in dumped


def test_kafka_payload_omits_missing_keeps_zero_false():
    cols = ["id", "amt", "flag", "gone"]
    types = {"id": "string", "amt": "integer", "flag": "boolean", "gone": "string"}
    payload = kafka_json_payload(("1", 0, False, Missing), cols, types)
    assert payload["id"] == "1"
    assert payload["amt"] == 0
    assert payload["flag"] is False
    assert "gone" not in payload
    assert kafka_json_payload({"id": "1", "gone": DF_MISSING_SENTINEL}, ["id", "gone"], types) == {
        "id": "1"
    }
