"""Kafka produce keys use present_cell_text, not str(value).

SQL_NULL_SENTINEL and DuckDB null used to become message keys. True
became True so dest true missed compaction / ordering identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.kafka_writer import kafka_produce_key  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_kafka_key_reader_null_is_absent():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", "", "   ", Missing, DF_MISSING_SENTINEL):
        assert kafka_produce_key(wire) is None, wire


def test_kafka_key_true_shares_dest_true():
    assert kafka_produce_key(True) == "true"
    assert kafka_produce_key("true") == "true"
    assert kafka_produce_key(True) != "True"
    assert kafka_produce_key(0) == "0"
    assert kafka_produce_key(False) == "false"
