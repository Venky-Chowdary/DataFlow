"""D235: TIMESTAMP_NTZ -> Kafka ``datetime`` is a wall-clock preserve.

Kafka values are JSON, so a ``datetime`` cell is written as ISO-8601 text that
keeps every fractional digit. Grading the bare ``datetime`` carrier as MySQL
FSP 0 blocked the plan with rc-fidelity-collapse although the topic held
``02:30:00.012345`` exactly (prod job 6ae903fb4fb8a205cd02ed5d).
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from services.type_coercion import is_lossy_coercion, is_precision_collapse_coercion
from services.type_system import destination_temporal_fractional_digits
from services.value_serializer import json_default


@pytest.mark.parametrize("src", ["TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ", "TIMESTAMP(6)"])
def test_wall_clock_into_kafka_datetime_is_not_a_collapse(src: str) -> None:
    assert not is_precision_collapse_coercion(src, "datetime", dest_db="kafka")
    assert not is_lossy_coercion(src, "DATETIME", dest_db="kafka")


def test_kafka_datetime_carrier_keeps_microseconds() -> None:
    assert destination_temporal_fractional_digits("datetime", dest_db="kafka") >= 6


def test_kafka_json_wire_renders_every_digit() -> None:
    cell = datetime(2024, 3, 10, 2, 30, 0, 12345)
    assert json.dumps({"ts": cell}, default=json_default) == (
        '{"ts": "2024-03-10T02:30:00.012345"}'
    )


def test_mysql_bare_datetime_still_narrows() -> None:
    assert is_precision_collapse_coercion("TIMESTAMP_NTZ(6)", "DATETIME", dest_db="mysql")
