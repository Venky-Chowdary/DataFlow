"""D258: Kafka -> new table, the plan's column type and the created column differed.

A topic whose ISO string field mixed DATE with TIMESTAMP (or zoneless with
zoned) values was typed TEXT by the Kafka reader, because its per-message merge
only knew numbers. The mapping then re-typed the declared TEXT from samples
(``honor_declared_strings``), so plan, introspect and CREATE each reported a
different carrier. The reader now joins temporal carriers on the shared
``services.type_lattice`` chain DATE < TIMESTAMP < TIMESTAMPTZ.
"""

from __future__ import annotations

import itertools

import pytest

from connectors.kafka_reader import _kafka_column_types, merge_kafka_logicals
from tests.test_d151_d161_kafka_source_pg_e2e import (  # noqa: F401 - fixtures
    _BROKER,
    _column_types,
    _produce,
    _run,
    table,
    topic,
)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["2024-03-10", "2024-03-10T02:30:00"], "TIMESTAMP"),
        (["2024-03-10T02:30:00", "2024-03-10 02:30:00.123456", "2024-03-11"], "TIMESTAMP"),
        (["2024-03-10T02:30:00Z", "2024-03-10T02:30:00"], "TIMESTAMPTZ"),
        (["2024-03-10", "2024-03-10T02:30:00+05:30"], "TIMESTAMPTZ"),
    ],
)
def test_mixed_iso_temporals_join_on_the_lattice(values, expected):
    for order in itertools.permutations(values):
        native, decisions = _kafka_column_types([{"ts": v} for v in order])
        assert native["ts"] == expected, order
        assert decisions == {}


@pytest.mark.parametrize(
    "values",
    [
        ["2024-03-10T02:30:00", "not a time"],
        ["2024-03-10T02:30:00", "02:30:00"],
        ["2024-03-10", "20240101"],
    ],
)
def test_temporal_beside_non_temporal_text_stays_text(values):
    native, _ = _kafka_column_types([{"ts": v} for v in values])
    assert native["ts"] == "TEXT"


def test_temporal_beside_a_json_number_is_a_stated_text_decision():
    native, decisions = _kafka_column_types([{"ts": "2024-03-10T02:30:00"}, {"ts": 5}])
    assert native["ts"] == "TEXT"
    assert "ts" in decisions


def test_fold_is_page_order_independent():
    pages = ["DATE", "TIMESTAMP", "TIMESTAMPTZ", "DATE"]
    results = set()
    for order in itertools.permutations(pages):
        acc = ""
        for logical in order:
            acc = merge_kafka_logicals(acc, logical)
        results.add(acc)
    assert results == {"TIMESTAMPTZ"}


def test_numeric_merge_is_unchanged():
    assert merge_kafka_logicals("INTEGER", "DOUBLE") == "DOUBLE"
    assert merge_kafka_logicals("BIGINT", "DECIMAL") == "DECIMAL"
    assert merge_kafka_logicals("INTEGER", "TIMESTAMP") == "TEXT"


def test_live_plan_type_is_the_created_column_and_gate8_passes(topic, table):
    from services.schema_introspect import _introspect_kafka

    _produce(
        topic,
        [
            (b"1", {"id": 1, "ts": "2024-03-10"}),
            (b"2", {"id": 2, "ts": "2024-03-10T02:30:00.123456"}),
            (b"3", {"id": 3, "ts": "2024-11-03 01:30:00"}),
        ],
    )
    plan = _introspect_kafka(host=_BROKER["host"], port=_BROKER["port"], table=topic)
    planned = {c["name"]: c["data_type"] for c in plan["columns"]}
    assert planned["ts"] == "TIMESTAMP"

    result = _run(topic, table, "full_refresh_overwrite")
    assert result.success, result.error
    assert _column_types(table)["ts"] == "timestamp without time zone"
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon.get("message")
    assert not (recon.get("sample_compare") or {}).get("mismatches")
