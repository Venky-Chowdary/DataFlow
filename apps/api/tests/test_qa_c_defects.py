"""QA-C defects: CDC resume cursor, parallel abort, overwrite keep, types, endpoints.

Measured on these fixtures. Not a claim about a live 400k job.
"""

from __future__ import annotations

import threading
from decimal import Decimal


def test_resume_token_does_not_adopt_a_live_gtid() -> None:
    from connectors.mysql_change_stream import resume_token_from_consumed

    previous = {"file": "mysql-bin.000004", "pos": 1200, "gtid": "uuid:1-10"}
    # An empty poll has no consumed position. The live master must not be merged in.
    assert resume_token_from_consumed(previous, None) == previous
    consumed = {"file": "mysql-bin.000004", "pos": 1500, "tables": ["orders"]}
    token = resume_token_from_consumed(previous, consumed)
    assert token is not None
    assert token["file"] == "mysql-bin.000004"
    assert token["pos"] == 1500
    assert "gtid" not in token


def test_binlog_schema_match_is_case_insensitive() -> None:
    from connectors.mysql_change_stream import binlog_schema_matches

    assert binlog_schema_matches("QA", "qa") is True
    assert binlog_schema_matches("other", "qa") is False
    assert binlog_schema_matches("anything", "") is True


def test_abort_drain_applies_only_the_committed_prefix() -> None:
    from services.parallel_chunks import ChunkDispatcher

    started = threading.Event()
    release = threading.Event()

    def process(idx: int, item: int) -> int:
        if idx == 0:
            started.set()
            release.wait(timeout=2)
        return idx

    with ChunkDispatcher(max_workers=1, max_inflight=4) as dispatcher:
        dispatcher.submit(0, 0, process)
        assert started.wait(timeout=2)
        dispatcher.submit(1, 1, process)
        dispatcher.submit(2, 2, process)
        dispatcher.abort()
        release.set()
        committed = dispatcher.drain_committed_prefix()

    assert [idx for idx, _value in committed] == [0]


def test_dynamodb_endpoint_collapses_doubled_scheme_and_default_port() -> None:
    from connectors.aws_common import resolve_endpoint_url

    url = resolve_endpoint_url(
        {"host": "http://bore.pub:20988", "port": 443}
    )
    assert url == "http://bore.pub:20988"
    assert resolve_endpoint_url(
        {"host": "http://http://bore.pub:20988:443", "port": 443}
    ) == "http://bore.pub:20988"
    assert resolve_endpoint_url({"host": "minio", "port": 9000}) == "http://minio:9000"
    assert resolve_endpoint_url({"host": "us-east-1", "port": 443}) == ""
    assert resolve_endpoint_url({"host": "localhost", "port": 8000}) == "http://localhost:8000"


def test_form_default_443_does_not_hide_a_saved_tunnel() -> None:
    from src.transfer.connector_capabilities import prefer_listen_port

    assert prefer_listen_port("dynamodb", 443, 20988) == 20988
    assert prefer_listen_port("redis", 6379, 6380) == 6380
    assert prefer_listen_port("dynamodb", 8000, 20988) == 8000


def test_redis_dial_uses_the_url_host_and_port() -> None:
    from connectors.redis_reader import redis_dial_endpoint

    assert redis_dial_endpoint("http://bore.pub:20988", 443) == ("bore.pub", 20988)
    assert redis_dial_endpoint("cache.internal", 6380) == ("cache.internal", 6380)


def test_numeric_38_10_stats_do_not_raise_invalid_operation() -> None:
    from services.data_profiler import _numeric_stats

    wide = "1234567890123456789012345678.1234567890"
    stats = _numeric_stats([wide, wide])
    assert stats["min"] == Decimal(wide)
    assert stats["max"] == Decimal(wide)


def test_round_of_numeric_38_does_not_raise_invalid_operation() -> None:
    from services.shape_expr import _round_half_up

    value = Decimal("1234567890123456789012345678.1234567890")
    rounded = _round_half_up(value, 10)
    assert rounded == Decimal("1234567890123456789012345678.1234567890")


def test_float_nan_is_not_serialized_as_sql_null() -> None:
    from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string

    assert cell_to_string(float("nan"), preserve_sql_null=True) == "NaN"
    assert cell_to_string(float("nan"), preserve_sql_null=True) != SQL_NULL_SENTINEL
    assert cell_to_string(float("inf"), preserve_sql_null=True) == "Infinity"
    assert cell_to_string(Decimal("NaN"), preserve_sql_null=True) == "NaN"


def test_sql_bind_quarantines_nan_instead_of_binding_it() -> None:
    from connectors.writer_common import bind_rows_keeping_numbers

    rejected: list[dict] = []
    bound, kept = bind_rows_keeping_numbers(
        [("NaN",)],
        ["amount"],
        ["DOUBLE"],
        rejected,
        "quarantine",
        engine="mysql",
    )
    assert bound == []
    assert kept == []
    assert rejected
    assert "non-finite" in rejected[0]["reason"]


def test_three_decimal_wire_stays_decimal_auto_does_not() -> None:
    from services.data_profiler import profile_column
    from services.transform_engine import (
        NUMBER_LOCALE_WIRE,
        reset_active_number_locale,
        set_active_number_locale,
    )

    auto = profile_column("amount", ["1.234", "10.129"])
    assert "DECIMAL" not in str(auto["inferred_type"]).upper()

    token = set_active_number_locale(NUMBER_LOCALE_WIRE)
    try:
        wired = profile_column("amount", ["1.234", "10.129"])
    finally:
        reset_active_number_locale(token)
    assert str(wired["inferred_type"]).upper().startswith("DECIMAL")


def test_overwrite_keeps_a_destination_column_the_source_dropped() -> None:
    from services.overwrite_keep import append_kept_column_sql, columns_to_keep

    kept = columns_to_keep(
        [
            {"name": "id", "ddl_type": "bigint"},
            {"name": "legacy_note", "ddl_type": "varchar(64)"},
        ],
        [{"source": "id", "target": "id"}],
    )
    assert kept == [{"name": "legacy_note", "ddl_type": "varchar(64)"}]
    body = append_kept_column_sql(
        "`id` bigint, PRIMARY KEY (`id`)",
        kept,
        dialect="mysql",
        existing=["id"],
    )
    assert body.index("legacy_note") < body.upper().index("PRIMARY KEY")
    assert "NULL" in body
