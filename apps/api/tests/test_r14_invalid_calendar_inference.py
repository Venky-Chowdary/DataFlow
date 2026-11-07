"""QA browser 3 (review follow-up): where an impossible day votes for a date column.

``classify_value`` is shared by database narrowing (catalog TEXT columns,
Elasticsearch keyword samples, generic SQL sample typing): it keeps
``2024-02-30`` text, exactly as before round 14. Only the record-batch
inference (a file's rows, ``infer_columns_from_rows``) counts an impossible
day as a vote for its temporal family, and only when every other non-blank
sample is a real date/time, at least one is, and the impossible ones stay
below ``connectors.sql_temporal.INVALID_CALENDAR_MAX_SHARE``. A zoned stamp
stays zoned.
"""

from __future__ import annotations

import pytest

from connectors.sql_temporal import INVALID_CALENDAR_MAX_SHARE
from services.schema_inference import (
    _invalid_calendar_family,
    classify_value,
    infer_column,
    infer_columns_from_rows,
)

REAL = [f"2024-01-{d:02d}" for d in range(1, 20)]


@pytest.mark.parametrize("token", ["2024-02-30", "1234-56-78", "0000-00-00", "2024-02-10 25:61:00",
                                   "2024-02-30 10:00:00+05:00"])
def test_shared_classifier_keeps_impossible_days_text(token):
    assert classify_value(token) == "VARCHAR"
    assert classify_value(token, field_name="d") == "VARCHAR"


def test_database_narrowing_is_unchanged():
    # The default (database, catalog, keyword) path never re-types the column.
    assert infer_column([*REAL, "2024-02-30"], field_name="d")["logical_type"] == "VARCHAR"


def test_a_file_column_with_one_impossible_day_is_a_date_column():
    assert infer_column([*REAL, "2024-02-30"], field_name="d", invalid_calendar_votes=True)["logical_type"] == "DATE"
    rows = [[v] for v in [*REAL, "2024-02-30"]]
    assert infer_columns_from_rows(["d"], rows, invalid_calendar_votes=True)[0]["inferred_type"] == "DATE"


def test_a_database_batch_keeps_its_text_column_text():
    # Neo4j / Couchbase / InfluxDB sample documents through the same row
    # inference: 19 real dates and one '2024-02-30' stay VARCHAR by default.
    rows = [[v] for v in [*REAL, "2024-02-30"]]
    assert infer_columns_from_rows(["d"], rows)[0]["inferred_type"] == "VARCHAR"
    from services import schema_introspect, schema_introspect_stores
    from pathlib import Path

    for mod in (schema_introspect, schema_introspect_stores):
        assert "invalid_calendar_votes=True" not in Path(mod.__file__).read_text()


@pytest.mark.parametrize("samples", [
    ["1234-56-78", "2210-44-19"],                    # no real date at all
    ["2024-01-01", "2024-02-30", "2024-04-31"],      # impossible days are not a minority
    [*REAL, "1234-56-78"],                           # month 56 is never a vote
    [*REAL[:3], "2024-02-30"],                       # 1 of 4 = 25% >= the share
    [*REAL, "2024-02-30", "next tuesday"],           # a word keeps the column text
])
def test_no_vote_without_a_dominant_real_date_majority(samples):
    assert infer_column(samples, field_name="d", invalid_calendar_votes=True)["logical_type"] != "DATE"


def test_the_share_is_the_documented_constant():
    assert INVALID_CALENDAR_MAX_SHARE == 0.2


@pytest.mark.parametrize("token", ["1234-56-78", "2024-00-10", "2024-13-01", "2024-02-00",
                                   "2024-01-32", "0000-00-00", "2024-13-01 10:00:00"])
def test_only_a_day_or_clock_overflow_inside_a_real_month_votes(token):
    # Month 00 / over 12, day 00 / over 31, year 0000: another layout or no
    # date at all — never re-voted DATE.
    assert _invalid_calendar_family(token) == ""


def test_a_yyyy_dd_mm_file_is_not_typed_date():
    # Day/month swapped rows: 13..31 land in the month slot. They must not be
    # out-voted into a DATE column (they would load as other days).
    swapped = [f"2024-{d:02d}-01" for d in range(1, 13)] * 2 + ["2024-25-01", "2024-28-01"]
    assert infer_column(swapped, field_name="d", invalid_calendar_votes=True)["logical_type"] != "DATE"


@pytest.mark.parametrize("token,family", [
    ("2024-02-30", "DATE"), ("2025-04-31", "DATE"), ("2024-02-30 10:00:00", "TIMESTAMP"),
    ("2024-02-30 10:00:00+05:00", "TIMESTAMPTZ"), ("2024-02-30T10:00:00Z", "TIMESTAMPTZ"),
    ("2024-02-30 10:00:00 UTC", "TIMESTAMPTZ"), ("2024-02-10 25:61:00-03", "TIMESTAMPTZ"),
])
def test_family_keeps_the_zone(token, family):
    assert _invalid_calendar_family(token) == family


def test_a_zoned_file_column_with_one_impossible_stamp_stays_zoned():
    zoned = [f"2024-01-{d:02d} 10:00:00+05:00" for d in range(1, 20)]
    out = infer_column([*zoned, "2024-02-30 10:00:00+05:00"], field_name="ts", invalid_calendar_votes=True)
    assert out["logical_type"] == "TIMESTAMPTZ"


@pytest.mark.parametrize("token", ["", "abc", "2024-02-28", "2024-02-29", "31/02/2025", "20240230", "42",
                                   "10:30:00", "25:61", "2024-13", "x" * 300])
def test_values_the_rule_does_not_flag(token):
    assert _invalid_calendar_family(token) == ""


def test_every_file_path_types_an_impossible_day_column_the_same_way():
    """Review: FileParser.infer_schema / infer_schema_map take the same opt-in,
    passed by the file adapters only (adapters file/GCS/ADLS/S3 reads,
    file_stream peeks, Kafka sampling, the upload profile route)."""
    import re as _re
    from pathlib import Path

    from services.file_parser import FileParser
    from services.schema_inference import infer_schema_map

    records = [{"d": v} for v in [*REAL, "2024-02-30"]]
    assert FileParser.infer_schema(records, invalid_calendar_votes=True)["d"] == "DATE"
    assert FileParser.infer_schema(records)["d"] == "VARCHAR"
    assert infer_schema_map({"d": [*REAL, "2024-02-30"]}, invalid_calendar_votes=True)[0]["d"] == "DATE"

    api = Path(__file__).resolve().parents[1]
    adapters = (api / "src/transfer/adapters.py").read_text()
    # Database readers (DynamoDB, Elasticsearch) keep their text columns text.
    for engine in ("dynamodb", "elasticsearch"):
        block = adapters[adapters.index(f'if db_type == "{engine}":'):]
        block = block[: block.index("return _pack_source_read")]
        assert "invalid_calendar_votes" not in block, engine
    for engine in ("gcs", "adls", "s3"):
        block = adapters[adapters.index(f'if db_type == "{engine}":'):]
        block = block[: block.index("return _pack_source_read")]
        assert "invalid_calendar_votes=True" in block, engine
    file_stream = (api / "src/transfer/file_stream.py").read_text()
    calls = _re.findall(r"FileParser\.infer_schema\(([^()]*)\)", file_stream)
    assert calls and all("invalid_calendar_votes=True" in c for c in calls), calls
    router = (api / "src/routers/connectors_router.py").read_text()
    assert "FileParser.infer_schema(result.data, invalid_calendar_votes=True)" in router
