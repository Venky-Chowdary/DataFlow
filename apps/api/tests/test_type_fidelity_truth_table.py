"""Accuracy review finding 8: type grading checked against an independent oracle.

tests/fixtures/type_fidelity_truth_table.json is a hand-reviewed table of risky
type families (Oracle FLOAT/NUMBER, byte vs character lengths on Redshift,
Oracle and MySQL utf8mb3, money, unsigned integers, time zones and offsets,
MongoDB millisecond dates, booleans, binary, JSON), each row with vendor
documentation citations. It is not derived from the product's predicate.

* ``loses``: the classifier must not grade the pair a safe path, and it must
  need a Risk Contract (the fidelity predicate calls it lossy or the contract
  requires approval);
* ``loses_if_values`` / ``loses_disclosed``: a safe grade must carry the
  ``unmeasured_loss`` sentence (a dropped UTC offset is loaded as the instant
  by product decision D136 and disclosed);
* ``keeps``: the product's carrier must not be called lossy (no refused
  second run of the product's own table).

Offset-bearing rows also name what the writer does with an aware value
(``writer``), pinned below on the writer's own coercion: a route that keeps the
clock digits and drops the offset moves the instant, so its row must be
``loses`` (Risk Contract), never a disclosure (lead decision on finding 8).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datetime import datetime, timedelta, timezone

from connectors.sql_temporal import coerce_sql_temporal
from services.conversion_contract import ConversionClass, classify_conversion
from services.source_engine_scope import bind_source_engine
from services.type_coercion import is_lossy_coercion
from services.type_system import create_new_mapping_target_type

TABLE = json.loads((Path(__file__).parent / "fixtures" / "type_fidelity_truth_table.json").read_text())
SAFE = {
    ConversionClass.IDENTITY.value, ConversionClass.EQUIVALENT.value, ConversionClass.LOSSLESS.value,
    ConversionClass.WIDENING.value, ConversionClass.REPRESENTATION.value, ConversionClass.NORMALIZATION.value,
}
ROWS = TABLE["rows"]
# Test ids avoid the word "mongodb": a conftest hook skips such ids without a live MongoDB.
IDS = [f"{r['engine']}:{r['source_type']}->{r['dest'].replace('mongodb', 'bson')}:{r['target']}" for r in ROWS]


def test_every_row_is_cited() -> None:
    assert len(ROWS) >= 40
    for r in ROWS:
        assert r["truth"] in TABLE["truth_values"]
        assert r["citations"] and all(c.startswith("https://") for c in r["citations"]), r


def _grade(r):
    with bind_source_engine(r["engine"]):
        target = (create_new_mapping_target_type(r["source_type"], r["dest"], source_db=r["engine"])
                  if r["target"] == "create_new" else r["target"])
        conv = classify_conversion(r["source_type"], target, dest_db=r["dest"])
        lossy = bool(is_lossy_coercion(r["source_type"], target, dest_db=r["dest"], source_db=r["engine"],
                                       dest_table_exists=True))
    return target, conv, lossy


def _agrees(truth, target, conv, lossy) -> None:
    cls = conv["conversion_class"]
    if truth == "loses":
        assert cls not in SAFE and (lossy or conv.get("requires_risk_contract")), (target, conv)
    elif truth in ("loses_if_values", "loses_disclosed"):
        assert cls not in SAFE or conv.get("unmeasured_loss"), (target, conv)
    else:
        assert not lossy, (target, conv)


@pytest.mark.parametrize("r", ROWS, ids=IDS)
def test_grade_agrees_with_the_truth_table(r) -> None:
    # No Oracle destination session was checked: the unpinned truth applies.
    _agrees(r["truth"], *_grade(r))


PINNED = [r for r in ROWS if r.get("truth_on_utc_session")]
PINNED_IDS = [i for i, r in zip(IDS, ROWS) if r.get("truth_on_utc_session")]


@pytest.mark.parametrize("r", PINNED, ids=PINNED_IDS)
def test_grade_on_a_session_that_read_back_utc(r) -> None:
    from services.oracle_session_scope import bind_oracle_utc_pin

    with bind_oracle_utc_pin(True, "session time zone read back as +00:00"):
        target, conv, lossy = _grade(r)
    _agrees(r["truth_on_utc_session"], target, conv, lossy)
    if r["truth_on_utc_session"] == "loses_disclosed":
        assert conv.get("unmeasured_loss") and not lossy, conv


# One aware wire value: 03:04:05.123 at +05:30 is 21:34:05.123 UTC the day before.
_WIRE = "2024-01-02 03:04:05.123+05:30"
_CLOCK = datetime(2024, 1, 2, 3, 4, 5, 123000)
_INSTANT = datetime(2024, 1, 1, 21, 34, 5, 123000, tzinfo=timezone.utc)
WRITER_ROWS = [r for r in ROWS if r.get("writer")]
WRITER_IDS = [i for i, r in zip(IDS, ROWS) if r.get("writer")]


def test_every_destination_of_an_offset_source_is_covered() -> None:
    dests = {r["dest"] for r in WRITER_ROWS}
    assert {"sqlserver", "postgresql", "mysql", "mariadb", "snowflake", "bigquery", "oracle", "mongodb"} <= dests
    assert set(TABLE["writer_values"]) >= {r["writer"] for r in WRITER_ROWS}


@pytest.mark.parametrize("r", WRITER_ROWS, ids=WRITER_IDS)
def test_writer_route_matches_the_row(r) -> None:
    if r["writer"] in {"wall_clock", "driver"}:
        assert r["truth"] == "loses", "a route that moves the instant needs a Risk Contract"
    if r["writer"] == "driver":
        # The Oracle writer binds UTC clock digits (python-oracledb drops tzinfo),
        # so the instant holds exactly when the session reads them as UTC.
        from sqlalchemy.dialects import oracle

        from connectors.generic_sql import _to_sa_value

        local = "LOCAL" in r["target"].upper() or r["target"] == "create_new" and r["engine"] == "postgresql"
        sa_type = oracle.TIMESTAMP(local_timezone=True) if local else oracle.TIMESTAMP(timezone=True)
        got = _to_sa_value(_WIRE, "TIMESTAMP(6) WITH TIME ZONE", sa_type=sa_type, db_type="oracle")
        assert got.tzinfo is None and got == _INSTANT.replace(tzinfo=None)
        return
    if r["dest"] == "mongodb":
        # The BSON writer is pinned by D136 (tests/test_d136_d148_d156_bson_date_instant_model.py).
        return
    with bind_source_engine(r["engine"]):
        target = (create_new_mapping_target_type(r["source_type"], r["dest"], source_db=r["engine"])
                  if r["target"] == "create_new" else r["target"])
    got = coerce_sql_temporal(_WIRE, target, engine=r["dest"])
    if r["writer"] == "offset":
        assert got.utcoffset() == timedelta(hours=5, minutes=30) and got == _INSTANT
    elif r["writer"] == "utc":
        assert (got.astimezone(timezone.utc) if got.tzinfo else got.replace(tzinfo=timezone.utc)) == _INSTANT
    else:
        assert r["writer"] == "wall_clock" and got == _CLOCK


FILE_ROWS = [r for r in ROWS if r.get("route") == "file_text"]


@pytest.mark.parametrize("r", FILE_ROWS, ids=[f"{r['dest']}:{r['target']}" for r in FILE_ROWS])
def test_offset_text_into_a_zoneless_column_is_disclosed_by_validate(r) -> None:
    from services.coercion_probe import analyze_coercion

    def probe(values, table_exists=True):
        report = analyze_coercion(
            sample_rows=[{"at": v} for v in values], mappings=[{"source": "at", "target": "at"}],
            source_types={"at": r["source_type"]}, dest_types={"at": r["target"]} if table_exists else {},
            dest_db_type=r["dest"], table_exists=table_exists,
        )
        return (report["by_source"].get("at") or {}).get("unmeasured_loss", "")

    offsets = ["2024-01-02T03:04:05.123+05:30", "2024-07-01T23:30:00-09:30"]
    assert "not the offset" in probe(offsets)
    assert "not the offset" in probe(offsets, table_exists=False)
    assert probe(["2024-01-02T03:04:05Z", "2024-07-01 23:30:00+00:00"]) == ""
