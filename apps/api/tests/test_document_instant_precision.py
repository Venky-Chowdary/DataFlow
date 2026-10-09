"""DEF-B-004: SQL → MongoDB create-new judged on the digits the rows hold.

BSON date counts milliseconds. A MySQL ``DATETIME(6)``, SQL Server
``DATETIME2(7)`` or Oracle ``TIMESTAMP(6)`` column was a fidelity collapse on
its declaration alone, so every create-new from those engines blocked even when
the table held whole seconds. The opposite hole was worse: a declared zone
rewrote the column to a bare ``TIMESTAMPTZ``, which read as "no precision", and
the write dropped the microseconds without a word.

The rule now has one owner on each side:

* the classifier reads the source's real fractional digits (explicit or the
  engine default) and the measured population — whole milliseconds pass,
  anything finer, or nothing measured, stays a truncation;
* the writer refuses a cell with a non-zero digit below the millisecond unless
  the operator accepted the risk on that column, and reports the accepted
  truncation instead of hiding it.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timezone

import pytest

from services.document_instant import (
    cell_exceeds_document_instant,
    population_fits_document_instant,
    source_fractional_digits,
)
from services.source_engine_scope import bind_source_engine
from services.timezone_policy import effective_source_type
from services.type_system import is_lossy_coercion, is_precision_collapse_coercion

WHOLE_MS = ["2024-01-01 10:00:00.123000", "2024-01-01 10:00:01", None]
MICROS = ["2024-01-01 10:00:00.123000", "2024-01-01 10:00:00.123456"]

# (source engine, introspected type) for the four engines QA still saw blocked
# plus the PostgreSQL instant that passed while truncating.
ROUTES = [
    ("mysql", "DATETIME(6)"),
    ("mariadb", "DATETIME(6)"),
    ("sqlserver", "DATETIME2(7)"),
    ("sqlserver", "DATETIME2"),
    ("oracle", "TIMESTAMP(6)"),
    ("postgresql", "timestamp"),
    ("postgresql", "timestamptz"),
]


def _declared(engine: str, source_type: str) -> str:
    """The type a declared zone makes true (no-op for an instant source)."""
    with bind_source_engine(engine):
        return effective_source_type(source_type, "assume_timezone:UTC")


@pytest.mark.parametrize("engine,source_type", ROUTES)
def test_whole_millisecond_population_is_not_a_collapse(engine, source_type):
    eff = _declared(engine, source_type)
    with bind_source_engine(engine):
        assert is_lossy_coercion(eff, "date", dest_db="mongodb", population=WHOLE_MS) is False
        assert (
            is_precision_collapse_coercion(
                eff, "date", dest_db="mongodb", population=WHOLE_MS
            )
            is False
        )


@pytest.mark.parametrize("engine,source_type", ROUTES)
def test_measured_microseconds_stay_a_truncation(engine, source_type):
    eff = _declared(engine, source_type)
    with bind_source_engine(engine):
        assert is_lossy_coercion(eff, "date", dest_db="mongodb", population=MICROS) is True


@pytest.mark.parametrize("engine,source_type", ROUTES)
def test_unmeasured_sub_millisecond_column_stays_a_truncation(engine, source_type):
    """A sample nobody took is not proof the column holds whole milliseconds."""
    eff = _declared(engine, source_type)
    with bind_source_engine(engine):
        assert is_lossy_coercion(eff, "date", dest_db="mongodb") is True
        assert is_lossy_coercion(eff, "date", dest_db="mongodb", population=[None, ""]) is True


def test_declared_zone_keeps_the_columns_precision():
    assert _declared("mysql", "DATETIME(6)") == "TIMESTAMPTZ(6)"
    assert _declared("sqlserver", "DATETIME2") == "TIMESTAMPTZ(7)"
    assert _declared("mysql", "DATETIME") == "TIMESTAMPTZ(0)"
    assert _declared("postgresql", "timestamptz") == "timestamptz"


def test_whole_second_and_millisecond_sources_need_no_measurement():
    for engine, source_type in [
        ("mysql", "DATETIME"),
        ("mysql", "TIMESTAMPTZ"),
        ("sqlserver", "DATETIME"),
        ("postgresql", "timestamptz(3)"),
    ]:
        eff = _declared(engine, source_type)
        with bind_source_engine(engine):
            assert is_lossy_coercion(eff, "date", dest_db="mongodb") is False, (engine, source_type)


def test_zoneless_source_still_needs_its_zone():
    """Precision fitting does not answer the zone question."""
    with bind_source_engine("mysql"):
        assert is_lossy_coercion("DATETIME(6)", "date", dest_db="mongodb", population=WHOLE_MS) is True


def test_engine_defaults_resolve_bare_spellings():
    with bind_source_engine("postgresql"):
        assert source_fractional_digits("timestamptz") == 6
    with bind_source_engine("sqlserver"):
        assert source_fractional_digits("DATETIME2") == 7
    with bind_source_engine("mysql"):
        assert source_fractional_digits("DATETIME") == 0
    assert source_fractional_digits("TIMESTAMPTZ(4)") == 4


def test_cell_rule_ignores_trailing_zeros_and_reads_seven_digit_text():
    assert cell_exceeds_document_instant("2024-01-01 10:00:00.120000") is False
    assert cell_exceeds_document_instant(datetime(2024, 1, 1, 10, 0, 0, 123000)) is False
    assert cell_exceeds_document_instant(datetime(2024, 1, 1, 10, 0, 0, 123001)) is True
    assert cell_exceeds_document_instant("2024-01-01 10:00:00.1230001") is True
    assert population_fits_document_instant([None]) is None
    assert population_fits_document_instant(None) is None


def _capturing_client(captured: dict):
    class _Result:
        def __init__(self, n):
            self.inserted_ids = list(range(n))

    class _Coll:
        def insert_many(self, docs, ordered=False):
            captured.setdefault("docs", []).extend(docs)
            return _Result(len(docs))

    class _DB:
        def list_collection_names(self, **_kwargs):
            return []

        def __getitem__(self, _name):
            return _Coll()

    class _Client:
        def __getitem__(self, _name):
            return _DB()

    return _Client()


def _write(monkeypatch, rows, *, acknowledged: bool, error_policy: str = "quarantine"):
    from connectors.mongodb_writer import write_mapped_rows

    captured: dict = {}
    monkeypatch.setattr(
        "connectors.mongodb_common._mongo_client",
        lambda *_args, **_kwargs: _capturing_client(captured),
    )
    mapping = {"source": "ts", "target": "ts", "source_type": "TIMESTAMPTZ(6)"}
    if acknowledged:
        mapping["risk_acknowledged"] = True
    result = write_mapped_rows(
        host="localhost",
        port=27017,
        database="dataflow_test",
        username="",
        password="",
        connection_string="",
        ssl=False,
        schema="dataflow_test",
        table_name="instant_precision",
        headers=["ts"],
        data_rows=[[r] for r in rows],
        mappings=[mapping],
        column_types={"ts": "TIMESTAMPTZ(6)"},
        error_policy=error_policy,
    )
    return result, captured


def test_writer_quarantines_a_microsecond_cell_and_writes_the_rest(monkeypatch):
    result, captured = _write(
        monkeypatch,
        ["2024-01-01T10:00:00.123+00:00", "2024-01-01T10:00:00.123456+00:00"],
        acknowledged=False,
    )
    assert result.rows_written == 1
    assert len(captured["docs"]) == 1
    assert captured["docs"][0]["ts"] == datetime(2024, 1, 1, 10, 0, 0, 123000, tzinfo=timezone.utc)
    assert len(result.rejected_details) == 1
    assert "finer digits" in result.rejected_details[0]["reason"]


def test_writer_truncates_under_an_accepted_risk_and_says_so(monkeypatch):
    result, captured = _write(
        monkeypatch, ["2024-01-01T10:00:00.123456+00:00"], acknowledged=True
    )
    assert result.ok is True, result.error
    assert result.rows_written == 1
    assert any("sub-millisecond digits truncated" in w for w in result.warnings)


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _reachable("localhost", 27017), reason="MongoDB not reachable")
def test_live_mongo_round_trip_keeps_milliseconds_and_refuses_microseconds():
    import pymongo

    from connectors.mongodb_writer import write_mapped_rows

    collection = "instant_precision_" + uuid.uuid4().hex[:8]
    result = write_mapped_rows(
        host="localhost",
        port=27017,
        database="dataflow_test",
        username="",
        password="",
        connection_string="",
        ssl=False,
        schema="dataflow_test",
        table_name=collection,
        headers=["id", "ts"],
        data_rows=[
            ["1", "2024-01-01T10:00:00.123+00:00"],
            ["2", "2024-01-01T10:00:00.123456+00:00"],
        ],
        mappings=[
            {"source": "id", "target": "id", "source_type": "INTEGER"},
            {"source": "ts", "target": "ts", "source_type": "TIMESTAMPTZ(6)"},
        ],
        column_types={"id": "INTEGER", "ts": "TIMESTAMPTZ(6)"},
        error_policy="quarantine",
    )
    client = pymongo.MongoClient("localhost", 27017, serverSelectionTimeoutMS=2000)
    try:
        docs = list(client["dataflow_test"][collection].find({}, {"_id": 0}))
        assert result.rows_written == 1, result.error
        assert len(result.rejected_details) == 1
        assert len(docs) == 1
        assert docs[0]["ts"].microsecond == 123000
    finally:
        client["dataflow_test"].drop_collection(collection)
        client.close()
