"""Reversibility gate: fifth adversarial review.

Real engine runs (SQLite files, in-process job store) wherever the finding was
confirmed on the engine; the rest at the function the finding names.
"""

from __future__ import annotations

import pytest

from tests.test_reversibility_review3 import _q, _run, fake  # noqa: F401

# ------------------------------------------- C-1 / M-3: the destination engine by its family

#: (value, declared type, canonical engine, alias): the alias must answer as the canonical engine does.
ALIASED_CASTS = [
    ("2024-01-02 10:00:00.500000", "TIMESTAMP", "postgresql", "supabase"),
    ("2024-01-02 10:00:00.5", "TIMESTAMP", "postgresql", "amazon_rds_postgresql"),
    ("3.14159265", "REAL", "postgresql", "supabase"),
    ("1", "DOUBLE PRECISION", "postgresql", "neon"),
    ("0.1", "REAL", "postgresql", "alloydb"),
    ("3.14159265", "REAL", "sqlserver", "azure_sql_database"),
    ("10:00:00.500000", "TIME", "postgresql", "supabase"),
]


@pytest.mark.parametrize("value, ddl, canonical, alias", ALIASED_CASTS)
def test_c1_m3_the_cast_rule_renders_an_aliased_destination_as_its_engine(value, ddl, canonical, alias):
    from services.reversibility_render import cast_changes

    expected = cast_changes(value, None, ddl, canonical, has_converted=False)
    assert expected is not None
    assert cast_changes(value, None, ddl, alias, has_converted=False) is expected, (value, ddl, alias)


def test_c1_m3_the_gate_names_the_destination_engine_by_its_family():
    from types import SimpleNamespace

    from services.reversibility_sources import _engine

    assert _engine({"format": "supabase"}) == "postgresql"
    assert _engine(SimpleNamespace(format="Amazon_RDS_PostgreSQL")) == "postgresql"
    assert _engine({"type": "azure_sql_database"}) == "sqlserver"
    assert _engine({"format": "amazon_rds_oracle"}) == "oracle"
    assert _engine({"format": "amazon_rds_mysql"}) == "mysql"
    assert _engine({"format": "csv"}) == "csv" and _engine({"format": "sqlite"}) == "sqlite"


# ------------------------------------- C-2 / M-3: floating point and declared precision

@pytest.mark.parametrize("value, ddl, engine, changed", [
    # MySQL shows a single-precision FLOAT to six significant digits.
    ("1.234567", "FLOAT", "mysql", True),
    ("16777217", "FLOAT", "mysql", True),
    ("1.23457", "FLOAT", "mysql", False),
    ("1.5", "FLOAT", "mysql", False),
    ("1.5", "FLOAT32", "mysql", False),
    ("0.1", "FLOAT32", "amazon_rds_mysql", False),
    ("1.0000001", "FLOAT32", "mysql", True),
    ("1e-07", "FLOAT", "mysql", True),
    ("1.5", "FLOAT(7,3)", "mysql", True),  # a FLOAT(M,D) shows D decimals: 1.500
    ("1.500", "FLOAT(7,3)", "mysql", False),
    ("0.1", "DOUBLE", "mysql", False),
    # Single precision elsewhere keeps the shortest text that reads back as the same float4.
    ("3.14159265", "REAL", "postgresql", True),
    ("16777217", "REAL", "postgresql", True),
    ("16777217", "DOUBLE PRECISION", "postgresql", False),
    ("0.1", "REAL", "postgresql", False),
    ("3.14159265", "REAL", "sqlserver", True),
    # SQL Server shows an integral float without ".0".
    ("1", "FLOAT", "sqlserver", False),
    ("1", "REAL", "sqlserver", False),
    ("1.0", "FLOAT", "sqlserver", True),
])
def test_m3_a_float_cast_from_text_is_checked_by_the_engines_display(value, ddl, engine, changed):
    from services.reversibility_render import cast_changes

    assert cast_changes(value, None, ddl, engine, has_converted=False) is changed, (value, ddl, engine)


@pytest.mark.parametrize("value, ddl, engine, changed", [
    ("2024-01-02 10:00:00.5", "TIMESTAMP(0)", "postgresql", True),
    ("10:00:00.5", "TIME(0)", "postgresql", True),
    ("2024-01-02 10:00:00", "TIMESTAMP(0)", "postgresql", False),
    ("2024-01-02 10:00:00.5", "TIMESTAMP(3)", "postgresql", False),
    ("2024-01-02 10:00:00.1234", "TIMESTAMP(3)", "postgresql", True),
    ("2024-01-02 10:00:00.5", "TIMESTAMP_NTZ(0)", "postgresql", True),
    ("2024-01-02 10:00:00.500000", "DATETIME", "mysql", True),  # a bare MySQL DATETIME keeps whole seconds
    ("2024-01-02 10:00:00", "DATETIME", "mysql", False),
    ("10:00:00.250000", "TIME(6)", "mysql", False),
])
def test_m3_a_timestamp_cast_honours_the_declared_fractional_precision(value, ddl, engine, changed):
    from services.reversibility_render import cast_changes

    assert cast_changes(value, None, ddl, engine, has_converted=False) is changed, (value, ddl, engine)


def test_m3_the_writer_check_compares_what_the_writer_converted_too():
    """The text renders back as itself, but the writer converted it to another value: that lands, so it changes."""
    from services import reversibility_render
    from services.reversibility_enforce import changes

    rule = {"code": "cast", "target_type": "INTEGER", "dest_db": "sqlite"}
    assert changes(rule, "7", 7) is False
    assert changes(rule, "7", 8) is True
    assert reversibility_render.cast_changes("2024-01-05", "2024-1-5", "DATE", "postgresql") is True
    assert reversibility_render.cast_changes("2024-01-05", __import__("datetime").date(2024, 1, 6), "DATE",
                                             "supabase") is True


# ------------------------------------- H-1 / H-2: the backstop counts checked rows per table

def _up(col: str) -> dict:
    return {"source": col, "target": col, "transform": "uppercase", "confidence": 0.99,
            "source_type": "TEXT", "target_type": "TEXT"}


def _two_tables(tmp_path, people: int = 10, orders: int = 30):
    from tests.test_reversibility_review3 import _exec

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"U{i}@EXAMPLE.COM") for i in range(1, people + 1)])
    _exec(src, "CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)")
    _exec(src, "INSERT INTO orders VALUES (?, ?)", [(i, f"NOTE {i}") for i in range(1, orders + 1)])
    return src, dst


def test_h1_an_honest_two_stream_enforced_run_succeeds_and_each_table_is_checked_on_its_own_rows(tmp_path,
                                                                                                fake):  # noqa: F811
    from tests.test_reversibility_review3 import _ID, _sqlite_request

    src, dst = _two_tables(tmp_path)
    contracts = [{"name": "people", "primary_key": ["id"], "selected": True, "mappings": [dict(_ID), _up("email")]},
                 {"name": "orders", "primary_key": ["id"], "selected": True, "mappings": [dict(_ID), _up("note")]}]
    res = _run(fake, _sqlite_request(src, dst, mappings=[], stream_contracts=contracts))
    enforcement = (res.destination_summary or {}).get("reversibility_enforcement") or {}
    assert res.success is True, (res.error, enforcement)
    assert enforcement.get("enforced") is True, enforcement
    by = {(c["table"], c["column"]): c for c in enforcement["columns"]}
    assert by[("people", "email")]["rows_checked"] == 10 and by[("people", "email")]["rows_written"] == 10, by
    assert by[("orders", "note")]["rows_checked"] == 30 and by[("orders", "note")]["rows_written"] == 30, by
    assert _q(dst, "SELECT (SELECT count(*) FROM people), (SELECT count(*) FROM orders)") == [(10, 30)]


def test_h1_h2_the_tally_is_a_count_of_rows_checked_per_table_not_the_highest_row_number():
    from services import reversibility_enforce as enforce

    people, orders = {"source": "email"}, {"source": "note"}
    assert enforce.stamp(people, table="people", column="email", code="normalize", job_id="h2job")
    assert enforce.stamp(orders, table="orders", column="note", code="normalize", job_id="h2job")
    for _batch in range(3):  # a writer that never passes row_number_start: every batch starts at row 1
        enforce.note_rows_checked([people], 1, 10_000)
    enforce.note_rows_checked([orders], 1, 30)
    streams = [{"name": "people", "records_processed": 30_000}, {"name": "orders", "records_processed": 30}]
    fields = {"records_processed": 30_030, "destination_summary": {"streams": streams}}
    assert enforce.record_on_success("h2job", fields) is None
    found = fields["destination_summary"][enforce.SUMMARY_KEY]
    assert found["enforced"] is True
    assert {(c["table"], c["rows_checked"], c["rows_written"]) for c in found["columns"]} == {
        ("people", 30_000, 30_000), ("orders", 30, 30)}
    # One table's own shortfall still fails the run, named against that table's own written count.
    assert enforce.stamp(people, table="people", column="email", code="normalize", job_id="h2short")
    assert enforce.stamp(orders, table="orders", column="note", code="normalize", job_id="h2short")
    enforce.note_rows_checked([people], 1, 10_000)
    enforce.note_rows_checked([orders], 1, 30)
    short = {"records_processed": 30_030, "destination_summary": {"streams": streams}}
    note = enforce.record_on_success("h2short", short)
    assert note and "people.email was checked on only 10,000 of 30,000 written row(s)" in note, note
    assert "orders.note" not in note


def test_h1_without_a_per_table_written_count_the_comparison_is_recorded_unverified_not_failed():
    from services import reversibility_enforce as enforce

    a, b = {"source": "email"}, {"source": "note"}
    assert enforce.stamp(a, table="people", column="email", code="normalize", job_id="h1unv")
    assert enforce.stamp(b, table="orders", column="note", code="normalize", job_id="h1unv")
    enforce.note_rows_checked([a], 1, 10)
    enforce.note_rows_checked([b], 1, 30)
    fields = {"records_processed": 40, "destination_summary": {}}  # two tables, no per-stream stats
    assert enforce.record_on_success("h1unv", fields) is None
    found = fields["destination_summary"][enforce.SUMMARY_KEY]
    assert found["enforced"] is True
    assert {c["table"]: c["rows_written"] for c in found["columns"]} == {"people": None, "orders": None}
    assert "no per-table written count" in found["unverified"], found


def test_h1_the_verification_re_read_does_not_count_as_checked_rows():
    """Gate-8's remap passes the same rows through the stamped mappings; only the write is counted."""
    from connectors.writer_common import build_mapped_rows_with_details, map_rows_for_fingerprint
    from services import reversibility_enforce as enforce

    mapping = {"source": "v", "target": "v", "transform": "uppercase"}
    assert enforce.stamp(mapping, table="t", column="v", code="normalize", job_id="h1fp")
    kw = dict(headers=["v"], data_rows=[["A"], ["B"]], mappings=[mapping], target_cols=["v"])
    build_mapped_rows_with_details(**kw)
    map_rows_for_fingerprint(**kw)
    found = enforce.summary("h1fp")
    assert found["columns"][0]["rows_checked"] == 2, found
