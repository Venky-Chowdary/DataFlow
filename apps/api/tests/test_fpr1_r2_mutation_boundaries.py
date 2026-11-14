"""PR 411 review 2: pin the boundaries the review's surviving mutants crossed.

* A bool folds to '1'/'0' only in a column carrying SOURCE_BOOLEAN; under any
  other carried kind it is the wire text the writer binds (N9).
* Only a typed database source has a catalog to mark from: a CSV or search
  index source never marks, and its catalog is not read (N10).
* A typed datetime bound for an unmarked text column compares as the wire text
  the writer stores for it, fraction digits and offset included, not as the
  instant (N11).
* Text with surrounding whitespace is not folded as an instant (N4): see
  ``test_fpr1_r2_instant_keeps_awareness``.
"""

from __future__ import annotations

import datetime as dt

import pytest

import services.dest_physical_types as dpt
from services.reconciliation import fingerprint_for_reconcile, normalize_cell
from services.value_serializer import cell_to_string


@pytest.mark.parametrize("ddl", ["TEXT CARRYING SOURCE_INSTANT", "TEXT"])
def test_a_bool_folds_only_under_source_boolean(ddl):
    assert normalize_cell(True, ddl_type=ddl) == "true"
    assert normalize_cell(False, ddl_type=ddl) == "false"
    assert fingerprint_for_reconcile(True, ddl_type=ddl) != fingerprint_for_reconcile(
        "1", ddl_type=ddl
    )
    assert normalize_cell(True, ddl_type="TEXT CARRYING SOURCE_BOOLEAN") == "1"


@pytest.mark.parametrize("source_db_type", ["csv", "elasticsearch"])
def test_an_untyped_source_never_marks_or_reads_a_catalog(monkeypatch, source_db_type):
    reads: list[str] = []

    def fake(db_type, cfg, *, table, columns=None):
        reads.append(table)
        return {"c": "boolean"}

    monkeypatch.setattr(dpt, "catalog_declared_types", fake)
    mappings = [{"source": "c", "target": "c", "target_type": "TEXT"}]
    assert dpt.mark_typed_source_text_carriers({"c": "TEXT"}, mappings, source_db_type, {}, "t") == {
        "c": "TEXT"
    }
    assert reads == []
    # The same call from a typed database source does mark (the guard is the source kind).
    assert dpt.mark_typed_source_text_carriers({"c": "TEXT"}, mappings, "mysql", {}, "t") == {
        "c": "TEXT CARRYING SOURCE_BOOLEAN"
    }


@pytest.mark.parametrize(
    "value",
    [
        dt.datetime(2024, 1, 1, 12, 0, 0, 500000),  # noqa: DTZ001
        dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc),
        dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=1))),
    ],
)
def test_a_typed_datetime_into_unmarked_text_is_the_wire_text(value):
    wire = cell_to_string(value)
    assert normalize_cell(value, ddl_type="TEXT") == wire
    assert normalize_cell(value, ddl_type="TEXT") == normalize_cell(wire, ddl_type="TEXT")


def test_unmarked_text_does_not_equate_another_spelling_of_the_instant():
    value = dt.datetime(2024, 1, 1, 12, 0, 0, 500000)  # noqa: DTZ001
    for other in ("2024-01-01 12:00:00.5", "2024-01-01T12:00:00.5"):
        assert normalize_cell(value, ddl_type="TEXT") != normalize_cell(other, ddl_type="TEXT")
    aware = dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc)
    assert normalize_cell(aware, ddl_type="TEXT") != normalize_cell(
        "2024-01-01T12:00:00", ddl_type="TEXT"
    )
