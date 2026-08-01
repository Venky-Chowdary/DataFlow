"""Wave 101: Iceberg upsert must read only the keys a batch touches.

Every CDC batch carries ``_df_lsn``, so the sparse/LSN overlay used to call a
bare ``tbl.scan()`` and materialise the whole destination table into Python
dicts once per 20k-row chunk. That does not survive a real lakehouse table.
The scan is now pushed down as a primary-key row filter, sliced so a large
batch does not build one enormous boolean expression, with a full-scan
fallback whenever the predicate cannot be built (correctness before speed).
"""

from __future__ import annotations

import sys
import types

import pyarrow as pa
import pytest

from connectors.iceberg_writer import _PK_SCAN_SLICE, _scan_existing_by_pk


class _FakeScan:
    def __init__(self, table: pa.Table, row_filter, calls: list):
        self._table = table
        self._row_filter = row_filter
        calls.append(row_filter)

    def to_arrow(self) -> pa.Table:
        if self._row_filter is None:
            return self._table
        wanted = set(getattr(self._row_filter, "values", []))
        mask = [v in wanted for v in self._table.column("id").to_pylist()]
        return self._table.filter(pa.array(mask))


class _FakeTable:
    """Minimal stand-in for a pyiceberg Table with a recorded scan history."""

    def __init__(self, table: pa.Table):
        self._table = table
        self.scan_calls: list = []

    def scan(self, row_filter=None):
        return _FakeScan(self._table, row_filter, self.scan_calls)


def _dest_table(n: int) -> pa.Table:
    return pa.table(
        {
            "id": [str(i) for i in range(n)],
            "note": [f"note-{i}" for i in range(n)],
            "_df_lsn": [f"0/{i}" for i in range(n)],
        }
    )


@pytest.fixture()
def stub_expressions(monkeypatch):
    """Install a minimal ``pyiceberg.expressions`` so the predicate can build."""

    class In:
        def __init__(self, field, values):
            self.field = field
            self.values = list(values)

    class EqualTo:
        def __init__(self, field, value):
            self.field = field
            self.value = value

    class And:
        def __init__(self, left, right):
            self.left, self.right = left, right

    class Or:
        def __init__(self, left, right):
            self.left, self.right = left, right

    module = types.ModuleType("pyiceberg.expressions")
    module.In, module.EqualTo, module.And, module.Or = In, EqualTo, And, Or
    pkg = sys.modules.get("pyiceberg") or types.ModuleType("pyiceberg")
    monkeypatch.setitem(sys.modules, "pyiceberg", pkg)
    monkeypatch.setitem(sys.modules, "pyiceberg.expressions", module)
    return module


def test_bounded_scan_reads_only_batch_keys(stub_expressions):
    tbl = _FakeTable(_dest_table(1000))
    keys = [("7",), ("42",), ("900",)]

    existing = _scan_existing_by_pk(tbl, ["id"], keys)

    assert set(existing) == {("7",), ("42",), ("900",)}
    assert existing[("42",)]["note"] == "note-42"
    # One filtered scan, and it was not a full-table read.
    assert len(tbl.scan_calls) == 1
    assert tbl.scan_calls[0] is not None
    # Predicate carries both the string form and the int coercion so a typed
    # Iceberg column still matches a string CDC key.
    assert {"7", "42", "900"} <= {str(v) for v in tbl.scan_calls[0].values}
    assert {7, 42, 900} <= {v for v in tbl.scan_calls[0].values if isinstance(v, int)}


def test_bounded_scan_slices_large_key_sets(stub_expressions):
    total = _PK_SCAN_SLICE * 2 + 5
    tbl = _FakeTable(_dest_table(total))
    keys = [(str(i),) for i in range(total)]

    existing = _scan_existing_by_pk(tbl, ["id"], keys)

    assert len(existing) == total
    # Sliced on key-tuples (each key then expands to type variants).
    assert len(tbl.scan_calls) == 3
    assert all(
        len({str(v) for v in c.values}) <= _PK_SCAN_SLICE for c in tbl.scan_calls
    )


def test_bounded_scan_dedupes_repeated_keys(stub_expressions):
    tbl = _FakeTable(_dest_table(10))
    keys = [("3",), ("3",), ("3",), ("4",)]

    existing = _scan_existing_by_pk(tbl, ["id"], keys)

    assert set(existing) == {("3",), ("4",)}
    assert len(tbl.scan_calls) == 1
    assert {"3", "4"} <= {str(v) for v in tbl.scan_calls[0].values}


def test_falls_back_to_full_scan_without_pyiceberg(monkeypatch):
    # No pyiceberg.expressions available: the predicate cannot be built, so we
    # must still return the requested rows via a full scan (filtered to the
    # batch's key set — never the whole table) instead of reading none.
    monkeypatch.setitem(sys.modules, "pyiceberg.expressions", None)
    tbl = _FakeTable(_dest_table(5))

    existing = _scan_existing_by_pk(tbl, ["id"], [("1",), ("2",)])

    assert set(existing) == {("1",), ("2",)}
    assert tbl.scan_calls[-1] is None


def test_string_cdc_key_matches_int_destination_row(stub_expressions):
    """CDC often delivers string PKs; Iceberg columns are frequently typed ints."""
    table = pa.table(
        {
            "id": [7, 42],
            "note": ["seven", "forty-two"],
            "_df_lsn": ["0/1", "0/2"],
        }
    )
    tbl = _FakeTable(table)

    existing = _scan_existing_by_pk(tbl, ["id"], [("7",), ("42",)])

    assert set(existing) == {("7",), ("42",)}
    assert existing[("42",)]["note"] == "forty-two"


def test_composite_keys_build_or_of_and(stub_expressions):
    table = pa.table(
        {
            "id": ["1", "2"],
            "region": ["us", "eu"],
            "_df_lsn": ["0/1", "0/2"],
        }
    )

    class _CompositeTable(_FakeTable):
        def scan(self, row_filter=None):
            self.scan_calls.append(row_filter)
            outer = self

            class _S:
                def to_arrow(self_inner):
                    return outer._table

            return _S()

    tbl = _CompositeTable(table)
    existing = _scan_existing_by_pk(tbl, ["id", "region"], [("1", "us"), ("2", "eu")])

    assert set(existing) == {("1", "us"), ("2", "eu")}
    predicate = tbl.scan_calls[0]
    assert predicate is not None
    # Composite keys must not collapse to a single-column IN.
    assert not hasattr(predicate, "values")
