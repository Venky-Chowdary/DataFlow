"""Audit C3 / H4b — a text cursor is ordered by the source as text.

* H4b: a TEXT cursor holding ``'1'..'12'``. The engine's MAX is ``'9'``; the
  sync compared numerically, so the verification window excluded ``'10'..'12'``
  and every run failed after the write with "L4/L5 localized: row id='10'
  column None source=None target=None" and removed the destination table.
* C3: a TEXT cursor holding ISO timestamps. Rows at ``08:00Z`` and ``09:00Z``
  load; a row written ``05:30-04:00`` (09:30Z) sorts below the watermark as
  text, is never read, and every later run is green.

One ordering for both sides: where text order and the sync's order cannot
agree the cursor is refused, with the reason, before anything is written.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services import cursor_order as co  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _exec(path: Path, sql: str, rows=None):
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            return (conn.execute(sql) if rows is None else conn.executemany(sql, rows)).fetchall()
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields=None, **kw):
            self.jobs.setdefault(job_id, {}).update({**(fields or {}), **kw})
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, src: Path, dst: Path, cursor: str, *, skip_preflight: bool = True, **extra):
    contract = {
        "name": "t", "cursor_field": cursor, "sync_mode": "incremental_append",
        "selected": True, **extra,
    }
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append", stream_contracts=[contract],
        skip_preflight=skip_preflight, validation_mode="strict",
    )
    job_id = "txt" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _dest_ids(dst: Path) -> list[int]:
    if not dst.exists():
        return []
    try:
        return [r[0] for r in _exec(dst, "SELECT id FROM t ORDER BY id")]
    except sqlite3.OperationalError:
        return []


# --------------------------------------------------------------------------
# Shapes: when does text order equal the sync's order?
# --------------------------------------------------------------------------


def test_numbers_in_text_are_orderable_only_at_one_zero_padded_width():
    assert co.text_cursor_shape(["0001", "0002", "0012"]).orderable
    shape = co.text_cursor_shape([str(i) for i in range(1, 13)])
    assert shape.kind == co.SHAPE_NUMBER and not shape.orderable
    # The pair that shows it: numerically low < high, as text the other way.
    assert int(shape.low) < int(shape.high) and shape.low > shape.high
    # One width is orderable as seen; '99' then '100' is caught by the width probe.
    assert co.text_cursor_shape(["8", "9"]).orderable  # one width; the per-run width check guards the next


def test_timestamps_in_text_are_orderable_only_in_one_offset_and_precision():
    assert co.text_cursor_shape(["2024-01-01T08:00:00Z", "2024-01-01T09:00:00Z"]).orderable
    mixed = co.text_cursor_shape(
        ["2024-01-01T08:00:00Z", "2024-01-01T09:00:00Z", "2024-01-01T05:30:00-04:00"]
    )
    assert mixed.kind == co.SHAPE_MIXED_OFFSETS and not mixed.orderable
    assert (mixed.low, mixed.high) == ("2024-01-01T09:00:00Z", "2024-01-01T05:30:00-04:00")
    # One offset, two precisions: '.5Z' sorts before 'Z'.
    assert not co.text_cursor_shape(["2024-01-01T09:00:00Z", "2024-01-01T09:00:00.5Z"]).orderable
    # A wall clock carries no offset to disagree on.
    assert co.text_cursor_shape(["2024-01-01 08:00:00", "2024-01-01T09:00:00"]).orderable


def test_plain_text_and_harmless_mixes_stay_orderable():
    assert co.text_cursor_shape(["A-1", "A-10", "B-2"]).orderable
    assert co.text_cursor_shape(["100", "A1"]).orderable  # both orders agree
    assert co.text_cursor_shape([]).orderable
    bad = co.text_cursor_shape(["2", "10", "1a"])
    assert bad.kind == co.SHAPE_MIXED and not bad.orderable


def test_refusals_name_the_column_and_the_values():
    reason = co.text_cursor_refusal("batch_no", co.text_cursor_shape(["9", "10"]))
    assert "batch_no" in reason and "'10' sorts before '9'" in reason
    reason = co.text_cursor_refusal(
        "updated_at",
        co.text_cursor_shape(["2024-01-01T09:00:00Z", "2024-01-01T05:30:00-04:00"]),
    )
    assert "more than one UTC offset" in reason
    assert "2024-01-01T05:30:00-04:00" in reason and "2024-01-01T09:00:00Z" in reason
    assert co.text_cursor_refusal("c", co.text_cursor_shape(["a", "b"])) == ""


def test_only_text_declared_columns_are_judged_as_text():
    for declared in ("TEXT", "VARCHAR(64)", "character varying", "NVARCHAR(20)", "string", "CLOB"):
        assert co.is_text_declared(declared), declared
    for declared in ("INTEGER", "BIGINT", "TIMESTAMPTZ", "timestamp without time zone", "DATE", ""):
        assert not co.is_text_declared(declared), declared


# --------------------------------------------------------------------------
# H4b — numeric strings in a TEXT cursor
# --------------------------------------------------------------------------


def test_incremental_text_numeric_cursor_first_load(tmp_path, isolated):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, batch_no TEXT NOT NULL, v TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, str(i), f"v{i}") for i in range(1, 13)])

    result = _run(isolated, src, dst, "batch_no")
    error = result.error or ""
    # Not the garbage a correct load used to die with after writing ...
    assert "column None" not in error and "source=None" not in error, error
    # ... but the reason, with the column and the values, before any write.
    assert not result.success
    assert "batch_no" in error and "text" in error and "sorts before" in error, error
    assert _dest_ids(dst) == []


def test_zero_padded_text_cursor_loads_and_advances(tmp_path, isolated):
    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, batch_no TEXT NOT NULL, v TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, f"{i:04d}", f"v{i}") for i in range(1, 13)])
    first = _run(isolated, src, dst, "batch_no")
    assert first.success, first.error
    assert _dest_ids(dst) == list(range(1, 13))
    _exec(src, "INSERT INTO t VALUES (13, '0013', 'new')")
    second = _run(isolated, src, dst, "batch_no")
    assert second.success, second.error
    assert _dest_ids(dst) == list(range(1, 14))
    # A value that breaks the width is refused, not skipped.
    _exec(src, "INSERT INTO t VALUES (14, '14', 'unpadded')")
    third = _run(isolated, src, dst, "batch_no")
    assert not third.success and "batch_no" in (third.error or ""), third.error
    assert _dest_ids(dst) == list(range(1, 14))


def test_ladder_names_a_row_missing_on_one_side_without_none_fields():
    from services.verification_ladder import localization_summary

    text = localization_summary(
        {
            "rows": [
                {"pk_column": "id", "pk": "10", "reason": "missing_side",
                 "source_present": False, "target_present": True},
                {"pk_column": "id", "pk": "11", "reason": "missing_side",
                 "source_present": False, "target_present": True},
            ]
        },
        "id",
        ["id"],
    )
    assert "None" not in text, text
    assert "id='10'" in text and "destination" in text and "1 more" in text, text
    cell = localization_summary(
        {"rows": [{"pk": "5", "column": "name", "source_value": "a", "target_value": "b"}]},
        "id",
        ["id"],
    )
    assert cell == "L4/L5 localized: row id='5' column 'name' source='a' target='b'"
