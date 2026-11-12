"""PR 407 round-2 review — regressions of the commit-safe incremental window.

Each test reproduces a round-2 finding. Live cases need a reachable
PostgreSQL; they skip, never pass, when it is not there.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
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
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(tmp_path))
    return fake


# --------------------------------------------------------------------------
# H-1 — a file source's date cursor is bounded on the value the watermark keeps
# --------------------------------------------------------------------------

_DATES = {
    "us": (["12/30/2025", "12/31/2025"], ["01/15/2026", "02/03/2026"]),
    "eu": (["30/12/2025", "31/12/2025"], ["15/01/2026", "03/02/2026"]),
}


def _write_source(path, fmt: str, rows: list[tuple[int, str, str]]) -> None:
    if fmt == "jsonl":
        import json

        path.write_text("".join(json.dumps({"id": i, "d": d, "v": v}) + "\n" for i, d, v in rows))
    else:
        path.write_text("id,d,v\n" + "".join(f"{i},{d},{v}\n" for i, d, v in rows))


def _two_file_runs(fake, tmp_path, *, dates: str, fmt: str, preflight: bool):
    first, second = _DATES[dates]
    dst = tmp_path / "dest.sqlite"
    src = tmp_path / f"orders.{fmt}"
    rows: list[tuple[int, str, str]] = []
    outcomes = []
    for run, new in enumerate((first, second), start=1):
        rows += [(len(rows) + 1 + i, d, f"r{run}") for i, d in enumerate(new)]
        _write_source(src, fmt, rows)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format=fmt), source_filename=src.name,
            source_path=str(src), source_content=src.read_bytes(),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="orders"),
            mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
            sync_mode="incremental_append", validation_mode="strict", skip_preflight=not preflight,
            stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                               "cursor_field": "d", "selected": True,
                               "cursor_semantics": "insert_only"}],
        )
        job_id = "r2f" + uuid.uuid4().hex[:16]
        fake.update_job_status(job_id, "pending", transfer_request={})
        outcomes.append(UniversalTransferEngine().execute_tracked(request, job_id))
    with sqlite3.connect(dst) as conn:
        landed = conn.execute("SELECT id FROM orders ORDER BY id").fetchall()
    return outcomes, [r[0] for r in landed]


@pytest.mark.parametrize("path", ["copy", "row"])
@pytest.mark.parametrize("preflight", [False, True], ids=["no-preflight", "preflight"])
@pytest.mark.parametrize("dates", ["us", "eu"])
def test_csv_non_iso_date_cursor_second_run_writes_only_new_rows(
    isolated, tmp_path, monkeypatch, dates, preflight, path
):
    """Run 2 of a CSV schedule whose cursor is a US or EU date writes the two
    new rows only. The bound used to compare the raw '12/30/2025' with the
    stored ISO watermark, rank it as text above every date, and re-append
    every earlier row while the run reported success."""
    if path == "row":
        if preflight:
            pytest.skip("the preflight read scope of a file source is fixed next")
        monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    (run1, run2), landed = _two_file_runs(isolated, tmp_path, dates=dates, fmt="csv", preflight=preflight)
    assert run1.success and run1.records_transferred == 2, run1.error
    assert run2.success, run2.error
    assert run2.records_transferred == 2, (run2.records_transferred, landed)
    assert landed == [1, 2, 3, 4]


@pytest.mark.parametrize("preflight", [False, True], ids=["no-preflight", "preflight"])
@pytest.mark.parametrize("dates", ["us", "eu"])
def test_jsonl_non_iso_date_cursor_second_run_writes_only_new_rows(isolated, tmp_path, dates, preflight):
    (run1, run2), landed = _two_file_runs(isolated, tmp_path, dates=dates, fmt="jsonl", preflight=preflight)
    assert run1.success and run1.records_transferred == 2, run1.error
    assert run2.success, run2.error
    assert run2.records_transferred == 2, (run2.records_transferred, landed)
    assert landed == [1, 2, 3, 4]


def test_file_cursor_bound_reads_cells_as_the_cursor_type():
    """The bound reads a file cell the way the write path does: by the column's
    declared or inferred type, under the file's date order."""
    from services.sync_cursor import cursor_value_coercer, records_after_watermark
    from services.transform_engine import reset_active_date_locale, set_active_date_locale

    records = [{"d": d} for d in ("12/30/2025", "12/31/2025", "01/15/2026")]
    token = set_active_date_locale("MDY")
    try:
        coerce = cursor_value_coercer("date")
        assert coerce is not None and coerce("01/15/2026") == "2026-01-15"
        delta, unbounded = records_after_watermark(records, "d", "2025-12-31", coerce=coerce)
    finally:
        reset_active_date_locale(token)
    assert unbounded == 0
    assert [r["d"] for r in delta] == ["01/15/2026"]
    # A text column keeps its own text; an unreadable date keeps its text (it
    # stays in the delta and the write path judges it).
    assert cursor_value_coercer("string") is None
    assert cursor_value_coercer("date")("not a date") == "not a date"


@pytest.mark.parametrize("dates", ["us", "eu"])
def test_file_to_file_export_non_iso_date_cursor_second_run_writes_only_new_rows(
    isolated, tmp_path, monkeypatch, dates
):
    """The buffered (batch) path bounds a file the same way: run 2 exports
    the two new rows, neither every row again nor none."""
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    first, second = _DATES[dates]
    src = tmp_path / "orders.csv"
    rows: list[tuple[int, str, str]] = []
    written = []
    for run, new in enumerate((first, second), start=1):
        rows += [(len(rows) + 1 + i, d, f"r{run}") for i, d in enumerate(new)]
        _write_source(src, "csv", rows)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"), source_filename=src.name,
            source_path=str(src), source_content=src.read_bytes(),
            destination=EndpointConfig(kind="file_export", format="csv"),
            mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
            sync_mode="incremental_append", validation_mode="strict", skip_preflight=True,
            stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                               "cursor_field": "d", "selected": True,
                               "cursor_semantics": "insert_only"}],
        )
        job_id = "r2e" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        written.append(result.records_transferred)
    assert written == [2, 2]


def test_text_typed_calendar_cursor_on_a_file_route_is_refused_not_misread():
    """A cursor the route keeps as text cannot be read as a date: as text
    '01/15/2026' sorts before '12/31/2025', so a later row would be skipped.
    The bound refuses it, as a SQL text cursor of that shape is refused."""
    from services.sync_cursor import records_after_watermark

    with pytest.raises(ValueError, match="day/month or month/day"):
        records_after_watermark([{"d": "01/15/2026"}], "d", "12/31/2025", text_calendar_refusal=True)


@pytest.mark.parametrize("cells", ["date-cells", "us-text-cells"])
def test_excel_date_cursor_second_run_writes_only_new_rows(isolated, tmp_path, cells):
    """An Excel source: real date cells and US date text both bound run 2 to
    the two new rows."""
    from datetime import date

    import openpyxl

    first = [date(2025, 12, 30), date(2025, 12, 31)]
    second = [date(2026, 1, 15), date(2026, 2, 3)]
    dst = tmp_path / "dest.sqlite"
    src = tmp_path / "orders.xlsx"
    rows: list[tuple[int, object, str]] = []
    written = []
    for run, new in enumerate((first, second), start=1):
        rows += [(len(rows) + 1 + i, d, f"r{run}") for i, d in enumerate(new)]
        book = openpyxl.Workbook()
        sheet = book.active
        sheet.append(["id", "d", "v"])
        for i, d, v in rows:
            sheet.append([i, d if cells == "date-cells" else d.strftime("%m/%d/%Y"), v])
        book.save(src)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="excel"), source_filename=src.name,
            source_path=str(src), source_content=src.read_bytes(),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="orders"),
            mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
            sync_mode="incremental_append", validation_mode="strict", skip_preflight=True,
            stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                               "cursor_field": "d", "selected": True,
                               "cursor_semantics": "insert_only"}],
        )
        job_id = "r2x" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        written.append(result.records_transferred)
    with sqlite3.connect(dst) as conn:
        landed = [r[0] for r in conn.execute("SELECT id FROM orders ORDER BY id")]
    assert written == [2, 2] and landed == [1, 2, 3, 4], (written, landed)
