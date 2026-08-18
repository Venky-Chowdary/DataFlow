"""A narrowing carrier must be decided before the write for tables too.

The reported defect arrived on a CSV, but the sampling gap is not a file
problem: a Postgres/MySQL/Snowflake source is also previewed at Validate. These
cover the projected read-only re-read that feeds the same scan for a table
source, and the honesty rules around it — projection, paging, and readers that
cannot be paged independently.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.population_fit_scan import scan_population_fit
from src.transfer import source_peek
from src.transfer.models import EndpointConfig

MAPPINGS = [
    {
        "source": "arr_time",
        "target": "arr_time",
        "confidence": 0.93,
        "target_type": "NUMBER(11,8)",
    }
]
COLUMN_TYPES = {"arr_time": "DECIMAL(12,9)", "flight_no": "VARCHAR(10)"}
DEST_TYPES = {"arr_time": "NUMBER(11,8)", "flight_no": "VARCHAR(10)"}


class _Batch:
    def __init__(self, headers: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.headers = headers
        self.rows = rows
        self.total_rows = len(rows)


def _fake_reader(
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, Any]],
    *,
    calls: list[dict[str, Any]],
    page: int = 100,
) -> None:
    """Page an in-memory table the way an offset reader would."""

    def _read_batch(
        src_type: str,
        cfg: dict[str, Any],
        table: str,
        columns: list[str] | None,
        offset: int,
        limit: int,
        database: str = "",
        **kw: Any,
    ):
        calls.append({"columns": list(columns or []), "offset": offset, "limit": limit})
        cols = list(columns or list(rows[0].keys() if rows else []))
        window = rows[offset : offset + limit]
        return _Batch(cols, [tuple(r.get(c) for c in cols) for r in window])

    monkeypatch.setattr(source_peek, "_OFFSET_PAGEABLE", frozenset({"postgresql"}))
    import src.transfer.stream as stream_mod

    monkeypatch.setattr(stream_mod, "_read_batch", _read_batch)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", page)
    monkeypatch.setattr(stream_mod, "_source_name", lambda _src: "flights")
    import src.transfer.adapters as adapters_mod

    monkeypatch.setattr(adapters_mod, "resolve_connector_config", lambda _src: {})


def _source() -> EndpointConfig:
    return EndpointConfig(kind="database", format="postgresql", table="flights")


def _table_rows(count: int, *, unfit_at: tuple[int, ...] = ()) -> list[dict[str, Any]]:
    return [
        {
            "arr_time": "9999.99999999" if i in unfit_at else "12.34567890",
            "flight_no": f"DL{i}",
        }
        for i in range(1, count + 1)
    ]


def test_table_pass_reads_every_page_and_finds_the_late_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _fake_reader(monkeypatch, _table_rows(450, unfit_at=(431,)), calls=calls, page=100)

    report = scan_population_fit(
        source_peek.iter_stream_source_column_rows(_source(), ["arr_time"]),
        MAPPINGS,
        dest_types=DEST_TYPES,
        source_types=COLUMN_TYPES,
        dest_db="snowflake",
        job_error_policy="fail",
        rows_total=450,
        rows_are_population=True,
    )

    assert report.evidence == "exact"
    assert report.rows_scanned == 450
    assert report.findings[0].example_rows == (431,)
    # Paged, not one unbounded SELECT.
    assert [c["offset"] for c in calls] == [0, 100, 200, 300, 400]


def test_only_bounded_columns_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _fake_reader(monkeypatch, _table_rows(10), calls=calls, page=100)

    list(source_peek.iter_stream_source_column_rows(_source(), ["arr_time"]))

    assert calls, "the projected read must reach the reader"
    assert all(c["columns"] == ["arr_time"] for c in calls)


def test_a_cursor_only_reader_yields_nothing_rather_than_claiming_a_walk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _fake_reader(monkeypatch, _table_rows(10), calls=calls, page=100)
    source = EndpointConfig(kind="database", format="dynamodb", table="flights")

    rows = list(source_peek.iter_stream_source_column_rows(source, ["arr_time"]))

    assert rows == []
    assert calls == []
    # No rows means unmeasured evidence, never a clean claim.
    report = scan_population_fit(
        rows,
        MAPPINGS,
        dest_types=DEST_TYPES,
        source_types=COLUMN_TYPES,
        dest_db="snowflake",
        rows_total=10,
        rows_are_population=True,
    )
    assert report.evidence == "unmeasured"
    assert report.findings == ()


def test_no_bounded_column_issues_no_query(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _fake_reader(monkeypatch, _table_rows(10), calls=calls, page=100)

    rows = list(source_peek.iter_stream_source_column_rows(_source(), []))

    assert rows == []
    assert calls == []


def test_row_limit_stops_the_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _fake_reader(monkeypatch, _table_rows(450, unfit_at=(431,)), calls=calls, page=100)

    rows = list(
        source_peek.iter_stream_source_column_rows(_source(), ["arr_time"], limit=120)
    )

    assert len(rows) == 120
