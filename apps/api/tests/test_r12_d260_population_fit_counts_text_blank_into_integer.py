"""D260: preflight said 1 row would be held out; the write held out 2.

PostgreSQL ``qty TEXT`` held ``'oops'`` and ``''`` into an existing MySQL
``INT``. The writer refuses both: a text column's ``''`` is a stored value, not
a SQL NULL (blank_cell_contract). The population fit scan skipped every blank
cell, so g3f counted only ``'oops'`` while the sample transform counted both.
"""

from __future__ import annotations

import pytest

from services.population_fit_scan import scan_population_fit

_ROWS = [
    {"id": i, "qty": "oops" if i == 5 else ("" if i == 7 else str(i * 3))}
    for i in range(1, 11)
]
_MAPPINGS = [
    {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INT"},
    {"source": "qty", "target": "qty", "source_type": "TEXT", "target_type": "INT",
     "transform": "integer"},
]


def _held_rows(source_kind: str, source_type: str = "TEXT", source_format: str = "") -> dict:
    mappings = [dict(m) for m in _MAPPINGS]
    mappings[1]["source_type"] = source_type
    report = scan_population_fit(
        _ROWS,
        mappings,
        dest_types={"id": "INT", "qty": "INT"},
        source_types={"id": "INTEGER", "qty": source_type},
        dest_db="mysql",
        dialect_label="mysql",
        job_error_policy="quarantine",
        rows_total=len(_ROWS),
        rows_are_population=True,
        source_kind=source_kind,
        source_format=source_format,
        sync_mode="full_refresh_append",
        dest_table_exists=True,
    )
    return {f.target.source: (f.unfit_rows, f.example_rows) for f in report.findings}


def test_text_source_blank_into_integer_is_counted_like_the_writer_holds_it_out() -> None:
    unfit, rows = _held_rows("database")["qty"]
    assert unfit == 2
    assert set(rows) == {5, 7}


@pytest.mark.parametrize(
    ("source_kind", "source_type", "source_format"),
    [
        ("file", "TEXT", "csv"),
        ("database", "DECIMAL(10,2)", ""),
    ],
)
def test_blank_the_writer_stores_as_null_is_not_counted(
    source_kind: str, source_type: str, source_format: str
) -> None:
    unfit, rows = _held_rows(source_kind, source_type, source_format)["qty"]
    assert unfit == 1
    assert set(rows) == {5}
