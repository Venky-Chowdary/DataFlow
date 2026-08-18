"""Validate must name a bounded-carrier defect that Execute would hit.

The production run these cover: 1M CSV rows, ``DECIMAL(12,9) → NUMBER(11,8)``,
Validate green on 25 preview rows, Run failed with 0 rows committed. Preflight
now scans the rows the caller actually holds and states the evidence, and the
file Execute path hands it a fresh read-only pass over the same bytes the writer
is about to stream.
"""

from __future__ import annotations

import csv
import io

from services.population_fit_scan import GATE_ID
from services.preflight_service import run_file_preflight

MAPPINGS = [
    {
        "source": "arr_time",
        "target": "arr_time",
        "confidence": 0.93,
        "target_type": "NUMBER(11,8)",
    }
]
COLUMN_TYPES = {"arr_time": "DECIMAL(12,9)"}
DEST_TYPES = {"arr_time": "NUMBER(11,8)"}


def _rows(count: int, *, unfit_at: tuple[int, ...] = ()) -> list[dict[str, str]]:
    return [
        {"arr_time": "9999.99999999" if i in unfit_at else "12.34567890"}
        for i in range(1, count + 1)
    ]


def _preflight(**kw):
    params = dict(
        columns=["arr_time"],
        column_types=COLUMN_TYPES,
        row_count=1_000,
        mappings=MAPPINGS,
        destination_connected=True,
        destination_column_types=DEST_TYPES,
        destination_db_type="snowflake",
        estimated_bytes=4096,
    )
    params.update(kw)
    return run_file_preflight(**params)


def _gate(result: dict) -> dict:
    gates = [g for g in result.get("gates") or [] if g.get("id") == GATE_ID]
    assert gates, "the population fit gate must always be stated"
    return gates[0]


def test_preview_only_validate_warns_and_never_claims_population_fit() -> None:
    rows = _rows(1_000, unfit_at=(431,))
    result = _preflight(sample_rows=rows[:25])

    gate = _gate(result)
    assert gate["status"] == "warn"
    assert result["population_fit"]["evidence"] == "sampled"
    assert result["population_fit"]["scanned_population"] is False
    assert not any(b.get("id") == GATE_ID for b in result["blockers"])


def test_population_rows_block_validate_with_the_offending_row_numbers() -> None:
    rows = _rows(1_000, unfit_at=(431, 433))
    result = _preflight(
        sample_rows=rows[:25],
        population_rows=rows,
        rows_are_population=True,
    )

    blocker = next(b for b in result["blockers"] if b.get("id") == GATE_ID)
    assert result["passed"] is False
    assert _gate(result)["status"] == "block"
    assert result["population_fit"]["evidence"] == "exact"
    assert result["population_fit"]["rows_scanned"] == 1_000
    assert blocker["details"]["findings"][0]["example_rows"] == [431, 433]


def test_clean_population_does_not_block_and_reports_exact_evidence() -> None:
    rows = _rows(500)
    result = _preflight(
        row_count=500,
        sample_rows=rows[:25],
        population_rows=rows,
        rows_are_population=True,
    )

    assert _gate(result)["status"] == "pass"
    assert result["population_fit"]["evidence"] == "exact"
    assert not any(b.get("id") == GATE_ID for b in result["blockers"])


def test_a_generator_of_population_rows_is_accepted_and_consumed_once() -> None:
    """The file Execute path streams rows; preflight must not need a list."""
    rows = _rows(300, unfit_at=(299,))
    result = _preflight(
        row_count=300,
        sample_rows=rows[:25],
        population_rows=(r for r in rows),
        rows_are_population=True,
    )

    assert result["passed"] is False
    assert result["population_fit"]["rows_scanned"] == 300
    assert result["population_fit"]["findings"][0]["example_rows"] == [299]


def test_widening_declaration_keeps_the_gate_passing_without_rows() -> None:
    result = _preflight(
        column_types={"arr_time": "DECIMAL(11,8)"},
        sample_rows=_rows(5),
    )
    gate = _gate(result)

    assert gate["status"] == "pass"
    assert "no value scan required" in gate["message"]
    assert result["population_fit"]["safe_by_declaration"] == ["arr_time"]


def test_file_row_iterator_replays_every_row_of_a_csv() -> None:
    """``iter_source_rows`` is the pre-write pass the file engine hands preflight."""
    from transfer.file_stream import iter_source_rows

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["arr_time"])
    writer.writeheader()
    for row in _rows(2_000, unfit_at=(1_999,)):
        writer.writerow(row)
    content = buf.getvalue().encode("utf-8")

    seen = list(iter_source_rows(content, "flights.csv", batch_size=250))
    assert len(seen) == 2_000
    assert seen[1_998]["arr_time"] == "9999.99999999"

    # Read-only: a second pass sees the same rows, so the writer's own stream is
    # untouched by the scan.
    assert len(list(iter_source_rows(content, "flights.csv"))) == 2_000


def test_file_iterator_feeds_preflight_end_to_end() -> None:
    from transfer.file_stream import iter_source_rows

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["arr_time"])
    writer.writeheader()
    for row in _rows(5_000, unfit_at=(4_812,)):
        writer.writerow(row)
    content = buf.getvalue().encode("utf-8")

    result = _preflight(
        row_count=5_000,
        sample_rows=_rows(25),
        population_rows=iter_source_rows(content, "flights.csv"),
        rows_are_population=True,
    )

    assert result["passed"] is False
    assert result["population_fit"]["evidence"] == "exact"
    assert result["population_fit"]["findings"][0]["example_rows"] == [4_812]
