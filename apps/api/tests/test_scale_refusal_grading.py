"""A refused matrix cell passes only for the refusal it provokes.

"Not successful, nothing landed" is also what a dead engine, a wrong password
or a harness typo look like, so grading on that alone reported an outage as
proof of the ragged-row contract — and reported it under the same `pass` token
as a cell that moved 100,000 rows.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.scale import file_matrix as fm  # noqa: E402
from tests.scale import report  # noqa: E402

RAGGED = (
    "CSV row has 13 value-bearing cells but the header names 11 column(s); "
    "refuse silent column drop"
)
EXPECT = ("value-bearing cells", "refuse silent column drop", "ragged")


def _cell(**kw):
    return fm.CellResult(
        name="csv_ragged_to_postgres",
        route="file_to_postgres",
        store="local",
        source="csv_ragged",
        destination="postgresql",
        **kw,
    )


def _score(error: str, *, success: bool = False, landed: int = 0):
    result = SimpleNamespace(success=success, error=error, records_transferred=0)
    with patch.object(fm.rb, "pg_table_count", return_value=landed):
        return fm._finish_refusal(
            _cell(), result, "t", "a ragged row against a fixed header", expect=EXPECT
        )


def test_the_stated_refusal_passes_under_its_own_token():
    scored = _score(RAGGED)
    assert scored.status == fm.REFUSED
    assert scored.status != "pass"
    assert scored.dest_rows_independent == 0
    assert "independent COUNT(*) after refusal = 0" == scored.verification


def test_an_unrelated_outage_is_not_proof_of_the_contract():
    for outage in (
        "could not connect to server: Connection refused",
        "password authentication failed for user \"dataflow\"",
        "FileNotFoundError: fixture missing",
        "",
    ):
        scored = _score(outage)
        assert scored.status == "fail", outage
        assert any("not for a ragged row" in n for n in scored.notes), outage


def test_acceptance_and_partial_write_still_fail():
    assert _score(RAGGED, success=True).status == "fail"
    partial = _score(RAGGED, landed=7)
    assert partial.status == "fail"
    assert any("partial write: 7 rows" in n for n in partial.notes)


def test_a_population_too_small_to_carry_the_dirt_is_skipped_not_graded():
    ragged = next(c for c in fm.build_cells() if c.name == "csv_ragged_to_postgres")
    assert ragged.requires_dirt == ("ragged_row",)
    reason = fm._dirt_gate(ragged, 2000)
    assert "carries no ragged_row" in reason and ">= 25000" in reason
    assert fm._dirt_gate(ragged, 25000) == ""

    strict = next(
        c for c in fm.build_cells() if c.name == "strict_csv_file_to_postgres"
    )
    assert strict.requires_dirt == ("quarantine_cell",)
    assert "quarantine_cell" in fm._dirt_gate(strict, 2000)
    assert fm._dirt_gate(strict, 11000) == ""


def test_report_counts_a_refusal_apart_from_a_transferred_population():
    records = [
        {"status": "pass"},
        {"status": fm.REFUSED},
        {"status": "fail"},
        {"status": "skip (engine has no live driver: ...)"},
    ]
    assert report.summary(records) == (1, 1, 1, 1)
