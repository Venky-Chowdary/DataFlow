"""What Shape declared is what the destination really holds.

Unit tests prove the engine's arithmetic and its accounting. They do not prove
that the *streaming writer* saw shaped rows: the file path maps, fingerprints,
audits and writes inside its own batch loop, so a recipe wired in at the wrong
place would still pass every unit test while the raw values landed.

These tests run the real engine into live PostgreSQL and then re-read the table
with a second driver, asserting four properties a client can see:

* rounded values land rounded, filtered rows do not land at all, and the run's
  ledger says so instead of calling the missing rows a loss;
* chunk boundaries are invisible — a recipe applied 2 rows at a time lands the
  same population as one applied in a single batch, because a schedule that
  re-reads a grown file must not shape it differently;
* a recipe that is not the one Validate approved refuses before any write;
* a row the recipe refuses fails the run, rather than truncating the load at
  that row and reporting the rows before it as a success.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import psycopg2
import pytest

from src.transfer import file_stream
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.typed_fidelity_helpers import pg_endpoint, require_ports, uniq

pytestmark = pytest.mark.timeout(300)

# amount carries more scale than the destination column keeps; "skip" rows are
# the ones the recipe removes on the read.
CSV = (
    b"id,name,amount\n"
    b"1,ada,10.129\n"
    b"2,skip,20.500\n"
    b"3,grace,30.987\n"
    b"4,skip,40.001\n"
    b"5,alan,50.555\n"
)

ROUND_AND_FILTER = {
    "steps": [
        {"op": "round_number", "column": "amount", "options": {"places": 2}},
        {"op": "filter_rows", "options": {"condition": "[name] <> 'skip'"}},
    ]
}


def _mappings() -> list[dict[str, object]]:
    return [
        {
            "source": name,
            "target": name,
            "target_type": target_type,
            "approved": True,
            "confidence": 0.99,
        }
        for name, target_type in (
            ("id", "BIGINT"),
            ("name", "TEXT"),
            ("amount", "DECIMAL(9,2)"),
        )
    ]


def _request(
    table: str,
    *,
    recipe: dict | None = None,
    approved_hash: str = "",
) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=pg_endpoint(table),
        source_content=CSV,
        source_filename="amounts.csv",
        mappings=_mappings(),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe or {},
        approved_shape_recipe_hash=approved_hash,
    )


def _connect():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )


def _landed(table: str) -> list[tuple]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name, amount FROM public."{table}" ORDER BY id')
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _drop(table: str) -> None:
    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


def _execute(request: TransferRequest):
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


@pytest.fixture()
def pg_table():
    require_ports(5432, 27017)
    table = uniq("shape_exec")
    yield table
    _drop(table)


def test_a_shaped_population_is_what_the_destination_holds(pg_table):
    """Rounded values land rounded; filtered rows are removed, not lost."""
    result = _execute(_request(pg_table, recipe=ROUND_AND_FILTER))
    assert result.success, result.error
    assert result.records_transferred == 3, result.error

    landed = _landed(pg_table)
    assert [r[0] for r in landed] == [1, 3, 5]
    assert [r[1] for r in landed] == ["ada", "grace", "alan"]
    # Rounded on the read, so the destination's own scale never had to truncate.
    assert [r[2] for r in landed] == [
        Decimal("10.13"),
        Decimal("30.99"),
        Decimal("50.56"),
    ]

    summary = result.destination_summary or {}
    assert summary.get("shape_recipe_hash")
    assert summary.get("rows_shaped_in") == 5
    assert summary.get("rows_shape_filtered") == 2
    assert summary.get("rows_shape_diverted") == 0
    # Two rows the recipe removed — stated as a shaping effect, never as a
    # quarantine finding or a writer rejection.
    assert summary.get("shape_proof", {}).get("balanced") is True
    assert not summary.get("rejected_details")


def test_chunk_boundaries_do_not_change_the_shaped_population(pg_table, monkeypatch):
    """Two rows at a time must land exactly what one batch lands.

    A schedule re-reads a file that has grown, so the batch split differs
    between beats; if shaping were per-batch stateful in the wrong way, a beat
    would load a different population than the one that was approved.
    """
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", 2)
    result = _execute(_request(pg_table, recipe=ROUND_AND_FILTER))
    assert result.success, result.error
    assert result.records_transferred == 3, result.error
    assert [r[0] for r in _landed(pg_table)] == [1, 3, 5]
    assert [r[2] for r in _landed(pg_table)] == [
        Decimal("10.13"),
        Decimal("30.99"),
        Decimal("50.56"),
    ]
    summary = result.destination_summary or {}
    assert summary.get("rows_shaped_in") == 5
    assert summary.get("rows_shape_filtered") == 2


def test_the_approved_recipe_is_the_only_recipe_that_runs(pg_table):
    """A recipe Validate never saw refuses before the table is created."""
    result = _execute(
        _request(pg_table, recipe=ROUND_AND_FILTER, approved_hash="not-that-recipe")
    )
    assert result.success is False
    assert "re-validate" in (result.error or "").lower() or "approved" in (
        result.error or ""
    ).lower()
    with pytest.raises(psycopg2.Error):
        _landed(pg_table)


def test_a_refused_row_fails_the_run_instead_of_truncating_the_load(pg_table):
    """Fail-closed shaping must not end the stream and call it a success.

    The refusal is raised by the reader that feeds the writer's batch loop, so
    the property under test is that the failure reaches the caller at all: a
    swallowed reader error would report the rows written before row 3 as a
    completed transfer.
    """
    recipe = {
        "steps": [
            {
                "op": "parse_number",
                "column": "name",
                "on_error": "refuse",
            }
        ]
    }
    result = _execute(_request(pg_table, recipe=recipe))
    assert result.success is False
    # The refusal locates itself: which step, which column, which source row.
    error = result.error or ""
    assert "parse_number" in error
    assert "'name'" in error
    assert "source row 1" in error
