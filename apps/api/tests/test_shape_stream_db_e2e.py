"""A database stream shapes the rows it writes, and still reads the whole table.

The file path materializes its rows, so a recipe applied there cannot get the
pagination wrong. A database stream is the opposite: it pages the source with
OFFSET / keyset bookmarks, and a recipe that removes rows changes how many rows
the page has *after* the source decided which rows it handed over. Advancing the
next read by the survivors re-reads rows that were already read (duplicates) or
never reads the table's tail (silent loss) — while every count in the run agrees
with itself, which is the worst kind of wrong.

These tests run the real streaming engine PostgreSQL → PostgreSQL with pages
smaller than the table and re-read the destination with a second driver:

* the destination holds the shaped population exactly once, tail included;
* the ledger states the removed rows as a shaping effect, not as a loss;
* a recipe that is not the approved one refuses before the table is created;
* a refused row fails the run instead of committing the rows before it.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import psycopg2
import pytest

from src.transfer import stream as stream_mod
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from tests.typed_fidelity_helpers import pg_endpoint, require_ports, uniq

# amount carries more scale than the destination column keeps; "skip" rows are
# the ones the recipe removes on the read. Row 5 is the tail: it only lands if
# pagination advanced by the rows the source handed over.
SOURCE_ROWS = (
    (1, "ada", Decimal("10.129")),
    (2, "skip", Decimal("20.500")),
    (3, "grace", Decimal("30.987")),
    (4, "skip", Decimal("40.001")),
    (5, "alan", Decimal("50.555")),
)

ROUND_AND_FILTER = {
    "steps": [
        {"op": "round_number", "column": "amount", "options": {"places": 2}},
        {"op": "filter_rows", "options": {"condition": "[name] <> 'skip'"}},
    ]
}


def _connect():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )


def _seed(table: str) -> None:
    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f"""
                CREATE TABLE public."{table}" (
                  id INT PRIMARY KEY,
                  name TEXT NOT NULL,
                  amount NUMERIC(12,3) NOT NULL
                )
                """
            )
            cur.executemany(
                f'INSERT INTO public."{table}" (id, name, amount) VALUES (%s, %s, %s)',
                [tuple(r) for r in SOURCE_ROWS],
            )
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


def _landed(table: str) -> list[tuple]:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, name, amount FROM public."{table}" ORDER BY id')
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _mappings(amount_type: str = "DECIMAL(9,2)") -> list[dict[str, object]]:
    """``amount_type`` is the carrier: a shaped run rounds to fit the narrow one."""
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
            ("amount", amount_type),
        )
    ]


def _request(
    src_table: str,
    dest_table: str,
    *,
    recipe: dict | None = None,
    approved_hash: str = "",
    amount_type: str = "DECIMAL(9,2)",
    source_filter: dict | None = None,
) -> TransferRequest:
    return TransferRequest(
        source=pg_endpoint(src_table),
        destination=pg_endpoint(dest_table),
        mappings=_mappings(amount_type),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe or {},
        approved_shape_recipe_hash=approved_hash,
        source_filter=source_filter or {},
    )


def _execute(request: TransferRequest):
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


@pytest.fixture()
def tables(monkeypatch):
    require_ports(5432, 27017)
    src = uniq("shape_src")
    dest = uniq("shape_dst")
    # Pages smaller than the table, and smaller than the run of rows the recipe
    # removes, so every pagination mistake shows up as a wrong population.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 2)
    _seed(src)
    yield src, dest
    _drop(src)
    _drop(dest)


def test_a_paged_stream_lands_the_shaped_population_once(tables):
    """Shaped values, no duplicates, and the tail of the table still arrives."""
    src, dest = tables
    result = _execute(_request(src, dest, recipe=ROUND_AND_FILTER))
    assert result.success, result.error
    assert result.records_transferred == 3, result.error

    landed = _landed(dest)
    # id 5 is on the last page: it only lands if the next OFFSET counted the
    # rows the source handed over, not the ones the recipe left behind. And ids
    # appear once: a page re-read would duplicate 3 or 5.
    assert [r[0] for r in landed] == [1, 3, 5]
    assert [r[1] for r in landed] == ["ada", "grace", "alan"]
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
    # The two removed rows are a shaping effect — never a quarantine finding.
    assert summary.get("shape_proof", {}).get("balanced") is True
    assert not summary.get("rejected_details")


def test_no_recipe_leaves_the_paged_stream_exactly_as_it_was(tables):
    """The whole table lands unshaped when no recipe is declared."""
    src, dest = tables
    # The source's own carrier: without a recipe nothing rounds, so the narrowing
    # gate is right to refuse DECIMAL(9,2) — which is the very reason the shaped
    # run above is allowed to use it.
    result = _execute(_request(src, dest, amount_type="DECIMAL(12,3)"))
    assert result.success, result.error
    assert result.records_transferred == 5, result.error
    landed = _landed(dest)
    assert [r[0] for r in landed] == [1, 2, 3, 4, 5]
    summary = result.destination_summary or {}
    assert "shape_recipe_hash" not in summary
    assert "shape_proof" not in summary


def test_a_source_filter_and_a_recipe_each_apply_once_to_a_page(tables):
    """Two rewrites of the same page, in order, neither repeated nor skipped.

    The first page is prepared twice (its DDL is committed before any worker
    runs), so a page that is filtered again would drop rows it already kept, and
    a page shaped again would round an already-rounded value.
    """
    src, dest = tables
    result = _execute(
        _request(
            src,
            dest,
            recipe=ROUND_AND_FILTER,
            source_filter={"column": "id", "operator": "gt", "value": 1},
            # A row filter means this run's rows are not the table's population,
            # so the narrowing scan is (rightly) skipped and a narrow carrier
            # cannot be proven here; the source's own carrier keeps this test
            # about pagination and once-only application.
            amount_type="DECIMAL(12,3)",
        )
    )
    assert result.success, result.error

    landed = _landed(dest)
    # id 1 is removed by the source filter, ids 2 and 4 by the recipe, and id 5
    # still arrives: both rewrites shrank the page, and the OFFSET moved by the
    # rows the source handed over.
    assert [r[0] for r in landed] == [3, 5]
    assert [r[2] for r in landed] == [Decimal("30.99"), Decimal("50.56")]

    summary = result.destination_summary or {}
    # The recipe only ever saw the rows the filter let through, and it saw each
    # of them once — 4 reads, not 5 and not 6.
    assert summary.get("rows_shaped_in") == 4
    assert summary.get("rows_shape_filtered") == 2
    assert summary.get("shape_proof", {}).get("balanced") is True
    # Proof names which authority removed a row: the operator's declared scope
    # removed one before the recipe was ever offered it, the recipe removed two.
    assert summary.get("rows_source_filtered") == 1
    assert summary.get("rows_shaped_out") == 2
    assert summary.get("rows_removed_on_read") == 3
    assert int(summary.get("source_row_count") or 0) == 5


def test_the_approved_recipe_is_the_only_recipe_a_stream_runs(tables):
    """A recipe Validate never saw refuses before the destination is created."""
    src, dest = tables
    result = _execute(
        _request(src, dest, recipe=ROUND_AND_FILTER, approved_hash="not-that-recipe")
    )
    assert result.success is False
    error = (result.error or "").lower()
    assert "re-validate" in error or "approved" in error
    with pytest.raises(psycopg2.Error):
        _landed(dest)


def test_a_refused_row_fails_the_paged_stream_instead_of_committing_part(tables):
    """Fail-closed shaping must not end the stream and call the pages a success."""
    src, dest = tables
    recipe = {
        "steps": [{"op": "parse_number", "column": "name", "on_error": "refuse"}]
    }
    result = _execute(_request(src, dest, recipe=recipe))
    assert result.success is False
    error = result.error or ""
    assert "parse_number" in error
    assert "'name'" in error
    assert "source row 1" in error
