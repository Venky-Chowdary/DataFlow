"""X-134: append first load into a run-created/empty table gets value proof.

Count-only engine-copy proof was justified by ``dest = before + delta``: on an
append rerun the destination holds pre-existing rows, so a whole-table count
(or a value census) has two different populations to excuse. On a **first
load** there is no ``before`` — ``dest == staged`` exactly — and the
``(pk, row-hash)`` key census is a valid value compare. A first-load coercion
loss (e.g. MySQL DATETIME(6) microseconds truncated into SQLite text) must
fail with the culprit key instead of riding out as ``partial``/``row_count``.
"""
from __future__ import annotations


def _first_load(*, mode="incremental_append", before=0, target=40, expected=40,
                existed=False):
    from src.transfer import reconcile_step as rs

    return rs._is_append_first_load(
        mode,
        before,
        target,
        expected,
        {"dest_table_existed_before": existed},
    )


def test_first_load_into_run_created_table_is_comparable() -> None:
    assert _first_load() is True


def test_existing_table_is_conservatively_not_comparable() -> None:
    # Even with a zero pre-count, a table that existed keeps the conservative
    # append scope — only a table the run created is proven dest == staged.
    assert _first_load(existed=True, before=0) is False


def test_occupied_table_later_append_is_not_comparable() -> None:
    # dest = before + delta: the census cannot compare two populations.
    assert _first_load(existed=True, before=10, target=50, expected=50) is False


def test_non_append_mode_is_not_first_load() -> None:
    assert _first_load(mode="incremental_upsert") is False
    assert _first_load(mode="full_refresh_overwrite") is False
    assert _first_load(mode="incremental_deduped") is False


def test_count_mismatch_is_not_comparable() -> None:
    assert _first_load(target=39, expected=40) is False


def test_unknown_existence_probe_still_comparable_when_empty() -> None:
    from src.transfer import reconcile_step as rs

    # The existence probe is best-effort; rows_before == 0 already proves
    # there were no pre-existing rows, so an unknown flag must not excuse a
    # skipped value compare.
    assert rs._is_append_first_load(
        "incremental_append", 0, 40, 40, {"dest_table_existed_before": None}
    ) is True
    assert rs._is_append_first_load("append", 0, 20, 20, {}) is True


def test_reconcile_marks_first_load_populations_comparable() -> None:
    """The impl wires the predicate: first-load append upgrades the count
    scope to comparable so the X-68 census gate runs a value compare."""
    import inspect

    from src.transfer import reconcile_step as rs

    src = inspect.getsource(rs._run_reconciliation_impl)
    assert "_is_append_first_load(" in src
