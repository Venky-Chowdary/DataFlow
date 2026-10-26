"""Regression: contract-declared column names must resolve against
folded-case engine headers (Snowflake/Oracle fold unquoted names to
UPPERCASE). All lookups used to be case-sensitive `headers.index(name)`
or `name in headers`, so on a folded engine the cursor column, keyset
bookmark columns, pk source columns, and reconcile key extraction all
silently found nothing: `running_cursor` stayed None, window/boundary
dedupe was skipped, and `written_ids` came back empty — which in turn
disabled the bounded incremental re-read and the keyed readback scope.

These tests fail on c6c16128 (exact-case lookups only) and pass after the
shared `header_column_index` helper resolves exact-then-unique-folded.
"""

from __future__ import annotations


def test_header_column_index_exact_match():
    from services.keyset_pagination import header_column_index

    assert header_column_index(["id", "seq", "v"], "seq") == 1


def test_header_column_index_folded_match():
    from services.keyset_pagination import header_column_index

    # Snowflake / Oracle fold unquoted identifiers to upper-case.
    assert header_column_index(["ID", "SEQ", "V"], "seq") == 1
    assert header_column_index(["ID", "SEQ", "V"], "id") == 0


def test_header_column_index_ambiguous_fold_returns_none():
    from services.keyset_pagination import header_column_index

    # Two headers fold to the same name — ambiguous, must not pick one.
    assert header_column_index(["Seq", "SEQ"], "seq") is None
    assert header_column_index(["a", "b"], "missing") is None
    assert header_column_index([], "x") is None


def test_max_cursor_value_uses_folded_pk():
    from services.sync_cursor import max_cursor_value

    # Engine folded both columns; the contract names are lower-case.
    headers = ["ID", "SEQ", "V"]
    rows = [["1", "1", "a"], ["2", "3", "b"], ["3", "3", "c"]]
    mark = max_cursor_value(rows, headers, "seq", "id")
    assert mark is not None
    # Composite bookmark over the folded headers: max seq=3, tie id=3.
    assert "3" in mark and mark.count("\x1f") >= 1


def test_max_keyset_bookmark_folded_headers():
    from services.keyset_pagination import max_keyset_bookmark

    headers = ["ID", "SEQ"]
    rows = [["1", "5"], ["2", "7"], ["3", "6"]]
    mark = max_keyset_bookmark(rows, headers, ["seq", "id"])
    assert mark is not None


def test_source_key_values_folded_record_keys():
    from src.transfer.reconcile_step import _source_key_values

    # Reconcile samples keep the engine's folded headers as dict keys.
    sample = [{"ID": "1", "SEQ": "1"}, {"ID": "2", "SEQ": "2"}]
    keys = _source_key_values(sample, sort_key="id", mappings=[])
    assert set(keys) == {"1", "2"}
