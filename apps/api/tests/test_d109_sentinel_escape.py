"""D109 — a real string equal to ``__DF_SQL_NULL__`` must not decode as NULL.

The extract wire carries SQL NULL as the ``__DF_SQL_NULL__`` string, so a
literal colliding with it was silently turned into SQL NULL on every
destination and every verifier reported a match (fingerprints were computed
after the decode on both sides). The wire now escapes colliding literals with
a ``__DF_ESC__`` prefix; decode layers strip one prefix when materializing.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.value_serializer import (
    SQL_NULL_SENTINEL,
    cell_to_string,
    is_reader_null_cell,
)

LITERAL = "__DF_SQL_NULL__"


def _esc_prefix() -> str:
    from services.value_serializer import ESCAPED_WIRE_PREFIX

    return ESCAPED_WIRE_PREFIX


def _unesc(v):
    from services.value_serializer import unescape_wire_text

    return unescape_wire_text(v)


ESCAPED = "__DF_ESC__" + LITERAL  # "__DF_ESC____DF_SQL_NULL__"


def test_wire_escapes_colliding_literal():
    assert cell_to_string(LITERAL, preserve_sql_null=True) == ESCAPED


def test_wire_escapes_case_and_whitespace_collisions():
    for v in ["__df_sql_null__", " __DF_SQL_NULL__ ", "__df_ddb_null__"]:
        out = cell_to_string(v, preserve_sql_null=True)
        assert out == _esc_prefix() + v, out
        assert not is_reader_null_cell(out)


def test_missing_sentinel_stays_absent_on_the_wire():
    # ``__DF_MISSING__`` is itself the missing sentinel string — escaping it
    # turned absent fields into literal text and flipped inferred types
    # (mongo→snowflake ``created`` DATE instead of TIMESTAMP_TZ, silencing the
    # zoneless refusal). The sentinel must keep serializing as absent.
    assert cell_to_string("__DF_MISSING__", preserve_sql_null=True) == ""
    assert cell_to_string("__DF_MISSING__") == ""


def test_wire_escapes_escape_prefix_recursively():
    assert cell_to_string("__DF_ESC__x", preserve_sql_null=True) == "__DF_ESC____DF_ESC__x"
    assert _unesc("__DF_ESC____DF_ESC__x") == "__DF_ESC__x"


def test_true_null_still_emits_sentinel():
    assert cell_to_string(None, preserve_sql_null=True) == SQL_NULL_SENTINEL
    assert is_reader_null_cell(SQL_NULL_SENTINEL)


def test_ordinary_strings_untouched():
    for v in ["plain", "NULL", "null", "None", "", "__DF", "__DF_SQL_NUL"]:
        assert cell_to_string(v, preserve_sql_null=True) == v


def test_sql_bind_leaf_materializes_literal_not_null():
    from connectors.sql_bind import normalize_sql_bind_value

    # NULL wire → SQL NULL bind.
    assert normalize_sql_bind_value(SQL_NULL_SENTINEL, "VARCHAR(8)") is None
    # Escaped literal → the literal string, not NULL (D109).
    assert normalize_sql_bind_value(ESCAPED, "VARCHAR(64)") == LITERAL


def test_json_record_materializes_literal_not_null():
    from connectors.writer_common import mapped_row_to_json_record

    rec = mapped_row_to_json_record((ESCAPED, SQL_NULL_SENTINEL), ["a", "b"])
    assert rec["a"] == LITERAL
    assert rec["b"] is None


def test_apply_transform_keeps_wire_form_for_binder():
    from services.transform_engine import apply_transform

    out, err = apply_transform(ESCAPED, "none")
    assert err is None
    # apply_transform is mid-wire: the escaped form flows through so the bind
    # layer can still distinguish it from the NULL sentinel.
    assert out == ESCAPED
    out2, err2 = apply_transform(SQL_NULL_SENTINEL, "none")
    assert err2 is None and out2 is None


def test_quarantine_wire_round_trip():
    from connectors.writer_common import quarantine_cell_wire
    from services.quarantine_dlq import _unwire_quarantine_detail

    wire = quarantine_cell_wire(ESCAPED)
    assert wire != SQL_NULL_SENTINEL  # not collapsed into NULL
    detail = _unwire_quarantine_detail({"row": {"c": wire}})
    # Unwire yields the decoded literal (a WireLiteral) — binds as text, never
    # re-decodes as NULL (D113 WireLiteral layer).
    assert detail["row"]["c"] == LITERAL
    # Replay → binder materializes the literal.
    from connectors.sql_bind import normalize_sql_bind_value

    assert normalize_sql_bind_value(detail["row"]["c"], "TEXT") == LITERAL
    # A real NULL stays NULL through the same path.
    assert _unwire_quarantine_detail({"row": {"c": SQL_NULL_SENTINEL}})["row"]["c"] is None


def test_sqlite_dest_stores_literal_and_null(tmp_path):
    """End-to-end on the SQLite writer's own bind: literal stays literal,
    source NULL stays NULL, and they remain distinguishable."""
    from connectors.sqlite_writer import _to_sqlite_value

    db = tmp_path / "d109.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany(
            "INSERT INTO t (id, v) VALUES (?, ?)",
            [
                (1, _to_sqlite_value(ESCAPED, "TEXT")),          # literal
                (2, _to_sqlite_value(SQL_NULL_SENTINEL, "TEXT")),  # source NULL
                (3, _to_sqlite_value("plain", "TEXT")),
            ],
        )
        rows = dict(conn.execute("SELECT id, v FROM t").fetchall())
    assert rows[1] == LITERAL
    assert rows[1] is not None
    assert rows[2] is None
    assert rows[3] == "plain"


def test_ddl_sample_probe_skips_null_measures_literal():
    """D106: VARCHAR(8) with real max length 5 must not be measured at the
    sentinel's 15 chars; a real 15-char literal IS measured."""
    from services.ddl_compatibility import _sample_values, _max_string_len

    rows = [
        {"code": SQL_NULL_SENTINEL},        # SQL NULL — no width evidence
        {"code": "abc"},
        {"code": ESCAPED},                  # literal 15 chars
    ]
    vals = _sample_values(rows, "code")
    # The SQL NULL row contributes no width evidence; the escaped literal is
    # measured as its real 15 chars (not the sentinel's coincidence).
    assert vals == ["abc", LITERAL]
    assert _max_string_len(vals) == len(LITERAL)


def test_proof_norm_distinguishes_literal_from_null():
    """stream._pin_norm must not hash a literal like NULL (verifier blind spot)."""
    from src.transfer.stream import _pin_norm  # noqa: PLC2701 — proof helper

    null_hash = _pin_norm(SQL_NULL_SENTINEL, "VARCHAR")
    literal_hash = _pin_norm(ESCAPED, "VARCHAR")
    assert null_hash == "\x00NULL\x00"
    assert literal_hash != "\x00NULL\x00"
    assert literal_hash != null_hash
