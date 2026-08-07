"""Additive ADD under partial Studio — Map stamp required (no VARCHAR invent)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from connectors.writer_common import gate_additive_types_under_partial_studio


def test_gate_additive_refuses_without_map_stamp():
    types, err = gate_additive_types_under_partial_studio(
        target_cols=["id", "note"],
        target_types=["INTEGER", "VARCHAR"],
        existing={"id"},
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "n", "target": "note"},  # no stamp
        ],
        studio_err="PostgreSQL live schema is missing mapped field(s)",
        product="PostgreSQL",
        materialize_stamp=lambda s: s,
    )
    assert err is not None
    assert "note" in err.lower()
    assert "ADD" in err or "additive" in err.lower()


def test_gate_additive_looks_up_stamp_by_target_not_index():
    """Mappings need not be index-aligned with target_cols (omits / reorder)."""
    types, err = gate_additive_types_under_partial_studio(
        target_cols=["id", "note"],
        target_types=["INTEGER", "VARCHAR"],
        existing={"id"},
        mappings=[
            {"source": "n", "target": "note", "target_type": "TEXT"},
            {"source": "id", "target": "id", "target_type": "INTEGER"},
        ],
        studio_err="partial",
        product="PostgreSQL",
        materialize_stamp=lambda s: f"STAMPED:{s}",
    )
    assert err is None
    assert types[1] == "STAMPED:TEXT"



def test_gate_additive_noop_without_studio_err():
    types, err = gate_additive_types_under_partial_studio(
        target_cols=["id", "note"],
        target_types=["INTEGER", "VARCHAR"],
        existing={"id"},
        mappings=[
            {"source": "n", "target": "note"},
        ],
        studio_err=None,
        product="PostgreSQL",
        materialize_stamp=lambda s: s,
    )
    assert err is None
    assert types == ["INTEGER", "VARCHAR"]


def test_gate_additive_pads_empty_types_under_deferred_map():
    """Deferred Map leaves target_types=[] — stamps must land on additive index."""
    types, err = gate_additive_types_under_partial_studio(
        target_cols=["id", "note"],
        target_types=[],  # PG/MySQL/SF under studio_err before rematerialize
        existing={"id"},
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "n", "target": "note", "target_type": "TEXT"},
        ],
        studio_err="partial",
        product="PostgreSQL",
        materialize_stamp=lambda s: f"STAMPED:{s}",
    )
    assert err is None
    assert len(types) == 2
    assert types[0] == ""  # existing — physical owns later
    assert types[1] == "STAMPED:TEXT"


def test_gate_additive_empty_types_refuses_unstamped():
    types, err = gate_additive_types_under_partial_studio(
        target_cols=["id", "note"],
        target_types=[],
        existing={"id"},
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "n", "target": "note"},  # no stamp
        ],
        studio_err="partial",
        product="MySQL",
        materialize_stamp=lambda s: s,
    )
    assert err is not None
    assert "note" in err.lower()


def test_sqlite_backfill_refuses_partial_studio_without_stamp(tmp_path: Path):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "partial_add.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE orders (id INTEGER)")
    conn.commit()
    conn.close()

    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        table_name="orders",
        headers=["id", "note"],
        data_rows=[["1", "hello"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "note", "target": "note", "target_type": "VARCHAR"},
        ],
        column_types={"id": "INTEGER", "note": "VARCHAR"},
        create_table=False,
        backfill_new_fields=True,
        # Partial Studio: id typed, note missing — Map stamp on note still OK.
        destination_column_types={"id": "INTEGER"},
        error_policy="quarantine",
        write_mode="insert",
    )
    # note has Map target_type VARCHAR — allowed under partial Studio.
    assert result.ok is True, result.error

    conn = sqlite3.connect(str(db))
    cols = {row[1] for row in conn.execute("PRAGMA table_info(orders)")}
    conn.close()
    assert "note" in cols


def test_sqlite_backfill_refuses_partial_studio_unstamped(tmp_path: Path):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "partial_add_refuse.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE orders (id INTEGER)")
    conn.commit()
    conn.close()

    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        table_name="orders",
        headers=["id", "note"],
        data_rows=[["1", "hello"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            # No target_type on additive note
            {"source": "note", "target": "note"},
        ],
        column_types={"id": "INTEGER", "note": "VARCHAR"},
        create_table=False,
        backfill_new_fields=True,
        destination_column_types={"id": "INTEGER"},
        error_policy="quarantine",
        write_mode="insert",
    )
    assert result.ok is False
    assert "note" in (result.error or "").lower()


def test_sqlite_partial_studio_fail_policy_defers_map_until_pragma(tmp_path: Path):
    """Partial Studio + error_policy=fail must not abort on pre-physical Map invent.

    Live INTEGER affinity rematerialize owns the carrier; Map VARCHAR stamps must
    not fail-closed before PRAGMA overlay (generic_sql / PG parity).
    """
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "defer_map_fail.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE amounts (amount REAL)")
    conn.commit()
    conn.close()

    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        table_name="amounts",
        headers=["amount"],
        data_rows=[["12.50"]],
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "target_type": "VARCHAR",
                "source_type": "VARCHAR",
            }
        ],
        column_types={"amount": "VARCHAR"},
        create_table=False,
        # Partial Studio: empty coverage for amount → studio_err, defer Map.
        destination_column_types={"other": "INTEGER"},
        error_policy="fail",
        write_mode="insert",
    )
    assert result.ok is True, result.error
    assert result.rows_written == 1

    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute("SELECT amount FROM amounts").fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert float(rows[0][0]) == 12.5


def test_sqlite_create_new_no_studio_still_uses_map_stamps(tmp_path: Path):
    """No Studio dict → create-new still materializes from Map (unchanged)."""
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "no_studio_create.db"
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="main",
        connection_string="",
        ssl=False,
        table_name="orders",
        headers=["id", "note"],
        data_rows=[["1", "hello"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "note", "target": "note", "target_type": "TEXT"},
        ],
        column_types={"id": "INTEGER", "note": "TEXT"},
        create_table=True,
        error_policy="fail",
        write_mode="insert",
    )
    assert result.ok is True, result.error
    assert result.rows_written == 1
