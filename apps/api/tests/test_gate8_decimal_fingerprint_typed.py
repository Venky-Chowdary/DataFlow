"""A driver Decimal fingerprints as a number, whatever engine is named.

MySQL/PostgreSQL return ``DECIMAL(12,3)`` cells as ``Decimal('1.000')``. Rendered
to text first, ``1.000`` is a locale-ambiguous literal the number canonicalizer
refuses to fold, so the engine-aware read-back digest said ``1.000`` while the
write pass said ``1`` — and a faithful 2,000-row mirror (whose amounts crossed
1.0) failed Gate-8 on every integral-valued decimal.
"""

from decimal import Decimal

import pytest

from services.reconciliation import fingerprint_for_reconcile, normalize_cell


@pytest.mark.parametrize(
    "value",
    [Decimal("1.000"), Decimal("1000.000"), Decimal("0.500"), Decimal("-2.10"), Decimal("1E+3")],
)
@pytest.mark.parametrize(
    ("engine", "ddl"),
    [("mysql", ""), ("mysql", "DECIMAL(12,3)"), ("postgresql", "NUMERIC(12,3)"), ("", "")],
)
def test_decimal_fingerprint_is_engine_independent(value, engine, ddl):
    assert fingerprint_for_reconcile(value, ddl_type=ddl, engine=engine) == normalize_cell(value)


@pytest.mark.parametrize("ddl", ["decimal", "DECIMAL(12,3)", "NUMERIC(12,3)"])
def test_sqlite_text_carrier_keeps_decimal_fingerprint_typed(ddl):
    """SQLite lands exact decimals in TEXT; the digest still hashes a number."""
    from services.reconciliation import _column_fingerprint_ddl, checksum_rows

    assert _column_fingerprint_ddl("amount", "sqlite", {"amount": ddl}) != "TEXT"
    typed = checksum_rows(
        [{"amount": Decimal("1.000")}], ["amount"],
        dest_db_type="sqlite", dest_types={"amount": ddl},
    )
    read_back = checksum_rows(
        [{"amount": "1.000"}], ["amount"],
        dest_db_type="sqlite", dest_types={"amount": ddl},
    )
    assert typed == read_back
    assert _column_fingerprint_ddl("name", "sqlite", {"name": "string"}) == "TEXT"


def test_integral_decimal_matches_write_pass_integer():
    assert fingerprint_for_reconcile(Decimal("1.000"), engine="mysql") == "1"
    assert fingerprint_for_reconcile(1, engine="mysql") == "1"


def test_physical_text_carrier_does_not_erase_numeric_logical_type():
    """Writer-stamped TEXT for a DECIMAL column keeps the numeric digest steer."""
    from services.reconciliation import overlay_physical_dest_types

    merged = overlay_physical_dest_types(
        {"amount": "DECIMAL(12,3)", "when": "DATETIME(6)", "name": "string"},
        {"amount": "TEXT", "when": "datetime", "name": "TEXT", "extra": "INTEGER"},
    )
    assert merged == {
        "amount": "DECIMAL(12,3)",
        "when": "datetime",
        "name": "TEXT",
        "extra": "INTEGER",
    }
    assert overlay_physical_dest_types({"a": "int"}, None) == {"a": "int"}


def test_sqlite_readback_digest_is_steered_like_the_write_pass(tmp_path):
    """verify_sqlite_table must hash a TEXT-carried decimal as the number written."""
    import sqlite3

    from services.reconciliation import checksum_rows, verify_sqlite_table

    path = tmp_path / "rb.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER, amount TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)", [(1, "0.002"), (2, "10.500")]
        )
    types = {"id": "INTEGER", "amount": "NUMERIC(12,3)"}
    write_pass = checksum_rows(
        [{"id": 1, "amount": Decimal("0.002")}, {"id": 2, "amount": Decimal("10.5")}],
        ["id", "amount"], dest_db_type="sqlite", dest_types=types,
    )
    count, read_back = verify_sqlite_table(
        connection_string="", database=str(path), table_name="t",
        target_columns=["id", "amount"], dest_types=types,
    )
    assert (count, read_back) == (2, write_pass)
    _, untyped = verify_sqlite_table(
        connection_string="", database=str(path), table_name="t",
        target_columns=["id", "amount"],
    )
    assert untyped != write_pass
