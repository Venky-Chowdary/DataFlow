"""D9: MySQL DECIMAL precision is not the wire display width.

pymysql puts MySQL's column_length — precision + sign + decimal point —
in the PEP 249 precision slot: ``CAST(x AS DECIMAL(38,10))`` described as
DECIMAL(40,10), and rc-fidelity-collapse refused it (or it landed on
Snowflake as VARCHAR). The mapper must recover the declared precision on
both the table and the query path (both read ``cursor_declared_carriers``).
"""

from __future__ import annotations

import pytest


def test_mysql_newdecimal_display_width_recovers_precision():
    from services.decimal_observe import cursor_declared_numeric_types

    # PEP 249: (name, type_code, display_size, internal_size, precision,
    #           scale, null_ok). pymysql fills index 4 with column_length.
    description = [
        ("x", 246, 40, 40, 40, 10, None),   # DECIMAL(38,10) wire meta
        ("y", 246, 22, 22, 22, 10, None),   # DECIMAL(20,10)
        ("z", 246, 39, 39, 39, 0, None),    # DECIMAL(38,0) → length M+1
    ]
    out = cursor_declared_numeric_types(["x", "y", "z"], description, "mysql")
    assert out["x"] == "DECIMAL(38,10)", out
    assert out["y"] == "DECIMAL(20,10)", out
    assert out["z"] == "DECIMAL(38,0)", out


def test_postgres_numeric_oid_untouched():
    from services.decimal_observe import cursor_declared_numeric_types

    # PG OID 1700 reports true precision — no width adjustment.
    description = [("n", 1700, 0, 0, 38, 10, None)]
    out = cursor_declared_numeric_types(["n"], description, "postgresql")
    assert out["n"] == "DECIMAL(38,10)", out


def test_live_mysql_cast_decimal_38_10():
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            connect_timeout=2,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"local MySQL unreachable: {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT CAST(1234567.8901234567 AS DECIMAL(38,10)) AS x")
            desc = cur.description
    finally:
        conn.close()
    from services.decimal_observe import cursor_declared_numeric_types

    out = cursor_declared_numeric_types(["x"], desc, "mysql")
    assert out["x"] == "DECIMAL(38,10)", (
        f"declared carrier must be DECIMAL(38,10), got {out!r} from {desc!r}"
    )
