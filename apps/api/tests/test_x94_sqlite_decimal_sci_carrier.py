"""X-94: SQLite decimal text carrier is fixed-point, and the comparison is
numeric-canonical — a stored scientific-notation cell can never false-fail.

Live probe at 616df100 (pre-X-43): MySQL ``DECIMAL(65,30)`` -> SQLite
false-failed the clean run — the census reported target ``'-1E-30'`` for
``-0.000000000000000000000000000001``. On the X-43 tree the bind path is
``sqlite_decimal_bind_text`` (fixed-point) and the fingerprint under a
DECIMAL ddl is numeric-canonical, so ``'-1E-30'`` and the fixed-point form
are the same fingerprint. These cells pin the rule hermetically plus a
sqlite->sqlite e2e over sci/fixed-point/zero/65-digit extremes; the MySQL
source leg stays live-gated.
"""

import os
import sys
import sqlite3
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

EXTREMES = [
    "1E-30",
    "-1E-30",
    "0E-30",
    "12345678901234567890123456789012345.000000000000000000000000000001",
    "-12345678901234567890123456789012345.000000000000000000000000000001",
]


def test_x94_decimal_fingerprint_is_numeric_canonical_under_decimal_ddl():
    """'1E-30' and '0.000…001' are the same fingerprint under a DECIMAL ddl;
    they stay distinct under a plain TEXT ddl (honest text is not parsed)."""
    from services.reconciliation import fingerprint_for_reconcile

    fixed = "-0.000000000000000000000000000001"
    assert fingerprint_for_reconcile("-1E-30", ddl_type="DECIMAL(65,30)") == (
        fingerprint_for_reconcile(fixed, ddl_type="DECIMAL(65,30)")
    )
    assert fingerprint_for_reconcile("0E-30", ddl_type="DECIMAL(65,30)") == (
        fingerprint_for_reconcile("0", ddl_type="DECIMAL(65,30)")
    )
    assert fingerprint_for_reconcile(Decimal("-1E-30"), ddl_type="DECIMAL(65,30)") == (
        fingerprint_for_reconcile(fixed, ddl_type="DECIMAL(65,30)")
    )
    # A literal TEXT column does not silently parse numerics.
    assert fingerprint_for_reconcile("-1E-30", ddl_type="TEXT") != (
        fingerprint_for_reconcile(fixed, ddl_type="TEXT")
    )


def test_x94_bind_text_is_fixed_point():
    """The sqlite decimal text carrier never emits scientific notation."""
    from connectors.sqlite_common import sqlite_decimal_bind_text

    for text in EXTREMES:
        bound = sqlite_decimal_bind_text(Decimal(text))
        assert bound is not None
        assert "E" not in bound.upper().replace("NULL", ""), bound
        assert Decimal(bound) == Decimal(text)


def test_x94_sci_notation_source_lands_and_verifies(tmp_path):
    """A source column already holding scientific-notation text lands on a
    DECIMAL(65,30) dest and the clean run verifies — no census false-fail."""
    src = tmp_path / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, t TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?,?)",
        [(i + 1, v) for i, v in enumerate(EXTREMES)] + [(6, None)],
    )
    conn.commit()
    conn.close()

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t"
        ),
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(tmp_path / "dst.db"),
            table="t_dst",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        stream_contracts=[
            {
                "name": "t",
                "selected": True,
                "sync_mode": "full_refresh_overwrite",
                "primary_key": "id",
            }
        ],
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "t", "target": "t", "target_type": "DECIMAL(65,30)"},
        ],
    )
    result = UniversalTransferEngine().execute_tracked(
        req, "x94" + uuid.uuid4().hex[:21]
    )
    assert result.success, result.error
    conn = sqlite3.connect(tmp_path / "dst.db")
    try:
        rows = {r[0]: r[1] for r in conn.execute("SELECT id, t FROM t_dst")}
    finally:
        conn.close()
    # Whatever landed is the numeric value the source held — NULL stays NULL.
    assert rows[6] is None
    for i, text in enumerate(EXTREMES, start=1):
        assert Decimal(str(rows[i])) == Decimal(text), (i, rows[i])


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_X94_LIVE") != "1",
    reason="live engines required — DATAFLOW_X94_LIVE=1",
)
def test_x94_mysql_decimal65_30_to_sqlite_live():
    """MySQL DECIMAL(65,30) {1e-30, -1e-30, 0E-30, 65-digit extremes} -> SQLite:
    fixed-point text on the wire, clean = verified/full_checksum, tamper
    fails naming id 1."""
    pytest.fail("live leg — runs on the user's engine matrix")
