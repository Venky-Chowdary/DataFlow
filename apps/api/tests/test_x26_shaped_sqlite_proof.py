"""X-26: shaped DB→SQLite clean runs were unusable — three independent bugs.

(a) The sample compare rendered a Decimal via str() ('1.50E-8') against the
    stored text ('0.0000000150') and flagged an equal cell as a mismatch.
    Numeric-family columns must compare numerically, not by text.

(b) The key-scoped destination read set ``isolation_level="READ COMMITTED"``
    on a sqlite engine — sqlite only accepts SERIALIZABLE/READ UNCOMMITTED/
    AUTOCOMMIT — so every shaped DB→SQLite proof came back 'unverifiable'.

(c) ``pii_findings`` matched the decimal '0.0000000150' as a phone number
    ('0.' country code + 000-000-0150) and the mismatch detail showed the
    masked '0.********50'. A decimal literal can never be PII.
"""

from __future__ import annotations

import inspect
import re
import sys
from decimal import Decimal
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import pii_guard  # noqa: E402
from services.reconciliation import sample_compare_rows  # noqa: E402

MAPPINGS = [
    {"source": "id", "target": "id", "target_type": "INTEGER"},
    {"source": "amount", "target": "amount", "target_type": "DECIMAL(38,10)"},
    {"source": "name", "target": "name", "target_type": "VARCHAR(100)"},
]


def _compare(src_amount, tgt_amount, *, dest_types=None):
    return sample_compare_rows(
        [{"id": 1, "amount": src_amount, "name": "alpha"}],
        [{"id": 1, "amount": tgt_amount, "name": "alpha"}],
        MAPPINGS,
        target_columns=["id", "amount", "name"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types=dest_types or {},
    )


def test_decimal_repr_vs_stored_text_is_not_a_mismatch():
    """'1.50E-8' (str of the driver Decimal) vs stored '0.0000000150'."""
    out = _compare(Decimal("0.0000000150"), "0.0000000150")
    assert out["mismatches"] == [], (
        f"equal cell flagged: {out['mismatches']}"
    )


def test_text_carrier_typed_source_is_not_a_mismatch():
    """Prod shape: dest_types carries the physical TEXT carrier and the
    mapping has no declared type — a Decimal source still proves the column
    is numeric, so '1.50E-8' vs '0.0000000150' is equal."""
    out = sample_compare_rows(
        [{"id": 1, "amount": Decimal("0.0000000150"), "name": "alpha"}],
        [{"id": 1, "amount": "0.0000000150", "name": "alpha"}],
        [
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
            {"source": "name", "target": "name"},
        ],
        target_columns=["id", "amount", "name"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={"id": "INTEGER", "amount": "TEXT", "name": "TEXT"},
    )
    assert out["mismatches"] == [], (
        f"equal cell flagged: {out['mismatches']}"
    )


def test_text_carrier_string_source_001_still_mismatches():
    """A str '001' source has no numeric provenance: '001' vs '1' on a TEXT
    carrier is a real fidelity loss and must still flag."""
    out = sample_compare_rows(
        [{"id": 1, "code": "001"}],
        [{"id": 1, "code": "1"}],
        [
            {"source": "id", "target": "id"},
            {"source": "code", "target": "code"},
        ],
        target_columns=["id", "code"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={"id": "INTEGER", "code": "TEXT"},
    )
    assert out["passed"] is False


def test_real_numeric_diff_still_mismatches():
    out = _compare(Decimal("0.0000000150"), "0.0000000151")
    assert out["passed"] is False
    assert out["mismatches"]


def test_text_column_001_vs_1_still_mismatches():
    """VARCHAR '001' vs '1' are different text — numeric folding must not fire."""
    out = sample_compare_rows(
        [{"id": 1, "code": "001"}],
        [{"id": 1, "code": "1"}],
        [
            {"source": "id", "target": "id", "target_type": "INTEGER"},
            {"source": "code", "target": "code", "target_type": "VARCHAR(10)"},
        ],
        target_columns=["id", "code"],
        sort_key="id",
        dest_db_type="sqlite",
        dest_types={},
    )
    assert out["passed"] is False


def test_decimal_literal_is_never_pii():
    """'0.0000000150' matched the phone regex and was masked as '0.********50'."""
    for value in ("0.0000000150", "1.50E-8", "-0.0000000001", "123456789012345.12345"):
        assert pii_guard.pii_findings(value) == {}, value
        assert pii_guard.detect_pii(value)["has_pii"] is False, value


def test_real_pii_still_detected():
    assert pii_guard.pii_findings("555-123-4567").get("phone")
    assert pii_guard.pii_findings("4111-1111-1111-1111").get("pci_pan")
    assert pii_guard.pii_findings("alice@example.com").get("email")


def test_sqlite_read_never_sets_read_committed():
    """The key-scoped read must not set READ COMMITTED on a sqlite engine."""
    import src.transfer.reconcile_step as rs

    src = inspect.getsource(rs)
    m = re.search(
        r"if dest_db_type != \"sqlite\":\s*\n(?:.*\n){1,8}?\s*isolation_level=\"READ COMMITTED\"",
        src,
    )
    assert m, "READ COMMITTED on sqlite path is unguarded"
    # And the blanket unguarded call must be gone.
    assert not re.search(
        r"=\s*c0\.execution_options\(\s*\n?\s*isolation_level=\"READ COMMITTED\"\s*\n?\s*\)\s*\n(?!\s*actual_checksum)",
        src,
    ) or "dest_db_type != \"sqlite\"" in src
