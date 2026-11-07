"""Regression: a declared decimal mapping always wins the fingerprint digest
basis over a sample-inferred declaration.

The file→DB write pass used to let the CSV sample-inferred schema type
(DECIMAL(17,2)) override an approved mapping type (DECIMAL(38,10)) when
choosing the fingerprint basis. Rows whose real values fit the declared type
but not the inference were rejected as scale violations inside the digest, so
they vanished from the source checksum and the run died on a checksum
mismatch. Inferred types may only fill an empty/TEXT carrier — they never
override a declared non-text type.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

# '0.00000015' (scale 8) and '123456789012345.12345' (precision 20, scale 5)
# both violate a sample-inferred DECIMAL(17,2) but fit the approved
# DECIMAL(38,10) mapping. Under the bug they were dropped from the source
# digest and the run failed closed on a checksum mismatch.
CSV = (
    b"id,amount\n"
    b"1,0.00000015\n"
    b"2,123456789012345.12345\n"
    b"3,10.5\n"
)


def _port_up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _mysql_request(table: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="mysql",
            table=table,
            host="127.0.0.1",
            port=3306,
            database="dataflow",
            username="dataflow",
            password="dataflow",
        ),
        source_filename="amounts.csv",
        source_content=CSV,
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        skip_preflight=True,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95, "target_type": "integer"},
            {
                "source": "amount",
                "target": "amount",
                "confidence": 0.95,
                "target_type": "DECIMAL(38,10)",
            },
        ],
    )


@pytest.mark.skipif(not _port_up(3306), reason="MySQL not reachable on 127.0.0.1:3306")
def test_csv_mysql_declared_decimal_wins_digest_basis_over_inferred():
    pymysql = pytest.importorskip("pymysql")
    table = f"decldec_{uuid.uuid4().hex[:8]}"
    result = UniversalTransferEngine().execute(_mysql_request(table))
    summary = result.destination_summary or {}
    try:
        assert result.success, result.error
        assert result.records_transferred == 3
        # The digest basis stamped by the write pass is the declared type —
        # never the sample inference that would have rejected these rows.
        stamped = summary.get("gate8_digest_types") or {}
        assert str(stamped.get("amount", "")).upper().startswith("DECIMAL(38"), stamped
    finally:
        conn = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
        )
        try:
            with conn.cursor() as cur:
                if result.success:
                    from decimal import Decimal

                    cur.execute(f"SELECT amount FROM `{table}` ORDER BY id")
                    got = [Decimal(str(r[0])) for r in cur.fetchall()]
                    assert got == [
                        Decimal("0.0000001500"),
                        Decimal("123456789012345.1234500000"),
                        Decimal("10.5000000000"),
                    ], got
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            conn.commit()
        finally:
            conn.close()
