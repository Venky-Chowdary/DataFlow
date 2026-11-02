"""D9 live proof: a hosted read-back digests the whole object over real HTTP.

Unlike the stubbed rule test, this drives the Salesforce connector against the
in-process REST double: real requests, real Describe, real SOQL paging (with
the ``OFFSET`` cap that forces the keyset path), against an object holding
2,300 records — well past the 500 the verifier used to read. The old cap could
only ever return ``count = 500`` and the digest of the first 500 rows, which a
strict reconcile then compared against the source's whole-table digest.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services import reconciliation as rc  # noqa: E402
from tests.salesforce_test_server import (  # noqa: E402
    QUERY_BATCH_SIZE,
    start_salesforce_server,
)

POPULATION = 2_300
assert POPULATION > QUERY_BATCH_SIZE, "the read must cross a queryMore boundary"
COLUMNS = ["Id", "Name", "AccountNumber"]


@pytest.fixture
def salesforce():
    server, httpd = start_salesforce_server()
    server.seed(
        "Account",
        [
            {"Name": f"Acme {i:05d}", "AccountNumber": f"K{i:05d}"}
            for i in range(POPULATION)
        ],
    )
    try:
        yield server
    finally:
        httpd.shutdown()


def _verify(server, limit: int) -> tuple[int, str]:
    cfg = server.endpoint_config("Account")
    return rc.verify_salesforce_object(
        host=cfg.get("host", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        connection_string=cfg.get("connection_string", ""),
        api_key=str(cfg.get("api_key") or ""),
        object_name="Account",
        target_columns=COLUMNS,
        limit=limit,
    )


def test_whole_object_readback_counts_and_digests_every_record(salesforce):
    count, checksum = _verify(salesforce, 0)
    assert count == POPULATION, (
        f"read-back saw {count} of {POPULATION} records — a capped read-back "
        "reports a destination population nobody counted"
    )
    assert checksum

    # The digest the old cap produced, shown to be a different value: it is the
    # first 500 records, and comparing it to a whole-source digest fails a load
    # in which every row landed.
    capped_count, capped_checksum = _verify(salesforce, 500)
    assert capped_count == 500
    assert capped_checksum != checksum

    # Stable across reads: the digest is of the population, not of arrival order.
    assert _verify(salesforce, 0) == (POPULATION, checksum)
