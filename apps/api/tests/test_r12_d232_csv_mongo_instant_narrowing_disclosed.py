"""D232: CSV microsecond instants into a Mongo BSON date are narrowed to ms.

A CSV source declares no fractional precision, so Gate-8 fingerprinted both
sides at the BSON millisecond granularity and reported "checksums match"
without naming the narrowing. The measured source cells carry six digits; the
verdict must name the column as compared at the carrier's granularity.
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

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _mongo_up() -> bool:
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            return True
    except OSError:
        return False


def _csv(with_micros: bool) -> bytes:
    lines = ["id,name,ts"]
    for i in range(1, 7):
        frac = f".0123{i}5" if with_micros else ".012"
        lines.append(f"{i},n{i},2024-03-10T02:30:0{i}{frac}Z")
    return ("\n".join(lines) + "\n").encode()


def _run(with_micros: bool) -> tuple[dict, list]:
    import pymongo
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="ts",
        source_type="TIMESTAMPTZ",
        destination_type="date",
        approved_by="risk-approver@example.com",
        reason="Accept BSON millisecond instants",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=True,
    ).to_dict()
    mappings = [
        {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 1.0},
        {"source": "name", "target": "name", "target_type": "string", "confidence": 1.0},
        {"source": "ts", "target": "ts", "target_type": "date", "confidence": 1.0,
         "risk_acknowledged": True, "risk_contract": contract},
    ]
    coll_name = "d232_narrow_" + uuid.uuid4().hex[:8]
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="seed.csv",
        source_content=_csv(with_micros),
        destination=EndpointConfig(
            kind="database", format="mongodb", host="localhost", port=27017,
            database="dataflow", table=coll_name,
        ),
        sync_mode="full_refresh_overwrite",
        mappings=mappings,
        column_types={"id": "INTEGER", "name": "VARCHAR", "ts": "TIMESTAMPTZ"},
        skip_preflight=True,
    )
    request.triggered_by = "risk-approver@example.com"
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    client = pymongo.MongoClient("localhost", 27017)
    try:
        docs = list(client["dataflow"][coll_name].find({}, {"_id": 0, "ts": 1}))
        client["dataflow"].drop_collection(coll_name)
    finally:
        client.close()
    assert result.success, result.error
    return dict(result.reconciliation or {}), docs


@pytest.mark.skipif(not _mongo_up(), reason="MongoDB not reachable on localhost:27017")
def test_measured_microseconds_into_bson_date_are_named_not_silently_matched():
    recon, docs = _run(with_micros=True)
    assert docs and all(d["ts"].microsecond % 1000 == 0 for d in docs)
    rounded = recon.get("carrier_rounded_columns") or []
    assert [c["column"] for c in rounded] == ["ts"], recon.get("message")
    assert rounded[0]["kept_fractional_digits"] == 3
    message = str(recon.get("message"))
    assert "carrier's granularity" in message
    assert "Row fidelity verified" not in message


@pytest.mark.skipif(not _mongo_up(), reason="MongoDB not reachable on localhost:27017")
def test_millisecond_cells_that_fit_the_carrier_are_not_reported_as_narrowed():
    recon, _docs = _run(with_micros=False)
    assert recon.get("passed") is True
    assert not recon.get("carrier_rounded_columns")
