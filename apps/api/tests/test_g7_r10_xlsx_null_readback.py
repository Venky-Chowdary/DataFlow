"""G7 re-review F1: an XLSX NULL reads back as NULL, an empty string as ''.

The object-store XLSX encoder writes SQL NULL as no cell and ``''`` as an
empty string cell. ``excel_parser.iter_excel_dicts`` turned every blank into
``''`` and dropped trailing missing cells, so Gate-8 compared ``''`` with a
source NULL and failed the run (base: the cell held ``__DF_SQL_NULL__``).
Gate-8's destination read-back now reads a header column with no cell as
None and keeps an empty string cell as ``''``. Source ingest
(``iter_excel_dicts`` default) is unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _workbook() -> bytes:
    from connectors.object_store_materialize import ObjectStoreEncoder

    enc = ObjectStoreEncoder(
        key="out/r.xlsx",
        target_cols=["id", "note", "amount"],
        dest_types={"id": "TEXT", "note": "TEXT", "amount": "NUMERIC(12,2)"},
    )
    enc.append_rows([("1", None, "10.50"), ("2", "", "20.25"), ("3", "x", None)])
    return enc.finish().read_all()


def test_destination_read_back_keeps_null_and_empty_apart(tmp_path):
    from services.dest_precount import _iter_artifact_records

    path = tmp_path / "r.xlsx"
    path.write_bytes(_workbook())
    with open(path, "rb") as fh:
        rows = list(_iter_artifact_records(fh, name="r.xlsx", fmt="excel"))
    assert rows == [
        {"id": "1", "note": None, "amount": "10.5"},
        {"id": "2", "note": "", "amount": "20.25"},
        {"id": "3", "note": "x", "amount": None},
    ]


def test_source_ingest_reading_is_unchanged():
    """Uploads / file sources keep their blank-cell behaviour."""
    from services.excel_parser import iter_excel_dicts

    rows = list(iter_excel_dicts(_workbook()))
    assert rows == [
        {"id": "1", "note": "", "amount": "10.5"},
        {"id": "2", "note": "", "amount": "20.25"},
        {"id": "3", "note": "x"},
    ]


def test_sftp_xlsx_with_null_and_empty_passes_gate8(local_sftp):
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    import uuid

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    cfg = local_sftp.endpoint_config("/nulls.xlsx")
    destination = EndpointConfig(
        kind="database", format="sftp", host=cfg["host"], port=cfg["port"],
        username=cfg["username"], password=cfg["password"], database=cfg["database"],
        table=cfg["table"], extra={"host_key": cfg["host_key"]},
    )
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=destination,
        source_filename="in.csv",
        source_content=b'id,note,amount\n1,,10.50\n2,"",20.25\n3,x,30.00\n',
        mappings=[{"source": c, "target": c, "confidence": 0.99} for c in ("id", "note", "amount")],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    recon = result.reconciliation or {}
    assert recon.get("passed") is True, recon
