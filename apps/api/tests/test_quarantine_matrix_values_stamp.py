"""Write-quarantine matrix must stamp full mapped ``values`` for replay."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import apply_write_quarantine_matrix  # noqa: E402


def test_decimal_holdout_stamps_full_mapped_values():
    details: list[dict] = []
    rows = [
        ("1", "10.5", "ok"),
        ("2", "999999999999999999999", "keep-me"),
    ]
    out = apply_write_quarantine_matrix(
        rows,
        ["id", "amount", "note"],
        ["INTEGER", "DECIMAL(10,2)", "VARCHAR(20)"],
        details,
        "quarantine",
        dialect_label="postgres",
    )
    assert len(out) == 1
    assert out[0][0] == "1"
    assert details, "expected quarantine detail"
    d = details[0]
    assert isinstance(d.get("values"), dict)
    assert d["values"]["id"] == "2"
    assert d["values"]["note"] == "keep-me"
    assert "amount" in d["values"]


def test_all_quarantined_file_transfer_keeps_details(tmp_path: Path):
    """Single unfit row must not be mislabeled 'No records found in file'."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest = tmp_path / "all_q.db"
    result = UniversalTransferEngine().execute(
        TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database",
                format="sqlite",
                table="ages",
                connection_string=f"sqlite:///{dest}",
                database=str(dest),
            ),
            source_filename="ages.csv",
            source_content=b"id,age\n1,bad\n",
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {
                    "source": "age",
                    "target": "age",
                    "confidence": 0.95,
                    "target_type": "integer",
                },
            ],
            column_types={"id": "string", "age": "string"},
        )
    )
    assert result.success is True
    assert int(result.destination_summary.get("rejected_rows") or 0) >= 1
    details = result.destination_summary.get("rejected_details") or []
    assert details
    assert isinstance(details[0].get("values"), dict)
    assert "id" in details[0]["values"] or "age" in details[0]["values"]
