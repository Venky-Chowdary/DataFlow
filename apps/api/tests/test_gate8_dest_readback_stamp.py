"""Gate-8 dest-engine COUNT stamp — never invent a population."""

from __future__ import annotations

from services.reconciliation import attach_dest_readback


def test_attach_dest_readback_stamps_count_and_checksum():
    out = attach_dest_readback(
        {
            "target_rows": 12,
            "target_rows_before": 4,
            "target_checksum": "abc123",
            "coverage": "full_checksum",
            "assurance_level": "full_checksum",
        }
    )
    rb = out["dest_readback"]
    assert rb["dest_count"] == 12
    assert rb["dest_count_before"] == 4
    assert rb["dest_checksum"] == "abc123"
    assert rb["source"] == "gate8_dest_readback"
    assert rb["coverage"] == "full_checksum"
    assert rb["assurance_level"] == "full_checksum"
    assert rb.get("population_proof") is None


def test_attach_dest_readback_skips_unmeasured_count():
    assert "dest_readback" not in attach_dest_readback({})
    assert "dest_readback" not in attach_dest_readback({"target_rows": None})
    assert "dest_readback" not in attach_dest_readback({"target_rows": -1})
    assert "dest_readback" not in attach_dest_readback({"target_rows": "n/a"})


def test_attach_dest_readback_does_not_invent_before_count():
    out = attach_dest_readback({"target_rows": 3, "target_checksum": "x"})
    assert out["dest_readback"]["dest_count"] == 3
    assert out["dest_readback"]["dest_count_before"] is None
