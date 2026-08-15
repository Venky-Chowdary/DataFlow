"""Streaming write-pass fingerprints are remapped source, not writer-ack theatre."""

from __future__ import annotations

from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import run_reconciliation


def test_inline_write_pass_is_remapped_not_writer_ack(monkeypatch):
    dest = EndpointConfig(kind="database", format="postgresql", table="customer")

    def fake_verify(*_a, **_k):
        return 150_000, "abc123independent"

    monkeypatch.setattr("src.transfer.reconcile_step.verify_target", fake_verify)
    monkeypatch.setattr(
        "src.transfer.reconcile_step._engine_digest_enabled",
        lambda: False,
    )
    monkeypatch.setattr(
        "src.transfer.reconcile_step._writer_supplied_engine_digests",
        lambda *_a, **_k: None,
    )

    report = run_reconciliation(
        endpoint=dest,
        records=[],
        columns=["c_custkey"],
        rows_written=150_000,
        writer_checksum="abc123independent",
        dest_summary={
            "source_row_count": 150_000,
            "checksum_mode": "inline_write_pass",
            "dest_count_before": 0,
            "sync_mode": "full_refresh_append",
        },
        mappings=[{"source": "C_CUSTKEY", "target": "c_custkey"}],
        source_schema={"C_CUSTKEY": "DECIMAL(38,0)"},
        validation_mode="strict",
    )
    assert report["source_checksum_provenance"] == "remapped_source_rows"
    assert report["source_checksum"] == "abc123independent"
    assert report.get("assurance_level") in {"full_checksum", "row_count"}
    assert not str(report.get("message") or "").lower().startswith("row fidelity verified") or (
        report.get("assurance_level") == "full_checksum"
    )


def test_writer_ack_message_does_not_claim_row_fidelity(monkeypatch):
    dest = EndpointConfig(kind="database", format="postgresql", table="customer")

    def fake_verify(*_a, **_k):
        return 150_000, ""

    monkeypatch.setattr("src.transfer.reconcile_step.verify_target", fake_verify)
    monkeypatch.setattr(
        "src.transfer.reconcile_step._engine_digest_enabled",
        lambda: False,
    )
    monkeypatch.setattr(
        "src.transfer.reconcile_step._writer_supplied_engine_digests",
        lambda *_a, **_k: None,
    )

    report = run_reconciliation(
        endpoint=dest,
        records=[],
        columns=["c_custkey"],
        rows_written=150_000,
        writer_checksum="writeronly",
        dest_summary={
            "source_row_count": 150_000,
            "checksum_mode": "writer_last_batch",
            "sync_mode": "full_refresh_append",
        },
        mappings=[{"source": "C_CUSTKEY", "target": "c_custkey"}],
        validation_mode="strict",
    )
    assert report.get("assurance_level") == "writer_ack"
    assert "row fidelity verified" not in str(report.get("message") or "").lower()
