"""SaaS / Kafka Gate-8: sample-verified phase + verify routing."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_stamp_sample_verified_phase():
    from services.reconciliation import stamp_post_write_phase

    out = stamp_post_write_phase(
        {
            "passed": True,
            "message": "Gate-8 sample-verified 4 key-aligned field(s) for 'hubspot'",
            "source_checksum": "abc",
            "target_checksum": "",
            "sample_compare": {"passed": True, "compared": 4},
        }
    )
    assert out["phase"] == "post_write_sample_verified"


def test_run_reconciliation_sample_verified_when_no_full_verifier():
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import run_reconciliation

    endpoint = EndpointConfig(kind="database", format="hubspot", table="contacts")
    sample = [{"email": "a@x.com", "firstname": "A"}]
    with patch(
        "src.transfer.reconcile_step.resolve_connector_config",
        return_value={"type": "hubspot"},
    ), patch(
        "src.transfer.reconcile_step.verify_target",
        return_value=(-1, ""),
    ), patch(
        "src.transfer.reconcile_step.read_target_sample",
        return_value=sample,
    ):
        report = run_reconciliation(
            endpoint=endpoint,
            records=sample,
            columns=["email", "firstname"],
            rows_written=1,
            writer_checksum="w",
            dest_summary={
                "table": "contacts",
                "sync_mode": "incremental_append",
                "reconcile_sample": sample,
                "source_row_count": 1,
            },
            mappings=[
                {"source": "email", "target": "email"},
                {"source": "firstname", "target": "firstname"},
            ],
            validation_mode="balanced",
        )
    assert report["passed"] is True, report
    assert report["phase"] == "post_write_sample_verified"
    assert "sample-verified" in report["message"].lower()


def test_verify_target_routes_hubspot_and_kafka():
    from services.reconciliation import verify_target

    with patch(
        "services.reconciliation.verify_hubspot_object",
        return_value=(2, "hs"),
    ) as hs:
        assert verify_target(
            "hubspot",
            {"password": "tok"},
            schema="",
            table_name="contacts",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (2, "hs")
        hs.assert_called_once()

    with patch(
        "services.reconciliation.verify_kafka_topic",
        return_value=(5, "k"),
    ) as kf:
        assert verify_target(
            "kafka",
            {"host": "localhost"},
            schema="",
            table_name="events",
            fallback_rows=-1,
            fallback_checksum="",
        ) == (5, "k")
        kf.assert_called_once()


def test_hubspot_writer_meta_reconcile_sample():
    from connectors.writer_common import WriteResult

    # Smoke: WriteResult accepts meta (HubSpot stamps reconcile_sample there).
    r = WriteResult(
        ok=True,
        rows_written=1,
        table_name="contacts",
        target_schema="",
        checksum="x",
        chunks_completed=1,
        meta={"reconcile_sample": [{"email": "a@x.com"}], "written_ids": ["1"]},
    )
    assert r.meta["written_ids"] == ["1"]


def test_writer_diagnostics_promotes_meta():
    from src.transfer.adapters import _writer_diagnostics
    from connectors.writer_common import WriteResult

    d = _writer_diagnostics(
        WriteResult(
            ok=True,
            rows_written=1,
            table_name="t",
            target_schema="",
            checksum="c",
            chunks_completed=1,
            meta={"reconcile_sample": [{"id": "1"}], "source_row_count": 1},
        )
    )
    assert d["reconcile_sample"] == [{"id": "1"}]
    assert d["source_row_count"] == 1
