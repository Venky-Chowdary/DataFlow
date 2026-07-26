"""Shared-path proofs: Mongo introspect≡execute, JSON flatten, resume slice, MySQL bool/JSON."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def test_mysql_bool_and_json_wire_from_mongo_strings():
    from connectors.mysql_writer import _to_mysql_value

    assert _to_mysql_value("false", "BOOLEAN") == 0
    assert _to_mysql_value("true", "TINYINT") == 1
    assert _to_mysql_value({"email": True}, "JSON") == '{"email":true}'
    assert _to_mysql_value("", "JSON") is None


def test_struct_flatten_uses_json_default_not_repr():
    from bson import ObjectId

    from services.json_intelligence import (
        STRUCT_POLICY_FLATTEN_TOP_LEVEL,
        apply_struct_policies_to_row,
    )

    oid = ObjectId()
    # Array under nested object is serialized via json.dumps(default=…)
    row = {"notifications": {"items": [{"id": oid, "ok": True}]}}
    out = apply_struct_policies_to_row(
        row, {"notifications": STRUCT_POLICY_FLATTEN_TOP_LEVEL}
    )
    items = out.get("notifications_items")
    assert items is not None
    text = items if isinstance(items, str) else str(items)
    assert "ObjectId(" not in text
    assert str(oid) in text


def test_mongo_introspect_expands_like_execute():
    from src.transfer.endpoint_intelligence import _attach_db_sample
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database",
        format="mongodb",
        database="demo",
        collection="users",
    )
    docs = [
        {"_id": "a1", "email": "a@x.com", "notifications": {"email": True, "sms": False}},
        {"_id": "a2", "email": "b@x.com", "notifications": {"email": False}},
    ]
    coll = MagicMock()
    coll.find.return_value.max_time_ms.return_value.limit.return_value = docs
    coll.estimated_document_count.return_value = 2
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db

    out: dict = {"connected": True, "message": ""}
    with (
        patch(
            "src.transfer.endpoint_intelligence.resolve_connector_config",
            return_value={"type": "mongodb", "database": "demo"},
        ),
        patch("src.transfer.endpoint_intelligence._mongo_client", return_value=client),
        patch(
            "src.transfer.endpoint_intelligence.mongodb_connection_string",
            return_value="mongodb://x",
        ),
    ):
        _attach_db_sample(out, endpoint, sample_limit=100)

    cols = out.get("columns") or []
    assert "email" in cols
    # Flattened nested keys must appear (execute path parity)
    assert any(c.startswith("notifications") for c in cols), cols
    sample = (out.get("sample_data") or [None])[0]
    assert isinstance(sample, dict)
    # Wire matrix uses strings, not raw nested dicts
    nested_keys = [k for k in sample if k.startswith("notifications")]
    assert nested_keys
    for k in nested_keys:
        assert not isinstance(sample[k], dict), sample[k]


def test_checkpoint_has_progress_and_resume_requires_it():
    from services.checkpoint_service import Checkpoint
    from src.transfer.engine import UniversalTransferEngine, _checkpoint_has_progress
    from src.transfer.models import EndpointConfig, TransferRequest

    assert not _checkpoint_has_progress(None)
    assert not _checkpoint_has_progress(Checkpoint(job_id="j"))
    assert _checkpoint_has_progress(Checkpoint(job_id="j", rows_processed=10))
    assert _checkpoint_has_progress(Checkpoint(job_id="j", offset=5))

    # Append/insert resume without progress must not silently restart from zero.
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="t"),
        source_filename="t.csv",
        source_content=b"id\n1\n",
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )
    with patch(
        "src.transfer.engine.CheckpointService.load",
        return_value=None,
    ), patch(
        "src.transfer.engine.get_mongodb_service",
    ) as mongo_fn:
        mongo = MagicMock()
        mongo.get_job.return_value = {}
        mongo_fn.return_value = mongo
        with pytest.raises(ValueError, match="No durable checkpoint"):
            UniversalTransferEngine()._execute_tracked_core(
                req, "job-no-cp", resume=True
            )


def test_analyze_coercion_mysql_json_wire_runs():
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[{"notifications": '{"email":true}'}],
        mappings=[
            {
                "source": "notifications",
                "target": "notifications",
                "target_type": "JSON",
                "transform": "json",
            }
        ],
        source_types={"notifications": "JSON"},
        dest_types={"notifications": "JSON"},
        dest_db_type="mysql",
    )
    assert report["sampled_rows"] == 1
    # Valid JSON should not block
    assert report["has_blocking_failures"] is False


def test_mark_dlq_promoted_mongodb_branch():
    from services.dest_quarantine import mark_dlq_promoted
    from src.transfer.models import EndpointConfig

    result_mock = MagicMock()
    result_mock.modified_count = 2
    coll = MagicMock()
    coll.update_many.return_value = result_mock
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db

    dest = EndpointConfig(
        kind="database",
        format="mongodb",
        database="demo",
        collection="users",
        host="localhost",
        port=27017,
    )
    with (
        patch(
            "src.transfer.adapters.resolve_connector_config",
            return_value={"type": "mongodb", "database": "demo", "host": "localhost", "port": 27017},
        ),
        patch("connectors.mongodb_common._mongo_client", return_value=client),
        patch(
            "connectors.mongodb_common.normalize_mongodb_connection_string",
            return_value="mongodb://localhost",
        ),
    ):
        out = mark_dlq_promoted(dest, qids=["q1", "q2"], job_id="job-1")
    assert out.get("updated") == 2
    assert out.get("driver") == "mongodb"
    coll.update_many.assert_called_once()
