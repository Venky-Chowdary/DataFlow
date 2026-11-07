"""An extra row added to the S3 object between write and reconciliation fails.

The overwrite leftover MERGE inside reconciliation rewrote the object this run
had just written to drop the extra key, then verified the repaired object as
10 rows: success=true, while what was written was never checked. Leftover
MERGE now leaves the run's own objects alone, so Gate-8 counts the extra row.
"""

from __future__ import annotations

import uuid

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_x5_file_destination_continuity import _s3_dest, s3  # noqa: F401
from tests.test_x5_same_run_artifact_readback import engine_module
from tests.test_x5_sftp_append_keeps_prior_rows import _source

KEY = "out/export.csv"


def _run(s3_ctx, tmp_path, monkeypatch, *, extra_row: bool):
    client, url, port = s3_ctx
    dest = _s3_dest(url, port, "out")
    dest.extra = {**(dest.extra or {}), "file_format": "csv"}
    source = EndpointConfig(
        kind="database", format="sqlite", database=str(_source(tmp_path, 1, 11)), table="t"
    )
    real = engine_module.run_reconciliation
    seen: dict = {}

    def patched(*a, **k):
        if extra_row:
            lines = client.get_object(Bucket="x5bucket", Key=KEY)["Body"].read().decode().splitlines()
            idx = lines[0].split(",").index("id")
            row = lines[1].split(",")
            row[idx] = "99"
            lines.append(",".join(row))
            client.put_object(Bucket="x5bucket", Key=KEY, Body=("\n".join(lines) + "\n").encode())
        seen["checked"] = client.get_object(Bucket="x5bucket", Key=KEY)["Body"].read()
        return real(*a, **k)

    monkeypatch.setattr(engine_module, "run_reconciliation", patched)
    req = TransferRequest(
        source=source, destination=dest, sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        stream_contracts=[{
            "name": "t", "selected": True, "primary_key": ["id"],
            "sync_mode": "full_refresh_overwrite",
        }],
    )
    return UniversalTransferEngine().execute_tracked(req, "xr-" + uuid.uuid4().hex[:8]), seen


def test_extra_row_added_before_reconciliation_fails_the_run(s3, tmp_path, monkeypatch):  # noqa: F811
    client = s3[0]
    result, seen = _run(s3, tmp_path, monkeypatch, extra_row=True)
    assert not result.success, "an extra row in the written object must fail the run"
    assert "extra" in (result.error or "").lower(), result.error
    keys = [o["Key"] for o in client.list_objects_v2(Bucket="x5bucket").get("Contents", [])]
    if KEY in keys:
        # Never repaired behind the operator's back.
        assert client.get_object(Bucket="x5bucket", Key=KEY)["Body"].read() == seen["checked"]


def test_clean_overwrite_still_passes(s3, tmp_path, monkeypatch):  # noqa: F811
    result, _seen = _run(s3, tmp_path, monkeypatch, extra_row=False)
    assert result.success, result.error


@pytest.mark.parametrize("protected", [False, True])
def test_leftover_merge_skips_objects_this_run_wrote(protected, s3):  # noqa: F811
    client, url, port = s3
    from services.row_conservation import apply_inferred_leftover_deletes

    body = b"id,v\n1,a\n2,b\n3,c\n99,ghost\n"
    client.put_object(Bucket="x5bucket", Key="lo/export.csv", Body=body)
    cfg = {
        "host": "127.0.0.1", "port": port, "database": "x5bucket", "username": "x5key",
        "password": "x5secret", "connection_string": url, "path_style": True,
        "file_format": "csv",
    }
    if protected:
        from connectors.object_store_leftover import LEFTOVER_PROTECTED_OBJECTS

        cfg[LEFTOVER_PROTECTED_OBJECTS] = ["lo/export.csv"]
    deleted = apply_inferred_leftover_deletes(
        db_type="s3", cfg=cfg, schema="", table_name="lo", key_columns=["id"],
        keys=[("1",), ("2",), ("3",)], complete_snapshot=True,
        sync_mode="full_refresh_overwrite",
    )
    after = client.get_object(Bucket="x5bucket", Key="lo/export.csv")["Body"].read()
    if protected:
        assert not deleted and after == body
    else:
        assert deleted == 1 and b"ghost" not in after


def test_changed_cell_message_names_row_without_census_clause(s3, tmp_path, monkeypatch):  # noqa: F811
    client = s3[0]
    real = engine_module.run_reconciliation

    def patched(*a, **k):
        lines = client.get_object(Bucket="x5bucket", Key=KEY)["Body"].read().decode().splitlines()
        head = lines[0].split(",")
        for i, line in enumerate(lines[1:], start=1):
            row = line.split(",")
            if row[head.index("id")] == "3":
                row[head.index("v")] = "TAMPERED"
                lines[i] = ",".join(row)
        client.put_object(Bucket="x5bucket", Key=KEY, Body=("\n".join(lines) + "\n").encode())
        return real(*a, **k)

    monkeypatch.setattr(engine_module, "run_reconciliation", patched)
    dest = _s3_dest(s3[1], s3[2], "out")
    dest.extra = {**(dest.extra or {}), "file_format": "csv"}
    source = EndpointConfig(
        kind="database", format="sqlite", database=str(_source(tmp_path, 1, 11)), table="t"
    )
    req = TransferRequest(
        source=source, destination=dest, sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        stream_contracts=[{
            "name": "t", "selected": True, "primary_key": ["id"],
            "sync_mode": "full_refresh_overwrite",
        }],
    )
    result = UniversalTransferEngine().execute_tracked(req, "xc-" + uuid.uuid4().hex[:8])
    err = result.error or ""
    assert not result.success
    assert "row 3 column v" in err and "TAMPERED" in err, err
    assert "culprit keys could not be named" not in err, err


def test_checksum_only_failure_names_the_changed_row_beyond_the_sample(s3, tmp_path, monkeypatch):  # noqa: F811
    client = s3[0]
    real = engine_module.run_reconciliation

    def patched(*a, **k):
        lines = client.get_object(Bucket="x5bucket", Key=KEY)["Body"].read().decode().splitlines()
        head = lines[0].split(",")
        for i, line in enumerate(lines[1:], start=1):
            row = line.split(",")
            if row[head.index("id")] == "4993":
                row[head.index("v")] = "FAR"
                lines[i] = ",".join(row)
        client.put_object(Bucket="x5bucket", Key=KEY, Body=("\n".join(lines) + "\n").encode())
        return real(*a, **k)

    monkeypatch.setattr(engine_module, "run_reconciliation", patched)
    dest = _s3_dest(s3[1], s3[2], "out")
    dest.extra = {**(dest.extra or {}), "file_format": "csv"}
    source = EndpointConfig(
        kind="database", format="sqlite", database=str(_source(tmp_path, 1, 5001)), table="t"
    )
    req = TransferRequest(
        source=source, destination=dest, sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        stream_contracts=[{
            "name": "t", "selected": True, "primary_key": ["id"],
            "sync_mode": "full_refresh_overwrite",
        }],
    )
    result = UniversalTransferEngine().execute_tracked(req, "xf-" + uuid.uuid4().hex[:8])
    err = result.error or ""
    assert not result.success
    assert "row 4993 column v" in err and "FAR" in err, err
    assert "No differing cell was found" not in err, err
