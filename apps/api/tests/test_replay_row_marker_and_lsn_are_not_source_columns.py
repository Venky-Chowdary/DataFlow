"""A quarantine replay judges source columns, not the keys a quarantined row carries.

A finding that refused the whole row (a destination CHECK, a unique key: the
writers record ``"column": "*"``) and a CDC image (the writer stamps the
``_df_lsn`` it was read at) put those keys on the replayed records. The replay
runs without Validate, so it applies G13 itself (source_column_coverage); it
counted ``*`` and ``_df_lsn`` as source columns nobody had decided on and
refused every such replay with "2 source column(s) are neither mapped nor
declared omitted: _df_lsn, *" (train 2 CI: test_cdc_check_refusal_holdout,
test_quarantine_replay_cross_run_stale_cdc, test_d294_d401_replay_live_pg_mysql).

A real source column the map leaves out still refuses the replay.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient


def _quarantined_run(tmp_path: Path) -> tuple[TestClient, str, Path, list[dict]]:
    from src.main import app
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest = tmp_path / "replay_marker.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="users",
                                   connection_string=f"sqlite:///{dest}", database=str(dest)),
        source_filename="users.csv", source_content=b"id,age\n1,30\n2,not-a-number\n",
        sync_mode="full_refresh_overwrite", skip_preflight=True, validation_mode="balanced",
        mappings=[{"source": "id", "target": "id", "confidence": 0.95},
                  {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"}],
        column_types={"id": "string", "age": "string"},
    )
    result = UniversalTransferEngine().execute(request)
    assert result.success is True, result.error
    client = TestClient(app)
    findings = client.get(f"/api/v1/connectors/jobs/{result.job_id}/quarantine").json()["quarantine"]
    assert len(findings) == 1, findings
    return client, str(result.job_id), dest, findings


def _whole_row(finding: dict, values: dict) -> dict:
    """The finding as a writer records a refusal of the whole row, with the edit."""
    return {**finding, "column": "*", "value": "25", "values": dict(values),
            "source_values": {k: v for k, v in values.items() if k != "_df_lsn"}}


def _ages(dest: Path) -> dict[int, int]:
    with sqlite3.connect(dest) as db:
        return {int(i): int(a) for i, a in db.execute("SELECT id, age FROM users")}


def test_a_whole_row_finding_replays(tmp_path: Path) -> None:
    client, job_id, dest, findings = _quarantined_run(tmp_path)
    rows = [_whole_row(findings[0], {"id": "2", "age": "25"})]
    replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": rows})
    assert replay.status_code == 200, replay.text
    assert replay.json()["rows_written"] == 1
    assert _ages(dest) == {1: 30, 2: 25}


def _as_cdc_route(job_id: str) -> None:
    """The parent job as a CDC route stores it (the stamp is a CDC writer's)."""
    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    saved = dict(store.get_job(job_id)["transfer_request"])
    saved["sync_mode"] = "cdc_incremental"
    store.update_job_fields(job_id, {"transfer_request": saved})


def test_a_cdc_image_with_its_lsn_stamp_replays(tmp_path: Path) -> None:
    client, job_id, dest, findings = _quarantined_run(tmp_path)
    _as_cdc_route(job_id)
    with sqlite3.connect(dest) as db:  # a CDC destination keeps the LSN it applied
        db.execute("ALTER TABLE users ADD COLUMN _df_lsn TEXT")
    rows = [_whole_row(findings[0], {"id": "2", "age": "25", "_df_lsn": "0/16B3748"})]
    replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": rows})
    assert replay.status_code == 200, replay.text
    assert _ages(dest) == {1: 30, 2: 25}


def test_a_source_column_the_map_leaves_out_still_refuses_the_replay(tmp_path: Path) -> None:
    client, job_id, dest, findings = _quarantined_run(tmp_path)
    rows = [_whole_row(findings[0], {"id": "2", "age": "25", "nickname": "bo"})]
    replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": rows})
    assert replay.status_code == 409, replay.text
    detail = replay.json()["detail"]
    assert "neither mapped nor declared omitted: nickname" in detail, detail
    assert "*" not in detail.split("omitted:", 1)[1].split("—", 1)[0], detail
    assert _ages(dest) == {1: 30}


def test_a_real_df_lsn_source_column_on_a_batch_route_is_still_judged(tmp_path: Path) -> None:
    """Outside CDC, ``_df_lsn`` is a source column (a table a CDC pipeline
    filled, copied on in batch): leaving it unmapped refuses the replay by name
    instead of dropping it silently."""
    client, job_id, dest, findings = _quarantined_run(tmp_path)
    with sqlite3.connect(dest) as db:
        db.execute("ALTER TABLE users ADD COLUMN _df_lsn TEXT")
    rows = [_whole_row(findings[0], {"id": "2", "age": "25", "_df_lsn": "0/16B3748"})]
    rows[0]["source_values"] = {"id": "2", "age": "25", "_df_lsn": "0/16B3748"}
    replay = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={"rows": rows})
    assert replay.status_code == 409, replay.text
    assert "neither mapped nor declared omitted: _df_lsn" in replay.json()["detail"]
    assert _ages(dest) == {1: 30}
