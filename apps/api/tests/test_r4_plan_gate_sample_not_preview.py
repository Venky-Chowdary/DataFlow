"""The plan's gates judge every sampled row, not the 25-row display preview.

``transfer_tools._sample_rows`` asked ``sample_connector_object`` for 50 rows
and fed the gates ``output["rows"]``, the operator preview capped at 25. Once
D62 routed SFTP through that sampler (S3/GCS/ADLS already were), a bad value
in row 30 of a 30-row CSV was invisible to plan_transfer: no gate named it,
the signed CAST_AND_CONTINUE contract was attached to nothing, and the job's
own Validate (which reads the file) blocked the load (D189 regression).

Real SFTP server (tests/sftp_test_server.py), saved connector, the same
sampler the plan uses.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


def _csv(rows: int) -> str:
    lines = ["id,name,amount"]
    for i in range(1, rows + 1):
        amount = "not-a-number" if i == rows else f"{i}.25"
        name = "" if i == 3 else f"name {i}"
        lines.append(f"{i},{name},{amount}")
    return "\n".join(lines) + "\n"


@pytest.fixture
def sftp_conn(tmp_path):
    pytest.importorskip("paramiko")
    from services.connector_store import create_connector, delete_connector, get_connector
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "sftp"
    root.mkdir()
    (root / "people.csv").write_text(_csv(30), encoding="utf-8")
    server, runner = start_sftp_server(str(root))
    cfg = server.endpoint_config("/people.csv")
    saved = create_connector({
        "name": f"sftp-gate-{uuid.uuid4().hex[:8]}", "type": "sftp", "role": "source",
        "host": cfg["host"], "port": cfg["port"], "username": cfg["username"],
        "password": cfg["password"], "database": cfg["database"], "host_key": cfg["host_key"],
    })
    try:
        yield get_connector(saved.id) or {"id": saved.id, "type": "sftp"}
    finally:
        runner.stop()
        delete_connector(saved.id)


def _as_dict(conn) -> dict:
    if isinstance(conn, dict):
        return conn
    return dict(getattr(conn, "model_dump", lambda: vars(conn))())


def test_plan_gate_sample_reaches_the_bad_row_past_the_preview(sftp_conn):
    from src.ai.copilot.transfer_tools import _sample_rows

    rows = _sample_rows(_as_dict(sftp_conn), "people.csv", limit=50)
    ids = [int(r["id"]) for r in rows]
    assert ids == list(range(1, 31)), ids
    assert rows[-1]["amount"] == "not-a-number"
    # NULL / empty distinction survives the sampler.
    assert rows[2]["name"] in (None, "")


def test_operator_preview_stays_capped(sftp_conn):
    from src.ai.copilot.query_tools import sample_connector_object

    conn = _as_dict(sftp_conn)
    res = sample_connector_object(
        connector_id=str(conn.get("id")), table="people.csv", limit=50, analyze=False
    )
    assert res.success, res.error
    assert len(res.output["rows"]) == 25
    assert res.output["row_count"] == 30
