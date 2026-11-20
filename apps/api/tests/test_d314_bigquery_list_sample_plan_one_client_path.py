"""D314 / D387: a BigQuery connector that tests green lists, samples and plans.

QA (service-account BigQuery, project ``datawrap-qa``, host empty):
``test_connector`` answered "BigQuery connected - 1 tables", but
``sample_connector_object`` failed "SQLAlchemy dialect/driver for bigquery is
not available" and ``plan_transfer`` failed. Three faults on one route:

* the sample went through the SQL query console (a SQLAlchemy dialect the image
  does not ship) instead of the BigQuery reader;
* the plan's source introspection dropped the connector's service-account key,
  so it refused "no service-account key of its own";
* every reader query named ``datawrap_qa.<dataset>.<table>``: the project ID's
  hyphen was "sanitized" to an underscore. A GCP project ID is lowercase
  letters, digits and hyphens
  (https://cloud.google.com/resource-manager/docs/creating-managing-projects),
  so ``datawrap_qa`` is another, invalid project.

No Google account here. The fake below answers the BigQuery REST v2 calls the
``google-cloud-bigquery`` client makes (``tables.list``, ``tables.get``,
``jobs.insert``, ``jobs.getQueryResults``) with the documented JSON shapes
(https://cloud.google.com/bigquery/docs/reference/rest), at the client's
``_call_api`` seam, so the client's own URL building, credential handling and
row decoding run for real. Every call records the API base URL it was sent to.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

bigquery = pytest.importorskip("google.cloud.bigquery")

PROJECT = "datawrap-qa"
DATASET = "dfqa"
TABLE = "dfqa_src"
GOOGLE_API = "https://bigquery.googleapis.com"
SCHEMA = {
    "fields": [
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    ]
}
ROWS = [(1, "a"), (2, None), (3, "")]


def _service_account_json() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return json.dumps({
        "type": "service_account", "project_id": PROJECT, "private_key_id": "k1",
        "private_key": pem, "client_email": f"sa@{PROJECT}.iam.gserviceaccount.com",
        "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token",
    })


class _FakeBigQuery:
    """BigQuery REST v2 answers for one dataset with one table."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.sql: list[str] = []
        self._jobs: dict[str, str] = {}

    def _ref(self) -> dict:
        return {"projectId": PROJECT, "datasetId": DATASET, "tableId": TABLE}

    def _rows(self, rows: list[tuple]) -> list[dict]:
        return [{"f": [{"v": None if v is None else str(v)} for v in r]} for r in rows]

    def _answer_query(self, sql: str) -> dict:
        target = f"`{PROJECT}.{DATASET}.{TABLE}`"
        if target not in sql:
            # BigQuery: "Not found: Project datawrap_qa" for a misspelt project.
            raise AssertionError(f"query names another table than {target}: {sql}")
        if re.match(r"\s*SELECT COUNT\(\*\)", sql, re.I):
            return {
                "schema": {"fields": [{"name": "cnt", "type": "INTEGER", "mode": "NULLABLE"}]},
                "rows": self._rows([(len(ROWS),)]), "totalRows": "1",
            }
        limit = re.search(r"LIMIT (\d+)", sql)
        offset = re.search(r"OFFSET (\d+)", sql)
        lo = int(offset.group(1)) if offset else 0
        hi = lo + int(limit.group(1)) if limit else len(ROWS)
        page = ROWS[lo:hi]
        return {"schema": SCHEMA, "rows": self._rows(page), "totalRows": str(len(page))}

    def call_api(self, client, retry, span_name=None, span_attributes=None,
                 job_ref=None, headers=None, **kw):
        base = client._connection.API_BASE_URL
        method, path = kw.get("method"), kw.get("path")
        self.calls.append((base, method, path))
        root = f"/projects/{PROJECT}"
        if path == f"{root}/datasets/{DATASET}/tables":
            return {"tables": [{"tableReference": self._ref(), "type": "TABLE"}]}
        if path == f"{root}/datasets/{DATASET}/tables/{TABLE}":
            return {"tableReference": self._ref(), "schema": SCHEMA,
                    "numRows": str(len(ROWS)), "type": "TABLE"}
        if path == f"{root}/datasets/{DATASET}/tables/{TABLE}/data":
            return {"totalRows": str(len(ROWS)), "rows": self._rows(ROWS)}
        if method == "POST" and path == f"{root}/jobs":
            sql = kw["data"]["configuration"]["query"]["query"]
            self.sql.append(sql)
            job_id = f"job{len(self._jobs)}"
            self._jobs[job_id] = sql
            return {
                "jobReference": {"projectId": PROJECT, "jobId": job_id, "location": "US"},
                "status": {"state": "DONE"},
                "configuration": kw["data"]["configuration"],
                "statistics": {"query": {"statementType": "SELECT"}},
            }
        m = re.fullmatch(rf"{root}/queries/(job\d+)", path or "")
        if m:
            return {"jobComplete": True,
                    "jobReference": {"projectId": PROJECT, "jobId": m.group(1), "location": "US"},
                    **self._answer_query(self._jobs[m.group(1)])}
        m = re.fullmatch(rf"{root}/jobs/(job\d+)", path or "")
        if m:
            return {"jobReference": {"projectId": PROJECT, "jobId": m.group(1), "location": "US"},
                    "status": {"state": "DONE"},
                    "configuration": {"query": {"query": self._jobs[m.group(1)]}}}
        raise AssertionError(f"unexpected BigQuery API call {method} {path}")


@pytest.fixture
def fake_bq(monkeypatch):
    fake = _FakeBigQuery()

    def _call_api(self, retry, *args, **kwargs):
        return fake.call_api(self, retry, *args, **kwargs)

    monkeypatch.setattr(bigquery.Client, "_call_api", _call_api)
    return fake


@pytest.fixture
def bq_connector(fake_bq):
    from services.connector_store import create_connector, delete_connector

    conn = create_connector({
        "name": f"d314-bq-{uuid.uuid4().hex[:6]}", "type": "bigquery", "host": "",
        "database": PROJECT, "schema": DATASET, "service_account": _service_account_json(),
    })
    fake_bq.calls.clear()
    yield conn
    delete_connector(conn.id)


def _only_google(fake: _FakeBigQuery) -> None:
    assert fake.calls, "no BigQuery API call was made"
    assert {base for base, _m, _p in fake.calls} == {GOOGLE_API}


def test_project_id_hyphens_are_kept_in_query_references():
    from connectors.sql_identifiers import quote_table_ref

    assert quote_table_ref("t", dialect="bigquery", project="datawrap-qa", dataset="ds") == (
        "`datawrap-qa.ds.t`"
    )
    assert quote_table_ref(
        "t", dialect="bigquery", project="example.com:my-proj", dataset="ds"
    ) == "`example.com:my-proj.ds.t`"


def test_test_list_and_sample_use_the_same_google_client(fake_bq, bq_connector):
    from src.ai.copilot.query_tools import sample_connector_object
    from src.ai.copilot.schema_tools import list_connector_objects
    from src.ai.copilot.tools import get_pilot_tools

    tested = get_pilot_tools().execute("test_connector", {"connector_id": bq_connector.id})
    assert tested.success and tested.output["ok"], tested.error
    listed = list_connector_objects(connector_id=bq_connector.id)
    assert listed.success, listed.error
    assert listed.output["objects"] == [TABLE]
    sampled = sample_connector_object(
        connector_id=bq_connector.id, table=TABLE, limit=5, analyze=False
    )
    assert sampled.success, sampled.error
    assert sampled.output["column_type_source"] == "reader"
    rows = {r["id"]: r["name"] for r in sampled.output["rows"]}
    assert rows == {"1": "a", "2": None, "3": ""}
    _only_google(fake_bq)
    assert fake_bq.sql and all(f"`{PROJECT}.{DATASET}.{TABLE}`" in q for q in fake_bq.sql)


def test_plan_and_run_read_with_the_connectors_own_key(fake_bq, bq_connector, tmp_path):
    from conftest import spend_pilot_ack
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools

    db = tmp_path / "d314.db"
    dst = create_connector({"name": f"d314-dst-{uuid.uuid4().hex[:6]}", "type": "sqlite",
                            "database": str(db)})
    args = {
        "source_connector_id": bq_connector.id, "source_table": TABLE,
        "dest_connector_id": dst.id, "dest_table": "bq_copy",
        "sync_mode": "full_refresh_overwrite",
    }
    try:
        plan = get_pilot_tools().execute("plan_transfer", args)
        assert plan.success, plan.error
        staged = get_pilot_tools().execute("start_transfer", args)
        assert staged.success, staged.error
        job_id = spend_pilot_ack(staged.output["ack_id"], "d314-test")["job_id"]
        deadline = time.time() + 120
        job: dict = {}
        while time.time() < deadline:
            job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
            if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
                break
            time.sleep(0.5)
        assert job.get("status") == "completed", job
    finally:
        delete_connector(dst.id)
    _only_google(fake_bq)
    import sqlite3

    with sqlite3.connect(db) as conn:
        got = sorted(conn.execute("SELECT id, name FROM bq_copy").fetchall())
    assert got == [(1, "a"), (2, None), (3, "")]
