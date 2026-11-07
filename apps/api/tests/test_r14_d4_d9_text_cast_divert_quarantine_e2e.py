"""QA MySQL-D4 / Snowflake-D9 — cast text to integer with on_error=divert.

A text ``ratio`` holding ``'abc'`` and ``''`` cast to integer with
``on_error=divert`` must divert exactly those rows to quarantine and load the
rest, the transform proof must verify the loaded rows (a diverted row is not
"cast_column did not apply"), the table the run created must stay, and the
quarantine view must answer — it returned HTTP 500 (``unhashable type:
'dict'``: a divert finding carries its record in ``row``).

Routes: MySQL -> PostgreSQL, MySQL -> MySQL (live), PostgreSQL -> Snowflake
(fakesnow), strict and lenient, each run twice into the table the first run
created. NULL, whitespace-padded and unicode cells ride along.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

from tests import r4_route_harness as h
from tests import test_r12_snowflake_transform_e2e as sfmod
from tests.test_d120_snowflake_strict_e2e import snowflake_semantics  # noqa: F401
from tests.test_r12_snowflake_transform_e2e import client  # noqa: F401

pytestmark = pytest.mark.timeout(600)

ROWS = [
    (1, "3", "plain"),
    (2, "abc", "x"),
    (3, "", "y"),
    (4, None, "z"),
    (5, "7", "Zoë 東京 ✓"),
    (6, " 8 ", "w"),
]
RECIPE = {"steps": [{
    "op": "cast_column", "column": "ratio", "options": {"to_type": "integer"},
    "on_error": "divert",
}]}
LOADED = {1: (3, "plain"), 4: (None, "z"), 5: (7, "Zoë 東京 ✓"), 6: (8, "w")}
DIVERTED = {2: "abc", 3: ""}


def _mysql_endpoint(prefix: str) -> dict[str, str]:
    return {
        f"{prefix}_host": "127.0.0.1", f"{prefix}_port": "3306",
        f"{prefix}_database": os.environ.get("MYSQL_DATABASE", "dataflow"),
        f"{prefix}_username": os.environ.get("MYSQL_USER", "dataflow"),
        f"{prefix}_password": os.environ.get("MYSQL_PASSWORD", ""),
    }


def _seed_mysql() -> str:
    table = f"r14d4_{uuid.uuid4().hex[:6]}"
    conn = h.mysql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE TABLE `{table}` (id int PRIMARY KEY, ratio varchar(20), "  # nosec B608
            "label varchar(40)) CHARSET utf8mb4"
        )
        cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s, %s)", ROWS)  # nosec B608
    finally:
        conn.close()
    return table


def _run_from_mysql(api, src: str, dst: str, dest_format: str, mode: str) -> dict:
    data = {
        "source_kind": "database", "dest_kind": "database",
        "source_table": src, "dest_table": dst, "async_mode": "false",
        "validation_mode": mode, "sync_mode": "full_refresh_overwrite",
        "source_format": "mysql", **_mysql_endpoint("source"),
        "dest_format": dest_format,
    }
    if dest_format == "postgresql":
        data.update(dest_schema="public", **sfmod._side("dest", sfmod.PG_ENDPOINT))
    else:
        data.update(_mysql_endpoint("dest"))
    for key, value in sfmod._shaped(RECIPE).items():
        data[key] = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
    resp = api.post("/api/v1/transfer/run", data=data)
    assert resp.status_code == 200, resp.text[:1500]
    return h.wait_job(str(resp.json().get("job_id") or ""))


def _assert_quarantine(api, job: dict) -> None:
    job_id = str(job.get("job_id") or job.get("_id") or job.get("id") or "")
    resp = api.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
    assert resp.status_code == 200, resp.text[:800]
    body = resp.json()
    assert body["rejected_rows"] == 2 and body["finding_rows"] == 2, body
    assert body["source"] == "write", body["source"]
    got = {}
    for row in body["quarantine"]:
        assert row["column"] == "ratio", row
        assert "is not a number" in row["reason"], row
        got[int(row["row"]["id"])] = row["value"]
    assert got == DIVERTED, got


def _assert_job(job: dict, run: int) -> None:
    assert job.get("status") == "completed_with_quarantine", (
        run, job.get("status"), job.get("error"), job.get("message"))
    proof = sfmod._recon(job).get("transform_applied") or {}
    assert proof.get("status") == "verified", (run, proof)
    assert "did not apply" not in str(job.get("message") or ""), job.get("message")


@pytest.mark.parametrize("mode", ["strict", "lenient"])
@pytest.mark.parametrize("dest_format", ["postgresql", "mysql"])
def test_mysql_text_cast_divert_loads_the_rest_and_quarantine_answers(client, dest_format, mode):  # noqa: F811
    src = _seed_mysql()
    dst = f"r14d4d_{uuid.uuid4().hex[:6]}"
    try:
        for run in (1, 2):
            job = _run_from_mysql(client, src, dst, dest_format, mode)
            _assert_job(job, run)
            _assert_quarantine(client, job)
            if dest_format == "postgresql":
                rows = h.query("postgresql", f'SELECT id, ratio, label FROM public."{dst}"')  # nosec B608
            else:
                rows = h.query("mysql", f"SELECT id, ratio, label FROM `{dst}`")  # nosec B608
            got = {int(r[0]): (None if r[1] is None else int(r[1]), r[2]) for r in rows}
            assert got == LOADED, (run, got)
    finally:
        h.drop_table("mysql", src)
        h.drop_table(dest_format, dst)


@pytest.mark.parametrize("mode", ["strict", "lenient"])
def test_pg_text_cast_divert_into_snowflake_keeps_the_table(client, mode):  # noqa: F811
    src = sfmod._seed_pg("id integer PRIMARY KEY, ratio text, label text", ROWS)
    dst = f"r14d9_{uuid.uuid4().hex[:6]}"
    try:
        for run in (1, 2):
            job = sfmod._run(client, source=src, dest=dst, validation_mode=mode, **sfmod._shaped(RECIPE))
            _assert_job(job, run)
            _assert_quarantine(client, job)
            got = {
                int(r[0]): (None if r[1] is None else int(r[1]), r[2])
                for r in sfmod._sf(f'SELECT "ID", "RATIO", "LABEL" FROM "PUBLIC"."{dst.upper()}"')
            }
            assert got == LOADED, (run, got)
    finally:
        h.drop_table("postgresql", src)
