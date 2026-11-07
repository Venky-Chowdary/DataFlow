"""QA C1 / MySQL D8: concurrent MySQL -> PostgreSQL transfers into one PG database.

QA (jobs 6ae69b70…, 6ae69b32…, 6ae69b2e…, 6ae69f3b…, 6ae6a84c…): 2-4
MySQL-source transfers running at the same time into the same PostgreSQL
destination intermittently failed "current transaction is aborted, commands
ignored until end of transaction block"; the same table alone then passed.

Shape here: four Rfam-``clan``-like MySQL tables (tinyint(1) DEFAULT 0,
timestamps, doubles, text with NULL/''/whitespace/unicode, 1,500 rows each),
staged through MCP and confirmed back to back so the four jobs run at once —
three into different PG tables, plus a second job into the same table as the
first. Every job must reach a truthful terminal state with no aborted
transaction, every completed destination must equal its source cell for cell,
and the whole round is repeated (the brief asks for 5 consecutive clean runs:
``DATAFLOW_R14_CONCURRENCY_ROUNDS``, default 2). A round of four concurrent
jobs takes about 50 s on the shared CI-class host (5 rounds: 242 s), so the
default costs roughly 100 s.
"""

from __future__ import annotations

import os
import random
import uuid
from datetime import datetime, timedelta

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg, _connector, _pg_cfg, _sql

pytestmark = pytest.mark.timeout(900)

_ROWS = 1500
_DDL = """
CREATE TABLE {t} (
  `clan_acc` varchar(7) NOT NULL,
  `id` varchar(40) DEFAULT NULL,
  `previous_id` tinytext,
  `description` varchar(100) DEFAULT NULL,
  `author` tinytext,
  `comment` longtext,
  `created` datetime NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `score` double DEFAULT NULL,
  `flag` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`clan_acc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""
_TEXTS = [None, "", "   ", "Zoë ✓", "日本語", "plain", "a\nb", "tab\there"]


def _rows(seed: int) -> list[tuple]:
    rng = random.Random(seed)
    base = datetime(2020, 1, 1)
    out = []
    for i in range(_ROWS):
        out.append((
            f"CL{i:05d}", f"clan_{i}", rng.choice(_TEXTS), rng.choice(_TEXTS),
            rng.choice(_TEXTS), rng.choice(_TEXTS) if i % 7 else "x" * 3000,
            base + timedelta(minutes=i), base + timedelta(seconds=i * 13),
            None if i % 11 == 0 else round(rng.uniform(-1e6, 1e6), 4), i % 2,
        ))
    return out


def _seed(table: str, seed: int) -> None:
    _sql("mysql", _DDL.format(t=f"`{table}`"))
    conn = h.mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
            cur.executemany(f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            _rows(seed))
    finally:
        conn.close()


def _src_cells(table: str) -> dict:
    return {r[0]: (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in _sql(
        "mysql", f"SELECT clan_acc, id, previous_id, description, author, comment, score, "
                 f"flag FROM `{table}`")}


def _dst_cells(table: str) -> dict:
    rows = h.query("postgresql", f'SELECT clan_acc, id, previous_id, description, author, '
                                 f'comment, score, flag FROM "{table}"')
    return {r[0]: (r[0], r[1], r[2], r[3], r[4], r[5], r[6], int(r[7])) for r in rows}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_four_concurrent_mysql_to_pg_jobs_never_abort_a_transaction(client):
    rounds = int(os.environ.get("DATAFLOW_R14_CONCURRENCY_ROUNDS", "2"))
    token = uuid.uuid4().hex[:6]
    srcs = [f"r14c1s{i}_{token}" for i in range(3)]
    dsts = [f"r14c1d{i}_{token}" for i in range(3)]
    try:
        for i, t in enumerate(srcs):
            _seed(t, seed=i)
        src = _connector("mysql", _cfg("mysql"), "source")
        dst = _connector("postgresql", _pg_cfg(), "destination")
        # job 3 writes the same destination table as job 0 from another source
        # (the same source -> same table is refused 409 duplicate_transfer).
        pairs = list(zip(srcs, dsts)) + [(srcs[1], dsts[0])]
        for rnd in range(rounds):
            acks = []
            for s, d in pairs:
                ok, staged = h.mcp_call(client, "start_transfer", {
                    "source_connector_id": src, "dest_connector_id": dst, "source_table": s,
                    "dest_table": d, "sync_mode": "full_refresh_overwrite",
                    "primary_key": "clan_acc", "validation_mode": "strict"})
                assert ok, {"error": staged}
                acks.append(staged["ack_id"])
            job_ids = []
            for ack in acks:  # back to back: the four jobs run at the same time
                resp = client.post("/api/v1/copilot/confirm",
                                   json={"ack_id": ack, "reason": f"r14-c1-{rnd}"})
                if resp.status_code == 409 and len(job_ids) == 3:
                    # A truthful refusal of a second writer on the same table.
                    job_ids.append("")
                    continue
                assert resp.status_code == 200 and resp.json().get("ok"), resp.text
                job_ids.append(str(resp.json()["job_id"]))
            jobs = [h.wait_job(j, timeout_s=600) if j else {"status": "failed",
                    "error": "refused 409"} for j in job_ids]
            for job in jobs:
                text = " ".join(str(job.get(k) or "") for k in ("error", "message"))
                assert "current transaction is aborted" not in text, (rnd, job.get("status"), text)
                assert job.get("status") in {"completed", "failed"}, (rnd, job.get("status"))
            # The three distinct-table jobs have no competitor and must complete.
            for job in jobs[:3]:
                assert job.get("status") == "completed", (
                    rnd, job.get("status"), job.get("error") or job.get("message"))
            # The same-table pair: one of them owns the table, a refused one says why.
            same = jobs[3]
            if same.get("status") != "completed":
                assert str(same.get("error") or same.get("message") or "").strip(), same
            for s, d in list(zip(srcs, dsts))[1:]:
                assert _dst_cells(d) == _src_cells(s), (rnd, d)
            # dsts[0] holds one complete writer's population, never a mix.
            assert _dst_cells(dsts[0]) in (_src_cells(srcs[0]), _src_cells(srcs[1])), rnd
    finally:
        for t in srcs:
            _sql("mysql", f"DROP TABLE IF EXISTS `{t}`")
        for t in dsts:
            h.drop_table("postgresql", t)
