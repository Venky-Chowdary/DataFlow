"""MX3-11 — strict validation must not silently read "4,0" as 4.0.

QA: PG (query mode, TEXT column) -> PG DOUBLE PRECISION with
validation_mode=strict wrote row 4 as 4.0 with no quarantine or warning.
With no number locale declared, a lone decimal comma is a locale guess
(EU 4.0; not a US number at all). Strict refuses the guess; balanced keeps
the existing Auto reading.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


@pytest.mark.parametrize(
    ("value", "strict", "expected"),
    [
        ("4,0", True, None),
        ("1.234,5", True, None),
        ("4,0", False, "4.0"),
        ("1,234.5", True, "1234.5"),
        ("1,234,567", True, "1234567"),
        ("4.0", True, "4.0"),
    ],
)
def test_strict_refuses_only_decimal_comma_guesses(value, strict, expected):
    from services.transform_engine import (
        _parse_decimal,
        reset_strict_number_reading,
        set_strict_number_reading,
        strict_decimal_comma_reason,
    )

    token = set_strict_number_reading(strict)
    try:
        assert _parse_decimal(value) == expected
        if expected is None:
            assert "decimal separator" in strict_decimal_comma_reason(value)
    finally:
        reset_strict_number_reading(token)


@pytest.fixture
def comma_route():
    import psycopg2

    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    name = f"MX311{sfx}"
    saved = create_connector({
        "name": name, "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
        "schema": "public",
    })
    made: list[str] = []

    def tables(tag: str):
        src, dst = f"mx311s_{sfx}_{tag}", f"mx311d_{sfx}_{tag}"
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, flt TEXT)")
            cur.executemany(
                f"INSERT INTO {src} VALUES (%s,%s)",
                [(1, "1.5"), (2, "2.5"), (3, "not-a-number"), (4, "4,0"), (5, "5.5")],
            )
            cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, flt DOUBLE PRECISION)")
        made.extend([src, dst])
        return src, dst

    try:
        yield {"conn": conn, "name": name, "tables": tables}
    finally:
        with conn.cursor() as cur:
            for t in made:
                cur.execute(f"DROP TABLE IF EXISTS {t}")
        conn.close()
        delete_connector(saved.id)


def _run(route, mode: str, tag: str):
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    src, dst = route["tables"](tag)
    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": route["name"],
        "source_read_mode": "query",
        "source_query": f"SELECT id, flt FROM {src}",
        "dest_connector_name": route["name"], "dest_table": dst,
        "sync_mode": "full_refresh_append", "validation_mode": mode,
        "risk_acceptance": {
            "execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "accepted",
        },
    })
    assert staged.success, staged.error
    confirmed = spend_pilot_ack(staged.output["ack_id"], "mx311-test")
    assert confirmed["ok"] is True, confirmed
    deadline = time.time() + 120
    job: dict = {}
    while time.time() < deadline:
        job = dict(DataPilotTools().execute("get_job", {"job_id": confirmed["job_id"]}).output or {})
        if str(job.get("status") or "") not in {"", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    with route["conn"].cursor() as cur:
        cur.execute(f"SELECT id, flt FROM {dst} ORDER BY id")
        rows = cur.fetchall()
    return job, rows


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_strict_quarantines_ambiguous_decimal_comma(comma_route):
    job, rows = _run(comma_route, "strict", "s")
    assert rows == [(1, 1.5), (2, 2.5), (5, 5.5)], (rows, job)
    held = {s["row"]: s for s in job.get("quarantine_samples") or []}
    assert 4 in held, job
    assert "4,0" in held[4]["value"]
    assert "comma" in held[4]["reason"].lower() or "locale" in held[4]["reason"].lower(), held[4]
