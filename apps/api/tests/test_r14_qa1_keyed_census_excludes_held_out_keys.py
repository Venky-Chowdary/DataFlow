"""QA1 #1 counters: a keyed upsert's key census must not count quarantined keys.

Repro (live MySQL 8 -> PostgreSQL 16, incremental_upsert, 7/10 quarantined):
the census is probed before the write over all 10 live keys, so the Run page
showed INSERTS 7 / UPDATES 3 while psql showed 3 new rows and 0 updated. The 7
held-out keys never landed; the 3 of them already stored kept their old value.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

from services.row_conservation import CENSUS_KEY  # noqa: E402

MAPS = [{"source": c, "target": c} for c in ("id", "name", "qty")]
WIDTH = "value length 40 exceeds PostgreSQL VARCHAR(20) — quarantined (would truncate on write)"


def _summary() -> dict:
    return {
        CENSUS_KEY: {"unique_batch_keys": 10, "dest_preexisting": 3, "events_read": 10},
        "rejected_details": [
            {"column": "name", "value": "custom…xxxx", "source_pk": str(i), "reason": WIDTH}
            for i in range(1, 8)
        ] + [{"column": "id", "source_pk": "9", "reason": "duplicate key violates unique constraint"}],
    }


def test_held_out_keys_leave_the_census(monkeypatch):
    from services import dest_precount
    from src.transfer.reconcile_step import _census_without_held_out_keys

    probed: list = []

    def hits(db_type, cfg, *, schema, table_name, key_columns, keys):
        probed.extend(keys)
        return sum(1 for (k,) in keys if k in (1, 2, 3))

    monkeypatch.setattr(dest_precount, "destination_key_hits", hits)
    summary = _summary()
    _census_without_held_out_keys(
        summary, db_type="postgresql", cfg={}, schema="public", table_name="t", key_columns=["id"]
    )
    census = summary[CENSUS_KEY]
    assert sorted(probed) == [(i,) for i in range(1, 8)]  # the UNIQUE refusal is not held out
    assert (census["inserts"], census["updates"], census["expected_delta"]) == (3, 0, 3)
    assert (census["held_out_keys"], census["held_out_keys_stored"]) == (7, 3)

    # Reconcile can run twice on one summary (retry); the keys leave once.
    _census_without_held_out_keys(
        summary, db_type="postgresql", cfg={}, schema="public", table_name="t", key_columns=["id"]
    )
    assert summary[CENSUS_KEY]["unique_batch_keys"] == 3


def test_failed_probe_leaves_the_census_alone(monkeypatch):
    from services import dest_precount
    from src.transfer.reconcile_step import _census_without_held_out_keys

    monkeypatch.setattr(dest_precount, "destination_key_hits", lambda *a, **k: None)
    summary = _summary()
    before = dict(summary[CENSUS_KEY])
    _census_without_held_out_keys(
        summary, db_type="postgresql", cfg={}, schema="public", table_name="t", key_columns=["id"]
    )
    assert summary[CENSUS_KEY] == before


def _up() -> bool:
    import socket

    for port in (3306, 5432):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
        except OSError:
            return False
    return True


@pytest.mark.parametrize("mode", ["incremental_upsert", "incremental_deduped"])
def test_live_upsert_census_counts_only_landed_keys(mode):
    if not _up():
        pytest.skip("MySQL :3306 / PostgreSQL :5432 not reachable")
    import psycopg2
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    t = "qa1c_" + uuid.uuid4().hex[:6]
    my = pymysql.connect(host="127.0.0.1", user="dataflow", password="dataflow",
                         database="dataflow", autocommit=True)
    pg = psycopg2.connect(host="127.0.0.1", dbname="dataflow", user="dataflow", password="dataflow")
    pg.autocommit = True
    try:
        with my.cursor() as c:
            c.execute(f"CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(100), qty INT)")
            for i in range(1, 11):
                name = f"customer-long-name-number-{i:02d}-" + "x" * (10 + i) if i <= 7 else f"short{i}"
                c.execute(f"INSERT INTO {t} VALUES (%s,%s,%s)", (i, name, i * 10))
        with pg.cursor() as c:
            c.execute(f"CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(20), qty INT)")
            c.execute(f"INSERT INTO {t} VALUES (1,'old1',1),(2,'old2',2),(3,'old3',3)")

        def ep(fmt: str, port: int, schema: str = "") -> EndpointConfig:
            return EndpointConfig(kind="database", format=fmt, host="127.0.0.1", port=port,
                                  database="dataflow", username="dataflow",
                                  password="dataflow", schema=schema, table=t)

        r = UniversalTransferEngine().execute(TransferRequest(
            source=ep("mysql", 3306), destination=ep("postgresql", 5432, "public"),
            sync_mode=mode, skip_preflight=True, validation_mode="balanced",
            stream_contracts=[{"primary_key": "id"}], mappings=MAPS,
        ))
        assert r.success, r.error
        census = (r.destination_summary or {}).get(CENSUS_KEY) or {}
        assert (census.get("inserts"), census.get("updates")) == (3, 0), census
        ledger = r.row_accounting or {}
        assert (ledger.get("inserts"), ledger.get("updates"), ledger.get("dest_delta")) == (3, 0, 3)
    finally:
        with my.cursor() as c:
            c.execute(f"DROP TABLE IF EXISTS {t}")
        with pg.cursor() as c:
            c.execute(f"DROP TABLE IF EXISTS {t}")
            c.execute(f"DROP TABLE IF EXISTS {t}_df_quarantine")
        my.close()
        pg.close()
