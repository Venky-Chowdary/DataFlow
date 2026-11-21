"""Trust audit P0-5 on a live PostgreSQL: a sampled run states a real 95% bound and can be signed.

An append into an occupied table is where Gate-8 compares values on a sample
(the whole-table digests cover rows this run never wrote). The sample is now a
uniform random draw over the 2,000 rows the run streamed (seeded, seed
recorded), so the Trust Report states the Clopper-Pearson bound the docs
promise and sign-off opens. A destination that changes one row in five is
caught by the same draw.
"""

from __future__ import annotations

import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")


@pytest.fixture()
def pg(monkeypatch):
    from tests.conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable: {type(exc).__name__}")
    conn.autocommit = True
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    import src.transfer.stream as stream_mod

    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 300)  # several pages, so the merge is exercised
    src, dst = f"p05_src_{uuid.uuid4().hex[:8]}", f"p05_dst_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id int primary key, name text)")  # nosec B608
        cur.execute(f"INSERT INTO {src} SELECT g, 'n' || g FROM generate_series(1, 2000) g")  # nosec B608
        cur.execute(f"CREATE TABLE {dst} (id int primary key, name text)")  # nosec B608
        cur.execute(f"INSERT INTO {dst} SELECT g, 'old' || g FROM generate_series(100001, 100100) g")  # nosec B608
    try:
        yield conn, cfg, src, dst
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")  # nosec B608
            cur.execute(f"DROP TABLE IF EXISTS {dst}")  # nosec B608
            cur.execute(f"DROP FUNCTION IF EXISTS {dst}_f() CASCADE")  # nosec B608
        conn.close()


def _run(cfg: dict, src: str, dst: str, tag: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    def ep(table: str) -> EndpointConfig:
        return EndpointConfig(kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
                              database=cfg["database"], username=cfg["username"],
                              password=cfg["password"], table=table, schema="public")

    req = TransferRequest(source=ep(src), destination=ep(dst), sync_mode="full_refresh_append",
                          skip_preflight=True, validation_mode="strict")
    return UniversalTransferEngine().execute_tracked(req, f"p05_{tag}_{os.getpid()}")


def _report(result) -> dict:
    from services.parity_report import build_parity_report, public_report

    job = {
        "_id": "p05", "status": "completed" if result.success else "failed",
        "reconciliation": result.reconciliation,
        "destination_summary": {**(result.destination_summary or {}), "primary_key_columns": ["id"]},
    }
    return public_report(build_parity_report(job, reread=False))


def test_append_sample_is_a_recorded_uniform_draw_with_a_signable_bound(pg):
    from services.parity_stats import clopper_pearson_upper

    _conn, cfg, src, dst = pg
    result = _run(cfg, src, dst, "clean")
    assert result.success, result.error
    sample = (result.reconciliation or {}).get("sample_compare") or {}
    seed = sample.get("sample_seed") or {}
    assert seed.get("method") == "uniform_random", sample
    assert len(seed.get("seed") or "") == 32 and seed.get("population_rows") == 2000
    keys = sorted(int(k) for k in seed.get("pk_values") or [])
    assert len(keys) == 50 == sample.get("rows_compared")
    # Not the first rows read: a uniform draw over 2,000 rows reaches the far end.
    assert keys[-1] > 1000 and keys[0] < 1000, keys

    report = _report(result)
    table = report["tables"][0]
    assert table["check"] == "sampled" and table["status"] in {"match", "explained"}
    conf = report["confidence"]
    assert conf["bound_valid"] is True
    assert conf["upper_bound_rate"] == pytest.approx(clopper_pearson_upper(0, 50))
    assert "with 95% confidence at most 5.82% of rows hold an unexplained difference" in report["verdict_reason"]
    assert "uniform random draw" in (conf.get("sample_description") or "")
    # The 100 rows the table held before are out of scope, not a difference no rule explains.
    assert report["parity"]["unexplained"] == 0
    assert report["verdict"] == "no_difference_found"
    assert report["signoff"]["status"] == "open", report["signoff"]


def test_the_uniform_draw_finds_rows_the_destination_changed(pg):
    conn, cfg, src, dst = pg
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE FUNCTION {dst}_f() RETURNS trigger LANGUAGE plpgsql AS "  # nosec B608
            "$$ BEGIN IF NEW.id % 5 = 0 THEN NEW.name := NEW.name || '!'; END IF; RETURN NEW; END $$"
        )
        cur.execute(f"CREATE TRIGGER {dst}_t BEFORE INSERT ON {dst} FOR EACH ROW EXECUTE FUNCTION {dst}_f()")  # nosec B608
    result = _run(cfg, src, dst, "tampered")
    sample = (result.reconciliation or {}).get("sample_compare") or {}
    draw = (result.destination_summary or {}).get("reconcile_sample_draw") or {}
    assert draw.get("method") == "uniform_random" and draw.get("population_rows") == 2000
    assert sample.get("mismatches"), sample  # one row in five changed: a 50-row draw sees it
    # The compare stops early on a failure, so the draw it did not finish is not
    # reported as random (an early stop would bias a bound); the failure stands.
    if int(sample.get("rows_compared") or 0) < 50:
        assert (sample.get("sample_seed") or {}).get("method") != "uniform_random"
        assert sample.get("uniform_draw_incomplete", {}).get("drawn_rows") == 50
    report = _report(result)
    assert report["verdict"] == "unexplained"
    assert report["signoff"]["status"] == "blocked"
