"""Text keys: key ranges need the same order on both sides (review 3, item 7).

``plan_for`` passed no collations, so every text key read as not
order-portable: no key ranges (mode H) and a streamed pass in one read — on
MySQL / MariaDB one SELECT capped at the stream timeout, so a large table
could never be fully compared. The catalogs' collations are read now: a text
key under a binary collation on both sides is chunked, and a pass that must
be one read says so (and why) in the result.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import _endpoint, _env, _report, _sql  # noqa: E402,F401

pg = pytest.mark.skipif(not parity_engines.available("postgresql"),
                        reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


def _text_key_job(src: str, dst: str) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job({
        "status": "completed",
        "transfer_request": {
            "source": _endpoint(src), "destination": _endpoint(dst), "sync_mode": "full_refresh_overwrite",
            "mappings": [{"source": "code", "target": "code"},
                         {"source": "name", "target": "name", "transform": "upper"}],
        },
        "destination_summary": {"table": dst, "primary_key_columns": ["code"]},
        "reconciliation": {"passed": True, "source_rows": 3000, "target_rows": 3000, "phase": "post_write",
                           "coverage_limit": {"rows": 3000, "limit": 100},
                           "verification_verdict_qualifier": "coverage_limited"},
    })


def _tables(collate: str) -> tuple[str, str]:
    src, dst = f"psd1_k_s_{uuid.uuid4().hex[:8]}", f"psd1_k_d_{uuid.uuid4().hex[:8]}"
    ddl = f"code text {collate} PRIMARY KEY, name varchar(40)"
    _sql(f'CREATE TABLE "{src}" ({ddl})',
         f"INSERT INTO \"{src}\" SELECT 'k-' || lpad(g::text, 5, '0'), 'n' || g FROM generate_series(1, 3000) g",
         f'CREATE TABLE "{dst}" ({ddl})', f'INSERT INTO "{dst}" SELECT code, upper(name) FROM "{src}"')
    return src, dst


@pg
def test_a_text_key_under_a_binary_collation_gets_key_ranges_and_a_chunked_stream() -> None:
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import verification
    from services.parity_scale.driver import ScanConfig

    src, dst = _tables('COLLATE "C"')
    try:
        job_id = _text_key_job(src, dst)
        spec, _why = verification.table_spec(get_mongodb_service().get_job(job_id))
        plan = verification.plan_for(spec)
        # Before: no collations were read, so this key was "without a binary collation".
        assert plan.order_portable, plan.order_reason
        block = verification.start_for_job(job_id, background=False,
                                           config=ScanConfig(segment_rows=500, stream_chunk_rows=500))
        ev = block["tables"][0]["evidence"]
        assert ev["mode"] == "R"
        assert ev["coverage"]["stream_reads"]["parts"] > 1
        assert block["state"] == "passed"
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


@pg
def test_a_stream_that_must_be_one_read_says_so() -> None:
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import verification
    from services.parity_scale.driver import ScanConfig

    src, dst = _tables('COLLATE "en-x-icu"')
    try:
        job_id = _text_key_job(src, dst)
        spec, _why = verification.table_spec(get_mongodb_service().get_job(job_id))
        assert not verification.plan_for(spec).order_portable
        block = verification.start_for_job(job_id, background=False, config=ScanConfig(segment_rows=500))
        reads = block["tables"][0]["evidence"]["coverage"]["stream_reads"]
        assert reads["parts"] == 1
        assert "does not order the same way on both databases" in reads["reason"]
        assert "binary collation" in reads["reason"]
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def _mysql_usable() -> bool:
    if not parity_engines.available("mysql"):
        return False
    try:
        parity_engines.connect("mysql").close()
        return True
    except Exception:  # noqa: BLE001 — a reachable port with other credentials is not usable here
        return False


@pytest.mark.skipif(not _mysql_usable(), reason="MySQL not usable (DATAFLOW_TEST_MYSQL_*)")
@pytest.mark.parametrize("collation, portable", [("nopad_bin", True), ("utf8mb4_bin", False)])
def test_mysql_key_collations_are_read_from_the_catalog(collation: str, portable: bool) -> None:
    from services.parity_scale import verification
    from services.parity_scale.plan import build_plan

    table = f"psd1_kc_{uuid.uuid4().hex[:8]}"
    conn = parity_engines.connect("mysql")
    if collation == "nopad_bin":
        # The server's own byte-order NO PAD collation: MariaDB names it utf8mb4_nopad_bin,
        # MySQL 8 (the CI server) utf8mb4_0900_bin; neither server knows the other's name.
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            mariadb = "mariadb" in str(cur.fetchone()[0]).lower()
        collation = "utf8mb4_nopad_bin" if mariadb else "utf8mb4_0900_bin"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE `{table}` (code VARCHAR(20) CHARACTER SET utf8mb4 COLLATE {collation} "
                    "PRIMARY KEY, v INT) ENGINE=InnoDB")
    conn.commit()
    try:
        found = verification.catalog_collations("mysql", conn, "", table)
        assert found == {"code": collation}
        types: dict[str, Any] = {"code": "varchar(20)", "v": "int"}
        plan = build_plan([{"source": "code", "target": "code"}, {"source": "v", "target": "v"}],
                          source_types=types, dest_types=types, source_engine="mysql", dest_engine="mysql",
                          key_columns=["code"], key_collations=found, dest_key_collations=found)
        assert plan.order_portable is portable, plan.order_reason
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.commit()
        conn.close()
