"""Continuous Fidelity proves two live datasets still carry the same population.

These tests hold the parallel-run check to three things: it detects the
divergences that matter and names the column; it is honest about what it did not
compare (an unsupported engine, a cross-engine statistic it declines); and its
report is tamper-evident so a stored or forwarded result cannot be quietly
edited. The service is exercised directly and through its HTTP surface.
"""

from __future__ import annotations

import socket
import uuid
from decimal import Decimal

import pytest

from services.continuous_fidelity import (
    ASSURANCE_ENGINE_PROFILE,
    ASSURANCE_NO_COLUMNS,
    ASSURANCE_UNSUPPORTED,
    DEFAULT_REQUIRED_CONSECUTIVE,
    FidelityReport,
    VERDICT_CUTOVER_READY,
    VERDICT_DIVERGING,
    VERDICT_IN_PROGRESS,
    VERDICT_UNPROVEN,
    _digest_report,
    _pairs_and_types,
    evaluate_campaign,
    population_comparable,
    record_check,
    run_fidelity_check,
)
from src.transfer.models import EndpointConfig

_PG_PORT, _MY_PORT = 5432, 3306


def _reachable(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


pg = pytest.mark.skipif(not _reachable(_PG_PORT), reason="PostgreSQL not reachable")
my = pytest.mark.skipif(not _reachable(_MY_PORT), reason="MySQL/MariaDB not reachable")

_MAPPINGS = [
    {"source": "id", "target": "id", "target_type": "bigint"},
    {"source": "email", "target": "email", "target_type": "text"},
    {"source": "amount", "target": "amount", "target_type": "numeric(12,2)"},
]


def _pg_endpoint(schema: str, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql", host="127.0.0.1", port=_PG_PORT,
        database="dataflow", username="dataflow", password="dataflow",
        schema=schema, table=table,
    )


def _my_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="mysql", host="127.0.0.1", port=_MY_PORT,
        database="dataflow", username="dataflow", password="dataflow",
        schema="dataflow", table=table,
    )


# --------------------------------------------------------------------------- #
# Pure logic — no database
# --------------------------------------------------------------------------- #


def test_report_digest_is_tamper_evident():
    report = FidelityReport(
        run_id="fid_x", checked_at="2026-01-01T00:00:00Z", passed=True,
        assurance_level=ASSURANCE_ENGINE_PROFILE, cross_engine=False,
        source={"engine": "postgresql", "schema": "public", "table": "a"},
        destination={"engine": "postgresql", "schema": "public", "table": "b"},
        source_rows=10, target_rows=10, row_balance_passed=True, columns_compared=3,
        divergent_columns=[], divergences=[], compared_statistics=["null_count"],
        not_compared=[], message="ok",
    )
    d = report.to_dict()
    # The stamped digest matches a recomputation over the body.
    assert d["report_digest"] == _digest_report(d)
    # Editing any field invalidates it.
    tampered = dict(d)
    tampered["passed"] = False
    assert tampered["report_digest"] != _digest_report(tampered)


def test_pairs_and_types_honours_omit_and_type_override():
    mappings = [
        {"source": "id", "target": "id", "target_type": "int"},
        {"source": "secret", "target": "", "intentional_omit": True},
        {"source": "amt", "target": "total", "inferredType": "numeric"},
    ]
    pairs, types = _pairs_and_types(mappings, {"total": "numeric(12,2)"})
    assert pairs == [("id", "id"), ("amt", "total")]
    # Explicit column_types override the mapping's inferred type.
    assert types["total"] == "numeric(12,2)"
    assert types["id"] == "int"


def test_column_types_alone_define_an_identity_mapping():
    pairs, types = _pairs_and_types([], {"id": "bigint", "name": "text"})
    assert sorted(pairs) == [("id", "id"), ("name", "name")]


def _cycle(*, passed: bool, level: str = ASSURANCE_ENGINE_PROFILE, cols=None) -> dict:
    return {
        "passed": passed,
        "assurance_level": level,
        "divergent_columns": list(cols or []),
        "message": "ok" if passed else "diverged",
    }


def test_campaign_unproven_without_engine_cycles():
    state = evaluate_campaign([])
    assert state["verdict"] == VERDICT_UNPROVEN
    assert state["consecutive_passes"] == 0
    skipped = evaluate_campaign([_cycle(passed=False, level=ASSURANCE_UNSUPPORTED)])
    assert skipped["verdict"] == VERDICT_UNPROVEN


def test_campaign_skips_probe_timeouts_and_counts_trailing_passes():
    """A timeout is not a dirty cycle — same lesson as source-schema memory."""
    history = [
        _cycle(passed=True),
        _cycle(passed=False, level=ASSURANCE_NO_COLUMNS),
        _cycle(passed=True),
    ]
    state = evaluate_campaign(history, required_consecutive=3)
    assert state["verdict"] == VERDICT_IN_PROGRESS
    assert state["consecutive_passes"] == 0
    assert state["screening_observed"] == 2


def test_campaign_divergence_resets_the_window():
    history = [
        _cycle(passed=True),
        _cycle(passed=True),
        _cycle(passed=False, cols=["amount"]),
    ]
    state = evaluate_campaign(history, required_consecutive=3)
    assert state["verdict"] == VERDICT_DIVERGING
    assert state["consecutive_passes"] == 0
    assert "amount" in state["next_action"]


def test_campaign_profiles_never_confer_cutover_ready():
    """Column-profile greens are screening — Google Dual Run compares outputs."""
    history = [_cycle(passed=True)] * DEFAULT_REQUIRED_CONSECUTIVE
    state = evaluate_campaign(history)
    assert state["verdict"] == VERDICT_IN_PROGRESS
    assert state["consecutive_passes"] == 0
    assert "Gate-8" in state["next_action"]


def _gate8(*, passed: bool, checksum_match: bool | None = None) -> dict:
    return {
        "passed": passed,
        "assurance_level": "full_checksum" if passed else "none",
        "checksum_match": True if passed and checksum_match is None else checksum_match,
        "message": "ok" if passed else "checksum mismatch",
        "source_rows": 10,
        "target_rows": 10 if passed else 9,
    }


def test_campaign_cutover_ready_is_n_consecutive_gate8():
    history = [_gate8(passed=True)] * DEFAULT_REQUIRED_CONSECUTIVE
    state = evaluate_campaign(history)
    assert state["verdict"] == VERDICT_CUTOVER_READY
    assert state["consecutive_passes"] == DEFAULT_REQUIRED_CONSECUTIVE
    assert "not migration_proven" in state["next_action"]
    assert "migration_proven" in state["note"]


def test_campaign_profile_pass_does_not_break_gate8_window():
    history = [
        _gate8(passed=True),
        _cycle(passed=True),
        _gate8(passed=True),
        _gate8(passed=True),
    ]
    state = evaluate_campaign(history, required_consecutive=3)
    assert state["verdict"] == VERDICT_CUTOVER_READY
    assert state["consecutive_passes"] == 3


def test_campaign_profile_fail_after_gate8_diverges():
    history = [
        _gate8(passed=True),
        _gate8(passed=True),
        _gate8(passed=True),
        _cycle(passed=False, cols=["amount"]),
    ]
    state = evaluate_campaign(history, required_consecutive=3)
    assert state["verdict"] == VERDICT_DIVERGING
    assert state["consecutive_passes"] == 0


def test_record_gate8_cycle_confers_cutover_after_n():
    from services.continuous_fidelity import record_gate8_cycle

    campaign = None
    recon = {
        "passed": True,
        "assurance_level": "full_checksum",
        "checksum_match": True,
        "source_rows": 4,
        "target_rows": 4,
        "message": "Row fidelity verified",
    }
    for _ in range(DEFAULT_REQUIRED_CONSECUTIVE):
        campaign = record_gate8_cycle(campaign, recon, required_consecutive=3)
    assert campaign["verdict"] == VERDICT_CUTOVER_READY
    assert campaign["consecutive_passes"] == 3


def test_writer_ack_does_not_confer_cutover():
    history = [
        {
            "passed": True,
            "assurance_level": "writer_ack",
            "message": "writer digest",
        }
    ] * 5
    state = evaluate_campaign(history, required_consecutive=3)
    assert state["verdict"] == VERDICT_UNPROVEN
    assert state["consecutive_passes"] == 0


def test_compact_gate8_refuses_checksum_mismatch():
    from services.continuous_fidelity import compact_gate8

    cycle = compact_gate8(
        {
            "passed": True,
            "assurance_level": "full_checksum",
            "checksum_match": False,
            "source_rows": 10,
            "target_rows": 10,
            "message": "counts match, digest does not",
        }
    )
    assert cycle["passed"] is False
    state = evaluate_campaign([cycle], required_consecutive=3)
    assert state["verdict"] == VERDICT_DIVERGING


def test_record_check_appends_and_is_tamper_evident():
    report = FidelityReport(
        run_id="fid_c", checked_at="2026-01-01T00:00:00Z", passed=True,
        assurance_level=ASSURANCE_ENGINE_PROFILE, cross_engine=False,
        source={"engine": "postgresql", "schema": "public", "table": "a"},
        destination={"engine": "postgresql", "schema": "public", "table": "b"},
        source_rows=10, target_rows=10, row_balance_passed=True, columns_compared=3,
        divergent_columns=[], divergences=[], compared_statistics=["null_count"],
        not_compared=[], message="ok",
    )
    campaign = record_check(None, report, required_consecutive=3)
    assert campaign["verdict"] == VERDICT_IN_PROGRESS
    assert campaign["consecutive_passes"] == 0
    assert campaign["history"][0]["report_digest"].startswith("sha256:")
    again = record_check(campaign, report)
    again = record_check(again, report)
    assert again["verdict"] == VERDICT_IN_PROGRESS
    assert again["consecutive_passes"] == 0


def test_append_sync_is_not_a_same_population_dual_run():
    assert population_comparable("full_refresh_overwrite") is True
    assert population_comparable("full_refresh_append") is False
    assert population_comparable("incremental_deduped") is False
    assert population_comparable("scd2") is False


def test_unsupported_engine_declines_without_connecting():
    """A Snowflake/Mongo endpoint is refused fast, before any connection."""
    report = run_fidelity_check(
        source=EndpointConfig(kind="database", format="snowflake", table="a"),
        destination=EndpointConfig(kind="database", format="snowflake", table="b"),
        mappings=_MAPPINGS,
    )
    assert report.passed is False
    assert report.assurance_level == ASSURANCE_UNSUPPORTED
    assert report.to_dict()["report_digest"].startswith("sha256:")


def test_missing_table_is_reported_cleanly():
    report = run_fidelity_check(
        source=_pg_endpoint("public", ""),
        destination=_pg_endpoint("public", ""),
        mappings=_MAPPINGS,
    )
    assert report.assurance_level == ASSURANCE_NO_COLUMNS


# --------------------------------------------------------------------------- #
# Live same-engine (PostgreSQL)
# --------------------------------------------------------------------------- #


@pytest.fixture()
def pg_pair():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(host="127.0.0.1", port=_PG_PORT, dbname="dataflow",
                            user="dataflow", password="dataflow")
    conn.autocommit = True
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"cf_src_{sfx}", f"cf_dst_{sfx}"
    with conn.cursor() as cur:
        for t in (src, dst):
            cur.execute(f'CREATE TABLE "{t}" (id bigint, email text, amount numeric(12,2))')
        cur.execute(
            f"INSERT INTO \"{src}\" SELECT g, 'e'||g, (g%1000)::numeric/100 "
            "FROM generate_series(1, 4000) g"
        )
        cur.execute(f'INSERT INTO "{dst}" SELECT * FROM "{src}"')
    try:
        yield conn, src, dst
    finally:
        with conn.cursor() as cur:
            for t in (src, dst):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()


@pg
def test_identical_datasets_prove_parity(pg_pair):
    _conn, src, dst = pg_pair
    report = run_fidelity_check(
        source=_pg_endpoint("public", src), destination=_pg_endpoint("public", dst),
        mappings=_MAPPINGS,
    )
    assert report.passed
    assert report.assurance_level == ASSURANCE_ENGINE_PROFILE
    assert report.source_rows == report.target_rows == 4000
    assert report.divergences == []


@pg
def test_silently_nulled_column_is_named(pg_pair):
    conn, src, dst = pg_pair
    with conn.cursor() as cur:
        cur.execute(f'UPDATE "{dst}" SET email = NULL WHERE id <= 250')
    report = run_fidelity_check(
        source=_pg_endpoint("public", src), destination=_pg_endpoint("public", dst),
        mappings=_MAPPINGS,
    )
    assert not report.passed
    assert report.divergent_columns == ["email"]
    assert any(d.column == "email" and d.statistic == "null_count" for d in report.divergences)


@pg
def test_dropped_rows_fail_the_row_balance(pg_pair):
    conn, src, dst = pg_pair
    with conn.cursor() as cur:
        cur.execute(f'DELETE FROM "{dst}" WHERE id <= 5')
    report = run_fidelity_check(
        source=_pg_endpoint("public", src), destination=_pg_endpoint("public", dst),
        mappings=_MAPPINGS,
    )
    assert not report.passed
    assert not report.row_balance_passed
    assert report.source_rows == 4000 and report.target_rows == 3995


# --------------------------------------------------------------------------- #
# Live cross-engine (PostgreSQL source, MySQL/MariaDB destination)
# --------------------------------------------------------------------------- #


@pytest.fixture()
def cross_pair():
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    pgc = psycopg2.connect(host="127.0.0.1", port=_PG_PORT, dbname="dataflow",
                           user="dataflow", password="dataflow")
    pgc.autocommit = True
    myc = pymysql.connect(host="127.0.0.1", port=_MY_PORT, user="dataflow",
                          password="dataflow", database="dataflow", autocommit=True)
    sfx = uuid.uuid4().hex[:6]
    src, dst = f"cf_src_{sfx}", f"cf_my_{sfx}"
    rows = [(i, f"e{i}", Decimal(i % 1000) / 100) for i in range(1, 4001)]
    with pgc.cursor() as cur:
        cur.execute(f'CREATE TABLE "{src}" (id bigint, email text, amount numeric(12,2))')
        cur.executemany(f'INSERT INTO "{src}" VALUES (%s,%s,%s)', rows)
    with myc.cursor() as cur:
        cur.execute(f"CREATE TABLE `{dst}` (id bigint, email varchar(255), amount decimal(12,2))")
        cur.executemany(f"INSERT INTO `{dst}` (id,email,amount) VALUES (%s,%s,%s)", rows)
    try:
        yield pgc, myc, src, dst
    finally:
        with pgc.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        with myc.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        pgc.close()
        myc.close()


@pg
@my
def test_cross_engine_parity_holds_and_declares_its_limits(cross_pair):
    _pg, _my, src, dst = cross_pair
    report = run_fidelity_check(
        source=_pg_endpoint("public", src), destination=_my_endpoint(dst),
        mappings=_MAPPINGS,
    )
    assert report.passed
    assert report.cross_engine is True
    # This dataset has no zone-aware column, so the declined set is the
    # always-cross-engine set: collation-dependent text and order-dependent sums.
    declined = " ".join(report.not_compared)
    assert "collation" in declined and "float sum" in declined


@pg
@my
def test_cross_engine_numeric_drift_is_named(cross_pair):
    _pg, myc, src, dst = cross_pair
    with myc.cursor() as cur:
        cur.execute(f"UPDATE `{dst}` SET amount = amount + 0.01 WHERE id = 3")
    report = run_fidelity_check(
        source=_pg_endpoint("public", src), destination=_my_endpoint(dst),
        mappings=_MAPPINGS,
    )
    assert not report.passed
    assert "amount" in report.divergent_columns


# --------------------------------------------------------------------------- #
# HTTP surface
# --------------------------------------------------------------------------- #


@pg
def test_http_check_endpoint_returns_a_report(pg_pair):
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from src.routers.fidelity_router import router as fidelity_router

    _conn, src, dst = pg_pair
    app = fastapi.FastAPI()
    app.include_router(fidelity_router, prefix="/api/v1")
    client = TestClient(app)

    def _body(table: str) -> dict:
        return {
            "kind": "database", "format": "postgresql", "host": "127.0.0.1",
            "port": _PG_PORT, "database": "dataflow", "username": "dataflow",
            "password": "dataflow", "schema": "public", "table": table,
        }

    resp = client.post(
        "/api/v1/fidelity/check",
        json={"source": _body(src), "destination": _body(dst), "mappings": _MAPPINGS},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["passed"] is True
    assert payload["source_rows"] == 4000
    assert payload["report_digest"].startswith("sha256:")
