"""Unique-engine source × dest cartesian — live desktop backends.

100% is not claimed. Skip when a port is closed. Never invent Salesforce green.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from services.desktop_lab_cross import (
    CORE_UNIQUE_ENGINES,
    LIVE_UNIQUE_ENGINES,
    bind_live_engine,
    engines_for_run,
    run_live_engine_cross_matrix,
)


def test_cross_matrix_lists_unique_engines_not_saas_twins(monkeypatch):
    monkeypatch.delenv("DATAFLOW_CROSS_EXTENDED", raising=False)
    assert "postgresql" in LIVE_UNIQUE_ENGINES
    assert "mysql" in LIVE_UNIQUE_ENGINES
    assert "mongodb" in LIVE_UNIQUE_ENGINES
    assert "s3" in LIVE_UNIQUE_ENGINES
    assert "elasticsearch" in LIVE_UNIQUE_ENGINES
    assert "kafka" in LIVE_UNIQUE_ENGINES
    assert "elasticsearch" not in CORE_UNIQUE_ENGINES
    assert "kafka" not in CORE_UNIQUE_ENGINES
    assert "salesforce" not in LIVE_UNIQUE_ENGINES
    assert "hubspot" not in LIVE_UNIQUE_ENGINES
    assert "postgresql_rds" not in LIVE_UNIQUE_ENGINES
    assert len(LIVE_UNIQUE_ENGINES) == len(set(LIVE_UNIQUE_ENGINES))
    assert len(LIVE_UNIQUE_ENGINES) >= 10
    assert engines_for_run() == CORE_UNIQUE_ENGINES


def test_bind_skips_closed_port(tmp_path, monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setattr(mod, "_reachable", lambda *a, **k: False)
    bound = bind_live_engine("postgresql", "t", tmp_path)
    assert isinstance(bound, str)
    assert "not reachable" in bound


def test_pg_mysql_minio_cross_smoke_when_reachable(tmp_path):
    """Four OLTP/object pairs — the route the UI smoke missed."""
    from services.desktop_lab_cross import _seed, _transfer_pair, _sid

    wanted = ("postgresql", "mysql", "s3")
    seeds = {}
    for engine in wanted:
        bound, row = _seed(engine, tmp_path)
        if row["status"] == "skipped":
            continue
        assert row["status"] == "passed", row
        seeds[engine] = bound
    if len(seeds) < 2:
        return
    for src_id, src in seeds.items():
        for dst_id in seeds:
            dst = bind_live_engine(dst_id, _sid("d"), tmp_path)
            assert not isinstance(dst, str), dst
            outcome = _transfer_pair(src, dst)
            assert outcome["status"] == "passed", (src_id, dst_id, outcome)


@pytest.mark.skipif(
    os.environ.get("DATAFLOW_CROSS_MATRIX", "").strip() != "1",
    reason="Full unique-engine cartesian is opted in via DATAFLOW_CROSS_MATRIX=1",
)
def test_live_engine_cross_matrix_writes_artifact():
    report = run_live_engine_cross_matrix(persist=True)
    assert report["pairs"] == len(engines_for_run()) ** 2
    assert report["passed"] + report["failed"] + report["skipped"] == report["pairs"]
    assert report["honesty"]["catalog_tiles_are_not_transfer_live"] is True
    assert report["honesty"]["not_catalog_alias_cartesian"] is True
    for row in report["routes"]:
        if row["status"] == "passed":
            assert row.get("silent_loss") in (False, None)
            assert row.get("integrity") in {
                "passed",
                "dest_count_pair_payload_sampled_on_engine",
            }
    artifact = Path("/opt/cursor/artifacts/desktop_lab_cross.json")
    if artifact.is_file():
        saved = json.loads(artifact.read_text())
        assert saved["pairs"] == report["pairs"]
        assert saved["passed"] == report["passed"]


def test_pair_mappings_declare_mongo_id_omit_and_do_not_stamp_dest_types():
    from src.transfer.models import EndpointConfig

    from services.desktop_lab_cross import _pair_mappings

    mongo = EndpointConfig(kind="database", format="mongodb", table="t")
    maps = _pair_mappings(mongo)
    omit = [m for m in maps if m.get("intentional_omit")]
    assert omit and omit[0]["source"] == "_id"
    pg = EndpointConfig(kind="database", format="postgresql", table="t")
    pg_maps = _pair_mappings(pg)
    assert not any(m.get("intentional_omit") for m in pg_maps)
    assert all("target_type" not in m for m in pg_maps)
    redis = EndpointConfig(kind="database", format="redis", table="t")
    redis_omits = {m["source"] for m in _pair_mappings(redis) if m.get("intentional_omit")}
    assert redis_omits == {"redis_key", "redis_type"}
    es = EndpointConfig(kind="database", format="elasticsearch", table="t")
    es_omits = {m["source"] for m in _pair_mappings(es) if m.get("intentional_omit")}
    assert es_omits == {"_id", "_index"}


def test_elasticsearch_bind_skips_closed_port(tmp_path, monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setattr(mod, "_reachable", lambda *a, **k: False)
    bound = bind_live_engine("elasticsearch", "t", tmp_path)
    assert isinstance(bound, str)
    assert "9200" in bound


def test_kafka_bind_skips_closed_port(tmp_path, monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setattr(mod, "_reachable", lambda *a, **k: False)
    bound = bind_live_engine("kafka", "t", tmp_path)
    assert isinstance(bound, str)
    assert "9092" in bound


def test_kafka_uniqueness_is_a_payload_scan_not_sql_group_by():
    """Kafka has no relation for GROUP BY; uniqueness is the topic payload."""
    from services.source_duplicate_probe import (
        PAYLOAD_SCANNED_SOURCE_TYPES,
        READER_PAGED_SOURCE_TYPES,
        SQLISH_SOURCE_TYPES,
    )

    assert "kafka" in READER_PAGED_SOURCE_TYPES
    assert "kafka" in PAYLOAD_SCANNED_SOURCE_TYPES
    assert "kafka" not in SQLISH_SOURCE_TYPES


def test_extended_run_lists_sixteen_unique_engines(monkeypatch):
    monkeypatch.setenv("DATAFLOW_CROSS_EXTENDED", "1")
    engines = engines_for_run()
    assert "kafka" in engines
    assert len(engines) == 16
    assert len(engines) == len(set(engines))


def test_kafka_uniqueness_and_sql_dest_when_reachable(tmp_path):
    """kafka→sqlite previously fail-closed: probe did not address the topic."""
    from services.desktop_lab_cross import _cfg, _kafka_reread, _seed, _sid, _transfer_pair
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    bound, row = _seed("kafka", tmp_path)
    if row["status"] == "skipped":
        pytest.skip(row["error"] or "kafka not reachable")
    assert row["status"] == "passed", row
    src = _kafka_reread(bound)
    probe = probe_source_duplicate_keys_result(
        source_config=_cfg(src),
        source_table=bound.table,
        primary_key="id",
    )
    assert probe.status == "ran", probe.message
    assert probe.findings == []

    sqlite_dst = bind_live_engine("sqlite", _sid("d"), tmp_path)
    assert not isinstance(sqlite_dst, str), sqlite_dst
    sqlite_out = _transfer_pair(bound, sqlite_dst)
    assert sqlite_out["status"] == "passed", sqlite_out

    kafka_dst = bind_live_engine("kafka", _sid("d"), tmp_path)
    assert not isinstance(kafka_dst, str), kafka_dst
    kafka_out = _transfer_pair(bound, kafka_dst)
    assert kafka_out["status"] == "passed", kafka_out


def test_pair_timeout_is_skip_never_pass(monkeypatch):
    import services.desktop_lab_cross as mod

    monkeypatch.setenv("DATAFLOW_CROSS_EXTENDED", "")
    monkeypatch.setattr(mod, "engines_for_run", lambda: ("sqlite",))

    def boom(fn, timeout_sec, *args, **kwargs):
        raise TimeoutError("exceeded 15s")

    monkeypatch.setattr(mod, "_call_with_timeout", boom)
    report = mod.run_live_engine_cross_matrix(persist=False)
    assert report["passed"] == 0
    assert report["failed"] == 0
    assert report["skipped"] == 1
    assert report["routes"][0]["status"] == "skipped"
    assert "source sqlite was not seeded" in report["routes"][0]["error"]
    assert report["unique_engines_seed_skipped"]
    assert "exceeded" in report["unique_engines_seed_skipped"][0]["error"]


def test_iceberg_filesystem_warehouse_counts_reachable(tmp_path):
    from src.transfer.models import EndpointConfig
    from tests.test_execute_tracked_universal_matrix import _endpoint_reachable

    ep = EndpointConfig(
        kind="database",
        format="iceberg",
        database=str(tmp_path / "wh"),
        table="t_sku",
        schema="default",
    )
    assert _endpoint_reachable(ep) is True


def test_adls_introspect_dispatch_is_implemented():
    import inspect

    from src.transfer import endpoint_intelligence as ei

    src = inspect.getsource(ei.introspect_endpoint)
    sample = inspect.getsource(ei._attach_db_sample)
    assert 'if fmt == "adls"' in src
    assert 'if fmt == "adls"' in sample
    assert "connectors.adls" in src


def test_ensure_postgis_installs_inside_docker_not_host_socket():
    import inspect

    from tests import desktop_lab_untested as ut

    src = inspect.getsource(ut._ensure_postgis) + inspect.getsource(ut._postgres_docker_id)
    assert "publish=5432" in src
    assert "docker" in src
    assert "CREATE EXTENSION IF NOT EXISTS postgis" in src

