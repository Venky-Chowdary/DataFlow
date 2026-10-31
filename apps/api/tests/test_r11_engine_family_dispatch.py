"""QA round 11, item 1 — every duplex SQL tile gets its engine family's answer.

QA on 9c75ada4: ``alloydb``, ``amazon_rds_postgresql``,
``azure_database_for_postgresql`` and ``google_cloud_sql_postgresql`` were
blocked at preflight ("Duplicate-key probe not implemented", "lossy
TIMESTAMPTZ→TEXT", "not a supported destination", "existence unknown") while
execution dialed them through the PostgreSQL driver. Preflight dispatched on
the raw catalog tile id through private alias lists; execution used
``resolve_driver_type``. ``timescaledb`` and ``pgvector`` worked only because
they had been hand-patched into each list.

This matrix runs over EVERY catalog tile whose resolved driver is a duplex SQL
engine (PostgreSQL, MySQL, SQL Server, Oracle families) and asks each dispatch
site the same question it asks for the family's own id. The expected family
comes from the execution resolvers (``resolve_driver_type`` +
``source_read_driver``), so a tile added to the catalog tomorrow is covered
without editing this file.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.transfer.connector_capabilities import resolve_driver_type, source_read_driver

_CATALOG = Path(__file__).resolve().parents[1] / "data" / "connector_catalog.json"
_CANON = {"postgresql": "postgresql", "mysql": "mysql", "sqlserver": "sqlserver", "oracle": "oracle"}


def _execution_family(tile: str) -> str:
    return source_read_driver(resolve_driver_type(tile))


def _duplex_tiles() -> list[tuple[str, str]]:
    rows = json.loads(_CATALOG.read_text(encoding="utf-8"))["connectors"]
    out = []
    for row in rows:
        family = _execution_family(row["id"])
        if family in _CANON:
            out.append((row["id"], family))
    # Alias spellings saved rows and the resolver accept that are not tiles.
    for alias, family in (
        ("amazon_aurora_postgresql", "postgresql"),
        ("amazon_aurora_mysql", "mysql"),
        ("azure_sql", "sqlserver"),
        ("mssql", "sqlserver"),
        ("percona_mysql", "mysql"),
    ):
        if (alias, family) not in out:
            out.append((alias, family))
    return sorted(set(out))


TILES = _duplex_tiles()
IDS = [tile for tile, _family in TILES]


def test_matrix_covers_every_family_and_the_hosted_twins():
    families = {family for _tile, family in TILES}
    assert families == set(_CANON)
    names = set(IDS)
    for tile in (
        "alloydb", "amazon_rds_postgresql", "azure_database_for_postgresql",
        "google_cloud_sql_postgresql", "amazon_aurora", "amazon_rds_mysql",
        "azure_database_for_mysql", "google_cloud_sql_mysql", "amazon_rds_sql_server",
        "google_cloud_sql_sql_server", "azure_sql_database", "amazon_rds_oracle",
        "oracle_autonomous_warehouse", "timescaledb", "pgvector",
    ):
        assert tile in names, tile


def test_catalog_has_an_aurora_postgresql_tile():
    rows = json.loads(_CATALOG.read_text(encoding="utf-8"))["connectors"]
    tile = next((r for r in rows if r["id"] == "amazon_aurora_postgresql"), None)
    assert tile is not None, "no Amazon Aurora PostgreSQL tile in the catalog"
    assert _execution_family("amazon_aurora_postgresql") == "postgresql"
    # The original tile is Aurora MySQL; its label must say so.
    aurora = next(r for r in rows if r["id"] == "amazon_aurora")
    assert "mysql" in aurora["name"].lower()


def test_alias_spellings_name_the_catalog_ids():
    from src.transfer.connector_capabilities import CATALOG_ID_ALIASES

    assert CATALOG_ID_ALIASES.get("azure_database_for_postgresql") == "postgresql"
    assert CATALOG_ID_ALIASES.get("azure_database_for_mysql") == "mysql"


# --------------------------------------------------------------- ddl_type


_LOGICAL = [
    "TIMESTAMPTZ", "TIMESTAMP", "DATE", "TIME", "DECIMAL(10,2)", "NUMERIC(38,9)",
    "BIGINT", "INTEGER", "BOOLEAN", "TEXT", "VARCHAR(40)", "JSON", "UUID", "BYTEA",
    "DOUBLE PRECISION",
]


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_ddl_type_is_the_family_ddl(tile, family):
    from services.decision_kernel import type_invent
    from services.type_system import ddl_type

    for logical in _LOGICAL:
        assert ddl_type(tile, logical) == ddl_type(family, logical), (tile, logical)
        assert type_invent.ddl_type(tile, logical) == type_invent.ddl_type(family, logical), (
            tile, logical,
        )


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_type_system_normaliser_and_g6_kind_are_the_family(tile, family):
    from services.db_type_utils import normalize_dest_kind
    from services.dest_dialect_facts import _normalize_dest_db
    from services.dialect_profiles import default_schema_for, normalize_driver

    assert _normalize_dest_db(tile) == _normalize_dest_db(family)
    assert normalize_dest_kind(tile) == normalize_dest_kind(family)
    assert normalize_driver(tile) == normalize_driver(family)
    assert default_schema_for(tile) == default_schema_for(family)


# ------------------------------------------------------------- dup probe


def _cfg(tile: str) -> dict[str, Any]:
    return {
        "type": tile, "host": "127.0.0.1", "port": 1, "database": "d",
        "username": "u", "password": "p", "schema": "",
    }


def _dup_probe(monkeypatch, tile: str):
    import services.source_duplicate_probe as sdp

    monkeypatch.setattr(sdp, "_enforced_key_proves_unique", lambda *a, **k: None)
    monkeypatch.setattr(sdp, "_sql_duplicates", lambda *a, **k: [])
    return sdp.probe_source_duplicate_keys_result(
        source_config=_cfg(tile), source_table="t", primary_key="id"
    )


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_source_duplicate_probe_runs_for_the_family(monkeypatch, tile, family):
    got = _dup_probe(monkeypatch, tile)
    want = _dup_probe(monkeypatch, family)
    assert want.status == "ran"
    assert got.status == want.status, got.message
    assert "not implemented" not in (got.message or "")


# ------------------------------------------------------- orphan probes


_FK = [{"columns": ["pid"], "referred_table": "p", "referred_columns": ["id"]}]
_MAPS = [{"source": "pid", "target": "pid"}, {"source": "id", "target": "id"}]


def _sample_orphans(monkeypatch, tile: str) -> dict:
    import services.sample_orphan_probe as sop

    monkeypatch.setattr(sop, "_sql_existing_parent_keys", lambda cfg, **k: list(k.get("values") or []))
    monkeypatch.setattr(sop, "_sql_existing_parent_tuples", lambda cfg, **k: list(k.get("values") or []))
    return sop.probe_sample_fk_orphans(
        sample_rows=[{"id": 1, "pid": 7}], mappings=_MAPS, foreign_keys=_FK,
        source_config=_cfg(tile),
    )


def _population_orphans(monkeypatch, tile: str) -> dict:
    import services.population_orphan_probe as pop

    monkeypatch.setattr(
        pop, "_sql_population_orphan_scan",
        lambda cfg, **k: {"orphan_count": 0, "complete": True, "samples": [], "scanned": 1},
    )
    return pop.probe_population_fk_orphans(
        child_table="c", mappings=_MAPS, foreign_keys=_FK, source_config=_cfg(tile),
    )


def _verdict(report: dict) -> tuple:
    notes = " ".join(str(f.get("message") or "") for f in report.get("findings") or [])
    return bool(report.get("ran")), report.get("coverage"), "not implemented" in notes


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_orphan_probes_run_for_the_family(monkeypatch, tile, family):
    sample, sample_want = _sample_orphans(monkeypatch, tile), _sample_orphans(monkeypatch, family)
    assert _verdict(sample_want)[0] is True
    assert _verdict(sample) == _verdict(sample_want), sample.get("note")
    pop, pop_want = _population_orphans(monkeypatch, tile), _population_orphans(monkeypatch, family)
    assert _verdict(pop_want)[0] is True
    assert _verdict(pop) == _verdict(pop_want), pop.get("note")


# ------------------------------------------------------- collision probe


def _collision(monkeypatch, tile: str):
    import services.destination_key_collision_probe as dkc

    monkeypatch.setattr(dkc, "_sql_existing_keys", lambda *a, **k: [])
    monkeypatch.setattr(dkc, "_sql_existing_tuples", lambda *a, **k: [])
    single = dkc.probe_destination_key_collisions(
        destination_config=_cfg(tile), destination_db_type=tile,
        destination_table="t", key_column="id", values=[1, 2],
    )
    tuples = dkc.probe_destination_tuple_collisions(
        destination_config=_cfg(tile), destination_db_type=tile,
        destination_table="t", columns=["id", "k"], tuples=[(1, "a")],
    )
    return single.status, tuples.status


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_destination_collision_probe_runs_for_the_family(monkeypatch, tile, family):
    want = _collision(monkeypatch, family)
    assert want == ("ran", "ran")
    assert _collision(monkeypatch, tile) == want


# ------------------------------------------------------- privilege probe


def _privilege_route(monkeypatch, tile: str) -> str:
    import services.destination_privilege_probe as dpp

    seen: list[str] = []

    def fake(name):
        def _probe(*a, **k):
            seen.append(name)
            return dpp.PrivilegeProbeResult(
                can_write=True, can_create_table=True, status="ok", detail="fake", engine=name,
            )
        return _probe

    for name in ("_probe_postgres_family", "_probe_mysql", "_probe_sqlserver", "_probe_oracle"):
        monkeypatch.setattr(dpp, name, fake(name))
    result = dpp.probe_destination_privileges(
        tile, host="127.0.0.1", port=1, database="d", schema="s", table="t",
        username="u", password="p",
    )
    return seen[0] if seen else f"unavailable: {result.detail}"


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_destination_privilege_probe_dispatches_to_the_family(monkeypatch, tile, family):
    want = _privilege_route(monkeypatch, family)
    assert not want.startswith("unavailable"), want
    assert _privilege_route(monkeypatch, tile) == want


# ----------------------------------------------------------- FK catalog


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_foreign_key_catalog_probe_is_the_family_probe(tile, family):
    from services import foreign_key_metadata as fkm

    assert fkm.catalog_probe_dialect(family) is not None
    assert fkm.catalog_probe_dialect(tile) == fkm.catalog_probe_dialect(family)
    assert fkm.validation_catalog_dialect(tile) == fkm.validation_catalog_dialect(family)


# --------------------------------------------------- destination introspect


def _introspect_route(monkeypatch, tile: str) -> tuple[str, bool]:
    import connectors.mysql as mysql_mod
    import connectors.postgresql as pg_mod
    import src.transfer.endpoint_intelligence as ei
    from src.transfer.models import EndpointConfig

    seen: list[str] = []

    def fake(name):
        def _probe(**_k):
            seen.append(name)
            return SimpleNamespace(
                ok=True, tables=["t"], table_types={}, tables_truncated=False,
                message="ok", error="",
            )
        return _probe

    monkeypatch.setattr(pg_mod, "test_postgresql", fake("postgresql"))
    monkeypatch.setattr(mysql_mod, "test_mysql", fake("mysql"))
    monkeypatch.setattr(ei, "_attach_db_sample", lambda out, endpoint, *a, **k: seen.append("sample"))
    endpoint = EndpointConfig(
        kind="database", format=tile, host="127.0.0.1", port=1, database="d",
        username="u", password="p", table="t",
    )
    out = ei._introspect_endpoint_impl(endpoint)
    implemented = "not yet implemented" not in str(out.get("message") or "")
    return "+".join(seen), implemented


@pytest.mark.parametrize("tile,family", TILES, ids=IDS)
def test_destination_introspect_takes_the_family_branch(monkeypatch, tile, family):
    want = _introspect_route(monkeypatch, family)
    assert want[1] is True and "sample" in want[0]
    assert _introspect_route(monkeypatch, tile) == want


# ------------------------------------------------- one resolver, no lists


def test_engine_family_is_the_execution_resolver():
    from src.transfer.engine_family import engine_family

    for tile, family in TILES:
        assert engine_family(tile) == family, tile
    assert engine_family("pgvector") == "postgresql"
    assert engine_family("redshift") == "redshift"
    assert engine_family("duckdb") == "duckdb"
    assert engine_family("") == ""
