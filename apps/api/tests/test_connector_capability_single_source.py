"""One function answers for connector capabilities (audit M-4).

``get_connector_capability`` composes three facts that each have one owner:
driver read/write (``connector_capabilities``), CDC source (the Validate gate's
list) and merge strategy (``merge_registry``). These tests hold the owners and
the function together, so a registry can no longer say something the gate, the
writers or the catalog contradict.
"""

from __future__ import annotations

import pytest

from connectors.merge_registry import MERGE_STRATEGIES
from services.connector_capability_registry import (
    CAPABILITY_REGISTRY,
    capability_counts,
    export_live_capability_matrix,
    get_connector_capability,
    supports_cdc,
)
from services.preflight_cursor_gate import (
    CDC_CAPABLE_SOURCES,
    SOURCE_PRODUCTS_JUDGED_BY_OWN_NAME,
    build_sync_contract_gate,
    cdc_source_supported,
)
from src.transfer import connector_capabilities as cc
from src.transfer.registry import get_capabilities as transfer_capabilities

_ALL_KEYS = sorted(set(CAPABILITY_REGISTRY) | set(cc._DRIVER_CAPS) | set(cc._FILE_CAPS))


def _gate_refuses_cdc(source_type: str) -> bool:
    """Ask the real Validate gate whether ``source_type`` may drive CDC."""
    gate = build_sync_contract_gate(
        [{"name": "t", "primary_key": ["id"], "cursor_field": "id", "selected": True}],
        sync="cdc",
        validation="strict",
        dest="postgresql",
        src=source_type,
        kind="database",
        source_columns=["id"],
        pass_status="pass",
        block_status="block",
    )
    text = " ".join(str(v) for v in gate.values())
    return f"CDC is not supported for source type '{source_type}'" in text


@pytest.mark.parametrize("key", _ALL_KEYS)
def test_cdc_flag_is_exactly_what_the_gate_enforces(key: str) -> None:
    cap = get_connector_capability(key)
    driver = cc.resolve_driver_type(cap["normalized_key"])
    expected = cdc_source_supported(key, driver) and cdc_source_supported(cap["normalized_key"], driver)
    assert cap["supports_cdc"] is expected, key
    assert supports_cdc(key) is expected, key
    # The function and the gate must give the same verdict for the id itself.
    if key in cc._DRIVER_CAPS or key in cc._FILE_CAPS:
        assert _gate_refuses_cdc(key) is (not cap["supports_cdc"]), key


def test_engines_the_audit_named_no_longer_claim_cdc() -> None:
    for key in ("qdrant", "pgvector", "kafka"):
        cap = get_connector_capability(key)
        assert cap["supports_cdc"] is False, key
        assert cap["supports_cdc_sink"] is True, key
        assert _gate_refuses_cdc(key), key


def test_cdc_source_engines_are_the_gate_list() -> None:
    engines = set(capability_counts()["cdc_source_engines"])
    assert engines == {"postgresql", "mysql", "mongodb", "oracle", "sqlserver"}
    assert engines <= CDC_CAPABLE_SOURCES
    # The public capabilities payload reads the same list.
    assert set(transfer_capabilities()["cdc_capable_sources"]) == set(CDC_CAPABLE_SOURCES)


@pytest.mark.parametrize("product", sorted(SOURCE_PRODUCTS_JUDGED_BY_OWN_NAME))
def test_a_driver_alias_does_not_carry_cdc_to_another_product(product: str) -> None:
    """TimescaleDB reads through the PostgreSQL driver and is still not a CDC source."""
    assert get_connector_capability(product)["supports_cdc"] is False
    assert cdc_source_supported(product, "postgresql") is False


@pytest.mark.parametrize("alias", ["pg", "postgres", "mongo", "mssql", "amazon_rds_postgresql"])
def test_plain_aliases_keep_their_engine_cdc(alias: str) -> None:
    assert get_connector_capability(alias)["supports_cdc"] is True


@pytest.mark.parametrize("key", _ALL_KEYS)
def test_write_flags_never_exceed_the_driver_table(key: str) -> None:
    """A connector the driver table cannot write to must not claim upsert or merge."""
    cap = get_connector_capability(key)
    declared = cc._declared_capabilities(cc.resolve_driver_type(cap["normalized_key"]))
    if not declared.get("write"):
        assert not cap.get("supports_upsert"), key
        assert not cap.get("supports_merge"), key


def test_email_and_file_formats_do_not_claim_upsert() -> None:
    assert get_connector_capability("email")["supports_upsert"] is False
    for fmt in cc._FILE_CAPS:
        cap = get_connector_capability(fmt)
        assert cap["supports_upsert"] is False, fmt
        assert cap["supports_merge"] is False, fmt


@pytest.mark.parametrize("dialect", sorted(MERGE_STRATEGIES))
def test_merge_strategy_comes_from_the_merge_registry(dialect: str) -> None:
    cap = get_connector_capability(dialect)
    owner = MERGE_STRATEGIES.get(cap["normalized_key"]) or MERGE_STRATEGIES.get(
        cc.resolve_driver_type(cap["normalized_key"])
    )
    assert cap["merge_strategy"] == (owner or {}).get("strategy", "")
    if dialect in CAPABILITY_REGISTRY:
        # A dialect with a declared keyed-write strategy is an upsert destination.
        assert CAPABILITY_REGISTRY[dialect]["supports_upsert"] is True, dialect


def test_one_published_count() -> None:
    counts = capability_counts()
    live = cc.transfer_live_driver_types()
    assert counts["transfer_live_drivers"] == len(live) == cc.manifest_summary()["transfer_live_count"]
    matrix = export_live_capability_matrix()
    assert matrix["transfer_live_driver_count"] == counts["transfer_live_drivers"]
    assert matrix["unique_driver_count"] == counts["transfer_ready_catalog_drivers"]
    assert matrix["catalog_transfer_ready_tile_count"] == counts["transfer_ready_catalog_tiles"]
    assert set(counts["definitions"]) >= {
        "transfer_live_drivers",
        "transfer_ready_catalog_tiles",
        "transfer_ready_catalog_drivers",
    }
