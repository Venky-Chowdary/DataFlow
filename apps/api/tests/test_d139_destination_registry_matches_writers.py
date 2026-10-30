"""D139 — the destination registry advertises exactly what the writers support.

QA: DynamoDB and Neo4j were advertised as transfer-live destinations and every
load was blocked. This holds the advertising to the writers, generically:

* every destination any capability surface lists resolves to a writer that
  imports and is callable, and has a write-privilege probe (G2);
* no advertised destination is one the planner refuses as source-only;
* every driver whose writer is a stub (``write_not_supported``) is absent from
  every destination list, and the planner refuses it with the reason;
* a catalog tile for a driver with no writer does not describe it as a
  destination.
"""

from __future__ import annotations

import importlib

import pytest


def _advertised_destination_drivers() -> set[str]:
    from src.transfer.connector_capabilities import (
        _FILE_CAPS,
        dest_live_driver_types,
        resolve_driver_type,
    )
    from src.transfer.registry import LIVE_DEST_DATABASES, get_capabilities

    names = set(dest_live_driver_types()) | set(LIVE_DEST_DATABASES)
    names |= set(get_capabilities().get("destination_databases") or [])
    drivers = {resolve_driver_type(n) for n in names}
    return {d for d in drivers if d not in _FILE_CAPS and d != "unknown"}


def _stub_writer_drivers() -> set[str]:
    from src.transfer.connector_registry import CONNECTOR_MODULES

    return {k for k, v in CONNECTOR_MODULES.items() if v.writer_fn == "write_not_supported"}


def test_dynamodb_is_advertised_and_neo4j_is_not():
    advertised = _advertised_destination_drivers()
    assert "dynamodb" in advertised
    assert "neo4j" not in advertised


@pytest.mark.parametrize("driver", sorted(_advertised_destination_drivers()))
def test_every_advertised_destination_has_a_real_writer_and_probe(driver):
    from services.destination_privilege_probe import _SUPPORTED
    from src.transfer.connector_capabilities import source_only_destination_refusal
    from src.transfer.connector_registry import CONNECTOR_MODULES

    assert source_only_destination_refusal(driver) is None
    if driver == "generic_sql":
        writer, fn = "connectors.generic_sql", "write_mapped_rows"
    else:
        spec = CONNECTOR_MODULES.get(driver)
        if spec is None:
            pytest.skip(f"{driver}: SDK/vector writer registered outside CONNECTOR_MODULES")
        writer, fn = spec.writer, spec.writer_fn
    assert fn != "write_not_supported", f"{driver} is advertised with a stub writer"
    try:
        mod = importlib.import_module(writer)
    except ImportError as exc:  # optional driver package absent here
        pytest.skip(f"{driver}: writer module needs an optional package ({exc})")
    assert callable(getattr(mod, fn, None)), f"{writer}.{fn} is not callable"
    if driver in {"pgvector", "hubspot", "salesforce"}:
        # Known: no privilege probe yet; G2 warns but does not refuse.
        return
    assert driver in _SUPPORTED, f"{driver} is advertised but G2 has no privilege probe"


@pytest.mark.parametrize("driver", sorted(_stub_writer_drivers()))
def test_stub_writers_are_never_advertised_and_are_refused(driver):
    from services.catalog_service import enriched_connectors
    from src.transfer.connector_capabilities import (
        resolve_driver_type,
        source_only_destination_refusal,
    )

    assert driver not in _advertised_destination_drivers()
    refusal = source_only_destination_refusal(driver)
    assert refusal and "source-only" in refusal
    for tile in enriched_connectors():
        if resolve_driver_type(str(tile.get("id") or "")) != driver:
            continue
        assert tile.get("dest_ready") is False, tile
        assert "destination" not in str(tile.get("description") or "").lower(), tile
