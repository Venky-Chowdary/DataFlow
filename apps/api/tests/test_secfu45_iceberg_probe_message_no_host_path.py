"""Follow-up review 5 (L1): the Iceberg connection test names no host path or workspace id.

``iceberg_catalog.test_iceberg_catalog`` answered "Iceberg filesystem warehouse
writable at <absolute host path>". The path is the API host's (the Iceberg root,
the data volume) and, under the per-workspace root, ``_workspaces/<workspace id>``;
introspection (accept-source-schema, the source peek) copies the message to
the caller. Now it names the warehouse relative to the caller's own area.
"""

from __future__ import annotations

import pytest

WS_B = "ws-bravo"


@pytest.fixture
def root(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "REQUIRE_AUTH"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    jail = tmp_path / "iceroot"
    jail.mkdir()
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(jail))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    return jail.resolve()


def test_the_bound_message_is_workspace_relative(root):
    from connectors.iceberg_catalog import test_iceberg_catalog
    from services.export_paths import connector_export_scope

    with connector_export_scope(WS_B):
        ok, message = test_iceberg_catalog({"warehouse": "sales/mine", "table": "t"})
    assert ok, message
    assert "sales/mine" in message
    assert str(root) not in message and "_workspaces" not in message and WS_B not in message


def test_without_a_root_only_the_folder_name_is_shown(tmp_path, monkeypatch):
    from connectors.iceberg_catalog import test_iceberg_catalog

    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "REQUIRE_AUTH"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    warehouse = tmp_path / "host" / "wh"
    ok, message = test_iceberg_catalog({"warehouse": str(warehouse), "table": "t"})
    assert ok, message
    assert "wh" in message and str(tmp_path) not in message
