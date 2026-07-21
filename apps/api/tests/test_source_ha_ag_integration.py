"""SQL Server Always On AG probe — live IT (honest skip without dual-node infra).

Honesty
-------
- Single-node / Express / compose SQL Server correctly returns ``STANDALONE``
  (or AG DMVs unavailable → treated as non-AG). That is **not** dual-node proof.
- Dual-node AG assertions run only when ``DATAFLOW_AG_LIVE=1`` **and** a real
  Always On listener/replica is reachable via the usual SQL Server env.
- This test does **not** simulate failover or invent continuous CDC across a
  retention gap — cursor-gap fail-closed remains the recovery path.
"""

from __future__ import annotations

import os
import socket
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

_AG_LIVE = os.getenv("DATAFLOW_AG_LIVE", "").strip().lower() in {"1", "true", "yes"}

CFG = {
    "type": "sqlserver",
    "host": os.getenv("DATAFLOW_AG_HOST")
    or os.getenv("DATAFLOW_MSSQL_HOST")
    or "localhost",
    "port": int(os.getenv("DATAFLOW_AG_PORT") or os.getenv("DATAFLOW_MSSQL_PORT") or "1433"),
    "database": os.getenv("DATAFLOW_AG_DATABASE")
    or os.getenv("DATAFLOW_MSSQL_DATABASE")
    or "dataflow",
    "username": os.getenv("DATAFLOW_AG_USER")
    or os.getenv("DATAFLOW_MSSQL_USER")
    or "sa",
    "password": os.getenv("DATAFLOW_AG_PASSWORD")
    or os.getenv("DATAFLOW_MSSQL_PASSWORD")
    or "DataFlow_CDC_2022!",
    "multi_subnet_failover": _AG_LIVE,
}


def _sqlserver_port_open() -> bool:
    host = str(CFG["host"])
    port = int(CFG["port"])
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def test_sqlserver_ha_probe_standalone_or_ag_on_reachable_host():
    """When SQL Server answers, probe must classify honestly (no fake AG)."""
    if not _sqlserver_port_open():
        pytest.skip("SQL Server not reachable — cannot run HA probe IT")

    from services.source_ha_probe import probe_source_ha_safe

    probe = probe_source_ha_safe(CFG)
    assert probe.dialect == "sqlserver"
    assert probe.role in {
        "STANDALONE",
        "PRIMARY",
        "SECONDARY",
        "RESOLVING",
        "UNKNOWN",
    }
    if probe.role == "STANDALONE":
        assert probe.ha_enabled is False
        assert probe.topology in {"none", "unknown"}
    if probe.topology == "availability_group":
        assert probe.ha_enabled is True
        assert probe.role in {"PRIMARY", "SECONDARY", "RESOLVING"}


@pytest.mark.skipif(
    not _AG_LIVE,
    reason=(
        "Dual-node Always On AG IT requires DATAFLOW_AG_LIVE=1 and a real AG "
        "(listener/replica). Single-node compose returns STANDALONE — not AG proof."
    ),
)
def test_sqlserver_dual_node_ag_role_when_live():
    """Assert real AG topology — only when operator opts into AG live infra."""
    if not _sqlserver_port_open():
        pytest.skip("DATAFLOW_AG_LIVE set but SQL Server host/port not reachable")

    from services.source_ha_probe import probe_source_ha_safe

    probe = probe_source_ha_safe(CFG)
    assert probe.ha_enabled is True, probe.message
    assert probe.topology == "availability_group", probe.message
    assert probe.role in {"PRIMARY", "SECONDARY"}, probe.message
    assert probe.group_name, "AG group_name expected on dual-node probe"
    fields = probe.job_fields()
    assert fields["source_ha_role"] == probe.role
    assert fields["source_ha_topology"] == "availability_group"
