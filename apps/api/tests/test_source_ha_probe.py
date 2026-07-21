"""Source HA probe classification + MultiSubnetFailover URL (no network mocks)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_classify_sqlserver_standalone_when_empty():
    from services.source_ha_probe import classify_sqlserver_ag_rows

    probe = classify_sqlserver_ag_rows([])
    assert probe.role == "STANDALONE"
    assert probe.ha_enabled is False
    assert probe.topology == "none"
    fields = probe.job_fields()
    assert fields["source_ha_role"] == "STANDALONE"


def test_classify_sqlserver_primary_from_dmv_shape():
    from services.source_ha_probe import classify_sqlserver_ag_rows

    probe = classify_sqlserver_ag_rows([
        {
            "ag_name": "ag1",
            "replica_server_name": "sql-a",
            "role_desc": "PRIMARY",
            "operational_state_desc": "ONLINE",
            "connected_state_desc": "CONNECTED",
            "synchronization_health_desc": "HEALTHY",
        }
    ])
    assert probe.role == "PRIMARY"
    assert probe.ha_enabled is True
    assert probe.topology == "availability_group"
    assert probe.group_name == "ag1"
    assert probe.replica_name == "sql-a"


def test_classify_oracle_primary_and_standby():
    from services.source_ha_probe import classify_oracle_dg_row

    primary = classify_oracle_dg_row({
        "DATABASE_ROLE": "PRIMARY",
        "OPEN_MODE": "READ WRITE",
        "DATAGUARD_BROKER": "DISABLED",
        "DB_UNIQUE_NAME": "orcl",
    })
    assert primary.role == "PRIMARY"
    assert primary.topology == "none"

    standby = classify_oracle_dg_row({
        "DATABASE_ROLE": "PHYSICAL STANDBY",
        "OPEN_MODE": "MOUNTED",
        "DATAGUARD_BROKER": "ENABLED",
        "DB_UNIQUE_NAME": "orcl_stby",
    })
    assert standby.role == "PHYSICAL_STANDBY"
    assert standby.ha_enabled is True
    assert standby.topology == "data_guard"


def test_mssql_url_includes_multisubnet_failover():
    from connectors.generic_sql import _build_url

    url = _build_url({
        "type": "sqlserver",
        "host": "ag-listener.example",
        "port": 1433,
        "database": "app",
        "username": "sa",
        "password": "x",
        "multi_subnet_failover": True,
        "application_intent": "ReadWrite",
    })
    # SQLAlchemy URL — query params must carry ODBC MultiSubnetFailover.
    query = dict(url.query) if hasattr(url, "query") else {}
    assert query.get("MultiSubnetFailover") == "Yes"
    assert query.get("ApplicationIntent") == "ReadWrite"


def test_job_trust_caps_on_cursor_gap():
    from services.job_trust import compute_job_trust

    trust = compute_job_trust({
        "status": "failed",
        "records_processed": 10,
        "cdc_cursor_gap": True,
        "cdc_cursor_gap_code": "cdc_lsn_gap",
        "reconciliation": {"passed": False},
    })
    assert trust["score"] <= 28
    assert trust["cursor_gap"] is True
    assert trust["next_action"]["code"] == "cursor_gap"


def test_job_trust_exposes_source_ha_role():
    from services.job_trust import compute_job_trust

    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 100,
        "rejected_rows": 0,
        "reconciliation": {"passed": True},
        "source_ha_role": "PRIMARY",
    })
    assert trust["source_ha_role"] == "PRIMARY"
    assert trust["score"] >= 90
