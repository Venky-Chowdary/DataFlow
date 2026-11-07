"""Recovery / rollback honesty — never claim product undo that does not exist."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services.recovery_honesty import (  # noqa: E402
    BRANCH_SWITCH_CLAIMED,
    CDC_REWIND_CLAIMED,
    STAGING_SWAP_CLAIMED,
    TRANSFER_UNDO_CLAIMED,
    WAREHOUSE_RESTORE_CLAIMED,
    honesty_dict,
)
from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    with TestClient(app) as c:
        yield c


def test_recovery_honesty_refuses_product_undo_claims() -> None:
    assert TRANSFER_UNDO_CLAIMED is False
    assert STAGING_SWAP_CLAIMED is False
    assert WAREHOUSE_RESTORE_CLAIMED is False
    assert BRANCH_SWITCH_CLAIMED is False
    assert CDC_REWIND_CLAIMED is False

    h = honesty_dict()
    assert h["transfer_undo_claimed"] is False
    assert h["staging_swap_claimed"] is False
    assert h["warehouse_restore_claimed"] is False
    assert h["branch_switch_claimed"] is False
    assert h["cdc_rewind_claimed"] is False

    caps = h["capabilities"]
    assert caps["quarantine_holdout"]["available"] is True
    assert caps["checkpoint_resume"]["available"] is True
    assert caps["transfer_undo"]["available"] is False
    assert caps["staging_swap"]["available"] is False
    assert caps["warehouse_restore"]["available"] is False
    assert "MIGRATION_ROLLBACK" in h["operator_runbook"]


def test_workspace_security_posture_includes_recovery_honesty(client):
    """Workspace posture must publish recovery honesty — refuse invent of product undo."""
    ws = client.post("/api/v1/team/workspaces", json={"name": "Recovery Audit"}).json()["workspace"]
    ws_id = ws["id"]
    response = client.get(
        "/api/v1/workspace/security/posture",
        headers={"X-Workspace-Id": ws_id},
    )
    assert response.status_code == 200, response.text
    posture = response.json()
    assert "recovery_honesty" in posture, (
        "Security posture must surface recovery_honesty SSOT "
        "(same class as cdc_honesty) — refuse invent of product undo"
    )
    rh = posture["recovery_honesty"]
    assert rh["transfer_undo_claimed"] is False
    assert rh["warehouse_restore_claimed"] is False
    assert posture["transfer_undo_claimed"] is False
