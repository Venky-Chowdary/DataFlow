"""Re-measure Track D scheduler DST and workspace-ownership cells.

The scale harness recorded these as fail, then the product and the harness
expectation both changed (``workspace_access`` sibling-scope 404; DST graded
as three cases instead of one number). They were never re-run. This file is
that re-measure: the same zoneinfo arithmetic as ``tests.scale.scheduler_cells``
and the same sibling-header tenancy the control-plane cell asks, without the
100K beat.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import schedule_store, team_store, user_store  # noqa: E402
from services.auth_rate_limit import reset_auth_rate_limits  # noqa: E402
from services.schedule_store import compute_next_run  # noqa: E402
from src.main import app  # noqa: E402
from tests.scale.modes_matrix import Matrix  # noqa: E402
from tests.scale.scheduler_cells import (  # noqa: E402
    _expected_daily_cron,
    cadence_cells,
)


def test_track_d_cadence_cells_pass() -> None:
    """The Track D cadence suite, including the DST boundary cell."""
    matrix = Matrix()
    cadence_cells(matrix)
    by_mode = {cell.mode: cell for cell in matrix.cells}
    for name in (
        "scheduler interval next-run",
        "scheduler cron next-run (tz)",
        "scheduler DST boundary",
    ):
        cell = by_mode[name]
        assert cell.status == "pass", f"{name}: {cell.notes} {cell.detail}"


def test_dst_offset_rederived_spring_forward_and_fall_back() -> None:
    """Independent zoneinfo vs product, named the three DST ways to be wrong."""
    ny = ZoneInfo("America/New_York")

    def _next(base_local: datetime, cron: str) -> datetime:
        return datetime.fromisoformat(
            compute_next_run(
                "daily",
                base_local.astimezone(timezone.utc),
                cron=cron,
                tz="America/New_York",
            )
        )

    est_base = datetime(2026, 3, 6, 15, 0, tzinfo=ny)
    edt_base = datetime(2026, 3, 9, 15, 0, tzinfo=ny)
    noon_est = _next(est_base, "0 12 * * *")
    noon_edt = _next(edt_base, "0 12 * * *")
    assert noon_est == _expected_daily_cron(12, 0, "America/New_York", est_base)
    assert noon_edt == _expected_daily_cron(12, 0, "America/New_York", edt_base)
    assert noon_est.astimezone(ny).utcoffset() != noon_edt.astimezone(ny).utcoffset()

    gap_base = datetime(2026, 3, 7, 12, 0, tzinfo=ny)
    gap = _next(gap_base, "30 2 * * *")
    gap_local = gap.astimezone(ny)
    assert gap == _expected_daily_cron(2, 30, "America/New_York", gap_base)
    assert (gap_local.hour, gap_local.minute) == (2, 30)
    assert gap_local.date() > date(2026, 3, 8)

    fold_base = datetime(2026, 10, 31, 12, 0, tzinfo=ny)
    fold = _next(fold_base, "30 1 * * *")
    fold_local = fold.astimezone(ny)
    assert fold == _expected_daily_cron(1, 30, "America/New_York", fold_base)
    assert (fold_local.hour, fold_local.minute) == (1, 30)
    assert fold_local.date() == date(2026, 11, 1)
    assert fold_local.fold == 0


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    reset_auth_rate_limits()
    return tmp_path


def _admin(isolated) -> TestClient:
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _workspace(client: TestClient, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def test_sibling_workspace_header_cannot_read_or_list_schedule(isolated) -> None:
    """Same actor, two workspaces: X-Workspace-Id is the tenant boundary.

    Track D item 6: a user in two workspaces must not read one workspace's
    schedule while declaring the other in the header. List already filtered by
    scope; id-addressed GET has to agree.
    """
    admin = _admin(isolated)
    mine = _workspace(admin, "Mine")
    sibling = _workspace(admin, "Sibling")
    sched = schedule_store.create_schedule(
        {
            "name": "Mine nightly",
            "source_connector_id": "src-1",
            "source_table": "orders",
            "dest_connector_id": "dst-1",
            "dest_table": "orders_wh",
            "interval": "daily",
            "workspace_id": mine,
        }
    )

    listed = admin.get("/api/v1/schedules/", headers={"X-Workspace-Id": sibling})
    assert listed.status_code == 200, listed.text
    assert all(row["id"] != sched.id for row in listed.json())

    crossed = admin.get(
        f"/api/v1/schedules/{sched.id}", headers={"X-Workspace-Id": sibling}
    )
    assert crossed.status_code == 404, crossed.text

    owned = admin.get(
        f"/api/v1/schedules/{sched.id}", headers={"X-Workspace-Id": mine}
    )
    assert owned.status_code == 200, owned.text
    assert owned.json()["id"] == sched.id


def test_non_member_cannot_read_or_create_schedule(isolated) -> None:
    admin = _admin(isolated)
    mine = _workspace(admin, "Acme")
    sched = schedule_store.create_schedule(
        {
            "name": "Acme nightly",
            "source_connector_id": "src-1",
            "source_table": "orders",
            "dest_connector_id": "dst-1",
            "dest_table": "orders_wh",
            "interval": "daily",
            "workspace_id": mine,
        }
    )
    other = _workspace(admin, "Other-co")
    issued = admin.post(
        "/api/v1/team/users",
        json={
            "email": "stranger@example.com",
            "platform_role": "member",
            "workspace_id": other,
            "workspace_role": "editor",
        },
    )
    assert issued.status_code == 200, issued.text
    stranger = TestClient(app)
    login = stranger.post(
        "/api/v1/auth/login",
        json={
            "email": "stranger@example.com",
            "password": issued.json()["temporary_password"],
        },
    )
    assert login.status_code == 200, login.text
    stranger.headers["Authorization"] = f"Bearer {login.json()['token']}"

    denied = stranger.get(
        f"/api/v1/schedules/{sched.id}", headers={"X-Workspace-Id": mine}
    )
    assert denied.status_code in (403, 404), denied.text

    write_denied = stranger.post(
        "/api/v1/schedules/",
        json={
            "name": "intruder",
            "source_connector_id": "src-1",
            "source_table": "orders",
            "dest_connector_id": "dst-1",
            "dest_table": "orders_wh",
            "interval": "daily",
        },
        headers={"X-Workspace-Id": mine},
    )
    assert write_denied.status_code in (403, 404), write_denied.text
