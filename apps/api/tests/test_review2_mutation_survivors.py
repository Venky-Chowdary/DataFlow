"""Security review round 2: tests that kill the mutants that survived its pass.

- ``resume_no_residency``: the resume route's own residency refusal. (The
  review's plugin imported ``src.routers.connectors_router`` through the
  package, which re-exports the *router object* under that name, so that
  mutant was never applied; this test pins the route-level 403 anyway, and
  that the route refuses before it starts anything.)
- ``migrate_follows_symlinks``: the flat-exports migration never walks into a
  symlinked directory, so it never moves a file that lives outside the
  exports folder.

``verify_route_no_admin_check`` is killed in ``test_tenancy_route_scoping``
(a workspace admin verifying a connector of its own workspace gets 403).
"""

from __future__ import annotations

import os

from test_review_h2_h3_residency_at_run_and_query_export import (  # noqa: F401
    _job_into,
    _move,
    _s3_connector,
    eu_workspace,
    writes,
)
from _boto3_client_stub import stub_boto3_client  # noqa: F401  (the imported `writes` fixture needs it)
from tenancy_world import tenancy_world  # noqa: F401


def test_resume_route_itself_answers_403_before_starting_anything(eu_workspace, writes, monkeypatch):
    from services import checkpoint_service
    from services.mongodb_service import get_mongodb_service

    import src.transfer.background as bg

    world = eu_workspace
    bucket = _s3_connector(world, "eu-west-1")
    job_id, _job = _job_into(world, bucket)
    get_mongodb_service().update_job_status(job_id, "failed", error="interrupted")
    _move(world, bucket, "us-east-1")
    monkeypatch.setattr(checkpoint_service, "evaluate_resume_safety", lambda *_a, **_k: {"ok": True})
    started: list[tuple] = []
    monkeypatch.setattr(bg, "run_transfer_async", lambda *a, **k: started.append(a))

    refused = world.a.post(f"/api/v1/connectors/jobs/{job_id}/resume")
    assert refused.status_code == 403, refused.text
    assert refused.json()["detail"].startswith("Data residency violation:")
    assert started == [] and not writes
    stored = get_mongodb_service().get_job(job_id)
    assert stored["status"] == "failed" and stored.get("error") == "interrupted"


def test_migration_never_follows_a_symlinked_directory(tmp_path, monkeypatch):
    from services import export_migration
    from services.export_paths import workspace_exports_dir

    exports = tmp_path / "exports"
    outside = tmp_path / "elsewhere"
    exports.mkdir()
    outside.mkdir()
    (outside / "export_job-sym-1.csv").write_text("NOT-AN-EXPORT", encoding="utf-8")
    os.symlink(outside, exports / "linked", target_is_directory=True)
    monkeypatch.delenv("DATAWRAP_EXPORTS_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(exports))
    ws = "0b5c3f8e-0000-4000-8000-00000000000a"
    # Every path would be "owned" if the walk reached it.
    monkeypatch.setattr(export_migration, "_owner", lambda *_a, **_k: ws)
    monkeypatch.setattr(export_migration, "_recorded_owners", lambda *_a, **_k: {})

    assert export_migration._flat_files(exports.resolve()) == []
    summary = export_migration.migrate_flat_exports()
    assert summary["moved"] == 0
    assert (outside / "export_job-sym-1.csv").read_text(encoding="utf-8") == "NOT-AN-EXPORT"
    target = workspace_exports_dir(ws, create=False)
    assert not target.exists() or not any(target.rglob("*.csv"))
