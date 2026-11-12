"""Security review M3: the upgrade path for exports written before workspace folders.

Per-workspace export folders (audit H-2) left three gaps for a deployment that
already had exports in the flat ``exports/`` folder:

* a client-named flat file (``exports/orders.csv``) answered 404 to the
  workspace that wrote it; only a platform administrator could fetch it;
* a recurring append export found no file in its new workspace folder and
  silently started a fresh one holding only the new increment;
* deleting a workspace left ``exports/_workspaces/<id>/`` on disk.

Now a flat file whose owner the job records establish is moved into that
workspace's folder (at startup, and when an append first needs it), a file
with no provable single owner stays where it is (platform administrators
only, as before), and workspace teardown removes the workspace's folder.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401


@pytest.fixture
def world(tenancy_world, monkeypatch):  # noqa: F811
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    from services.export_paths import exports_dir

    tenancy_world.root = exports_dir()
    return tenancy_world


def _flat(world, relative: str, text: str, *owners: str) -> Path:
    """A pre-upgrade export in the flat folder, with one job record per owner naming it."""
    path = world.root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    for workspace_id in owners:
        world.job(
            workspace_id, destination_kind="file_export",
            destination_summary={"filename": path.name, "path": str(path), "download_url": f"/api/v1/transfer/download/{path.name}"},
        )
    return path


def _tree(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): p.read_text(encoding="utf-8") for p in sorted(root.rglob("*")) if p.is_file()}


def _get(client, name: str) -> int:
    return client.get(f"/api/v1/transfer/download/{name}").status_code


def test_flat_exports_move_to_the_workspace_their_job_records_name(world):
    from services.export_migration import migrate_flat_exports
    from services.mongodb_service import get_mongodb_service

    a, b = world.workspace_a, world.workspace_b
    _flat(world, "orders.csv", "A-ORDERS\n", a)
    _flat(world, "nightly/n.csv", "A-NIGHTLY\n", a)
    _flat(world, "b-only.csv", "B-ONLY\n", b)
    _flat(world, "legacy_client.csv", "NO-RECORD\n")
    _flat(world, "shared.csv", "TWO-WORKSPACES\n", a, b)
    _flat(world, "unscoped.csv", "UNSCOPED-JOB\n", "")
    job = world.job(a, name="jl")
    get_mongodb_service().update_job_status(job, "completed", destination_summary={"filename": f"export_{job}.csv"})
    _flat(world, f"export_{job}.csv", "A-JOB-EXPORT\n")
    _flat(world, f"quarantine_{job}_deadbeef.csv", "A-QUARANTINE\n")
    other = world.job(a, name="not-in-summary")
    _flat(world, f"export_{other}.csv", "NOT-IN-SUMMARY\n")
    _flat(world, "quarantine_0123456789abcdef01234567_deadbeef.csv", "ORPHAN\n")
    # A's folder already holds a file of the same name: never overwritten.
    _flat(world, "taken.csv", "FLAT-TAKEN\n", a)
    (world.root / "_workspaces" / a).mkdir(parents=True, exist_ok=True)
    (world.root / "_workspaces" / a / "taken.csv").write_text("A-CURRENT\n", encoding="utf-8")

    # Before the migration the owner is already recognised on download ...
    assert _get(world.a, "orders.csv") == 200
    assert _get(world.b, "orders.csv") == 404
    # ... and nobody but a platform administrator reaches a file with no owner.
    for name in ("legacy_client.csv", "shared.csv", "unscoped.csv"):
        assert (_get(world.a, name), _get(world.b, name), _get(world.admin, name)) == (404, 404, 200), name

    first = migrate_flat_exports()
    assert first["moved"] == 5, first
    tree = _tree(world.root)
    assert tree[f"_workspaces/{a}/orders.csv"] == "A-ORDERS\n"
    assert tree[f"_workspaces/{a}/nightly/n.csv"] == "A-NIGHTLY\n"
    assert tree[f"_workspaces/{b}/b-only.csv"] == "B-ONLY\n"
    assert tree[f"_workspaces/{a}/export_{job}.csv"] == "A-JOB-EXPORT\n"
    assert tree[f"_workspaces/{a}/quarantine_{job}_deadbeef.csv"] == "A-QUARANTINE\n"
    for moved in ("orders.csv", "nightly/n.csv", "b-only.csv", f"export_{job}.csv", f"quarantine_{job}_deadbeef.csv"):
        assert moved not in tree, moved
    # No provable single owner: left in place.
    for kept in ("legacy_client.csv", "shared.csv", "unscoped.csv", f"export_{other}.csv",
                 "quarantine_0123456789abcdef01234567_deadbeef.csv", "taken.csv"):
        assert kept in tree, kept
    assert tree[f"_workspaces/{a}/taken.csv"] == "A-CURRENT\n"
    assert first["left"] == 6 and first["kept_existing"] == 1, first

    assert _get(world.a, "orders.csv") == 200 and _get(world.b, "orders.csv") == 404
    assert _get(world.b, "b-only.csv") == 200 and _get(world.a, "b-only.csv") == 404
    assert _get(world.a, f"export_{job}.csv") == 200 and _get(world.b, f"export_{job}.csv") == 404
    for name in ("legacy_client.csv", "shared.csv", "unscoped.csv"):
        assert (_get(world.a, name), _get(world.b, name), _get(world.admin, name)) == (404, 404, 200), name

    # A second run changes nothing.
    second = migrate_flat_exports()
    assert second["moved"] == 0 and second["left"] == 6, second
    assert _tree(world.root) == tree


def test_owner_of_a_client_named_flat_export_can_download_it_and_nobody_else(world):
    """The reviewer's ``[E6]`` line for a file its workspace wrote before the upgrade."""
    _flat(world, "orders.csv", "A-ORDERS\n", world.workspace_a)
    assert _get(world.a, "orders.csv") == 200
    assert _get(world.b, "orders.csv") == 404
    # A job record of another workspace that names a file in ITS OWN folder
    # with the same name is not a claim on the flat one.
    from services.export_paths import generated_export_path

    mine = generated_export_path("report.csv", workspace_id=world.workspace_b)
    mine.write_text("B-REPORT\n", encoding="utf-8")
    world.job(world.workspace_b, destination_summary={"filename": "report.csv", "path": str(mine)})
    _flat(world, "report.csv", "FLAT-REPORT\n")
    assert _get(world.a, "report.csv") == 404
    assert world.b.get("/api/v1/transfer/download/report.csv").text == "B-REPORT\n"


def _append(world, job_id: str, rows: str, output_path: str = "ledger.csv") -> dict:
    from services.file_export_append import land_export_bytes
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="file_export", format="csv", output_path=output_path)
    return land_export_bytes(
        dest, f"id,v\n{rows}".encode(), job_id=job_id, sync_mode="incremental_append", export_name="ledger.csv"
    )


def test_recurring_append_export_continues_its_file_after_the_upgrade(world):
    a = world.workspace_a
    flat = _flat(world, "ledger.csv", "id,v\n1,a\n2,b\n", a)

    landed = _append(world, world.job(a, name="run 3"), "3,c\n")

    target = world.root / "_workspaces" / a / "ledger.csv"
    assert landed["path"] == str(target)
    assert target.read_text(encoding="utf-8") == "id,v\n1,a\n2,b\n3,c\n"
    assert not flat.exists()
    assert "legacy_flat_export" not in landed
    # And the next run appends to the same file.
    _append(world, world.job(a, name="run 4"), "4,d\n")
    assert target.read_text(encoding="utf-8") == "id,v\n1,a\n2,b\n3,c\n4,d\n"


def test_append_never_adopts_a_flat_file_it_cannot_be_shown_to_own(world):
    a, b = world.workspace_a, world.workspace_b
    owned_by_a = _flat(world, "ledger.csv", "id,v\n1,a\n", a)
    unknown = _flat(world, "mystery.csv", "id,v\n9,z\n")

    # B appends under the name of A's flat file: B gets its own file and is told.
    landed = _append(world, world.job(b, name="b run"), "7,b\n")
    assert landed["path"] == str(world.root / "_workspaces" / b / "ledger.csv")
    assert Path(landed["path"]).read_text(encoding="utf-8") == "id,v\n7,b\n"
    assert owned_by_a.read_text(encoding="utf-8") == "id,v\n1,a\n"
    assert "ledger.csv" in landed["legacy_flat_export"]

    # A file nobody's record names: a new file, and the run's summary says so
    # instead of silently holding only the new rows.
    landed = _append(world, world.job(a, name="a run"), "8,a\n", output_path="mystery.csv")
    assert Path(landed["path"]).read_text(encoding="utf-8") == "id,v\n8,a\n"
    assert unknown.read_text(encoding="utf-8") == "id,v\n9,z\n"
    assert "platform administrator" in landed["legacy_flat_export"]


def test_workspace_teardown_removes_its_export_folder(world):
    from services.export_paths import generated_export_path
    from services.workspace_teardown import teardown_workspace

    a, b = world.workspace_a, world.workspace_b
    for workspace_id in (a, b):
        generated_export_path("orders.csv", workspace_id=workspace_id).write_text("x\n", encoding="utf-8")
    nested = world.root / "_workspaces" / a / "nightly"
    nested.mkdir()
    (nested / "n.csv").write_text("y\n", encoding="utf-8")
    flat = _flat(world, "flat.csv", "FLAT\n")

    counted = teardown_workspace(a, actor="tenancy", dry_run=True)
    assert counted["export_files_removed"] == 2
    assert (world.root / "_workspaces" / a / "orders.csv").is_file()

    done = teardown_workspace(a, actor="tenancy")
    assert done["export_files_removed"] == 2 and done["workspace_removed"] is True
    assert not (world.root / "_workspaces" / a).exists()
    assert (world.root / "_workspaces" / b / "orders.csv").is_file()
    assert flat.is_file()
    # Deleting again (a resumed teardown) is not an error.
    assert teardown_workspace(a, actor="tenancy")["export_files_removed"] == 0


def test_the_migration_runs_at_startup_and_never_fails_the_boot(world, monkeypatch, caplog):
    import inspect

    from services import export_migration
    from src import main

    assert "migrate_flat_exports_at_startup" in inspect.getsource(main.lifespan)

    _flat(world, "orders.csv", "A-ORDERS\n", world.workspace_a)
    assert export_migration.migrate_flat_exports_at_startup()["moved"] == 1

    def broken():
        raise OSError("exports volume is read-only")

    monkeypatch.setattr(export_migration, "migrate_flat_exports", broken)
    assert export_migration.migrate_flat_exports_at_startup() == {}
