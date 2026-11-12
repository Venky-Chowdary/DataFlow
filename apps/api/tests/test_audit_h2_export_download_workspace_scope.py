"""Audit H-2: a local export is written to, and downloaded from, its workspace.

Every client-named export landed in one flat folder and
``GET /transfer/download/{filename}`` served any name in it to any ``job.read``
holder: a viewer of workspace B could fetch workspace A's ``orders.csv``, and
two workspaces exporting to the same ``output_path`` overwrote (or appended
into) each other's file.

Exports now land in a per-workspace folder and the download route serves only
from the caller's tenant scope. Files already in the flat folder are served by
job ownership (``export_<job>``, ``quarantine_<job>_...``) or not at all; a
platform administrator still reaches them. Through the real REST routes with
two workspaces.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401

DOWNLOAD = "/api/v1/transfer/download/"


def _exports(world) -> Path:
    return world.data_dir / "exports"


def _land(job_id: str, payload: bytes, output_path: str = "orders.csv") -> dict:
    """What the engine does when a job's file export finishes (worker, no request)."""
    from services.file_export_append import land_export_bytes

    return land_export_bytes(
        SimpleNamespace(format="csv", output_path=output_path),
        payload,
        job_id=job_id,
        sync_mode="full_refresh",
        export_name="export.csv",
    )


def _same_404(client, name: str) -> None:
    foreign = client.get(DOWNLOAD + name)
    missing = client.get(DOWNLOAD + "no-such-export-anywhere.csv")
    assert foreign.status_code == missing.status_code == 404, foreign.text
    assert foreign.json() == missing.json() == {"detail": "Export not found"}


def test_same_output_path_in_two_workspaces_is_two_files(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    job_b = tenancy_world.job(tenancy_world.workspace_b)
    landed_a = _land(job_a, b"id,secret\n1,A-EXPORT-SENTINEL\n")
    landed_b = _land(job_b, b"id,secret\n1,B-EXPORT-SENTINEL\n")

    assert landed_a["path"] != landed_b["path"]
    assert Path(landed_a["path"]).read_bytes() == b"id,secret\n1,A-EXPORT-SENTINEL\n"
    assert not (_exports(tenancy_world) / "orders.csv").exists(), "export landed in the shared flat folder"

    got_a = tenancy_world.a.get(landed_a["download_url"])
    got_b = tenancy_world.b.get(landed_b["download_url"])
    assert got_a.status_code == got_b.status_code == 200, (got_a.text, got_b.text)
    assert "A-EXPORT-SENTINEL" in got_a.text and "B-EXPORT-SENTINEL" not in got_a.text
    assert "B-EXPORT-SENTINEL" in got_b.text and "A-EXPORT-SENTINEL" not in got_b.text


def test_export_of_another_workspace_is_not_downloadable(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    named = _land(job_a, b"id\nA-EXPORT-SENTINEL\n", output_path="a-only-ledger.csv")
    generated = _land(job_a, b"id\nA-EXPORT-SENTINEL\n", output_path="")
    assert generated["filename"] == f"export_{job_a}.csv"

    for landed in (named, generated):
        assert tenancy_world.a.get(landed["download_url"]).status_code == 200
        _same_404(tenancy_world.b, landed["filename"])
    # A viewer of B (the route's permission is job.read) is refused the same way.
    viewer_b = tenancy_world.member("viewer-b@tenancy.example.com", tenancy_world.workspace_b, "viewer")
    _same_404(viewer_b, named["filename"])


def test_quarantine_export_is_downloadable_only_in_the_jobs_workspace(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(
        tenancy_world.workspace_a,
        rejected_details=[
            {"row": 1, "column": "amount", "target": "amount", "value": "A-QUARANTINE-SENTINEL",
             "reason": "not a number"},
        ],
    )
    exported = tenancy_world.a.post(f"/api/v1/connectors/jobs/{job_a}/quarantine/export", json={})
    assert exported.status_code == 200, exported.text
    body = exported.json()
    assert body["row_count"] == 1 and body["download_url"].startswith(DOWNLOAD)

    own = tenancy_world.a.get(body["download_url"])
    assert own.status_code == 200, own.text
    assert "not a number" in own.text
    _same_404(tenancy_world.b, body["filename"])
    assert not (_exports(tenancy_world) / body["filename"]).exists()


def test_files_already_in_the_flat_folder_follow_job_ownership_or_are_refused(tenancy_world):  # noqa: F811
    from services.mongodb_service import get_mongodb_service

    flat = _exports(tenancy_world)
    flat.mkdir(parents=True, exist_ok=True)
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    job_export = f"export_{job_a}.csv"
    get_mongodb_service().update_job_fields(
        job_a, {"destination_summary": {"filename": job_export, "path": str(flat / job_export)}}
    )
    quarantine = f"quarantine_{job_a}_deadbeef.csv"
    for name in (job_export, quarantine, "legacy-orders.csv"):
        (flat / name).write_text("id\nA-LEGACY-SENTINEL\n", encoding="utf-8")

    # B: none of them, each exactly like a name that does not exist.
    for name in (job_export, quarantine, "legacy-orders.csv"):
        _same_404(tenancy_world.b, name)
    # A: the two its job owns; a client-named file has no provable owner.
    assert tenancy_world.a.get(DOWNLOAD + job_export).status_code == 200
    assert tenancy_world.a.get(DOWNLOAD + quarantine).status_code == 200
    _same_404(tenancy_world.a, "legacy-orders.csv")
    # The platform administrator can still retrieve (and so migrate) all three.
    for name in (job_export, quarantine, "legacy-orders.csv"):
        got = tenancy_world.admin.get(DOWNLOAD + name)
        assert got.status_code == 200 and "A-LEGACY-SENTINEL" in got.text, (name, got.text)

    # A legacy name that claims A's job without being that job's export is not A's.
    (flat / f"export_{job_a}.json").write_text('{"x": "A-LEGACY-SENTINEL"}', encoding="utf-8")
    _same_404(tenancy_world.a, f"export_{job_a}.json")


def test_download_refuses_names_that_leave_the_callers_folder(tenancy_world):  # noqa: F811
    from services.export_paths import workspace_exports_dir

    job_a = tenancy_world.job(tenancy_world.workspace_a)
    landed = _land(job_a, b"id\nA-EXPORT-SENTINEL\n", output_path="a-only-ledger.csv")
    (tenancy_world.data_dir / "teams.json").exists()
    a_folder = Path(landed["path"]).parent.name
    for name in (
        "..%2F..%2Fteams.json",
        "%2e%2e%2fteams.json",
        "..%5C..%5Cteams.json",
        f"..%2F{a_folder}%2Fa-only-ledger.csv",
        f"_workspaces%2F{a_folder}%2Fa-only-ledger.csv",
        "%2e%2e",
        "_workspaces",
    ):
        got = tenancy_world.b.get(DOWNLOAD + name)
        assert got.status_code in {400, 404}, (name, got.status_code, got.text)
        assert "A-EXPORT-SENTINEL" not in got.text and "workspaces" not in got.text.lower().replace(
            "_workspaces", ""
        )

    # A link planted in B's folder to A's file is followed and refused.
    b_dir = workspace_exports_dir(tenancy_world.workspace_b)
    try:
        os.symlink(landed["path"], b_dir / "linked.csv")
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    _same_404(tenancy_world.b, "linked.csv")


def test_export_paths_are_confined_to_the_workspace_folder(tenancy_world):  # noqa: F811
    from services.export_paths import (
        ExportPathError,
        exports_dir,
        resolve_export_path,
        workspace_exports_dir,
    )

    a_dir = workspace_exports_dir(tenancy_world.workspace_a)
    b_dir = workspace_exports_dir(tenancy_world.workspace_b)
    assert a_dir != b_dir and a_dir.parent == b_dir.parent != exports_dir()

    with tenancy_world.as_workspace(tenancy_world.workspace_b):
        assert resolve_export_path("orders.csv", default_ext="csv") == b_dir / "orders.csv"
        assert resolve_export_path("nightly/orders", default_ext="csv") == b_dir / "nightly" / "orders.csv"
        # A saved absolute path inside the exports root keeps working, re-rooted.
        assert (
            resolve_export_path(str(exports_dir() / "nightly" / "o.csv"), default_ext="csv")
            == b_dir / "nightly" / "o.csv"
        )
        assert resolve_export_path(str(b_dir / "o.csv"), default_ext="csv") == b_dir / "o.csv"
        for hostile in (
            f"../{a_dir.name}/orders.csv",
            str(a_dir / "orders.csv"),
            str(exports_dir() / "_workspaces" / a_dir.name / "orders.csv"),
            "../../teams.json",
            "../../../exports/orders.csv",
        ):
            with pytest.raises(ExportPathError):
                resolve_export_path(hostile, default_ext="csv")
        # Spelling the container as a relative name is only a sub-folder of B's own.
        for nested in (
            f"_workspaces/{a_dir.name}/orders.csv",
            f"exports/_workspaces/{a_dir.name}/orders.csv",
        ):
            assert resolve_export_path(nested, default_ext="csv").is_relative_to(b_dir)

    # The single local operator (nothing bound) keeps the flat folder, and
    # cannot name a workspace's folder through it.
    assert resolve_export_path("orders.csv", default_ext="csv") == exports_dir() / "orders.csv"
    with pytest.raises(ExportPathError):
        resolve_export_path(f"_workspaces/{a_dir.name}/orders.csv", default_ext="csv")

    # An id that is not a safe folder name still gets a folder of its own.
    odd = workspace_exports_dir("../../etc")
    assert odd.parent == a_dir.parent and ".." not in odd.name and "/" not in odd.name


def test_append_run_extends_only_its_own_workspaces_file(tenancy_world):  # noqa: F811
    from services.file_export_append import land_export_bytes

    job_a = tenancy_world.job(tenancy_world.workspace_a)
    job_b = tenancy_world.job(tenancy_world.workspace_b)
    dest = SimpleNamespace(format="csv", output_path="ledger.csv")
    for job_id, row in ((job_a, b"1,A-ROW\n"), (job_b, b"1,B-ROW\n"), (job_a, b"2,A-ROW\n")):
        land_export_bytes(
            dest, b"id,v\n" + row, job_id=job_id, sync_mode="append", export_name="export.csv"
        )
    text_a = tenancy_world.a.get(DOWNLOAD + "ledger.csv").text
    text_b = tenancy_world.b.get(DOWNLOAD + "ledger.csv").text
    assert text_a.count("A-ROW") == 2 and "B-ROW" not in text_a
    assert text_b.count("B-ROW") == 1 and "A-ROW" not in text_b
