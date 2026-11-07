"""scripts/backfill_legacy_workspaces.py: dry run is the default, --apply is the
only way to write, and a rerun changes nothing. Runs on throwaway tmp stores
and the in-memory job store only."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
for _p in (_API_ROOT, _API_ROOT / "scripts"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from d154_world import build_world  # noqa: E402

import backfill_legacy_workspaces as cli  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch):
    return build_world(tmp_path, monkeypatch, signup=True)


def _snapshot(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _run(capsys, argv: list[str]) -> dict:
    assert cli.main(argv) == 0
    return json.loads(capsys.readouterr().out)


def test_default_is_a_dry_run_that_writes_nothing(world, tmp_path, capsys):
    before = _snapshot(tmp_path)
    report = _run(capsys, [])
    assert report["applied"] is False
    assert sum(c["assigned"] for c in report["counts"].values()) > 0
    assert _snapshot(tmp_path) == before

    explicit = _run(capsys, ["--dry-run"])
    assert explicit == report
    assert _snapshot(tmp_path) == before


def test_apply_writes_and_a_rerun_is_a_no_op(world, tmp_path, capsys):
    dry = _run(capsys, [])
    applied = _run(capsys, ["--apply"])
    assert applied["applied"] is True
    assert {k: v["assigned"] for k, v in applied["counts"].items()} == {
        k: v["assigned"] for k, v in dry["counts"].items()
    }
    settled = _snapshot(tmp_path)

    again = _run(capsys, ["--apply"])
    assert all(c["assigned"] == 0 for c in again["counts"].values()), again["counts"]
    assert again["created_workspaces"] == []
    assert _snapshot(tmp_path) == settled

    after_dry = _run(capsys, [])
    assert all(c["assigned"] == 0 for c in after_dry["counts"].values())


def test_dry_run_and_apply_are_mutually_exclusive():
    with pytest.raises(SystemExit) as exc:
        cli.main(["--dry-run", "--apply"])
    assert exc.value.code == 2


def test_apply_with_nothing_to_place_creates_no_workspace(world, tmp_path, capsys):
    from d154_world import ADMIN
    from services import team_store

    _run(capsys, ["--apply"])
    # Take the platform owner's workspace away; with no legacy row left the
    # rerun must not recreate it.
    for ws in team_store.list_workspaces_for_user(ADMIN):
        team_store.delete_workspace(workspace_id=ws.id)
    settled = _snapshot(tmp_path)
    again = _run(capsys, ["--apply"])
    assert again["created_workspaces"] == []
    assert _snapshot(tmp_path) == settled
