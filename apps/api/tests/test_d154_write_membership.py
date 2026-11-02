"""D154: mutating an id-addressed resource needs WRITE membership in the
resource's own workspace.

The caller's RBAC permission comes from their effective role, which with no
``X-Workspace-Id`` is the role in their own workspace. A viewer of workspace A
who is an admin of their own workspace therefore passed the permission gate
and, with read membership as the only per-object check, could delete or
re-run A's schedules, contracts, projects and recipes.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import build_world  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch):
    from services import team_store

    w = build_world(tmp_path, monkeypatch, signup=True)
    # B stays admin of their own workspace and becomes a VIEWER of A's.
    team_store.add_workspace_member(workspace_id=w.ws_a, email=w.b_email, role="viewer", added_by=w.a_email)
    return w


def test_viewer_of_a_can_read_but_not_mutate_as_with_no_header(world):
    ids, b = world.ids, world.b
    assert b.get(f"/api/v1/schedules/{ids['a_schedule']}").status_code == 200
    assert b.get(f"/api/v1/contracts/{ids['a_contract']}").status_code == 200
    mutations = (
        ("delete", f"/api/v1/schedules/{ids['a_schedule']}", None),
        ("patch", f"/api/v1/schedules/{ids['a_schedule']}", {"name": "hijacked"}),
        ("post", f"/api/v1/schedules/{ids['a_schedule']}/run", None),
        ("post", f"/api/v1/contracts/{ids['a_contract']}/deprecate", None),
        ("patch", f"/api/v1/transforms/{ids['a_project']}", {"name": "hijacked"}),
        ("delete", f"/api/v1/transforms/{ids['a_project']}", None),
        ("delete", f"/api/v1/connectors/{ids['a_connector']}", None),
        ("delete", f"/api/v1/connectors/saved/{ids['a_connector']}", None),
    )
    for verb, path, body in mutations:
        response = getattr(b, verb)(path, **({"json": body} if body is not None else {}))
        assert response.status_code in {403, 404}, (verb, path, response.status_code)
    # Nothing was changed.
    assert world.a.get(f"/api/v1/schedules/{ids['a_schedule']}").status_code == 200
    assert world.a.get(f"/api/v1/connectors/saved/{ids['a_connector']}").status_code == 200


def test_editor_membership_is_enough_to_mutate(world):
    from services import team_store

    team_store.add_workspace_member(workspace_id=world.ws_a, email=world.b_email, role="editor", added_by=world.a_email)
    patched = world.b.patch(
        f"/api/v1/schedules/{world.ids['a_schedule']}",
        json={"name": "renamed-by-editor"},
        headers={"X-Workspace-Id": world.ws_a},
    )
    assert patched.status_code == 200, patched.text


_GUARDS = ("assert_resource_workspace", "_bound_schedule", "_scoped_contract")
_ROUTERS = ("connectors", "saved_connectors", "schedules", "contracts", "transforms", "transform_library")
# POST routes that only compute (preview / plan / test) and change nothing.
_READ_ONLY_POSTS = {"compile_for_transfer", "upgrade_preview", "preview", "test_contract"}


def test_every_mutating_route_gates_its_object_in_write_mode():
    offenders = []
    for name in _ROUTERS:
        tree = ast.parse((_API_ROOT / "src" / "routers" / f"{name}_router.py").read_text())
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) or fn.name in _READ_ONLY_POSTS:
                continue
            verbs = {d.func.attr for d in fn.decorator_list if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)}
            if not verbs & {"post", "put", "patch", "delete"}:
                continue
            for call in ast.walk(fn):
                if not isinstance(call, ast.Call):
                    continue
                callee = getattr(call.func, "id", None) or getattr(call.func, "attr", None)
                if callee in _GUARDS:
                    write = next((k.value for k in call.keywords if k.arg == "write"), None)
                    if not (isinstance(write, ast.Constant) and write.value is True):
                        offenders.append(f"{name}_router.{fn.name}:{call.lineno}")
    assert not offenders, offenders
