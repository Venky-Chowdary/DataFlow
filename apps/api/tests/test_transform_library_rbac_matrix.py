"""Transform-library RBAC: every route has an explicit rule, and the role matrix
documented in docs/DEPLOYMENT.md follows from services/rbac.py's own tables."""

from __future__ import annotations

import pytest

from services import rbac
from src.main import app

_WRITE = {"POST", "PATCH", "PUT", "DELETE"}


def _routes() -> list[tuple[str, str]]:
    return sorted(
        (m, p)
        for m, p in rbac._walk_route_patterns(app.routes)
        if p.startswith("/api/v1/transform-library") and m not in {"HEAD", "OPTIONS"}
    )


def test_routes_exist():
    assert len(_routes()) == 13


@pytest.mark.parametrize("method,path", _routes())
def test_route_has_explicit_permission_equal_to_legacy_fallback(method, path):
    required = rbac._required_permission(method, path)
    assert required is not rbac.NO_RULE
    # Same permission the allow_and_log fallback demanded, so that mode is unchanged.
    assert required == rbac._fallback_permission(method)
    assert required == ("connector.write" if method in _WRITE else "job.read")


@pytest.mark.parametrize(
    "role,reads,writes",
    [
        ("viewer", True, False),
        ("operator", True, False),
        ("editor", True, True),
        ("admin", True, True),
    ],
)
def test_role_matrix(role, reads, writes):
    held = rbac.role_permissions(role)
    assert ("job.read" in held) is reads
    assert ("connector.write" in held) is writes
