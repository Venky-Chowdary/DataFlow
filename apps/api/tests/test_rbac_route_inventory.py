"""Keep the RBAC public surface and route permission table in sync."""

from __future__ import annotations

import re
import json
from collections.abc import Iterable
from pathlib import Path

import pytest
from fastapi.routing import APIRoute
from starlette.routing import Mount

from services import rbac
from src.main import app
from src.middleware import auth_middleware


# The public list bypasses standard role checks. SCIM remains separately gated
# by its bearer token and scim.provision rule; MCP tools/call uses its tool gate.
PUBLIC_ROUTES = frozenset(
    {
        ("GET", "/.well-known/oauth-protected-resource"),
        ("GET", "/.well-known/oauth-protected-resource/api/v1/mcp"),
        ("GET", "/api/v1/auth/bootstrap"),
        ("POST", "/api/v1/auth/login"),
        ("POST", "/api/v1/auth/logout"),
        ("GET", "/api/v1/auth/sso/providers"),
        ("GET", "/api/v1/auth/sso/{sso_type}/callback"),
        ("POST", "/api/v1/auth/sso/{sso_type}/callback"),
        ("GET", "/api/v1/auth/sso/{sso_type}/start"),
        ("GET", "/api/v1/catalog/connectors"),
        ("GET", "/api/v1/catalog/connectors/{connector_id}"),
        ("GET", "/api/v1/catalog/stats"),
        ("GET", "/api/v1/catalog/suggested/destinations"),
        ("GET", "/api/v1/catalog/suggested/sources"),
        ("DELETE", "/api/v1/mcp"),
        ("GET", "/api/v1/mcp"),
        ("POST", "/api/v1/mcp"),
        ("DELETE", "/api/v1/mcp/"),
        ("GET", "/api/v1/mcp/"),
        ("POST", "/api/v1/mcp/"),
        ("GET", "/api/v1/mcp/manifest"),
        ("GET", "/api/v1/mcp/status"),
        ("GET", "/api/v1/mcp/tools"),
        ("GET", "/api/v1/transfer/capabilities"),
        ("GET", "/api/v1/transfer/platform"),
        ("GET", "/api/v1/transfer/readiness"),
        ("GET", "/auth/bootstrap"),
        ("POST", "/auth/login"),
        ("POST", "/auth/logout"),
        ("GET", "/auth/sso/providers"),
        ("GET", "/auth/sso/{sso_type}/callback"),
        ("POST", "/auth/sso/{sso_type}/callback"),
        ("GET", "/auth/sso/{sso_type}/start"),
        ("GET", "/docs"),
        ("HEAD", "/docs"),
        ("GET", "/health"),
        ("GET", "/openapi.json"),
        ("HEAD", "/openapi.json"),
        ("GET", "/redoc"),
        ("HEAD", "/redoc"),
    }
)

MCP_TOOL_GATED_ROUTES = frozenset({("POST", "/api/v1/mcp/tools/call")})
SCIM_AUTH_GATED_PREFIX = "/api/v1/scim/v2"

# These policy mismatches were measured in the Phase A inventory. Keep them
# visible without silently changing the independent AuthMiddleware policy.
RBAC_ONLY_PUBLIC_ROUTES = frozenset(
    {
        ("GET", "/api/v1/transfer/capabilities"),
        ("GET", "/api/v1/transfer/platform"),
        ("GET", "/api/v1/transfer/readiness"),
    }
)
AUTH_ONLY_PUBLIC_ROUTES = frozenset(
    {
        ("GET", "/"),
        ("GET", "/api/v1/health"),
        ("GET", "/docs/oauth2-redirect"),
        ("HEAD", "/docs/oauth2-redirect"),
        ("GET", "/health/ready"),
    }
)


def _sample_path(template: str) -> str:
    return re.sub(r"\{[^{}]+\}", "x", template)


def _join_path(prefix: str, path: str) -> str:
    if not prefix:
        return path
    if prefix.endswith("/") and path.startswith("/"):
        return prefix + path[1:]
    return prefix + path


def _walk_routes(routes: Iterable[object], prefix: str = ""):
    for route in routes:
        if isinstance(route, Mount):
            yield from _walk_routes(route.routes, _join_path(prefix, route.path))
            continue

        if type(route).__name__ == "_IncludedRouter":
            context = getattr(route, "include_context", None)
            nested_prefix = getattr(context, "prefix", "") or ""
            original_router = getattr(route, "original_router", None)
            yield from _walk_routes(
                getattr(original_router, "routes", ()),
                _join_path(prefix, nested_prefix),
            )
            continue

        path = getattr(route, "path", None)
        if path is None:
            continue
        template = _join_path(prefix, path)
        methods = getattr(route, "methods", None) or {"GET"}
        for method in methods:
            yield method.upper(), template, _sample_path(template)


def _matching_rule(method: str, path: str) -> tuple[bool, str | None]:
    method = "GET" if method.upper() == "HEAD" else method.upper()
    for rule_method, exact_path, permission in rbac._EXACT_PATH_RULES:
        if method == rule_method and (
            path == exact_path
            or ("{" in exact_path and rbac._compile_route_template(exact_path).fullmatch(path))
        ):
            return True, permission
    for rule_method, prefix, permission in rbac._PATH_RULES:
        if rule_method not in ("*", method):
            continue
        if path.startswith(prefix):
            return True, permission
    return False, None


def _live_route_permissions(routes: Iterable[object]) -> dict[str, str]:
    mapping = {}
    for method, template, sample in _walk_routes(routes):
        key = f"{method} {template}"
        if (method, template) in MCP_TOOL_GATED_ROUTES:
            mapping[key] = "MCP_TOOL_GATE"
        elif rbac._is_public_path(sample):
            mapping[key] = "PUBLIC"
        else:
            _matched, permission = _matching_rule(method, sample)
            if permission is None:
                permission = rbac._required_permission(method, sample)
            mapping[key] = str(permission)
    return dict(sorted(mapping.items()))


def _snapshot_diff(actual: dict[str, str], expected: dict[str, str]) -> str:
    lines = []
    paste = []
    for key in sorted(actual.keys() | expected.keys()):
        current, prior = actual.get(key), expected.get(key)
        if current == prior:
            continue
        if prior is None:
            method, template = key.split(" ", 1)
            sample = _sample_path(template)
            matched, _permission = _matching_rule(method, sample)
            inherited = next(
                (
                    prefix
                    for rule_method, prefix, _value in rbac._PATH_RULES
                    if matched and rule_method in ("*", method) and sample.startswith(prefix)
                ),
                None,
            )
            suffix = f" (inherited from prefix rule '{inherited}')" if inherited else ""
            lines.append(f"+ {key} → {current}{suffix}")
        elif current is None:
            lines.append(f"- {key} → {prior}")
        else:
            lines.append(f"~ {key} → {prior} → {current}")
        if current is not None:
            paste.append(f'  "{key}": "{current}",')
    if not lines:
        return ""
    return (
        "\n".join(lines)
        + "\nreview the permission for this route, add an explicit rule if the inherited one is wrong, "
        "then update tests/fixtures/rbac_route_permissions.json\nJSON lines to paste:\n"
        + "\n".join(paste)
    )


def test_live_route_permission_snapshot_matches_reviewed_fixture():
    path = Path(__file__).parent / "fixtures" / "rbac_route_permissions.json"
    expected = json.loads(path.read_text(encoding="utf-8"))
    actual = _live_route_permissions(app.routes)
    differences = _snapshot_diff(actual, expected)
    assert not differences, differences


def test_snapshot_comparison_reports_an_unreviewed_route_and_inherited_rule():
    async def dummy():
        return {"ok": True}

    routes = [*app.routes, APIRoute("/api/v1/ops/dummy-new-route", dummy, methods=["POST"])]
    expected = _live_route_permissions(app.routes)
    actual = _live_route_permissions(routes)
    differences = _snapshot_diff(actual, expected)
    assert "+ POST /api/v1/ops/dummy-new-route → connector.write" in differences
    assert "inherited from prefix rule '/api/v1/ops/'" in differences


def _auth_middleware_treats_as_public(method: str, path: str) -> bool:
    return (
        path == "/"
        or path.startswith(auth_middleware._PUBLIC_PREFIXES)
        or auth_middleware._is_public_sso_path(path)
        or auth_middleware._is_public_mcp_path(path, method)
    )


def test_real_app_routes_have_reviewed_public_or_explicit_rbac_policy():
    routes = list(_walk_routes(app.routes))
    by_key = {(method, template): sample for method, template, sample in routes}

    rbac_public = {
        key for key, sample in by_key.items() if rbac._is_public_path(sample)
    }
    assert rbac_public == PUBLIC_ROUTES

    auth_public = {
        key
        for key, sample in by_key.items()
        if _auth_middleware_treats_as_public(key[0], sample)
    }
    assert rbac_public - auth_public == RBAC_ONLY_PUBLIC_ROUTES
    assert auth_public - rbac_public == AUTH_ONLY_PUBLIC_ROUTES

    no_rule_routes = [
        (method, template, sample)
        for (method, template), sample in by_key.items()
        if not _matching_rule(method, sample)[0]
    ]
    missing_rules = [
        (method, template, sample)
        for method, template, sample in no_rule_routes
        if (method, template) not in PUBLIC_ROUTES
        and (method, template) not in MCP_TOOL_GATED_ROUTES
    ]
    if missing_rules:
        rendered = "\n".join(
            f"  {method} {template}"
            for method, template, _sample in no_rule_routes
        )
        pytest.fail(
            f"{len(no_rule_routes)} route records have no _PATH_RULES match "
            f"({len(missing_rules)} non-public routes lack an explicit rule):\n"
            f"{rendered}"
        )

    stale_rules = [
        (method, prefix, permission)
        for method, prefix, permission in rbac._PATH_RULES
        if not any(
            method in ("*", route_method) and sample.startswith(prefix)
            for route_method, _template, sample in routes
        )
    ]
    stale_rules.extend(
        (method, exact_path, permission)
        for method, exact_path, permission in rbac._EXACT_PATH_RULES
        if not any(
            method == route_method
            and (
                sample == exact_path
                or (
                    "{" in exact_path
                    and rbac._compile_route_template(exact_path).fullmatch(sample)
                )
            )
            for route_method, _template, sample in routes
        )
    )
    assert not stale_rules, f"RBAC rules match no real route: {stale_rules!r}"

    assert MCP_TOOL_GATED_ROUTES <= by_key.keys()
    scim_routes = {
        (method, template, sample)
        for method, template, sample in routes
        if template.startswith(SCIM_AUTH_GATED_PREFIX)
    }
    assert scim_routes
    for method, template, sample in scim_routes:
        assert _matching_rule(method, sample) == (True, rbac.Permission.SCIM_PROVISION)
        assert (method, template) not in PUBLIC_ROUTES
    for method, template in MCP_TOOL_GATED_ROUTES:
        assert rbac._required_permission(method, _sample_path(template)) is None

    for method, template in rbac_public:
        assert (method, template) in PUBLIC_ROUTES

    for method, template, sample in routes:
        key = (method, template)
        if key in PUBLIC_ROUTES or key in MCP_TOOL_GATED_ROUTES:
            continue
        matched, permission = _matching_rule(method, sample)
        if matched:
            assert permission is not None
            assert rbac._required_permission(method, sample) == permission
        else:
            assert rbac._required_permission(method, sample) is rbac.NO_RULE

    get_templates = {template for method, template, _sample in routes if method == "GET"}
    for method, template, sample in routes:
        if method == "HEAD" and template in get_templates:
            assert rbac._required_permission("HEAD", sample) == rbac._required_permission(
                "GET", sample
            )
