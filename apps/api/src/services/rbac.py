"""Compatibility shim: canonical RBAC implementation lives in services.rbac."""

from __future__ import annotations

from services.rbac import (  # noqa: F401
    Permission,
    RBACMiddleware,
    has_permission,
    normalize_role,
    role_permissions,
)

__all__ = ["Permission", "RBACMiddleware", "has_permission", "normalize_role", "role_permissions"]
