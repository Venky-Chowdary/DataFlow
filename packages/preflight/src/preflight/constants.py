"""Shared preflight constants."""

from __future__ import annotations

SCHEMALESS_DESTS: frozenset[str] = frozenset({"mongodb", "dynamodb", "redis"})
