"""Canonical type system package for DataWrap.

This package will gradually absorb the monolithic ``services.type_system`` module
and the per-database logical mappers that currently live in
``services.schema_introspect``.
"""

from __future__ import annotations

from services.type.dialects import postgresql

__all__ = ["postgresql"]
