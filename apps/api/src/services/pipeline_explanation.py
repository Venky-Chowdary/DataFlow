"""Compatibility shim: canonical implementation now lives in services.pipeline_explanation."""
from __future__ import annotations

from services.pipeline_explanation import (
    _describe_type,
    _fmt_endpoint,
    _mapping_line,
    _sync_mode_note,
    build_pipeline_explanation,
)

__all__ = ['_fmt_endpoint', '_describe_type', '_sync_mode_note', '_mapping_line', 'build_pipeline_explanation']
