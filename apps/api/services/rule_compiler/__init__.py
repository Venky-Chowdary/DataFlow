"""Compile a customer rule workbook onto Transform + Map.

The LLM is not on this path. Closed-form cells become executable artifacts
the existing engines already run; everything else is a review item.
"""

from .apply import apply_compiled_projection, apply_selected_tables
from .compile import compile_rule_workbook
from .ingest import RuleIngestError, SUPPORTED
from .roles import infer_header_roles

__all__ = [
    "apply_compiled_projection",
    "apply_selected_tables",
    "compile_rule_workbook",
    "RuleIngestError",
    "SUPPORTED",
    "infer_header_roles",
]
