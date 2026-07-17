"""LLM privacy and policy gate.

Controls whether LLM inference is allowed and masks PII-like sample values
before any prompt is sent to a third-party provider.  This keeps the mapping
assistant privacy-safe even when no local model is configured.
"""

from __future__ import annotations

import os
import re

# PII / sensitive patterns we do not send to cloud LLMs.
_SANITIZE_RE = re.compile(
    r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"  # email
    r"|(\b\d{3}-\d{2}-\d{4}\b)"  # US SSN
    r"|(\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b)"  # credit card
    r"|(\b\d{3}-\d{3}-\d{4}\b)"  # phone
    r"|((?:\d{1,3}\.){3}\d{1,3})"  # IPv4
    r"|(https?://[^\s]+)"  # URL
    r"|(\b[A-Z]{2}\d[\s-]?[A-Z\d]{2,3}[\s-]?[A-Z\d]{2,3}\b)"  # UK postcodes
    r"|(\b[A-Z]{2}\d{2}[A-Z]{0,2}\b)"  # passport-ish
    r"|(sk-[A-Za-z0-9]{48})"  # OpenAI-style key
    r"|(AKIA[0-9A-Z]{16})"  # AWS access key id
    r"|(\b[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\b)",  # UUID
    re.IGNORECASE,
)


def is_llm_enabled() -> bool:
    """Return whether LLM inference is globally enabled."""
    return os.getenv("DATAFLOW_LLM_ENABLED", "true").lower() not in (
        "false", "0", "off", "disabled", "no"
    )


def is_pii_masking_enabled() -> bool:
    """Return whether PII masking before LLM prompts is required."""
    return os.getenv("DATAFLOW_PII_MASKING", "true").lower() not in (
        "false", "0", "off", "disabled", "no"
    )


def mask_pii_value(value: str | None) -> str:
    """Mask PII-like strings for LLM prompts."""
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    if not value:
        return value
    if _SANITIZE_RE.search(value):
        return "<redacted>"
    return value


def mask_pii_samples(samples: dict[str, list[str]] | None) -> dict[str, list[str]]:
    """Mask PII in a column->sample-values dictionary."""
    if not samples:
        return {}
    return {
        col: [mask_pii_value(v) for v in vals]
        for col, vals in samples.items()
    }


def llm_use_allowed(requested: bool) -> bool:
    """Combine caller request with global policy."""
    return requested and is_llm_enabled() and is_pii_masking_enabled()
