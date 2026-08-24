"""Host runtime probes used by object-store paths and demo readiness.

These are environment honesty checks — not product Planned/Certified status.
"""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=1)
def python_xml_runtime_ok() -> bool:
    """True when ``xml.etree`` / pyexpat can construct a parser.

    Broken Homebrew Python + system libexpat pairs fail botocore S3 XML
    parsing with ``No module named expat`` / missing ``XML_SetAllocTracker…``.
    """
    try:
        # Prefer defusedxml (Bandit B314 / audit §6.10); fall back to stdlib for
        # environments that only need a capability probe.
        try:
            from defusedxml import ElementTree as ET
        except ImportError:  # pragma: no cover
            from xml.etree import ElementTree as ET  # nosec B314 — capability probe only

        ET.XMLParser()
        return True
    except Exception:
        return False


def python_xml_runtime_skip_reason() -> str:
    return (
        "Python XML runtime (pyexpat/libexpat) is broken in this environment — "
        "S3/object-store botocore paths cannot parse responses. "
        "Reinstall python@3.12 + expat, or set DYLD_LIBRARY_PATH to Homebrew libexpat."
    )
