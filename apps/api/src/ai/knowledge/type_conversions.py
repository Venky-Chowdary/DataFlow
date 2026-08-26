"""
Datawrap — Type Conversion Matrix (NON-AUTHORITATIVE)

Module 12: This coarse logical matrix is for LLM/RAG assist only.
It MUST NOT drive Map, Validate, DDL, Execute, or proof decisions.

Authoritative SSOT:
  apps/api/services/conversion_contract.py
  apps/api/services/type_system.py (is_lossy_coercion / materialize_dest_ddl)
"""

from __future__ import annotations

AUTHORITATIVE = False
AUTHORITY_NOTE = (
    "Non-authoritative AI assist matrix. Use services.conversion_contract "
    "and services.type_system for migration decisions."
)

TYPE_CONVERSION_MATRIX: dict[str, dict[str, dict]] = {
    "string": {
        "integer": {
            "method": "cast",
            "lossy": True,
            "validation": r"^-?\d+$",
            "note": "Auto fails closed on 1,234 / 1.234 — set number locale US or EU. Assist only.",
        },
        "decimal": {
            "method": "cast",
            "lossy": True,
            "note": "Auto fails closed on a lone 1,234 / 1.234. $1,000.00 and €1.000,89 bind. Assist only.",
        },
        "boolean": {
            "method": "parse_bool",
            "lossy": False,
            "mapping": {"true": True, "false": False, "t": True, "f": False, "1": True, "0": False},
            "note": "Write-path tokens only (true/false/t/f/1/0). Informal yes/on refuse. Assist only.",
        },
        "datetime": {
            "method": "parse_date",
            "lossy": True,
            "formats": ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
            "note": "Auto fails closed on 01/02/2024 — set date locale MDY or DMY. Assist only.",
        },
        "date": {
            "method": "parse_date",
            "lossy": True,
            "formats": ["%Y-%m-%d"],
            "note": "Auto fails closed on 01/02/2024 — set date locale MDY or DMY. Assist only.",
        },
        "json": {"method": "parse_json", "lossy": False},
    },
    "integer": {
        "string": {"method": "to_string", "lossy": False},
        "decimal": {"method": "cast", "lossy": False},
        "boolean": {"method": "cast", "lossy": True, "note": "0=false, non-zero=true"},
        "datetime": {"method": "unix_timestamp", "lossy": False},
    },
    "decimal": {
        "string": {"method": "to_string", "lossy": False},
        "integer": {"method": "truncate", "lossy": True, "note": "Truncates decimal portion"},
        "boolean": {"method": "cast", "lossy": True},
    },
    "boolean": {
        "string": {"method": "to_string", "lossy": False, "mapping": {True: "true", False: "false"}},
        "integer": {"method": "cast", "lossy": False, "mapping": {True: 1, False: 0}},
    },
    "datetime": {
        "string": {"method": "format", "lossy": False, "format": "%Y-%m-%dT%H:%M:%SZ"},
        "date": {"method": "truncate_time", "lossy": True},
        "integer": {"method": "unix_timestamp", "lossy": False},
    },
    "date": {
        "string": {"method": "format", "lossy": False, "format": "%Y-%m-%d"},
        "datetime": {"method": "add_midnight", "lossy": False},
    },
    "json": {
        "string": {"method": "serialize", "lossy": False},
        "object": {"method": "parse", "lossy": False},
    },
    "array": {
        "string": {"method": "join", "lossy": True, "separator": ","},
        "json": {"method": "serialize", "lossy": False},
    },
    "binary": {
        "string": {"method": "base64_encode", "lossy": False},
        "hex": {"method": "hex_encode", "lossy": False},
    },
}


def suggest_type_conversion(source_type: str, target_type: str) -> dict | None:
    """Get a non-authoritative conversion hint (LLM assist only)."""
    source = source_type.lower()
    target = target_type.lower()
    if source == target:
        return {"method": "identity", "lossy": False, "authoritative": False}
    conversions = TYPE_CONVERSION_MATRIX.get(source, {})
    hit = conversions.get(target)
    if hit is None:
        return None
    return {**hit, "authoritative": False}


def suggest_type_conversion_non_authoritative(
    source_type: str, target_type: str
) -> dict:
    """Explicit non-authoritative wrapper for assistants."""
    base = suggest_type_conversion(source_type, target_type) or {
        "method": "unknown",
        "lossy": True,
    }
    return {
        **base,
        "authoritative": False,
        "authority_note": AUTHORITY_NOTE,
        "use_instead": "services.conversion_contract.classify_conversion",
    }


def get_compatible_types(source_type: str) -> list[str]:
    """List assist types — not a migration allow-list."""
    return list(TYPE_CONVERSION_MATRIX.get(source_type.lower(), {}).keys())
