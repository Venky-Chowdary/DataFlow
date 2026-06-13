"""
DataTransfer.space — Type Conversion Matrix

Universal type mapping rules for data transformation.
"""

from __future__ import annotations

TYPE_CONVERSION_MATRIX: dict[str, dict[str, dict]] = {
    "string": {
        "integer": {"method": "cast", "lossy": False, "validation": r"^-?\d+$"},
        "decimal": {"method": "cast", "lossy": False, "validation": r"^-?\d+\.?\d*$"},
        "boolean": {"method": "parse_bool", "lossy": False, "mapping": {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}},
        "datetime": {"method": "parse_date", "lossy": False, "formats": ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"]},
        "date": {"method": "parse_date", "lossy": False, "formats": ["%Y-%m-%d", "%m/%d/%Y"]},
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
    """Get conversion rule between source and target types."""
    source = source_type.lower()
    target = target_type.lower()
    if source == target:
        return {"method": "identity", "lossy": False}
    conversions = TYPE_CONVERSION_MATRIX.get(source, {})
    return conversions.get(target)


def get_compatible_types(source_type: str) -> list[str]:
    """List all types that can be converted from source type."""
    return list(TYPE_CONVERSION_MATRIX.get(source_type.lower(), {}).keys())
