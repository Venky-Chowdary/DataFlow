"""Universal route intelligence — any source × destination compatibility."""

from __future__ import annotations

from typing import Any


def analyze_route(
    source_kind: str,
    source_format: str,
    dest_kind: str,
    dest_format: str,
) -> dict[str, Any]:
    """Score and describe a transfer route with conversion and driver hints."""
    from src.transfer.registry import validate_transfer, get_capabilities as registry_caps
    from src.transfer.connector_capabilities import get_capabilities as driver_caps, manifest_summary
    from services.format_converter import can_convert, conversion_matrix

    src_fmt = (source_format or "csv").lower()
    dst_fmt = (dest_format or "mongodb").lower()
    ok, msg = validate_transfer(source_kind, src_fmt, dest_kind, dst_fmt)

    conversion_needed = (
        source_kind == "file"
        and dest_kind == "file_export"
        and src_fmt != dst_fmt
    )
    conversion_supported = (
        can_convert(src_fmt, dst_fmt) if conversion_needed else True
    )

    operation = "transfer"
    if source_kind == "file" and dest_kind == "database":
        operation = "upload"
    elif source_kind == "database" and dest_kind == "database":
        operation = "migration"
    elif source_kind == "file" and dest_kind == "file_export":
        operation = "convert"
    elif source_kind == "database" and dest_kind == "file_export":
        operation = "dump"

    score = 100 if ok else 0
    hints: list[str] = []
    warnings: list[str] = []

    if ok:
        if conversion_needed and conversion_supported:
            hints.append(f"Format conversion {src_fmt.upper()} → {dst_fmt.upper()} via unified converter")
            score = 95
        elif conversion_needed and not conversion_supported:
            ok = False
            score = 0
            warnings.append(f"Conversion {src_fmt} → {dst_fmt} not supported")
        if dest_kind == "database":
            caps = driver_caps(dst_fmt)
            if not caps.get("write"):
                ok = False
                score = 0
                warnings.append(f"Destination driver {dst_fmt} is not write-ready")
            else:
                hints.append(f"Destination {dst_fmt} supports typed DDL + batched write")
        if source_kind == "database":
            scaps = driver_caps(src_fmt)
            if not scaps.get("read"):
                warnings.append(f"Source driver {src_fmt} read path may be limited")
                score = min(score, 80)
    else:
        warnings.append(msg)

    manifest = manifest_summary()
    reg = registry_caps()

    alternatives: list[dict[str, str]] = []
    if not ok and source_kind == "file":
        for tgt in conversion_matrix()["matrix"].get(src_fmt, []):
            if validate_transfer("file", src_fmt, "file_export", tgt)[0]:
                alternatives.append({"dest_kind": "file_export", "dest_format": tgt, "reason": "Supported export format"})
        for db in reg.get("destination_databases", [])[:5]:
            if validate_transfer("file", src_fmt, "database", db)[0]:
                alternatives.append({"dest_kind": "database", "dest_format": db, "reason": "Live database route"})

    return {
        "supported": ok,
        "score": score,
        "message": msg,
        "operation": operation,
        "source_kind": source_kind,
        "source_format": src_fmt,
        "dest_kind": dest_kind,
        "dest_format": dst_fmt,
        "conversion_needed": conversion_needed,
        "conversion_supported": conversion_supported,
        "hints": hints,
        "warnings": warnings,
        "alternatives": alternatives[:6],
        "live_route_combinations": manifest.get("live_route_combinations", 0),
        "transfer_live_drivers": manifest.get("transfer_live_drivers", []),
    }
