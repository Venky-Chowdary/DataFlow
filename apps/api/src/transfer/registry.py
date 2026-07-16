"""Universal transfer capability registry — any source × any destination."""

from __future__ import annotations

LIVE_SOURCE_FORMATS = ["csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"]
LIVE_DEST_FILE_FORMATS = ["csv", "json", "jsonl", "tsv", "excel", "parquet", "ndjson"]

# Live drivers are discovered at import time; object stores and warehouses count
# as database destinations, while the listed file formats are file targets.
_FILE_FORMATS = {"csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"}

def _live_db_drivers(live_fn_name: str = "transfer_live_driver_types") -> list[str]:
    try:
        from .connector_capabilities import transfer_live_driver_types, source_live_driver_types, dest_live_driver_types
    except ImportError:
        # Support loading this file directly (e.g. test_registry.py)
        import importlib.util
        from pathlib import Path
        path = Path(__file__).resolve().parent / "connector_capabilities.py"
        spec = importlib.util.spec_from_file_location("connector_capabilities_for_registry", path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        transfer_live_driver_types = mod.transfer_live_driver_types
        source_live_driver_types = getattr(mod, "source_live_driver_types", transfer_live_driver_types)
        dest_live_driver_types = getattr(mod, "dest_live_driver_types", transfer_live_driver_types)
    if live_fn_name == "source":
        live_fn = source_live_driver_types
    elif live_fn_name == "dest":
        live_fn = dest_live_driver_types
    else:
        live_fn = transfer_live_driver_types
    return sorted(d for d in live_fn() if d not in _FILE_FORMATS)

LIVE_DEST_DATABASES = _live_db_drivers("dest")
LIVE_SOURCE_DATABASES = _live_db_drivers("source")

# (source_kind, source_format, dest_kind, dest_format) -> live
LIVE_MATRIX: set[tuple[str, str, str, str]] = set()

# File → Database
for _sf in LIVE_SOURCE_FORMATS:
    for _db in LIVE_DEST_DATABASES:
        LIVE_MATRIX.add(("file", _sf, "database", _db))

# File → File (any supported source format → any supported export format)
for _src_fmt in LIVE_SOURCE_FORMATS:
    for _dst_fmt in LIVE_DEST_FILE_FORMATS:
        LIVE_MATRIX.add(("file", _src_fmt, "file_export", _dst_fmt))

# Database → Database & Database → File
for _src in LIVE_SOURCE_DATABASES:
    for _db in LIVE_DEST_DATABASES:
        LIVE_MATRIX.add(("database", _src, "database", _db))
    for _ef in LIVE_DEST_FILE_FORMATS:
        LIVE_MATRIX.add(("database", _src, "file_export", _ef))


def validate_transfer(source_kind: str, source_format: str, dest_kind: str, dest_format: str) -> tuple[bool, str]:
    def _resolve(fmt: str) -> str:
        try:
            from .connector_capabilities import resolve_driver_type
            return resolve_driver_type(fmt)
        except Exception:
            try:
                from transfer.connector_capabilities import resolve_driver_type
                return resolve_driver_type(fmt)
            except Exception:
                return fmt

    def _source_only_supported(fmt: str) -> bool:
        try:
            from .connector_capabilities import get_capabilities, _source_only_ready
            caps = get_capabilities(fmt, fmt)
            return _source_only_ready(caps)
        except Exception:
            return False

    def _dest_supported(fmt: str) -> bool:
        if fmt.lower() in LIVE_DEST_FILE_FORMATS:
            return True
        if fmt in LIVE_DEST_DATABASES:
            return True
        try:
            from .connector_capabilities import dest_ready, get_capabilities
            caps = get_capabilities(fmt, fmt)
            return dest_ready(caps)
        except Exception:
            return False

    src_fmt = _resolve(source_format)
    dst_fmt = _resolve(dest_format)

    # Source-only SaaS connectors can feed any live destination.
    if source_kind == "database" and _source_only_supported(src_fmt):
        if dest_kind == "database" and _dest_supported(dst_fmt):
            return True, "supported"
        if dest_kind == "file_export" and dst_fmt.lower() in LIVE_DEST_FILE_FORMATS:
            return True, "supported"

    key = (source_kind, src_fmt.lower(), dest_kind, dst_fmt.lower())
    if key in LIVE_MATRIX:
        return True, "supported"
    for sk, sf, dk, df in LIVE_MATRIX:
        if sk == source_kind and dk == dest_kind and df == dst_fmt.lower():
            if source_kind == "file" and src_fmt.lower() in LIVE_SOURCE_FORMATS:
                return True, "supported"
    return False, f"Combination {source_kind}/{source_format} → {dest_kind}/{dest_format} not yet live"


def _live_catalog_ids() -> list[str]:
    """Return catalog IDs that are actually live so the UI can select them."""
    try:
        from services.catalog_service import search_catalog

        return [c["id"] for c in search_catalog(status="live", limit=1000).get("connectors", [])]
    except Exception:
        return _live_db_drivers()


def get_capabilities() -> dict:
    from .connector_capabilities import (
        _source_only_ready,
        dest_ready,
        get_capabilities as _caps,
        manifest_summary,
        resolve_driver_type,
        source_ready,
        transfer_live_driver_types,
    )

    combos = []
    for sk, sf, dk, df in sorted(LIVE_MATRIX):
        op = "upload" if sk == "file" and dk == "database" else (
            "migration" if sk == "database" and dk == "database" else (
                "convert" if sk == "file" and dk == "file_export" else "dump"
            )
        )
        combos.append({
            "source_kind": sk,
            "source_format": sf,
            "dest_kind": dk,
            "dest_format": df,
            "operation": op,
            "status": "live",
        })
    # Add source-only SaaS routes (source → database / file_export) so the UI can
    # advertise and validate them without bloating the seedable route matrix.
    for src in _source_only_driver_types():
        for dst in LIVE_DEST_DATABASES:
            combos.append({
                "source_kind": "database",
                "source_format": src,
                "dest_kind": "database",
                "dest_format": dst,
                "operation": "migration",
                "status": "live",
            })
        for dst_fmt in LIVE_DEST_FILE_FORMATS:
            combos.append({
                "source_kind": "database",
                "source_format": src,
                "dest_kind": "file_export",
                "dest_format": dst_fmt,
                "operation": "dump",
                "status": "live",
            })

    summary = manifest_summary()
    live_catalog = _live_catalog_ids()

    source_dbs = []
    dest_dbs = []
    for cid in live_catalog:
        driver = resolve_driver_type(cid)
        caps = _caps(driver, cid)
        if source_ready(caps) or _source_only_ready(caps):
            source_dbs.append(cid)
        if dest_ready(caps):
            dest_dbs.append(cid)

    return {
        "live_combinations": combos,
        "source_formats": LIVE_SOURCE_FORMATS,
        "destination_databases": dest_dbs,
        "destination_file_formats": LIVE_DEST_FILE_FORMATS,
        "source_databases": source_dbs,
        "transfer_live_drivers": transfer_live_driver_types(),
        "transfer_live_count": summary["transfer_live_count"],
        "connect_only_count": summary["connect_only_count"],
        "live_route_combinations": summary["live_route_combinations"],
        "operations": ["upload", "migration", "convert", "dump", "transfer"],
        "auto_ddl": True,
        "description": (
            f"{summary['transfer_live_count']} drivers support full transfer. "
            f"{summary['live_route_combinations']} route combinations are live. "
            "Catalog roadmap entries require driver implementation before production use."
        ),
    }


def _source_only_driver_types() -> list[str]:
    try:
        from .connector_capabilities import _DRIVER_CAPS, _source_only_ready
    except Exception:
        return []
    return sorted(k for k, v in _DRIVER_CAPS.items() if _source_only_ready(v))
