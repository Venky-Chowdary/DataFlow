"""Universal transfer capability registry — any source × any destination."""

from __future__ import annotations

LIVE_SOURCE_FORMATS = ["csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"]
LIVE_DEST_DATABASES = [
    "mongodb", "postgresql", "snowflake", "mysql", "bigquery", "redshift",
    "dynamodb", "s3", "gcs", "redis", "elasticsearch",
]
LIVE_SOURCE_DATABASES = [
    "postgresql", "mongodb", "snowflake", "mysql", "bigquery", "redshift",
    "dynamodb", "s3", "gcs", "redis", "elasticsearch",
]
LIVE_DEST_FILE_FORMATS = ["csv", "json", "jsonl", "tsv"]

# (source_kind, source_format, dest_kind, dest_format) -> live
LIVE_MATRIX: set[tuple[str, str, str, str]] = set()

# File → Database
for _sf in LIVE_SOURCE_FORMATS:
    for _db in LIVE_DEST_DATABASES:
        LIVE_MATRIX.add(("file", _sf, "database", _db))

# File → File (core conversions)
for _pair in [
    ("csv", "json"), ("csv", "csv"), ("csv", "jsonl"), ("csv", "tsv"),
    ("tsv", "csv"), ("tsv", "json"), ("json", "csv"), ("json", "json"), ("json", "jsonl"),
    ("jsonl", "csv"), ("jsonl", "json"),
]:
    LIVE_MATRIX.add(("file", _pair[0], "file_export", _pair[1]))

# Database → Database & Database → File
for _src in LIVE_SOURCE_DATABASES:
    for _db in LIVE_DEST_DATABASES:
        LIVE_MATRIX.add(("database", _src, "database", _db))
    for _ef in ["csv", "json", "jsonl", "tsv"]:
        LIVE_MATRIX.add(("database", _src, "file_export", _ef))


def validate_transfer(source_kind: str, source_format: str, dest_kind: str, dest_format: str) -> tuple[bool, str]:
    key = (source_kind, source_format.lower(), dest_kind, dest_format.lower())
    if key in LIVE_MATRIX:
        return True, "supported"
    for sk, sf, dk, df in LIVE_MATRIX:
        if sk == source_kind and dk == dest_kind and df == dest_format.lower():
            if source_kind == "file" and source_format.lower() in LIVE_SOURCE_FORMATS:
                return True, "supported"
    return False, f"Combination {source_kind}/{source_format} → {dest_kind}/{dest_format} not yet live"


def get_capabilities() -> dict:
    from .connector_capabilities import manifest_summary, transfer_live_driver_types

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
    summary = manifest_summary()
    return {
        "live_combinations": combos,
        "source_formats": LIVE_SOURCE_FORMATS,
        "destination_databases": LIVE_DEST_DATABASES,
        "destination_file_formats": LIVE_DEST_FILE_FORMATS,
        "source_databases": LIVE_SOURCE_DATABASES,
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
