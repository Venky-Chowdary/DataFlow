"""Universal transfer capability registry — any source × any destination."""

from __future__ import annotations

LIVE_SOURCE_FORMATS = ["csv", "tsv", "json", "jsonl", "ndjson"]
LIVE_DEST_DATABASES = ["mongodb", "postgresql", "snowflake"]
LIVE_DEST_FILE_FORMATS = ["csv", "json", "jsonl"]

# (source_kind, source_format, dest_kind, dest_format) -> live
LIVE_MATRIX: set[tuple[str, str, str, str]] = {
    # File → Database
    ("file", "csv", "database", "mongodb"),
    ("file", "csv", "database", "postgresql"),
    ("file", "csv", "database", "snowflake"),
    ("file", "tsv", "database", "mongodb"),
    ("file", "tsv", "database", "postgresql"),
    ("file", "tsv", "database", "snowflake"),
    ("file", "json", "database", "mongodb"),
    ("file", "json", "database", "postgresql"),
    ("file", "json", "database", "snowflake"),
    ("file", "jsonl", "database", "mongodb"),
    ("file", "jsonl", "database", "postgresql"),
    ("file", "jsonl", "database", "snowflake"),
    # File → File
    ("file", "csv", "file_export", "json"),
    ("file", "csv", "file_export", "csv"),
    ("file", "json", "file_export", "csv"),
    ("file", "json", "file_export", "json"),
    ("file", "json", "file_export", "jsonl"),
    # Database → Database
    ("database", "postgresql", "database", "mongodb"),
    ("database", "postgresql", "database", "postgresql"),
    ("database", "postgresql", "database", "snowflake"),
    ("database", "mongodb", "database", "mongodb"),
    ("database", "mongodb", "database", "postgresql"),
    ("database", "mongodb", "database", "snowflake"),
    # Database → File
    ("database", "postgresql", "file_export", "csv"),
    ("database", "postgresql", "file_export", "json"),
    ("database", "mongodb", "file_export", "csv"),
    ("database", "mongodb", "file_export", "json"),
    # Snowflake warehouse as source
    ("database", "snowflake", "database", "mongodb"),
    ("database", "snowflake", "database", "postgresql"),
    ("database", "snowflake", "database", "snowflake"),
    ("database", "snowflake", "file_export", "csv"),
    ("database", "snowflake", "file_export", "json"),
    # File → Snowflake warehouse (alias via wildcard)
}


def validate_transfer(source_kind: str, source_format: str, dest_kind: str, dest_format: str) -> tuple[bool, str]:
    key = (source_kind, source_format.lower(), dest_kind, dest_format.lower())
    if key in LIVE_MATRIX:
        return True, "supported"
    # Allow file formats to wildcard match
    for sk, sf, dk, df in LIVE_MATRIX:
        if sk == source_kind and dk == dest_kind and df == dest_format.lower():
            if source_kind == "file" and source_format.lower() in LIVE_SOURCE_FORMATS:
                return True, "supported"
    return False, f"Combination {source_kind}/{source_format} → {dest_kind}/{dest_format} not yet live"


def get_capabilities() -> dict:
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
    return {
        "live_combinations": combos,
        "source_formats": LIVE_SOURCE_FORMATS,
        "destination_databases": LIVE_DEST_DATABASES,
        "destination_file_formats": LIVE_DEST_FILE_FORMATS,
        "operations": ["upload", "migration", "convert", "dump", "transfer"],
        "auto_ddl": True,
        "description": "Universal Data Freedom — any file, any database, any warehouse",
    }
