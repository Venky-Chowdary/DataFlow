"""Bounds for connector and transfer requests, applied before any driver dial.

Port 0 is the unset default for portless engines (sqlite, files, object
stores). Every other port has to be 1–65535. Oversized fields are refused
here so a 99999 port or a multi-megabyte host never reaches a socket.
"""

from __future__ import annotations

PORTLESS_TYPES = frozenset({
    "sqlite",
    "duckdb",
    "s3",
    "gcs",
    "azure_blob",
    "file",
    "local",
    "csv",
    "json",
    "parquet",
    "excel",
    "xlsx",
})

NAME_MAX = 200
HOST_MAX = 253
IDENT_MAX = 128
CONNSTR_MAX = 4096
TYPE_MAX = 64
EXECUTE_LIMIT_MAX = 10_000_000
LIST_JOBS_MAX = 100
# Internal list pages (replay diffs) ask for a few hundred. The Pilot tool
# caps at LIST_JOBS_MAX before it reaches the store.
STORE_LIST_JOBS_MAX = 1000


def connector_port_error(port: object, db_type: str = "") -> str:
    """Empty when the port may be handed to a driver."""
    if isinstance(port, bool) or port is None:
        return "Port must be an integer from 1 to 65535."
    try:
        n = int(port)
    except (TypeError, ValueError):
        return "Port must be an integer from 1 to 65535."
    # 0 means "unset". TCP drivers fill their default before the probe
    # (effective_port). Portless engines stay at 0. Negative ports and
    # anything above 65535 never dial.
    if n == 0:
        # PORTLESS_TYPES keep 0. TCP types use 0 as unset until the draft
        # stamps the driver default. Either way, 0 is not a dial.
        return ""
    if n < 1 or n > 65535:
        return f"Port {n} is outside 1–65535."
    return ""


def _length_error(label: str, value: object, maximum: int, *, required: bool = False) -> str:
    text = "" if value is None else str(value)
    if required and not text.strip():
        return f"{label} is required."
    if len(text) > maximum:
        return f"{label} is longer than {maximum} characters."
    return ""


def connector_request_errors(
    *,
    name: object = "",
    db_type: object = "",
    host: object = "",
    port: object = 0,
    database: object = "",
    username: object = "",
    schema: object = "",
    connection_string: object = "",
    require_name: bool = False,
) -> str:
    """First bound that fails, or empty when the draft may be probed."""
    checks = (
        _length_error("Name", name, NAME_MAX, required=require_name),
        _length_error("Type", db_type, TYPE_MAX),
        _length_error("Host", host, HOST_MAX),
        _length_error("Database", database, IDENT_MAX),
        _length_error("Username", username, IDENT_MAX),
        _length_error("Schema", schema, IDENT_MAX),
        _length_error("Connection string", connection_string, CONNSTR_MAX),
        connector_port_error(port, str(db_type or "")),
    )
    return next((item for item in checks if item), "")


def bounded_job_list_limit(limit: object, *, default: int = 50, ceiling: int = STORE_LIST_JOBS_MAX) -> int:
    """Page size for the job store. Negative limits return nothing.

    MongoDB treats a negative ``limit`` as a positive page, which is how
    ``limit=-5`` came back as five jobs. Zero means an empty page.
    """
    if isinstance(limit, bool):
        return default
    try:
        n = int(limit)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if n < 1:
        return 0
    return min(n, ceiling)
