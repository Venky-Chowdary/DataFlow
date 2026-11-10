"""Where embedded databases and local warehouses may live on the API host.

Always on (every environment, no flag): SQLite/DuckDB paths are resolved
(``..`` and symlinks followed) and may not point into the service's own data
directory (job store, ledgers, secrets) except its ``sqlite/`` and ``duckdb/``
volume sub-directories; Iceberg schema, table and catalog names may not climb
out of the warehouse; an explicit ``DATAFLOW_SQLITE_ROOT`` /
``DATAFLOW_DUCKDB_ROOT`` / ``DATAFLOW_ICEBERG_ROOT`` is honoured.

Behind ``DATAFLOW_CONFINE_LOCAL_FILES`` (default off, the behaviour production
had): the *implicit* roots. With the flag on, a production process confines
SQLite to ``<data_dir>/sqlite``, DuckDB to ``<data_dir>/duckdb`` (or the SQLite
root) and Iceberg to ``<data_dir>/iceberg``, and anchors relative names there.
With it off, a path outside those folders is allowed and recorded as a
``local_file.would_confine`` audit event so the owner can see what the flag would
refuse (``scripts/audit_sqlite_paths.py`` lists the saved connectors).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.would_apply_audit import audit_would_apply

FLAG = "DATAFLOW_CONFINE_LOCAL_FILES"


def confine_local_files() -> bool:
    raw = str(getenv_brand("CONFINE_LOCAL_FILES", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def volume_root(subdir: str) -> Path:
    """``<data_dir>/<subdir>``: the persistent-volume folder the flag would confine to.

    Not created; ``implicit_root`` creates it when the flag is on.
    """
    from services.platform_config import data_dir_path

    return data_dir_path() / subdir


def implicit_root(subdir: str) -> Path | None:
    """The production default root, only while ``DATAFLOW_CONFINE_LOCAL_FILES`` is on."""
    from services.platform_config import is_production

    if not (confine_local_files() and is_production()):
        return None
    root = volume_root(subdir)
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return root


def is_within(child: Path, parent: Path) -> bool:
    c = os.path.normcase(str(child))
    p = os.path.normcase(str(parent))
    try:
        return os.path.commonpath([c, p]) == p
    except ValueError:  # different drives on Windows
        return False


def audit_would_confine(subdir: str, resolved: Path) -> None:
    """Flag off in production: record a local file the flag would refuse."""
    from services.platform_config import is_production

    if confine_local_files() or not is_production():
        return
    root = volume_root(subdir).resolve(strict=False)
    if is_within(resolved, root):
        return
    audit_would_apply(
        "local_file.would_confine",
        resource=f"{subdir}:{resolved}",
        dedupe_key=(subdir, str(resolved)),
        flag=FLAG,
        details={"kind": subdir, "path": str(resolved), "would_be_root": str(root)},
    )


# --- D276: SQLite as a destination on the hosted cloud ----------------------
#
# On the hosted cloud the API, the worker and the scheduler are separate
# services, each with its own filesystem; a volume attaches to one service
# only. A job writes the SQLite file on the worker, the API lists the file on
# its own disk, and a redeploy can replace either: "Data kept at <table>" was
# true on one server and false everywhere else. SQLite also has one writer at a
# time. So the hosted cloud does not offer SQLite as a destination; a
# self-hosted single-machine install keeps it, and SQLite as a source is
# unchanged. ``DATAWRAP_SQLITE_DESTINATION=on|off`` (or ``DATAFLOW_…``) decides
# explicitly, so a self-hoster on any cloud can turn it back on.
SQLITE_DESTINATION_FLAG = "DATAWRAP_SQLITE_DESTINATION"
SQLITE_DESTINATION_UNAVAILABLE_LABEL = "Not available on cloud"
_SQLITE_DRIVERS = frozenset({"sqlite", "sqlite3"})


def sqlite_destination_message() -> str:
    from services.brand import PRODUCT_NAME

    return (
        f"SQLite isn't available as a destination on {PRODUCT_NAME} Cloud: the file "
        "would live on a single server and isn't kept between runs. Use PostgreSQL, "
        f"MySQL or another database destination, or run {PRODUCT_NAME} self-hosted "
        f"(where {SQLITE_DESTINATION_FLAG}=on keeps it)."
    )


def sqlite_destination_allowed() -> bool:
    """May SQLite be a transfer destination in this process? Env reads only.

    1. ``DATAWRAP_SQLITE_DESTINATION`` / ``DATAFLOW_SQLITE_DESTINATION`` on/off wins;
    2. otherwise off on a deployed Railway service (deploy-time markers only —
       a ``railway run`` shell, CI or a test run is not the hosted cloud);
    3. otherwise on. Never keyed off ``is_production()``: a self-hosted
       production install keeps SQLite.
    """
    from services.brand_env import getenv_brand_bool
    from services.platform_config import is_railway_deployed

    return getenv_brand_bool("SQLITE_DESTINATION", not is_railway_deployed())


def _is_sqlite_destination(dest_type: str, connection_string: str = "") -> bool:
    raw = (dest_type or "").strip().lower()
    if not raw:
        return False
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
    except ImportError:  # loaded with apps/api/src on the path
        from transfer.connector_capabilities import resolve_driver_type  # type: ignore[no-redef]
    driver = resolve_driver_type(raw)
    if raw in _SQLITE_DRIVERS or driver in _SQLITE_DRIVERS:
        return True
    # A generic SQL connector pointed at a ``sqlite:`` URL writes the same local file.
    return driver == "generic_sql" and (connection_string or "").strip().lower().startswith("sqlite:")


def sqlite_destination_refusal(dest_type: str, connection_string: str = "") -> str | None:
    """The operator message when ``dest_type`` is SQLite and SQLite may not be a destination here."""
    if sqlite_destination_allowed() or not _is_sqlite_destination(dest_type, connection_string):
        return None
    return sqlite_destination_message()


def endpoint_sqlite_destination_refusal(endpoint: Any, workspace_id: str | None = None) -> str | None:
    """:func:`sqlite_destination_refusal` for a destination endpoint (DTO, dict or EndpointConfig).

    A saved ``connector_id`` without an inline format is looked up (one store
    read) only while SQLite is refused here, so this costs nothing elsewhere.
    """
    if endpoint is None or sqlite_destination_allowed():
        return None

    def get(key: str) -> Any:
        return endpoint.get(key) if isinstance(endpoint, dict) else getattr(endpoint, key, None)

    refusal = sqlite_destination_refusal(
        str(get("format") or get("type") or ""), str(get("connection_string") or "")
    )
    connector_id = str(get("connector_id") or "").strip()
    if refusal or not connector_id:
        return refusal
    from services.connector_store import get_connector

    saved = get_connector(connector_id, workspace_id=workspace_id or None)
    if saved is None:
        return None
    return sqlite_destination_refusal(
        str(getattr(saved, "type", "") or ""), str(getattr(saved, "connection_string", "") or "")
    )
