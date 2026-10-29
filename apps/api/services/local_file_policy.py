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

from services.brand_env import getenv_brand
from services.would_apply_audit import audit_would_apply

FLAG = "DATAFLOW_CONFINE_LOCAL_FILES"


def confine_local_files() -> bool:
    raw = str(getenv_brand("CONFINE_LOCAL_FILES", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def volume_root(subdir: str) -> Path:
    """``<data_dir>/<subdir>``: the persistent-volume folder the flag would confine to."""
    from services.platform_config import data_dir

    return data_dir() / subdir


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
