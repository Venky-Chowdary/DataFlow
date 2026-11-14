"""Where a file path a tenant supplies may point on the API host.

A connector's settings name files the API process then opens: a Google key
file (``services.google_credentials_policy``), a TLS CA / client certificate /
key / wallet (``connectors.tls_file_options``). The process can read the
platform's own files, so such a path may only point inside the areas a
connector may open on this server:

* an upload the workspace owns (``local_file_policy.connector_may_open_upload_path``);
* its own export folder (``export_paths.connector_may_open_export_path``);
* a directory the operator lists in ``DATAFLOW_SOURCE_PATH_ROOTS``.

Symlinks and ``..`` are resolved first, so a link or a climb out of an area is
judged where it lands.
"""

from __future__ import annotations

import os
from pathlib import Path


def host_file_allowed(path: str) -> bool:
    """``path`` (resolved) lies inside an area a connector may open."""
    from services.brand_env import getenv_brand_str
    from services.export_paths import connector_may_open_export_path, exports_dir
    from services.local_file_policy import connector_may_open_upload_path, is_within
    from services.platform_config import upload_dir_path

    try:
        resolved = Path(path).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        return False
    if is_within(resolved, upload_dir_path().expanduser().resolve(strict=False)):
        return connector_may_open_upload_path(resolved)
    if is_within(resolved, exports_dir(create=False).expanduser().resolve(strict=False)):
        return connector_may_open_export_path(resolved)
    roots = [raw.strip() for raw in getenv_brand_str("SOURCE_PATH_ROOTS", "").split(os.pathsep) if raw.strip()]
    return any(is_within(resolved, Path(root).expanduser().resolve(strict=False)) for root in roots)
