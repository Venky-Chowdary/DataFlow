"""Atomic JSON file writes for small persistence files."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

from services.value_serializer import json_default

_WRITE_LOCK = threading.Lock()


def write_json_atomic(
    path: Path,
    data: dict[str, Any],
    *,
    indent: int = 2,
    default: Any | None = json_default,
) -> None:
    """Write ``data`` to ``path`` atomically using a temp file and ``os.replace``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with _WRITE_LOCK:
        tmp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=path.parent, delete=False
            ) as tmp:
                tmp_path = Path(tmp.name)
                json.dump(data, tmp, indent=indent, default=default)
            os.replace(tmp_path, path)
        except Exception:
            if tmp_path is not None and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            raise
