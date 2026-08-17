"""Durable staging for file-source TransferRequests (claim-queue / HA).

Studio uploads arrive as in-memory ``source_content``. Mongo job payloads must
not embed multi-MB bytes, and claim workers reconstruct only from the payload.
Without a durable path/URI, production claim mode fails every file transfer with
``File re-upload required after restart`` — even on a fresh submit.

SSOT: spill to ``upload_dir`` always; also ``stage_bytes`` when object store is
configured so multi-replica workers can materialize.
"""

from __future__ import annotations

import logging
import re
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.transfer.models import TransferRequest

_logger = logging.getLogger(__name__)


def _safe_filename(name: str) -> str:
    raw = (name or "upload.bin").strip() or "upload.bin"
    base = Path(raw).name
    return re.sub(r"[^A-Za-z0-9._-]", "_", base)[:160] or "upload.bin"


#: Filename prefix for per-job source spills in the shared upload directory.
#: These belong to one transfer, not to the workspace's dataset catalog.
TRANSFER_STAGING_PREFIX = "xfer_"


def is_transfer_staging_file(name: str) -> bool:
    """True when ``name`` is a per-job transfer spill rather than a user upload."""
    return str(name or "").startswith(TRANSFER_STAGING_PREFIX)


def file_source_bytes_available(request: "TransferRequest") -> bool:
    """True when Execute can read file bytes (memory, path, or object URI)."""
    if getattr(request, "source", None) is None:
        return False
    if getattr(request.source, "kind", "") != "file":
        return True
    if request.source_content:
        return True
    path = (getattr(request, "source_path", None) or "").strip()
    if path and Path(path).is_file():
        return True
    uri = (getattr(request, "source_object_uri", None) or "").strip()
    return bool(uri.startswith("s3://"))


def requires_file_reupload(request: "TransferRequest") -> bool:
    """True when the *serialized* job cannot recover file bytes after claim/restart.

    In-memory ``source_content`` is stripped from Mongo payloads, so content alone
    still requires re-upload unless ``source_path`` / ``source_object_uri`` was
    stamped via :func:`persist_file_source`.
    """
    if getattr(request, "source", None) is None:
        return False
    if getattr(request.source, "kind", "") != "file":
        return False
    path = (getattr(request, "source_path", None) or "").strip()
    if path:
        return False
    uri = (getattr(request, "source_object_uri", None) or "").strip()
    return not uri.startswith("s3://")


def persist_file_source(
    request: "TransferRequest",
    *,
    job_token: str = "",
) -> "TransferRequest":
    """Spill ``source_content`` to disk (+ object store) and stamp path/URI.

    Idempotent when ``source_path`` already points at an existing file.
    Leaves ``source_content`` in memory for same-process local executors.
    """
    if getattr(request.source, "kind", "") != "file":
        return request
    existing = (getattr(request, "source_path", None) or "").strip()
    if existing and Path(existing).is_file():
        return request
    content = request.source_content or b""
    if not content:
        return request

    from services.platform_config import upload_dir

    token = (job_token or uuid.uuid4().hex)[:32]
    filename = _safe_filename(request.source_filename or "upload.bin")
    dest = upload_dir() / f"{TRANSFER_STAGING_PREFIX}{token}_{filename}"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)
    request.source_path = str(dest)

    try:
        from services.object_store import stage_bytes

        uri = stage_bytes(f"transfers/{token}/{filename}", content)
        if uri:
            request.source_object_uri = uri
    except Exception:
        _logger.debug("object-store stage skipped for transfer file", exc_info=True)
    return request


def hydrate_file_source(request: "TransferRequest") -> "TransferRequest":
    """Ensure ``source_path`` exists for claim workers (materialize from URI)."""
    if getattr(request.source, "kind", "") != "file":
        return request
    path = (getattr(request, "source_path", None) or "").strip()
    if path and Path(path).is_file():
        return request
    uri = (getattr(request, "source_object_uri", None) or "").strip()
    if not uri.startswith("s3://"):
        return request
    try:
        from services.object_store import materialize_local
        from services.platform_config import upload_dir

        filename = _safe_filename(request.source_filename or "upload.bin")
        dest = upload_dir() / f"hydrate_{uuid.uuid4().hex[:12]}_{filename}"
        if materialize_local(uri, dest):
            request.source_path = str(dest)
    except Exception:
        _logger.warning("Failed to materialize transfer source from %s", uri, exc_info=True)
    return request
