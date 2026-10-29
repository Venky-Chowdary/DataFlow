"""Locate an uploaded dataset file and refuse anything outside that tree.

Pilot names a dataset. The bytes live in an upload directory. A confirm ack
must not be able to point the engine at an arbitrary path.
"""

from __future__ import annotations

import os
from pathlib import Path


def dataset_roots() -> list[Path]:
    """Directories an uploaded dataset is allowed to live in."""
    from services.platform_config import upload_dir
    from src.ai.training.universal_data_feeder import UniversalDataFeeder

    roots: list[Path] = []
    seen: set[str] = set()
    for raw in (upload_dir(), *(UniversalDataFeeder().upload_dirs or [])):
        path = Path(raw)
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        roots.append(path)
    return roots


def assert_dataset_file(path: str | Path) -> Path:
    """Return the resolved file when it is a user upload under a dataset root."""
    from services.transfer_file_staging import is_transfer_staging_file

    raw = Path(path)
    if not str(path or "").strip():
        raise ValueError("Uploaded file is missing.")
    try:
        resolved = raw.resolve()
    except OSError as exc:
        raise ValueError("Uploaded file is not on disk.") from exc
    # Sandbox first: an ack naming a path outside the upload tree is refused
    # for that reason whether or not the file exists — existence is never a
    # hint about which directories are legal to read.
    if not _inside_any(resolved, dataset_roots()):
        raise ValueError("That file is outside the upload directory.")
    if is_transfer_staging_file(resolved.name):
        raise ValueError("That file belongs to one transfer, not to the dataset catalog.")
    if not resolved.is_file():
        raise ValueError("Uploaded file is not on disk.")
    return resolved


def _inside_any(resolved: Path, roots: list[Path]) -> bool:
    for root in roots:
        try:
            root_resolved = root.resolve()
        except OSError:
            continue
        if resolved == root_resolved or root_resolved in resolved.parents:
            return True
    return False


def transfer_source_roots() -> list[Path]:
    """Directories a file transfer may read ``source_path`` from.

    The dataset roots (which hold per-transfer staging and hydrated copies)
    plus any operator-declared ``DATAWRAP_SOURCE_PATH_ROOTS`` entries,
    separated by ``os.pathsep``.
    """
    from services.brand_env import getenv_brand_str

    roots = dataset_roots()
    for raw in getenv_brand_str("SOURCE_PATH_ROOTS", "").split(os.pathsep):
        if raw.strip():
            roots.append(Path(raw.strip()))
    return roots


def confine_source_path(path: str | Path) -> Path:
    """Resolve a transfer ``source_path`` and refuse it outside the source roots.

    Symlinks and ``..`` are resolved before the check, and the check runs
    before any existence test, so a request cannot read or probe host files.
    The returned path may not exist; callers decide what a missing file means.
    """
    if not str(path or "").strip():
        raise ValueError("File source path is missing.")
    try:
        resolved = Path(path).resolve()
    except (OSError, RuntimeError) as exc:
        raise ValueError("File source path cannot be resolved.") from exc
    if not _inside_any(resolved, transfer_source_roots()):
        raise ValueError("File source path is outside the upload directory.")
    return resolved
