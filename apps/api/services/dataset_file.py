"""Locate an uploaded dataset file and refuse anything outside that tree.

Pilot names a dataset. The bytes live in an upload directory. A confirm ack
must not be able to point the engine at an arbitrary path.
"""

from __future__ import annotations

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
    if not resolved.is_file():
        raise ValueError("Uploaded file is not on disk.")
    if is_transfer_staging_file(resolved.name):
        raise ValueError("That file belongs to one transfer, not to the dataset catalog.")
    for root in dataset_roots():
        try:
            root_resolved = root.resolve()
        except OSError:
            continue
        if resolved == root_resolved or root_resolved in resolved.parents:
            return resolved
    raise ValueError("That file is outside the upload directory.")
