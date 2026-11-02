"""Where control-plane metadata lives: MongoDB collections, or a JSON file.

Workspaces, memberships and user accounts are metadata, not transfer payload.
They persist in MongoDB when a real client is connected so several API instances
agree, and fall back to an atomically-replaced JSON file on a single-process
developer box. Both stores need the same two decisions — "is Mongo real?" and
"how do I write a file without a torn read?" — so they are owned here.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pymongo.database import Database

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platform
    fcntl = None  # type: ignore[assignment]

from pymongo.errors import PyMongoError

from services.mongodb_service import MongoDBService, get_mongodb_service
from services.value_serializer import json_default


def mongo_database() -> Database | None:
    """Return a live MongoDB database, or None when there is no real client.

    The in-memory stand-in reports itself as a service but keeps documents in a
    process dict, so a second API worker would not see them — metadata callers
    must treat that as "no Mongo" and use the file store instead.
    """
    try:
        svc = get_mongodb_service()
    except (PyMongoError, OSError, RuntimeError):
        return None
    if not isinstance(svc, MongoDBService) or svc.client is None:
        return None
    try:
        return svc.get_database()
    except (PyMongoError, OSError, RuntimeError):
        return None


def load_json_doc(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    """Read a JSON metadata file, returning ``default`` for absent/corrupt files."""
    if not path.exists():
        return dict(default)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    if not isinstance(raw, dict):
        return dict(default)
    return raw


def save_json_doc(path: Path, data: dict[str, Any]) -> None:
    """Write JSON metadata through a temp file so a reader never sees a partial write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")
    tmp.replace(path)


@contextmanager
def json_doc_transaction(path: Path, default: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Read-modify-write a JSON metadata file under an exclusive file lock.

    Two workers adding a member at the same time would otherwise each read the
    file, append their own row and write back — the second write silently
    dropping the first member. The lock serializes the whole cycle; the write
    itself is still the atomic temp-file replace above.

    ``flock`` is POSIX-only. On a platform without it the cycle still runs, so a
    single-process developer box keeps working, but concurrent writers are not
    serialized — MongoDB is the supported multi-worker backend.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with open(lock_path, "w", encoding="utf-8") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            data = load_json_doc(path, default)
            yield data
            save_json_doc(path, data)
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
