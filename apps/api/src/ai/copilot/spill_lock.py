"""An exclusive lock around a Pilot spill file's read-merge-write (review 2, low).

Two API workers sharing one spill file each wrote only their own entries, so
the last writer won and the other worker's result, approval or consume was
gone. A writer now takes this lock, folds in what is on disk and writes the
union. Where ``fcntl`` is unavailable (Windows) the merge still runs, unlocked.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)


@contextmanager
def spill_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive advisory lock on ``<path>.lock`` for the block."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover - not POSIX
        yield
        return
    lock_path = Path(path).with_name(Path(path).name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_spill(path: Path) -> dict[str, Any]:
    """The spill file as written, or ``{}`` when it is missing or unreadable."""
    try:
        if not Path(path).exists():
            return {}
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        _log.warning("pilot spill %s not read for merge: %s", Path(path).name, exc)
        return {}
    return raw if isinstance(raw, dict) else {}
