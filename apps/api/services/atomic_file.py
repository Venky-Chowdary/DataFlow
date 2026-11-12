"""Atomic file writes: a sibling temp file, then ``os.replace``."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import secrets
import stat
import tempfile
import threading
import time
from pathlib import Path
from typing import IO, Any, Iterator

from services.value_serializer import json_default

_WRITE_LOCK = threading.Lock()
logger = logging.getLogger(__name__)

#: ``atomic_text_file`` temp names: ``.<target name>.<random>.tmp``.
TEMP_SUFFIX = ".tmp"
#: A temp file untouched this long has no live writer (a writer's file grows
#: as it writes); ``sweep_stale_temp_files`` removes it.
STALE_TEMP_SECONDS = 60.0


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


def _open_sibling_temp(path: str) -> tuple[int, str]:
    """Create ``.<name>.<random>.tmp`` beside ``path``.

    Created like ``open()`` would (0o666 less the umask), not 0600 as
    ``mkstemp`` does; an existing target's own mode is copied onto it.
    """
    directory = os.path.dirname(path) or "."
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(100):
        tmp = os.path.join(directory, f".{os.path.basename(path)}.{secrets.token_hex(4)}{TEMP_SUFFIX}")
        try:
            fd = os.open(tmp, flags, 0o666)
        except FileExistsError:
            continue
        try:
            os.chmod(tmp, stat.S_IMODE(os.stat(path).st_mode))
        except FileNotFoundError:
            pass  # new file: keep the umask-derived mode
        except BaseException:
            os.close(fd)
            os.unlink(tmp)
            raise
        return fd, tmp
    raise FileExistsError(f"no free temp name beside {path}")


@contextlib.contextmanager
def atomic_text_file(path: str) -> Iterator[IO[str]]:
    """Write ``path`` via a sibling temp file + fsync + rename.

    Readers (and the next run) see the previous complete file or the new
    complete file, never half. The file keeps the target's mode, or gets the
    mode ``open()`` would give a new file.
    """
    fd, tmp = _open_sibling_temp(path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def sweep_stale_temp_files(directory: str, older_than_seconds: float = STALE_TEMP_SECONDS) -> list[str]:
    """Remove ``atomic_text_file`` temp files a killed writer left in ``directory``.

    Only dot-named ``*.tmp`` files untouched for ``older_than_seconds``; a
    live writer's file is younger. Returns the names removed.
    """
    removed: list[str] = []
    cutoff = time.time() - older_than_seconds
    try:
        entries = list(os.scandir(directory))
    except FileNotFoundError:
        return removed
    for entry in entries:
        if not (entry.name.startswith(".") and entry.name.endswith(TEMP_SUFFIX)):
            continue
        try:
            if entry.is_file(follow_symlinks=False) and entry.stat(follow_symlinks=False).st_mtime < cutoff:
                os.unlink(entry.path)
                removed.append(entry.name)
        except OSError as exc:  # raced with another sweeper or a permission issue
            logger.debug("Could not sweep %s: %s", entry.path, exc)
    if removed:
        logger.info("Removed %d stale temp file(s) in %s", len(removed), directory)
    return removed
