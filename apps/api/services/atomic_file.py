"""Atomic file writes: a sibling temp file, then ``os.replace``."""

from __future__ import annotations

import contextlib
import errno
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


try:  # POSIX only; F_FULLFSYNC exists on macOS
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - Windows
    _fcntl = None  # type: ignore[assignment]

#: Directory fsync failures that only mean "not supported here".
_UNSUPPORTED_DIR_FSYNC = {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EBADF}


def _flush_to_disk(fd: int) -> None:
    """fsync, or F_FULLFSYNC where the platform has it (macOS: fsync alone
    only reaches the drive's cache)."""
    full = getattr(_fcntl, "F_FULLFSYNC", None) if _fcntl is not None else None
    if full is not None:
        try:
            _fcntl.fcntl(fd, full)
            return
        except OSError:
            pass  # filesystem without F_FULLFSYNC: plain fsync below
    os.fsync(fd)


def fsync_directory(directory: Path) -> None:
    """Make renames in ``directory`` durable (POSIX); a no-op where unsupported.

    Without it a power loss can lose the rename itself, or keep it and lose
    the data it pointed at. Platforms without ``O_DIRECTORY`` (Windows) and
    filesystems that refuse a directory fsync (EINVAL/ENOTSUP) are skipped; an
    I/O error is logged as a warning (the data may not be durable).
    """
    flag = getattr(os, "O_DIRECTORY", None)
    if flag is None:
        return
    try:
        fd = os.open(directory, os.O_RDONLY | flag)
    except OSError:
        return
    try:
        _flush_to_disk(fd)
    except OSError as exc:
        if exc.errno in _UNSUPPORTED_DIR_FSYNC:
            logger.debug("Directory fsync not supported for %s: %s", directory, exc)
        else:
            logger.warning("Directory fsync failed for %s (%s): recent renames may not be durable", directory, exc)
    finally:
        os.close(fd)


def write_json_atomic(
    path: Path,
    data: dict[str, Any],
    *,
    indent: int = 2,
    default: Any | None = json_default,
    sync_directory: bool = True,
) -> None:
    """Write ``data`` to ``path`` atomically and durably.

    A sibling temp file is written and fsynced, then ``os.replace``d over
    ``path``, then the directory is fsynced: after a crash or power loss the
    file is the old complete one or the new complete one, never empty or half.

    Only the rename holds the process-wide ``_WRITE_LOCK``; the serialization
    and both fsyncs run outside it, so one slow flush does not stall every
    other store. That keeps the old guarantee: concurrent writers of one path
    each rename a complete file, and which lands last was never ordered by
    callers anyway (callers that need an order hold their own lock, as the plan
    store does). ``sync_directory=False`` lets a caller that rewrites many
    files in one directory call :func:`fsync_directory` once at the end.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
            json.dump(data, tmp, indent=indent, default=default)
            tmp.flush()
            _flush_to_disk(tmp.fileno())
        with _WRITE_LOCK:
            os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise
    if sync_directory:
        fsync_directory(path.parent)


_PATH_LOCKS: dict[str, threading.RLock] = {}
_PATH_LOCKS_GUARD = threading.Lock()


def path_update_lock(path: Path) -> threading.RLock:
    """The process-wide re-entrant lock for one JSON store path."""
    key = os.path.abspath(path)
    with _PATH_LOCKS_GUARD:
        lock = _PATH_LOCKS.get(key)
        if lock is None:
            lock = _PATH_LOCKS[key] = threading.RLock()
        return lock


@contextlib.contextmanager
def locked_update(path: Path) -> Iterator[None]:
    """Hold ``path``'s lock across a load-modify-``write_json_atomic`` of it.

    ``write_json_atomic`` makes each write whole, not a read-modify-write
    sequence: two threads that both load, change and write keep only the
    last writer's change. Stores without a lock of their own wrap the
    sequence in this (in-process; cross-process callers need a file lock,
    as the plan store has).
    """
    with path_update_lock(path):
        yield


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
