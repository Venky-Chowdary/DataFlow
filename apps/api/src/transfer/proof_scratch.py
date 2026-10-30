"""Run-scoped scratch space for the Gate-8 proof's disk-backed stores.

``df_pinmap_*.db`` / ``df_scope_*.db`` grow to hundreds of MB on large runs.
The wrapper around the proof bound a private directory for them, but only the
wrapper did: a store opened anywhere else (a direct call of the proof body, a
helper, a test) fell back to ``mkstemp`` in the SHARED temp dir and was removed
only by ``__del__`` — which never runs while an exception's traceback holds the
frame, and is not guaranteed at interpreter exit. Files leaked there were never
seen by the private-dir cleanup.

The rule now: a store's file always lives in a directory something owns.

* :func:`proof_scratch_scope` binds one private directory (reentrant: an inner
  scope reuses the outer). Every store opened inside it is closed and the
  directory removed when the scope ends, however it ends. The proof body
  enters it itself, so no caller can run it unscoped.
* :func:`scratch_file` outside any scope gives the store its own private
  directory; :func:`own_scratch` registers a ``weakref.finalize`` that closes
  the database and removes file and directory on ``close()``, on garbage
  collection, and at interpreter exit.
"""

from __future__ import annotations

import contextvars
import functools
import os
import shutil
import tempfile
import weakref
from contextlib import contextmanager
from typing import Any, Callable, Iterator, TypeVar

_PROOF_SCRATCH: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "df_proof_scratch", default=None
)
_PROOF_STORES: contextvars.ContextVar[list | None] = contextvars.ContextVar(
    "df_proof_stores", default=None
)

_F = TypeVar("_F", bound=Callable[..., Any])


@contextmanager
def proof_scratch_scope() -> Iterator[str]:
    """Bind a private scratch dir for this proof; reuse the outer one if bound."""
    active = _PROOF_SCRATCH.get()
    if active is not None:
        yield active
        return
    scratch = tempfile.mkdtemp(prefix="df_proof_")
    stores: list = []
    scratch_token = _PROOF_SCRATCH.set(scratch)
    stores_token = _PROOF_STORES.set(stores)
    try:
        yield scratch
    finally:
        _PROOF_STORES.reset(stores_token)
        _PROOF_SCRATCH.reset(scratch_token)
        for store in stores:
            try:
                store.close()
            except Exception:  # noqa: BLE001 - cleanup must not mask the result
                pass
        shutil.rmtree(scratch, ignore_errors=True)


def in_proof_scratch(fn: _F) -> _F:
    """Run ``fn`` inside :func:`proof_scratch_scope`."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with proof_scratch_scope():
            return fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def scratch_file(prefix: str, suffix: str) -> tuple[int, str, str | None]:
    """``(fd, path, owned_dir)`` — never a bare file in the shared temp dir.

    ``owned_dir`` is the private directory created for a store opened outside
    any scope; the store must remove it (see :func:`own_scratch`).
    """
    scope = _PROOF_SCRATCH.get()
    if scope is not None:
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=scope)
        return fd, path, None
    own = tempfile.mkdtemp(prefix="df_proof_")
    try:
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=own)
    except BaseException:
        shutil.rmtree(own, ignore_errors=True)
        raise
    return fd, path, own


def _release(holder: list, path: str, own_dir: str | None) -> None:
    db = holder[0] if holder else None
    if db is not None:
        try:
            db.close()
        except Exception:  # noqa: BLE001 - best-effort release
            pass
    try:
        os.unlink(path)
    except OSError:
        pass
    if own_dir:
        shutil.rmtree(own_dir, ignore_errors=True)


def own_scratch(store: Any, prefix: str, suffix: str) -> tuple[str, list]:
    """Give ``store`` a scratch file it owns until it closes or is collected.

    Returns ``(path, holder)``: put the opened database in ``holder[0]`` so the
    finalizer closes it before removing the file. ``store.close()`` should
    call ``store._scratch_release()``.
    """
    fd, path, own_dir = scratch_file(prefix, suffix)
    os.close(fd)
    holder: list = [None]
    store._scratch_release = weakref.finalize(store, _release, holder, path, own_dir)
    tracked = _PROOF_STORES.get()
    if tracked is not None:
        tracked.append(store)
    return path, holder
