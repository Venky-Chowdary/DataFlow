"""Restricted pickle loading for model / index artifacts.

``pickle.load`` executes whatever the payload names, so a tampered or
attacker-supplied ``.pkl`` on the API image is remote code execution inside the
transfer engine. Every artifact we load is produced by us, which means we can
afford the two controls a data platform is expected to have:

* **Class allowlist** — only the module prefixes an artifact legitimately needs
  can be resolved; anything else (``os``, ``subprocess``, ``builtins.eval``)
  raises before it is constructed.
* **Digest pinning** — an optional sidecar ``<artifact>.sha256`` (or an env
  override) must match, so a swapped file fails closed instead of loading.

Neither control is a substitute for not shipping pickles at all; they bound the
blast radius until model artifacts move to a non-executable format.
"""

from __future__ import annotations

import hashlib
import io
import logging
import pickle  # nosec B403 - loaded only through RestrictedUnpickler below
from pathlib import Path

logger = logging.getLogger(__name__)

# Only these builtins are ever needed to rebuild a container/scalar graph.
_SAFE_BUILTINS = frozenset(
    {
        "bool",
        "bytearray",
        "bytes",
        "complex",
        "dict",
        "float",
        "frozenset",
        "int",
        "list",
        "set",
        "str",
        "tuple",
    }
)


class UnsafePickleError(RuntimeError):
    """Raised when an artifact fails its allowlist or digest check."""


class RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that resolves only allowlisted modules/classes."""

    def __init__(self, file: io.BufferedIOBase, allowed_modules: frozenset[str]):
        super().__init__(file)
        self._allowed = allowed_modules

    def find_class(self, module: str, name: str):  # noqa: D102 - pickle protocol
        if module == "builtins":
            if name in _SAFE_BUILTINS:
                return super().find_class(module, name)
            raise UnsafePickleError(f"blocked builtin in pickle: {name}")
        root = module.split(".", 1)[0]
        if root in self._allowed or module in self._allowed:
            return super().find_class(module, name)
        raise UnsafePickleError(f"blocked module in pickle: {module}.{name}")


def expected_digest(path: Path) -> str:
    """Pinned SHA-256 for ``path`` from its ``.sha256`` sidecar, else ``''``."""
    sidecar = path.with_suffix(path.suffix + ".sha256")
    if not sidecar.is_file():
        return ""
    return sidecar.read_text(encoding="utf-8").strip().split()[0].lower()


def file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_restricted(
    path: Path,
    *,
    allowed_modules: frozenset[str] | set[str],
    require_digest: bool = False,
):
    """Load ``path`` under the allowlist, verifying a pinned digest if present.

    ``require_digest=True`` refuses artifacts that carry no pinned digest at all
    (use for artifacts shipped in the image; locally generated caches do not
    need one because they never leave the host).
    """
    pinned = expected_digest(path)
    if pinned:
        actual = file_digest(path)
        if actual != pinned:
            raise UnsafePickleError(
                f"digest mismatch for {path.name}: expected {pinned}, got {actual}"
            )
    elif require_digest:
        raise UnsafePickleError(
            f"{path.name} has no pinned {path.name}.sha256 digest — refusing to load"
        )
    with path.open("rb") as f:
        return RestrictedUnpickler(f, frozenset(allowed_modules)).load()
