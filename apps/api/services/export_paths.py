"""Single owner of where a file export may land on the API host.

Every export a client can name (``/query/export``, file-export destinations in
Transfer Studio) is confined to one folder: ``apps/api/exports``, or the folder an
administrator names in ``DATAFLOW_EXPORTS_DIR`` (``DATAWRAP_EXPORTS_DIR``). A
client-supplied ``output_path`` is a name under that folder, never a path into
the application tree: it is resolved (``..`` and symlinks followed), compared
structurally with ``os.path.commonpath`` — not a string prefix, which lets
``exports_evil/`` pass — and must carry an export-format extension, so it can
never overwrite a module, config file or store.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath, PureWindowsPath

from services.brand_env import getenv_brand

EXPORTS_DIRNAME = "exports"

#: Extensions an export writer produces. Anything else (``.py``, ``.cfg``,
#: ``.sqlite``, ``.pth`` …) is refused even inside the exports folder.
EXPORT_EXTENSIONS: frozenset[str] = frozenset(
    {
        "csv",
        "tsv",
        "json",
        "jsonl",
        "ndjson",
        "yaml",
        "yml",
        "fwf",
        "txt",
        "xlsx",
        "parquet",
        "avro",
        "orc",
        "xml",
        # A SQLite export is a database file written into the exports folder
        # (``file_export``/``sqlite`` routes to ``database``/``sqlite``, round 11).
        "sqlite",
        "sqlite3",
        "db",
    }
)


class ExportPathError(ValueError):
    """A client-supplied export path that would leave the exports folder."""


def api_root() -> Path:
    return Path(__file__).resolve().parents[1]


def exports_dir() -> Path:
    """The folder exports are confined to: ``DATAFLOW_EXPORTS_DIR``, else ``apps/api/exports``."""
    configured = (getenv_brand("EXPORTS_DIR") or "").strip()
    path = Path(configured).expanduser() if configured else api_root() / EXPORTS_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def _is_within(base: Path, candidate: Path) -> bool:
    b = os.path.normcase(str(base))
    c = os.path.normcase(str(candidate))
    try:
        return os.path.commonpath([b, c]) == b
    except ValueError:  # different drives on Windows
        return False


def _relative_parts(raw: str) -> tuple[str, ...]:
    """Path components of ``raw``, splitting on both ``/`` and ``\\``."""
    return tuple(p for p in PureWindowsPath(raw).parts if p not in ("", "."))


def resolve_export_path(output_path: str, *, default_ext: str) -> Path:
    """Absolute, confined path for a client-named export file.

    ``output_path`` may be a bare file name (``orders.csv``), a name under a
    sub-folder of the exports folder (``nightly/orders.csv``,
    ``exports/nightly/orders.csv``) or an absolute path that already lies
    inside the exports folder. A name without an extension gets
    ``default_ext``. Raises :class:`ExportPathError` for anything else.
    """
    raw = str(output_path or "").strip()
    if not raw:
        raise ExportPathError("Export file name is empty")
    if "\x00" in raw:
        raise ExportPathError("Export path contains a NUL byte")

    base = exports_dir()
    if os.path.isabs(raw) or PureWindowsPath(raw).is_absolute() or PurePosixPath(raw).is_absolute():
        candidate = Path(raw)
    else:
        parts = _relative_parts(raw)
        if parts and parts[0] == EXPORTS_DIRNAME:
            parts = parts[1:]
        if not parts:
            raise ExportPathError("Export path must name a file inside the exports folder")
        candidate = base.joinpath(*parts)

    if not candidate.suffix:
        ext = str(default_ext or "").strip().lower().lstrip(".")
        candidate = candidate.with_name(f"{candidate.name}.{ext}")

    resolved = candidate.resolve()
    if resolved == base or not _is_within(base, resolved):
        raise ExportPathError(
            "Export path must stay inside the server exports folder "
            f"({EXPORTS_DIRNAME}/); use a file name such as 'orders.csv'"
        )
    ext = resolved.suffix.lower().lstrip(".")
    if ext not in EXPORT_EXTENSIONS:
        raise ExportPathError(
            f"Export file extension '.{ext}' is not an export format; "
            f"allowed: {', '.join(sorted(EXPORT_EXTENSIONS))}"
        )
    if resolved.is_dir():
        raise ExportPathError("Export path names a folder, not a file")
    return resolved


def generated_export_path(name: str) -> Path:
    """Path for a server-generated export name (still confinement-checked)."""
    if os.path.basename(name) != name or name in ("", ".", ".."):
        raise ExportPathError("Generated export name must be a bare file name")
    return resolve_export_path(name, default_ext="")
