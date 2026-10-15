"""Scheduled file pickup — glob/prefix, manifest, wait-for-file.

A schedule names ``drop/*.csv`` or ``drop/`` before any object exists.
Each run lists the prefix, keeps files whose (name, size, checksum, mtime)
is not already in the manifest, and either loads those files or fails with
``file not arrived``. A wait window makes that failure retryable until the
window elapses.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field

_GLOB = re.compile(r"[*?\[\]]")
_DATE_TOKEN = re.compile(r"\{(?:date|yyyy|yyyymmdd|yyyy-mm-dd)\}", re.I)


class FileNotArrived(RuntimeError):
    """The pattern matched nothing. ``retryable`` is the wait window."""

    def __init__(self, message: str, *, retryable: bool, pattern: str) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.pattern = pattern


@dataclass(frozen=True)
class RemoteFile:
    name: str
    size: int = 0
    checksum: str = ""
    mtime: str = ""

    def identity(self) -> tuple[str, int, str, str]:
        return (self.name, int(self.size or 0), str(self.checksum or ""), str(self.mtime or ""))


@dataclass
class PickupPlan:
    pattern: str
    is_pattern: bool
    files: list[RemoteFile] = field(default_factory=list)
    action: str = "load"  # load | wait | elapsed | missing
    message: str = ""
    retryable: bool = False


def is_file_pattern(name: str) -> bool:
    """True for a glob, a directory prefix, or a dated name token."""
    text = (name or "").strip()
    if not text:
        return False
    if text.endswith("/"):
        return True
    if _GLOB.search(text):
        return True
    if _DATE_TOKEN.search(text):
        return True
    return False


def pattern_prefix(pattern: str) -> str:
    """Longest literal prefix a listing can use."""
    text = (pattern or "").strip()
    cut = len(text)
    for i, ch in enumerate(text):
        if ch in "*?[":
            cut = i
            break
        if text[i:].lower().startswith("{date}") or text[i:].lower().startswith("{yyyy"):
            cut = i
            break
    prefix = text[:cut]
    slash = prefix.rfind("/")
    if slash >= 0 and cut < len(text):
        return prefix[: slash + 1]
    return prefix


def matches_pattern(name: str, pattern: str) -> bool:
    key = (name or "").strip().lstrip("/")
    pat = (pattern or "").strip().lstrip("/")
    if not key or not pat:
        return False
    if pat.endswith("/"):
        return key.startswith(pat) and not key.endswith("/")
    if _DATE_TOKEN.search(pat) or _GLOB.search(pat):
        glob = _DATE_TOKEN.sub("*", pat)
        if fnmatch.fnmatch(key, glob):
            return True
        # SFTP listings are basenames. ``drop/*.csv`` still matches ``a.csv``
        # when the directory itself was the prefix that was listed.
        name_base = key.rsplit("/", 1)[-1]
        pat_base = glob.rsplit("/", 1)[-1]
        name_dir = key.rsplit("/", 1)[0] if "/" in key else ""
        pat_dir = glob.rsplit("/", 1)[0] if "/" in glob else ""
        if fnmatch.fnmatch(name_base, pat_base) and (not name_dir or name_dir == pat_dir):
            return True
        return False
    return key == pat


def select_new_files(
    files: list[RemoteFile],
    manifest: list[dict] | None,
    pattern: str,
) -> list[RemoteFile]:
    """Files that match ``pattern`` and are not already recorded."""
    seen = {
        (
            str(row.get("name") or ""),
            int(row.get("size") or 0),
            str(row.get("checksum") or ""),
            str(row.get("mtime") or ""),
        )
        for row in (manifest or [])
        if isinstance(row, dict)
    }
    chosen: list[RemoteFile] = []
    for item in files:
        if not matches_pattern(item.name, pattern):
            continue
        if item.identity() in seen:
            continue
        chosen.append(item)
    return chosen


def plan_pickup(
    *,
    pattern: str,
    files: list[RemoteFile],
    manifest: list[dict] | None = None,
    wait_seconds: int = 0,
    waited_seconds: int = 0,
) -> PickupPlan:
    """Decide load vs wait vs a clear 'file not arrived' failure."""
    pat = (pattern or "").strip()
    patterned = is_file_pattern(pat)
    new_files = select_new_files(files, manifest, pat)
    if new_files:
        return PickupPlan(pattern=pat, is_pattern=patterned, files=new_files, action="load")
    wait = max(0, int(wait_seconds or 0))
    waited = max(0, int(waited_seconds or 0))
    label = pat or "(empty pattern)"
    if wait and waited < wait:
        return PickupPlan(
            pattern=pat,
            is_pattern=patterned,
            action="wait",
            retryable=True,
            message=(
                f"file not arrived: {label} — no new file yet. "
                f"Waiting ({waited}s of {wait}s); the next attempt retries."
            ),
        )
    if wait and waited >= wait:
        return PickupPlan(
            pattern=pat,
            is_pattern=patterned,
            action="elapsed",
            retryable=False,
            message=(
                f"file not arrived: {label} — wait window elapsed "
                f"({wait}s) with no new file."
            ),
        )
    return PickupPlan(
        pattern=pat,
        is_pattern=patterned,
        action="missing",
        retryable=False,
        message=f"file not arrived: {label} — no matching file, and no wait window is set.",
    )


def list_remote_files(src_type: str, cfg: dict, bucket: str, pattern: str) -> list[RemoteFile]:
    """List objects that match a glob, prefix, or exact key."""
    kind = (src_type or "").strip().lower()
    prefix = pattern_prefix(pattern) if is_file_pattern(pattern) else pattern
    rows: list[dict] = []
    if kind == "s3":
        from connectors.s3_reader import list_object_stats

        rows = list_object_stats(cfg, bucket, prefix=prefix)
    elif kind == "gcs":
        from connectors.gcs_reader import list_objects

        rows = [{"name": key} for key in list_objects(cfg, bucket, prefix=prefix)]
    elif kind == "adls":
        from connectors.adls_reader import list_objects

        rows = [{"name": key} for key in list_objects(cfg, bucket, prefix=prefix)]
    elif kind == "sftp":
        from connectors.sftp_reader import list_files

        directory = bucket or "/"
        rows = [{"name": name} for name in list_files(cfg=cfg, directory=directory)]
    else:
        return []
    files = [
        RemoteFile(
            name=str(row.get("name") or ""),
            size=int(row.get("size") or 0),
            checksum=str(row.get("checksum") or ""),
            mtime=str(row.get("mtime") or ""),
        )
        for row in rows
        if str(row.get("name") or "")
    ]
    if is_file_pattern(pattern):
        return [item for item in files if matches_pattern(item.name, pattern)]
    exact = [item for item in files if item.name == pattern or item.name.endswith("/" + pattern)]
    return exact


def file_upsert_rereads(
    *,
    sync_mode: str,
    duplicate_policy: str = "",
    already_loaded: bool = False,
    has_key: bool = False,
) -> bool:
    """A changed file under upsert is reprocessed whole, not watermark-filtered.

    The same digest is already short-circuited by the file ledger. A new
    checksum must MERGE every row: a corrected file often keeps older
    timestamps, and a row watermark would skip the correction.
    """
    if already_loaded or not has_key:
        return False
    policy = (duplicate_policy or "").strip().lower().replace("-", "_")
    mode = (sync_mode or "").strip().lower().replace("-", "_")
    if policy in {"append", "allow_duplicates"}:
        return False
    if policy in {"upsert", "merge", "dedupe_skip", "skip_duplicates"}:
        return True
    return mode in {"upsert", "incremental_deduped", "incremental_upsert"}
