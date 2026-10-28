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
from datetime import date, datetime, timezone

_GLOB = re.compile(r"[*?\[\]]")
_DATE_TOKEN = re.compile(r"\{(?:date|yyyy|yyyymmdd|yyyy-mm-dd)\}", re.I)
_FILE_DAY = re.compile(r"20\d{2}[-_]?\d{2}[-_]?\d{2}")


FILE_NOT_ARRIVED = "expected_file_not_arrived"
FILE_NOT_ARRIVED_REMEDIATION = (
    "Put the file where this schedule reads it, or widen file_wait_seconds, "
    "then run the schedule again."
)


class FileNotArrived(RuntimeError):
    """The pattern matched nothing. ``retryable`` is the wait window."""

    status = FILE_NOT_ARRIVED

    def __init__(self, message: str, *, retryable: bool, pattern: str) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.pattern = pattern
        self.status = FILE_NOT_ARRIVED
        self.remediation = FILE_NOT_ARRIVED_REMEDIATION


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
    action: str = "load"  # load | unchanged | wait | elapsed | missing
    message: str = ""
    retryable: bool = False
    status: str = ""


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


def ordered_files(files: list[RemoteFile]) -> list[RemoteFile]:
    """Stable pickup order: path, then mtime, then checksum."""
    return sorted(files, key=lambda item: (item.name, item.mtime, item.checksum))


def select_new_files(
    files: list[RemoteFile],
    manifest: list[dict] | None,
    pattern: str,
) -> list[RemoteFile]:
    """Files that match ``pattern`` and are new or changed by checksum.

    A touched mtime with the same sha256 is not a new file. A checksum with
    no digest falls back to size and mtime so an unhashed listing still skips
    a byte-identical object.
    """
    loaded_checksums: dict[str, set[str]] = {}
    loaded_stats: set[tuple[str, int, str]] = set()
    for row in manifest or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "loaded").strip().lower()
        if status not in {"", "loaded"}:
            continue
        name = str(row.get("name") or row.get("path") or "")
        checksum = str(row.get("checksum") or row.get("sha256") or "")
        if name and checksum:
            loaded_checksums.setdefault(name, set()).add(checksum)
        loaded_stats.add((name, int(row.get("size") or 0), str(row.get("mtime") or "")))
    chosen: list[RemoteFile] = []
    for item in files:
        if not matches_pattern(item.name, pattern):
            continue
        if item.checksum and item.checksum in loaded_checksums.get(item.name, set()):
            continue
        if not item.checksum and (item.name, int(item.size or 0), item.mtime) in loaded_stats:
            continue
        chosen.append(item)
    return ordered_files(chosen)


def _pickup_day(today: date | datetime | str | None) -> date:
    if isinstance(today, datetime):
        return today.date()
    if isinstance(today, date):
        return today
    if isinstance(today, str) and today.strip():
        return date.fromisoformat(today.strip()[:10])
    return datetime.now(timezone.utc).date()


def _series_is_dated(pattern: str, files: list[RemoteFile]) -> bool:
    if _DATE_TOKEN.search(pattern or ""):
        return True
    return any(_FILE_DAY.search(item.name or "") for item in files)


def dated_file_day(name: str) -> date | None:
    """Calendar day embedded in a file name, or None when the name is not dated."""
    match = _FILE_DAY.search(name or "")
    if not match:
        return None
    digits = re.sub(r"\D", "", match.group(0))
    if len(digits) != 8:
        return None
    try:
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    except ValueError:
        return None


def direct_dated_file_warning(name: str, today: date | datetime | str | None = None) -> str:
    """Warn when an explicitly chosen file is not today's file.

    A glob or schedule is not required. Today's own file does not warn.
    A name with no date does not warn.
    """
    found = dated_file_day(name)
    if found is None:
        return ""
    day = _pickup_day(today)
    if found == day:
        return ""
    return f"expected file for {day.isoformat()} not arrived"


def attach_direct_dated_file_warning(
    payload: dict,
    name: str,
    today: date | datetime | str | None = None,
) -> str:
    """Append the direct-key warning onto a preflight or job summary."""
    note = direct_dated_file_warning(name, today)
    if not note or not isinstance(payload, dict):
        return note
    warnings = list(payload.get("warnings") or [])
    texts = {
        str(item.get("message") if isinstance(item, dict) else item)
        for item in warnings
    }
    if note not in texts:
        warnings.append({"id": "expected_file_not_arrived", "message": note})
    payload["warnings"] = warnings
    return note


def _listing_has_day(files: list[RemoteFile], day: date) -> bool:
    iso = day.isoformat()
    compact = day.strftime("%Y%m%d")
    return any(iso in item.name or compact in item.name for item in files)


def plan_pickup(
    *,
    pattern: str,
    files: list[RemoteFile],
    manifest: list[dict] | None = None,
    wait_seconds: int = 0,
    waited_seconds: int = 0,
    today: date | datetime | str | None = None,
) -> PickupPlan:
    """Decide load vs wait vs a clear 'file not arrived' failure."""
    pat = (pattern or "").strip()
    patterned = is_file_pattern(pat)
    new_files = select_new_files(files, manifest, pat)
    matched = [item for item in files if matches_pattern(item.name, pat)]
    if new_files:
        day = _pickup_day(today)
        notice = ""
        if _series_is_dated(pat, matched or new_files) and not _listing_has_day(files, day):
            # The older file still loads. Today's absence is a warning on
            # that same run, not only on a later run that finds nothing new.
            notice = f"expected file for {day.isoformat()} not arrived"
        return PickupPlan(
            pattern=pat,
            is_pattern=patterned,
            files=ordered_files(new_files),
            action="load",
            message=notice,
        )
    if matched:
        day = _pickup_day(today)
        if _series_is_dated(pat, matched) and not _listing_has_day(files, day):
            return PickupPlan(
                pattern=pat,
                is_pattern=patterned,
                action="warn",
                status=FILE_NOT_ARRIVED,
                retryable=False,
                message=f"expected file for {day.isoformat()} not arrived",
            )
        return PickupPlan(
            pattern=pat,
            is_pattern=patterned,
            action="unchanged",
            message="no new or changed files",
        )
    wait = max(0, int(wait_seconds or 0))
    waited = max(0, int(waited_seconds or 0))
    label = pat or "(empty pattern)"
    if wait and waited < wait:
        return PickupPlan(
            pattern=pat,
            is_pattern=patterned,
            action="wait",
            status=FILE_NOT_ARRIVED,
            retryable=True,
            message=(
                f"expected file not arrived: {label} — no new file yet. "
                f"Waiting ({waited}s of {wait}s); the next attempt retries. "
                f"{FILE_NOT_ARRIVED_REMEDIATION}"
            ),
        )
    if wait and waited >= wait:
        return PickupPlan(
            pattern=pat,
            is_pattern=patterned,
            action="elapsed",
            status=FILE_NOT_ARRIVED,
            retryable=False,
            message=(
                f"expected file not arrived: {label} — wait window elapsed "
                f"({wait}s) with no new file. {FILE_NOT_ARRIVED_REMEDIATION}"
            ),
        )
    return PickupPlan(
        pattern=pat,
        is_pattern=patterned,
        action="missing",
        status=FILE_NOT_ARRIVED,
        retryable=False,
        message=(
            f"expected file not arrived: {label} — no matching file, "
            f"and no wait window is set. {FILE_NOT_ARRIVED_REMEDIATION}"
        ),
    )


def file_not_arrived(pattern: str, extra: dict | None = None) -> FileNotArrived:
    """The operator error for a pattern or path that matches no file yet.

    ``extra`` carries the source's wait window; the message and retryability
    are the pickup plan's own, so a peek and a load say the same thing.
    """
    extra = extra if isinstance(extra, dict) else {}
    plan = plan_pickup(
        pattern=pattern,
        files=[],
        manifest=None,
        wait_seconds=int(extra.get("file_wait_seconds") or 0),
        waited_seconds=int(extra.get("file_waited_seconds") or 0),
    )
    return FileNotArrived(plan.message, retryable=plan.retryable, pattern=pattern)


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
        from connectors.sftp_reader import list_file_stats

        directory = bucket or "/"
        rows = list_file_stats(cfg=cfg, directory=directory, pattern=pattern)
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
    checksum_changed: bool = False,
) -> bool:
    """A changed file under upsert is reprocessed whole, not watermark-filtered.

    The same digest is already short-circuited by the file ledger. A new
    checksum must MERGE every row: a corrected file often keeps older
    timestamps, and a row watermark would skip the correction.

    The first incremental load stays cursor-bounded so identity COPY still
    sees a mapped cursor. Only a checksum that differs from the ledger
    clears that bound.
    """
    if already_loaded or not has_key:
        return False
    policy = (duplicate_policy or "").strip().lower().replace("-", "_")
    mode = (sync_mode or "").strip().lower().replace("-", "_")
    if policy in {"append", "allow_duplicates"}:
        return False
    incremental = mode in {
        "incremental",
        "incremental_append",
        "incremental_deduped",
        "incremental_upsert",
    }
    if incremental and not checksum_changed:
        return False
    if checksum_changed and (
        incremental or policy in {"upsert", "merge", "dedupe_skip", "skip_duplicates"} or mode == "upsert"
    ):
        return True
    if policy in {"upsert", "merge", "dedupe_skip", "skip_duplicates"}:
        return True
    return mode in {"upsert"}
