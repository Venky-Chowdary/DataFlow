"""Named data-path failures that carry a stable machine ``code``.

The job failure path merges ``failure_details`` into the job's error details
(``src.transfer.job_failure._job_failure_fields``), so the code reaches the API
and the UI without a rule-table entry. The message is plain words: what failed,
which object, why, and the one next action.
"""

from __future__ import annotations

from typing import Any


class DataPathError(RuntimeError):
    """A data-correctness refusal or failure with a stable ``code``."""

    code = "DATA_PATH_ERROR"

    def __init__(self, message: str, *, code: str | None = None, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = str(code or type(self).code)
        self.failure_details: dict[str, Any] = {"code": self.code, **(details or {})}


class CdcDeletesUnsupported(DataPathError):
    code = "CDC_DEST_DELETES_UNSUPPORTED"


class CdcBatchPartiallyApplied(DataPathError):
    code = "CDC_BATCH_PARTIALLY_APPLIED"


class CdcSourceTruncated(DataPathError):
    code = "CDC_SOURCE_TRUNCATED"


class CdcDecodeFailed(DataPathError):
    code = "CDC_DECODE_FAILED"


def key_list_text(keys: list[Any], limit: int = 10) -> str:
    """``1, 2, 3 and 7 more`` — a bounded list of keys for a message."""
    shown = [str(k) for k in list(keys)[:limit]]
    more = len(keys) - len(shown)
    return ", ".join(shown) + (f" and {more} more" if more > 0 else "")


__all__ = [
    "CdcBatchPartiallyApplied",
    "CdcDecodeFailed",
    "CdcDeletesUnsupported",
    "CdcSourceTruncated",
    "DataPathError",
    "key_list_text",
]
