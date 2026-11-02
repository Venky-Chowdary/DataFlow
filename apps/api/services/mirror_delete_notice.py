"""What a mirror run does with destination rows whose key left the source.

Owner decision (round 8): soft delete is the default (``_deleted`` flag,
``_deleted_at`` stamp, the physical row kept). A hard delete stays available
as an explicit ``delete_mode="hard"`` on the stream contract (Studio's Mirror
setting, the MCP ``delete_mode`` argument, a schedule's stored contract) and
is announced at plan / preflight time, before anything is deleted.

The sentences below are the product's single wording. The web app shows the
same text (``apps/web/src/lib/mirrorDeleteMode.ts``); a test keeps the two
identical so the warning, the ledger and the UI never disagree.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

MIRROR_SOFT_DELETE_TEXT = (
    "Soft delete (default): destination rows whose key is missing from the "
    "source are kept and flagged _deleted (with _deleted_at). Physical "
    "COUNT(*) stays; the active population is COUNT(*) WHERE NOT _deleted."
)
MIRROR_HARD_DELETE_WARNING = (
    "Hard delete declared: destination rows whose key is missing from the "
    "source will be physically deleted (DELETE). They cannot be recovered "
    "from the destination. Remove delete_mode=hard to keep them as _deleted "
    "rows instead."
)
#: What the finished run's ledger says happened to the missing keys.
MIRROR_LEDGER_HARD_TEXT = (
    "Keys missing from the source were physically deleted (hard delete declared)."
)
MIRROR_LEDGER_SOFT_TEXT = (
    "Keys missing from the source stay as _deleted rows; physical COUNT(*) does not drop."
)
WARNING_ID = "mirror_hard_delete"


def _mode_of(contract: Mapping[str, Any]) -> str:
    from services.mirror_engine import resolve_mirror_delete_mode

    raw = contract.get("delete_mode") or contract.get("delete_strategy") or ""
    return resolve_mirror_delete_mode(None, {"delete_mode": raw})


def mirror_delete_notice(
    sync_mode: str,
    stream_contracts: Iterable[Any] | None,
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """``{"mode", "message", "physical_delete", "streams"}`` for a mirror run, else None."""
    from services.sync_cursor import normalize_sync_mode

    contracts = [
        c
        for c in (stream_contracts or [])
        if isinstance(c, Mapping) and c.get("selected", True)
    ]
    modes = {
        normalize_sync_mode(str(c.get("sync_mode") or ""), default="") for c in contracts
    }
    run_mode = normalize_sync_mode(str(sync_mode or ""), default="")
    if run_mode != "mirror" and "mirror" not in modes:
        return None
    mirror_contracts = [
        c
        for c in contracts
        if normalize_sync_mode(str(c.get("sync_mode") or run_mode), default="") == "mirror"
    ]
    fallback = str((options or {}).get("delete_mode") or "").strip().lower()
    hard = [
        str(c.get("name") or "")
        for c in mirror_contracts
        if (_mode_of(c) if (c.get("delete_mode") or c.get("delete_strategy")) else fallback) == "hard"
    ]
    if not mirror_contracts and fallback == "hard":
        hard = [""]
    if hard:
        return {
            "mode": "hard",
            "physical_delete": True,
            "message": MIRROR_HARD_DELETE_WARNING,
            "streams": [n for n in hard if n],
        }
    return {"mode": "soft", "physical_delete": False, "message": MIRROR_SOFT_DELETE_TEXT, "streams": []}


def attach_mirror_delete_notice(
    result: dict[str, Any],
    sync_mode: str,
    stream_contracts: Iterable[Any] | None,
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Stamp ``mirror_delete`` on a preflight result; a hard delete is a warning."""
    notice = mirror_delete_notice(sync_mode, stream_contracts, options)
    if not notice:
        return result
    result["mirror_delete"] = notice
    if notice["physical_delete"]:
        bucket = result.setdefault("warnings", [])
        if isinstance(bucket, list) and not any(
            isinstance(w, Mapping) and w.get("id") == WARNING_ID for w in bucket
        ):
            bucket.insert(
                0,
                {
                    "id": WARNING_ID,
                    "severity": "danger",
                    "message": MIRROR_HARD_DELETE_WARNING,
                    "details": {"delete_mode": "hard", "streams": notice["streams"]},
                },
            )
    return result


def declare_contract_delete_mode(
    contracts: list[dict[str, Any]] | None, sync_mode: str, delete_mode: str
) -> tuple[list[dict[str, Any]], str]:
    """Put an explicit ``delete_mode`` on the run's stream contracts (MCP route).

    Returns ``(contracts, error)``. Only ``soft`` and ``hard`` exist, and only a
    mirror run deletes anything.
    """
    out = [dict(c) for c in contracts or []]
    declared = str(delete_mode or "").strip().lower()
    if not declared:
        return out, ""
    if declared not in {"soft", "hard"}:
        return out, f"delete_mode {delete_mode!r} is not one of 'soft' or 'hard'."
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(str(sync_mode or ""), default="") != "mirror":
        return out, "delete_mode applies to mirror sync only."
    if not out:
        return out, "A mirror run needs a primary key before a delete_mode can apply."
    for contract in out:
        contract["delete_mode"] = declared
    return out, ""
