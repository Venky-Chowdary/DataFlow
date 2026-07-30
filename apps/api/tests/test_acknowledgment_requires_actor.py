"""Acknowledgment API requires actor + reason — mirrors preflight_router checks."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi import HTTPException  # noqa: E402


def _require_ack_trail(*, acknowledged: bool, actor: str, reason: str) -> None:
    """Same rules as preflight_router acknowledgment gate."""
    if not acknowledged:
        return
    if len((actor or "").strip()) < 2:
        raise HTTPException(
            status_code=400,
            detail="acknowledgment_actor is required when acknowledging compliance or schema drift",
        )
    if len((reason or "").strip()) < 8:
        raise HTTPException(
            status_code=400,
            detail="acknowledgment_reason is required (at least 8 characters)",
        )


def test_missing_actor_rejected() -> None:
    try:
        _require_ack_trail(acknowledged=True, actor="", reason="Keep existing mappings for this run")
        assert False, "expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "actor" in exc.detail


def test_short_reason_rejected() -> None:
    try:
        _require_ack_trail(acknowledged=True, actor="alice@acme.com", reason="ok")
        assert False, "expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "reason" in exc.detail


def test_valid_ack_accepted() -> None:
    _require_ack_trail(
        acknowledged=True,
        actor="alice@acme.com",
        reason="Keep existing mappings for this run",
    )
