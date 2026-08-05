"""Policy for execute_tracked tests: prefer gates ON.

``skip_preflight=True`` teaches the wrong default. Use it only when a harness
cannot probe destinations (perf matrix, missing infra) and document why.
"""

from __future__ import annotations


def require_skip_reason(*, skip_preflight: bool, reason: str = "") -> bool:
    """Return skip_preflight after enforcing an explicit reason when True."""
    if skip_preflight and len((reason or "").strip()) < 8:
        raise ValueError(
            "skip_preflight=True requires reason (>=8 chars) — "
            "prefer False so G1–G9 run in CI"
        )
    return skip_preflight
