"""Debezium-compatible snapshot mode resolution.

Modes (aligned with Debezium PostgreSQL connector):
  - ``initial`` — snapshot when no watermark exists (default)
  - ``always`` — snapshot every job run, then stream
  - ``never`` — never snapshot; stream only (fails if no watermark)
  - ``initial_only`` — snapshot then stop (no stream poll)
  - ``when_needed`` — snapshot if slot/resume missing or broken
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class SnapshotMode(str, Enum):
    INITIAL = "initial"
    ALWAYS = "always"
    NEVER = "never"
    INITIAL_ONLY = "initial_only"
    WHEN_NEEDED = "when_needed"


def parse_snapshot_mode(raw: Any) -> SnapshotMode:
    text = str(raw or "initial").strip().lower().replace("-", "_")
    aliases = {
        "": SnapshotMode.INITIAL,
        "initial": SnapshotMode.INITIAL,
        "always": SnapshotMode.ALWAYS,
        "never": SnapshotMode.NEVER,
        "no_data": SnapshotMode.NEVER,
        "initial_only": SnapshotMode.INITIAL_ONLY,
        "initial_only_table": SnapshotMode.INITIAL_ONLY,
        "when_needed": SnapshotMode.WHEN_NEEDED,
        "whenneeded": SnapshotMode.WHEN_NEEDED,
    }
    if text not in aliases:
        raise ValueError(
            f"Unknown snapshot_mode '{raw}'. "
            f"Expected one of: {', '.join(m.value for m in SnapshotMode)}"
        )
    return aliases[text]


def should_run_snapshot(
    mode: SnapshotMode,
    *,
    watermark: str | None,
    resume_broken: bool = False,
) -> bool:
    if mode == SnapshotMode.ALWAYS:
        return True
    if mode == SnapshotMode.NEVER:
        if not watermark:
            raise ValueError(
                "snapshot_mode=never requires an existing CDC watermark/resume token"
            )
        return False
    if mode == SnapshotMode.INITIAL_ONLY:
        return True
    if mode == SnapshotMode.WHEN_NEEDED:
        return watermark is None or resume_broken
    # initial
    return watermark is None


def should_run_stream(mode: SnapshotMode) -> bool:
    return mode != SnapshotMode.INITIAL_ONLY


def resolve_snapshot_mode(
    stream_contracts: list[dict] | None,
    *,
    request_snapshot_mode: str = "",
    cfg_snapshot_mode: str = "",
) -> SnapshotMode:
    """Priority: stream contract → request → connector cfg → initial."""
    for raw in stream_contracts or []:
        if not raw.get("selected", True):
            continue
        if raw.get("snapshot_mode"):
            return parse_snapshot_mode(raw.get("snapshot_mode"))
    if request_snapshot_mode:
        return parse_snapshot_mode(request_snapshot_mode)
    if cfg_snapshot_mode:
        return parse_snapshot_mode(cfg_snapshot_mode)
    return SnapshotMode.INITIAL
