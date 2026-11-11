"""Anchoring of Evidence Ledger checkpoints in the write-once store.

Placeholder until the anchoring piece lands: a checkpoint is recorded with no
anchor, and every reader reports it as "not anchored".
"""

from __future__ import annotations

from typing import Any

NOT_ANCHORED = "Not anchored: this checkpoint has no write-once anchor yet."


def anchor_checkpoint(record: dict[str, Any]) -> dict[str, Any]:
    return record


def verify_checkpoint(checkpoint: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    return {"status": "not_checked", "detail": NOT_ANCHORED}, None
