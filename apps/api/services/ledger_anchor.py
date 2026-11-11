"""Anchoring of Evidence Ledger checkpoints in the write-once store.

Placeholder until the anchoring piece lands: a checkpoint is recorded with no
anchor, which every reader reports as "not anchored".
"""

from __future__ import annotations

from typing import Any


def anchor_checkpoint(record: dict[str, Any]) -> dict[str, Any]:
    return record
