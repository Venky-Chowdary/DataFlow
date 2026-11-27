"""Q2 (train review): a key change is refused by a current stamp.

PR #435's interim digest strips key flags with the scores, so for a keyed
decision it equalled the current hash of the same rows without the key: stamp
a plan with no key, mark ``id`` is_primary_key, Execute with only the stamp,
and it was accepted. The interim digest is now accepted only for a decision
with no key-flagged row.
"""

from __future__ import annotations

import pytest

from services.decision_kernel.execute_gate import (
    build_artifact_from_mappings,
    enforce_decision_artifact,
)

_ROWS = [
    {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer",
     "confidence": 0.9, "create_new": True},
    {"source": "code", "target": "code", "source_type": "text", "target_type": "text",
     "confidence": 0.9, "create_new": True},
]


def _keyed(*names: str) -> list[dict]:
    return [{**row, "is_primary_key": row["source"] in names} for row in _ROWS]


def _stamp(maps: list[dict]) -> str:
    return build_artifact_from_mappings(
        maps, dest_db="postgresql", source_db="postgresql", source_fingerprint="fp",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:postgresql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    ).content_hash


def _enforce(maps: list[dict], stamp: str):
    return enforce_decision_artifact(
        mappings=maps, dest_db="postgresql", source_db="postgresql",
        approved_content_hash=stamp, source_fingerprint="fp", dest_fingerprint="",
        destination_table_exists=False, sync_mode="incremental_append",
    )


@pytest.mark.parametrize("stamped, run", [
    ((), ("id",)),           # a key added after Validate
    (("id",), ()),           # a key removed
    (("id",), ("code",)),    # the key moved to another column
    (("id",), ("id", "code")),
])
def test_a_current_stamp_refuses_any_key_change(stamped, run):
    stamp = _stamp(_keyed(*stamped))
    err, art = _enforce(_keyed(*run), stamp)
    assert err and art is None, (stamped, run)
    same, ok = _enforce(_keyed(*stamped), stamp)
    assert same is None and ok is not None
