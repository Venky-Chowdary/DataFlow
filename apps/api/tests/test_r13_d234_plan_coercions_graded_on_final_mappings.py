"""D234 — a plan never shows a block-severity coercion beside safe_to_start=true.

The MCP plan graded coercions in the mapping pipeline, then applied column
transforms, the declared source zone and the contracts it signed, and ran
preflight on that final set. Coercions stayed graded on the first draft:
``ts TIMESTAMPTZ -> DATE`` lossy/block beside an identical ``tstz`` graded
preserve, and ``safe_to_start`` true. 23b65cbbc re-graded only for signed
contracts. Now the plan re-grades its final mapping set with the same
validator, and an uncleared block keeps ``safe_to_start`` false.
"""

from __future__ import annotations

from src.ai.copilot.transfer_tools import (
    _regrade_signed_coercions,
    _uncleared_coercion_blocks,
)

_STALE = {
    "coercion_issues": [{
        "source": "ts", "target": "ts", "source_type": "TIMESTAMP",
        "target_type": "date", "lossy": True, "severity": "block",
        "message": "ts (TIMESTAMP) → ts (date)",
    }],
    "quality_issues": ["ts (TIMESTAMP) → ts (date)"],
}


def _row(src, tgt, s_type, t_type, transform="none", **extra):
    return {"source": src, "target": tgt, "source_type": s_type, "target_type": t_type,
            "transform": transform, "create_new": True,
            "assignment_strategy": "identity_passthrough", **extra}


def test_zone_declared_on_the_final_mapping_clears_the_stale_block():
    final = [_row("ts", "ts", "TIMESTAMP", "date", "assume_timezone:UTC"),
             _row("tstz", "tstz", "TIMESTAMPTZ(3)", "date")]
    out = _regrade_signed_coercions(
        _STALE, final, dest_db_type="mongodb", dest_exists=False,
        samples={"ts": ["2024-01-01 10:00:00"], "tstz": ["2024-01-01 10:00:00+00:00"]},
    )
    assert not _uncleared_coercion_blocks(out), out["coercion_issues"]
    assert "ts (TIMESTAMP) → ts (date)" not in out["quality_issues"]


def test_a_real_lossy_final_mapping_still_blocks_safe_to_start():
    final = [_row("amt", "amt", "DECIMAL(14,4)", "DECIMAL(8,2)", create_new=False,
                  assignment_strategy="optimal_bipartite_hungarian")]
    out = _regrade_signed_coercions(
        {"coercion_issues": []}, final, dest_db_type="postgresql", dest_exists=True,
    )
    assert _uncleared_coercion_blocks(out), out["coercion_issues"]
    assert all(i["severity"] == "block" and i["lossy"] for i in out["coercion_issues"])
