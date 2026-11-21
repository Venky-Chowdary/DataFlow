"""Assignment policy around the semantic mapper's pair scores.

Kept apart from :mod:`services.semantic_mapper` (Phase F8 size freeze). Every
rule here is generic (no column names from any benchmark) and was tuned only on
the dev split of the held-out set (tests/fixtures/mapping_holdout_v1.json).

* :func:`pin_decisive_pairs`: a source and a destination that are each other's
  clear best match are bound before the bipartite solver runs. Maximising the
  *sum* of uncalibrated scores let two weak pairs (0.70 + 0.70) outvote one
  decisive pair (0.99 + a leftover), so ``dept_cd`` lost ``department_code``
  to a column it shares no token with.
"""

from __future__ import annotations

# A pair is decisive when it is the best for its source and for its
# destination by at least this margin, and scores at least this much.
DECISIVE_MIN_SCORE = 0.75
DECISIVE_MARGIN = 0.05


# --------------------------------------------------------------------------
# Decisive pairs
# --------------------------------------------------------------------------


def pin_decisive_pairs(
    pinned: dict[str, str],
    source_columns: list[str],
    target_columns: list[str],
    pair_scores: dict[tuple[str, str], tuple[float, str]],
) -> dict[str, str]:
    """Extend ``pinned`` with mutual-best pairs; returns a new dict."""
    out = dict(pinned)
    changed = True
    while changed:
        changed = False
        free_src = [s for s in source_columns if s not in out]
        taken = set(out.values())
        free_tgt = [t for t in target_columns if t not in taken]
        if not free_src or not free_tgt:
            break
        for src in free_src:
            row = sorted(((pair_scores[(src, t)][0], t) for t in free_tgt), reverse=True)
            best, tgt = row[0]
            if best < DECISIVE_MIN_SCORE:
                continue
            if len(row) > 1 and best - row[1][0] < DECISIVE_MARGIN:
                continue
            col = sorted((pair_scores[(s, tgt)][0] for s in free_src if s != src), reverse=True)
            if col and best - col[0] < DECISIVE_MARGIN:
                continue
            out[src] = tgt
            changed = True
            break
    return out
