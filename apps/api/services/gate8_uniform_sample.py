"""A uniform random Gate-8 sample over every row a streaming run offered the writer (P0-5).

Gate-8's value sample used to be the first rows the run read (then the lowest
keys among them): a sample that says nothing about the rows it did not check,
so the Trust Report could never state the 95% upper bound the docs promised,
and every sampled run was unsignable.

This module draws a **uniform random sample without replacement** of up to
``k`` rows from the whole population the run streamed, in one pass and in
memory bounded by ``k`` per page:

* each page (``idx``) draws ``min(k, n_page)`` row positions uniformly with a
  generator seeded by ``"<seed>:<idx>"``;
* the coordinator merges each page's sample into the running sample in commit
  order: of ``min(k, N + n)`` slots, the number taken from the page follows the
  hypergeometric law (drawn as ``rng.sample(range(N + n), ...)``, seeded by
  ``"<seed>:merge:<idx>"``), and the slots are filled by uniform sub-samples of
  the running sample and of the page's sample. Merging two uniform samples
  this way gives a uniform sample of the union (Algorithm "merge of
  reservoirs"; see ``tests/test_gate8_uniform_sample.py`` for the frequency
  test).

The seed is random per run (``secrets``) and recorded with the sample
(``reconcile_sample_draw``), so the draw is reproducible for an auditor who
replays the same pages, and the sampled keys are listed in the Gate-8 report.
The Clopper-Pearson bound in :mod:`services.parity_stats` is valid for this
method only (:data:`services.parity_evidence.UNIFORM_RANDOM_SAMPLE_METHODS`).
"""

from __future__ import annotations

import random
import secrets
from typing import Any

METHOD = "uniform_random"
ALGORITHM = (
    "uniform random sample without replacement over every row the run offered the writer: "
    "a seeded draw per page, merged across pages with hypergeometric slot counts"
)


def new_draw(size: int) -> dict[str, Any]:
    """The draw's identity for one run: method, a fresh random seed, the sample size."""
    return {"method": METHOD, "seed": secrets.token_hex(16), "size": int(size), "algorithm": ALGORITHM}


def page_sample_indices(seed: str, idx: int, n: int, k: int) -> list[int]:
    """Positions of a uniform sample of ``min(k, n)`` rows of page ``idx`` (``n`` rows)."""
    if n <= 0 or k <= 0:
        return []
    rng = random.Random(f"{seed}:{int(idx)}")  # nosec B311 - sampling, not security
    return sorted(rng.sample(range(n), min(k, n)))


class UniformSampleMerger:
    """The running uniform sample over every page merged so far."""

    def __init__(self, seed: str, k: int) -> None:
        self.seed = str(seed)
        self.k = max(int(k), 0)
        self.population = 0
        self.rows: list[Any] = []
        #: True when this run resumed without the earlier pages' draw: the
        #: sample then covers only the rows written after the resume (T3).
        self.partial = False

    def add(self, idx: int, page_sample: list[Any], page_rows: int) -> None:
        """Merge page ``idx``'s uniform sample (of ``page_rows`` rows) into the running one."""
        n = max(int(page_rows), 0)
        if n == 0:
            return
        page_sample = list(page_sample)[: min(self.k, n)]
        total = self.population + n
        size = min(self.k, total)
        rng = random.Random(f"{self.seed}:merge:{int(idx)}")  # nosec B311 - sampling, not security
        slots = rng.sample(range(total), size)
        from_page = sum(1 for s in slots if s >= self.population)
        # The page's sample holds min(k, n) >= from_page rows; the running one min(k, N) >= the rest.
        from_page = min(from_page, len(page_sample))
        keep = min(size - from_page, len(self.rows))
        self.rows = rng.sample(self.rows, keep) + rng.sample(page_sample, from_page)
        self.population = total

    def draw_record(self, draw: dict[str, Any]) -> dict[str, Any]:
        """What Gate-8 and the Trust Report need to know about this draw."""
        return {
            **draw, "population_rows": self.population, "drawn_rows": len(self.rows),
            "covers_whole_run": not self.partial,
        }

    def state(self, draw: dict[str, Any], chunk_index: int) -> dict[str, Any]:
        """The checkpointed draw after page ``chunk_index``: enough to continue it."""
        return {
            **draw, "population_rows": self.population, "rows": list(self.rows),
            "chunk_index": int(chunk_index), "partial": self.partial,
        }


def resume_draw(checkpoint: Any, k: int, *, resumed: bool) -> tuple[dict[str, Any], UniformSampleMerger]:
    """``(draw, merger)`` for this pass: the checkpoint's draw when resuming (T3).

    A resumed pass continues the seed, population and rows the checkpoint
    carried, so the sample is still uniform over every row the run offered
    the writer. A resumed pass without that state starts a draw over the new
    pages only and is marked ``partial``: it is not reported as uniform over
    the run, and no bound is stated for it.
    """
    state = getattr(checkpoint, "reconcile_sample_draw_state", None) if resumed else None
    if (
        isinstance(state, dict) and state.get("seed") and state.get("method") == METHOD
        and not state.get("partial")
        # Saved with the same page as the resume point: an older copy (the
        # job document lagging the sidecar token) would miss pages.
        and state.get("chunk_index") is not None
        and int(state["chunk_index"]) == int(getattr(checkpoint, "chunk_index", 0) or 0)
    ):
        draw = {key: state[key] for key in ("method", "seed", "size", "algorithm") if key in state}
        merger = UniformSampleMerger(str(state["seed"]), int(state.get("size") or k))
        merger.population = max(int(state.get("population_rows") or 0), 0)
        merger.rows = [row for row in state.get("rows") or [] if isinstance(row, dict)][: merger.k]
        return draw, merger
    draw = new_draw(k)
    merger = UniformSampleMerger(draw["seed"], k)
    merger.partial = bool(resumed)
    return draw, merger
