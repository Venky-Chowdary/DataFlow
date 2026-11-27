"""Trust audit P0-5: Gate-8's sample is a seeded uniform random draw over every row offered.

Unit proofs of the sampler (:mod:`services.gate8_uniform_sample`): every row of
the population is equally likely to be drawn whatever the page sizes, pairs
are equally likely too (a draw without replacement), the draw is reproducible
from its recorded seed, and different seeds give different draws. The live
proof on PostgreSQL is ``test_trust_p0_5_uniform_sample_live_pg.py``.
"""

from __future__ import annotations

from itertools import combinations

import pytest

from services.gate8_uniform_sample import UniformSampleMerger, new_draw, page_sample_indices

PAGES = [3, 7, 1, 9, 4]  # 24 rows, uneven pages, one smaller than k
K = 6


def _draw(seed: str, pages: list[int] = PAGES, k: int = K) -> tuple[list[int], int]:
    merger = UniformSampleMerger(seed, k)
    start = 0
    for idx, n in enumerate(pages):
        rows = list(range(start, start + n))
        picked = [rows[i] for i in page_sample_indices(seed, idx, n, k)]
        merger.add(idx, picked, n)
        start += n
    return merger.rows, merger.population


def test_size_population_and_no_repeats():
    rows, population = _draw("seed-a")
    assert population == sum(PAGES)
    assert len(rows) == K and len(set(rows)) == K
    assert all(0 <= r < population for r in rows)
    small, small_pop = _draw("seed-a", pages=[2, 1], k=K)
    assert sorted(small) == [0, 1, 2] and small_pop == 3  # fewer rows than k: every row


def test_reproducible_from_the_recorded_seed():
    assert _draw("seed-x") == _draw("seed-x")
    assert _draw("seed-x")[0] != _draw("seed-y")[0]
    draw = new_draw(50)
    assert draw["method"] == "uniform_random" and len(draw["seed"]) == 32 and draw["size"] == 50
    assert new_draw(50)["seed"] != draw["seed"]


def test_every_row_and_every_pair_is_equally_likely():
    trials = 30000
    n = sum(PAGES)
    single = [0] * n
    pairs: dict[tuple[int, int], int] = {}
    for t in range(trials):
        rows, _ = _draw(f"s{t}")
        for r in rows:
            single[r] += 1
        for pair in combinations(sorted(rows), 2):
            pairs[pair] = pairs.get(pair, 0) + 1
    expected = trials * K / n  # 7,500
    # Chi-square over 24 rows (23 dof): the 0.999 quantile is about 49.7.
    chi = sum((c - expected) ** 2 / expected for c in single)
    assert chi < 49.7, (chi, single)
    assert max(abs(c - expected) / expected for c in single) < 0.05
    expected_pair = trials * K * (K - 1) / (n * (n - 1))
    assert len(pairs) == n * (n - 1) // 2
    assert max(abs(c - expected_pair) / expected_pair for c in pairs.values()) < 0.25


@pytest.mark.parametrize("k", [1, 5, 50])
def test_page_sample_indices_are_a_subset_of_the_page(k):
    picked = page_sample_indices("s", 3, 20, k)
    assert picked == sorted(set(picked)) and len(picked) == min(k, 20)
    assert all(0 <= i < 20 for i in picked)
    assert page_sample_indices("s", 3, 0, k) == []
