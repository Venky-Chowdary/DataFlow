"""Hot type-string helpers are memoized, and memoization changes no verdict.

These four helpers run once per *cell* on the bind and fingerprint paths — a
10M-row load calls them hundreds of millions of times over a vocabulary of a
few dozen distinct type strings, and un-memoized they dominated the profile
(regex recompilation alone was ~30s of a 219s profiled run).

A cache is only safe if the function is pure, so this pins both properties:
the cache exists (a future refactor that drops it is a real regression), and
every cached answer equals the uncached ``__wrapped__`` answer.
"""

from __future__ import annotations

import pytest

from connectors.sql_temporal import sql_base_type
from services.decision_kernel.type_invent import normalize_logical_type
from services.type_system import strip_collation_qualifier, strip_identity_qualifier

TYPE_STRINGS = [
    "BIGINT",
    "bigint",
    "VARCHAR(64)",
    "NUMERIC(12,2)",
    "TIMESTAMP(6) WITH TIME ZONE",
    "TIMESTAMP WITHOUT TIME ZONE",
    "TIMESTAMPTZ(3)",
    "JSONB",
    "BIT VARYING(8)",
    "INTEGER GENERATED ALWAYS AS IDENTITY",
    "BIGINT AUTO_INCREMENT",
    "VARCHAR(20) COLLATE utf8mb4_bin",
    "Nullable(DateTime64(3))",
    "DECIMAL(38,10)",
    "",
]

MEMOIZED = [
    normalize_logical_type,
    strip_identity_qualifier,
    strip_collation_qualifier,
    sql_base_type,
]


@pytest.mark.parametrize("fn", MEMOIZED, ids=lambda f: f.__name__)
def test_helper_is_memoized(fn) -> None:
    assert hasattr(fn, "cache_info"), f"{fn.__name__} lost its lru_cache"


@pytest.mark.parametrize("fn", MEMOIZED, ids=lambda f: f.__name__)
@pytest.mark.parametrize("raw", TYPE_STRINGS)
def test_cached_answer_equals_uncached(fn, raw: str) -> None:
    uncached = fn.__wrapped__(raw)
    assert fn(raw) == uncached
    # Second call is served from the cache — it must not drift.
    assert fn(raw) == uncached


def test_cache_actually_hits() -> None:
    normalize_logical_type.cache_clear()
    for _ in range(50):
        normalize_logical_type("NUMERIC(12,2)")
    info = normalize_logical_type.cache_info()
    assert info.hits >= 49, info


def test_none_and_case_variants_are_distinct_keys_but_agree() -> None:
    """``None`` is hashable and must not collide with the empty string."""
    assert normalize_logical_type(None) == normalize_logical_type.__wrapped__(None)
    assert normalize_logical_type("BIGINT") == normalize_logical_type("bigint")
