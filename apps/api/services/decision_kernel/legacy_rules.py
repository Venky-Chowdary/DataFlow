"""Plan rules as feature/Cleanup applied them, for verifying a stored stamp (review Q1).

A schedule's stamp is a hash of the decision Validate made. This release
changed three type rules (QA batch 1 / PR #435), so rebuilding an unchanged
Map now yields a different decision and the old stamp read as "Map/DDL
drifted" on every beat:

* PostgreSQL ``uuid`` into Snowflake ``STRING`` / ``TEXT`` / bare ``VARCHAR``
  was graded a lossy UUID-domain collapse; it is now the lossless
  ``VARCHAR(16777216)`` (``type_system.snowflake_unbounded_text_alias``).
* PostgreSQL ``bit(1)`` into MySQL was ``BOOLEAN``; it is now ``BIT(1)``.
* A varying / unbounded / wider-than-64 PostgreSQL bit string into MySQL was
  ``BIT(n)`` clamped to 64; it is now 0/1 digit text.

Inside :func:`legacy_plan_rules` those points answer as they did before, so a
stamp minted by the previous release can be matched against the same Map.
Nothing is ever planned or written under these rules.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_LEGACY: ContextVar[bool] = ContextVar("datawrap_legacy_plan_rules", default=False)


def legacy_rules_active() -> bool:
    return _LEGACY.get()


@contextmanager
def legacy_plan_rules() -> Iterator[None]:
    token = _LEGACY.set(True)
    try:
        yield
    finally:
        _LEGACY.reset(token)
