"""Per-transfer duplicate policy.

Three outcomes, one owner:

* ``upsert`` — MERGE / ON CONFLICT DO UPDATE. Default when a business key
  exists and the operator did not ask for duplicates.
* ``dedupe_skip`` — insert-if-not-exists (ON CONFLICT DO NOTHING /
  WHEN NOT MATCHED THEN INSERT). Existing rows stay.
* ``append`` — plain INSERT, duplicates allowed. Only when the operator
  sets ``duplicate_policy=append`` or ``allow_duplicates``.

An append sync mode with a business key used to INSERT again and turn 25
rows into 50. That path now upserts unless append was chosen explicitly.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass

_MATCH = contextvars.ContextVar("df_duplicate_match_action", default="update")
_ORDER = contextvars.ContextVar("df_duplicate_order_column", default="")

# incremental_append is cursor-bounded: only rows after the watermark are
# written. Rewriting it to a full-file upsert reloaded the history (the
# file incremental tests). full_refresh_append / append with a key is the
# path that turned 25 rows into 50.
_APPEND_SYNC = frozenset(
    {
        "full_refresh_append",
        "append",
    }
)


class DuplicatePolicyError(ValueError):
    """The requested policy cannot be applied on this route."""


@dataclass(frozen=True)
class DuplicateDecision:
    policy: str
    write_mode: str
    match_action: str
    explicit: bool
    reason: str


def normalize_duplicate_policy(raw: str | None) -> str:
    text = (raw or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "merge": "upsert",
        "update": "upsert",
        "on_conflict_update": "upsert",
        "skip": "dedupe_skip",
        "skip_duplicates": "dedupe_skip",
        "insert_if_not_exists": "dedupe_skip",
        "do_nothing": "dedupe_skip",
        "ignore": "dedupe_skip",
        "allow_duplicates": "append",
        "insert": "append",
    }
    return aliases.get(text, text)


def resolve_duplicate_policy(
    *,
    duplicate_policy: str = "",
    allow_duplicates: bool = False,
    sync_mode: str = "",
    business_key: bool = False,
    write_mode: str = "",
) -> DuplicateDecision:
    """Choose write mode and match action for this transfer."""
    explicit = normalize_duplicate_policy(duplicate_policy)
    if allow_duplicates and not explicit:
        explicit = "append"
    mode = (sync_mode or "").strip().lower().replace("-", "_")
    caller = (write_mode or "").strip().lower()

    if explicit == "append":
        return DuplicateDecision(
            "append",
            "insert",
            "update",
            True,
            "Append was chosen explicitly — duplicate rows are allowed.",
        )
    if explicit == "dedupe_skip":
        if not business_key:
            raise DuplicatePolicyError(
                "Skip duplicates needs a business key. Declare a primary key, "
                "or choose append to allow duplicate rows."
            )
        return DuplicateDecision(
            "dedupe_skip",
            "upsert",
            "skip",
            True,
            "Skip duplicates — existing keys stay, new keys insert.",
        )
    if explicit == "upsert":
        if not business_key:
            raise DuplicatePolicyError(
                "Upsert needs a business key. Declare a primary key, "
                "or choose append to allow duplicate rows."
            )
        return DuplicateDecision(
            "upsert",
            "upsert",
            "update",
            True,
            "Upsert on the business key — existing rows update, new rows insert.",
        )
    if explicit and explicit not in {"upsert", "dedupe_skip", "append"}:
        raise DuplicatePolicyError(
            f"Unknown duplicate_policy {duplicate_policy!r}. "
            "Use upsert, dedupe_skip, or append."
        )

    if business_key and (mode in _APPEND_SYNC or caller == "insert"):
        if mode in _APPEND_SYNC:
            return DuplicateDecision(
                "upsert",
                "upsert",
                "update",
                False,
                "A business key is declared, so append will not insert duplicates. "
                "Set duplicate_policy=append to allow them.",
            )
    if caller == "upsert" and business_key:
        return DuplicateDecision(
            "upsert",
            "upsert",
            "update",
            False,
            "Upsert on the business key.",
        )
    return DuplicateDecision(
        "append" if caller != "upsert" else "upsert",
        caller or "insert",
        "update",
        False,
        "",
    )


def set_match_action(action: str):
    return _MATCH.set("skip" if (action or "").strip().lower() == "skip" else "update")


def reset_match_action(token) -> None:
    _MATCH.reset(token)


def current_match_action() -> str:
    return _MATCH.get()


def set_order_column(name: str):
    return _ORDER.set((name or "").strip())


def reset_order_column(token) -> None:
    _ORDER.reset(token)


def current_order_column() -> str:
    return _ORDER.get()


def order_wins(incoming: object, kept: object) -> bool:
    """True when ``incoming`` should replace ``kept`` (last-wins on ties)."""
    if incoming is None and kept is None:
        return True
    if incoming is None:
        return False
    if kept is None:
        return True
    try:
        return float(incoming) >= float(kept)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return str(incoming) >= str(kept)
