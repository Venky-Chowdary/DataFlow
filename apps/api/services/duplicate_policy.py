"""Per-transfer duplicate policy.

Three outcomes, one owner:

* ``upsert`` — MERGE / ON CONFLICT DO UPDATE. Default when a business key
  exists and the operator did not ask for duplicates.
* ``dedupe_skip`` — insert-if-not-exists (ON CONFLICT DO NOTHING /
  WHEN NOT MATCHED THEN INSERT). Existing rows stay.
* ``append`` — plain INSERT, duplicates allowed. Only when the operator
  sets ``duplicate_policy=append`` or ``allow_duplicates``.

``full_refresh_append`` stays an insert: two runs land 2N. A schedule that
sets ``duplicate_policy=upsert`` (the schedule default when a key exists)
MERGEs instead. ``skip_duplicates`` inserts only new keys.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any

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


def validate_schedule_file_policy(
    duplicate_policy: str | None,
    in_file_dedupe: str | None = "block",
    *,
    primary_key: str = "",
) -> tuple[str, str]:
    """Reject an unknown schedule policy. Empty policy stays empty.

    Upsert and skip-duplicates need a key. Append is the explicit opt-in
    that keeps duplicate rows. In-file dedupe is ``block`` or ``keep_last``.
    """
    policy = normalize_duplicate_policy(duplicate_policy)
    if policy and policy not in {"upsert", "dedupe_skip", "append"}:
        raise DuplicatePolicyError(
            f"Unknown duplicate_policy {duplicate_policy!r}. "
            "Use upsert, skip_duplicates, or append."
        )
    dedupe = (in_file_dedupe or "block").strip().lower().replace("-", "_")
    if dedupe in {"last", "last_wins"}:
        dedupe = "keep_last"
    if dedupe not in {"block", "keep_last"}:
        raise DuplicatePolicyError(
            f"Unknown in_file_dedupe {in_file_dedupe!r}. Use block or keep_last."
        )
    if policy in {"upsert", "dedupe_skip"} and not str(primary_key or "").strip():
        raise DuplicatePolicyError(
            "Upsert and skip_duplicates need a primary key. "
            "Declare one, or choose append to allow duplicate rows."
        )
    return policy, dedupe


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

    # full_refresh_append is already an explicit sync mode: two runs land 2N.
    # Rewriting it to upsert failed Gate-8 (dest Δ 0) and the sync-mode matrix.
    # Upsert and skip-duplicates apply when duplicate_policy names them.
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


def collapse_keep_last(
    rows: list[dict],
    key_columns: list[str],
    key_fn: Any = None,
) -> tuple[list[dict], int]:
    """Keep the last row for each key. Earlier copies are not a second insert.

    ``block`` is the other in-file choice: the caller refuses the file instead
    of calling this. Rows with a NULL / missing key are kept in order: ``''`` is
    a key like any other, so two ``''`` rows collapse (D53).
    """
    from services.value_serializer import is_reader_null_cell

    keys = [str(col) for col in key_columns if str(col).strip()]
    if not keys:
        return list(rows), 0
    index: dict[tuple, int] = {}
    kept: list[dict] = []
    dropped = 0
    for row in rows:
        if not isinstance(row, dict):
            kept.append(row)
            continue
        identity = key_fn(row) if key_fn is not None else tuple(row.get(col) for col in keys)
        if not isinstance(identity, tuple):
            identity = (identity,)
        if any(value is None or is_reader_null_cell(value) for value in identity):
            kept.append(row)
            continue
        previous = index.get(identity)
        if previous is None:
            index[identity] = len(kept)
            kept.append(row)
            continue
        kept[previous] = row
        dropped += 1
    return kept, dropped


def iter_keep_last(batches, key_columns: list[str]):
    """Collapse one file to last-wins, then yield it in the original chunk sizes."""
    rows: list[dict] = []
    width = 0
    for batch in batches:
        chunk = list(batch or [])
        if chunk and width == 0:
            width = len(chunk)
        rows.extend(chunk)
    collapsed, _dropped = collapse_keep_last(rows, key_columns)
    step = width or 1000
    for start in range(0, len(collapsed), step):
        yield collapsed[start : start + step]


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
