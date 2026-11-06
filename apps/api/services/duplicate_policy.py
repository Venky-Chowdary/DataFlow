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


def route_duplicate_options(*extras: Any) -> dict[str, str]:
    """``in_file_dedupe`` / ``duplicate_policy`` the route chose, from the first
    extra (source, then destination) that names each.

    Validate reads them through ``preflight_file_plan`` and the file writer
    reads ``destination.extra``; the engine stamps this answer on the
    destination so both see one choice. A ``keep_last`` set on the source alone
    used to clear Validate while the writer loaded every copy.
    """
    from services.preflight_file_plan import _plan_duplicate_policy, _plan_in_file_dedupe

    configs = [e for e in extras if isinstance(e, dict)]
    out: dict[str, str] = {}
    dedupe = _plan_in_file_dedupe(*configs)
    if dedupe:
        out["in_file_dedupe"] = dedupe
    policy = _plan_duplicate_policy(*configs)
    if policy:
        out["duplicate_policy"] = policy
    return out


def keep_last_records(request: Any, records: list[dict]) -> list[dict]:
    """The buffered file route's keep_last: the same collapse the file stream runs.

    ``file_stream`` collapses through :func:`iter_keep_last`; a file the engine
    reads whole (priority/limit, staging, mirror) is collapsed here, before
    the write and before Gate-8 hashes ``records``. A table is never collapsed
    here — Validate refuses keep_last on one (``KEEP_LAST_TABLE_REFUSAL``).
    """
    source = getattr(request, "source", None)
    if str(getattr(source, "kind", "") or "").lower() != "file" or not records:
        return records
    options = route_duplicate_options(
        dict(getattr(source, "extra", None) or {}),
        dict(getattr(getattr(request, "destination", None), "extra", None) or {}),
    )
    if options.get("in_file_dedupe") != "keep_last":
        return records
    from services.sync_cursor import resolve_sync_contract

    contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
    keys = list(contract.primary_key_columns()) if contract else []
    if not keys:
        return records
    collapsed, _dropped = collapse_keep_last(list(records), keys)
    return collapsed


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


def latest_version_per_key(
    rows: list[dict],
    *,
    cursor_column: str,
    key_columns: list[str],
) -> list[dict]:
    """One row per key — the one an ordered upsert keeps.

    The writer reads ``ORDER BY cursor, key`` and keeps the last copy of a key
    it sees. Ranking a population the same way needs the watermark's typed
    comparison (``services.sync_cursor.compare_cursor_values``: ``999`` before
    ``1000``, instants by value, NULL lowest) with the key as the tie-break;
    sorting the wire text ranked ``"999"`` above ``"1000"`` and let a NULL
    cursor win. Equal ``(cursor, key)`` keep arrival order (stable sort): two
    versions with the same cursor and key are not provably ordered on either
    side, so the survivor here is the last one *this* read saw, which matches
    the writer only when both reads returned them in the same order. Then
    :func:`collapse_keep_last`.
    """
    from functools import cmp_to_key

    from services.sync_cursor import compare_cursor_values
    from services.value_serializer import is_reader_null_cell

    keys = [str(c) for c in key_columns if str(c or "").strip()]
    if not rows or not keys:
        return list(rows)

    def _typed(value: Any) -> str | None:
        return None if value is None or is_reader_null_cell(value) else str(value)

    ordered = list(rows)
    if cursor_column:
        def _writer_order(a: dict, b: dict) -> int:
            by_cursor = compare_cursor_values(_typed(a.get(cursor_column)), _typed(b.get(cursor_column)))
            if by_cursor:
                return by_cursor
            for col in keys:
                by_key = compare_cursor_values(_typed(a.get(col)), _typed(b.get(col)))
                if by_key:
                    return by_key
            return 0

        ordered = sorted(ordered, key=cmp_to_key(_writer_order))
    kept, _dropped = collapse_keep_last(ordered, keys)
    return kept


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
