"""What an incremental cursor column means, and what that permits.

An incremental sync reads rows whose cursor value is past the last watermark.
Whether that read loses data depends entirely on what the column means in the
source, and no property of the column establishes it:

* ``created_at`` is a perfectly ordered timestamp on a table whose rows are
  updated in place. Every update after the first run is invisible forever, and
  the run still reports success.
* ``order_date`` is set from a calendar, not a clock, so a row inserted today
  with last month's date is already behind the watermark and is skipped
  forever — silent loss on the insert path itself.
* ``updated_at`` is safe only because a trigger or the application maintains
  it. That is a fact about the source system, not about the column.

So the meaning is declared, and the declaration is what the product reasons
about. A route that has not declared it gets the guarantee it can prove — and
never the guarantee it cannot.
"""

from __future__ import annotations

from dataclasses import dataclass, field

#: The cursor column is written on insert and never changes; rows are never
#: updated. Inserts are captured; there are no updates to miss.
INSERT_ONLY = "insert_only"

#: The source maintains the column on every change (trigger, ORM hook, or
#: application invariant). Inserts and updates are both captured.
MODIFICATION_TIMESTAMP = "modification_timestamp"

#: A strictly increasing generated value (identity, sequence, LSN-like counter)
#: assigned on insert. Inserts are captured in order; updates are not.
MONOTONIC_SEQUENCE = "monotonic_sequence"

#: A log position supplied by the source's change stream. The change stream, not
#: the column, carries the completeness guarantee.
CDC_POSITION = "cdc_position"

#: Declared explicitly as neither monotonic nor update-bearing — a business or
#: calendar date. Kept as a declarable value so the refusal can name it.
BUSINESS_DATE = "business_date"

CURSOR_SEMANTICS: frozenset[str] = frozenset(
    {
        INSERT_ONLY,
        MODIFICATION_TIMESTAMP,
        MONOTONIC_SEQUENCE,
        CDC_POSITION,
        BUSINESS_DATE,
    }
)

#: Semantics under which a row that changes after it was read is read again.
_CAPTURES_UPDATES: frozenset[str] = frozenset({MODIFICATION_TIMESTAMP, CDC_POSITION})

#: Semantics under which a newly inserted row always sorts after the watermark.
_MONOTONIC_ON_INSERT: frozenset[str] = frozenset(
    {INSERT_ONLY, MODIFICATION_TIMESTAMP, MONOTONIC_SEQUENCE, CDC_POSITION}
)

#: Sync modes whose contract with the operator is that a changed source row is
#: reflected at the destination. Reading with an insert-only cursor cannot
#: deliver that, however green the run looks.
MODES_PROMISING_UPDATE_CAPTURE: frozenset[str] = frozenset(
    {"incremental_deduped", "scd2", "mirror", "cdc"}
)


@dataclass(frozen=True)
class CursorSemanticsVerdict:
    """Whether this cursor can support what the sync mode promises."""

    status: str = "not_applicable"  # ok | block | not_applicable
    declared: str = ""
    cursor_field: str = ""
    sync_mode: str = ""
    reason: str = ""
    #: The one thing the operator should do. Anything else is a second-order
    #: option, not a competing primary action.
    primary_action: str = ""
    alternatives: list[str] = field(default_factory=list)
    #: What the run may honestly claim to capture with this declaration.
    captures_updates: bool = False
    monotonic_on_insert: bool = False

    @property
    def blocks(self) -> bool:
        return self.status == "block"

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "declared": self.declared,
            "cursor_field": self.cursor_field,
            "sync_mode": self.sync_mode,
            "reason": self.reason,
            "primary_action": self.primary_action,
            "alternatives": list(self.alternatives),
            "captures_updates": self.captures_updates,
            "monotonic_on_insert": self.monotonic_on_insert,
        }


def evaluate_cursor_semantics(
    *,
    sync_mode: str,
    cursor_field: str,
    declared: str,
    validation_mode: str = "strict",
) -> CursorSemanticsVerdict:
    """Judge a cursor declaration against what the sync mode promises.

    Refuses rather than guesses: an undeclared cursor on a mode that promises
    update capture is not assumed safe, because the assumption is invisible
    until a client notices a stale row months later.
    """
    from services.sync_cursor import normalize_sync_mode, requires_incremental

    mode = normalize_sync_mode(sync_mode)
    cursor = (cursor_field or "").strip()
    if not requires_incremental(mode) or not cursor:
        return CursorSemanticsVerdict(sync_mode=mode, cursor_field=cursor)

    value = (declared or "").strip().lower()
    if value and value not in CURSOR_SEMANTICS:
        return CursorSemanticsVerdict(
            status="block",
            declared=value,
            cursor_field=cursor,
            sync_mode=mode,
            reason=(
                f"Unknown cursor semantics '{value}' — the product cannot reason "
                "about a meaning it does not define."
            ),
            primary_action=(
                "Declare what "
                f"{cursor} means in the source: "
                + ", ".join(sorted(CURSOR_SEMANTICS))
            ),
        )

    captures = value in _CAPTURES_UPDATES
    monotonic = value in _MONOTONIC_ON_INSERT
    strict = (validation_mode or "strict").strip().lower() != "balanced"
    promises_updates = mode in MODES_PROMISING_UPDATE_CAPTURE

    if monotonic:
        # Every row the source adds sorts after the watermark, so nothing is
        # skipped. Whether updates are seen depends on the class, and a class
        # that does not see them is still sound when the source makes none.
        if promises_updates and not captures:
            reason = (
                f"{cursor} declared '{value}': it does not move when a row is "
                f"updated, so '{mode}' reflects source changes only because you "
                "declared this source does not make them. New rows are captured "
                "in full."
            )
        else:
            reason = (
                f"{cursor} declared '{value}' — captures "
                + ("inserts and updates" if captures else "inserts only")
            )
        return CursorSemanticsVerdict(
            status="ok",
            declared=value,
            cursor_field=cursor,
            sync_mode=mode,
            reason=reason,
            captures_updates=captures,
            monotonic_on_insert=True,
        )

    # Not monotonic on insert: a row can arrive behind the watermark and be
    # skipped permanently, and under an update-bearing mode an in-place edit is
    # never re-read. Both are silent, so neither may be assumed away.
    if value == BUSINESS_DATE:
        reason = (
            f"{cursor} declared '{value}': it comes from a calendar rather than "
            "the insert order, so a row inserted with an earlier date stays "
            f"behind the watermark and is never read"
            + (
                f", and an update that leaves it untouched is never re-read either"
                if promises_updates
                else ""
            )
            + "."
        )
    elif promises_updates:
        reason = (
            f"'{mode}' keeps the destination in step with changed source rows, "
            f"but nothing states that {cursor} moves when a row changes. An "
            "update that leaves it untouched is never re-read, and the run still "
            "reports success."
        )
    else:
        if not strict:
            return CursorSemanticsVerdict(
                status="ok",
                declared=value,
                cursor_field=cursor,
                sync_mode=mode,
                reason=(
                    f"{cursor} is undeclared — balanced validation accepts it, and "
                    "this run claims insert capture only: neither completeness nor "
                    "update capture is proven."
                ),
                captures_updates=False,
                monotonic_on_insert=False,
            )
        reason = (
            f"Nothing states that a row inserted into this source always carries "
            f"a {cursor} value later than the ones already read. A backdated "
            "insert lands behind the watermark and is skipped permanently, with "
            "no error."
        )
    return CursorSemanticsVerdict(
        status="block",
        declared=value,
        cursor_field=cursor,
        sync_mode=mode,
        reason=reason,
        primary_action=(
            f"Declare what {cursor} means in the source"
            if not value
            else (
                "Select a cursor the source assigns in insert order, or maintains "
                "on every change"
            )
        ),
        alternatives=[
            f"Declare it '{MODIFICATION_TIMESTAMP}' if the source updates it on "
            "every change",
            f"Declare it '{INSERT_ONLY}' if rows are inserted once and never "
            "updated",
            "Use full_refresh_overwrite to re-read the whole source each run",
            "Use CDC, which reads the source's own change log instead of a column",
        ],
        captures_updates=captures,
        monotonic_on_insert=False,
    )
