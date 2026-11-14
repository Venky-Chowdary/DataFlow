"""Whether a tool's result is actually in the answer.

``grounded`` used to mean "a tool returned success this turn". A read that
could not reach its store, or a result the composer rendered nothing from,
still made the turn grounded at 0.96 — next to an answer that stated none of
it. This module is the minimal honest version of that flag: a tool counts as
evidence only when its result was read from the store *and* rendered into the
answer. (Checking each stated fact against the tool output is the verifier's
job and is not done here.)
"""

from __future__ import annotations

from typing import Any

#: A result with one of these stages or opens something; it is used even when
#: the composer adds no sentence for it.
_ACTION_KEYS = ("ack_id", "action", "actions", "requires_confirm", "screen", "pending_action")


def result_unread(result: Any) -> bool:
    """True when the tool ran but its store could not be read (``unread`` output)."""
    output = getattr(result, "output", None)
    return isinstance(output, dict) and output.get("unread") is True


def _stages_action(result: Any) -> bool:
    output = getattr(result, "output", None)
    return isinstance(output, dict) and any(output.get(key) for key in _ACTION_KEYS)


class RenderTracker:
    """Records which successful tool results added nothing to the answer."""

    def __init__(self, parts: list[str]):
        self._parts = parts
        self._current: Any = None
        self._mark = 0
        self.silent: set[int] = set()

    def begin(self, result: Any) -> None:
        """Start the span of one tool result (closing the previous one)."""
        self.close()
        self._current = result
        self._mark = len(self._parts)

    def close(self) -> None:
        result = self._current
        self._current = None
        if result is None or not getattr(result, "success", False):
            return
        if len(self._parts) == self._mark and not _stages_action(result):
            self.silent.add(id(result))


def result_used(result: Any, silent: set[int] | frozenset[int] = frozenset()) -> bool:
    """A successful result that was read and rendered — the only kind that is evidence."""
    return bool(getattr(result, "success", False)) and id(result) not in silent and not result_unread(result)
