"""Memory-bounded accumulators for long-running transfer loops.

A transfer can run for hours over millions of rows, appending diagnostics on
every batch. Several such accumulators were capped only where they were
*rendered* (``warnings[:10]``), which bounded the operator's view but not the
process — the underlying lists kept growing for the life of the job, and the
worst case was a job that quarantined heavily and then died of memory pressure
or blew past MongoDB's 16 MB document limit while trying to checkpoint.

These types cap at construction so every producer inherits the bound, and they
subclass the builtins so existing slicing, ``in``, ``[-1]`` and truthiness
checks at the call sites keep working untouched.
"""

from __future__ import annotations

from typing import Any, Iterable


class BoundedStrings(list):
    """A de-duplicating string list that stops growing at ``cap`` entries.

    Deduplication is what makes a small cap safe here: per-batch diagnostics are
    overwhelmingly the same handful of messages repeated, so the first ``cap``
    *distinct* entries carry essentially the full signal while the retained
    memory stays flat.

    ``dropped`` counts entries refused after the cap was reached, so a caller
    can honestly report "showing N, M more suppressed" instead of implying the
    list is complete.
    """

    __slots__ = ("cap", "dropped", "_seen")

    def __init__(self, items: Iterable[str] | None = None, *, cap: int = 200) -> None:
        super().__init__()
        self.cap = max(1, int(cap))
        self.dropped = 0
        self._seen: set[str] = set()
        if items:
            self.extend(items)

    def append(self, item: Any) -> None:  # type: ignore[override]
        text = item if isinstance(item, str) else str(item)
        if text in self._seen:
            return
        if len(self) >= self.cap:
            self.dropped += 1
            return
        self._seen.add(text)
        super().append(text)

    def extend(self, items: Iterable[Any]) -> None:  # type: ignore[override]
        for item in items or ():
            self.append(item)

    @property
    def truncated(self) -> bool:
        """Whether any entry was suppressed by the cap."""
        return self.dropped > 0


class BoundedList(list):
    """A capped list of arbitrary items, preserving insertion order.

    Unlike :class:`BoundedStrings` this does not deduplicate, because the items
    are usually structured records (quarantine details, reconcile samples) whose
    equality is not a useful signal. It simply refuses to grow past ``cap`` and
    counts what it dropped.
    """

    __slots__ = ("cap", "dropped")

    def __init__(self, items: Iterable[Any] | None = None, *, cap: int = 500) -> None:
        super().__init__()
        self.cap = max(1, int(cap))
        self.dropped = 0
        if items:
            self.extend(items)

    def append(self, item: Any) -> None:  # type: ignore[override]
        if len(self) >= self.cap:
            self.dropped += 1
            return
        super().append(item)

    def extend(self, items: Iterable[Any]) -> None:  # type: ignore[override]
        for item in items or ():
            self.append(item)

    @property
    def truncated(self) -> bool:
        return self.dropped > 0
