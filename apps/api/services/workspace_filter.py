"""Row filter by owning workspace — the store-side half of the tenant scope.

Pure and dependency-free so every store (jobs, schedules, audit, usage, DLQ)
can take one without import cycles. ``services.tenant_scope`` builds it from
the request; ``None`` in a store signature means system scope (no filter).
"""

from __future__ import annotations

import contextlib
import contextvars
import re
from dataclasses import dataclass
from typing import Any, Iterator


def norm_workspace_id(workspace_id: Any) -> str:
    """``""`` for every spelling of "no workspace" (empty, None, missing)."""
    return str(workspace_id or "").strip()


#: Reserved ``X-Workspace-Id`` values naming a platform administrator's views,
#: not a workspace (D218). Workspace ids are uuid4, so neither collides.
#: ``__all__`` is every workspace (the same as sending no header);
#: ``__unassigned__`` is the legacy rows that carry no workspace at all, which
#: otherwise had no switcher entry once the UI always names a workspace.
ALL_WORKSPACES = "__all__"
UNASSIGNED_WORKSPACE = "__unassigned__"
WORKSPACE_VIEWS = frozenset({ALL_WORKSPACES, UNASSIGNED_WORKSPACE})


def stored_workspace_id(header_value: Any) -> str:
    """The workspace id a header value names as rows store it: a view names
    none (``""``), so it is never stamped on, or looked up as, a row."""
    ws = norm_workspace_id(header_value)
    return "" if ws in WORKSPACE_VIEWS else ws


_BLANK = re.compile(r"^\s+$")


@dataclass(frozen=True)
class WorkspaceFilter:
    """Store-level row filter. ``workspace_ids=None`` means every workspace."""

    workspace_ids: frozenset[str] | None
    include_unscoped: bool

    @classmethod
    def everything(cls) -> "WorkspaceFilter":
        return cls(None, True)

    @classmethod
    def nothing(cls) -> "WorkspaceFilter":
        return cls(frozenset(), False)

    @classmethod
    def only(cls, workspace_id: str | None) -> "WorkspaceFilter":
        """Exactly one workspace; ``""`` selects only the unscoped rows."""
        ws = norm_workspace_id(workspace_id)
        return cls(frozenset({ws}), False) if ws else cls(frozenset(), True)

    def intersect(self, other: "WorkspaceFilter | None") -> "WorkspaceFilter":
        """Rows both filters allow; ``None`` (system scope) narrows nothing."""
        if other is None:
            return self
        if self.workspace_ids is None:
            ids = other.workspace_ids
        elif other.workspace_ids is None:
            ids = self.workspace_ids
        else:
            ids = self.workspace_ids & other.workspace_ids
        return WorkspaceFilter(ids, self.include_unscoped and other.include_unscoped)

    @property
    def is_everything(self) -> bool:
        return self.workspace_ids is None and self.include_unscoped

    def matches(self, resource_workspace_id: Any) -> bool:
        ws = norm_workspace_id(resource_workspace_id)
        if not ws:
            return self.include_unscoped
        return self.workspace_ids is None or ws in self.workspace_ids

    def mongo_query(self, field: str = "workspace_id") -> dict[str, Any]:
        """The same predicate as :meth:`matches` for a Mongo ``find``.

        ``$in``/``$nin`` with ``None`` also match a missing field, which is how
        the oldest rows were written. A whitespace-only id is "no workspace"
        to :func:`norm_workspace_id` too; without the regex arm no Mongo query
        matched it, so such a row was visible to nobody and the legacy backfill
        never found it (D218).
        """
        if self.is_everything:
            return {}
        unscoped: list[Any] = ["", None, _BLANK]
        if self.workspace_ids is None:
            return {field: {"$nin": unscoped}}
        arms: list[dict[str, Any]] = [{field: {"$in": sorted(self.workspace_ids)}}]
        if self.include_unscoped:
            arms.append({field: {"$in": unscoped}})
        return arms[0] if len(arms) == 1 else {"$or": arms}


# The filter of the request (or Pilot/MCP turn) being served. Stores that can
# be reached without a request in hand (connectors, Pilot tool reads) consult
# it so a caller that forgot to pass a filter still cannot read another
# workspace's rows. Nothing bound = system scope (schedulers, workers, scripts).
_bound: contextvars.ContextVar[WorkspaceFilter | None] = contextvars.ContextVar(
    "bound_workspace_filter", default=None
)


# The workspace a row written during this request belongs to ("" = unscoped).
_bound_stamp: contextvars.ContextVar[str] = contextvars.ContextVar(
    "bound_workspace_stamp", default=""
)


# Who is acting ("" = nobody bound: the single operator, or system code).
_bound_actor: contextvars.ContextVar[str] = contextvars.ContextVar(
    "bound_workspace_actor", default=""
)


# Why this request has no workspace to write into (the refusal the route
# would answer), or None when the stamp is usable. A request in a platform
# admin's view (Unassigned / All with no workspace of their own) used to bind
# stamp "" and every store that stamps rows wrote them into the legacy bucket
# (D218). The stores read the stamp through :func:`current_workspace_stamp`,
# so the one seam raises instead.
_bound_stamp_refusal: contextvars.ContextVar[BaseException | None] = contextvars.ContextVar(
    "bound_workspace_stamp_refusal", default=None
)


@contextlib.contextmanager
def bound_workspace_filter(
    workspace_filter: WorkspaceFilter | None,
    stamp: str = "",
    actor: str = "",
    stamp_refusal: BaseException | None = None,
) -> Iterator[None]:
    """``stamp_refusal`` is raised by every write-side stamp read inside the
    block; ``None`` keeps the refusal of the enclosing bind (a nested
    connector-scoped bind must not reopen a refused write)."""
    token = _bound.set(workspace_filter)
    stamp_token = _bound_stamp.set(stamp)
    actor_token = _bound_actor.set(actor)
    refusal_token = _bound_stamp_refusal.set(
        stamp_refusal if stamp_refusal is not None else _bound_stamp_refusal.get()
    )
    try:
        yield
    finally:
        _bound_stamp_refusal.reset(refusal_token)
        _bound_actor.reset(actor_token)
        _bound_stamp.reset(stamp_token)
        _bound.reset(token)


def current_actor() -> str:
    """The authenticated actor of this request, for state that must not be
    shared between accounts (Pilot working memory)."""
    return _bound_actor.get()


def current_workspace_stamp(*, for_write: bool = True) -> str:
    """The workspace for a row this request writes into a store that has no
    explicit workspace parameter (preflight runs, plans, drafts).

    A bound request that has no workspace to write into raises its refusal
    here, so no store stamps ``""`` and lands a new row in the legacy bucket
    (D218). ``for_write=False`` is the read ("which workspace does this turn
    act in"): ``""`` for such a request.
    """
    if for_write:
        refusal = _bound_stamp_refusal.get()
        if refusal is not None:
            raise refusal
    return _bound_stamp.get()


def current_workspace_filter() -> WorkspaceFilter | None:
    return _bound.get()


def bound_allows(workspace_id: Any) -> bool:
    """Whether the bound filter covers a workspace (always, when none is bound)."""
    visible = _bound.get()
    return visible is None or visible.matches(workspace_id)

