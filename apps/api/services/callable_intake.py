"""One intake step for "is this source a table, a query or a procedure?".

QA C3 (third review): only the form route (``/transfer/run``) resolved
query/procedure placements. ``/transfer/execute``, a schedule fire, preflight
and chat Confirm read the endpoint as sent, so ONE stream contract carrying
``source_read_mode: "query"`` (or ``"call"`` with an empty statement) copied
the whole ``source.table``, and an unknown mode word reached ~40 unguarded
``is_callable_source`` calls as HTTP 500.

:func:`apply_callable_intake` is that step for every route. It resolves
placements with :func:`services.procedure_source.resolve_callable_placements`:
one selected contract's CALL/SELECT moves onto the endpoint, a mode word goes
through the one alias table, and a word with no statement is refused. What it
returns has been validated, so later ``source_read_mode_of`` calls never see
an invalid mode. ``job_submission.create_pending_job`` runs it again, so a
route that skips it still cannot queue an unresolved source. Running it
twice changes nothing.

It raises :class:`~services.procedure_source.CallablePlacementError` (a
``ProcedureSourceError``). Routes answer that with 422. The scheduler parks
the schedule with the message.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

#: Keys the intake writes back onto a mapping-shaped endpoint (preflight's
#: ``source_config``). Everything else in it is left exactly as sent.
_CALLABLE_KEYS = ("source_read_mode", "source_query", "procedure_call", "procedure_params")


def apply_callable_intake(
    target: Any,
    stream_contracts: list[Any] | None = None,
    *,
    form: Mapping[str, Any] | None = None,
    source_kind: str | None = None,
) -> list[Any] | None:
    """Resolve and validate query/procedure placements in place.

    ``target`` is a ``TransferRequest`` (its ``source`` and ``stream_contracts``
    are updated), an ``EndpointConfig``, or a mutable endpoint mapping such as
    preflight's ``source_config``. Returns the resolved stream contracts.
    """
    from services.procedure_source import resolve_callable_placements

    if hasattr(target, "source") and hasattr(target, "stream_contracts"):
        contracts = apply_callable_intake(
            target.source,
            target.stream_contracts if stream_contracts is None else stream_contracts,
            form=form,
            source_kind=source_kind,
        )
        if contracts is not None:
            target.stream_contracts = contracts
        return contracts
    if isinstance(target, MutableMapping):
        nested = target.get("extra") if isinstance(target.get("extra"), MutableMapping) else None
        view = {**dict(nested or {}),
                **{k: target[k] for k in _CALLABLE_KEYS if k in target}}
        kind = source_kind or str(target.get("kind") or "database")
        extra, contracts = resolve_callable_placements(
            form=form, source_extra=view, stream_contracts=stream_contracts, source_kind=kind,
        )
        for key in _CALLABLE_KEYS:
            if key in extra:
                target[key] = extra[key]
                if nested is not None:
                    nested[key] = extra[key]
        return contracts
    kind = source_kind or str(getattr(target, "kind", "") or "database")
    extra, contracts = resolve_callable_placements(
        form=form,
        source_extra=getattr(target, "extra", None) or {},
        stream_contracts=stream_contracts,
        source_kind=kind,
    )
    target.extra = extra
    return contracts
