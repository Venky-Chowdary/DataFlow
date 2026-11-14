"""What a cancelled multi-stream run left in each stream's own destination object.

D199 / C16 count the destination once a cancelled run has stopped, against the
count it held before the run. A multi-stream run writes stream after stream,
each into its own object, but the measurement saw one object: the route that
saw the cancel counted the stream it was writing (the streams that had already
committed were left out, so two whole tables read "No row of this run was
kept"), and a route that did not count fell back to the request's primary
table against ``cancel_rows_before`` — which ``setdefault`` had pinned to the
first stream's pre-run count. The overwrite pin (``overwrite_rows_before``)
was likewise counted on the first stream and reused for every later one.

Each stream now pins its pre-run count under its own destination object
(:func:`pin_rows_before`), the previous stream's single-object pin is dropped
before the next stream starts (:func:`forget_previous_stream_pins`), and the
cancel measures every object the run reached against its own count
(:func:`measure_multi_stream_cancel`).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

logger = logging.getLogger(__name__)

#: ``destination.extra`` key: ``{object: {"rows_before": n, "sync_mode": s}}``
#: in the order the streams reached their objects.
ROWS_BEFORE_KEY = "stream_rows_before"

#: Stamps on ``destination.extra`` that describe one stream's object only.
_SINGLE_OBJECT_PINS = ("overwrite_rows_before",)

_COUNTS = (
    "destination_rows_now",
    "destination_rows_before",
    "destination_rows_by_run",
    "destination_table_exists",
)


def pin_rows_before(cfg: Any, source_name: str, rows: int) -> None:
    """Record ``rows`` as the pre-run count of the object this stream writes.

    ``cfg`` is the stream's run config (destination, driver type, sync mode).
    The first count per object wins, as ``cancel_rows_before`` always did: a
    resumed or retried pass already holds rows this job wrote. That
    single-object key is still set for single-stream routes (C16; the
    write-failure undo keeps reading ``dest_rows_before``).
    """
    from src.transfer.adapters import resolve_dest_table

    destination = getattr(cfg, "destination", None)
    extra = getattr(destination, "extra", None)
    if not isinstance(extra, dict):
        return
    extra.setdefault("cancel_rows_before", int(rows))
    table = str(resolve_dest_table(cfg.dest_type, destination, source_name) or "")
    if not table:
        return
    per = extra.get(ROWS_BEFORE_KEY)
    if not isinstance(per, dict):
        per = extra[ROWS_BEFORE_KEY] = {}
    per.setdefault(
        table, {"rows_before": int(rows), "sync_mode": str(getattr(cfg, "effective_sync", "") or "")}
    )


def forget_previous_stream_pins(destination: Any) -> None:
    """Drop the pre-run stamps the previous stream left for its own object.

    ``overwrite_rows_before`` is pinned once, by the first clear; left in
    place, every later stream of a multi-stream overwrite read the first
    stream's pre-run count as its own.
    """
    extra = getattr(destination, "extra", None)
    if isinstance(extra, dict):
        for key in _SINGLE_OBJECT_PINS:
            extra.pop(key, None)


def _sum(values: list[Any]) -> int | None:
    if not values or any(not isinstance(v, int) for v in values):
        return None
    return sum(values)


def measure_multi_stream_cancel(exc: BaseException, request: Any) -> bool:
    """Count every stream object a cancelled multi-stream run reached (never raises).

    See :func:`_measure`. A destination that cannot be read keeps the route's
    own figures: the failure path must still record the cancel.
    """
    try:
        return _measure(exc, request)
    except Exception:  # noqa: BLE001 - the cancel is recorded either way
        logger.warning("multi-stream cancel measurement unavailable", exc_info=True)
        return False


def _measure(exc: BaseException, request: Any) -> bool:
    """Count every stream object a cancelled multi-stream run reached.

    Each object is measured against its own pre-run count. A stream that
    finished had replaced (overwrite) or extended (append) its object; the last
    one reached is the stream the cancel stopped, measured as a single-stream
    cancel is — by its route when the route counted it. The totals replace the
    route's single-object figures on ``exc`` (an unknown total removes them);
    ``exc.destination_streams`` keeps the breakdown. Returns ``False`` for a
    single-stream run or when no stream pinned a count, so the caller keeps the
    single-object measurement.
    """
    from services.job_status import measure_cancel_landing
    from services.sync_cursor import resolve_selected_sync_contracts
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type
    from src.transfer.engine_family import is_schemaless_family

    destination = getattr(request, "destination", None)
    if str(getattr(destination, "kind", "") or "") != "database":
        return False
    if len(resolve_selected_sync_contracts(getattr(request, "stream_contracts", None))) < 2:
        return False
    per = (getattr(destination, "extra", None) or {}).get(ROWS_BEFORE_KEY)
    if not isinstance(per, dict) or not per:
        return False
    dest_type = resolve_driver_type(str(getattr(destination, "format", "") or ""))
    dest_cfg = resolve_connector_config(destination)
    write_mode = str(getattr(request, "write_mode", "") or "")
    route_counted = getattr(exc, "destination_rows_now", None) is not None
    tables = list(per)
    streams: list[dict[str, Any]] = []
    for index, table in enumerate(tables):
        pinned = per[table] if isinstance(per[table], dict) else {}
        stopped = index == len(tables) - 1
        if stopped and route_counted:
            probe: Any = exc
        else:
            probe = SimpleNamespace(
                write_rolled_back=stopped and getattr(exc, "write_rolled_back", None) is True
            )
            measure_cancel_landing(
                probe,
                dest_type=dest_type,
                dest_cfg=dest_cfg,
                table=table,
                sync_mode=str(pinned.get("sync_mode") or getattr(request, "sync_mode", "") or ""),
                rows_before=pinned.get("rows_before"),
                table_existed_before=None,
                write_mode=write_mode,
                # A finished stream's overwrite was applied: every row there is
                # this run's. The stopped one is judged like a single stream.
                atomic_overwrite=not is_schemaless_family(dest_type) if stopped else False,
                overwrite_applied=None if stopped else True,
            )
        before = getattr(probe, "destination_rows_before", None)
        streams.append(
            {
                "table": table,
                "rows_before": before if isinstance(before, int) else pinned.get("rows_before"),
                "rows_now": getattr(probe, "destination_rows_now", None),
                "rows_by_run": getattr(probe, "destination_rows_by_run", None),
                "table_exists": getattr(probe, "destination_table_exists", None),
            }
        )
    totals = {
        "destination_rows_now": _sum([s["rows_now"] for s in streams]),
        "destination_rows_before": _sum([s["rows_before"] for s in streams]),
        "destination_rows_by_run": _sum([s["rows_by_run"] for s in streams]),
        "destination_table_exists": False
        if all(s["table_exists"] is False for s in streams)
        else None,
    }
    for key in _COUNTS:
        if totals[key] is None:
            exc.__dict__.pop(key, None)
        else:
            setattr(exc, key, totals[key])
    exc.destination_streams = [  # type: ignore[attr-defined]
        {k: v for k, v in s.items() if v is not None} for s in streams
    ]
    return True
