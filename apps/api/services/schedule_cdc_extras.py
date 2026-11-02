"""CDC Advanced extras on a schedule — same knobs Destination Advanced sends.

Studio Execute stamps these onto endpoint ``extra``. A scheduled beat that
omits them silently diverges: append-only CDC becomes a PK-upsert refusal,
SQL Server row filter falls back to ``all``, and Always On reconnect skips
``MultiSubnetFailover``.
"""

from __future__ import annotations

from typing import Any

CDC_ROW_FILTERS = frozenset({"all", "all update old", "net"})


def named_cdc_row_filter(raw: Any) -> str:
    value = str(raw or "").strip().lower().replace("_", " ").replace("-", " ")
    value = " ".join(value.split())
    if value in {"all update old", "allupdateold"}:
        return "all update old"
    return value if value in CDC_ROW_FILTERS else "all"


def schedule_cdc_extras(
    sync_mode: str,
    *,
    allow_append_only: Any = False,
    cdc_row_filter: Any = "",
    multi_subnet_failover: Any = False,
) -> dict[str, Any]:
    cdc = str(sync_mode or "").strip().lower() == "cdc"
    if not cdc:
        return {
            "allow_append_only": False,
            "cdc_row_filter": "",
            "multi_subnet_failover": False,
        }
    return {
        "allow_append_only": bool(allow_append_only),
        "cdc_row_filter": named_cdc_row_filter(cdc_row_filter),
        "multi_subnet_failover": bool(multi_subnet_failover),
    }


def apply_cdc_schedule_extras(source: Any, destination: Any, sched: Any) -> None:
    extras = schedule_cdc_extras(
        getattr(sched, "sync_mode", ""),
        allow_append_only=getattr(sched, "allow_append_only", False),
        cdc_row_filter=getattr(sched, "cdc_row_filter", ""),
        multi_subnet_failover=getattr(sched, "multi_subnet_failover", False),
    )
    dest_extra = dict(getattr(destination, "extra", None) or {})
    src_extra = dict(getattr(source, "extra", None) or {})
    if extras["allow_append_only"]:
        dest_extra["allow_append_only"] = True
    filter_value = extras["cdc_row_filter"]
    if filter_value and filter_value != "all":
        src_extra["cdc_row_filter"] = filter_value
    if extras["multi_subnet_failover"]:
        src_extra["multi_subnet_failover"] = True
    destination.extra = dest_extra
    source.extra = src_extra
