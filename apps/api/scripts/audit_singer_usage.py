"""Read-only: which saved connectors are Singer taps, and what uses them?

Singer taps run executables on the API host, so they stay disabled until the
operator sets DATAWRAP_SINGER_TAPS_DIR and are usable by the platform admin
only. Run this to see who is affected before (or after) that rollout:

    python scripts/audit_singer_usage.py
    python scripts/audit_singer_usage.py --file connectors.json   # schedules: <DATAFLOW_DATA_DIR>/schedules.json
    MONGO=... python scripts/audit_singer_usage.py --mongo-url-env MONGO

It lists every saved connector of type ``singer_tap`` (workspace, name, the tap
executable if one is recorded, and whether the taps directory can run it today)
and every schedule/pipeline that reads from or writes to one (name, workspace,
enabled, cadence). It never writes, never starts a tap, and never prints
``tap_config`` or any secret.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import audit_common

SINGER_TYPE = "singer_tap"


def _tap_name(connector: Any) -> str:
    command = (getattr(connector, "extra", None) or {}).get("tap_command") or ""
    if isinstance(command, (list, tuple)):
        command = command[0] if command else ""
    parts = str(command).split()
    return os.path.basename(parts[0]) if parts else ""


def _runnable(tap: str) -> str:
    """Why a tap can or cannot run on this server right now."""
    from services.request_principal import SingerUnavailable, singer_taps_dir

    try:
        root = singer_taps_dir()
    except SingerUnavailable as exc:
        return exc.code
    if not tap:
        return "no tap_command recorded"
    if not tap.lower().startswith("tap-"):
        return "not a tap-* executable"
    return "ok" if os.access(os.path.join(root, tap), os.X_OK) else "tap not found in taps dir"


def singer_usage(connectors: list[Any], schedules: list[Any]) -> list[dict[str, Any]]:
    rows = []
    for connector in connectors:
        if str(connector.type or "").strip().lower() != SINGER_TYPE:
            continue
        tap = _tap_name(connector)
        used_by = [
            {
                "schedule": s.name,
                "id": s.id,
                "workspace": s.workspace_id or "(legacy)",
                "enabled": bool(s.enabled),
                "interval": s.interval,
                "role": "source" if s.source_connector_id == connector.id else "destination",
            }
            for s in schedules
            if connector.id in (s.source_connector_id, s.dest_connector_id)
        ]
        rows.append(
            {
                "name": connector.name,
                "id": connector.id,
                "workspace": connector.workspace_id or "(legacy)",
                "tap": tap,
                "runnable": _runnable(tap),
                "used_by": used_by,
            }
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    audit_common.add_store_args(parser)
    args = parser.parse_args(argv)

    connectors = audit_common.load_saved_connectors(args)
    from services import schedule_store

    schedules = [
        s
        for s in schedule_store.list_schedules()
        if not args.workspace or s.workspace_id == args.workspace
    ]
    rows = singer_usage(connectors, schedules)
    if args.json:
        print(json.dumps({"singer_connectors": len(rows), "rows": rows}, indent=2))
        return 0
    for r in rows:
        print(f"{r['workspace']:<24} {r['name']:<32} tap={r['tap'] or '-':<20} runnable: {r['runnable']}")
        for u in r["used_by"]:
            print(
                f"    {u['role']:<11} schedule {u['schedule']!r} ({u['workspace']}) "
                f"{'enabled' if u['enabled'] else 'paused'}, {u['interval']}"
            )
    pipelines = sum(len(r["used_by"]) for r in rows)
    print(f"\n{len(rows)} Singer connectors, used by {pipelines} schedules/pipelines")
    return 0


if __name__ == "__main__":
    sys.exit(main())
