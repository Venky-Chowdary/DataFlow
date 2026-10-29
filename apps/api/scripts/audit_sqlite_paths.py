"""Read-only: where do saved SQLite/DuckDB connectors point, and are they inside the proposed root?

Production can confine local database files to the persistent volume
(``DATAFLOW_CONFINE_LOCAL_FILES=1``, off by default). That rule refuses a file
anywhere else, and anchors a relative name at the root, so a connector that works
today can point at a different (empty) file after the flag. Run this first:

    python scripts/audit_sqlite_paths.py                     # store as configured
    python scripts/audit_sqlite_paths.py --file connectors.json --data-dir /data
    python scripts/audit_sqlite_paths.py --cwd /app/apps/api --copy-plan > migrate.sh

For each saved ``sqlite`` / ``duckdb`` connector it prints the path the API opens
today (relative names resolve against ``--cwd``, the API's working directory), the
path the flag would open, whether the file exists, and a verdict:
``inside root`` (nothing to do), ``outside root`` / ``relative`` (copy it first),
``service data directory`` (refused with or without the flag), ``in-memory``.
``--copy-plan`` prints the ``mkdir`` / ``sqlite3 .backup`` / ``cp`` commands to put
each file under the proposed root. The script only reads: it never copies,
creates a directory, opens a database or writes to a store. The commands it prints
are for a person to review and run.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

import audit_common

KINDS = ("sqlite", "duckdb")


def _roots(kind: str) -> list[Path]:
    from connectors.sqlite_common import proposed_root

    root, _explicit = proposed_root(kind)
    return [Path(part).expanduser().resolve(strict=False) for part in root.split(os.pathsep) if part.strip()]


def _classify(kind: str, connector: Any, cwd: Path) -> dict[str, Any]:
    from connectors.sqlite_common import duckdb_unconfined_path, sqlite_unconfined_path
    from services.local_file_policy import is_within
    from services.platform_config import data_dir_path

    row: dict[str, Any] = {
        "name": connector.name,
        "id": connector.id,
        "workspace": connector.workspace_id or "(legacy)",
        "type": kind,
        "configured": "",
        "today_path": "",
        "under_flag_path": "",
        "exists": False,
        "verdict": "",
        "needs_copy": False,
    }
    try:
        if kind == "sqlite":
            raw = sqlite_unconfined_path(connector.database, connector.connection_string)
        else:
            raw = duckdb_unconfined_path(connector.database, connector.connection_string, connector.host)
    except ValueError as exc:
        row["verdict"] = f"invalid path: {exc}"
        return row
    row["configured"] = raw
    if not raw or raw == ":memory:":
        row["verdict"] = "in-memory" if raw else "no path"
        return row
    if kind == "duckdb" and raw.lower().startswith(("md:", "motherduck:")):
        row["verdict"] = "MotherDuck (not a local file)"
        return row

    roots = _roots(kind)
    candidate = Path(raw).expanduser()
    relative = not candidate.is_absolute()
    today = (cwd / candidate if relative else candidate).resolve(strict=False)
    under_flag = (roots[0] / candidate if relative else candidate).resolve(strict=False)
    row["today_path"] = str(today)
    row["under_flag_path"] = str(under_flag)
    row["exists"] = today.is_file()

    state_dir = data_dir_path().resolve(strict=False)
    volume = state_dir / kind
    if is_within(today, state_dir) and not is_within(today, volume):
        row["verdict"] = "service data directory (refused with or without the flag)"
    elif not relative and any(is_within(today, r) for r in roots):
        row["verdict"] = "inside root"
    elif relative:
        row["verdict"] = f"relative (the flag opens {under_flag})"
        row["needs_copy"] = row["exists"] and not under_flag.is_file()
    else:
        row["verdict"] = "outside root"
        row["needs_copy"] = row["exists"]
    return row


def audit_rows(connectors: list[Any], cwd: Path) -> list[dict[str, Any]]:
    rows = []
    for connector in connectors:
        kind = str(connector.type or "").strip().lower()
        if kind in KINDS:
            rows.append(_classify(kind, connector, cwd))
    return rows


def copy_plan(rows: list[dict[str, Any]]) -> list[str]:
    """Shell lines (for a person to run) copying each file that needs it under its root."""
    lines = ["#!/bin/sh", "# Review before running. Copies only; originals are left in place.", "set -eu"]
    taken: dict[str, str] = {}
    for row in rows:
        if not row["needs_copy"]:
            continue
        root = _roots(row["type"])[0]
        name = Path(row["today_path"]).name
        target = root / name
        if str(target) in taken and taken[str(target)] != row["today_path"]:
            stem, dot, ext = name.rpartition(".")
            name = f"{stem or name}-{row['id'][:8]}{dot}{ext}" if stem else f"{name}-{row['id'][:8]}"
            target = root / name
        taken[str(target)] = row["today_path"]
        src, dst = shlex.quote(row["today_path"]), shlex.quote(str(target))
        lines.append(f"# connector {row['name']!r} ({row['workspace']}): set database to {name!r} afterwards")
        lines.append(f"mkdir -p {shlex.quote(str(root))}")
        if row["type"] == "sqlite":
            lines.append(f"[ -e {dst} ] || sqlite3 {src} \".backup {shlex.quote(str(target))}\"")
        else:
            lines.append(f"cp -p -n {src} {dst}  # stop jobs writing this DuckDB file first")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    audit_common.add_store_args(parser)
    parser.add_argument("--cwd", default=os.getcwd(), help="the API's working directory (relative paths resolve here)")
    parser.add_argument("--data-dir", help="the API's DATAFLOW_DATA_DIR (proposed roots are <data-dir>/sqlite|duckdb)")
    parser.add_argument("--copy-plan", action="store_true", help="print the copy commands instead of the table")
    args = parser.parse_args(argv)
    if args.data_dir:
        audit_common.set_env("DATAFLOW_DATA_DIR", args.data_dir)

    rows = audit_rows(audit_common.load_saved_connectors(args), Path(args.cwd))
    if args.copy_plan:
        print("\n".join(copy_plan(rows)))
        return 0
    if args.json:
        print(json.dumps({"local_file_connectors": len(rows), "needs_copy": sum(r["needs_copy"] for r in rows), "rows": rows}, indent=2))
        return 0
    for r in rows:
        print(
            f"{r['workspace']:<20} {r['name']:<28} {r['type']:<7} "
            f"{'exists ' if r['exists'] else 'missing'} {r['verdict']}\n"
            f"    today: {r['today_path'] or r['configured'] or '-'}"
        )
    outside = [r for r in rows if r["needs_copy"]]
    print(
        f"\n{len(rows)} local-file connectors; {len(outside)} need their file copied under the "
        "proposed root before DATAFLOW_CONFINE_LOCAL_FILES=1"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
