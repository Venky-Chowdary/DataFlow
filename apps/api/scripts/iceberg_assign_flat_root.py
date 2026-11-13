#!/usr/bin/env python3
"""Assign an Iceberg catalog or warehouse from before the per-workspace partition to one workspace.

The Iceberg local root (``DATAFLOW_ICEBERG_ROOT``, else ``<data_dir>/iceberg``
under confinement) is divided per workspace: new catalogs and tables live in
``<root>/_workspaces/<workspace>/``. An entry in the flat part of the root
(``<root>/<name>``) opens only for platform callers until the operator assigns
it to the workspace that owns it; it then opens for that workspace only. The
files are not moved (Iceberg metadata records absolute file locations).

    python scripts/iceberg_assign_flat_root.py --list
    python scripts/iceberg_assign_flat_root.py --assign <name> --workspace <workspace id>
    python scripts/iceberg_assign_flat_root.py --revoke <name>

``--list`` prints each flat entry, the workspace it is assigned to, and the
workspaces of saved Iceberg connectors that name it (a hint; the operator
decides). Assign an entry only to the workspace whose data it holds: a SQLite
catalog file and the warehouse folder its tables live in are two entries when
they sit side by side in the root.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _claims(root: Path) -> dict[str, set[str]]:
    """Flat entry -> workspaces of the saved Iceberg connectors that name it (as before the partition)."""
    from services.connector_store import list_connectors
    from services.iceberg_workspace_root import _entry, _local_paths
    from services.local_file_policy import is_within
    from services.workspace_filter import bound_workspace_filter
    from src.transfer.connector_capabilities import resolve_driver_type

    with bound_workspace_filter(None):  # every workspace's connectors, for the hint only
        connectors = list_connectors()
    claims: dict[str, set[str]] = {}
    for connector in connectors:
        if resolve_driver_type(str(getattr(connector, "type", "") or "").lower()) != "iceberg":
            continue
        fields: dict[str, Any] = {
            key: getattr(connector, key, None)
            for key in ("type", "connection_string", "warehouse", "database", "region", "extra")
        }
        for _label, raw in _local_paths(fields):
            text = raw[len("file://"):] if raw.lower().startswith("file://") else raw
            candidate = Path(text).expanduser()
            resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
            entry = _entry(root, resolved) if is_within(resolved, root) else ""
            if entry:
                claims.setdefault(entry, set()).add(str(getattr(connector, "workspace_id", "") or "-"))
    return claims


def main(argv: list[str] | None = None) -> int:
    from services.iceberg_workspace_root import (
        _root,
        assign_flat_entry,
        flat_entries,
        flat_owners,
        revoke_flat_entry,
    )

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--list", action="store_true", help="list the flat entries and their assignment")
    action.add_argument("--assign", metavar="NAME", help="a top-level entry of the flat part of the root")
    action.add_argument("--revoke", metavar="NAME", help="remove an entry's assignment")
    parser.add_argument("--workspace", default="", help="the workspace id (with --assign)")
    args = parser.parse_args(argv)
    try:
        root = _root(None)
        if args.list:
            owners = flat_owners(root)
            claims = _claims(root)
            for name in flat_entries(root):
                named_by = ", ".join(sorted(claims.get(name, set()))) or "-"
                print(f"{name}\tassigned={owners.get(name, '-')}\tnamed_by={named_by}")
            return 0
        if args.assign:
            assign_flat_entry(args.assign, args.workspace, root)
            print(f"assigned {args.assign} to workspace {args.workspace}")
            return 0
        removed = revoke_flat_entry(args.revoke, root)
        print(f"revoked {args.revoke}" if removed else f"{args.revoke} was not assigned")
        return 0
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
