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

``--list`` prints each flat entry, the workspace it is assigned to, and each
saved Iceberg connector that names it with its workspace (a hint; the operator
decides); a claim from a workspace other than the assigned one is marked
unverified. ``--assign`` refuses a workspace that does not exist. Assign an entry only to the workspace whose data it holds: a SQLite
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


def _claims(root: Path) -> dict[str, list[tuple[str, str]]]:
    """Flat entry -> ``(connector id, its workspace)`` for each saved Iceberg connector naming it
    (read as before the partition)."""
    from services.connector_store import list_connectors
    from services.iceberg_workspace_root import _entry, _local_paths
    from services.local_file_policy import is_within
    from services.workspace_filter import bound_workspace_filter
    from src.transfer.connector_capabilities import resolve_driver_type

    with bound_workspace_filter(None):  # every workspace's connectors, for the hint only
        connectors = list_connectors()
    claims: dict[str, list[tuple[str, str]]] = {}
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
            claim = (str(getattr(connector, "id", "") or "?"), str(getattr(connector, "workspace_id", "") or "-"))
            if entry and claim not in claims.setdefault(entry, []):
                claims[entry].append(claim)
    return claims


def _shown_claims(claims: list[tuple[str, str]], owner: str) -> str:
    """``connector@workspace``; a claim from another workspace than the owner is unverified (review 5, L2)."""
    shown = []
    for connector_id, workspace in sorted(claims):
        mark = " (unverified: not the assigned workspace)" if owner and workspace != owner else ""
        shown.append(f"{connector_id}@{workspace}{mark}")
    return ", ".join(shown) or "-"


def _workspace_exists(workspace_id: str) -> bool:
    from services.team_store import get_workspace

    return bool(workspace_id) and get_workspace(workspace_id) is not None


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
                owner = owners.get(name, "")
                print(f"{name}\tassigned={owner or '-'}\tclaims={_shown_claims(claims.get(name, []), owner)}")
            return 0
        if args.assign:
            if not _workspace_exists(str(args.workspace or "").strip()):
                raise ValueError("that workspace does not exist; check the id (team workspaces list)")
            assign_flat_entry(args.assign, args.workspace, root)
            print(f"assigned {args.assign} to workspace {args.workspace}")
            others = [c for c in _claims(root).get(args.assign.strip(), []) if c[1] != args.workspace]
            if others:
                print(f"claims from other workspaces: {_shown_claims(others, args.workspace)}")
            return 0
        removed = revoke_flat_entry(args.revoke, root)
        print(f"revoked {args.revoke}" if removed else f"{args.revoke} was not assigned")
        return 0
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
