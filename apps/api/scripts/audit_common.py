"""Shared plumbing for the read-only saved-connector audit scripts.

``audit_egress_impact.py``, ``audit_singer_usage.py`` and
``audit_sqlite_paths.py`` only read: they call ``list_connectors()`` (and, for
Singer, the schedule store's list calls) and print. Nothing here writes to a
store, a file, or a database. The one side effect: importing the app's stores
creates the (empty) data directory if it does not exist yet.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

API = Path(__file__).resolve().parents[1]
for _path in (API, API / "src"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


def add_store_args(parser: argparse.ArgumentParser) -> None:
    """``--file`` / ``--mongo-url-env`` choose the store; neither means "as the app is configured"."""
    parser.add_argument(
        "--file",
        help="connector store JSON file (same as DATAFLOW_CONNECTOR_STORE)",
    )
    parser.add_argument(
        "--mongo-url-env",
        metavar="VAR",
        help="read connectors from the MongoDB whose URL is in environment variable VAR",
    )
    parser.add_argument("--workspace", help="limit to one workspace id")
    parser.add_argument("--json", action="store_true", help="print JSON instead of a table")


def set_env(name: str, value: str) -> None:
    """Set ``DATAFLOW_<suffix>`` (dropping a ``DATAWRAP_<suffix>`` that would shadow it)."""
    suffix = name.split("_", 1)[1]
    os.environ.pop(f"DATAWRAP_{suffix}", None)
    os.environ[f"DATAFLOW_{suffix}"] = value


def configure_store(file: str | None = None, mongo_url_env: str | None = None) -> None:
    """Point the connector store at ``file`` or at the Mongo URL held in ``$mongo_url_env``."""
    if file and mongo_url_env:
        raise SystemExit("choose one of --file and --mongo-url-env")
    if file:
        set_env("DATAFLOW_CONNECTOR_STORE", file)
        set_env("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    elif mongo_url_env:
        url = os.environ.get(mongo_url_env, "").strip()
        if not url:
            raise SystemExit(f"environment variable {mongo_url_env} is not set")
        os.environ["MONGODB_URI"] = url
        set_env("DATAFLOW_CONNECTOR_STORE_BACKEND", "mongo")


def load_saved_connectors(args: argparse.Namespace) -> list[Any]:
    """Every saved connector (system scope), optionally for one workspace."""
    configure_store(args.file, args.mongo_url_env)
    from services.connector_store import list_connectors

    connectors = list_connectors()
    if args.workspace:
        connectors = [c for c in connectors if c.workspace_id == args.workspace]
    return sorted(connectors, key=lambda c: (c.workspace_id, c.name))
