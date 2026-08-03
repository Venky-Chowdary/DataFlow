"""Regression: get/show tables from connector must list objects, not sample 'tables'."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_get_tables_from_connector_lists_objects():
    from src.ai.copilot.tools import infer_tools_from_message

    for q in (
        "can you get tables from PostgresVenkat",
        "get tables from PostgresVenkat",
        "show tables from PostgresVenkat",
        "list tables on PostgresVenkat",
    ):
        planned = infer_tools_from_message(q)
        names = [n for n, _ in planned]
        assert "list_connector_objects" in names, q
        assert "sample_connector_object" not in names, (q, planned)
        args = next(a for n, a in planned if n == "list_connector_objects")
        assert "postgresvenkat" in str(args.get("connector_name", "")).lower()


def test_get_users_data_still_samples():
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message("can you get users data from postgres")
    assert planned[0][0] == "sample_connector_object"
    assert planned[0][1].get("table") == "users"
