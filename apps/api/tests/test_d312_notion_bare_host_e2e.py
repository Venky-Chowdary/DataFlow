"""D312: a Notion connector saved with the bare host ``api.notion.com``.

QA: with host ``api.notion.com`` the probe URL became
``https://api.notion.com/search``, a route Notion does not serve; every Notion
endpoint is under ``/v1`` (https://developers.notion.com/reference/intro).
PR #433 (cf2b30f1b) keeps the manifest's ``/v1`` path when the form names only
the Notion origin. This proves it on the operator's spellings and on every
path that reads the connector (test, sample, plan), not only on the SDK
constructor: each must call ``POST https://api.notion.com/v1/...``.

Notion's REST API is emulated at the ``requests`` layer by the D311 fake
(POST-only reads, 400 on any other path).
"""

from __future__ import annotations

import pytest

# Built at import from the environment; the D311 harness then sets
# DATAFLOW_SEED_DEMO="" for the app, which Settings cannot parse as a bool.
import services.config  # noqa: F401,E402
from tests.test_d311_notion_plan_reads_database_rows import (  # noqa: F401 - fixtures
    _DB_ID,
    _TOKEN,
    client,
    notion,
)

HOSTS = ["api.notion.com", "api.notion.com/", "https://api.notion.com",
         "https://api.notion.com/", "https://api.notion.com/v1", "https://api.notion.com/v1/"]


def _assert_v1_only(api) -> None:
    urls = [u.split("?")[0] for _m, u, _b in api.calls]
    assert urls, "no Notion call was made"
    assert all(u.startswith("https://api.notion.com/v1/") for u in urls), urls
    assert not any("/v1/v1/" in u for u in urls), urls


@pytest.mark.parametrize("host", HOSTS)
def test_probe_calls_v1_search_for_every_host_spelling(client, notion, host):
    from src.transfer.connector_registry import run_probe

    api = notion(1)
    ok, msg = run_probe("notion", {"type": "notion", "host": host, "port": 443,
                                   "api_key": _TOKEN, "database": _DB_ID})
    assert ok, msg
    _assert_v1_only(api)
    assert ("POST", "https://api.notion.com/v1/search") in [(m, u) for m, u, _ in api.calls]


@pytest.mark.parametrize("host", ["api.notion.com", "https://api.notion.com/v1/"])
def test_sample_and_plan_read_v1_with_a_bare_host(client, notion, tmp_path, host):
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.query_tools import sample_connector_object
    from src.ai.copilot.transfer_tools import plan_transfer

    api = notion(30)
    src = create_connector({
        "name": "d312-notion", "type": "notion", "role": "source", "workspace_id": "",
        "host": host, "database": _DB_ID, "auth_mode": "api_key", "api_key": _TOKEN,
    })
    dst = create_connector({"name": "d312-sqlite", "type": "sqlite", "role": "destination",
                            "workspace_id": "", "database": str(tmp_path / "d312.db")})
    try:
        sampled = sample_connector_object(connector_id=src.id, table="database_rows", limit=10)
        assert sampled.success, sampled.error
        assert sampled.output["row_count"] == 10
        plan = plan_transfer(source_connector_id=src.id, source_table="database_rows",
                             dest_connector_id=dst.id, dest_table="notion_rows",
                             sync_mode="full_refresh_overwrite")
        assert plan.success, plan.error
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
    _assert_v1_only(api)
