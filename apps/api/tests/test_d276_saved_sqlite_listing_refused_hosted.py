"""D276: hosted SQLite is refused for an already-saved connector too — list and sample.

QA (Nov 10/16, round 50): create/test on Cloud now refuse SQLite, but an
existing SQLite connector "lists 0 tables" — the listing still resolved and
opened a server path, and the operator saw an empty source instead of the
reason. The review noted list/sample on an already-saved connector were not
checked. This drives the shared introspect path (Studio's source list, the
Query browser and MCP list/sample use it) on a hosted deployment with a
connector that predates the refusal, and proves no SQLite file is opened.
"""

from __future__ import annotations

import sqlite3

import pytest


@pytest.fixture
def hosted(monkeypatch):
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "d276-saved")
    for name in ("DATAWRAP_SQLITE_DESTINATION", "DATAFLOW_SQLITE_DESTINATION",
                 "DATAWRAP_LOCAL_FILE_DESTINATIONS", "DATAFLOW_LOCAL_FILE_DESTINATIONS"):
        monkeypatch.delenv(name, raising=False)


def test_listing_a_saved_sqlite_connector_refuses_with_the_reason_and_opens_nothing(hosted, monkeypatch, tmp_path):
    from services.local_file_policy import sqlite_destination_allowed
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    assert sqlite_destination_allowed() is False
    path = tmp_path / "qav_test.db"
    sqlite3.connect(str(path)).close()
    opened: list[str] = []
    real_connect = sqlite3.connect

    def _spy(*args, **kwargs):
        opened.append(str(args[0] if args else kwargs.get("database")))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", _spy)
    endpoint = EndpointConfig.from_dict("database", {"format": "sqlite", "database": str(path), "table": "t"})
    out = introspect_endpoint(endpoint)
    assert out.get("connected") is False, out
    text = str(out.get("message") or out.get("error") or "")
    assert "SQLite isn't available" in text and "Cloud" in text, out
    assert not out.get("objects")
    assert not [p for p in opened if "qav_test" in p], opened
