"""D276 (review): a SQLite URL with a DBAPI suffix is SQLite on the hosted cloud too.

``_is_sqlite_destination`` matched only ``sqlite:``, so a generic SQL connector
pointed at ``sqlite+pysqlite:////tmp/qa.db`` or ``sqlite+aiosqlite:...`` was
saved, tested, listed and written on the hosted cloud, opening a server path.
The one predicate guards the save, the connection test (connector_probe), the
listing (endpoint_intelligence) and the run, so each is driven here.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.test_d276_sqlite_destination_not_on_hosted_cloud import (  # noqa: F401  (fixtures)
    REFUSAL,
    _hosted,
    _not_hosted,
    _run,
    _seed,
    _tables,
    fake_store,
)
from tests.test_u1_d425_connector_save_refusals_http import _detail, _saved_rows, client  # noqa: F401

DRIVERS = ["sqlite+pysqlite", "sqlite+aiosqlite", "SQLite+PySQLite"]


def _url(driver: str, path) -> str:
    return f"{driver}:///{path}"


@pytest.fixture
def opened(monkeypatch):
    seen: list[str] = []
    real = sqlite3.connect

    def _spy(*args, **kwargs):
        seen.append(str(args[0] if args else kwargs.get("database")))
        return real(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", _spy)
    return seen


@pytest.mark.parametrize("driver", DRIVERS)
def test_save_refuses_it(client, monkeypatch, tmp_path, driver):  # noqa: F811
    _hosted(monkeypatch)
    r = client.post("/api/v1/connectors/saved", json={
        "name": "d276-url", "type": "generic_sql", "role": "both",
        "connection_string": _url(driver, tmp_path / "qa.db"), "host": "", "port": 0,
    })
    assert r.status_code == 422, r.text
    assert "SQLite isn't available" in _detail(r)
    assert _saved_rows() == []


@pytest.mark.parametrize("driver", DRIVERS)
def test_connection_test_of_a_saved_connector_refuses_it_and_opens_nothing(
    client, monkeypatch, tmp_path, opened, driver,  # noqa: F811
):
    from services.connector_store import create_connector

    path = tmp_path / "saved.db"
    _seed(path)
    _not_hosted(monkeypatch)
    conn = create_connector({"name": "d276-saved", "type": "generic_sql", "role": "both",
                             "connection_string": _url(driver, path), "workspace_id": ""})
    _hosted(monkeypatch)
    opened.clear()
    r = client.post(f"/api/v1/connectors/saved/{conn.id}/test")
    assert r.status_code == 200, r.text
    assert r.json()["success"] is False and "SQLite isn't available" in r.json()["message"], r.json()
    assert not [p for p in opened if "saved.db" in p], opened


@pytest.mark.parametrize("driver", DRIVERS)
def test_listing_refuses_it_and_opens_nothing(monkeypatch, tmp_path, opened, driver):
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    path = tmp_path / "listed.db"
    _seed(path)
    _hosted(monkeypatch)
    opened.clear()
    out = introspect_endpoint(EndpointConfig.from_dict(
        "database", {"format": "generic_sql", "connection_string": _url(driver, path), "table": "customers"}))
    assert out.get("connected") is False, out
    assert "SQLite isn't available" in str(out.get("message") or out.get("error") or ""), out
    assert not [p for p in opened if "listed.db" in p], opened


@pytest.mark.parametrize("driver", DRIVERS)
def test_a_run_into_it_is_refused_before_any_row(tmp_path, fake_store, monkeypatch, driver):  # noqa: F811
    from src.transfer.models import EndpointConfig

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _seed(src)
    _hosted(monkeypatch)
    _job, result = _run(fake_store, EndpointConfig(
        kind="database", format="generic_sql", connection_string=_url(driver, dst), table="customers"), src)
    assert not result.success
    assert REFUSAL in str(result.error), result.error
    assert _tables(dst) == []
