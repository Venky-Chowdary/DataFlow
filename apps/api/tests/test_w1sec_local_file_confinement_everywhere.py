"""SEC-9: SQLite/DuckDB local-file confinement is on by default in every environment.

Outside ``DATAFLOW_ENV=production`` a saved SQLite connector could read any
file on the API host and a SQLite destination could create files anywhere.
Now only the data, upload and export areas (and ``DATAFLOW_SOURCE_PATH_ROOTS``)
open, unless the operator explicitly sets ``DATAFLOW_CONFINE_LOCAL_FILES=0``.
"""

from __future__ import annotations

import shutil
import sqlite3
import uuid
from pathlib import Path

import pytest

from tests.tenancy_world import tenancy_world  # noqa: F401

SECRET = "w1sec_server_sqlite_secret"


@pytest.fixture
def host_dir(monkeypatch):
    """A folder on the host outside every area a connector may open."""
    for name in ("DATAFLOW_CONFINE_LOCAL_FILES", "DATAWRAP_CONFINE_LOCAL_FILES", "DATAFLOW_SQLITE_ROOT",
                 "DATAWRAP_SQLITE_ROOT", "DATAFLOW_DUCKDB_ROOT", "DATAWRAP_DUCKDB_ROOT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    folder = Path.home() / f".w1sec_{uuid.uuid4().hex[:8]}"
    folder.mkdir()
    db = folder / "secret.db"
    conn = sqlite3.connect(db)
    conn.execute("create table w1sec_t (id integer primary key, secret text)")
    conn.execute("insert into w1sec_t values (1, ?)", (SECRET,))
    conn.commit()
    conn.close()
    yield folder
    shutil.rmtree(folder, ignore_errors=True)


def test_a_host_file_outside_the_areas_is_refused_by_default(tenancy_world, host_dir):  # noqa: F811
    c = tenancy_world.a
    saved = c.post("/api/v1/connectors/saved", json={"name": "w1sec-sq", "type": "sqlite",
                                                      "database": str(host_dir / "secret.db")})
    if saved.status_code in (200, 201):
        q = c.post("/api/v1/query/execute", json={"connector_id": saved.json()["id"],
                                                  "query": "select * from w1sec_t", "language": "sql"})
        assert SECRET not in q.text and q.status_code >= 400, q.text[:300]
    else:
        assert saved.status_code in (400, 403, 422), saved.text[:300]
    target = host_dir / "written.db"
    r = c.post("/api/v1/transfer/execute", json={
        "source": {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"},
        "destination": {"kind": "database", "format": "sqlite", "database": str(target), "table": "t"},
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}], "async_mode": False})
    assert not target.exists(), r.text[:300]


def test_the_default_applies_outside_production_and_the_opt_out_is_explicit(host_dir, monkeypatch):
    from connectors.sqlite_common import sqlite_file_path
    from services.local_file_policy import confine_local_files, local_file_settings

    assert confine_local_files() is True
    assert local_file_settings() == {"local_file_confinement": "enforced"}
    with pytest.raises(ValueError):
        sqlite_file_path(str(host_dir / "secret.db"), "", "")
    monkeypatch.setenv("DATAFLOW_CONFINE_LOCAL_FILES", "0")
    assert sqlite_file_path(str(host_dir / "secret.db"), "", "") == str(host_dir / "secret.db")
    assert local_file_settings() == {"local_file_confinement": "opted_out"}
