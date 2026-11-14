"""SEC-12: a file export's server path is not part of the transfer response."""

from __future__ import annotations

import sqlite3

from tests.tenancy_world import tenancy_world  # noqa: F401


def test_file_export_response_names_the_download_not_the_server_path(tenancy_world, tmp_path_factory):  # noqa: F811
    from services.export_paths import exports_dir

    db = tmp_path_factory.mktemp("w1sec_src") / "w1sec_src.db"
    conn = sqlite3.connect(db)
    conn.execute("create table w1sec_t (id integer primary key)")
    conn.execute("insert into w1sec_t values (1), (2)")
    conn.commit()
    conn.close()
    body = {"source": {"kind": "database", "format": "sqlite", "database": str(db), "table": "w1sec_t"},
            "destination": {"kind": "file_export", "format": "csv"},
            "mappings": [{"source": "id", "target": "id", "confidence": 1.0}], "async_mode": False}
    r = tenancy_world.a.post("/api/v1/transfer/execute", json=body)
    assert r.status_code == 200 and r.json().get("success") is True, r.text[:400]
    root = str(exports_dir(create=False).resolve())
    assert root not in r.text and '"path"' not in r.text, r.text[:400]
    dest = r.json()["destination"]
    assert dest.get("filename") and dest.get("download_url"), dest
