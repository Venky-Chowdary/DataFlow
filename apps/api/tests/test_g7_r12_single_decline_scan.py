"""G7 final-check F-a: each bulk-load path scans its temp file for declines once.

Before: S3→PostgreSQL read the downloaded object once for the ``\\N`` decline
and again for the TSV quote decline (then ``_census_csv_cells`` parses it);
S3/local→MySQL would read it once per decline. The decline flags now come
from one streaming pass (``copy_s3_common.scan_delimited_file``).
"""

from __future__ import annotations

import builtins
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _count_opens(monkeypatch, module, target: Path) -> dict[str, int]:
    """Count opens of ``target`` and bytes read from it inside ``module``."""
    stats = {"opens": 0, "bytes": 0}

    class _Counting:
        def __init__(self, handle):
            self._h = handle

        def read(self, *a):
            data = self._h.read(*a)
            stats["bytes"] += len(data)
            return data

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._h.close()

        def __getattr__(self, name):
            return getattr(self._h, name)

    def _open(path, *a, **k):
        handle = builtins.open(path, *a, **k)
        if Path(path).resolve() == target.resolve():
            stats["opens"] += 1
            return _Counting(handle)
        return handle

    monkeypatch.setattr(module, "open", _open, raising=False)
    return stats


def test_mysql_load_scans_the_file_once(monkeypatch, tmp_path):
    import connectors.mysql_load_data as mld
    from services import copy_s3_common, copy_s3_mysql

    path = tmp_path / "src.tsv"
    path.write_bytes(b"id\tnote\n" + b"".join(b"%d\tvalue %d\n" % (i, i) for i in range(5000)))
    stats = _count_opens(monkeypatch, copy_s3_common, path)
    monkeypatch.setattr(mld, "mysql_load_data_session_ready", lambda *_a, **_k: (True, ""))
    monkeypatch.setattr(mld, "execute_load_data_local", lambda *_a, **_k: None)
    monkeypatch.setattr(mld, "fetch_load_data_warnings", lambda _cur: [])
    copy_s3_mysql._load_delimited_into_mysql(
        object(), object(), path=str(path), table_q="`t`", columns=["id", "note"], ext="tsv"
    )
    assert stats == {"opens": 1, "bytes": path.stat().st_size}


@pytest.mark.parametrize("body, declines", [
    (b"id\tnote\n1\tplain\n2\ta,b\n", None),
    (b"id\tnote\n1\t\\N\n", "NULL"),
    (b'id\tnote\n1\t5" pipe\n', "quote"),
])
def test_pg_decline_scans_read_the_object_once(monkeypatch, body, declines):
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")
    from services import copy_s3_common, copy_s3_pg
    from services.copy_fast_path import FastPathUnavailable

    seen: dict[str, dict[str, int]] = {}

    class _FakeS3:
        def download_file(self, _bucket, _key, path):
            Path(path).write_bytes(body)
            seen["stats"] = _count_opens(monkeypatch, copy_s3_common, Path(path))

    monkeypatch.setattr(copy_s3_pg, "s3_list_keys", lambda *_a, **_k: ["src.tsv"])
    monkeypatch.setattr(copy_s3_pg, "s3_dest_count", lambda *_a, **_k: body.count(b"\n") - 1)
    monkeypatch.setattr(copy_s3_pg, "s3_client", lambda *_a, **_k: _FakeS3())
    monkeypatch.setattr(copy_s3_pg, "s3_bucket", lambda *_a, **_k: "bucket")
    table = f"g7_scan_{uuid.uuid4().hex[:8]}"
    kwargs = dict(
        source_cfg={"type": "s3"}, source_table="src.tsv",
        dest_cfg={"host": "127.0.0.1", "port": 5432, "database": "dataflow",
                  "username": "dataflow", "password": "dataflow", "schema": "public"},
        dest_schema="public", dest_table=table, pairs=[("id", "id"), ("note", "note")],
        pg_ddls=["TEXT", "TEXT"], replace_destination=True,
    )
    try:
        if declines:
            with pytest.raises(FastPathUnavailable, match=declines):
                copy_s3_pg.copy_s3_to_postgres(**kwargs)
        else:
            copy_s3_pg.copy_s3_to_postgres(**kwargs)
    finally:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')  # nosec B608
        conn.close()
    assert seen["stats"] == {"opens": 1, "bytes": len(body)}
