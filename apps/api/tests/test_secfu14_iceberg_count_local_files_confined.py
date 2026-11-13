"""Follow-up review 1, M3: the Iceberg destination COUNT reads only confined local files.

``dest_precount._iceberg_local_path`` turned any data-file path a catalog
listed into a local ``Path`` and counted its rows, with no confinement. A
tenant-run REST catalog decides those paths, so it could count the rows of
(or test for) any file the API process can read, another workspace's export
included: security follow-up 1's confinement did not apply.

Local data-file paths now go through the Iceberg jail
(``iceberg_catalog._jail_local_path``: under the Iceberg warehouse folder
while confinement is on, the production default), and a local path listed by
a remote catalog (REST, Glue, Hive) is never read: the COUNT is unmeasured.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from services import dest_precount


def _parquet(path: Path, rows: int = 3) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"v": list(range(rows))}), path)
    return path


@pytest.fixture
def prod(tmp_path, monkeypatch):
    for name in ("ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "ENV"):
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    return tmp_path


_LOCAL_CATALOG = {"type": "iceberg", "table": "t", "extra": {"catalog_type": "sql"}}


def test_a_file_outside_the_iceberg_folder_is_not_counted(prod):
    other = _parquet(prod / "exports" / "other-ws" / "export.parquet")
    for uri in (str(other), other.as_uri()):
        assert dest_precount._count_iceberg_data_uri(uri, endpoint=dict(_LOCAL_CATALOG)) is None, uri
    assert dest_precount._iceberg_local_path(str(other)) is None


def test_a_relative_name_cannot_climb_out_of_the_warehouse(prod):
    from services.platform_config import data_dir

    _parquet(prod / "exports" / "x.parquet")
    warehouse = data_dir() / "iceberg" / "wh"
    warehouse.mkdir(parents=True)
    assert dest_precount._iceberg_local_path("../../../exports/x.parquet", warehouse=str(warehouse)) is None


def test_a_file_inside_the_iceberg_folder_is_still_counted(prod):
    from services.platform_config import data_dir

    inside = _parquet(data_dir() / "iceberg" / "wh" / "t" / "data" / "f.parquet")
    assert dest_precount._count_iceberg_data_uri(str(inside), endpoint=dict(_LOCAL_CATALOG)) == 3
    assert dest_precount._count_iceberg_data_uri(
        "t/data/f.parquet", endpoint=dict(_LOCAL_CATALOG), warehouse=str(inside.parents[2])
    ) == 3


class _Inspect:
    def __init__(self, paths):
        import pyarrow as pa

        self._data = pa.table({"file_path": paths})
        self._deletes = pa.table({"file_path": pa.array([], pa.string()), "content": pa.array([], pa.int32())})

    def data_files(self):
        return self._data

    def delete_files(self):
        return self._deletes


class _Catalog:
    def __init__(self, paths):
        self._paths = paths

    def load_table(self, _identifier):
        table = type("T", (), {})()
        table.inspect = _Inspect(self._paths)
        return table


@pytest.mark.parametrize("env", ["production", "development"])
def test_a_remote_catalog_cannot_name_a_local_file(prod, monkeypatch, env):
    from connectors import iceberg_catalog

    monkeypatch.setenv("DATAFLOW_ENV", env)
    victim = _parquet(prod / "exports" / "other-ws" / "export.parquet")
    endpoint = {"type": "iceberg", "table": "t", "connection_string": "https://catalog.example.com/v1"}
    for listed in ([str(victim)], [victim.as_uri()], ["s3://bucket/wh/t/data/a.parquet", str(victim)]):
        monkeypatch.setattr(iceberg_catalog, "load_catalog", lambda _e, p=listed: _Catalog(p))
        assert dest_precount._iceberg_catalog_snapshot(dict(endpoint)) is None, listed
    remote = ["s3://bucket/wh/t/data/a.parquet"]
    monkeypatch.setattr(iceberg_catalog, "load_catalog", lambda _e: _Catalog(remote))
    assert dest_precount._iceberg_catalog_snapshot(dict(endpoint)) == (remote, None)
