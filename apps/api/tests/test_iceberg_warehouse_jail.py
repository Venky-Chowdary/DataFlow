"""Iceberg local warehouses, SQLite catalogs and table dirs stay under the Iceberg root."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.iceberg_catalog import (
    parse_iceberg_catalog_config,
    resolve_local_warehouse,
)
from connectors.iceberg_catalog import (
    test_iceberg_catalog as probe_iceberg_catalog,
)
from connectors.iceberg_writer import _resolve_iceberg_table_dir, write_mapped_rows


@pytest.fixture()
def jail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "iceberg_root"
    root.mkdir()
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(root))
    return root.resolve()


def test_absolute_warehouse_outside_root_rejected_before_mkdir(jail: Path, tmp_path: Path) -> None:
    outside = tmp_path / "outside" / "wh"
    with pytest.raises(ValueError, match="ICEBERG_ROOT"):
        parse_iceberg_catalog_config({"warehouse": str(outside), "table": "t"})
    assert not outside.exists()
    ok, msg = probe_iceberg_catalog({"connection_string": f"file://{outside}"})
    assert not ok and "ICEBERG_ROOT" in msg
    assert not outside.exists()


def test_dotdot_warehouse_escape_rejected(jail: Path) -> None:
    with pytest.raises(ValueError, match="ICEBERG_ROOT"):
        resolve_local_warehouse("../../etc", create=False)
    with pytest.raises(ValueError, match="ICEBERG_ROOT"):
        resolve_local_warehouse("/etc", create=False)


def test_relative_warehouse_resolves_under_root(jail: Path) -> None:
    cfg = parse_iceberg_catalog_config({"warehouse": "tenant_a/wh", "table": "t"})
    assert Path(cfg["properties"]["warehouse"]) == jail / "tenant_a" / "wh"
    assert (jail / "tenant_a" / "wh").is_dir()


def test_symlink_escape_rejected(jail: Path, tmp_path: Path) -> None:
    target = tmp_path / "elsewhere"
    target.mkdir()
    (jail / "link").symlink_to(target)
    with pytest.raises(ValueError, match="ICEBERG_ROOT"):
        resolve_local_warehouse("link", create=False)


def test_sql_catalog_path_cannot_escape_warehouse(jail: Path, tmp_path: Path) -> None:
    for bad in (str(tmp_path / "evil.db"), "../evil.db", "../../evil.db"):
        with pytest.raises(ValueError, match="catalog_path"):
            parse_iceberg_catalog_config(
                {
                    "warehouse": "wh",
                    "table": "t",
                    "extra": {"catalog_type": "sql", "catalog_path": bad},
                }
            )
    cfg = parse_iceberg_catalog_config(
        {"warehouse": "wh", "table": "t", "extra": {"catalog_type": "sql", "catalog_path": "cat.db"}}
    )
    assert cfg["properties"]["uri"] == f"sqlite:///{jail / 'wh' / 'cat.db'}"


def test_sqlite_uri_catalog_jailed(jail: Path, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="ICEBERG_ROOT"):
        parse_iceberg_catalog_config(
            {"connection_string": f"sqlite:///{tmp_path / 'evil.db'}", "table": "t"}
        )
    cfg = parse_iceberg_catalog_config({"connection_string": "sqlite:///cat.db", "table": "t"})
    assert cfg["properties"]["uri"] == f"sqlite:///{jail / 'cat.db'}"


@pytest.mark.parametrize(
    ("schema", "table"),
    [("../../tmp", "t"), ("ns", "../../escape"), ("..", "t"), ("ns", "a/b")],
)
def test_namespace_and_table_cannot_traverse(jail: Path, schema: str, table: str) -> None:
    with pytest.raises(ValueError):
        parse_iceberg_catalog_config({"warehouse": "wh", "schema": schema, "table": table})


def test_writer_table_dir_rejects_dotdot_schema(tmp_path: Path) -> None:
    cfg = {"database": str(tmp_path / "wh"), "connection_string": str(tmp_path / "wh")}
    for schema, table in (("../../tmp", "orders"), ("ns", "../orders"), ("", "ns/../../o"), (str(tmp_path), "o")):
        with pytest.raises(ValueError):
            _resolve_iceberg_table_dir(cfg, table, schema)
    assert _resolve_iceberg_table_dir(cfg, "ns.orders", None) == (tmp_path / "wh").resolve() / "ns" / "orders"


def test_writer_refuses_escape_without_writing(tmp_path: Path) -> None:
    wh = tmp_path / "wh"
    result = write_mapped_rows(
        host="",
        database=str(wh),
        connection_string=str(wh),
        schema="../../escaped",
        table_name="orders",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id", "confidence": 1}],
        column_types={"id": "string"},
        create_table=True,
    )
    assert not result.ok
    assert "warehouse" in (result.error or "")
    assert not (tmp_path.parent / "escaped").exists()


def test_writer_warehouse_outside_root_rejected(jail: Path, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    result = write_mapped_rows(
        host="",
        database=str(outside),
        connection_string=str(outside),
        table_name="orders",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id", "confidence": 1}],
        column_types={"id": "string"},
        create_table=True,
    )
    assert not result.ok
    assert not outside.exists()


def test_production_defaults_root_to_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAFLOW_ICEBERG_ROOT", raising=False)
    monkeypatch.delenv("DATAWRAP_ICEBERG_ROOT", raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    with pytest.raises(ValueError, match="ICEBERG_ROOT"):
        resolve_local_warehouse("/etc", create=False)
    assert resolve_local_warehouse("wh", create=False) == (tmp_path / "data" / "iceberg" / "wh").resolve()


def test_dev_without_root_keeps_absolute_warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAFLOW_ICEBERG_ROOT", raising=False)
    monkeypatch.delenv("DATAWRAP_ICEBERG_ROOT", raising=False)
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    assert resolve_local_warehouse(str(tmp_path / "wh")) == (tmp_path / "wh").resolve()
