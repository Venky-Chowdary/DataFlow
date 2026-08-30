"""Iceberg catalog dest COUNT applies MoR without the write-driver gate.

resolve_iceberg_write_path fail-closes WRITE when pyiceberg is missing.
Dest COUNT used that gate and left catalog MoR unmeasured (None) even
when inspect.delete_files() was readable. Layout is catalog vs
filesystem; missing driver fails inside load_catalog, not as a silent
local warehouse.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import resolve_iceberg_write_path  # noqa: E402
from services.dest_precount import _iceberg_dest_layout  # noqa: E402


def _sql_endpoint() -> dict:
    return {
        "type": "iceberg",
        "connection_string": "sqlite:///tmp/catalog.db",
        "warehouse": "/tmp/wh",
        "table": "orders",
        "schema": "default",
    }


def test_catalog_sql_layout_does_not_need_pyiceberg():
    assert _iceberg_dest_layout(_sql_endpoint()) == "catalog"


def test_filesystem_layout_stays_filesystem():
    assert (
        _iceberg_dest_layout(
            {
                "type": "iceberg",
                "warehouse": "/tmp/wh",
                "table": "orders",
                "schema": "default",
            }
        )
        == "filesystem"
    )


def test_write_path_still_refuses_catalog_without_driver():
    from connectors.iceberg_writer import _pyiceberg_available

    if _pyiceberg_available():
        pytest.skip("pyiceberg present — write gate is catalog")
    with pytest.raises(RuntimeError):
        resolve_iceberg_write_path(_sql_endpoint())
