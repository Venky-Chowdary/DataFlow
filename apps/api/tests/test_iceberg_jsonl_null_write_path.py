"""Iceberg JSONL fallback binds reader-null via to_json_value.

When pyarrow is missing, leftover dumps used json.dumps on raw cells.
Extract SQL_NULL_SENTINEL became a JSON string. Residual Missing still
raises. 0 / false stay present.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import (  # noqa: E402
    _iceberg_jsonl_payload,
    _write_data_file,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_iceberg_jsonl_payload_null_vs_missing():
    got = _iceberg_jsonl_payload(
        {
            "id": 0,
            "note": SQL_NULL_SENTINEL,
            "flag": False,
            "blank": "",
        },
        {"id": "integer", "note": "string", "flag": "boolean", "blank": "string"},
    )
    assert got == {"id": 0, "note": None, "flag": False, "blank": ""}
    with pytest.raises(ValueError, match="DF_MISSING"):
        _iceberg_jsonl_payload({"gone": Missing})
    with pytest.raises(ValueError, match="DF_MISSING"):
        _iceberg_jsonl_payload({"gone": DF_MISSING_SENTINEL})


def test_iceberg_jsonl_file_binds_reader_null(tmp_path, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _no_pyarrow(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pyarrow" or name.startswith("pyarrow."):
            raise ImportError("forced jsonl fallback")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _no_pyarrow)
    rel, n, _chk, warnings = _write_data_file(
        tmp_path / "data",
        ["id", "note"],
        [("1", SQL_NULL_SENTINEL)],
        column_types={"id": "string", "note": "string"},
    )
    assert n == 1
    assert rel.endswith(".jsonl")
    assert any("jsonl" in w for w in warnings)
    text = (tmp_path / rel).read_text(encoding="utf-8")
    row = json.loads(text.strip())
    assert row == {"id": "1", "note": None}
    assert SQL_NULL_SENTINEL not in text
