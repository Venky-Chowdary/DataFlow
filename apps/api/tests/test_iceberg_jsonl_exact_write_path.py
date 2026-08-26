"""Iceberg snapshot JSONL reread uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 before leftover MERGE
compared keys. IEEE-exact 1.5 stays float. Corrupt lines still refuse.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.iceberg_writer import _read_snapshot_data_file  # noqa: E402

LONG = "1.234567890123456789"


def test_iceberg_jsonl_reread_keeps_long_fraction(tmp_path: Path):
    path = tmp_path / "data" / "part-0.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(
        f'{{"id": 1, "amt": {LONG}, "n": 1.5}}\n',
        encoding="utf-8",
    )
    rows = _read_snapshot_data_file("data/part-0.jsonl", path, ["id", "amt", "n"])
    assert rows[0]["amt"] == Decimal(LONG)
    assert rows[0]["amt"] != json.loads(path.read_text())["amt"]
    assert rows[0]["n"] == 1.5
    assert isinstance(rows[0]["n"], float)
    assert rows[0]["id"] == 1


def test_iceberg_jsonl_corrupt_line_refuses(tmp_path: Path):
    path = tmp_path / "bad.jsonl"
    path.write_text('{"id":1}\n{not-json}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="corrupt"):
        _read_snapshot_data_file("bad.jsonl", path, ["id"])
