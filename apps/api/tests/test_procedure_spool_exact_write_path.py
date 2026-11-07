"""Procedure result-spool reread uses json_loads_exact.

A JSON number with extra fraction digits used to collapse to IEEE, then
str() wrote the short spelling. Decimal text stays full digits. IEEE-exact
1.5 stays 1.5.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.procedure_source import _read_spool_page, _ResultSpool  # noqa: E402

LONG = "1.234567890123456789"


def test_procedure_spool_keeps_json_number_digits(tmp_path: Path):
    path = tmp_path / "call.jsonl"
    path.write_text(f'[{LONG}, 1.5, 1]\n', encoding="utf-8")
    spool = _ResultSpool(path=path, headers=["amt", "n", "id"], total=1, schema={})
    headers, rows, total = _read_spool_page(spool, offset=0, limit=10)
    assert headers == ["amt", "n", "id"]
    assert total == 1
    assert rows[0][0] == LONG
    assert rows[0][0] != str(json.loads(f"[{LONG}]")[0])
    assert rows[0][1] == "1.5"
    assert rows[0][2] == "1"
