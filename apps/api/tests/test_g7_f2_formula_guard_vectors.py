"""G7-F2: the API and the web app apply one spreadsheet formula guard.

``tests/fixtures/formula_guard_vectors.json`` is the single vector set for the
OWASP prefix rule. This test pins ``services.csv_safety`` to it; the web app's
``src/lib/csvSafety.ts`` is pinned to the same file by ``csvSafety.test.ts``
(``npm test`` in apps/web). A change to either rule fails one of the two until
the vectors — and therefore both implementations — agree again.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
_VECTORS = json.loads((_API_ROOT / "tests" / "fixtures" / "formula_guard_vectors.json").read_text("utf-8"))["vectors"]
_WEB_LIB = _API_ROOT.parent / "web" / "src" / "lib"


@pytest.mark.parametrize("vector", _VECTORS, ids=[repr(v["input"]) for v in _VECTORS])
def test_python_rule_matches_the_shared_vectors(vector):
    from services.csv_safety import neutralize_export_cell, neutralize_formula_cell, restore_export_cell

    value = vector["input"]
    assert neutralize_export_cell(value) == vector["export"]
    assert neutralize_formula_cell(value) == vector["operator"]
    assert restore_export_cell(vector["export"]) == vector["restored"] == ("" if value is None else str(value))


def test_vectors_cover_the_rule_edges():
    exports = {json.dumps(v["input"]): v["export"] for v in _VECTORS}
    assert exports['"-5"'] == "-5" and exports["-5"] == "-5"  # numerals exempt
    assert exports['"\'=x"'] == "''=x"  # reversible
    assert exports['"-\\u0661\\u0662"'] == "'-١٢"  # ASCII digits only
    assert exports['"-5\\n"'] == "'-5\n"  # whole string
    assert len(_VECTORS) >= 30


def test_web_helper_is_pinned_to_the_same_vectors():
    """The TS side reads this exact file; if the test or the import moves, this fails."""
    ts_test = (_WEB_LIB / "csvSafety.test.ts").read_text("utf-8")
    assert "formula_guard_vectors.json" in ts_test
    assert "neutralizeExportCell" in ts_test and "restoreExportCell" in ts_test
    helper = (_WEB_LIB / "csvSafety.ts").read_text("utf-8")
    assert "csv_safety.py" in helper
