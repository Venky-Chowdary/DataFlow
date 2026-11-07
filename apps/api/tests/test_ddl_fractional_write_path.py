"""G6 fractional→integer samples use the write path, not ASCII-dot isdigit.

Auto 1.000 / 1.234 used to look fractional (1.000 → 1000 isdigit). Locale
money $10.50 the write path stores must still fail-fast.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.ddl_compatibility import evaluate_ddl_compatibility  # noqa: E402


def _eval(rows: list[dict[str, str]]) -> list[str]:
    _ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "qty", "target": "qty", "confidence": 0.99}],
        source_schema={"qty": "INTEGER"},
        target_schema={"qty": "INTEGER"},
        table_exists=True,
        dest_connected=True,
        dest_db_type="postgresql",
        sample_rows=rows,
    )
    return issues


def test_locale_money_fraction_fails_fast():
    issues = _eval([{"qty": "$10.50"}, {"qty": "€2.000,50"}])
    assert any("Fractional" in i for i in issues)


def test_bindable_dot_fraction_still_fails_fast():
    issues = _eval([{"qty": "1.5"}])
    assert any("Fractional" in i for i in issues)


def test_auto_grouping_does_not_invent_fraction():
    issues = _eval([{"qty": "1,234"}, {"qty": "1.000"}, {"qty": "1.234"}])
    assert not any("Fractional" in i for i in issues)
