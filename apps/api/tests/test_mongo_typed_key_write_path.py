"""Mongo leftover keys use write-path parsers, not yes/float(text) invent.

yes used to become True. Auto 1.234 used to become a float PK. Locale
money the write path stores must still bind so leftover delete finds
the row. Dest-canonical Decimal text on a float PK stays identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.table_manager import _mongo_typed_key  # noqa: E402


def test_bool_pk_uses_write_path_tokens_only():
    assert _mongo_typed_key("true", True) is True
    assert _mongo_typed_key("1", True) is True
    assert _mongo_typed_key("false", False) is False
    assert _mongo_typed_key("0", False) is False
    assert _mongo_typed_key(True, True) is True
    with pytest.raises(ValueError, match="refuse invent"):
        _mongo_typed_key("yes", True)
    with pytest.raises(ValueError, match="refuse invent"):
        _mongo_typed_key("on", False)


def test_int_pk_locale_money_binds_auto_grouping_refuses():
    assert _mongo_typed_key("$1,234", 1) == 1234
    assert _mongo_typed_key("€1.234", 1) == 1234
    assert _mongo_typed_key(99, 1) == 99
    with pytest.raises(ValueError, match="refuse invent"):
        _mongo_typed_key("1.234", 1)
    with pytest.raises(ValueError, match="refuse invent"):
        _mongo_typed_key("1,234", 1)
    with pytest.raises(ValueError, match="refuse invent"):
        _mongo_typed_key("true", 1)


def test_float_pk_dest_canonical_and_locale_money():
    assert _mongo_typed_key("1.234", 1.0) == pytest.approx(1.234)
    assert _mongo_typed_key("$1,234.56", 1.0) == pytest.approx(1234.56)
    assert _mongo_typed_key("€1.234", 1.0) == pytest.approx(1234.0)
    with pytest.raises(ValueError, match="refuse invent"):
        _mongo_typed_key("1,234", 1.0)
