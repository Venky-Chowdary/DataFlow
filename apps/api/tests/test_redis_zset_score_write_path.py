"""Redis zset scores use the write-path float carrier, not float(text).

float('1.234') invented 1.234. 2**53+1 collapsed. Locale money binds.
Native IEEE floats pass through. Auto 1,234 refuses.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.redis_reader import (  # noqa: E402
    _read_redis_value,
    redis_zset_score_carrier,
)


def test_plain_and_native_float_still_bind():
    assert redis_zset_score_carrier(1.5) == 1.5
    assert redis_zset_score_carrier("1.5") == 1.5
    assert redis_zset_score_carrier(2) == 2.0
    assert redis_zset_score_carrier("0.025") == pytest.approx(0.025)


def test_locale_money_binds():
    assert redis_zset_score_carrier("$1.50") == pytest.approx(1.5)
    assert redis_zset_score_carrier("$1,234.56") == pytest.approx(1234.56)


def test_auto_grouping_refuses():
    with pytest.raises(ValueError, match="refuse invent"):
        redis_zset_score_carrier("1,234")
    with pytest.raises(ValueError, match="refuse invent"):
        redis_zset_score_carrier("1.234")


def test_ieee_lossy_mantissa_and_bool_refuse():
    with pytest.raises(ValueError, match="refuse invent"):
        redis_zset_score_carrier("9007199254740993")
    with pytest.raises(ValueError, match="refuse invent"):
        redis_zset_score_carrier(9007199254740993)
    with pytest.raises(ValueError, match="refuse invent"):
        redis_zset_score_carrier(True)


def test_zset_read_uses_carrier():
    client = MagicMock()
    client.zcard.return_value = 2
    client.zrange.return_value = [("member-a", 1.5), (b"member-b", "$1.50")]
    payload = json.loads(_read_redis_value(client, "leaders", "zset"))
    assert payload == [["member-a", 1.5], ["member-b", 1.5]]

    client.zrange.return_value = [("bad", "1,234")]
    with pytest.raises(ValueError, match="refuse invent"):
        _read_redis_value(client, "leaders", "zset")
