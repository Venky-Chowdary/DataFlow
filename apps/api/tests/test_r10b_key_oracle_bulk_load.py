"""Preflight's destination key check must fit its deadline (r10b follow-up).

The SQL Server key oracle loaded every distinct key with a parameterised
executemany, which pyodbc sends one row per round trip: about 0.5 ms a key,
so 50k keys took ~25 s and the 200k cap ~100 s, inside preflight's 45 s
deadline. Keys now go up in a few OPENJSON round trips; a database without
OPENJSON (compatibility level < 130) falls back to the row-wise load.
"""

from __future__ import annotations

import time

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.skipif(not h.port_up(1433), reason="SQL Server not running")


def _keys(n: int) -> list[tuple[str]]:
    # 'k1' and 'K1' are one key under the default case-insensitive collation.
    return [(f"k{i}",) for i in range(n)] + [("K1",)]


def test_fifty_thousand_keys_are_judged_well_inside_the_deadline():
    from services.dest_key_oracle import _engine_groups

    n = 50_000
    started = time.monotonic()
    groups = _engine_groups(dict(h.dest_config("sqlserver")), "sqlserver", ["NVARCHAR(100)"], _keys(n))
    elapsed = time.monotonic() - started
    assert groups == [[1, n]]
    assert elapsed < 15, f"key oracle took {elapsed:.1f}s for {n} keys"


def test_without_openjson_the_row_wise_load_still_finds_the_collision(monkeypatch):
    import json

    from services.dest_key_oracle import _engine_groups

    real_dumps = json.dumps

    def no_openjson(obj, *args, **kwargs):
        if isinstance(obj, list) and obj and isinstance(obj[0], list):
            raise RuntimeError("OPENJSON unavailable at this compatibility level")
        return real_dumps(obj, *args, **kwargs)

    monkeypatch.setattr(json, "dumps", no_openjson)
    groups = _engine_groups(dict(h.dest_config("sqlserver")), "sqlserver", ["NVARCHAR(100)"], _keys(300))
    assert groups == [[1, 300]]
