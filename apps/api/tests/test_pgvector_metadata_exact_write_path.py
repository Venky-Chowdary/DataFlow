"""pgvector dest metadata flatten uses json_loads_exact.

stdlib json.loads collapsed 1.234567890123456789 inside metadata JSON
before Gate-8 / Validate sample compared cells. IEEE-exact 1.5 stays
float. Invalid metadata stays empty.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.target_sample_vector import load_pgvector_metadata  # noqa: E402

LONG = "1.234567890123456789"


def test_pgvector_metadata_keeps_long_fraction():
    raw = f'{{"amt": {LONG}, "n": 1.5, "id": 1}}'
    meta = load_pgvector_metadata(raw)
    assert meta["amt"] == Decimal(LONG)
    assert meta["amt"] != json.loads(raw)["amt"]
    assert meta["n"] == 1.5
    assert meta["id"] == 1


def test_pgvector_metadata_passthrough_and_invalid():
    tree = {"amt": Decimal(LONG)}
    assert load_pgvector_metadata(tree) is tree
    assert load_pgvector_metadata("{not-json}") == {}
    assert load_pgvector_metadata("[1, 2]") == {}
    assert load_pgvector_metadata("") == {}
    assert load_pgvector_metadata(None) == {}
