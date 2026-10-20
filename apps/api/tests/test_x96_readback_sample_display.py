"""X-96 (part 1): the read-back sample mismatch message must show the raw
cell, not the compare fingerprint.

The sample-compare fingerprint applies the destination collation's fold
plan — on an accent-insensitive collation (``_CI_AI``, ``general_ci``) the
fingerprint folds 'xé' -> 'xe' for COMPARISON (correct). The mismatch
message used to display that folded fingerprint, so the operator saw
'source=...xe...' while the L4/L5 localized clause in the same message
showed the real 'xé'. The display now renders the raw cell.
"""
from __future__ import annotations

from services.reconciliation import sample_compare_rows


_SRC = '{"a": [1.5, "xé"], "b": 1}'


def _compare(ddl: str):
    src = [{"id": 1, "t": _SRC}]
    tgt = [{"id": 1, "t": "[]"}]
    return sample_compare_rows(
        src,
        tgt,
        [{"source": "t", "target": "t"}],
        sort_key="id",
        dest_db_type="mssql",
        dest_types={"t": ddl},
    )


def test_accent_preserved_under_accent_insensitive_collation():
    res = _compare("nvarchar(20) COLLATE Latin1_General_CI_AI")
    assert res["mismatches"], res
    m = res["mismatches"][0]
    assert "xé" in m["source_value"], m["source_value"]
    assert m["target_value"] == "[]"


def test_accent_preserved_under_sensitive_collation():
    res = _compare("nvarchar(max)")
    assert res["mismatches"], res
    assert "xé" in res["mismatches"][0]["source_value"]


def test_null_renders_as_null_token():
    src = [{"id": 1, "t": _SRC}]
    tgt = [{"id": 1, "t": None}]
    res = sample_compare_rows(
        src,
        tgt,
        [{"source": "t", "target": "t"}],
        sort_key="id",
        dest_db_type="mssql",
        dest_types={"t": "nvarchar(max)"},
    )
    assert res["mismatches"], res
    assert res["mismatches"][0]["target_value"] == "null"
