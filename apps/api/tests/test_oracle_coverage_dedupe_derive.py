"""Oracle coverage commit: dedupe reports what the winner checks found
(verified when they passed, honest skip naming why when unverified), and
derive_column/set_value expressions compile to source SQL for the
independent oracle's whitelist.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.transfer.reconcile_step import (
    _expression_oracle_sql,
    _transform_applied_proof,
)


def _proof(recon, steps):
    dest = MagicMock()
    return _transform_applied_proof(
        shape_recipe={"steps": steps},
        dest_summary={"shape_recipe_hash": "h1"},
        source_endpoint=None,
        endpoint=MagicMock(),
        columns=["id"],
        mappings=None,
        source_schema=None,
        recon=recon,
    )


def test_derive_column_expression_compiles_per_dialect():
    for src, q in [("postgresql", '"'), ("sqlite", '"'), ("mysql", "`"), ("mssql", "[")]:
        assert _expression_oracle_sql(
            "upper(note)", {}, src_type=src, qs=q
        ) is not None


def test_unlisted_function_stays_uncovered():
    assert _expression_oracle_sql(
        "sha256_hex(x)", {}, src_type="postgresql", qs='"'
    ) is None


def test_dedupe_verified_when_winner_checks_pass():
    out = _proof(
        {"dedupe_checks": [
            {"check": "winner_uniqueness", "status": "passed"},
            {"check": "winner_ordering", "status": "passed"},
        ]},
        [{"op": "dedupe_rows", "enabled": True}],
    )
    assert out["status"] == "verified"
    assert "winner" in out.get("provenance", "")


def test_dedupe_skip_names_unverified_reason():
    out = _proof(
        {"dedupe_checks": [
            {"check": "winner_uniqueness", "status": "passed"},
            {"check": "winner_ordering", "status": "unverified",
             "reason": "arrival-based rule"},
        ]},
        [{"op": "dedupe_rows", "enabled": True}],
    )
    assert out["status"] == "skipped"
    assert "arrival-based" in out["reason"]


def test_dedupe_without_checks_stays_honest_skip():
    out = _proof({}, [{"op": "dedupe_rows", "enabled": True}])
    assert out["status"] == "skipped"
    assert "winner checks" in out["reason"]
