"""Million-row proof owner — conservation never invents a green COUNT.

Live PG→MySQL smoke uses discover_oltp_pair(). Unreachable ports skip with
reason. Set BENCH_ROWS=1000000 to run the full fixture through the same path.
"""

from __future__ import annotations

import os

import pytest

from services.million_row_bench import run_pg_mysql_volume
from services.million_row_proof import (
    assert_clean_conservation,
    discover_oltp_pair,
    row_conservation,
    skip_reason_if_unreachable,
)


def test_clean_conservation_requires_dest_equals_source_and_zero_rejected():
    ok = row_conservation(source_rows=1_000_000, dest_count=1_000_000, rejected_rows=0)
    assert ok["clean"] is True
    assert ok["balanced"] is True
    assert ok["verdict"] == "OK"
    assert_clean_conservation(ok)


def test_mismatch_is_not_ok_even_if_engine_claimed_success():
    bad = row_conservation(source_rows=1_000_000, dest_count=999_998, rejected_rows=0)
    assert bad["clean"] is False
    assert bad["balanced"] is False
    assert bad["verdict"] == "MISMATCH"
    with pytest.raises(AssertionError, match="row conservation failed"):
        assert_clean_conservation(bad)


def test_quarantine_is_balanced_not_clean():
    q = row_conservation(source_rows=100, dest_count=90, rejected_rows=10)
    assert q["clean"] is False
    assert q["balanced"] is True
    assert q["verdict"] == "BALANCED_QUARANTINE"
    with pytest.raises(AssertionError):
        assert_clean_conservation(q)


def test_discover_or_explicit_skip_reason():
    pair = discover_oltp_pair()
    if pair is None:
        reason = skip_reason_if_unreachable()
        assert reason
        assert "COUNT(*)" in reason
        pytest.skip(reason)
    pg, mysql = pair
    assert pg["port"] in {5432, 5433}
    assert mysql["port"] in {3306, 3307}


def test_live_pg_mysql_stream_conservation_smoke(tmp_path):
    """Same engine path as the 1M bench, default 2_000 rows so CI stays honest."""
    skip = skip_reason_if_unreachable()
    if skip:
        pytest.skip(skip)

    rows = int(os.environ.get("BENCH_ROWS", "2000"))
    if rows > 200_000 and os.getenv("CI"):
        pytest.skip("Skip multi-hundred-k scale on CI; run BENCH_ROWS locally")

    dest = os.environ.get("BENCH_DEST", f"bench_proof_{rows}")
    proof = tmp_path / f"pg_mysql_{rows}_proof.json"
    report = run_pg_mysql_volume(
        rows=rows,
        dest_table=dest,
        fail_closed=True,
        proof_path=proof,
    )
    assert report["conservation"]["clean"] is True
    assert report["dest_count"] == rows
    assert report["rejected_rows"] == 0
    assert proof.exists()
    # Named fixture has a single mapped PK, so shards prove dest COUNT per range.
    assert report.get("shard_mode") == "pk"
    assert report.get("copy_split") == "ctid"
    parts = report.get("partition_proof") or []
    assert parts
    assert sum(int(p["source_count"]) for p in parts) == rows
    assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
