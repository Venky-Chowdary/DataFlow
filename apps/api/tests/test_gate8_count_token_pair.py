"""A keyed engine copy's ``pk_join_count:<n>`` pair is a row count, not a digest.

Regression for an incremental_deduped PG→PG run whose second schedule fired
into 500 occupied rows: both engine checksums were ``pk_join_count:37``, Gate-8
took the pair as value digests, stamped the batch size (37) as the destination
population, and L1 refused ``37 - 500 == 25``.
"""
from __future__ import annotations

from src.transfer.reconcile_step import (
    _engine_count_proof_only,
    _writer_supplied_engine_digests,
)


def _summary(token: str) -> dict:
    return {
        "engine_source_checksum": token,
        "engine_target_checksum": token,
        "checksum": token,
        "rows_written": 37,
        "proof_scope": "dest_pk_join_equals_staging",
    }


def test_count_token_pair_is_not_an_engine_digest_pair():
    assert _writer_supplied_engine_digests(_summary("pk_join_count:37")) is None
    assert _writer_supplied_engine_digests(_summary("dest_count:500")) is None


def test_count_token_pair_is_graded_as_count_proof():
    assert _engine_count_proof_only(_summary("pk_join_count:37")) == 37


def test_real_engine_digest_pair_is_still_accepted():
    summary = _summary("0123456789abcdef")
    assert _writer_supplied_engine_digests(summary) == (
        "0123456789abcdef",
        "0123456789abcdef",
        37,
    )
