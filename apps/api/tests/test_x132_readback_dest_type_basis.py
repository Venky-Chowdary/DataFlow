"""X-132 regression — destination read-backs must fingerprint on the same
``gate8_digest_types`` basis the write pass used.

Reproduced on ``dynamodb -> redis``: the write pass fingerprinted
``Decimal('1000.00')`` under the declared carrier type ``string`` and emitted
``amount=1000.00``; the read-back hashed the JSON doc ``"1000.00"`` with no
type basis, canonicalized it to ``1000``, and Gate-8 failed a faithful load
with a checksum mismatch while every key-aligned cell compared equal.

``verify_redis_prefix`` (and ``verify_bigquery_table``) never received the
``dest_types`` that ``verify_target`` already holds — every other destination
family got them. This test pins the basis on both the function signature and
the ``verify_target`` call sites.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

import services.reconciliation as recon


class _FakeRedis:
    def __init__(self, docs: dict[str, bytes]):
        self._docs = docs

    def scan(self, *, cursor: int, match: str, count: int):
        return 0, list(self._docs.keys())

    def get(self, key):
        return self._docs.get(key)


@pytest.fixture()
def fake_redis_docs(monkeypatch):
    docs = {
        "pfx:col_1": b'{"id": "1", "amount": "1000.00"}',
        "pfx:col_2": b'{"id": "2", "amount": "2000.50"}',
    }
    client = _FakeRedis(docs)
    import connectors.redis_reader as rr

    monkeypatch.setattr(rr, "_redis_client", lambda cfg, **kw: client)
    return docs


def _call_redis(**extra):
    return recon.verify_redis_prefix(
        host="localhost",
        port=6379,
        database="0",
        username="",
        password="",
        connection_string="",
        ssl=False,
        prefix="pfx",
        target_columns=["id", "amount"],
        **extra,
    )


def test_redis_prefix_digest_uses_write_pass_basis(fake_redis_docs):
    """Read-back digest must equal the write-pass fingerprint digest.

    The write pass hashed ``Decimal('1000.00')`` under the declared
    ``string`` carrier -> ``amount=1000.00``.  Hashing the read-back
    ``"1000.00"`` without the basis folds it to ``1000`` — same stored
    bytes, two digests, false Gate-8 failure.
    """
    from connectors.writer_common import row_fingerprints
    from services.fingerprint_accumulator import FingerprintAccumulator

    dest_types = {"id": "string", "amount": "string"}
    acc = FingerprintAccumulator()
    acc.add_many(
        row_fingerprints(
            [(Decimal("1"), Decimal("1000.00")), (Decimal("2"), Decimal("2000.50"))],
            ["id", "amount"],
            dest_db_type="redis",
            dest_types=dest_types,
        )
    )
    expected = acc.digest()

    count, chk = _call_redis(dest_db_type="redis", dest_types=dest_types)
    assert count == 2
    assert chk == expected


def test_redis_prefix_basis_changes_the_digest(fake_redis_docs):
    """Guard the basis is actually applied, not ignored."""
    count_t, chk_t = _call_redis(
        dest_db_type="redis", dest_types={"id": "string", "amount": "string"}
    )
    count_u, chk_u = _call_redis()
    assert count_t == count_u == 2
    assert chk_t != chk_u  # '1000.00' as text vs '1000' canonicalized


def test_verify_target_forwards_dest_types_to_redis(monkeypatch):
    """``verify_target`` already holds the ``gate8_digest_types`` basis —
    the redis branch must hand it through like every other destination.
    """
    seen = {}

    def spy(**kw):
        seen.update(kw)
        return 2, "chk"

    monkeypatch.setattr(recon, "verify_redis_prefix", spy)
    recon.verify_target(
        "redis",
        {"host": "x", "database": "0"},
        schema="",
        table_name="pfx",
        fallback_rows=-1,
        fallback_checksum="",
        target_columns=["id", "amount"],
        dest_types={"id": "string", "amount": "string"},
    )
    assert seen.get("dest_db_type") == "redis"
    assert seen.get("dest_types") == {"id": "string", "amount": "string"}


def test_verify_target_forwards_dest_types_to_bigquery(monkeypatch):
    seen = {}

    def spy(**kw):
        seen.update(kw)
        return 2, "chk"

    monkeypatch.setattr(recon, "verify_bigquery_table", spy)
    recon.verify_target(
        "bigquery",
        {"database": "proj", "host": "x"},
        schema="ds",
        table_name="t",
        fallback_rows=-1,
        fallback_checksum="",
        target_columns=["id", "amount"],
        dest_types={"amount": "NUMERIC"},
    )
    assert seen.get("dest_types") == {"amount": "NUMERIC"}


def test_bigquery_table_forwards_basis_to_checksum(monkeypatch):
    """The digest itself must be taken on the dest basis, not just the arg."""
    seen = {}

    def spy_ccf(rows, columns=None, **kw):
        seen.update(kw)
        return "chk"

    monkeypatch.setattr(recon, "canonical_checksum_from_iter", spy_ccf)
    import connectors.bigquery_conn as bqc
    import services.dest_precount as dp

    class _Field:
        name = "id"

    class _Table:
        schema = [_Field()]

    class _Client:
        def get_table(self, _id, **kw):
            return _Table()

        def list_rows(self, _id):
            yield [1]

    monkeypatch.setattr(bqc, "get_client", lambda **kw: _Client())
    monkeypatch.setattr(dp, "_bigquery_row_count", lambda *a, **kw: 1)

    count, chk = recon.verify_bigquery_table(
        project_id="p",
        dataset_id="d",
        connection_string="",
        table_name="t",
        target_columns=["id"],
        dest_types={"id": "INT64"},
    )
    assert count == 1
    assert chk == "chk"
    assert seen.get("dest_db_type") == "bigquery"
    assert seen.get("dest_types") == {"id": "INT64"}
