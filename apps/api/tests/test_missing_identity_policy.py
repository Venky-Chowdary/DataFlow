"""Which destinations may accept a write with no identity key at all.

Two different questions get asked of the same sink and must not share an answer:

* ``sync_requires_unique_identity`` — a key *is* bound; must it be unique?
* ``missing_identity_blocks`` — no key is bound; is that fatal?

A sink that derives its key from row data (Redis, DynamoDB, Elasticsearch,
vector stores) loses rows on a keyless write, because two rows resolving to the
same key overwrite each other. MongoDB assigns its own ``_id`` when none is
mapped, so an insert-only run lands every document exactly once.
"""

from __future__ import annotations

import pytest

from services.primary_key import (
    KEY_ADDRESSED_DESTS,
    missing_identity_blocks,
    sync_requires_unique_identity,
)

DERIVED_KEY = ["redis", "dynamodb", "elasticsearch", "pinecone", "qdrant", "pgvector"]
INSERT_MODES = ["full_refresh_overwrite", "full_refresh_append", "incremental_append"]
UPSERT_MODES = ["upsert", "cdc", "scd2", "full_refresh_mirror", "reverse_etl"]


@pytest.mark.parametrize("dest", DERIVED_KEY)
@pytest.mark.parametrize("mode", INSERT_MODES + UPSERT_MODES)
def test_derived_key_sinks_always_need_an_identity(dest: str, mode: str):
    assert missing_identity_blocks(mode, dest_kind=dest) is True


@pytest.mark.parametrize("mode", INSERT_MODES)
def test_mongo_insert_accepts_server_assigned_id(mode: str):
    assert missing_identity_blocks(mode, dest_kind="mongodb") is False


@pytest.mark.parametrize("mode", UPSERT_MODES)
def test_mongo_upsert_still_needs_a_key(mode: str):
    assert missing_identity_blocks(mode, dest_kind="mongodb") is True


@pytest.mark.parametrize("mode", INSERT_MODES)
def test_sql_insert_needs_no_identity(mode: str):
    assert missing_identity_blocks(mode, dest_kind="postgresql") is False


@pytest.mark.parametrize("mode", INSERT_MODES + UPSERT_MODES)
def test_a_bound_mongo_id_must_still_be_unique(mode: str):
    """Duplicate ``_id`` in one batch is last-write-wins — always a hard block."""
    assert sync_requires_unique_identity(mode, dest_kind="mongodb") is True


@pytest.mark.parametrize("dest", sorted(KEY_ADDRESSED_DESTS))
@pytest.mark.parametrize("mode", INSERT_MODES + UPSERT_MODES)
def test_every_key_addressed_sink_enforces_uniqueness_on_a_bound_key(
    dest: str, mode: str
):
    assert sync_requires_unique_identity(mode, dest_kind=dest) is True
