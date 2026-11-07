"""D104: a second enabled pipeline must not be created onto a destination
table another enabled pipeline already owns — silently accepting it surfaced
only at run time as a key collision (pipeline A's unique index rejected
pipeline B's append) with misleading corrective guidance. create_schedule
now blocks, naming the conflicting pipeline and its sync mode, unless the
operator explicitly passes shared_destination_acknowledged.
"""

import pytest

import services.schedule_store as store


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")


def _base(**extra):
    return {
        "name": "pipe-a", "source_connector_id": "src-pg",
        "source_table": "hq_s", "dest_connector_id": "dst-pg",
        "dest_table": "hq_shared", "interval": "hourly",
        "sync_mode": "incremental_upsert", "primary_key": "id",
        "mappings": [{"source": "id", "target": "id"}], **extra,
    }


def test_second_enabled_pipeline_on_same_dest_is_blocked(temp_store):
    a = store.create_schedule(_base())
    assert a.enabled
    with pytest.raises(ValueError) as ei:
        store.create_schedule(
            _base(name="pipe-b", sync_mode="full_refresh_append")
        )
    msg = str(ei.value)
    assert a.id in msg
    assert "incremental_deduped" in msg  # names the conflicting pipeline's sync mode (normalized)


def test_shared_dest_allowed_with_explicit_acknowledgement(temp_store):
    store.create_schedule(_base())
    b = store.create_schedule(
        _base(
            name="pipe-b",
            sync_mode="full_refresh_append",
            shared_destination_acknowledged=True,
        )
    )
    assert b.id


def test_disabled_owner_does_not_block(temp_store):
    a = store.create_schedule(_base(enabled=False))
    assert not a.enabled
    b = store.create_schedule(_base(name="pipe-b"))
    assert b.id and b.id != a.id


def test_retargeting_onto_an_owned_dest_is_blocked_on_update(temp_store):
    store.create_schedule(_base())
    b = store.create_schedule(_base(name="pipe-b", dest_table="hq_other"))
    with pytest.raises(ValueError, match="already written"):
        store.update_schedule(b.id, {"dest_table": "hq_shared"})
