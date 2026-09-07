"""A Validate stamp names one route, and an edit that moves the route drops it.

Editing a schedule's destination kept the Decision Artifact hash taken against
the previous one, so every later run — cadence tick and "Run now" alike —
refused with "Decision Artifact content_hash mismatch", and no control in the
product could clear it. The schedule was dead until it was deleted and rebuilt.
"""

import pytest

import services.schedule_store as store


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    yield


def _stamped():
    sched = store.create_schedule({
        "name": "Nightly",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })
    stamped = store.update_schedule(
        sched.id,
        {
            "approved_decision_artifact_hash": "a" * 64,
            "approved_ddl_identity_hash": "b" * 64,
        },
    )
    assert stamped is not None
    assert stamped.approved_decision_artifact_hash == "a" * 64
    return stamped


def test_editing_the_destination_drops_the_stamp_taken_for_the_old_one(temp_store):
    stamped = _stamped()
    moved = store.update_schedule(stamped.id, {"dest_table": "orders_wh_v2"})
    assert moved is not None
    assert moved.dest_table == "orders_wh_v2"
    assert moved.approved_decision_artifact_hash == ""
    assert moved.approved_ddl_identity_hash == ""


def test_editing_the_mappings_drops_the_stamp(temp_store):
    stamped = _stamped()
    moved = store.update_schedule(
        stamped.id,
        {
            "mappings": [
                {"source": "id", "target": "id"},
                {"source": "amt", "target": "amt"},
            ]
        },
    )
    assert moved is not None
    assert moved.approved_decision_artifact_hash == ""


def test_an_unrelated_edit_keeps_the_stamp(temp_store):
    """Renaming or re-timing a schedule does not move the route it was approved for."""
    stamped = _stamped()
    same = store.update_schedule(
        stamped.id, {"name": "Nightly (EU)", "interval": "hourly"}
    )
    assert same is not None
    assert same.approved_decision_artifact_hash == "a" * 64
    assert same.approved_ddl_identity_hash == "b" * 64


def test_a_re_validate_that_moves_the_route_keeps_its_own_new_stamp(temp_store):
    """Studio re-Validate sends the new route and the hash it just computed."""
    stamped = _stamped()
    revalidated = store.update_schedule(
        stamped.id,
        {"dest_table": "orders_wh_v2", "approved_decision_artifact_hash": "c" * 64},
    )
    assert revalidated is not None
    assert revalidated.approved_decision_artifact_hash == "c" * 64
