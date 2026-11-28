"""D432 on live MongoDB: the undo of a failed connector delete is a conditional write.

Another replica's write between the undo's read and its write must win: the
undo matches nothing and says so, instead of three-way merging onto it.
Needs the CI MongoDB (r8_ent_harness skips without one).
"""

from __future__ import annotations

from r8_ent_harness import ent  # noqa: F401
from r11_live_world import live  # noqa: F401


def test_a_replica_write_between_read_and_restore_wins_on_live_mongo(live, monkeypatch):
    import services.schedule_store as store
    from tests.test_d432_connector_delete_needs_bound_schedule_ack import _paused_with_retry

    assert store._mongo_backend() is not None
    sched, version, moved = _paused_with_retry(store)
    coll = live.db["pipeline_schedules"]
    # Another replica edits the paused schedule after this one read it.
    coll.update_one({"_id": sched.id}, {"$set": {"notify_on_success": True}, "$inc": {"version": 1}})

    assert store.restore_if_unchanged(sched.id, version, moved) == "changed"
    doc = coll.find_one({"_id": sched.id})
    assert doc["enabled"] is False and not doc.get("retry_at") and doc["notify_on_success"] is True

    # Untouched since, the same restore applies (revision now one further on).
    fresh = int(doc["version"])
    assert store.restore_if_unchanged(sched.id, fresh, moved) == "restored"
    assert coll.find_one({"_id": sched.id})["enabled"] is True
