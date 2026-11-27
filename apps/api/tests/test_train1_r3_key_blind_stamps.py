"""Review 3 items 1 and 3: a stamp that does not cover key flags never approves a key change.

Item 1: the feature/Cleanup (base) digest drops each row's key flag, so a base
stamp minted for a Map keyed on ``id`` was accepted for the same Map keyed on
``code``, with the key removed, or with one added. The PR #435 interim digest
drops them too, and the Q2 guard only looked at whether the CURRENT rows had a
key, so an interim stamp minted with a key accepted its removal.

Now (``stamp_key_refusal``): an interim stamp is accepted only when neither the
stamped nor the current decision is keyed; a base stamp accepts a keyed Map only
when the key set recorded with that stamp (:mod:`services.stamp_key_records`) is
the current one, and with no record refuses with "re-Validate this route". An
unkeyed Map keeps running on an unrecorded stamp.

Item 3: tests that fail if the interim digest is accepted for a keyed decision,
if the previous-release fallback accepts a current-format stamp, or if PR #435's
scored digest is dropped.
"""

from __future__ import annotations

import pytest

from services.decision_kernel.execute_gate import (
    build_artifact_from_mappings,
    create_new_validate_holds_after_dest_exists,
    enforce_decision_artifact,
)
from services.decision_kernel.legacy_rules import legacy_plan_rules
from services.decision_kernel.models import (
    NO_KEY_RECORD_REASON,
    accepted_stamps,
    base_content_hash,
    compute_content_hash,
    interim_content_hash,
    scored_structural_hash,
)

_ROWS = [
    {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer",
     "confidence": 0.9, "create_new": True},
    {"source": "code", "target": "code", "source_type": "text", "target_type": "text",
     "confidence": 0.9, "create_new": True},
]


def _keyed(*names: str) -> list[dict]:
    return [{**row, "is_primary_key": row["source"] in names} for row in _ROWS]


def _art(maps: list[dict]):
    return build_artifact_from_mappings(
        maps, dest_db="postgresql", source_db="postgresql", source_fingerprint="fp",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:postgresql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )


def _enforce(maps: list[dict], stamp: str, record=None, **extra):
    args = dict(
        mappings=maps, dest_db="postgresql", source_db="postgresql",
        approved_content_hash=stamp, source_fingerprint="fp", dest_fingerprint="",
        destination_table_exists=False, sync_mode="incremental_append",
        stamp_key_record=(lambda _stamp: record) if record is not None else None,
    )
    args.update(extra)
    return enforce_decision_artifact(**args)


def _base(maps: list[dict]) -> str:
    return base_content_hash(_art(maps).to_dict())


def _interim(maps: list[dict]) -> str:
    return interim_content_hash(_art(maps).to_dict())


@pytest.mark.parametrize("stamped, run, record", [
    ((), ("id",), None),                       # key added, no record
    ((), ("id",), frozenset()),                # key added, recorded unkeyed
    (("id",), (), frozenset({"id"})),          # key removed
    (("id",), ("code",), frozenset({"id"})),   # key moved
    (("id",), ("code",), None),                # key moved, no record
    (("id",), ("id", "code"), frozenset({"id"})),  # key widened
])
def test_a_base_stamp_refuses_every_key_change(stamped, run, record):
    stamp = _base(_keyed(*stamped))
    # The bug: the base digest is the same for every key set.
    assert stamp == _base(_keyed(*run))
    err, art = _enforce(_keyed(*run), stamp, record)
    assert err and "re-Validate this route" in err, err
    assert art is None
    # The same on the supplied-payload path.
    payload = {**_art(_keyed(*stamped)).to_dict(), "content_hash": stamp}
    err, art = _enforce(_keyed(*run), stamp, record, artifact_payload=payload)
    assert err and "re-Validate this route" in err, err


def test_a_base_stamp_runs_an_unchanged_keyed_decision_on_its_record_only():
    stamp = _base(_keyed("id"))
    err, art = _enforce(_keyed("id"), stamp, frozenset({"id"}))
    assert err is None, err
    assert art is not None
    err, art = _enforce(_keyed("id"), stamp)
    assert err is not None and NO_KEY_RECORD_REASON in err, err


def test_an_unkeyed_decision_keeps_running_on_an_unrecorded_base_stamp():
    stamp = _base(_keyed())
    assert _enforce(_keyed(), stamp)[0] is None
    assert _enforce(_keyed(), stamp, frozenset())[0] is None


@pytest.mark.parametrize("stamped, run, record", [
    ((), ("id",), None),
    ((), ("id",), frozenset()),
    (("id",), (), frozenset({"id"})),          # the reviewer's repro: minted keyed, key removed
    (("id",), ("code",), frozenset({"id"})),
    (("id",), ("id",), frozenset({"id"})),     # even unchanged: interim never carries a key
])
def test_an_interim_stamp_is_accepted_only_when_neither_side_is_keyed(stamped, run, record):
    # The interim digest equals for every key set too.
    stamp = _interim(_keyed())
    assert stamp == _interim(_keyed(*stamped)) == _interim(_keyed(*run))
    err, art = _enforce(_keyed(*run), stamp, record)
    assert err and "re-Validate this route" in err, err
    assert art is None


def test_an_interim_stamp_of_an_unkeyed_decision_still_runs():
    stamp = _interim(_keyed())
    assert _enforce(_keyed(), stamp)[0] is None
    assert _enforce(_keyed(), stamp, frozenset())[0] is None
    # Item 3 (a): the interim digest is not an accepted stamp of a keyed decision.
    assert stamp in accepted_stamps(_art(_keyed()))
    assert _interim(_keyed("id")) not in accepted_stamps(_art(_keyed("id")))


def test_the_create_new_dest_exists_hold_applies_the_key_rule():
    stamp = _base(_keyed("id"))
    kwargs = dict(
        dest_db="postgresql", approved_content_hash=stamp, live_dest_fingerprint="abc",
        source_fingerprint="fp", source_db="postgresql", sync_mode="incremental_append",
        destination_table_exists=True, dest_column_names=["id", "code"],
        source_column_names=["id", "code"],
    )
    assert not create_new_validate_holds_after_dest_exists(
        mappings=_keyed("code"), recorded_keys=frozenset({"id"}), **kwargs)[0]
    assert create_new_validate_holds_after_dest_exists(
        mappings=_keyed("id"), recorded_keys=frozenset({"id"}), **kwargs)[0]


# -- item 3 -------------------------------------------------------------------------


def _bit_maps(target_type: str) -> list[dict]:
    return [
        {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer",
         "confidence": 0.95, "create_new": True},
        {"source": "c", "target": "c", "source_type": "bit(1)", "target_type": target_type,
         "confidence": 0.9, "create_new": True},
    ]


def _mysql(maps: list[dict]):
    return build_artifact_from_mappings(
        maps, dest_db="mysql", source_db="postgresql", source_fingerprint="", dest_fingerprint="",
        sync_mode="incremental_append", error_policy="quarantine", route_id="validate:mysql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )


def test_the_previous_release_fallback_never_accepts_a_current_format_stamp():
    """Item 3 (b): a current-format digest of the plan the OLD rules make is not a
    stamp anyone minted; only previous-release formats match under those rules."""
    with legacy_plan_rules():
        legacy = _mysql(_bit_maps("BIT(1)"))
    current = _mysql(_bit_maps("BIT(1)"))
    assert legacy.content_hash != current.content_hash
    forged = compute_content_hash(legacy.to_dict())
    err, art = enforce_decision_artifact(
        mappings=_bit_maps("BIT(1)"), dest_db="mysql", source_db="postgresql",
        approved_content_hash=forged, source_fingerprint="", dest_fingerprint="",
        destination_table_exists=False, sync_mode="incremental_append", error_policy="quarantine",
    )
    assert err is not None and art is None
    # The previous release's own (base) stamp of it still runs.
    err, art = enforce_decision_artifact(
        mappings=_bit_maps("BIT(1)"), dest_db="mysql", source_db="postgresql",
        approved_content_hash=base_content_hash(legacy.to_dict()), source_fingerprint="",
        dest_fingerprint="", destination_table_exists=False, sync_mode="incremental_append",
        error_policy="quarantine",
    )
    assert err is None, err


def test_a_pr435_scored_stamp_is_accepted():
    """Item 3 (c): PR #435's scored digest (classification inside) is still a stamp."""
    art = _art(_keyed("id"))
    scored = scored_structural_hash(art.to_dict())
    assert scored != art.content_hash
    err, out = _enforce(_keyed("id"), scored)
    assert err is None, err
    assert out is not None
    # Covering key flags, it refuses a key change without any record.
    err, _ = _enforce(_keyed("code"), scored)
    assert err is not None


# -- the record a key-blind stamp is judged against ---------------------------------


@pytest.fixture
def _records(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import services.schedule_store as store
    from services import stamp_key_records

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(stamp_key_records, "_collection", lambda: None)
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    # Known connectors, so the stamp's format can be told on the edit.
    monkeypatch.setattr("services.connector_store.get_connector",
                        lambda *a, **k: SimpleNamespace(to_dict=lambda: {"type": "postgresql"}))
    return stamp_key_records


def test_records_are_append_only_per_schedule_and_unknown_is_no_record(_records):
    stamp = "a" * 64
    args = dict(workspace_id="ws", schedule_id="s1", stamp=stamp)
    assert _records.record_if_absent(**args, key_columns=["ID"])
    assert not _records.record_if_absent(**args, key_columns=["code"])
    assert _records.lookup(**args) == frozenset({"id"})
    other = {**args, "schedule_id": "s2"}
    _records.record_if_absent(**other, key_columns=None)
    assert _records.lookup(**other) is None


def _sched_base(maps: list[dict]) -> str:
    """The base stamp a schedule replays for these rows (empty source fingerprint)."""
    return base_content_hash(build_artifact_from_mappings(
        maps, dest_db="postgresql", source_db="postgresql", source_fingerprint="",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:postgresql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    ).to_dict())


def _stored_schedule(store, stamp: str):
    sched = store.PipelineSchedule.from_dict({
        "id": "s1", "name": "n", "source_connector_id": "src", "source_table": "o",
        "dest_connector_id": "dst", "dest_table": "o_wh", "interval": "hourly",
        "sync_mode": "incremental_append", "workspace_id": "ws", "enabled": False,
        "mappings": _keyed("id"), "approved_decision_artifact_hash": stamp,
    })
    store._save_all([sched])
    return sched


def _run_on(record_reader, maps, stamp):
    from types import SimpleNamespace

    from src.transfer.engine_identity import _stamp_key_record

    request = SimpleNamespace(workspace_id="ws", decision_stamp_schedule_id="s1",
                              destination=SimpleNamespace(connector_id="dst2", table="o_wh2"))
    return _enforce(maps, stamp, record=None, source_fingerprint="",
                    stamp_key_record=lambda s: _stamp_key_record(request, s))


@pytest.mark.parametrize("patch", [
    {"dest_table": "o_wh2"},
    {"dest_connector_id": "dst2"},
    {},
])
def test_an_edit_never_carries_an_old_stamp_onto_a_moved_key(_records, patch):
    """Round 2 item 1 repro: PATCH the route (or nothing else) with the key moved
    to ``code`` and the old stamp resupplied: the stamp is dropped with a plain
    note, and the stamp's record stays the unedited schedule's key."""
    import services.schedule_store as store

    stamp = _sched_base(_keyed("id"))
    _stored_schedule(store, stamp)
    warnings: list[str] = []
    updated = store.update_schedule(
        "s1", {**patch, "mappings": _keyed("code"), "approved_decision_artifact_hash": stamp},
        warnings=warnings,
    )
    assert updated.approved_decision_artifact_hash == ""
    assert any("Run Validate again" in w for w in warnings), warnings
    assert _records.lookup(workspace_id="ws", schedule_id="s1", stamp=stamp) == frozenset({"id"})
    # A run that still carries the old stamp (a stored request) is refused.
    err, art = _run_on(None, _keyed("code"), stamp)
    assert err and "key columns changed" in err, err
    assert art is None


def test_an_unedited_pre_upgrade_schedule_still_runs(_records):
    import services.schedule_runner as runner  # noqa: F401 - the run's own record write
    import services.schedule_store as store

    stamp = _sched_base(_keyed("id"))
    sched = _stored_schedule(store, stamp)
    _records.record_schedule(sched, trusted_rows=True)  # what the scheduler does first
    err, art = _run_on(None, _keyed("id"), stamp)
    assert err is None, err
    assert art is not None


def test_a_stamp_a_new_schedule_brings_names_no_key_and_a_failed_write_fails_the_save(
    _records, monkeypatch,
):
    stamp = _base(_keyed("id"))
    sched = {"id": "s9", "approved_decision_artifact_hash": stamp, "workspace_id": "ws",
             "mappings": _keyed("id")}
    _records.record_schedule(sched, trusted_rows=False, strict=True)
    _records.record_schedule(sched, trusted_rows=True)  # the scheduler cannot launder it
    assert _records.lookup(workspace_id="ws", schedule_id="s9", stamp=stamp) is None

    def broken(**_kw):
        raise OSError("store down")

    monkeypatch.setattr(_records, "record_if_absent", broken)
    with pytest.raises(OSError):
        _records.record_schedule({**sched, "id": "s10"}, trusted_rows=False, strict=True)


# -- round 2 item 2: no record means the stored Decision Artifact decides ------------


def test_without_a_record_the_stored_artifact_decides_and_none_refuses(_records, monkeypatch):
    from services import mongodb_service

    memory = mongodb_service.MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", memory)
    from types import SimpleNamespace

    from src.transfer.engine_identity import _stamp_key_record

    stamp = _base(_keyed("id"))
    request = SimpleNamespace(workspace_id="ws", decision_stamp_schedule_id="",
                              destination=SimpleNamespace(connector_id="d", table="t"))
    reader = lambda s: _stamp_key_record(request, s)  # noqa: E731
    # No stored artifact: refused even for an unkeyed Map (the key may have been removed).
    err, _ = _enforce(_keyed(), stamp, record=None, stamp_key_record=reader)
    assert err and "re-Validate this route" in err, err
    # The artifact the previous release stored for that stamp keeps the key flag.
    stored = {**_art(_keyed("id")).to_dict(), "content_hash": stamp}
    memory._jobs["j1"] = {"_id": "j1", "workspace_id": "ws", "decision_artifact_hash": stamp,
                          "decision_artifact": stored}
    err, _ = _enforce(_keyed(), stamp, record=None, stamp_key_record=reader)
    assert err and "key columns changed" in err, err
    err, _ = _enforce(_keyed("code"), stamp, record=None, stamp_key_record=reader)
    assert err and "key columns changed" in err, err
    assert _enforce(_keyed("id"), stamp, record=None, stamp_key_record=reader)[0] is None
    # Interim: an unkeyed stored decision runs an unkeyed Map; a keyed one never.
    interim = _interim(_keyed())
    memory._jobs["j2"] = {"_id": "j2", "workspace_id": "ws", "decision_artifact_hash": interim,
                          "decision_artifact": {**_art(_keyed()).to_dict(), "content_hash": interim}}
    assert _enforce(_keyed(), interim, record=None, stamp_key_record=reader)[0] is None


# -- round 2 item 4: a wider live column on the stamp-only path -----------------------


def test_the_stamp_only_path_lets_a_same_family_wider_live_column_pass():
    """As before review 3: a live column wider in the same type family is
    harmless for writes on the stamp-only path; narrowing, another type and an
    extra column stay drift there, and the payload path stays exact."""
    from services.schema_fingerprint import fingerprint_schema

    maps = [
        {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "BIGINT", "create_new": True},
        {"source": "name", "target": "name", "source_type": "VARCHAR(40)", "target_type": "VARCHAR(40)",
         "create_new": True},
    ]
    art = build_artifact_from_mappings(
        maps, dest_db="mysql", source_db="postgresql", source_fingerprint="",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:mysql",
        tenant_id="anonymous", artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )

    def gate(live: dict[str, str], payload: bool = False):
        return enforce_decision_artifact(
            mappings=maps, dest_db="mysql", source_db="postgresql",
            approved_content_hash=art.content_hash,
            artifact_payload=art.to_dict() if payload else None,
            source_fingerprint="", dest_fingerprint=fingerprint_schema(list(live), live),
            destination_table_exists=True, dest_column_names=list(live),
            source_column_names=["id", "name"], live_column_types=live,
            sync_mode="incremental_append", error_policy="quarantine",
        )

    planned = {"id": "bigint", "name": "varchar(40)"}
    assert gate(planned)[0] is None
    assert gate({**planned, "name": "varchar(80)"})[0] is None
    for drifted in ({**planned, "name": "varchar(5)"}, {**planned, "id": "int"},
                    {**planned, "id": "text"}, {**planned, "note": "int"}):
        err, _ = gate(drifted)
        assert err and "drifted" in err.lower(), (drifted, err)
    err, _ = gate({**planned, "name": "varchar(80)"}, payload=True)
    assert err and "drifted" in err.lower(), err
