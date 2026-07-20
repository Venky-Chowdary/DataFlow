"""Schedule store unit tests."""

from datetime import datetime, timedelta, timezone

import pytest

import services.schedule_store as store


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    yield path


def test_create_and_list(temp_store):
    sched = store.create_schedule({
        "name": "Nightly sync",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
    })
    assert sched.enabled
    assert sched.interval == "daily"
    assert len(store.list_schedules()) == 1


def test_assert_signed_contract_fail_closed(temp_store, monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)

    draft = DataContract(name="draft-orders", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    with pytest.raises(ValueError, match="must be SIGNED"):
        store.assert_signed_contract(draft.id, require_signed=True)

    with pytest.raises(ValueError, match="no contract_id"):
        store.assert_signed_contract("", require_signed=True)

    store.assert_signed_contract("", require_signed=False)

    draft.status = ContractStatus.SIGNED
    backend.save_contract(draft)
    store.assert_signed_contract(draft.id, require_signed=True)


def test_create_rejects_unsigned_contract(temp_store, monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    draft = DataContract(name="unsigned", status=ContractStatus.DRAFT)
    backend.save_contract(draft)

    with pytest.raises(ValueError, match="SIGNED"):
        store.create_schedule({
            "name": "Bad",
            "source_connector_id": "a",
            "source_table": "t",
            "dest_connector_id": "b",
            "dest_table": "t2",
            "interval": "daily",
            "contract_id": draft.id,
            "require_signed_contract": True,
        })


def test_create_persists_signed_contract(temp_store, monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    signed = DataContract(name="governed", status=ContractStatus.SIGNED)
    backend.save_contract(signed)

    sched = store.create_schedule({
        "name": "Governed nightly",
        "source_connector_id": "a",
        "source_table": "t",
        "dest_connector_id": "b",
        "dest_table": "t2",
        "interval": "daily",
        "contract_id": signed.id,
        "require_signed_contract": True,
    })
    assert sched.contract_id == signed.id
    assert sched.require_signed_contract is True


def test_due_schedules(temp_store):
    sched = store.create_schedule({
        "name": "Hourly",
        "source_connector_id": "a",
        "source_table": "t1",
        "dest_connector_id": "b",
        "dest_table": "t2",
        "interval": "hourly",
    })
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    store.update_schedule(sched.id, {"next_run_at": past})
    due = store.due_schedules()
    assert any(s.id == sched.id for s in due)


def test_mark_run_updates_next(temp_store):
    sched = store.create_schedule({
        "name": "Weekly",
        "source_connector_id": "a",
        "source_table": "t1",
        "dest_connector_id": "b",
        "dest_table": "t2",
        "interval": "weekly",
    })
    updated = store.mark_schedule_run(sched.id, "job-123")
    assert updated is not None
    assert updated.last_job_id == "job-123"
    assert updated.run_count == 1
    assert updated.next_run_at is not None
