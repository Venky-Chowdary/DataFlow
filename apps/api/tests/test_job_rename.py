"""Job display-name uniqueness (rename)."""

from services.mongodb_service import MemoryMongoDBService, _job_name_key


def test_job_name_key_normalizes_case_and_space():
    assert _job_name_key("  Payroll Sync  ") == "payroll sync"
    assert _job_name_key("ABC") == _job_name_key("abc")


def test_is_job_name_taken_case_insensitive_within_workspace():
    mongo = MemoryMongoDBService()
    a = mongo.create_transfer_job({
        "name": "Nightly customers",
        "workspace_id": "ws1",
        "source_name": "customers",
        "destination_collection": "cust",
    })
    b = mongo.create_transfer_job({
        "name": "Other job",
        "workspace_id": "ws1",
        "source_name": "orders",
        "destination_collection": "ord",
    })

    assert mongo.is_job_name_taken("nightly customers", workspace_id="ws1", exclude_job_id=b)
    assert not mongo.is_job_name_taken("nightly customers", workspace_id="ws1", exclude_job_id=a)
    assert not mongo.is_job_name_taken("Brand new name", workspace_id="ws1", exclude_job_id=b)
    # Other workspace does not collide
    assert not mongo.is_job_name_taken("nightly customers", workspace_id="ws2", exclude_job_id=None)


def test_rename_fields_persist_name_key():
    mongo = MemoryMongoDBService()
    jid = mongo.create_transfer_job({
        "name": "Old",
        "workspace_id": "",
        "source_name": "t",
        "destination_collection": "d",
    })
    assert mongo.update_job_fields(jid, {"name": "New Label", "name_key": _job_name_key("New Label")})
    job = mongo.get_job(jid)
    assert job["name"] == "New Label"
    assert job["name_key"] == "new label"
    assert mongo.is_job_name_taken("new label", workspace_id="", exclude_job_id="other")
