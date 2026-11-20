"""D316: Mongo CDC keyed on a business key checks change-stream pre-images at
preflight (R8: plan predicts Execute).

A delete event's ``documentKey`` is ``_id`` only, so a pipeline keyed on a
business column can address the deleted row only through the pre-image, which
MongoDB records only when the collection has
``changeStreamPreAndPostImages: {enabled: true}`` (MongoDB 6.0+, ``collMod``).
Execute already refused such a run (``mongo_pre_images_disabled``) after
Confirm; g9c passed it because the Mongo probe only asked whether change
streams were available. The gate must give the same text Execute gives.

Live MongoDB replica set; skipped when it is not reachable.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_d315_mongo_cdc_missing_nested_field_pg_e2e import CFG, _ready

pytestmark = pytest.mark.skipif(
    not _ready(), reason="MongoDB replica set (change streams) not reachable"
)


@pytest.fixture
def collection():
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc

    name = "p2d316_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="_id")
    cdc.client[cdc.db_name].create_collection(name)
    cdc.coll.insert_many([{"_id": i, "id": i, "v": f"v{i}"} for i in range(1, 4)])
    try:
        yield cdc, name
    finally:
        cdc.coll.drop()
        cdc.client.close()


def _gates(name: str, key: str):
    from services.preflight_population import _cdc_log_capture_gates

    return _cdc_log_capture_gates(
        [{"name": name, "selected": True, "sync_mode": "cdc", "primary_key": key}],
        source_type="mongodb",
        source_config=dict(CFG),
        source_table=name,
        catalog_primary_key_columns=["_id"],
        mappings=None,
    )


def _execute_refusal(name: str, key: str) -> str:
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc
    from services.cdc_capability import LogCaptureUnavailable

    reader = MongodbChangeStreamCdc(CFG, collection=name, primary_key=key)
    try:
        with pytest.raises(LogCaptureUnavailable) as caught:
            reader._assert_delete_keying_possible()
        return str(caught.value)
    finally:
        reader.client.close()


def test_business_key_without_pre_images_blocks_with_the_execute_text(collection):
    _cdc, name = collection
    gates = _gates(name, "id")
    assert len(gates) == 1
    gate = gates[0]
    assert gate["id"] == "g9c_cdc_log_capture"
    assert gate["status"] == "block", gate
    assert gate["details"]["cause"] == "mongo_pre_images_disabled"
    assert gate["message"] == _execute_refusal(name, "id")
    assert "changeStreamPreAndPostImages" in gate["message"]
    assert "key the pipeline on _id" in gate["message"]


def test_business_key_with_pre_images_passes(collection):
    cdc, name = collection
    cdc.client[cdc.db_name].command(
        "collMod", name, changeStreamPreAndPostImages={"enabled": True}
    )
    gate = _gates(name, "id")[0]
    assert gate["status"] == "pass", gate


def test_id_key_needs_no_pre_images(collection):
    _cdc, name = collection
    gate = _gates(name, "_id")[0]
    assert gate["status"] == "pass", gate
