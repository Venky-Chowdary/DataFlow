"""D135: one authSource resolution for every MongoDB path.

Connectors Test probed ``authSource=admin`` first and went green, while every
read / write / introspect path built its URI with the connector *database* as
authSource. A user defined in ``admin`` (the managed-Mongo norm) therefore
tested green, listed collections, and then failed every read with
"Authentication failed (18)".
"""

from __future__ import annotations

import os
import uuid

import pytest

from connectors.mongodb_common import normalize_mongodb_connection_string

_LIVE_URI = os.environ.get("DATAFLOW_LIVE_MONGO_URI", "")


def test_credentials_without_auth_source_default_to_admin():
    uri = normalize_mongodb_connection_string(
        "", database="appdb", host="db.example", port=27017, username="u", password="p",
    )
    assert "authSource=admin" in uri
    assert "/appdb" in uri


def test_url_and_explicit_auth_source_still_win():
    assert "authSource=appdb" in normalize_mongodb_connection_string(
        "mongodb://u:p@db.example:27017/appdb?authSource=appdb", database="appdb",
    )
    assert "authSource=other" in normalize_mongodb_connection_string(
        "mongodb://u:p@db.example:27017/appdb", database="appdb", auth_source="other",
    )


@pytest.fixture
def admin_user_on_app_db():
    """A user defined in ``admin`` with readWrite on a separate app database."""
    if not _LIVE_URI:
        pytest.skip("DATAFLOW_LIVE_MONGO_URI not set")
    pymongo = pytest.importorskip("pymongo")
    root = pymongo.MongoClient(_LIVE_URI, serverSelectionTimeoutMS=3000)
    try:
        root.admin.command("ping")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"live MongoDB unavailable: {exc}")
    suffix = uuid.uuid4().hex[:10]
    db_name, user, password = f"d135_{suffix}", f"d135_{suffix}", f"pw_{suffix}"
    root.admin.command(
        "createUser", user, pwd=password, roles=[{"role": "readWrite", "db": db_name}],
    )
    root[db_name]["people"].insert_many([{"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}])
    # Credentials ride in the URI (the replica-set seed must survive), the app
    # database is the separate form field — exactly the QA connector shape.
    host_part = _LIVE_URI.split("://", 1)[1].split("/", 1)
    query = host_part[1].split("?", 1)[1] if "?" in host_part[1] else ""
    conn = f"mongodb://{user}:{password}@{host_part[0]}/" + (f"?{query}" if query else "")
    try:
        yield {"connection_string": conn, "database": db_name}
    finally:
        root.drop_database(db_name)
        root.admin.command("dropUser", user)
        root.close()


def test_read_path_authenticates_like_connector_test(admin_user_on_app_db):
    from connectors.mongodb_reader import read_collection_batch
    from src.transfer.adapters import probe_mongodb

    cfg = dict(admin_user_on_app_db)
    ok, msg = probe_mongodb(dict(cfg))
    assert ok, msg

    # The read path gets the connector config as saved (no auth_source field):
    # it must authenticate against the same database the Test used.
    batch = read_collection_batch(cfg=cfg, database=cfg["database"], collection="people")
    assert batch.total_rows == 2
    assert len(batch.rows) == 2


def test_explicit_auth_source_is_honoured_by_connector_test(admin_user_on_app_db):
    from src.transfer.adapters import probe_mongodb

    # An explicit (wrong) auth source must not be silently swapped for admin
    # by Test — otherwise Test and the run paths disagree again.
    cfg = {**admin_user_on_app_db, "auth_source": admin_user_on_app_db["database"]}
    ok, _msg = probe_mongodb(cfg)
    assert not ok
    assert cfg["auth_source"] == admin_user_on_app_db["database"]
