"""MongoDB URI normalization — ensures connection strings carry authSource and ssl."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mongodb_common import normalize_mongodb_connection_string


def test_appends_auth_source_and_database_from_form():
    # Pasted connection strings with a separate Database field default authSource
    # to the database name, and use the database as the default database path.
    uri = normalize_mongodb_connection_string(
        "mongodb://mongo:pass@mongodb.railway.internal:27017",
        database="trueresume",
    )
    assert "trueresume" in uri
    assert "authSource=trueresume" in uri


def test_preserves_existing_auth_source():
    uri = normalize_mongodb_connection_string(
        "mongodb://mongo:pass@mongodb.railway.internal:27017/trueresume?authSource=admin",
        database="trueresume",
    )
    assert "authSource=admin" in uri
    assert "authSource=trueresume" not in uri


def test_explicit_auth_source_override():
    uri = normalize_mongodb_connection_string(
        "mongodb://mongo:pass@mongodb.railway.internal:27017/trueresume",
        database="trueresume",
        auth_source="admin",
    )
    assert "authSource=admin" in uri
    assert "authSource=trueresume" not in uri


def test_appends_ssl_when_requested():
    uri = normalize_mongodb_connection_string(
        "mongodb://mongo:pass@mongodb.railway.internal:27017/trueresume",
        database="trueresume",
        ssl=True,
    )
    assert "ssl=true" in uri


def test_builds_uri_from_host_port_user_pass():
    uri = normalize_mongodb_connection_string(
        "",
        database="dataflow",
        host="localhost",
        port=27017,
        username="user",
        password="pass",
    )
    assert uri.startswith("mongodb://user:pass@localhost:27017")
    assert "authSource=dataflow" in uri


def test_injects_form_credentials_into_host_only_uri():
    """Railway-style paste: host URI without userinfo + form username/password."""
    uri = normalize_mongodb_connection_string(
        "mongodb://mongodb.railway.internal:27017/trueresume",
        database="trueresume",
        username="mongo",
        password="s#cret!",
        auth_source="admin",
    )
    assert "mongo:" in uri
    assert "@mongodb.railway.internal:27017" in uri
    assert "authSource=admin" in uri
    # Special characters in password must be percent-encoded.
    assert "s%23cret" in uri or "s%23cret%21" in uri


# ---------------------------------------------------------------------------
# Regression: shared pooled client must self-heal after any code path closes
# it, and CDC streams must own a dedicated (uncached) client so their close()
# never poisons the process pool. (Bug: "Cannot use MongoClient after close".)
# ---------------------------------------------------------------------------


def test_pooled_client_self_heals_after_close():
    """A closed cached client is evicted and rebuilt, never handed back dead."""
    from connectors import mongodb_common as mc

    conn = "mongodb://localhost:27017/selfheal_test"
    mc._mongo_client_cache.pop(conn, None)
    first = mc._mongo_client(conn)
    assert first is mc._mongo_client(conn)  # pooled: same instance reused

    # Simulate a sibling path closing the shared client.
    first.close()
    assert mc._client_is_closed(first) is True

    rebuilt = mc._mongo_client(conn)
    assert rebuilt is not first
    assert mc._client_is_closed(rebuilt) is False
    mc.close_mongo_client(conn)
    assert conn not in mc._mongo_client_cache


def test_close_mongo_client_evicts_and_is_idempotent():
    from connectors import mongodb_common as mc

    conn = "mongodb://localhost:27017/evict_test"
    client = mc._mongo_client(conn)
    assert conn in mc._mongo_client_cache
    mc.close_mongo_client(conn)
    assert conn not in mc._mongo_client_cache
    assert mc._client_is_closed(client) is True
    # Second call must not raise on an already-evicted key.
    mc.close_mongo_client(conn)


def test_new_mongo_client_is_not_pooled():
    """CDC streams get an isolated client whose close() cannot affect the pool."""
    from connectors import mongodb_common as mc

    conn = "mongodb://localhost:27017/dedicated_test"
    mc._mongo_client_cache.pop(conn, None)
    pooled = mc._mongo_client(conn)
    dedicated = mc._new_mongo_client(conn)
    assert dedicated is not pooled
    assert conn in mc._mongo_client_cache  # dedicated client never entered cache

    dedicated.close()  # stream shutdown
    # Pool client is untouched and still usable.
    assert mc._client_is_closed(pooled) is False
    assert mc._mongo_client(conn) is pooled
    mc.close_mongo_client(conn)
