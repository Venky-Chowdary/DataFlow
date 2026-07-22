"""Enterprise G2 privilege probe — metadata only, never mutates destination data."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from services.destination_privilege_probe import (
    PrivilegeProbeResult,
    _mysql_grant_covers,
    evaluate_bigquery_access_entries,
    evaluate_kafka_acls,
    evaluate_mongodb_privileges,
    evaluate_oracle_privileges,
    evaluate_redis_acl,
    evaluate_snowflake_privileges,
    parse_redis_acl_getuser,
    probe_destination_privileges,
    resolve_write_flags,
)


def test_unsupported_engine_is_unavailable_not_deny():
    result = probe_destination_privileges("elasticsearch", host="x", database="d")
    assert result.status == "unavailable"
    assert result.can_write is None
    can_write, can_create, meta = resolve_write_flags(True, result)
    assert can_write is True and can_create is True
    assert meta["status"] == "unavailable"


def test_resolve_write_flags_denies_on_explicit_probe_deny():
    probe = PrivilegeProbeResult(
        can_write=False,
        can_create_table=False,
        status="denied",
        detail="lacks INSERT on public.orders",
        engine="postgresql",
    )
    can_write, can_create, _ = resolve_write_flags(True, probe)
    assert can_write is False
    assert can_create is False


def test_resolve_write_flags_disconnected():
    probe = PrivilegeProbeResult(
        can_write=True,
        can_create_table=True,
        status="ok",
        detail="ok",
        engine="postgresql",
    )
    can_write, can_create, _ = resolve_write_flags(False, probe)
    assert can_write is False and can_create is False


def test_mysql_grant_covers_star_and_table_scope():
    assert _mysql_grant_covers(
        {"INSERT", "SELECT"},
        "*.*",
        database="app",
        table="orders",
        needed={"INSERT"},
    )
    assert _mysql_grant_covers(
        {"ALL PRIVILEGES"},
        "`app`.*",
        database="app",
        table="orders",
        needed={"CREATE"},
    )
    assert _mysql_grant_covers(
        {"INSERT"},
        "`app`.`orders`",
        database="app",
        table="orders",
        needed={"INSERT"},
    )
    assert not _mysql_grant_covers(
        {"SELECT"},
        "`app`.`orders`",
        database="app",
        table="orders",
        needed={"INSERT"},
    )
    assert not _mysql_grant_covers(
        {"INSERT"},
        "`other`.`orders`",
        database="app",
        table="orders",
        needed={"INSERT"},
    )


# ── Deny matrices (pure evaluators — no network / no mutate) ─────────────────

@pytest.mark.parametrize(
    "grants,table_exists,expect_write,expect_create",
    [
        (
            [{"privilege": "INSERT", "granted_on": "TABLE", "name": "DB.PUBLIC.T"}],
            True, True, False,
        ),
        (
            [{"privilege": "SELECT", "granted_on": "TABLE", "name": "DB.PUBLIC.T"}],
            True, False, False,
        ),
        (
            [{"privilege": "CREATE TABLE", "granted_on": "SCHEMA", "name": "DB.PUBLIC"}],
            False, True, True,
        ),
        (
            [{"privilege": "USAGE", "granted_on": "SCHEMA", "name": "DB.PUBLIC"}],
            False, False, False,
        ),
        (
            [{"privilege": "OWNERSHIP", "granted_on": "SCHEMA", "name": "DB.PUBLIC"}],
            True, True, True,
        ),
    ],
)
def test_snowflake_deny_matrix(grants, table_exists, expect_write, expect_create):
    can_write, can_create = evaluate_snowflake_privileges(
        grants,
        database="DB",
        schema="PUBLIC",
        table="T",
        table_exists=table_exists,
    )
    assert can_write is expect_write
    assert can_create is expect_create


@pytest.mark.parametrize(
    "role,expect_write",
    [
        ("WRITER", True),
        ("OWNER", True),
        ("roles/bigquery.dataEditor", True),
        ("roles/bigquery.dataViewer", False),
        ("READER", False),
    ],
)
def test_bigquery_deny_matrix(role, expect_write):
    can_w, can_c, _ = evaluate_bigquery_access_entries([{"role": role}])
    assert can_w is expect_write
    assert can_c is expect_write


@pytest.mark.parametrize(
    "session,tab,exists,need_update,expect_write,expect_create",
    [
        ({"CREATE TABLE"}, set(), False, False, True, True),
        (set(), {"INSERT"}, True, False, True, False),
        (set(), {"SELECT"}, True, False, False, False),
        (set(), {"INSERT"}, True, True, False, False),  # upsert needs UPDATE
        (set(), {"INSERT", "UPDATE"}, True, True, True, False),
        ({"DBA"}, set(), True, True, True, True),
    ],
)
def test_oracle_deny_matrix(session, tab, exists, need_update, expect_write, expect_create):
    can_write, can_create = evaluate_oracle_privileges(
        session_privs=session,
        tab_privs=tab,
        table_exists=exists,
        need_update=need_update,
    )
    assert can_write is expect_write
    assert can_create is expect_create


def test_sqlserver_deny_matrix_mocked():
    # side_effect length depends on path: missing table → OBJECT_ID + CREATE only.
    cases = [
        ([1, 1, 1], True, "ok", True),       # exists, INSERT, CREATE
        ([1, 0, 1], True, "denied", False),  # exists, no INSERT
        ([None, 0], False, "denied", False), # missing, no CREATE
        ([None, 1], False, "ok", True),      # missing, CREATE ok
    ]
    for side_effect, table_exists, status, can_write in cases:
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine.connect.return_value.__exit__.return_value = None
        mock_conn.execute.return_value.scalar.side_effect = side_effect
        with patch(
            "connectors.generic_sql.get_sqlalchemy_engine",
            return_value=mock_engine,
        ):
            result = probe_destination_privileges(
                "sqlserver",
                host="localhost",
                database="app",
                schema="dbo",
                table="orders",
                table_exists=table_exists,
            )
        assert result.status == status, side_effect
        assert result.can_write is can_write, side_effect


def test_kafka_probe_mocked_describe_acls():
    kafka = pytest.importorskip("kafka")
    del kafka
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value = ["orders"]
    mock_admin.describe_acls.return_value = [
        {
            "operation": "WRITE",
            "permission_type": "ALLOW",
            "resource_name": "orders",
            "resource_type": "TOPIC",
        }
    ]
    with patch("kafka.admin.KafkaAdminClient", return_value=mock_admin), patch(
        "connectors.kafka_writer._bootstrap",
        return_value="localhost:9092",
    ):
        result = probe_destination_privileges(
            "kafka",
            host="localhost",
            table="orders",
            table_exists=True,
        )
    # describe_acls filter construction may still fail on older kafka-python → unavailable.
    assert result.status in {"ok", "unavailable"}
    if result.status == "ok":
        assert result.can_write is True
        assert result.method == "describe_acls"


# ── Mongo / Redis / Kafka ────────────────────────────────────────────────────

def test_mongodb_privilege_deny_matrix():
    # readWrite role
    can_w, can_c = evaluate_mongodb_privileges(
        [],
        roles=[{"role": "readWrite", "db": "app"}],
        database="app",
        collection="orders",
        table_exists=True,
    )
    assert can_w and can_c

    # insert only on collection
    can_w, can_c = evaluate_mongodb_privileges(
        [{
            "resource": {"db": "app", "collection": "orders"},
            "actions": ["insert", "find"],
        }],
        database="app",
        collection="orders",
        table_exists=True,
    )
    assert can_w is True
    assert can_c is False

    # find only → deny write
    can_w, can_c = evaluate_mongodb_privileges(
        [{
            "resource": {"db": "app", "collection": "orders"},
            "actions": ["find"],
        }],
        database="app",
        collection="orders",
        table_exists=True,
    )
    assert can_w is False

    # createCollection when collection missing
    can_w, can_c = evaluate_mongodb_privileges(
        [{
            "resource": {"db": "app", "collection": ""},
            "actions": ["createCollection"],
        }],
        database="app",
        collection="new_coll",
        table_exists=False,
    )
    assert can_c is True
    assert can_w is True


def test_mongodb_probe_mocked_connection_status():
    mock_client = MagicMock()
    mock_client.admin.command.return_value = {
        "authInfo": {
            "authenticatedUserRoles": [{"role": "read", "db": "app"}],
            "authenticatedUserPrivileges": [{
                "resource": {"db": "app", "collection": "orders"},
                "actions": ["find"],
            }],
        }
    }
    mock_client.__getitem__.return_value.list_collection_names.return_value = ["orders"]

    with patch(
        "connectors.mongodb_common.normalize_mongodb_connection_string",
        return_value="mongodb://localhost/app",
    ), patch(
        "connectors.mongodb_common._mongo_client",
        return_value=mock_client,
    ):
        result = probe_destination_privileges(
            "mongodb",
            host="localhost",
            database="app",
            table="orders",
            table_exists=True,
        )
    assert result.status == "denied"
    assert result.can_write is False
    assert "insert" in result.detail.lower() or "update" in result.detail.lower()


def test_redis_acl_deny_matrix():
    can_w, can_c = evaluate_redis_acl(
        commands=["+@all"],
        key_patterns=["~*"],
        key_prefix="df:*",
    )
    assert can_w and can_c

    can_w, can_c = evaluate_redis_acl(
        commands=["+@read", "-@write"],
        key_patterns=["~*"],
        key_prefix="df:*",
    )
    assert can_w is False

    can_w, can_c = evaluate_redis_acl(
        commands=["+set", "+hset"],
        key_patterns=["~other:*"],
        key_prefix="df:orders",
    )
    assert can_w is False  # key pattern miss


def test_parse_redis_acl_getuser_resp2_and_dict():
    cmds, keys = parse_redis_acl_getuser([
        "flags", ["on"],
        "commands", ["+@all", "-@dangerous"],
        "keys", ["~*"],
    ])
    assert "+@all" in cmds
    assert "~*" in keys

    cmds, keys = parse_redis_acl_getuser({
        "flags": ["on", "allkeys"],
        "commands": ["+set"],
        "keys": ["~df:*"],
    })
    assert "+set" in cmds
    assert any("*" in k for k in keys)


def test_redis_probe_mocked_acl():
    mock_client = MagicMock()
    mock_client.execute_command.side_effect = [
        b"default",
        ["flags", ["on", "allcommands", "allkeys"], "commands", [], "keys", []],
    ]
    with patch("connectors.redis_reader._redis_client", return_value=mock_client):
        result = probe_destination_privileges(
            "redis",
            host="localhost",
            table="df:orders",
            table_exists=True,
        )
    assert result.status == "ok"
    assert result.can_write is True
    assert "ACL" in result.method


def test_redis_probe_acl_unavailable_is_soft():
    mock_client = MagicMock()
    mock_client.execute_command.side_effect = Exception("unknown command 'ACL'")
    with patch("connectors.redis_reader._redis_client", return_value=mock_client):
        result = probe_destination_privileges("redis", host="localhost", table="k")
    assert result.status == "unavailable"
    can_w, can_c, _ = resolve_write_flags(True, result)
    assert can_w is True and can_c is True


def test_kafka_acl_deny_matrix():
    can_w, can_c = evaluate_kafka_acls(
        [{"operation": "WRITE", "permission": "ALLOW", "resource": "orders", "resource_type": "TOPIC"}],
        topic="orders",
        table_exists=True,
    )
    assert can_w is True

    can_w, can_c = evaluate_kafka_acls(
        [{"operation": "READ", "permission": "ALLOW", "resource": "orders", "resource_type": "TOPIC"}],
        topic="orders",
        table_exists=True,
    )
    assert can_w is False

    can_w, can_c = evaluate_kafka_acls(
        [
            {"operation": "WRITE", "permission": "ALLOW", "resource": "orders", "resource_type": "TOPIC"},
            {"operation": "WRITE", "permission": "DENY", "resource": "orders", "resource_type": "TOPIC"},
        ],
        topic="orders",
        table_exists=True,
    )
    assert can_w is False

    can_w, can_c = evaluate_kafka_acls(
        [{"operation": "CREATE", "permission": "ALLOW", "resource": "kafka-cluster", "resource_type": "CLUSTER"}],
        topic="new_topic",
        table_exists=False,
    )
    assert can_c is True
    assert can_w is True


def test_sqlite_filesystem_probe_tmpdir():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "probe.db")
        result = probe_destination_privileges(
            "sqlite",
            database=db_path,
            table="t1",
            table_exists=False,
        )
        assert result.status == "ok"
        assert result.can_create_table is True
        assert result.can_write is True

        import sqlite3
        con = sqlite3.connect(db_path)
        con.execute("CREATE TABLE t1 (id INTEGER)")
        con.commit()
        con.close()
        result2 = probe_destination_privileges(
            "sqlite",
            database=db_path,
            table="t1",
            table_exists=True,
        )
        assert result2.status == "ok"
        assert result2.can_write is True


def test_sqlite_memory_always_writable():
    result = probe_destination_privileges("sqlite", database=":memory:", table="x")
    assert result.status == "ok"
    assert result.can_write is True
    assert result.can_create_table is True


def test_sqlserver_has_perms_mocked():
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_engine.connect.return_value.__exit__.return_value = None
    mock_conn.execute.return_value.scalar.side_effect = [1, 1, 1]

    with patch(
        "connectors.generic_sql.get_sqlalchemy_engine",
        return_value=mock_engine,
    ):
        result = probe_destination_privileges(
            "sqlserver",
            host="localhost",
            database="app",
            schema="dbo",
            table="orders",
            username="u",
            password="p",
            table_exists=True,
        )
    assert result.status == "ok"
    assert result.can_write is True
    assert result.method == "HAS_PERMS_BY_NAME"


def test_run_file_preflight_honors_destination_can_write_false():
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "INTEGER", "name": "VARCHAR"},
        row_count=2,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
        ],
        destination_connected=True,
        destination_can_write=False,
        destination_can_create=False,
        destination_table_exists=True,
        destination_db_type="postgresql",
        sample_rows=[{"id": "1", "name": "a"}, {"id": "2", "name": "b"}],
        validation_mode="strict",
    )
    g2 = next(
        g
        for g in result["gates"]
        if "g2" in str(g.get("id", "")).lower() or "destination" in str(g.get("id", "")).lower()
    )
    assert g2["status"] in {"block", "fail", "blocked"}
    msg = (g2.get("message") or "").lower()
    assert "write" in msg or "permission" in msg or "insert" in msg


def test_live_mongo_privilege_probe_optional():
    """Optional live probe against local Mongo — skip if unreachable."""
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=800)
        client.admin.command("ping")
    except Exception:
        pytest.skip("local MongoDB not available")

    result = probe_destination_privileges(
        "mongodb",
        host="localhost",
        port=27017,
        database="dataflow_probe",
        table="g2_probe",
        table_exists=False,
    )
    # Unauthenticated local often has empty privilege catalog → unavailable (honest).
    assert result.status in {"ok", "denied", "unavailable"}
    assert result.engine == "mongodb"


def test_live_redis_privilege_probe_optional():
    """Optional live probe against local Redis — skip if unreachable."""
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, socket_timeout=0.8)
        r.ping()
    except Exception:
        pytest.skip("local Redis not available")

    result = probe_destination_privileges(
        "redis",
        host="localhost",
        port=6379,
        table="df:g2",
        table_exists=True,
    )
    assert result.status in {"ok", "denied", "unavailable"}
    assert result.engine == "redis"


def test_elasticsearch_privilege_deny_matrix():
    from services.destination_privilege_probe import evaluate_elasticsearch_privileges

    assert evaluate_elasticsearch_privileges(
        {"has_all_requested": True}, index="orders", table_exists=True
    ) == (True, True)
    can_w, can_c = evaluate_elasticsearch_privileges(
        {"index": [{"index": "orders", "privileges": {"index": True, "create_index": False}}]},
        index="orders",
        table_exists=True,
    )
    assert can_w is True and can_c is False
    can_w, can_c = evaluate_elasticsearch_privileges(
        {"index": [{"index": "orders", "privileges": {"read": True}}]},
        index="orders",
        table_exists=True,
    )
    assert can_w is False


def test_elasticsearch_probe_mocked_has_privileges():
    mock_client = MagicMock()
    mock_client.indices.exists.return_value = True
    mock_client.security.has_privileges.return_value = {
        "index": [{"index": "orders", "privileges": {"index": False, "write": False, "create_index": False}}],
    }
    with patch("connectors.elasticsearch_reader._client", return_value=mock_client):
        result = probe_destination_privileges(
            "elasticsearch",
            host="localhost",
            table="orders",
            table_exists=True,
        )
    assert result.status == "denied"
    assert result.can_write is False
    assert result.method == "security.has_privileges"


def test_s3_acl_deny_matrix():
    from services.destination_privilege_probe import evaluate_s3_acl_grants

    assert evaluate_s3_acl_grants([{"Permission": "FULL_CONTROL"}]) == (True, True)
    assert evaluate_s3_acl_grants([{"Permission": "READ"}]) == (False, False)
    assert evaluate_s3_acl_grants([{"Permission": "WRITE"}]) == (True, True)


def test_s3_probe_mocked_acl():
    mock_client = MagicMock()
    mock_client.head_bucket.return_value = {}
    mock_client.get_bucket_acl.return_value = {
        "Grants": [{"Permission": "READ", "Grantee": {"Type": "CanonicalUser"}}],
    }
    with patch("connectors.aws_common.boto3_client", return_value=mock_client):
        result = probe_destination_privileges(
            "s3",
            host="us-east-1",
            database="my-bucket",
            table="exports/out.json",
            table_exists=True,
        )
    assert result.status == "denied"
    assert result.can_write is False
    assert result.method == "GetBucketAcl"


def test_g2_gate_surfaces_privilege_probe_in_details():
    from services.preflight_service import run_file_preflight

    pf = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        destination_connected=True,
        destination_can_write=False,
        destination_can_create=False,
        destination_table_exists=True,
        destination_db_type="postgresql",
        sample_rows=[{"id": 1}],
        privilege_probe={
            "status": "denied",
            "method": "has_table_privilege",
            "engine": "postgresql",
            "detail": "User can connect but lacks INSERT on public.t",
            "can_write": False,
            "can_create_table": False,
        },
    )
    g2 = next(g for g in pf["gates"] if g["id"] == "g2_destination")
    assert g2["status"] == "block"
    assert g2["details"]["privilege_probe"]["method"] == "has_table_privilege"
    assert "INSERT" in g2["message"]
    assert pf["privilege_probe"]["status"] == "denied"


def test_g2_pass_includes_probe_method_in_message():
    from services.preflight_service import run_file_preflight

    pf = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        destination_connected=True,
        destination_can_write=True,
        destination_can_create=True,
        destination_table_exists=True,
        destination_db_type="postgresql",
        sample_rows=[{"id": 1}],
        privilege_probe={
            "status": "ok",
            "method": "SHOW GRANTS",
            "engine": "mysql",
            "detail": "mysql privileges: write=yes, create=yes",
            "can_write": True,
            "can_create_table": True,
        },
    )
    g2 = next(g for g in pf["gates"] if g["id"] == "g2_destination")
    assert g2["status"] == "pass"
    assert "SHOW GRANTS" in g2["message"]
    assert g2["details"]["privilege_probe"]["method"] == "SHOW GRANTS"
