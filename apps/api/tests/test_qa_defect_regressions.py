"""QA defects: saved host, upsert key, procedure URL, source zone, timestamp precision.

Each case is the route the report named. A pass here is that fixture only.
"""

from __future__ import annotations

from src.transfer.models import EndpointConfig


def test_job_keeps_saved_bore_host_when_the_form_sends_localhost(monkeypatch):
    from transfer import adapters
    from transfer.adapters import resolve_connector_config

    saved = {
        "host": "xyz.bore.pub",
        "port": 41234,
        "database": "app",
        "schema": "",
        "username": "u",
        "password": "secret",
        "connection_string": "",
        "type": "mysql",
    }
    monkeypatch.setattr(adapters, "_lookup_saved_connector", lambda *_a, **_k: saved)
    ep = EndpointConfig(
        format="mysql",
        connector_id="conn-bore",
        host="localhost",
        port=3306,
    )
    cfg = resolve_connector_config(ep)
    assert cfg["host"] == "xyz.bore.pub"
    assert cfg["port"] == 41234


def test_a_real_inline_host_still_overrides_the_saved_connector(monkeypatch):
    from transfer import adapters
    from transfer.adapters import resolve_connector_config

    saved = {
        "host": "xyz.bore.pub",
        "port": 41234,
        "database": "app",
        "type": "mysql",
        "username": "u",
        "password": "secret",
    }
    monkeypatch.setattr(adapters, "_lookup_saved_connector", lambda *_a, **_k: saved)
    ep = EndpointConfig(
        format="mysql",
        connector_id="conn-bore",
        host="other.example.com",
        port=3307,
    )
    cfg = resolve_connector_config(ep)
    assert cfg["host"] == "other.example.com"
    assert cfg["port"] == 3307


def test_a_saved_localhost_connector_stays_localhost(monkeypatch):
    from transfer import adapters
    from transfer.adapters import resolve_connector_config

    saved = {
        "host": "localhost",
        "port": 3306,
        "database": "app",
        "type": "mysql",
        "username": "u",
        "password": "secret",
    }
    monkeypatch.setattr(adapters, "_lookup_saved_connector", lambda *_a, **_k: saved)
    ep = EndpointConfig(format="mysql", connector_id="local", host="localhost", port=3306)
    cfg = resolve_connector_config(ep)
    assert cfg["host"] == "localhost"
    assert cfg["port"] == 3306


def test_omitted_host_does_not_become_localhost_before_merge():
    ep = EndpointConfig.from_dict("database", {"format": "mysql", "connector_id": "c1"})
    assert ep.host == ""


def test_catalog_primary_key_maps_when_every_column_is_mapped():
    from services.primary_key import mapped_catalog_upsert_key

    src, tgt = mapped_catalog_upsert_key(
        ["id"],
        [{"source": "id", "target": "order_id"}],
    )
    assert src == ["id"]
    assert tgt == ["order_id"]


def test_catalog_primary_key_refuses_a_partial_composite():
    from services.primary_key import mapped_catalog_upsert_key

    assert mapped_catalog_upsert_key(
        ["org_id", "id"],
        [{"source": "id", "target": "id"}],
    ) == ([], [])


def test_procedure_url_uses_format_when_type_was_not_stamped():
    from connectors.generic_sql import _build_url

    url = _build_url(
        {
            "format": "postgresql",
            "host": "db.example",
            "port": 5432,
            "database": "app",
            "username": "u",
            "password": "p",
        }
    )
    rendered = str(url)
    assert "db.example" in rendered
    assert "localhost" not in rendered


def test_declared_zone_clears_naive_mysql_timestamp_into_timestamptz():
    from services.coercion_probe import analyze_coercion
    from src.ai.copilot.transfer_tools import (
        _declare_zone_on_schema,
        _stamp_zone_transform,
    )

    rows, declared = _declare_zone_on_schema(
        [
            {
                "name": "created_at",
                "inferred_type": "TIMESTAMPTZ",
                "samples": ["2024-06-01 12:00:00"],
            }
        ],
        "America/New_York",
    )
    assert "created_at" in declared
    assert rows[0]["inferred_type"] == "TIMESTAMPTZ"
    mappings = _stamp_zone_transform(
        [{"source": "created_at", "target": "created_at", "transform": "none"}],
        "America/New_York",
        declared,
    )
    report = analyze_coercion(
        sample_rows=[{"created_at": "2024-06-01 12:00:00"}],
        mappings=mappings,
        source_types={"created_at": rows[0]["inferred_type"]},
        dest_types={"created_at": "TIMESTAMPTZ"},
        dest_db_type="postgresql",
        table_exists=True,
    )
    assert report["has_blocking_failures"] is False


def test_sqlserver_timestamp_ntz7_fits_postgres_microsecond_ceiling():
    from services.type_system import temporal_precision_would_narrow

    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", "TIMESTAMP(6)", dest_db="postgresql"
        )
        is False
    )
    assert (
        temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", "TIMESTAMP(3)", dest_db="postgresql"
        )
        is True
    )


def test_sftp_handshake_retries_without_strict_kex(monkeypatch):
    import paramiko

    from connectors.sftp_common import SFTPConfig, _open_sftp_transport

    calls: list[bool] = []

    class _Transport:
        def __init__(self, sock, strict_kex=True, server_sig_algs=True, **_kwargs):
            self.strict_kex = strict_kex
            self.server_sig_algs = server_sig_algs
            calls.append(strict_kex)

        def start_client(self, timeout=30):
            if self.strict_kex:
                raise paramiko.SSHException("Connection closed before auth")

        def close(self):
            return None

        def get_remote_server_key(self):
            return None

    import socket

    monkeypatch.setattr(paramiko, "Transport", _Transport)
    # ``socket`` is imported inside the handshake, so the stdlib module is the
    # object the retry dials. A dotted path on sftp_common does not exist.
    monkeypatch.setattr(socket, "create_connection", lambda *_a, **_k: object())
    monkeypatch.setattr("connectors.sftp_common.verify_host_key", lambda *_a, **_k: None)
    cfg = SFTPConfig()
    cfg.host = "files.example"
    cfg.port = 22
    cfg.username = "u"
    cfg.password = "p"
    transport = _open_sftp_transport(cfg)
    assert calls == [True, False]
    assert transport.strict_kex is False


def test_sftp_handshake_retries_after_eof_before_auth(monkeypatch):
    """A socket drop during kex is EOFError, which is not an SSHException.

    Paramiko 5 raises that when the peer closes before any auth packet.
    The retry must still reach a login attempt.
    """
    import socket

    import paramiko

    from connectors.sftp_common import SFTPConfig, _open_sftp_transport

    calls: list[tuple[bool, bool]] = []

    class _Transport:
        def __init__(self, sock, strict_kex=True, server_sig_algs=True, **_kwargs):
            self.strict_kex = strict_kex
            self.server_sig_algs = server_sig_algs
            calls.append((strict_kex, server_sig_algs))

        def start_client(self, timeout=30):
            if self.strict_kex or self.server_sig_algs:
                raise EOFError("Connection reset by peer")

        def close(self):
            return None

        def get_remote_server_key(self):
            return None

    monkeypatch.setattr(paramiko, "Transport", _Transport)
    monkeypatch.setattr(socket, "create_connection", lambda *_a, **_k: object())
    monkeypatch.setattr("connectors.sftp_common.verify_host_key", lambda *_a, **_k: None)
    cfg = SFTPConfig()
    cfg.host = "files.example"
    cfg.port = 22
    transport = _open_sftp_transport(cfg)
    assert calls == [(True, True), (False, True), (False, False)]
    assert transport.server_sig_algs is False


def test_sftp_host_key_refusal_is_not_retried(monkeypatch):
    import socket

    import paramiko

    from connectors.sftp_common import SFTPConfig, _open_sftp_transport

    calls: list[bool] = []

    class _Transport:
        def __init__(self, sock, strict_kex=True, server_sig_algs=True, **_kwargs):
            self.strict_kex = strict_kex
            self.server_sig_algs = server_sig_algs
            calls.append(strict_kex)
            self.closed = False

        def start_client(self, timeout=30):
            return None

        def close(self):
            self.closed = True

    monkeypatch.setattr(paramiko, "Transport", _Transport)
    monkeypatch.setattr(socket, "create_connection", lambda *_a, **_k: object())

    def _refuse(*_a, **_k):
        raise RuntimeError("SFTP host key is not trusted")

    monkeypatch.setattr("connectors.sftp_common.verify_host_key", _refuse)
    cfg = SFTPConfig()
    cfg.host = "files.example"
    cfg.port = 22
    try:
        _open_sftp_transport(cfg)
        raised = False
    except RuntimeError as exc:
        raised = "not trusted" in str(exc)
    assert raised
    assert calls == [True]


def test_mongo_named_epoch_is_an_instant_and_an_id_stays_bigint():
    from services.schema_introspect import _sample_logical_type

    assert _sample_logical_type(1_717_200_000_000, "created_at") == "TIMESTAMPTZ"
    assert _sample_logical_type(1_717_200_000, "updated_at") == "TIMESTAMPTZ"
    assert _sample_logical_type(1_717_200_000_000, "_id") == "BIGINT"
    assert _sample_logical_type(42, "created_at") == "INTEGER"


def test_empty_string_stays_present_on_text_and_null_on_numeric():
    """MySQL '' into VARCHAR NOT NULL is a value. '' into INTEGER NOT NULL is not."""
    from preflight.gates import _sample_is_sql_null_for_not_null

    assert _sample_is_sql_null_for_not_null("", "VARCHAR") is False
    assert _sample_is_sql_null_for_not_null("", "TEXT") is False
    assert _sample_is_sql_null_for_not_null("", "INTEGER") is True
    assert _sample_is_sql_null_for_not_null(None, "VARCHAR") is True


def test_kafka_lists_user_topics_when_none_is_named(monkeypatch):
    from services.schema_introspect import _introspect_kafka

    monkeypatch.setattr(
        "connectors.kafka_reader.list_topics",
        lambda _cfg: ["orders", "customers"],
    )
    out = _introspect_kafka(host="broker.example", port=9092)
    assert out["ok"] is True
    assert out["tables"] == ["orders", "customers"]


def test_neo4j_lists_labels(monkeypatch):
    from services.schema_introspect import _introspect_neo4j

    monkeypatch.setattr(
        "connectors.neo4j.list_labels",
        lambda **_k: ["Customer", "Order"],
    )
    out = _introspect_neo4j(host="graph.example", port=7474)
    assert out["ok"] is True
    assert out["tables"] == ["Customer", "Order"]


def test_qdrant_lists_collections_when_none_is_named(monkeypatch):
    class _Resp:
        status_code = 200
        content = b"{}"

        def json(self):
            return {"result": {"collections": [{"name": "docs"}]}}

    class _Sess:
        def get(self, *_a, **_k):
            return _Resp()

    monkeypatch.setattr(
        "connectors.qdrant_writer.qdrant_rest",
        lambda _cfg: (_Sess(), "http://q.example:6333", {}),
    )
    from services.schema_introspect import _introspect_qdrant

    out = _introspect_qdrant(host="q.example")
    assert out["ok"] is True
    assert out["tables"] == ["docs"]


def test_weaviate_lists_classes_and_named_properties(monkeypatch):
    class _Resp:
        content = b"{}"

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "classes": [
                    {
                        "class": "Article",
                        "properties": [{"name": "title", "dataType": ["text"]}],
                    }
                ]
            }

    monkeypatch.setattr("requests.get", lambda *_a, **_k: _Resp())
    from services.schema_introspect import _introspect_weaviate

    listed = _introspect_weaviate(host="wv.example")
    assert listed["tables"] == ["Article"]
    named = _introspect_weaviate(host="wv.example", table="Article")
    assert named["columns"][0]["name"] == "title"


def test_elasticsearch_lists_user_indices(monkeypatch):
    class _Indices:
        @staticmethod
        def get_alias(index="*"):
            return {"orders": {}, ".security": {}}

    class _Client:
        indices = _Indices()

    monkeypatch.setattr(
        "connectors.elasticsearch_reader._client",
        lambda _cfg: _Client(),
    )
    from services.schema_introspect import _introspect_elasticsearch

    out = _introspect_elasticsearch(host="es.example")
    assert out["ok"] is True
    assert out["tables"] == ["orders"]


def test_cdc_uses_the_catalog_primary_key_when_the_contract_omits_it(monkeypatch):
    from src.transfer.cdc_transfer import _catalog_cdc_primary_key

    def _rich(*_a, **_k):
        return ({"id": "INTEGER"}, {"id": False}, {"primary_key_columns": ["id"]})

    monkeypatch.setattr(
        "src.transfer.adapters._introspect_table_schema_rich",
        _rich,
    )
    assert _catalog_cdc_primary_key(
        "postgresql",
        {},
        "orders",
        [{"source": "id", "target": "id"}],
    ) == "id"
    assert _catalog_cdc_primary_key(
        "postgresql",
        {},
        "orders",
        [{"source": "name", "target": "name"}],
    ) == ""


def test_sqlserver_object_list_comes_from_the_catalog(monkeypatch):
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    def _schema(*_a, **kwargs):
        assert kwargs.get("table", "") == ""
        return {"ok": True, "tables": ["dbo.orders", "sales.customers"], "columns": []}

    monkeypatch.setattr("services.schema_introspect.introspect_schema", _schema)
    out = introspect_endpoint(
        EndpointConfig(kind="database", format="sqlserver", host="mssql.example", database="app")
    )
    assert out["connected"] is True
    assert [item["name"] for item in out["objects"]] == ["dbo.orders", "sales.customers"]
