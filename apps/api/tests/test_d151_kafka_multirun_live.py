"""D151 (live broker): repeated runs of one Kafka-source route keep loading.

The staged SQLite overwrite published only on a page count the stream guessed
for a topic of unknown size, so it never published; and a second run into the
table the first created was blocked because an unkeyed message's all-NULL
``_kafka_key`` mapped below the confidence floor. A topic is now read to a
frozen log end, the page that reaches it is the last one, and the connector's
own envelope columns are typed evidence.
"""
from __future__ import annotations

import os
import urllib.parse as up
import uuid
from unittest.mock import patch

import pytest

from tests.test_d151_kafka_start_position_live import (  # noqa: F401 - fixtures
    _BROKER,
    _Mongo,
    _count,
    _produce,
    _run,
    topic,
)


def test_full_refresh_overwrite_publishes_on_every_run(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(topic, range(1, 6))
    for n in (1, 2, 3):
        result = _run(topic, db, "full_refresh_overwrite")
        assert result.success, (n, result.error)
        assert _count(db) == 5, n


def test_multi_page_topic_loads_whole_and_is_stable_across_runs(topic, tmp_path):
    db = str(tmp_path / "dest.db")
    _produce(topic, range(1, 6001))
    for n in (1, 2):
        result = _run(topic, db, "full_refresh_overwrite")
        assert result.success, (n, result.error)
        assert result.records_transferred == 6000, n
        assert _count(db) == 6000, n


def test_messages_produced_before_the_first_run_are_all_read(topic, tmp_path):
    # A brand-new route group has no committed offset; it starts at the log start.
    db = str(tmp_path / "dest.db")
    _produce(topic, range(1, 4))
    first = _run(topic, db, "incremental_append")
    assert first.success, first.error
    assert first.records_transferred == 3


def test_three_runs_through_a_saved_connector_into_sqlserver(topic, monkeypatch, tmp_path):
    sa = pytest.importorskip("sqlalchemy")
    password = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD")
    if not password:
        pytest.skip("SQL Server password not configured")
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")
    kafka = cs.create_connector(
        {"name": "k-d151", "type": "kafka", "role": "source", "host": _BROKER["host"],
         "port": _BROKER["port"]}
    )
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    dest = EndpointConfig(
        kind="database", format="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
        schema="dbo", table=topic, username="sa", password=password,
        extra={"trust_server_certificate": True},
    )
    engine = sa.create_engine(
        "mssql+pyodbc:///?odbc_connect=" + up.quote_plus(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
            f"UID=sa;PWD={password};TrustServerCertificate=yes"
        )
    )
    _produce(topic, range(1, 6))
    rows = None
    try:
        for n in (1, 2, 3):
            request = TransferRequest(
                source=EndpointConfig(
                    kind="database", format="kafka", connector_id=kafka.id, table=topic
                ),
                destination=dest,
                sync_mode="full_refresh_append",
                validation_mode="strict",
            )
            with patch("src.transfer.engine.get_mongodb_service", return_value=_Mongo()):
                result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
            assert result.success, (n, result.error)
            assert result.records_transferred == 5, n
        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"select count(*) from dbo.[{topic}]")).scalar()
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text(f"drop table if exists dbo.[{topic}]"))
    assert rows == 15
