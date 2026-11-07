"""D239: a URL-form host's own port is the port shown and stored.

Connectors saved with host ``http://h:PORT`` (DynamoDB-local, fake GCS,
InfluxDB, S3-moto) are dialed on PORT, but the draft stamped the driver
default and the probe brief reported effective_port 443/8086.
"""

from __future__ import annotations

import pytest

from src.ai.copilot.connector_create import build_connector_draft
from src.ai.copilot.lifecycle_tools import _connector_brief


@pytest.mark.parametrize(
    "ctype,host,port",
    [
        ("influxdb", "http://127.0.0.1:18086", 18086),
        ("dynamodb", "http://dynamo.qa.test:8000", 8000),
        ("s3", "http://moto.qa.test:4566", 4566),
        ("gcs", "http://fake-gcs.qa.test:4443", 4443),
    ],
)
def test_draft_takes_the_url_port(ctype: str, host: str, port: int) -> None:
    draft = build_connector_draft("", {"name": "d239", "type": ctype, "host": host})
    assert draft["port"] == port


def test_explicit_port_without_url_is_kept() -> None:
    draft = build_connector_draft("", {"name": "d239", "type": "influxdb", "host": "influx.qa.test", "port": 9999})
    assert draft["port"] == 9999


def test_url_without_port_keeps_driver_default() -> None:
    draft = build_connector_draft("", {"name": "d239", "type": "influxdb", "host": "https://influx.qa.test"})
    assert draft["port"] == 8086


def test_brief_reports_the_dialed_url_port() -> None:
    brief = _connector_brief({"type": "influxdb", "host": "http://127.0.0.1:18086", "port": 8086})
    assert brief["effective_port"] == 18086
    assert brief["effective_host"] == "127.0.0.1"


def test_store_saves_the_url_port() -> None:
    from services.connector_store import create_connector, delete_connector

    conn = create_connector({"name": "d239-store", "type": "influxdb", "host": "http://127.0.0.1:18086"})
    try:
        assert conn.port == 18086
    finally:
        delete_connector(conn.id)
