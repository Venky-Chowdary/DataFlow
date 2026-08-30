"""SaaS / HTTP source readers use load_http_json, not Response.json().

stdlib Response.json() collapsed 1.234567890123456789 in CRM / N1QL /
InfluxQL / Cypher cells before flatten. IEEE-exact 1.5 stays float.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LONG = "1.234567890123456789"


def _ok(text: str):
    return SimpleNamespace(text=text, raise_for_status=lambda: None)


def test_stripe_read_keeps_long_fraction():
    from connectors.stripe import read_object

    raw = '{"data":[{"id":"cus_1","amt": ' + LONG + ', "n": 1.5}],"has_more":false}'
    with patch("connectors.stripe.request", return_value=_ok(raw)):
        batch = read_object(cfg={"api_key": "sk"}, object="customers", limit=1)
    flat = " ".join(str(c) for row in batch.rows for c in row)
    assert LONG in flat
    assert str(json.loads(raw)["data"][0]["amt"]) not in {LONG}


def test_hubspot_read_keeps_long_fraction():
    from connectors.hubspot import read_object

    def _req(*, method, url, **_kw):
        if "/properties/" in url:
            return _ok('{"results":[{"name":"amt","type":"number"}]}')
        return _ok(
            '{"results":[{"id":"1","properties":{"amt": '
            + LONG
            + ', "n": 1.5}}],"paging":{}}'
        )

    with patch("connectors.hubspot.request", side_effect=_req):
        batch = read_object(cfg={"api_key": "pat"}, object="contacts", limit=1)
    flat = " ".join(str(c) for row in batch.rows for c in row)
    assert LONG in flat


def test_salesforce_query_keeps_long_fraction():
    from connectors import salesforce

    describe = [{"name": "Id", "type": "id"}, {"name": "Amt", "type": "double"}]
    query_text = (
        '{"totalSize":1,"done":true,"records":[{"Id":"001A","Amt": '
        + LONG
        + ", \"n\": 1.5}]}"
    )

    def _req(*, method, url, token="", params=None, timeout=60):
        return _ok(query_text)

    with patch.object(salesforce, "_access", return_value=("tok", "https://x.my.salesforce.com")):
        with patch.object(salesforce, "describe_sobject", return_value=describe):
            with patch.object(salesforce, "request", side_effect=_req):
                batch = salesforce.read_object(cfg={}, object="Account", limit=1)
    flat = " ".join(str(c) for row in batch.rows for c in row)
    assert LONG in flat


def test_couchbase_n1ql_keeps_long_fraction():
    from connectors.couchbase import _n1ql

    raw = '{"results":[{"amt": ' + LONG + ', "n": 1.5}]}'
    with patch("connectors.couchbase.requests.post", return_value=_ok(raw)):
        body = _n1ql("http://cb/query/service", "u", "p", "SELECT 1")
    assert body["results"][0]["amt"] == Decimal(LONG)
    assert body["results"][0]["n"] == 1.5


def test_influx_query_keeps_long_fraction():
    from connectors.influxdb import _query

    raw = (
        '{"results":[{"series":[{"columns":["amt","n"],"values":[['
        + LONG
        + ", 1.5]]}]}]}"
    )
    with patch("connectors.influxdb.requests.get", return_value=_ok(raw)):
        body = _query("http://influx", "db", "SELECT *")
    assert body["results"][0]["series"][0]["values"][0][0] == Decimal(LONG)
    assert body["results"][0]["series"][0]["values"][0][1] == 1.5


def test_neo4j_cypher_keeps_long_fraction():
    from connectors.neo4j import _run_cypher

    raw = (
        '{"results":[{"columns":["amt"],"data":[{"row":['
        + LONG
        + ']}]}],"errors":[]}'
    )
    with patch("connectors.neo4j.requests.post", return_value=_ok(raw)):
        body = _run_cypher("http://neo/db/neo4j/tx/commit", "u", "p", "RETURN 1")
    assert body["results"][0]["data"][0]["row"][0] == Decimal(LONG)


def test_hubspot_cdk_read_keeps_long_fraction():
    from connectors.sdk.hubspot_cdk import HubSpotCDKConnector

    raw = (
        '{"results":[{"id":"1","properties":{"amt": '
        + LONG
        + ', "email":"a@b.com"}}],"paging":{}}'
    )
    with patch("connectors.sdk.hubspot_cdk.request", return_value=_ok(raw)):
        batches = list(
            HubSpotCDKConnector({"api_key": "tok"}).read("contacts", state=None, limit=1)
        )
    assert batches[0].records[0]["amt"] == Decimal(LONG)
