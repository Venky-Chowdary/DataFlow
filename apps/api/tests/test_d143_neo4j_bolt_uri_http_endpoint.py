"""D143: a Neo4j connector on Bolt 7687 failed "RemoteDisconnected".

The connector speaks the HTTP Cypher API. A Bolt URI or the Bolt port was
dialed as HTTP, and the Bolt listener closes an HTTP request without a reply.
The Bolt address names the server; the HTTP API is that server's 7474 (7473
over TLS).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.neo4j import _url, read_object, test_connection as neo4j_test_connection


@pytest.mark.parametrize(
    ("host", "port", "ssl", "expected"),
    [
        ("bolt://db.example:7687", 0, False, "http://db.example:7474/db/neo4j/tx/commit"),
        ("neo4j://db.example", 7687, False, "http://db.example:7474/db/neo4j/tx/commit"),
        ("db.example", 7687, False, "http://db.example:7474/db/neo4j/tx/commit"),
        ("db.example", 7687, True, "https://db.example:7473/db/neo4j/tx/commit"),
        ("bolt+s://db.example:7687", 0, False, "https://db.example:7473/db/neo4j/tx/commit"),
        # Managed Bolt URI with no port (Aura): HTTPS on the default port.
        ("neo4j+s://abc.databases.neo4j.io", 0, False, "https://abc.databases.neo4j.io/db/neo4j/tx/commit"),
        # Unchanged HTTP spellings.
        ("localhost", 7474, False, "http://localhost:7474/db/neo4j/tx/commit"),
        ("http://h:7474", 0, False, "http://h:7474/db/neo4j/tx/commit"),
        ("https://h:8443", 7474, False, "https://h:8443/db/neo4j/tx/commit"),
    ],
)
def test_bolt_addresses_resolve_to_the_http_cypher_endpoint(host, port, ssl, expected):
    assert _url(host, port, ssl, "neo4j") == expected


def _ok_response():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.text = '{"results": [{"columns": ["count"], "data": [{"row": [0]}]}], "errors": []}'
    resp.json.return_value = {"results": [{"columns": ["count"], "data": [{"row": [0]}]}], "errors": []}
    resp.headers = {"content-type": "application/json"}
    resp.content = resp.text.encode()
    return resp


def test_connection_test_uses_a_pasted_bolt_uri():
    with patch("connectors.neo4j.requests.post", return_value=_ok_response()) as post:
        ok, msg = neo4j_test_connection(
            host="", port=7687, connection_string="neo4j://graph.internal:7687",
            username="neo4j", password="pw",
        )
    assert ok, msg
    assert post.call_args.args[0] == "http://graph.internal:7474/db/neo4j/tx/commit"


def test_read_dials_http_not_the_bolt_port():
    with patch("connectors.neo4j.requests.post", return_value=_ok_response()) as post:
        read_object(cfg={"host": "graph.internal", "port": 7687, "database": "neo4j"}, object="Person")
    assert post.call_args.args[0] == "http://graph.internal:7474/db/neo4j/tx/commit"
