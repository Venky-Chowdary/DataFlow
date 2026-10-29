"""D142 — Test, read and write of a custom endpoint must dial one address.

QA: an S3-compatible endpoint's data read dropped the port the test used, and a
port typed into the URL got a default port appended (also Elasticsearch).

Measured causes:

* ``run_probe`` filters its keyword arguments to the probe's signature, and
  ``test_s3`` named neither ``endpoint_url`` nor ``path_style``. A connector
  with an explicit endpoint URL was *tested* at ``host:port`` while every read
  and write dialled the endpoint URL (``resolve_endpoint_url``).
* The Elasticsearch probe and reader each built ``{scheme}://{host}:{port}``
  from the form fields, so a host that already carried a scheme or a port
  became ``http://http://es:9200:9200``.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from src.transfer.models import EndpointConfig

_LIST_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    b"<Name>b</Name><Prefix></Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>"
    b"<IsTruncated>false</IsTruncated>"
    b"<Contents><Key>data.csv</Key><Size>4</Size><ETag>&quot;e&quot;</ETag></Contents>"
    b"</ListBucketResult>"
)


class _FakeS3(BaseHTTPRequestHandler):
    seen: list[str] = []

    def _answer(self, body: bytes = b"") -> None:
        type(self).seen.append(f"{self.command} {self.path}")
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/xml")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self) -> None:  # noqa: N802
        self._answer()

    def do_GET(self) -> None:  # noqa: N802
        self._answer(_LIST_XML)

    def log_message(self, *args) -> None:
        return


@pytest.fixture()
def s3_endpoint(monkeypatch):
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("NO_PROXY", "*")
    handler = type("Handler", (_FakeS3,), {"seen": []})
    server = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", handler
    server.shutdown()
    server.server_close()


def test_s3_test_and_read_dial_the_endpoint_url_the_connector_carries(s3_endpoint):
    from connectors.s3_reader import list_objects
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_registry import run_probe

    url, handler = s3_endpoint
    # The form defaults: a region in Host and 443 in Port; the real address is
    # the Endpoint URL field.
    fields = {
        "host": "us-east-1",
        "port": 443,
        "database": "b",
        "username": "key",
        "password": "secret",
        "endpoint_url": url,
        "path_style": True,
    }
    ok, message = run_probe("s3", dict(fields))
    assert ok, message
    probed = list(handler.seen)
    assert probed, "the test never reached the endpoint URL the read uses"

    handler.seen.clear()
    endpoint = EndpointConfig(kind="database", format="s3", **fields)
    assert list_objects(resolve_connector_config(endpoint), "b") == ["data.csv"]
    assert handler.seen, "the read never reached the endpoint URL"


@pytest.mark.parametrize(
    ("host", "port", "ssl", "expected"),
    [
        # A scheme and port already in the host are the address, not a prefix.
        ("http://127.0.0.1:19200", 9200, False, "http://127.0.0.1:19200"),
        ("https://es.example.com:9243", 443, True, "https://es.example.com:9243"),
        ("es.example.com:9201", 9200, False, "http://es.example.com:9201"),
        # The port field still supplies a port the host does not carry.
        ("https://es.example.com", 9200, False, "https://es.example.com:9200"),
        ("127.0.0.1", 9201, False, "http://127.0.0.1:9201"),
        # 443 means TLS on the default port, as before.
        ("es.example.com", 443, False, "https://es.example.com"),
        ("es.example.com", 9200, True, "https://es.example.com:9200"),
    ],
)
def test_elasticsearch_test_and_read_build_the_same_url(monkeypatch, host, port, ssl, expected):
    import elasticsearch

    import connectors.elasticsearch as probe_mod
    from connectors.elasticsearch_reader import _client

    hosts: list[str] = []

    class _Recorder:
        def __init__(self, **kwargs):
            hosts.append(kwargs["hosts"][0])
            raise RuntimeError("recorded")

    monkeypatch.setattr(elasticsearch, "Elasticsearch", _Recorder)
    probe_mod.test_elasticsearch(
        host=host,
        port=port,
        database="",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=ssl,
    )
    with pytest.raises(RuntimeError, match="recorded"):
        _client({"host": host, "port": port, "ssl": ssl})

    assert hosts == [expected, expected]
