"""XML is streamable when the document is a unique repeating list-of-object."""

from __future__ import annotations

from src.transfer.file_stream import (
    STREAMABLE_TYPES,
    _batch_iterator_for_type,
    peek_file_source,
    should_stream_file,
)
from src.transfer.models import EndpointConfig


def _records_xml() -> bytes:
    return (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b"<records>\n"
        b"  <record><order_no>1</order_no><customer>ada</customer>"
        b'<line_items>[{"sku":"A"}]</line_items></record>\n'
        b"  <record><order_no>2</order_no><customer>grace</customer>"
        b'<line_items>[{"sku":"C"}]</line_items></record>\n'
        b"</records>\n"
    )


def test_xml_is_in_streamable_types():
    assert "xml" in STREAMABLE_TYPES


def test_peek_and_batch_xml_unique_path():
    content = _records_xml()
    headers, schema, total, sample = peek_file_source(content, "orders.xml")
    assert total == 2
    assert "order_no" in headers
    assert "line_items" in headers
    assert len(sample) == 2
    assert schema
    batches = list(_batch_iterator_for_type("xml", content, 1))
    assert sum(len(b) for b in batches) == 2
    dest = EndpointConfig(kind="database", format="postgresql")
    assert should_stream_file(content, "orders.xml", dest) is True


def test_unmeasured_xml_refuses_stream():
    sibling = (
        b"<root><orders><order><id>1</id></order></orders>"
        b"<items><item><id>a</id></item></items></root>"
    )
    try:
        peek_file_source(sibling, "ambiguous.xml")
    except ValueError as exc:
        assert "unmeasured" in str(exc).lower()
    else:
        raise AssertionError("sibling collections must stay unmeasured, not dest=guess")
