"""Wave 52: XML well-formed bind + CITEXT carrier honesty."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_coerce_xml_document_and_fragment():
    from connectors.sql_bind import coerce_xml_wire, normalize_sql_bind_value

    doc = "<root><a>1</a></root>"
    assert coerce_xml_wire(doc) == doc
    frag = "<a>1</a><b>2</b>"
    assert coerce_xml_wire(frag) == frag
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_xml_wire({"a": 1})
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_xml_wire("not xml")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_xml_wire("<root>unclosed")
    assert normalize_sql_bind_value(doc, "XML") == doc


def test_coerce_xml_without_defusedxml_still_accepts_well_formed(monkeypatch):
    """A missing defusedxml package is not malformed XML.

    ImportError used to be swallowed as ``xml wire failed parse``, so PG
    XML→XML dest-exists looked like a fidelity collapse on a well-formed
    ``<item sku="A"/>`` fixture.
    """
    import connectors.sql_bind as bind
    import sys

    monkeypatch.setitem(sys.modules, "defusedxml", None)
    monkeypatch.setitem(sys.modules, "defusedxml.ElementTree", None)

    doc = '<item sku="A"/>'
    assert bind.coerce_xml_wire(doc) == doc
    with pytest.raises(ValueError, match="refuse invent"):
        bind.coerce_xml_wire("<root>unclosed")


def test_coerce_citext_preserves_case():
    from connectors.sql_bind import coerce_citext_wire, normalize_sql_bind_value

    assert coerce_citext_wire("HelloWorld") == "HelloWorld"
    assert normalize_sql_bind_value("AbC", "CITEXT") == "AbC"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_citext_wire(42)
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_citext_wire({"x": 1})
