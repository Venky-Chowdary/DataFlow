"""Elasticsearch declares its fields, so a decimal is not dynamic-mapped text.

Left undeclared, the first document decides the field type: the writer sends a
``DECIMAL`` as a precision-preserving string, dynamic mapping calls that
``text``, and the next run reads ``text`` back as this route's own destination
carrier and refuses its own output. The unit tests below pin the declared
shapes; the live test proves the mapping, the stored ``_source``, an
Elasticsearch-side numeric range query, and an independent reread.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.elasticsearch_mapping import (  # noqa: E402
    carrier_for_es_field_type,
    carrier_from_es_property,
    es_index_properties,
    es_property_for_carrier,
    is_es_field_type,
)


def test_bounded_decimal_declares_fixed_point_scaled_float():
    prop = es_property_for_carrier("DECIMAL(12,4)", "keyword")
    assert prop["type"] == "scaled_float"
    assert prop["scaling_factor"] == 10**4
    assert carrier_from_es_property(prop) == "DECIMAL(12,4)"


def test_small_scale_decimal_scales_by_its_own_scale():
    prop = es_property_for_carrier("NUMERIC(9,2)", "keyword")
    assert prop["type"] == "scaled_float"
    assert prop["scaling_factor"] == 100
    assert carrier_from_es_property(prop) == "DECIMAL(9,2)"


def test_zero_scale_numeric_declares_the_integer_field_type():
    prop = es_property_for_carrier("NUMERIC(9,0)", "long")
    assert prop["type"] == "long"
    assert carrier_from_es_property(prop) == "NUMERIC(9,0)"


def test_exact_text_field_keeps_the_carrier_it_holds():
    # A keyword holds the writer's exact wire text, so a reread recovers the
    # carrier instead of reporting every stored value as TEXT.
    assert carrier_from_es_property(
        es_property_for_carrier("TIMESTAMPTZ", "keyword")
    ) == "TIMESTAMPTZ"


def test_unknown_scale_decimal_stays_exact_keyword():
    prop = es_property_for_carrier("DECIMAL", "keyword")
    assert prop["type"] == "keyword"
    assert carrier_from_es_property(prop) == "DECIMAL"


def test_wide_decimal_stays_exact_keyword_instead_of_rounding():
    # 38 digits scaled by 10^10 leaves the backing long: scaled_float would
    # round the value the source declared exactly.
    prop = es_property_for_carrier("DECIMAL(38,10)", "keyword")
    assert prop["type"] == "keyword"
    assert carrier_from_es_property(prop) == "DECIMAL(38,10)"


def test_max_safe_precision_still_scales():
    prop = es_property_for_carrier("DECIMAL(18,2)", "keyword")
    assert prop["type"] == "scaled_float"
    assert prop["scaling_factor"] == 100


def test_text_carrier_declares_keyword_subfield_like_dynamic_mapping():
    prop = es_property_for_carrier("TEXT", "text")
    assert prop["type"] == "text"
    assert prop["fields"]["keyword"]["type"] == "keyword"
    assert carrier_from_es_property(prop) == "TEXT"


def test_document_carrier_is_declared_textual_not_object():
    # The writer stores JSON as a document string; an object container declared
    # without children would pin a shape this transfer cannot prove.
    prop = es_property_for_carrier("JSON", "object")
    assert prop["type"] == "text"
    assert carrier_from_es_property(prop) == "JSON"
    array_prop = es_property_for_carrier("ARRAY<JSON>", "nested")
    assert array_prop["type"] == "text"
    assert carrier_from_es_property(array_prop) == "ARRAY<JSON>"


def test_timestamp_carrier_survives_the_date_field_type():
    prop = es_property_for_carrier("TIMESTAMPTZ", "date")
    assert prop["type"] == "date"
    assert carrier_from_es_property(prop) == "TIMESTAMPTZ"


def test_es_date_is_read_back_as_an_instant_not_a_calendar_day():
    assert carrier_for_es_field_type("date") == "TIMESTAMP"
    assert is_es_field_type("date") is True
    assert is_es_field_type("DECIMAL(12,4)") is False


def test_integer_and_boolean_carriers_declare_native_types():
    assert es_property_for_carrier("INT4", "long")["type"] == "long"
    assert es_property_for_carrier("BOOLEAN", "boolean")["type"] == "boolean"
    assert es_property_for_carrier("DOUBLE PRECISION", "double")["type"] == "double"


def test_no_carrier_at_all_is_left_to_dynamic_mapping():
    assert es_property_for_carrier("", "") == {}
    # An object *container* is never declared as one — the writer stores the
    # document as JSON text, and declaring children this transfer has not proven
    # would pin a shape.
    assert es_property_for_carrier("", "object")["type"] == "text"


def test_unknown_carrier_declares_text_and_keeps_what_it_was():
    prop = es_property_for_carrier("SOME_VENDOR_TYPE", "not_a_field_type")
    assert prop["type"] == "text"
    assert carrier_from_es_property(prop) == "SOME_VENDOR_TYPE"


def test_index_properties_skip_document_identity():
    props = es_index_properties(
        ["_id", "amt", "note"],
        {"amt": "DECIMAL(12,4)", "note": "TEXT"},
        {"amt": "keyword", "note": "text"},
    )
    assert "_id" not in props
    assert props["amt"]["type"] == "scaled_float"
    assert props["note"]["type"] == "text"


def test_carrier_from_property_without_metadata_is_empty():
    assert carrier_from_es_property({"type": "text"}) == ""
    assert carrier_from_es_property({"type": "text", "meta": {}}) == ""
    assert carrier_from_es_property("text") == ""


def test_writer_binds_decimal_as_fixed_point_text_not_float():
    from decimal import Decimal

    from connectors.elasticsearch_writer import _to_es_value

    assert _to_es_value(Decimal("10.5000"), "DECIMAL(12,4)") == "10.5000"
    assert _to_es_value("-0.0000000001", "DECIMAL(38,10)") == "-0.0000000001"
    assert _to_es_value(
        "12345678901234567890.1234567890", "DECIMAL(38,10)"
    ) == "12345678901234567890.1234567890"


def test_writer_binds_an_es_date_field_as_an_instant():
    from connectors.elasticsearch_writer import _to_es_value

    # ``date`` arriving as the wire type must not truncate the time of day, and
    # an offset must not be dropped into a wall clock hours away.
    utc = _to_es_value("2024-12-31T23:59:59+00:00", "date")
    assert utc.isoformat() == "2024-12-31T23:59:59+00:00"
    shifted = _to_es_value("2024-12-31T10:00:00+05:30", "date")
    assert shifted.isoformat() == "2024-12-31T04:30:00+00:00"
    naive = _to_es_value("2024-12-31T10:00:00", "date")
    assert naive.isoformat() == "2024-12-31T10:00:00"


def test_sql_carrier_names_are_not_read_as_field_types():
    from datetime import date

    from connectors.elasticsearch_mapping import is_es_field_type
    from connectors.elasticsearch_writer import _to_es_value

    # ``date``/``text``/``long`` name both a field type and a SQL carrier; only
    # the mapping's lowercase spelling is a field type.
    assert is_es_field_type("date") is True
    assert is_es_field_type("DATE") is False
    assert is_es_field_type("TEXT") is False
    assert is_es_field_type("LONG") is False
    assert _to_es_value("2024-06-01", "DATE") == date(2024, 6, 1)


def test_live_mapping_response_is_read_through_its_body():
    """The live client answers with ObjectApiResponse, not a dict."""

    class _Resp:
        def __init__(self, body):
            self.body = body

    from unittest.mock import MagicMock

    from connectors.elasticsearch_writer import _fetch_es_physical_types

    client = MagicMock()
    client.indices.get_mapping.return_value = _Resp(
        {
            "idx": {
                "mappings": {
                    "properties": {
                        "amt": {
                            "type": "scaled_float",
                            "scaling_factor": 10000,
                            "meta": {"df_carrier": "DECIMAL(12,4)"},
                        }
                    }
                }
            }
        }
    )
    physical, exc = _fetch_es_physical_types(client, "idx", ["amt"])
    assert exc is None
    assert physical["amt"] == "DECIMAL(12,4)"


def test_mapped_source_carriers_prefer_the_source_carrier_over_the_field_type():
    from connectors.elasticsearch_writer import _mapped_source_carriers

    carriers = _mapped_source_carriers(
        [
            {"source": "amt", "target": "amt", "target_type": "keyword"},
            {"source": "ts", "target": "ts", "source_type": "TIMESTAMPTZ"},
        ],
        {"amt": "DECIMAL(12,4)"},
        ["amt", "ts", "extra"],
        ["DECIMAL(12,4)", "TIMESTAMPTZ", "TEXT"],
    )
    assert carriers["amt"] == "DECIMAL(12,4)"
    assert carriers["ts"] == "TIMESTAMPTZ"
    assert carriers["extra"] == "TEXT"


def test_existing_index_refuses_when_a_field_cannot_be_declared():
    """A refused ``put_mapping`` leaves the field dynamic — refuse the write.

    Writing anyway would let the first document decide the field type, which is
    the defect this module exists to prevent.
    """
    from unittest.mock import MagicMock, patch

    from connectors import elasticsearch_writer as ew

    client = MagicMock()
    client.indices.exists.return_value = True
    client.indices.get_mapping.return_value = {
        "idx": {"mappings": {"properties": {"id": {"type": "long"}}}}
    }
    client.indices.put_mapping.side_effect = RuntimeError("strict template")
    with patch.object(ew, "_client", return_value=client):
        result = ew.write_mapped_rows(
            host="localhost",
            port=9200,
            database="idx",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="idx",
            headers=["id", "amt"],
            data_rows=[["1", "10.5000"]],
            mappings=[
                {"source": "id", "target": "id", "target_type": "long"},
                {"source": "amt", "target": "amt", "target_type": "keyword"},
            ],
            column_types={"id": "INT4", "amt": "DECIMAL(12,4)"},
            create_table=True,
        )
    assert result.ok is False
    assert result.rows_written == 0
    assert "amt" in (result.error or "")
    client.bulk.assert_not_called()


def test_live_postgresql_to_elasticsearch_decimal_carriers():
    """Real transfer: declared mapping, exact ``_source``, numeric range, reread."""
    import psycopg2

    from tests.typed_fidelity_helpers import (
        drop_pg_table,
        elasticsearch_endpoint,
        pg_endpoint,
        require_ports,
        run_typed_transfer,
        uniq,
    )

    require_ports(5432, 9200)
    from elasticsearch import Elasticsearch

    src = uniq("es_dec_src")
    idx = uniq("es_dec_dst")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE public."{src}" (
              id INT PRIMARY KEY,
              bounded NUMERIC(12,4),
              wide NUMERIC(38,10),
              ts_utc TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO public."{src}" VALUES
              (1, 10.5000, 12345678901234567890.1234567890,
               '2024-12-31 23:59:59+00'),
              (2, -3.2500, -0.0000000001, '2025-01-01 00:00:00+00'),
              (3, NULL, NULL, NULL)
            """
        )
    conn.close()
    es = Elasticsearch("http://127.0.0.1:9200")
    try:
        result = run_typed_transfer(pg_endpoint(src), elasticsearch_endpoint(idx))
        assert result.success, result.error
        es.indices.refresh(index=idx)

        props = es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"]
        assert props["bounded"]["type"] == "scaled_float"
        assert props["bounded"]["scaling_factor"] == 10**4
        assert carrier_from_es_property(props["bounded"]) == "DECIMAL(12,4)"
        # Beyond the exact long range the value stays a keyword term.
        assert props["wide"]["type"] == "keyword"
        assert props["ts_utc"]["type"] == "date"

        assert es.count(index=idx)["count"] == 3
        docs = {
            hit["_id"]: hit["_source"]
            for hit in es.search(
                index=idx, body={"size": 10, "query": {"match_all": {}}}
            )["hits"]["hits"]
        }
        assert docs["1"]["bounded"] == "10.5000"
        assert docs["1"]["wide"] == "12345678901234567890.1234567890"
        assert docs["1"]["ts_utc"].startswith("2024-12-31T23:59:59")
        assert docs["2"]["bounded"] == "-3.2500"
        assert docs["2"]["wide"] == "-0.0000000001"
        assert docs["3"]["bounded"] is None

        # A numeric mapping — not text — answers a range query.
        assert (
            es.count(index=idx, body={"query": {"range": {"bounded": {"gte": 1}}}})[
                "count"
            ]
            == 1
        )

        # Second run: the route must not refuse its own output, and the mapping
        # must not be reinterpreted.
        again = run_typed_transfer(pg_endpoint(src), elasticsearch_endpoint(idx))
        assert again.success, again.error
        es.indices.refresh(index=idx)
        assert es.count(index=idx)["count"] == 3
        props2 = es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"]
        assert props2["bounded"]["type"] == "scaled_float"

        # Independent reread through the product's own introspection path.
        from services.schema_introspect import introspect_schema

        info = introspect_schema(
            db_type="elasticsearch",
            host="localhost",
            port=9200,
            ssl=False,
            database=idx,
            table=idx,
        )
        assert info.get("ok"), info.get("error")
        seen = {c["name"]: c["inferred_type"] for c in info["columns"]}
        assert seen["bounded"] == "DECIMAL(12,4)"
        assert seen["wide"] == "DECIMAL(38,10)"
        assert seen["ts_utc"] in {"TIMESTAMP", "TIMESTAMPTZ"}
    finally:
        drop_pg_table(src)
        es.indices.delete(index=idx, ignore_unavailable=True)
        es.close()
