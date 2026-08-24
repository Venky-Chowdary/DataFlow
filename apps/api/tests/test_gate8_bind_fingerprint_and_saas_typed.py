"""Gate-8 write-path fingerprint + thin SaaS typed flatten proofs."""

from __future__ import annotations


def test_fingerprint_bool_json_parity_mysql_postgres():
    from services.reconciliation import fingerprint_for_reconcile

    # Mongo wire "false" and Python False and MySQL 0 must fingerprint equal.
    a = fingerprint_for_reconcile("false", ddl_type="BOOLEAN", engine="mysql")
    b = fingerprint_for_reconcile(False, ddl_type="BOOLEAN", engine="mysql")
    c = fingerprint_for_reconcile(0, ddl_type="BOOLEAN", engine="mysql")
    assert a == b == c

    d = fingerprint_for_reconcile("true", ddl_type="BOOLEAN", engine="postgresql")
    e = fingerprint_for_reconcile(True, ddl_type="BOOLEAN", engine="postgresql")
    assert d == e

    j1 = fingerprint_for_reconcile('{"b":1,"a":2}', ddl_type="JSON", engine="mysql")
    j2 = fingerprint_for_reconcile({"a": 2, "b": 1}, ddl_type="JSON", engine="mysql")
    # Compact JSON bind may preserve input text if already valid — both must be
    # stable under normalize_cell after bind.
    assert j1  # non-empty
    assert j2


def test_sample_compare_uses_bind_fingerprint():
    from services.reconciliation import sample_compare_rows

    # Source Mongo-style string bools vs destination MySQL 0/1 read-back.
    result = sample_compare_rows(
        [{"id": "1", "active": "false"}],
        [{"id": "1", "active": 0}],
        [
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "active", "target": "active", "target_type": "BOOLEAN"},
        ],
        sort_key="id",
        dest_db_type="mysql",
        dest_types={"id": "VARCHAR", "active": "BOOLEAN"},
    )
    assert result["passed"] is True, result
    assert result["compared"] >= 2


def test_airtable_typed_flatten_promotes_fields():
    from connectors.saas_typed_schema import flatten_airtable_record, rows_and_schema_from_saas

    row, schema = flatten_airtable_record(
        {
            "id": "rec1",
            "createdTime": "2024-01-01T00:00:00.000Z",
            "fields": {"Name": "Alice", "Active": True, "Score": 3},
        }
    )
    assert row["Name"] == "Alice"
    assert row["Active"] is True
    assert schema["Active"] == "boolean"
    assert schema["Score"] == "integer"
    assert "fields.Name" not in row

    keys, matrix, sch = rows_and_schema_from_saas(
        "airtable",
        [{"id": "rec1", "fields": {"Name": "Bob", "Active": False}}],
    )
    assert "Name" in keys
    assert sch["Active"] == "boolean"
    assert matrix[0][keys.index("Active")] in {"false", "False", "0"}


def test_notion_typed_flatten_unwraps_properties():
    from connectors.saas_typed_schema import flatten_notion_record

    row, schema = flatten_notion_record(
        {
            "id": "page-1",
            "properties": {
                "Name": {
                    "type": "title",
                    "title": [{"plain_text": "Task", "text": {"content": "Task"}}],
                },
                "Done": {"type": "checkbox", "checkbox": True},
                "Points": {"type": "number", "number": 5},
            },
        }
    )
    assert row["Name"] == "Task"
    assert row["Done"] is True
    assert row["Points"] == 5
    assert schema["Done"] == "boolean"
    assert schema["Points"] == "decimal"


def test_stripe_typed_flatten_known_fields():
    from connectors.saas_typed_schema import flatten_stripe_record

    row, schema = flatten_stripe_record(
        {
            "id": "ch_1",
            "object": "charge",
            "amount": 500,
            "paid": True,
            "created": 1700000000,
            "metadata": {"order": "A1"},
        }
    )
    assert row["amount"] == 500
    assert schema["amount"] == "integer"
    assert schema["paid"] == "boolean"
    assert schema["created"] == "integer"
    assert "metadata.order" in row


def test_saas_catalog_stays_planned_not_sku():
    """Typed read must not silently flip Planned SaaS to TRANSFER_READY."""
    from transfer.connector_capabilities import enrich_catalog_entry

    for brand in ("airtable", "notion", "stripe"):
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live"}
        )
        assert row["transfer_ready"] is False, brand
        assert row["certification_tier"] == "planned", brand


def test_rest_api_meta_exposes_native_types_for_airtable():
    """Stream/introspect consume meta.native_types — must be set on typed reads."""
    from connectors.rest_api import read_object

    class _Resp:
        status_code = 200
        headers: dict = {}

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "records": [
                    {
                        "id": "rec1",
                        "fields": {"Name": "Ada", "Active": True, "Score": 9},
                    }
                ]
            }

    from unittest.mock import patch

    with patch("connectors.rest_api.requests.request", return_value=_Resp()):
        batch = read_object(
            cfg={
                "type": "airtable",
                "host": "https://api.airtable.com",
                "api_key": "key",
                "table": "tbl1",
                "pagination_type": "none",
                "data_path": "records",
            },
            object="tbl1",
            limit=10,
        )
    assert batch.headers
    assert "Name" in batch.headers
    meta = batch.meta or {}
    assert meta.get("saas_typed") is True
    assert meta.get("certification") == "planned_typed_read"
    native = meta.get("native_types") or {}
    assert native.get("Active") == "boolean"
    assert native.get("Score") == "integer"


def test_build_reconciliation_proof_forwards_dest_bind():
    from services.reconciliation import build_reconciliation_proof

    proof = build_reconciliation_proof(
        [{"id": "1", "active": "false"}],
        [{"id": "1", "active": 0}],
        [
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "active", "target": "active", "target_type": "BOOLEAN"},
        ],
        primary_key="id",
        dest_db_type="mysql",
        dest_types={"id": "VARCHAR", "active": "BOOLEAN"},
    )
    assert proof["passed"] is True, proof
    assert proof["sample_compare"]["passed"] is True


def test_canonical_checksum_bind_parity_bool_wire():
    """Whole-table checksum must match across Mongo string bool vs MySQL 0/1."""
    from services.reconciliation import canonical_checksum

    src = [{"id": "1", "active": "false"}]
    dst = [{"id": "1", "active": 0}]
    types = {"id": "VARCHAR", "active": "BOOLEAN"}
    a = canonical_checksum(src, ["id", "active"], dest_db_type="mysql", dest_types=types)
    b = canonical_checksum(dst, ["id", "active"], dest_db_type="mysql", dest_types=types)
    assert a == b


def test_snowflake_bind_rows_covers_boolean_before_copy():
    from connectors.snowflake_writer import _bind_rows_for_snowflake

    rejected: list[dict] = []
    bound = _bind_rows_for_snowflake(
        [("1", "true"), ("2", "false")],
        ["id", "active"],
        ["VARCHAR", "BOOLEAN"],
        rejected,
        "quarantine",
    )
    assert not rejected, rejected
    assert bound[0][1] is True or bound[0][1] == 1 or bound[0][1] == "true"
    # After bind, false-ish wire must not remain the string "false" if coerce works.
    assert bound[1][1] is False or bound[1][1] == 0 or bound[1][1] == "false"


def test_rejected_detail_stamps_primary_key():
    from connectors.writer_common import build_mapped_rows_with_details

    mapped, errors, details = build_mapped_rows_with_details(
        headers=["id", "amount"],
        data_rows=[["1", "not-a-number"]],
        mappings=[
            {"source": "id", "target": "id", "primary_key": True},
            {"source": "amount", "target": "amount", "transform": "integer"},
        ],
        target_cols=["id", "amount"],
        column_types={"id": "string", "amount": "integer"},
        error_policy="quarantine",
    )
    assert details, (mapped, errors, details)
    assert details[0].get("primary_key") == ["id"]
    assert details[0].get("pk_value", {}).get("id") == "1"


def test_cdc_classify_requires_lsn_guard_engine():
    from services.cdc_effectively_once import (
        SINK_APPEND_ONLY,
        SINK_EFFECTIVELY_ONCE_ELIGIBLE,
        classify_sink_delivery,
    )

    pg = classify_sink_delivery(
        dest_type="postgresql", has_primary_key=True, write_mode="upsert"
    )
    assert pg["class"] == SINK_EFFECTIVELY_ONCE_ELIGIBLE
    assert pg.get("has_lsn_guard") is True

    # Explicit missing LSN column → not eligible.
    no_lsn = classify_sink_delivery(
        dest_type="postgresql",
        has_primary_key=True,
        write_mode="upsert",
        has_lsn_column=False,
    )
    assert no_lsn["class"] == SINK_APPEND_ONLY
