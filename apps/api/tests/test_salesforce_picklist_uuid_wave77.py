"""Wave 77: Salesforce picklist/ID carriers + UUID→string collapse honesty.

Research anchors
----------------
- Salesforce SOAP: picklist/multipicklist are strings; multipicklist is
  semicolon-delimited. Closed domains from Describe picklistValues.
- Id / reference are 15/18-char Salesforce IDs (not open VARCHAR).
- Compound address/location → JSON envelope (not invent flat VARCHAR).
- Iceberg/Snowflake: nested UUID often stored as string (pg_lake compatibility
  mode) — valid but must surface in preflight (never silent green).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_salesforce_picklist_and_id_carriers():
    from connectors.salesforce_writer import salesforce_field_to_carrier

    assert salesforce_field_to_carrier({"type": "id"}) == "VARCHAR(18)"
    assert salesforce_field_to_carrier({"type": "reference"}) == "VARCHAR(18)"

    pick = salesforce_field_to_carrier(
        {
            "type": "picklist",
            "picklistValues": [
                {"active": True, "value": "Open", "label": "Open"},
                {"active": True, "value": "Closed", "label": "Closed"},
                {"active": False, "value": "Legacy", "label": "Legacy"},
            ],
        }
    )
    assert pick == "ENUM('Open','Closed')"

    multi = salesforce_field_to_carrier(
        {
            "type": "multipicklist",
            "picklistValues": [
                {"active": True, "value": "A"},
                {"active": True, "value": "B"},
            ],
        }
    )
    assert multi == "SET('A','B')"

    # No values → bounded VARCHAR (length from Describe).
    assert salesforce_field_to_carrier(
        {"type": "picklist", "length": 40}
    ) == "VARCHAR(40)"

    assert salesforce_field_to_carrier({"type": "address"}) == "JSON"
    assert salesforce_field_to_carrier({"type": "location"}) == "JSON"
    assert salesforce_field_to_carrier(
        {"type": "currency", "precision": 18, "scale": 2}
    ) == "DECIMAL(18,2)"
    assert salesforce_field_to_carrier({"type": "datetime"}) == "TIMESTAMPTZ"


def test_uuid_collapse_honesty_iceberg_snowflake():
    from services.type_system import (
        ddl_type,
        is_lossy_coercion,
        is_nested_shape_collapse,
        is_precision_collapse_coercion,
        uuid_would_collapse,
    )

    assert uuid_would_collapse("UUID", "VARCHAR") is True
    assert uuid_would_collapse("UUID", "STRING") is True
    assert uuid_would_collapse("UUID", "UUID") is False
    assert is_precision_collapse_coercion("UUID", "VARCHAR") is True
    assert is_lossy_coercion("UUID", "VARCHAR") is True

    # Nested UUID → width-safe string leaves (Snowflake structured compatibility).
    assert ddl_type("snowflake", "list<uuid>") == "ARRAY(VARCHAR(36))"
    assert ddl_type("snowflake", "struct<a:uuid>") == "OBJECT(a VARCHAR(36))"
    assert is_nested_shape_collapse("ARRAY<UUID>", "ARRAY<VARCHAR>") is True
    assert is_nested_shape_collapse("STRUCT<a:UUID>", "STRUCT<a:VARCHAR>") is True
    # Width-safe carriers are not a domain collapse (create-new MySQL/Snowflake).
    assert uuid_would_collapse("UUID", "VARCHAR(36)") is False
    assert is_lossy_coercion("UUID", "CHAR(36)") is False
