"""Salesforce against a real REST API, not a patched ``requests``.

Salesforce routes are credential-gated, so they skipped everywhere and the
connector's transfer-live declaration rested on unit tests that patched the HTTP
client. Patching proves a call was formed; it does not prove a row survives
Describe, SOQL paging, a write and a read-back.

``salesforce_test_server`` serves the subset of the REST API the connector uses
with the contracts it depends on — bearer auth, the 2000-row OFFSET cap,
per-record composite results — so these routes execute here with no tenant.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.salesforce_test_server import (  # noqa: E402
    SObject,
    start_salesforce_server,
)

_ACCOUNTS = [
    {
        "Name": "Acme",
        "AnnualRevenue": "1000.50",
        "NumberOfEmployees": "10",
        "Industry": "Technology",
        "IsActive": True,
        "ExternalKey__c": "K1",
        "CreatedDate": "2024-01-05T00:00:00Z",
        # A formula field is read-only, not empty: a real org computes it.
        "RevenuePerHead": "100.05",
    },
    {
        "Name": "Globex",
        "AnnualRevenue": "2000.25",
        "NumberOfEmployees": "20",
        "Industry": "Finance",
        "IsActive": False,
        "ExternalKey__c": "K2",
        "CreatedDate": "2024-02-11T00:00:00Z",
        "RevenuePerHead": "100.01",
    },
]

_COLUMNS = [
    "Id",
    "Name",
    "AnnualRevenue",
    "NumberOfEmployees",
    "Industry",
    "IsActive",
    "CreatedDate",
    "RevenuePerHead",
    "ExternalKey__c",
]


@pytest.fixture
def salesforce():
    server, httpd = start_salesforce_server()
    try:
        yield server
    finally:
        httpd.shutdown()


def _endpoint(server, sobject: str = "Account") -> EndpointConfig:
    cfg = server.endpoint_config(sobject)
    return EndpointConfig(
        kind="database",
        format="salesforce",
        host=cfg["host"],
        port=443,
        api_key=cfg["api_key"],
        database=sobject,
        table=sobject,
    )


def _probe_cfg(server, sobject: str = "Account") -> dict:
    cfg = server.endpoint_config(sobject)
    return {
        "type": "salesforce",
        "host": cfg["host"],
        "api_key": cfg["api_key"],
        "database": sobject,
    }


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


# ── connectivity and metadata ────────────────────────────────────────────────


def test_connection_requires_a_valid_token(salesforce):
    from connectors.salesforce import test_salesforce

    cfg = salesforce.endpoint_config("Account")
    ok, message = test_salesforce(host=cfg["host"], api_key=cfg["api_key"])
    assert ok is True, message
    denied, _msg = test_salesforce(host=cfg["host"], api_key="not-the-token")
    assert denied is False


def test_introspect_returns_typed_columns_and_the_identity(salesforce):
    """Salesforce declared ``introspect: True`` and answered "not implemented".

    The Describe metadata was already modelled; only the endpoint dispatch was
    missing, so a Salesforce *destination* could never prove its object exists
    and every route into one failed G2 on unknown existence.
    """
    from src.transfer.endpoint_intelligence import introspect_endpoint

    info = introspect_endpoint(_endpoint(salesforce))
    assert info["connected"] is True
    assert info["table_exists"] is True
    schema = info["schema"]
    assert schema["Id"] == "VARCHAR(18)"
    assert schema["AnnualRevenue"] == "DECIMAL(18,2)"
    assert schema["NumberOfEmployees"] == "INTEGER"
    assert schema["IsActive"] == "BOOLEAN"
    assert schema["CreatedDate"] == "TIMESTAMPTZ"
    # A picklist carries its domain rather than degrading to free text.
    assert schema["Industry"].startswith("ENUM(")
    assert info["primary_key_columns"] == ["Id"]


def test_introspect_reports_a_missing_object_as_absent(salesforce):
    """Absent and unreadable are different answers, and neither means create.

    A SaaS object cannot be created by a transfer, so the message has to say
    that rather than let Map flip into create-new.
    """
    from src.transfer.endpoint_intelligence import introspect_endpoint

    info = introspect_endpoint(_endpoint(salesforce, "NoSuchObject__c"))
    assert info["table_exists"] is False
    assert "not an object" in (info["message"] or "")


# ── uniqueness ───────────────────────────────────────────────────────────────


def test_id_uniqueness_is_structural(salesforce):
    """Salesforce assigns Id, so it cannot repeat — stronger than any sample."""
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    salesforce.seed("Account", _ACCOUNTS)
    probe = probe_source_duplicate_keys_result(
        source_config=_probe_cfg(salesforce), source_table="Account", primary_key="Id"
    )
    assert probe.status == "ran", probe.message
    assert probe.findings == []


def test_non_key_identity_is_counted_in_the_org(salesforce):
    """A non-Id identity can repeat, so it is aggregated rather than assumed.

    Aggregating in the org is what keeps this usable against a real tenant,
    where the object may hold millions of rows and OFFSET is capped at 2000.
    """
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    salesforce.seed(
        "Account",
        [
            {"Name": "Acme", "ExternalKey__c": "K1"},
            {"Name": "Globex", "ExternalKey__c": "K2"},
            {"Name": "Acme", "ExternalKey__c": "K3"},
        ],
    )
    duplicated = probe_source_duplicate_keys_result(
        source_config=_probe_cfg(salesforce), source_table="Account", primary_key="Name"
    )
    assert duplicated.status == "ran", duplicated.message
    assert [f["value"] for f in duplicated.findings] == ["Acme"]
    assert duplicated.findings[0]["count"] == 2

    clean = probe_source_duplicate_keys_result(
        source_config=_probe_cfg(salesforce),
        source_table="Account",
        primary_key="ExternalKey__c",
    )
    assert clean.status == "ran"
    assert clean.findings == []


# ── the route ────────────────────────────────────────────────────────────────


def test_salesforce_to_postgresql_moves_every_row(salesforce):
    salesforce.seed("Account", _ACCOUNTS)
    table = f"sf_live_{uuid.uuid4().hex[:8]}"
    conn = _pg_conn()
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_endpoint(salesforce),
                destination=EndpointConfig(
                    kind="database",
                    format="postgresql",
                    host="localhost",
                    port=5432,
                    database="dataflow",
                    username="dataflow",
                    password="dataflow",
                    schema="public",
                    table=table,
                ),
                sync_mode="full_refresh_overwrite",
                skip_preflight=False,
                validation_mode="strict",
                stream_contracts=[
                    {
                        "name": "Account",
                        "sync_mode": "full_refresh_overwrite",
                        "primary_key": "Id",
                        "selected": True,
                    }
                ],
                mappings=[
                    {"source": c, "target": c, "confidence": 0.99}
                    for c in _COLUMNS
                    if c != "Industry"
                ]
                # Industry is a picklist: landing its ENUM domain in free text
                # is a real collapse, asserted on its own below. Omitting it
                # here is declared, not silent.
                + [
                    {
                        "source": "Industry",
                        "target": "",
                        "confidence": 0.0,
                        "intentional_omit": True,
                    }
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        assert result.records_transferred == 2
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position",
                (table,),
            )
            types = dict(cur.fetchall())
            # A custom field keeps its API name. Collapsing the underscore run
            # renamed every Salesforce custom field, which all end in __c.
            assert "ExternalKey__c" in types, types
            assert types["AnnualRevenue"] == "numeric"
            assert types["NumberOfEmployees"] == "bigint"
            assert types["IsActive"] == "boolean"
            cur.execute(
                f'SELECT "Name", "AnnualRevenue", "ExternalKey__c" FROM "{table}" '
                'ORDER BY "Name"'
            )
            rows = cur.fetchall()
        assert [r[0] for r in rows] == ["Acme", "Globex"]
        assert str(rows[0][1]).startswith("1000.5")
        assert [r[2] for r in rows] == ["K1", "K2"]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


def test_picklist_domain_collapse_is_surfaced(salesforce):
    """A picklist landing in free text loses its domain, and must say so.

    This was invisible while Salesforce had no endpoint introspect: with no
    declared source type there was nothing to collapse *from*, so the route
    looked clean. Describing the field is what makes the risk reportable.
    """
    from services.type_system import enum_domain_would_collapse
    from src.transfer.endpoint_intelligence import introspect_endpoint

    schema = introspect_endpoint(_endpoint(salesforce))["schema"]
    assert enum_domain_would_collapse(schema["Industry"], "TEXT") is True
    # A field that was never an enum must not be reported as one.
    assert enum_domain_would_collapse(schema["Name"], "TEXT") is False


# ── the write contract ───────────────────────────────────────────────────────


def test_writer_upserts_by_external_id(salesforce):
    """Reverse-ETL keys on an External Id so a re-run updates, never duplicates."""
    from connectors.salesforce_writer import write_mapped_rows

    cfg = salesforce.endpoint_config("Account")
    columns = ["Name", "AnnualRevenue", "ExternalKey__c"]
    rows = [["Initech", "500.25", "K9"], ["Umbrella", "900.75", "K8"]]
    mappings = [{"source": c, "target": c} for c in columns]

    def _write():
        return write_mapped_rows(
            host=cfg["host"],
            port=443,
            api_key=cfg["api_key"],
            table_name="Account",
            headers=columns,
            data_rows=rows,
            mappings=mappings,
            column_types={
                "Name": "VARCHAR(255)",
                "AnnualRevenue": "DECIMAL(18,2)",
                "ExternalKey__c": "VARCHAR(64)",
            },
            write_mode="upsert",
            conflict_columns=["ExternalKey__c"],
            error_policy="quarantine",
        )

    first = _write()
    assert first.ok is True, first.error
    assert first.rows_written == 2
    assert len(salesforce.rows("Account")) == 2

    # A second run must update in place — at-least-once delivery must not
    # multiply records in the customer's CRM.
    second = _write()
    assert second.ok is True, second.error
    assert len(salesforce.rows("Account")) == 2
    landed = {r["ExternalKey__c"]: r for r in salesforce.rows("Account")}
    assert landed["K9"]["Name"] == "Initech"
    # The API re-types by field: a currency comes back as a JSON number.
    assert landed["K9"]["AnnualRevenue"] == 500.25


def test_writer_quarantines_a_record_the_api_rejects(salesforce):
    """A per-record failure is quarantined, not raised as a batch abort."""
    from connectors.salesforce_writer import write_mapped_rows

    cfg = salesforce.endpoint_config("Account")
    result = write_mapped_rows(
        host=cfg["host"],
        port=443,
        api_key=cfg["api_key"],
        table_name="Account",
        headers=["Name", "RevenuePerHead", "ExternalKey__c"],
        # RevenuePerHead is a formula field: Describe marks it not createable,
        # and Salesforce refuses the record rather than silently dropping it.
        data_rows=[["Acme", "12.5", "K1"]],
        mappings=[
            {"source": "Name", "target": "Name"},
            {"source": "RevenuePerHead", "target": "RevenuePerHead"},
            {"source": "ExternalKey__c", "target": "ExternalKey__c"},
        ],
        column_types={
            "Name": "VARCHAR(255)",
            "RevenuePerHead": "DECIMAL",
            "ExternalKey__c": "VARCHAR(64)",
        },
        write_mode="insert",
        error_policy="quarantine",
    )
    assert result.rows_written == 0
    assert result.rejected_details, "a refused record must be surfaced"
    assert salesforce.rows("Account") == []


def test_describe_failure_refuses_rather_than_guessing(salesforce):
    """Without Describe the writer has no schema contract, so it must refuse."""
    from connectors.salesforce_writer import write_mapped_rows

    cfg = salesforce.endpoint_config("Account")
    result = write_mapped_rows(
        host=cfg["host"],
        port=443,
        api_key="not-the-token",
        table_name="Account",
        headers=["Name"],
        data_rows=[["Acme"]],
        mappings=[{"source": "Name", "target": "Name"}],
        column_types={"Name": "VARCHAR(255)"},
        write_mode="insert",
        error_policy="quarantine",
    )
    assert result.ok is False
    assert result.rows_written == 0


# ── the double's own contracts ───────────────────────────────────────────────


def test_offset_beyond_the_cap_is_refused_like_salesforce(salesforce):
    """The 2000-row OFFSET cap is the reason the reader has a keyset path."""
    from tests.salesforce_test_server import SoqlError, run_soql

    with pytest.raises(SoqlError):
        run_soql(
            salesforce.store,
            "SELECT Id FROM Account ORDER BY Id LIMIT 10 OFFSET 5000",
        )


def test_unknown_field_is_refused_like_salesforce(salesforce):
    from tests.salesforce_test_server import SoqlError, run_soql

    with pytest.raises(SoqlError):
        run_soql(salesforce.store, "SELECT NoSuchField__c FROM Account")


def test_a_second_object_can_be_served(salesforce):
    """The store is not Account-only — routes may target any described object."""
    from tests.salesforce_test_server import run_soql

    salesforce.store["Contact"] = SObject("Contact")
    salesforce.seed("Contact", [{"Name": "Ada"}])
    rows = run_soql(salesforce.store, "SELECT Id, Name FROM Contact ORDER BY Id")
    assert [r["Name"] for r in rows] == ["Ada"]
