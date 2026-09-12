"""Honest absences: dbt Cloud and SSH tunnels, generated from enforcing modules."""

from __future__ import annotations

from src.ai.first_party.capability_contract import (
    airbyte_connector_pack_card,
    airbyte_is_transfer_ready,
    capability_cards,
    dbt_card,
    dbt_cloud_shipped,
    debezium_card,
    merge_sql_dialect_shipped,
    postgres_connect_accepts_tunnel,
    silent_data_loss_card,
    ssh_tunnel_card,
    transfer_requires_confirm,
    upsert_is_canonical_sync_mode,
    upsert_versus_merge_card,
)
from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
from src.ai.rag.product_facts import generated_sections


def test_enforcing_modules_do_not_ship_dbt_cloud_or_ssh_tunnels() -> None:
    assert dbt_cloud_shipped() is False
    assert postgres_connect_accepts_tunnel() is False
    dbt = dbt_card()
    ssh = ssh_tunnel_card()
    assert dbt is not None and "does not run dbt cloud" in dbt.text.lower()
    assert ssh is not None and "does not open ssh tunnels" in ssh.text.lower()
    assert "wal_level" not in dbt.text.lower()
    assert "wal_level" not in ssh.text.lower()


def test_generated_sections_include_the_capability_cards() -> None:
    titles = {section.section_title for section in generated_sections()}
    for title in (
        "Does Datawrap run dbt Cloud",
        "Does Datawrap open SSH tunnels",
        "Does Datawrap embed Debezium",
        "Does Datawrap have a Terraform provider",
        "Does a transfer start without Confirm",
        "Does Datawrap use AWS PrivateLink",
        "Does Datawrap run Airflow or Spark jobs",
        "Can I use Kafka as a source",
        "Do you have Salesforce",
        "Can I use an Iceberg Glue catalog",
        "Do you guarantee no silent data loss",
        "What is the difference between upsert and merge",
        "Does Datawrap load Airbyte connector packs",
        "Do you sign a SOC2 or HIPAA BAA",
    ):
        assert title in titles, title
    assert transfer_requires_confirm() is True
    assert debezium_card() is not None
    assert {c.title for c in capability_cards()} >= {
        "Does Datawrap embed Debezium",
        "Does a transfer start without Confirm",
    }


def test_dbt_and_ssh_leads_do_not_steal_cdc_or_studio() -> None:
    dbt = compose_product_answer(
        retrieve_product_answer("can I use dbt with datawrap?", limit=4)
    ) or ""
    ssh = compose_product_answer(
        retrieve_product_answer("do you open an SSH tunnel to postgres?", limit=4)
    ) or ""
    dbt_lead = dbt.split(". ")[0].lower()
    ssh_lead = ssh.split(". ")[0].lower()
    assert "does not run dbt" in dbt_lead
    assert "wal_level" not in dbt_lead
    assert "query playground" not in dbt_lead
    assert "does not open ssh" in ssh_lead
    assert "wal_level" not in ssh_lead
    assert "query playground" not in ssh_lead


def test_enforcing_modules_back_the_silent_wrong_cluster() -> None:
    from services.row_conservation import silent_loss_honesty

    honesty = silent_loss_honesty()
    assert honesty["legal_sla"] is False
    assert honesty["silent_drop"] is False
    assert upsert_is_canonical_sync_mode() is True
    assert merge_sql_dialect_shipped() is True
    assert airbyte_is_transfer_ready() is False
    loss = silent_data_loss_card()
    upsert = upsert_versus_merge_card()
    pack = airbyte_connector_pack_card()
    assert loss is not None and "does not invent a legal" in loss.text.lower()
    assert "query capture" not in (loss.text.lower())
    assert upsert is not None and "merge into" in upsert.text.lower()
    assert "nightly load" not in upsert.text.lower()
    assert pack is not None and "does not load airbyte" in pack.text.lower()
    from src.ai.first_party.capability_contract import compliance_attestation_card
    from src.routers.audit_router import audit_export_honesty

    attest = audit_export_honesty()
    assert attest["signed_soc2"] is False
    assert attest["signed_hipaa_baa"] is False
    cert = compliance_attestation_card()
    assert cert is not None and "does not invent a signed" in cert.text.lower()
    assert "hipaa baa" in cert.text.lower()


def test_loss_upsert_and_airbyte_pack_leads_do_not_steal_neighbors() -> None:
    loss = compose_product_answer(
        retrieve_product_answer("do you guarantee no data loss", limit=4)
    ) or ""
    upsert = compose_product_answer(
        retrieve_product_answer("what's the difference between upsert and merge", limit=4)
    ) or ""
    pack = compose_product_answer(
        retrieve_product_answer("can I use a custom Airbyte connector", limit=4)
    ) or ""
    wedge = compose_product_answer(
        retrieve_product_answer("how are you different from airbyte", limit=4)
    ) or ""
    iceberg = compose_product_answer(
        retrieve_product_answer("does iceberg use merge on read", limit=4)
    ) or ""
    hipaa = compose_product_answer(
        retrieve_product_answer("can you sign a HIPAA BAA", limit=4)
    ) or ""
    loss_lead = loss.split(". ")[0].lower()
    upsert_lead = upsert.split(". ")[0].lower()
    pack_lead = pack.split(". ")[0].lower()
    wedge_lead = wedge.split(". ")[0].lower()
    iceberg_lead = iceberg.split(". ")[0].lower()
    hipaa_lead = hipaa.split(". ")[0].lower()
    assert "does not invent a legal" in loss_lead
    assert "query capture" not in loss_lead
    assert "upsert is a sync mode" in upsert_lead
    assert "merge into" in upsert_lead
    assert "nightly load" not in upsert_lead
    assert "does not load airbyte" in pack_lead
    assert "semantic mapping" in wedge_lead
    assert "merge-on-read" in iceberg_lead
    assert "does not invent a signed" in hipaa_lead
    assert "hipaa review" not in hipaa_lead
