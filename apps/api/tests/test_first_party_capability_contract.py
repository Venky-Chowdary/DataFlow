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
        "Does Datawrap load Airbyte or Fivetran connector packs",
        "Can I undo a transfer",
        "Can a viewer see secrets",
        "Do you have a REST API",
        "Can I call Datawrap from GitHub Actions",
        "Do you support OpenLineage",
        "What is the difference between mirror and upsert",
        "Can I connect Snowflake with a private key",
        "Do you sign a SOC2 or HIPAA BAA",
        "Who can export audit logs as CSV",
        "Do you support IP allowlists",
        "Can I require MFA",
        "Where is the watermark stored",
        "What is the difference between incremental and upsert",
        "Do you support BigQuery as a destination",
        "Can I use a service principal for Azure",
        "Can I run transfers in parallel",
        "What happens if two jobs write the same table",
        "What is the difference between full refresh and incremental",
        "Do you support SCD1",
        "What is session timeout",
        "Can I set a custom domain",
        "Do you support data residency",
        "Do you support SQL Server",
        "Do you support Databricks as a destination",
        "Do you support Delta Lake",
        "Do you support Snowflake as a destination",
        "Do you support Oracle as a destination",
        "Do you support Postgres as a destination",
        "Do you support MongoDB",
        "Do you support S3 as a destination",
        "Do you support Redshift as a destination",
        "Do you support Azure Synapse",
        "What happens on a unique key collision",
        "Where is my data stored",
        "Do you support BYOK",
        "Can I use a GCP service account",
        "Can I use Workload Identity",
        "Do you support column-level lineage",
        "What if the source has no primary key",
        "What is G8",
        "Do you support Salesforce OAuth",
        "Can I get Slack alerts",
        "Can I send Teams alerts",
        "Do you have custom roles",
        "Do you support field-level encryption",
        "Do you support Apache Hudi",
        "Can I use Kafka consumer groups",
        "Can I use AWS Secrets Manager",
        "Can I send email alerts",
        "Do you support ServiceNow tickets",
        "Is Slack a connector",
        "Do you support Microsoft Teams as a destination",
        "Do you support HubSpot",
        "Do you support Stripe",
        "Do you support row-level security",
        "Do you support Snowflake dynamic tables",
        "Can I pause CDC",
        "Procedure: connect Salesforce",
        "Can I use a read replica for CDC",
        "Do you support Oracle LogMiner",
        "Do you support SQL Server CDC",
        "Can I use change tracking instead of CDC on SQL Server",
        "Can I filter CDC events",
        "Do you support ADLS as a destination",
        "Do you support Microsoft Fabric",
        "Do you support Power BI as a destination",
        "Do you support Azure Data Factory",
        "Do you support Oracle XStream",
        "Do you support SQL Server Always On",
        "Can I use Azure managed identity",
        "Can I set a CDC heartbeat interval",
        "What is the CDC fetch size",
        "Can I skip deletes in CDC",
        "Do you support Cosmos DB",
        "Do you support Event Hubs",
        "Do you support Azure Service Bus",
        "Do you support GCS as a destination",
        "Do you support Cloud SQL",
        "Do you support Azure SQL",
        "Do you support Pub/Sub",
        "Do you support Cloud Spanner",
        "Can I use Vertex AI as a destination",
        "Do you support SharePoint as a destination",
        "Do you support Dynamics 365",
        "Do you support Microsoft Purview",
        "Can I write to Excel Online",
        "Can I assume an AWS IAM role",
        "Can I set the replication slot name",
        "Can I do a blue-green cutover",
        "Do you support SCD type 2",
        "Can I use incremental by updated_at",
        "Do you support Azure Database for PostgreSQL",
        "Do you support Azure Database for MySQL",
        "Do you support Cloud SQL for SQL Server",
        "Do you support OneDrive as a destination",
        "Do you support Looker as a destination",
        "Do you support Looker Studio",
        "Do you support BigQuery Omni",
        "Can I write to Google Sheets",
        "Do you support AlloyDB",
        "Do you support Microsoft Graph as a source",
        "Do you support Azure OpenAI as a destination",
        "Do you support Azure Data Explorer",
        "Do you support Azure Event Grid",
        "Do you support Google Cloud Dataflow",
        "Do you support Power Platform",
        "Do you support Azure Conditional Access",
        "Do you support Cloud Composer",
        "Do you support Azure Blob Storage",
        "Do you support Azure Cache for Redis",
        "Do you support Azure PostgreSQL Flexible Server",
        "Do you support Firebase",
        "Do you support GKE as a destination",
        "Do you support Outlook as a destination",
        "Do you support YouTube as a source",
        "Do you support Microsoft Lists",
        "Do you support Cloud KMS",
        "Do you support Azure Synapse Link",
        "Do you support Azure Table Storage",
        "Do you support Azure Queue Storage",
        "Do you support SQL Server on Azure VMs",
        "Do you support Splunk as a destination",
        "Do you support Tableau as a destination",
        "Do you support Azure Data Lake Gen2",
        "Can I use a service principal for Azure SQL",
        "Do you support Amazon DynamoDB",
        "Do you support Elasticsearch",
        "Do you support Elastic Cloud",
        "Do you support OpenSearch",
        "Do you support Teams as a source",
        "Do you support Microsoft 365 as a destination",
        "Do you support Memorystore",
        "Do you support Azure SQL Edge",
        "Do you support Microsoft Intune",
        "Do you support Cloud Tasks",
        "Do you support VPC Service Controls",
        "Do you support Amazon Aurora",
        "Do you support Cloud Armor",
        "Do you support Azure Arc",
        "Do you support Qlik as a destination",
        "Do you support Entra ID Governance",
        "Do you support Cloud SQL for MySQL",
        "Do you support Azure HDInsight",
        "Do you support SSIS",
        "Do you support Gmail as a source",
        "Do you support BigQuery ML",
        "Do you support Amazon RDS for PostgreSQL",
        "Do you support Filestore",
        "Do you support Informatica as a destination",
        "Do you support AKS as a destination",
        "Do you support Azure AD B2C",
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
    assert "fivetran connector packs" in pack.text.lower()
    from src.ai.first_party.capability_contract import compliance_attestation_card
    from src.routers.audit_router import audit_export_honesty

    attest = audit_export_honesty()
    assert attest["signed_soc2"] is False
    assert attest["signed_hipaa_baa"] is False
    cert = compliance_attestation_card()
    assert cert is not None and "does not invent a signed" in cert.text.lower()
    assert "hipaa baa" in cert.text.lower()
    from src.ai.first_party.capability_contract import (
        adls_service_principal_shipped,
        audit_export_card,
        azure_service_principal_card,
        bigquery_destination_card,
        bigquery_is_transfer_ready,
        incremental_modes_are_canonical,
        incremental_versus_upsert_card,
        ip_allowlist_card,
        ip_allowlist_enforced_without_custom_domain,
        login_mfa_enforced,
        require_mfa_card,
        viewer_has_audit_read,
        watermark_store_honesty,
    )

    assert viewer_has_audit_read() is True
    assert login_mfa_enforced() is False
    assert ip_allowlist_enforced_without_custom_domain() is False
    assert bigquery_is_transfer_ready() is True
    assert adls_service_principal_shipped() is True
    assert incremental_modes_are_canonical() is True
    wm = watermark_store_honesty()
    assert wm["default_store"] == "resume_token"
    assert wm["exactly_once_claimed"] is False
    assert "_df_cdc_eos_watermarks" in str(wm["eos_table"])
    audit = audit_export_card()
    assert audit is not None and "audit.read" in audit.text
    assert "csv" in audit.text.lower()
    assert ip_allowlist_card() is not None
    assert require_mfa_card() is not None
    assert incremental_versus_upsert_card() is not None
    assert bigquery_destination_card() is not None
    assert azure_service_principal_card() is not None
    router = (
        __import__("pathlib").Path(__file__).resolve().parents[1]
        / "src"
        / "routers"
        / "workspace_router.py"
    ).read_text(encoding="utf-8")
    assert '"mfa_enforced": False' in router
    assert "ip_allowlist and (tenant.custom_domain" in router


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
    undo = compose_product_answer(
        retrieve_product_answer("can I undo a transfer", limit=4)
    ) or ""
    rollback = compose_product_answer(
        retrieve_product_answer("can I roll back a load", limit=4)
    ) or ""
    secrets = compose_product_answer(
        retrieve_product_answer("can a viewer see secrets", limit=4)
    ) or ""
    rest = compose_product_answer(
        retrieve_product_answer("do you have a REST API", limit=4)
    ) or ""
    gha = compose_product_answer(
        retrieve_product_answer("can I call this from GitHub Actions", limit=4)
    ) or ""
    lineage = compose_product_answer(
        retrieve_product_answer("do you support OpenLineage", limit=4)
    ) or ""
    mirror = compose_product_answer(
        retrieve_product_answer("what is the difference between mirror and upsert", limit=4)
    ) or ""
    keypair = compose_product_answer(
        retrieve_product_answer("can I connect Snowflake with a private key", limit=4)
    ) or ""
    fivetran = compose_product_answer(
        retrieve_product_answer("do you load Fivetran connector packs", limit=4)
    ) or ""
    audit_who = compose_product_answer(
        retrieve_product_answer("who can export audit logs", limit=4)
    ) or ""
    audit_csv = compose_product_answer(
        retrieve_product_answer("can I export audit logs as CSV", limit=4)
    ) or ""
    allowlist = compose_product_answer(
        retrieve_product_answer("do you support IP allowlists", limit=4)
    ) or ""
    mfa = compose_product_answer(
        retrieve_product_answer("can I require MFA", limit=4)
    ) or ""
    watermark = compose_product_answer(
        retrieve_product_answer("where is the CDC watermark stored", limit=4)
    ) or ""
    set_wm = compose_product_answer(
        retrieve_product_answer("can I set a watermark", limit=4)
    ) or ""
    incr = compose_product_answer(
        retrieve_product_answer("what is the difference between incremental and upsert", limit=4)
    ) or ""
    bq = compose_product_answer(
        retrieve_product_answer("do you support BigQuery as a destination", limit=4)
    ) or ""
    azure = compose_product_answer(
        retrieve_product_answer("can I use a service principal for Azure", limit=4)
    ) or ""
    loss_lead = loss.split(". ")[0].lower()
    upsert_lead = upsert.split(". ")[0].lower()
    pack_lead = pack.split(". ")[0].lower()
    wedge_lead = wedge.split(". ")[0].lower()
    iceberg_lead = iceberg.split(". ")[0].lower()
    hipaa_lead = hipaa.split(". ")[0].lower()
    undo_lead = undo.split(". ")[0].lower()
    rollback_lead = rollback.split(". ")[0].lower()
    secrets_lead = secrets.split(". ")[0].lower()
    rest_lead = rest.split(". ")[0].lower()
    gha_lead = gha.split(". ")[0].lower()
    lineage_lead = lineage.split(". ")[0].lower()
    mirror_lead = mirror.split(". ")[0].lower()
    keypair_lead = keypair.split(". ")[0].lower()
    fivetran_lead = fivetran.split(". ")[0].lower()
    assert "does not invent a legal" in loss_lead
    assert "query capture" not in loss_lead
    assert "upsert is a sync mode" in upsert_lead
    assert "merge into" in upsert_lead
    assert "nightly load" not in upsert_lead
    assert "does not load airbyte" in pack_lead
    assert "semantic mapping" in wedge_lead
    assert iceberg_lead.startswith("iceberg overwrite") or "copy-on-write" in iceberg_lead
    assert "upsert is a sync mode" not in iceberg_lead
    assert "does not invent a signed" in hipaa_lead
    assert "hipaa review" not in hipaa_lead
    assert "does not undo a transfer" in undo_lead
    assert "open **platform" not in undo_lead
    assert "does not undo a transfer" in rollback_lead
    assert "pgoutput" not in rollback_lead
    assert "cannot see secrets" in secrets_lead
    assert "viewer can do it" not in secrets_lead
    assert "/api/v1" in rest_lead
    assert "46 of them" not in rest_lead
    assert "github actions can call" in gha_lead
    assert "mcp server status" not in gha_lead
    assert "openlineage" in lineage_lead
    assert "mirror is upsert plus deletion" in mirror_lead
    assert "merge into" not in mirror_lead
    assert "key-pair" in keypair_lead or "key_pair" in keypair_lead
    assert "kms key" not in keypair_lead
    assert "does not load" in fivetran_lead and "fivetran" in fivetran_lead
    audit_who_lead = audit_who.split(". ")[0].lower()
    audit_csv_lead = audit_csv.split(". ")[0].lower()
    allowlist_lead = allowlist.split(". ")[0].lower()
    mfa_lead = mfa.split(". ")[0].lower()
    watermark_lead = watermark.split(". ")[0].lower()
    set_wm_lead = set_wm.split(". ")[0].lower()
    incr_lead = incr.split(". ")[0].lower()
    bq_lead = bq.split(". ")[0].lower()
    azure_lead = azure.split(". ")[0].lower()
    assert "audit.read" in audit_who_lead
    assert "signed soc 2" not in audit_who_lead
    assert "csv" in audit_csv_lead and "audit" in audit_csv_lead
    assert "file export" not in audit_csv_lead
    assert "select format csv" not in audit_csv_lead
    assert "ip allowlist" in allowlist_lead
    assert "mfa_enforced is false" in mfa_lead or "login mfa is not wired" in mfa_lead
    assert "resume token" in watermark_lead
    assert "exactly-once is not claimed platform-wide" not in watermark_lead or "resume token" in watermark_lead
    assert "separate run" not in watermark_lead
    assert "resume token" in set_wm_lead or "watermark" in set_wm_lead
    assert "separate run" not in set_wm_lead
    assert "cursor-bounded" in incr_lead
    assert "upsert is a sync mode" not in incr_lead
    assert "bigquery is a transfer-ready" in bq_lead
    assert "string" not in bq_lead
    assert "service principal" in azure_lead
    assert "schema registry" not in azure_lead
    parallel = compose_product_answer(
        retrieve_product_answer("can I run transfers in parallel", limit=4)
    ) or ""
    two_jobs = compose_product_answer(
        retrieve_product_answer("what happens if two jobs write the same table", limit=4)
    ) or ""
    full_refresh = compose_product_answer(
        retrieve_product_answer("what is the difference between full refresh and incremental", limit=4)
    ) or ""
    scd1 = compose_product_answer(
        retrieve_product_answer("do you support SCD1", limit=4)
    ) or ""
    session = compose_product_answer(
        retrieve_product_answer("what is session timeout", limit=4)
    ) or ""
    domain = compose_product_answer(
        retrieve_product_answer("can I set a custom domain", limit=4)
    ) or ""
    residency = compose_product_answer(
        retrieve_product_answer("do you support data residency", limit=4)
    ) or ""
    sqlserver = compose_product_answer(
        retrieve_product_answer("do you support SQL Server", limit=4)
    ) or ""
    databricks = compose_product_answer(
        retrieve_product_answer("do you support Databricks as a destination", limit=4)
    ) or ""
    unity = compose_product_answer(
        retrieve_product_answer("can I use Databricks Unity Catalog", limit=4)
    ) or ""
    delta = compose_product_answer(
        retrieve_product_answer("do you support Delta Lake", limit=4)
    ) or ""
    parallel_lead = parallel.split(". ")[0].lower()
    two_jobs_lead = two_jobs.split(". ")[0].lower()
    full_refresh_lead = full_refresh.split(". ")[0].lower()
    scd1_lead = scd1.split(". ")[0].lower()
    session_lead = session.split(". ")[0].lower()
    domain_lead = domain.split(". ")[0].lower()
    residency_lead = residency.split(". ")[0].lower()
    sqlserver_lead = sqlserver.split(". ")[0].lower()
    databricks_lead = databricks.split(". ")[0].lower()
    unity_lead = unity.split(". ")[0].lower()
    delta_lead = delta.split(". ")[0].lower()
    assert "transfer_workers" in parallel_lead
    assert "who can run" not in parallel_lead
    assert "destination lock" in two_jobs_lead
    assert "shared lsn" not in two_jobs_lead
    assert "whole-source" in full_refresh_lead
    assert "upsert is a sync mode" not in full_refresh_lead
    assert "does not ship scd1" in scd1_lead
    assert "session_timeout_enforced" in session_lead
    assert "custom_domain" in domain_lead
    assert "ip allowlist" not in domain_lead
    assert "data_region" in residency_lead
    assert "mirror deletes" not in residency_lead
    assert "sql server is a transfer-ready" in sqlserver_lead
    assert "change data capture (cdc) is log capture" not in sqlserver_lead
    assert "not a transfer-ready" in databricks_lead
    assert "configured the same way as a database" not in databricks_lead
    assert "unity catalog" in unity_lead
    assert "does not ship delta lake" in delta_lead
    assert "service principal" not in delta_lead


def test_pausing_cdc_drop_is_not_the_slot_definition() -> None:
    """``pausing`` must fire the pause-keep card, not ``pause(?:ing)?``."""
    from src.ai.rag.query_analysis import PAUSE_CDC_RE, analyze_query

    question = "does pausing CDC drop the replication slot"
    assert PAUSE_CDC_RE.search(question)
    analysis = analyze_query(question)
    assert "paus_cdc" in analysis.phrase_expansions
    assert "wal" not in analysis.anchor_terms
    body = compose_product_answer(retrieve_product_answer(question, limit=4)) or ""
    lead = body.split(". ")[0].lower()
    assert "does not drop" in lead
    assert "deleting a cdc schedule drops" not in lead
