"""Named-fixture QA for the first-party chatbot — Google/Microsoft operator asks.

This is 100% on **this fixture**, not all English. Each case is a wording an
enterprise evaluator actually types. A miss here is a silent-wrong answer
(dbt answered as Studio, SSH answered as wal_level), not a style nits.
"""

from __future__ import annotations

import pytest

from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer


def _lead(question: str) -> str:
    answer = retrieve_product_answer(question, limit=4)
    if not answer.hits:
        return f"[{answer.verdict.outcome}]"
    body = " ".join((compose_product_answer(answer) or "").split())
    return body.split(". ")[0].lower()


@pytest.mark.parametrize(
    "question,needles,forbidden",
    [
        ("can I use dbt with datawrap?", ("does not run dbt",), ("wal_level", "query playground")),
        ("do you support dbt Cloud?", ("does not run dbt",), ("wal_level",)),
        ("does datawrap run dbt as the transfer engine?", ("does not run dbt",), ("exactly-once",)),
        ("can we run our dbt models after the load?", ("dbt", "complement"), ("wal_level",)),
        ("hey do you guys have dbt cloud?", ("does not run dbt",), ("studio",)),
        ("do you open an SSH tunnel to postgres?", ("does not open ssh",), ("wal_level", "query playground")),
        ("can I connect through a bastion host?", ("does not open ssh", "bastion"), ("wal_level",)),
        ("do you support ssh tunnels?", ("does not open ssh",), ("wal_level",)),
        ("is there a jump host option for mysql?", ("does not open ssh",), ("binlog",)),
        ("is cdc exactly-once?", ("least",), ()),
        ("do you guarantee exactly once delivery?", ("least",), ()),
        ("are you chatgpt?", ("local engine",), ("we are chatgpt",)),
        ("do we have our own llm?", ("own local engine",), ()),
        ("gotta have logical wal for pg cdc right?", ("wal_level",), ("dbt", "ssh")),
        ("where do bad rows go?", ("quarantine",), ("dbt",)),
        ("how is this different from fivetran?", ("semantic mapping", "quarantine"), ("dbt cloud",)),
        ("can a viewer export yaml?", ("viewer", "yaml"), ("dbt",)),
        ("does iceberg use merge on read?", ("merge-on-read",), ("upsert is a sync mode", "merge into / on conflict")),
        ("who can start a transfer?", ("job.run",), ("dbt",)),
        ("do you have webhooks?", ("webhook",), ("dbt",)),
        ("do you embed Debezium?", ("does not embed debezium",), ("airbyte", "fivetran")),
        ("are you a Kafka Connect replacement?", ("not a kafka connect replacement",), ("latin-1",)),
        ("do you support Flink CDC?", ("does not run flink cdc",), ("tombstone",)),
        ("can I manage pipelines with Terraform?", ("does not ship a terraform",), ("new connection",)),
        ("do I have to Confirm before a transfer starts?", ("does not start without confirm",), ("overview",)),
        ("can I connect through AWS PrivateLink?", ("does not ship aws privatelink",), ("query playground",)),
        ("can Airflow trigger a transfer?", ("does not run airflow",), ("agent security",)),
        ("do you run Spark jobs?", ("does not run airflow", "spark"), ("who can run transfers",)),
        ("do you support Oracle GoldenGate?", ("does not embed oracle goldengate",), ()),
        ("can I use Kafka as a source?", ("kafka is a transfer-ready",), ("latin-1",)),
        ("do you have Salesforce?", ("salesforce is a transfer-ready",), ()),
        ("can I bring Iceberg with a Glue catalog?", ("glue",), ()),
        ("what about snowflake sharing?", ("does not implement snowflake secure",), ("shared lsn",)),
        ("is there a schema registry?", ("does not ship a standalone", "schema registry"), ()),
        ("do you guarantee no data loss", ("does not invent a legal", "quarantine"), ("query capture", "loss of deletes")),
        ("can you guarantee we never lose data", ("does not invent a legal",), ("query capture",)),
        ("what's the difference between upsert and merge", ("upsert is a sync mode", "merge into"), ("nightly load", "full_refresh")),
        ("upsert vs merge", ("upsert is a sync mode", "merge into"), ("table.upsert",)),
        ("is upsert the same as merge", ("upsert is a sync mode", "merge into"), ("catalog mode also",)),
        ("can I use a custom Airbyte connector", ("does not load airbyte",), ("differs from airbyte and fivetran",)),
        ("can I load an Airbyte connector pack", ("does not load airbyte",), ("optional add-ons",)),
        ("how are you different from airbyte", ("semantic mapping", "quarantine"), ("does not load airbyte",)),
        ("can I undo a transfer", ("does not undo a transfer",), ("open **platform", "salesforce")),
        ("can I roll back a load", ("does not undo a transfer",), ("pgoutput",)),
        ("can a viewer see secrets", ("cannot see secrets",), ("viewer can do it",)),
        ("do you have a REST API", ("/api/v1",), ("46 of them",)),
        ("can I call this from GitHub Actions", ("github actions can call",), ("mcp server status",)),
        ("do you support OpenLineage", ("openlineage",), ()),
        ("what is the difference between mirror and upsert", ("mirror is upsert plus deletion",), ("merge into",)),
        ("do you load Fivetran connector packs", ("does not load", "fivetran"), ("semantic mapping",)),
        ("can I connect Snowflake with a private key", ("key-pair",), ("kms key", "privatelink")),
        ("do you support SOC2", ("does not invent a signed", "soc 2"), ("type ii letter we issued",)),
        ("can you sign a HIPAA BAA", ("does not invent a signed", "hipaa baa"), ("for soc 2 / gdpr / hipaa review",)),
        ("who can export audit logs", ("audit.read",), ("signed soc 2", "type ii letter")),
        ("can I export audit logs as CSV", ("csv", "audit"), ("select format csv", "file export")),
        ("do you support IP allowlists", ("ip allowlist",), ("mfa_enforced",)),
        ("can I require MFA", ("login mfa is not wired",), ("ip allowlist",)),
        ("where is the CDC watermark stored", ("resume token",), ("separate run", "exactly-once delivery")),
        ("can I set a watermark", ("resume token", "watermark"), ("separate run",)),
        ("what is the difference between incremental and upsert", ("cursor-bounded",), ("upsert is a sync mode", "merge into")),
        ("do you support BigQuery as a destination", ("bigquery is a transfer-ready",), ("string",)),
        ("can I use a service principal for Azure", ("service principal",), ("schema registry", "delta lake")),
        ("can I run transfers in parallel", ("transfer_workers",), ("catalog, preflight", "who can run")),
        ("how many transfers can run at once", ("transfer_workers",), ("your it team configures sso",)),
        ("what happens if two jobs write the same table", ("destination lock",), ("shared lsn", "handoff")),
        ("do you lock the destination during write", ("destination lock",), ("cast integer", "normalize email")),
        ("do you support SCD1", ("does not ship scd1",), ("live tiles include",)),
        ("is upsert the same as SCD1", ("does not ship scd1",), ("merge into / on conflict",)),
        ("do you support SCD type 1", ("does not ship scd1",), ("history-keeping",)),
        ("what is the difference between full refresh and incremental", ("whole-source",), ("upsert is a sync mode",)),
        ("do you support data residency", ("data_region",), ("mirror deletes data",)),
        ("can I set a custom domain", ("custom_domain",), ("ip allowlist",)),
        ("what is session timeout", ("session_timeout_enforced",), ("mfa_enforced",)),
        ("do you support SQL Server", ("sql server is a transfer-ready",), ("change data capture (cdc) is log capture",)),
        ("do you support Databricks as a destination", ("not a transfer-ready",), ("configured the same way as a database",)),
        ("do you support Delta Lake", ("does not ship delta lake",), ("service principal", "adls")),
        ("can I use Databricks Unity Catalog", ("unity catalog",), ("glue catalog",)),
        ("do you support Snowflake as a destination", ("snowflake is a transfer-ready",), ("secure data sharing", "key-pair")),
        ("do you support Oracle as a destination", ("oracle is a transfer-ready",), ("goldengate",)),
        ("do you support Postgres as a destination", ("postgresql is a transfer-ready",), ("pgoutput", "wal_level")),
        ("do you support MongoDB", ("mongodb is a transfer-ready",), ("pre-image",)),
        ("do you support S3 as a destination", ("s3 is a transfer-ready",), ("redshift", "glue catalog")),
        ("do you support Redshift as a destination", ("not a transfer-ready",), ("ship with upsert",)),
        ("can I land tables in Redshift", ("not a transfer-ready",), ("ship with upsert",)),
        ("do you support Azure Synapse", ("not a transfer-ready",), ("service principal", "adls")),
        ("what happens on a unique key collision", ("duplicate keys",), ("migration certificate", "mirror is upsert")),
        ("what if two source rows have the same key", ("duplicate keys",), ("mirror is upsert plus deletion",)),
        ("where is my data stored", ("does not host",), ("data contract", "resume token")),
        ("do you support BYOK", ("byok",), ("cannot see secrets",)),
        ("can I use a GCP service account", ("gcp service account",), ("service principal", "adls")),
        ("can I use Workload Identity", ("does not ship gcp workload identity",), ("replica identity",)),
        ("do you support Private Service Connect", ("private service connect",), ("service principal",)),
        ("do you support column-level lineage", ("column-level lineage",), ("openlineage- and opentelemetry",)),
        ("can I schedule a transfer", ("cadence",), ("export yaml", "gitops read")),
        ("what if the source has no primary key", ("no primary key",), ("unsupported aspects", "certified rather")),
        ("what is Gate 8", ("g8", "reconciliation"), ("g1 source readable through g9",)),
        ("what is Gate 7", ("g7", "staging"), ("g8 sample reconciliation", "g1 source readable through g9")),
        ("what is Gate 9", ("g9", "data integrity"), ("g8 sample reconciliation", "g1 source readable through g9")),
        ("do you support Salesforce OAuth", ("pasted access token",), ("salesforce is a transfer-ready", "full refresh")),
        ("do you refresh Salesforce tokens", ("oauth", "pasted access token"), ("full_refresh", "whole-source")),
        ("can I get Slack alerts", ("slack", "webhook"), ("outside what the datawrap",)),
        ("can I send Teams alerts", ("teams", "webhook"), ("settings → sso", "team roles")),
        ("do you have custom roles", ("does not ship custom roles",), ("custom_domain", "custom domain")),
        ("do you support field-level encryption", ("does not ship field-level",), ("column-level lineage",)),
        ("do you support Apache Hudi", ("not a transfer-ready",), ("37 of them", "destinations a transfer can write")),
        ("can I use Kafka consumer groups", ("group_id",), ("grouped aggregate", "group by")),
        ("can I use AWS Secrets Manager", ("does not read aws secrets manager",), ("privatelink",)),
        ("do you support Okta", ("saml",), ("we ship scim", "custom_domain")),
        (
            "what is the difference between jobs and pipelines",
            ("pipelines owns the schedule",),
            ("destination lock", "shared lsn"),
        ),
        ("do you support HubSpot", ("hubspot is a transfer-ready",), ("adls, avro, bigquery",)),
        ("do you support Stripe", ("stripe is a transfer-ready",), ("46 of them",)),
        ("is Slack a connector", ("not a transfer-ready",), ("incoming webhook url",)),
        (
            "do you support Microsoft Teams as a destination",
            ("not a transfer-ready",),
            ("incoming webhook url", "settings → sso"),
        ),
        ("can I send email alerts", ("email", "kind=email"), ("kind=teams",)),
        ("do you support ServiceNow tickets", ("servicenow", "kind=servicenow"), ("we ship scim",)),
        ("do you support row-level security", ("does not ship row-level security",), ("row-level questions use",)),
        (
            "do you support Snowflake dynamic tables",
            ("does not ship snowflake dynamic tables",),
            ("snowflake is a transfer-ready",),
        ),
        ("can I use Entra ID", ("saml",), ("we ship scim",)),
        ("do you support Azure Key Vault", ("does not read", "azure key vault"), ("privatelink",)),
        ("can I pause CDC", ("enabled false", "does not drop"), ("replica identity", "pre-image", "right-side detail")),
        ("how do I pause CDC", ("pause", "does not drop"), ("right-side detail",)),
        ("does pausing CDC drop the replication slot", ("does not drop",), ("right-side detail", "deleting a cdc schedule drops")),
        ("if I pause CDC do I lose the slot", ("does not drop",), ("right-side detail",)),
        ("what is a replication slot", ("replication slot",), ("does not drop",)),
        ("what happens if I delete a CDC schedule", ("drops",), ("does not drop",)),
        ("how do I connect Salesforce", ("new connection", "access token"), ("salesforce is a transfer-ready",)),
        ("how do I set up Salesforce", ("new connection",), ("salesforce is a transfer-ready",)),
        ("can I use a read replica for CDC", ("does not connect cdc to a physical standby",), ("replica identity full so update",)),
        ("do you support Oracle LogMiner", ("logminer",), ("oracle is a transfer-ready", "does not embed oracle goldengate")),
        ("do you support SQL Server CDC", ("sqlservernativecdc",), ("change data capture (cdc) is log capture",)),
        (
            "can I use change tracking instead of CDC on SQL Server",
            ("change tracking",),
            ("change data capture (cdc) is log capture",),
        ),
        ("can I filter CDC events", ("does not ship a capture-side",), ("gtid is a mysql",)),
        ("how do I do the snapshot handoff", ("shared lsn",), ("binlog_format=row",)),
        ("what if the replication slot fills up", ("max_replication_slots",), ("right-side detail",)),
        ("do you have Salesforce", ("salesforce is a transfer-ready",), ("new connection",)),
        ("do you support Oracle as a destination", ("oracle is a transfer-ready",), ("logminer",)),
        ("do you support SQL Server", ("sql server is a transfer-ready",), ("sqlservernativecdc",)),
        ("how do I pause a schedule", ("pause", "activate"), ("does not drop the replication slot",)),
        ("can I land in ADLS", ("adls is a transfer-ready",), ("azure synapse is not",)),
        ("do you support Synapse", ("not a transfer-ready",), ("adls is a transfer-ready",)),
        ("can I write to Microsoft Fabric", ("does not ship microsoft fabric",), ("incoming webhook url", "microsoft teams is not")),
        ("do you support OneLake", ("onelake", "fabric is false"), ("incoming webhook url",)),
        ("do you support Power BI as a destination", ("does not ship power bi",), ("fabric is false",)),
        ("do you support Azure Data Factory", ("does not ship azure data factory",), ("azure synapse is not",)),
        ("do you support Oracle XStream", ("does not ship oracle xstream",), ("yes — oracle cdc reads",)),
        ("do you support SQL Server Always On", ("does not ship sql server always on",), ("sql server is a transfer-ready",)),
        ("can I use Azure managed identity", ("does not connect azure with managed identity",), ("azure synapse is not",)),
        ("can I set a CDC heartbeat interval", ("does not ship a cdc heartbeat interval",), ("postgresql interval",)),
        ("what is the CDC fetch size", ("does not ship a cdc fetch-size",), ("a cdc delete is applied",)),
        ("can I skip deletes in CDC", ("does not skip cdc deletes",), ("do pipelines skip preflight",)),
        ("do you support Cosmos DB", ("does not ship azure cosmos db",), ("change-stream pre-images",)),
        ("do you support Event Hubs", ("does not ship azure event hubs",), ("does not emit column-level lineage",)),
        ("do you support Azure Service Bus", ("does not ship azure service bus",), ("adls and azure blob connections accept",)),
        ("can I connect through Private Link", ("does not ship aws privatelink",), ("go to operations → jobs",)),
    ],
)
def test_enterprise_wording_leads_on_the_asked_fact(
    question: str, needles: tuple[str, ...], forbidden: tuple[str, ...]
) -> None:
    lead = _lead(question)
    missing = [n for n in needles if n.lower() not in lead]
    leaked = [f for f in forbidden if f.lower() in lead]
    assert not missing, f"{missing} not in {lead[:240]}"
    assert not leaked, f"{leaked} leaked into {lead[:240]}"


@pytest.mark.parametrize(
    "question",
    [
        "how do I cook rice tonight",
        "write me a poem about the sea",
        "what is the capital of France",
    ],
)
def test_off_subject_english_is_refused(question: str) -> None:
    answer = retrieve_product_answer(question, limit=4)
    assert answer.verdict.outcome == "refuse"
    assert not answer.hits


@pytest.mark.parametrize(
    "question",
    [
        "what is your uptime SLA",
        "how much does it cost",
        "how much does datawrap cost",
        "is there an SLA for job runtime",
        "do you support SCIM",
    ],
)
def test_compliance_and_commercial_asks_are_refused_not_invented(
    question: str,
) -> None:
    """No invented price or uptime SLA. Refuse is the honest product."""
    answer = retrieve_product_answer(question, limit=4)
    assert answer.verdict.outcome == "refuse"
    assert not answer.hits
