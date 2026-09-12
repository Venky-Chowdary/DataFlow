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
