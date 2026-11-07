"""Training pairs built from the product corpus — no external teacher.

A first-party model that learned from ChatGPT paraphrases would quietly
import that model's facts. We therefore synthesize *operator* paraphrases
with deterministic templates: chat filler, slang frames, and the canonical
questions already generated from enforcing modules.

Two pair types come out of this file:

* **align** ``(paraphrase, gold_question)`` — InfoNCE positives for the
  dual encoder. This is how open English reaches a documented heading.
* **copy** ``(question, evidence, answer)`` — teacher-forcing targets for
  the pointer-generator. The answer is always a prefix of the evidence, so
  the decoder is trained to copy, not to invent.
"""

from __future__ import annotations

from dataclasses import dataclass

# Chat wrappers operators actually type. Applied in front of a gold question
# so the dual encoder sees the same subject under different discourse.
_CHAT_PREFIXES: tuple[str, ...] = (
    "hey ",
    "wait so ",
    "can you tell me ",
    "i was wondering ",
    "confused about ",
    "quick question ",
    "pls explain ",
    "honestly ",
    "idk but ",
    "what about this: ",
    "can we ",
    "does this product support ",
    "is there support for ",
)

# Surface substitutions that do not add a new product. Each left-hand side
# is a wording we already accept in the deterministic rewrite.
_SLANG_SWAPS: tuple[tuple[str, str], ...] = (
    ("what is ", "whats "),
    ("do i need ", "gotta have "),
    ("do I need ", "gotta have "),
    ("wal_level logical", "logical wal"),
    ("export yaml", "download yaml"),
    ("can a viewer", "can viewers"),
    ("replication slot", "repl slot"),
    ("binlog_format", "bin log format"),
    ("where do bad rows end up", "where do bad rows go"),
)

# Explicit paraphrase → canonical alignments for the questions operators
# ask about the engine itself. These are the same subjects the generated
# Pilot-engine section answers — they do not invent dbt or SSH.
_CANONICAL_ALIGNS: tuple[tuple[str, str], ...] = (
    ("are you chatgpt", "does pilot use chatgpt or a third-party llm"),
    ("do you use openai", "does pilot use chatgpt or a third-party llm"),
    ("do you use openai by default", "does pilot use chatgpt or a third-party llm"),
    ("do we have our own llm", "does pilot use chatgpt or a third-party llm"),
    ("are you a foundation model", "does pilot use chatgpt or a third-party llm"),
    ("gotta have logical wal", "do i need wal_level logical"),
    ("gotta have logical wal for pg cdc", "do i need wal_level logical"),
    ("where do bad rows go", "where do bad rows end up"),
    ("is _df_lsn how you skip dupes", "is cdc exactly-once or at-least-once"),
    ("can I use dbt with datawrap", "does datawrap run dbt cloud"),
    ("do you support dbt cloud", "does datawrap run dbt cloud"),
    ("can we run our dbt models after the load", "does datawrap run dbt cloud"),
    ("do you open an ssh tunnel to postgres", "does datawrap open ssh tunnels"),
    ("can I connect through a bastion host", "does datawrap open ssh tunnels"),
    ("do you support ssh tunnels", "does datawrap open ssh tunnels"),
    ("do you embed debezium", "does datawrap embed debezium"),
    ("are you a kafka connect replacement", "does datawrap embed debezium"),
    ("do you support flink cdc", "does datawrap embed debezium"),
    ("can I manage pipelines with terraform", "does datawrap have a terraform provider"),
    ("do I have to confirm before a transfer starts", "does a transfer start without confirm"),
    ("can I connect through aws privatelink", "does datawrap use aws privatelink"),
    ("can airflow trigger a transfer", "does datawrap run airflow or spark jobs"),
    ("do you run spark jobs", "does datawrap run airflow or spark jobs"),
    ("do you support oracle goldengate", "does datawrap embed oracle goldengate"),
    ("can I use kafka as a source", "can I use kafka as a source"),
    ("do you have salesforce", "do you have salesforce"),
    ("can I bring iceberg with a glue catalog", "can I use an iceberg glue catalog"),
    ("what about snowflake sharing", "does datawrap use snowflake secure sharing"),
    ("is there a schema registry", "does datawrap include a schema registry"),
    ("do you guarantee no data loss", "do you guarantee no silent data loss"),
    ("zero data loss right", "do you guarantee no silent data loss"),
    ("can you guarantee we never lose data", "do you guarantee no silent data loss"),
    ("what's the difference between upsert and merge", "what is the difference between upsert and merge"),
    ("upsert vs merge", "what is the difference between upsert and merge"),
    ("is upsert the same as merge", "what is the difference between upsert and merge"),
    ("can I use a custom airbyte connector", "does datawrap load airbyte or fivetran connector packs"),
    ("can I load an airbyte connector pack", "does datawrap load airbyte or fivetran connector packs"),
    ("do you load fivetran connector packs", "does datawrap load airbyte or fivetran connector packs"),
    ("can I undo a transfer", "can I undo a transfer"),
    ("can I roll back a load", "can I undo a transfer"),
    ("can a viewer see secrets", "can a viewer see secrets"),
    ("do you have a rest api", "do you have a rest api"),
    ("can I call this from github actions", "can I call datawrap from github actions"),
    ("do you support openlineage", "do you support openlineage"),
    ("what is the difference between mirror and upsert", "what is the difference between mirror and upsert"),
    ("can I connect snowflake with a private key", "can I connect snowflake with a private key"),
    ("do you support soc2", "do you sign a soc2 or hipaa baa"),
    ("can you sign a hipaa baa", "do you sign a soc2 or hipaa baa"),
    ("who can export audit logs", "who can export audit logs as csv"),
    ("can I export audit logs as csv", "who can export audit logs as csv"),
    ("do you support ip allowlists", "do you support ip allowlists"),
    ("can I require mfa", "can I require mfa"),
    ("where is the cdc watermark stored", "where is the watermark stored"),
    ("can I set a watermark", "where is the watermark stored"),
    ("what is the difference between incremental and upsert", "what is the difference between incremental and upsert"),
    ("do you support bigquery as a destination", "do you support bigquery as a destination"),
    ("can I use a service principal for azure", "can I use a service principal for azure"),
    ("can I run transfers in parallel", "can I run transfers in parallel"),
    ("how many transfers can run at once", "can I run transfers in parallel"),
    ("what happens if two jobs write the same table", "what happens if two jobs write the same table"),
    ("do you lock the destination during write", "what happens if two jobs write the same table"),
    ("what is the difference between full refresh and incremental", "what is the difference between full refresh and incremental"),
    ("do you support scd1", "do you support scd1"),
    ("is upsert the same as scd1", "do you support scd1"),
    ("do you support scd type 1", "do you support scd1"),
    ("what is session timeout", "what is session timeout"),
    ("can I set a custom domain", "can I set a custom domain"),
    ("do you support data residency", "do you support data residency"),
    ("do you support sql server", "do you support sql server"),
    ("do you support databricks as a destination", "do you support databricks as a destination"),
    ("can I use databricks unity catalog", "do you support databricks as a destination"),
    ("do you support delta lake", "do you support delta lake"),
    ("do you support snowflake as a destination", "do you support snowflake as a destination"),
    ("do you support oracle as a destination", "do you support oracle as a destination"),
    ("do you support postgres as a destination", "do you support postgres as a destination"),
    ("do you support mongodb", "do you support mongodb"),
    ("do you support s3 as a destination", "do you support s3 as a destination"),
    ("do you support redshift as a destination", "do you support redshift as a destination"),
    ("can I land tables in redshift", "do you support redshift as a destination"),
    ("do you support azure synapse", "do you support azure synapse"),
    ("what happens on a unique key collision", "what happens on a unique key collision"),
    ("what if two source rows have the same key", "what happens on a unique key collision"),
    ("where is my data stored", "where is my data stored"),
    ("do you support byok", "do you support byok"),
    ("can I bring my own encryption key", "do you support byok"),
    ("can I use a gcp service account", "can I use a gcp service account"),
    ("can I use workload identity", "can I use workload identity"),
    ("do you support private service connect", "does datawrap use aws privatelink"),
    ("do you support column-level lineage", "do you support column-level lineage"),
    ("can I schedule a transfer", "can I schedule a transfer"),
    ("how do I schedule a transfer", "can I schedule a transfer"),
    ("what if the source has no primary key", "what if the source has no primary key"),
    ("what is gate 8", "what is g8"),
    ("what is g8", "what is g8"),
    ("do you support salesforce oauth", "do you support salesforce oauth"),
    ("can I connect salesforce with a connected app", "do you support salesforce oauth"),
    ("do you refresh salesforce tokens", "do you support salesforce oauth"),
    ("can I get slack alerts", "can I get slack alerts"),
    ("can I send teams alerts", "can I send teams alerts"),
    ("do you have custom roles", "do you have custom roles"),
    ("do you support field-level encryption", "do you support field-level encryption"),
    ("do you support apache hudi", "do you support apache hudi"),
    ("can I use kafka consumer groups", "can I use kafka consumer groups"),
    ("can I use aws secrets manager", "can I use aws secrets manager"),
    ("do you support hashicorp vault", "can I use aws secrets manager"),
    ("do you support okta", "how do I set up sso"),
    ("what is the difference between jobs and pipelines", "what is the difference between jobs and pipelines"),
    ("jobs vs pipelines", "what is the difference between jobs and pipelines"),
    ("do you support hubspot", "do you support hubspot"),
    ("do you support stripe", "do you support stripe"),
    ("is slack a connector", "is slack a connector"),
    ("can I send email alerts", "can I send email alerts"),
    ("do you support servicenow tickets", "do you support servicenow tickets"),
    ("do you support row-level security", "do you support row-level security"),
    ("do you support snowflake dynamic tables", "do you support snowflake dynamic tables"),
    ("can I use entra id", "how do I set up sso"),
    ("do you support azure key vault", "can I use aws secrets manager"),
    ("can I pause cdc", "can I pause cdc"),
    ("how do I pause cdc", "can I pause cdc"),
    ("does pausing cdc drop the replication slot", "does pausing cdc drop the replication slot"),
    ("if I pause cdc do I lose the slot", "does pausing cdc drop the replication slot"),
    ("how do I connect salesforce", "how do I connect salesforce"),
    ("can I use a read replica for cdc", "can I use a read replica for cdc"),
    ("do you support oracle logminer", "do you support oracle logminer"),
    ("do you support sql server cdc", "do you support sql server cdc"),
    ("can I filter cdc events", "can I filter cdc events"),
    ("can I land in adls", "do you support adls as a destination"),
    ("do you support microsoft fabric", "do you support microsoft fabric"),
    ("can I write to microsoft fabric", "do you support microsoft fabric"),
    ("do you support onelake", "do you support microsoft fabric"),
    ("do you support oracle xstream", "do you support oracle xstream"),
    ("do you support sql server always on", "do you support sql server always on"),
    ("can I use azure managed identity", "can I use azure managed identity"),
    ("can I set a cdc heartbeat interval", "can I set a cdc heartbeat interval"),
    ("can I skip deletes in cdc", "can I skip deletes in cdc"),
    ("do you support cosmos db", "do you support cosmos db"),
    ("do you support event hubs", "do you support event hubs"),
    ("do you support azure data factory", "do you support azure data factory"),
    ("do you support synapse", "do you support azure synapse"),
    ("do you support gcs as a destination", "do you support gcs as a destination"),
    ("do you support google cloud storage", "do you support gcs as a destination"),
    ("do you support cloud sql", "do you support cloud sql"),
    ("do you support azure sql", "do you support azure sql"),
    ("can I write to google pub/sub", "do you support pub/sub"),
    ("do you support pub/sub", "do you support pub/sub"),
    ("do you support cloud spanner", "do you support cloud spanner"),
    ("can I use vertex ai as a destination", "can I use vertex ai as a destination"),
    ("do you support sharepoint as a destination", "do you support sharepoint as a destination"),
    ("do you support dynamics 365", "do you support dynamics 365"),
    ("do you support microsoft purview", "do you support microsoft purview"),
    ("can I write to excel online", "can I write to excel online"),
    ("can I assume an aws iam role", "can I assume an aws iam role"),
    ("can I set the replication slot name", "can I set the replication slot name"),
    ("can I do a blue-green cutover", "can I do a blue-green cutover"),
    ("do you support scd type 2", "do you support scd type 2"),
    ("can I use incremental by updated_at", "can I use incremental by updated_at"),
    ("do you support azure database for postgresql", "do you support azure database for postgresql"),
    ("do you support azure database for mysql", "do you support azure database for mysql"),
    ("do you support cloud sql for sql server", "do you support cloud sql for sql server"),
    ("do you support onedrive as a destination", "do you support onedrive as a destination"),
    ("do you support looker as a destination", "do you support looker as a destination"),
    ("do you support looker studio", "do you support looker studio"),
    ("do you support bigquery omni", "do you support bigquery omni"),
    ("can I write to google sheets", "can I write to google sheets"),
    ("do you support alloydb", "do you support alloydb"),
    ("do you support microsoft graph as a source", "do you support microsoft graph as a source"),
    ("do you support azure openai as a destination", "do you support azure openai as a destination"),
    ("do you support azure data explorer", "do you support azure data explorer"),
    ("do you support azure event grid", "do you support azure event grid"),
    ("do you support google cloud dataflow", "do you support google cloud dataflow"),
    ("do you support power platform", "do you support power platform"),
    ("do you support azure conditional access", "do you support azure conditional access"),
    ("do you support cloud composer", "do you support cloud composer"),
    ("do you support azure blob storage", "do you support azure blob storage"),
    ("do you support azure cache for redis", "do you support azure cache for redis"),
    ("do you support azure postgresql flexible server", "do you support azure postgresql flexible server"),
    ("do you support firebase", "do you support firebase"),
    ("do you support firestore", "do you support firestore"),
    ("do you support bigtable", "do you support bigtable"),
    ("do you support dataproc", "do you support dataproc"),
    ("do you support entra pim", "do you support entra pim"),
    ("do you support gke as a destination", "do you support gke as a destination"),
    ("do you support outlook as a destination", "do you support outlook as a destination"),
    ("do you support youtube as a source", "do you support youtube as a source"),
    ("do you support google ads as a source", "do you support google ads as a source"),
    ("can I write to google drive", "can I write to google drive"),
    ("do you support google docs as a destination", "do you support google docs as a destination"),
    ("do you support google analytics", "do you support google analytics"),
    ("do you support cloud run as a destination", "do you support cloud run as a destination"),
    ("do you support azure stream analytics", "do you support azure stream analytics"),
    ("do you support microsoft lists", "do you support microsoft lists"),
    ("do you support exchange online", "do you support exchange online"),
    ("do you support github enterprise as a destination", "do you support github enterprise as a destination"),
    ("do you support bigquery data transfer service", "do you support bigquery data transfer service"),
    ("can I use a bigquery linked dataset", "can I use a bigquery linked dataset"),
    ("do you support cloud kms", "do you support cloud kms"),
    ("do you support app engine", "do you support app engine"),
    ("do you support data catalog", "do you support data catalog"),
    ("do you support cloud build", "do you support cloud build"),
    ("do you support artifact registry", "do you support artifact registry"),
    ("do you support cloud functions", "do you support cloud functions"),
    ("do you support azure files", "do you support azure files"),
    ("do you support log analytics", "do you support log analytics"),
    ("do you support azure ai search", "do you support azure ai search"),
    ("do you support azure analysis services", "do you support azure analysis services"),
    ("do you support cloud sql auth proxy", "do you support cloud sql auth proxy"),
    ("do you support azure synapse link", "do you support azure synapse link"),
    ("do you support azure table storage", "do you support azure table storage"),
    ("do you support azure queue storage", "do you support azure queue storage"),
    ("do you support sql server on azure vms", "do you support sql server on azure vms"),
    ("do you support splunk as a destination", "do you support splunk as a destination"),
    ("do you support tableau as a destination", "do you support tableau as a destination"),
    ("do you support azure data lake gen2", "do you support azure data lake gen2"),
    ("can I use a service principal for azure sql", "can I use a service principal for azure sql"),
    ("do you support amazon dynamodb", "do you support amazon dynamodb"),
    ("do you support elasticsearch", "do you support elasticsearch"),
    ("do you support elastic cloud", "do you support elastic cloud"),
    ("do you support opensearch", "do you support opensearch"),
    ("do you support teams as a source", "do you support teams as a source"),
    ("do you support microsoft 365 as a destination", "do you support microsoft 365 as a destination"),
    ("do you support memorystore", "do you support memorystore"),
    ("do you support azure sql edge", "do you support azure sql edge"),
    ("do you support microsoft intune", "do you support microsoft intune"),
    ("do you support microsoft defender", "do you support microsoft defender"),
    ("do you support microsoft sentinel", "do you support microsoft sentinel"),
    ("do you support azure machine learning as a destination", "do you support azure machine learning as a destination"),
    ("do you support search ads 360", "do you support search ads 360"),
    ("do you support cloud tasks", "do you support cloud tasks"),
    ("do you support vpc service controls", "do you support vpc service controls"),
    ("do you support azure firewall", "do you support azure firewall"),
    ("do you support campaign manager", "do you support campaign manager"),
    ("do you support amazon aurora", "do you support amazon aurora"),
    ("do you support amazon documentdb", "do you support amazon documentdb"),
    ("do you support cloud armor", "do you support cloud armor"),
    ("do you support cloud interconnect", "do you support cloud interconnect"),
    ("do you support cloud vpn", "do you support cloud vpn"),
    ("do you support azure arc", "do you support azure arc"),
    ("do you support azure lighthouse", "do you support azure lighthouse"),
    ("do you support azure monitor", "do you support azure monitor"),
    ("do you support azure devops", "do you support azure devops"),
    ("do you support azure boards", "do you support azure boards"),
    ("do you support azure migrate", "do you support azure migrate"),
    ("do you support display & video 360", "do you support display & video 360"),
    ("do you support cloud storage transfer service", "do you support cloud storage transfer service"),
    ("do you support qlik as a destination", "do you support qlik as a destination"),
    ("do you support entra id governance", "do you support entra id governance"),
    ("do you support cloud sql for mysql", "do you support cloud sql for mysql"),
    ("do you support azure hdinsight", "do you support azure hdinsight"),
    ("do you support azure expressroute", "do you support azure expressroute"),
    ("do you support ssis", "do you support ssis"),
    ("do you support ssrs", "do you support ssrs"),
    ("do you support gmail as a source", "do you support gmail as a source"),
    ("do you support calendar as a source", "do you support calendar as a source"),
    ("do you support azure cdn", "do you support azure cdn"),
    ("do you support azure blueprints", "do you support azure blueprints"),
    ("do you support azure automation", "do you support azure automation"),
    ("do you support bigquery ml", "do you support bigquery ml"),
    ("do you support amazon rds for postgresql", "do you support amazon rds for postgresql"),
    ("do you support amazon rds for mysql", "do you support amazon rds for mysql"),
    ("do you support cloud sql for postgresql", "do you support cloud sql for postgresql"),
    ("do you support filestore", "do you support filestore"),
    ("do you support datastream", "do you support datastream"),
    ("do you support dataplex", "do you support dataplex"),
    ("do you support informatica as a destination", "do you support informatica as a destination"),
    ("do you support talend as a destination", "do you support talend as a destination"),
    ("do you support matillion as a destination", "do you support matillion as a destination"),
    ("do you support google workspace as a source", "do you support google workspace as a source"),
    ("do you support aks as a destination", "do you support aks as a destination"),
    ("do you support azure functions", "do you support azure functions"),
    ("do you support azure batch", "do you support azure batch"),
    ("do you support cloud scheduler", "do you support cloud scheduler"),
    ("do you support vertex ai search", "do you support vertex ai search"),
    ("do you support business central", "do you support business central"),
    ("do you support application insights", "do you support application insights"),
    ("do you support azure site recovery", "do you support azure site recovery"),
    ("do you support entra external id", "do you support entra external id"),
    ("do you support azure ad b2c", "do you support azure ad b2c"),
    ("do you support google meet as a destination", "do you support google meet as a destination"),
    ("do you support google chat as a destination", "do you support google chat as a destination"),
    ("do you support yammer as a destination", "do you support yammer as a destination"),
    ("do you support viva as a destination", "do you support viva as a destination"),
    ("do you support copilot studio as a destination", "do you support copilot studio as a destination"),
    ("do you support azure logic apps", "do you support azure logic apps"),
    ("do you support eventarc", "do you support eventarc"),
    ("do you support dialogflow", "do you support dialogflow"),
    ("do you support azure policy", "do you support azure policy"),
    ("do you support microsoft planner", "do you support microsoft planner"),
    ("do you support microsoft to do", "do you support microsoft to do"),
    ("do you support microsoft bookings", "do you support microsoft bookings"),
    ("do you support classroom as a source", "do you support classroom as a source"),
    ("do you support azure signalr", "do you support azure signalr"),
    ("do you support azure service fabric", "do you support azure service fabric"),
    ("do you support keep as a source", "do you support keep as a source"),
    ("do you support appsheet as a destination", "do you support appsheet as a destination"),
    ("do you support azure communication services", "do you support azure communication services"),
    ("do you support azure api management", "do you support azure api management"),
    ("do you support cloud workflows", "do you support cloud workflows"),
    ("do you support chronicle as a destination", "do you support chronicle as a destination"),
    ("do you support microsoft forms", "do you support microsoft forms"),
    ("do you support microsoft project", "do you support microsoft project"),
    ("do you support azure container apps", "do you support azure container apps"),
    ("do you support azure front door", "do you support azure front door"),
    ("do you support identity platform", "do you support identity platform"),
    ("do you support pub/sub lite", "do you support pub/sub lite"),
    ("do you support amazon rds for sql server", "do you support amazon rds for sql server"),
    ("do you support cloud dns", "do you support cloud dns"),
    ("do you support cloud domains", "do you support cloud domains"),
    ("do you support natural language api", "do you support natural language api"),
    ("do you support azure database for mariadb", "do you support azure database for mariadb"),
    ("do you support azure relay", "do you support azure relay"),
    ("do you support azure remote rendering", "do you support azure remote rendering"),
    ("do you support azure quantum", "do you support azure quantum"),
    ("do you support azure orbital", "do you support azure orbital"),
    ("do you support azure local", "do you support azure local"),
    ("do you support windows 365 as a destination", "do you support windows 365 as a destination"),
    ("do you support api gateway", "do you support api gateway"),
    ("do you support azure test plans", "do you support azure test plans"),
    ("do you support azure dedicated sql pool", "do you support azure dedicated sql pool"),
    ("do you support cloud cdn", "do you support cloud cdn"),
    ("do you support cloud nat", "do you support cloud nat"),
    ("do you support cloud iap", "do you support cloud iap"),
    ("do you support cloud hsm", "do you support cloud hsm"),
    ("do you support certificate manager", "do you support certificate manager"),
    ("do you support binary authorization", "do you support binary authorization"),
    ("do you support service mesh", "do you support service mesh"),
    ("do you support apigee as a destination", "do you support apigee as a destination"),
    ("do you support azure iot central", "do you support azure iot central"),
    ("do you support azure time series insights", "do you support azure time series insights"),
    ("do you support azure maps", "do you support azure maps"),
    ("do you support azure notification hubs", "do you support azure notification hubs"),
    ("do you support azure web pubsub", "do you support azure web pubsub"),
    ("do you support azure repos as a destination", "do you support azure repos as a destination"),
    ("do you support azure pipelines as a destination", "do you support azure pipelines as a destination"),
    ("do you support bing ads", "do you support bing ads"),
    ("do you support retail api", "do you support retail api"),
    ("do you support healthcare api", "do you support healthcare api"),
    ("do you support cloud endpoints", "do you support cloud endpoints"),
    ("do you support google tag manager", "do you support google tag manager"),
    ("do you support search console as a source", "do you support search console as a source"),
    ("do you support cloud load balancing", "do you support cloud load balancing"),
    ("do you support assured workloads", "do you support assured workloads"),
    ("do you support config connector", "do you support config connector"),
    ("do you support app hub", "do you support app hub"),
    ("do you support microsoft advertising", "do you support microsoft advertising"),
    ("do you support azure artifacts", "do you support azure artifacts"),
    ("do you support document ai as a destination", "do you support document ai as a destination"),
    ("do you support fhir as a destination", "do you support fhir as a destination"),
    ("do you support github copilot as a destination", "do you support github copilot as a destination"),
    ("do you support iceberg", "do you support iceberg"),
    ("do you support sftp", "do you support sftp"),
    ("do you support kafka as a destination", "do you support kafka as a destination"),
    ("do you support mysql", "do you support mysql"),
    ("do you support redis", "do you support redis"),
    ("do you support google photos as a source", "do you support google photos as a source"),
    ("do you support google contacts as a source", "do you support google contacts as a source"),
    ("do you support google maps as a source", "do you support google maps as a source"),
    ("do you support google news as a source", "do you support google news as a source"),
    ("do you support google play as a source", "do you support google play as a source"),
    ("do you support vision ai as a destination", "do you support vision ai as a destination"),
    ("do you support speech-to-text as a destination", "do you support speech-to-text as a destination"),
    ("do you support earth engine", "do you support earth engine"),
    ("do you support bigquery bi engine", "do you support bigquery bi engine"),
    ("do you support azure media services", "do you support azure media services"),
    ("do you support azure cognitive services", "do you support azure cognitive services"),
    ("do you support azure bot service", "do you support azure bot service"),
    ("do you support azure confidential ledger", "do you support azure confidential ledger"),
    ("do you support azure operator nexus", "do you support azure operator nexus"),
    ("do you support microsoft clarity", "do you support microsoft clarity"),
    ("do you support anthos", "do you support anthos"),
    ("do you support azure iot hub", "do you support azure iot hub"),
    ("do you support merchant center", "do you support merchant center"),
    ("do you support google voice as a source", "do you support google voice as a source"),
    ("do you support google pay as a source", "do you support google pay as a source"),
    ("do you support google optimize", "do you support google optimize"),
    ("do you support translation api as a destination", "do you support translation api as a destination"),
    ("do you support recommendations ai", "do you support recommendations ai"),
    ("do you support vertex ai workbench", "do you support vertex ai workbench"),
    ("do you support azure health data services", "do you support azure health data services"),
    ("do you support azure video indexer", "do you support azure video indexer"),
    ("do you support cloud ids", "do you support cloud ids"),
    ("do you support cloud deploy", "do you support cloud deploy"),
    ("do you support cloud source repositories", "do you support cloud source repositories"),
    ("do you support cloud workstations", "do you support cloud workstations"),
    ("do you support artifact analysis", "do you support artifact analysis"),
    ("do you support confidential vm", "do you support confidential vm"),
    ("do you support azure stack hub", "do you support azure stack hub"),
    ("do you support azure stack hci", "do you support azure stack hci"),
    ("do you support dicom as a destination", "do you support dicom as a destination"),
    ("do you support microsoft copilot as a destination", "do you support microsoft copilot as a destination"),
    ("do you support backup for gke", "do you support backup for gke"),
    ("do you support connected sheets", "do you support connected sheets"),
    ("do you support looker embedded", "do you support looker embedded"),
    ("do you support colab enterprise", "do you support colab enterprise"),
    ("do you support automl", "do you support automl"),
    ("do you support digital twins", "do you support digital twins"),
    ("do you support spatial anchors", "do you support spatial anchors"),
    ("do you support floodlight", "do you support floodlight"),
    ("do you support beyondcorp", "do you support beyondcorp"),
    ("do you support tekton", "do you support tekton"),
    ("do you support traffic director", "do you support traffic director"),
    ("do you support parallelstore", "do you support parallelstore"),
    ("do you support netapp volumes", "do you support netapp volumes"),
)


@dataclass(frozen=True)
class AlignPair:
    """One InfoNCE positive: a paraphrase and the gold question it means."""

    query: str
    gold: str


@dataclass(frozen=True)
class CopyExample:
    """One pointer-generator example: copy the answer out of the evidence."""

    question: str
    evidence: str
    answer: str


def _first_sentences(text: str, limit: int = 2) -> str:
    parts: list[str] = []
    rest = (text or "").strip()
    while rest and len(parts) < limit:
        cut = rest.find(". ")
        if cut < 0:
            parts.append(rest.strip())
            break
        parts.append(rest[: cut + 1].strip())
        rest = rest[cut + 2 :].strip()
    return " ".join(p for p in parts if p)


def _paraphrases(gold: str) -> list[str]:
    out: list[str] = []
    seen = {gold.lower()}
    for prefix in _CHAT_PREFIXES:
        candidate = f"{prefix}{gold}".strip()
        key = candidate.lower()
        if key not in seen:
            seen.add(key)
            out.append(candidate)
    lowered = gold
    for src, dst in _SLANG_SWAPS:
        if src.lower() in lowered.lower():
            swapped = re_sub_ci(gold, src, dst)
            key = swapped.lower()
            if key not in seen:
                seen.add(key)
                out.append(swapped)
    return out


def re_sub_ci(text: str, src: str, dst: str) -> str:
    """Case-insensitive single substitution that keeps the rest of the text."""
    import re

    return re.sub(re.escape(src), dst, text, count=1, flags=re.I)


def _as_question(title: str) -> str:
    text = (title or "").strip()
    if not text:
        return ""
    if text.endswith("?"):
        return text
    if text.lower().startswith(("what ", "how ", "can ", "does ", "do ", "who ", "where ", "is ", "why ")):
        return text
    return f"what is {text[0].lower() + text[1:]}" if text else ""


def _generated_sections():
    from src.ai.rag.product_facts import generated_sections

    return generated_sections()


def gold_questions() -> tuple[str, ...]:
    """Canonical questions the dual encoder may rewrite *to*."""
    seen: list[str] = []
    keys: set[str] = set()
    for _query, gold in _CANONICAL_ALIGNS:
        key = gold.lower()
        if key not in keys:
            keys.add(key)
            seen.append(gold)
    try:
        sections = _generated_sections()
    except Exception:
        sections = ()
    for section in sections:
        question = _as_question(section.section_title)
        key = question.lower()
        if question and key not in keys:
            keys.add(key)
            seen.append(question)
    return tuple(seen)


def align_pairs() -> tuple[AlignPair, ...]:
    """InfoNCE positives, including identity pairs so a gold stays near itself."""
    pairs: list[AlignPair] = []
    seen: set[tuple[str, str]] = set()

    def add(query: str, gold: str) -> None:
        key = (query.strip().lower(), gold.strip().lower())
        if not key[0] or not key[1] or key in seen:
            return
        seen.add(key)
        pairs.append(AlignPair(query=query.strip(), gold=gold.strip()))

    for query, gold in _CANONICAL_ALIGNS:
        add(query, gold)
        add(gold, gold)
        for para in _paraphrases(gold):
            add(para, gold)
    for gold in gold_questions():
        add(gold, gold)
        for para in _paraphrases(gold):
            add(para, gold)
    return tuple(pairs)


def copy_examples() -> tuple[CopyExample, ...]:
    """Teacher-force the first sentences of each generated section."""
    examples: list[CopyExample] = []
    try:
        sections = _generated_sections()
    except Exception:
        sections = ()
    for section in sections:
        evidence = (section.text or "").strip()
        answer = _first_sentences(evidence, limit=2)
        if not evidence or not answer:
            continue
        question = _as_question(section.section_title)
        examples.append(
            CopyExample(question=question, evidence=evidence, answer=answer)
        )
        for para in _paraphrases(question)[:4]:
            examples.append(
                CopyExample(question=para, evidence=evidence, answer=answer)
            )
    return tuple(examples)
