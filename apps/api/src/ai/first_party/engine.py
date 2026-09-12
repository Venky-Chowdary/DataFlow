"""Serve path: semantic rewrite + copy-grounded narrate, both fail-closed.

This module is the only thing ``query_analysis`` and ``product_docs``
import. Training stays in ``checkpoint.train_checkpoint``. If the
``npz`` is missing, NumPy is missing, or a gate fails, we return the
deterministic rewrite / extractive draft unchanged. That is the
enterprise contract: a trained head is an improvement when it is
grounded, never a new source of product facts.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from .claims import invented_claims
from .checkpoint import FirstPartyCheckpoint, default_artifact_path, load_checkpoint
from .dual_encoder import nearest_gold
from .pointer_gen import tokens_grounded_in_evidence
from .tokens import word_tokens

# High enough that a gold must be a paraphrase, not a neighbor heading.
# Measured so ``rice`` / off-subject slang stay unrewritten.
REWRITE_COSINE = 0.86

# Heading-ish words the gold is allowed to add without changing the subject.
# ``pilot`` is how the engine section names itself; it is not a CDC claim.
_GENERIC_OK_NEW = frozenset(
    {
        "pilot",
        "datawrap",
        "engine",
        "local",
        "document",
        "product",
        "help",
        "own",
        "third",
        "party",
        "llm",
        "model",
        "generat",
        "pointer",
        "encoder",
        "copy",
    }
)

# Tight families: a new term is allowed only when the source already named
# something in the same family. ``wal`` may become ``wal_level``; ``cdc``
# may not become ``quarantine``.
_SUBJECT_FAMILIES: tuple[frozenset[str], ...] = (
    frozenset({"wal", "wal_level", "pgoutput", "logical", "toast", "publication"}),
    frozenset({"cdc", "capture", "lsn", "df_lsn", "_df_lsn", "binlog", "gtid", "handoff"}),
    frozenset({"quarantine", "rejected", "reject", "bad", "row"}),
    frozenset({"chatgpt", "openai", "anthropic", "ollama", "hybrid", "llm", "engine"}),
    frozenset({"viewer", "editor", "admin", "role", "rbac"}),
    frozenset({"yaml", "gitops", "export", "manifest"}),
    frozenset({"gate", "preflight", "validat", "g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8", "g9"}),
    frozenset({"iceberg", "mor", "cow"}),
    frozenset({"merge", "dialect", "conflict"}),
    frozenset({"mirror", "leftover"}),
    frozenset({"airbyte", "fivetran", "pack", "cdk"}),
    frozenset({"undo", "rollback", "restore"}),
    frozenset({"github", "actions"}),
    frozenset({"openlineage", "lineage"}),
    frozenset({"secret"}),
    frozenset({"byok", "kms"}),
    frozenset({"key_pair", "private_key"}),
    frozenset({"loss", "silent", "ledger", "unaccounted"}),
    frozenset({"soc2", "hipaa", "baa", "gdpr", "dpa", "attestation"}),
    frozenset({"replica", "identity", "tombston", "delet"}),
    frozenset({"dbt", "complement", "export"}),
    frozenset({"ssh", "tunnel", "bastion", "jump"}),
    frozenset({"debezium", "kafka", "flink", "connect", "bridge"}),
    frozenset({"terraform", "gitops", "yaml", "provider"}),
    frozenset({"privatelink", "vpc", "peering"}),
    frozenset({"airflow", "spark", "orchestrat"}),
    frozenset({"glue", "iceberg", "catalog", "nessie"}),
    frozenset({"confirm", "requires_confirm"}),
    frozenset({"audit", "audit_read"}),
    frozenset({"mfa", "login"}),
    frozenset({"allowlist", "cidr"}),
    frozenset({"watermark", "resume", "checkpoint"}),
    frozenset({"incremental", "cursor"}),
    frozenset({"principal", "adls", "service_account"}),
    frozenset({"bigquery"}),
    frozenset({"parallel", "inflight"}),
    frozenset({"scd1"}),
    frozenset({"residency"}),
    frozenset({"session", "ttl"}),
    frozenset({"custom_domain", "vanity"}),
    frozenset({"sqlserver", "mssql"}),
    frozenset({"databricks", "unity"}),
    frozenset({"delta"}),
    frozenset({"full_refresh"}),
    frozenset({"redshift"}),
    frozenset({"synapse"}),
    frozenset({"snowflake"}),
    frozenset({"oracle"}),
    frozenset({"postgresql", "postgres"}),
    frozenset({"mongodb"}),
    frozenset({"mysql"}),
    frozenset({"redis"}),
    frozenset({"sftp"}),
    frozenset({"s3"}),
    frozenset({"collision", "duplicate"}),
    frozenset({"primary_key"}),
    frozenset({"g1"}),
    frozenset({"g2"}),
    frozenset({"g3"}),
    frozenset({"g4"}),
    frozenset({"g5"}),
    frozenset({"g6"}),
    frozenset({"g7"}),
    frozenset({"g8", "reconcil"}),
    frozenset({"g9"}),
    frozenset({"salesforce_oauth", "salesforc_oauth"}),
    frozenset({"slack"}),
    frozenset({"teams_notify", "team_notify"}),
    frozenset({"custom_roles", "custom_role"}),
    frozenset({"field_encryption"}),
    frozenset({"hudi"}),
    frozenset({"kafka_group"}),
    frozenset({"external_vault"}),
    frozenset({"email_notify"}),
    frozenset({"servicenow_notify"}),
    frozenset({"slack_connector"}),
    frozenset({"teams_dest"}),
    frozenset({"hubspot"}),
    frozenset({"stripe"}),
    frozenset({"rls"}),
    frozenset({"snowflake_dynamic"}),
    frozenset({"pause_cdc"}),
    frozenset({"salesforce_connect"}),
    frozenset({"cdc_read_replica"}),
    frozenset({"oracle_logminer"}),
    frozenset({"sqlserver_cdc"}),
    frozenset({"sqlserver_ct"}),
    frozenset({"cdc_event_filter"}),
    frozenset({"cdc_skip_deletes"}),
    frozenset({"cdc_heartbeat_interval"}),
    frozenset({"cdc_fetch_size"}),
    frozenset({"oracle_xstream"}),
    frozenset({"sqlserver_ag"}),
    frozenset({"fabric"}),
    frozenset({"power_bi"}),
    frozenset({"adf"}),
    frozenset({"azure_managed_identity"}),
    frozenset({"cosmos"}),
    frozenset({"event_hubs"}),
    frozenset({"service_bus"}),
    frozenset({"cloud_sql"}),
    frozenset({"azure_sql"}),
    frozenset({"gcs"}),
    frozenset({"pubsub"}),
    frozenset({"spanner"}),
    frozenset({"vertex_ai"}),
    frozenset({"sharepoint"}),
    frozenset({"dynamics365", "dataverse"}),
    frozenset({"purview"}),
    frozenset({"excel_online"}),
    frozenset({"aws_iam_role"}),
    frozenset({"custom_slot_name"}),
    frozenset({"blue_green_cutover"}),
    frozenset({"scd2"}),
    frozenset({"incremental_updated_at"}),
    frozenset({"azure_database_postgresql"}),
    frozenset({"azure_database_mysql"}),
    frozenset({"cloud_sql_sqlserver"}),
    frozenset({"onedrive"}),
    frozenset({"looker"}),
    frozenset({"looker_studio"}),
    frozenset({"bigquery_omni"}),
    frozenset({"google_sheets"}),
    frozenset({"alloydb"}),
    frozenset({"microsoft_graph"}),
    frozenset({"azure_openai"}),
    frozenset({"kusto"}),
    frozenset({"event_grid"}),
    frozenset({"dataflow_google"}),
    frozenset({"power_platform"}),
    frozenset({"conditional_access"}),
    frozenset({"cloud_composer"}),
    frozenset({"azure_blob"}),
    frozenset({"azure_redis", "redis"}),
    frozenset({"azure_flexible_server"}),
    frozenset({"firebase", "firebase_ready"}),
    frozenset({"firestore"}),
    frozenset({"bigtable"}),
    frozenset({"dataproc"}),
    frozenset({"entra_pim"}),
    frozenset({"gke"}),
    frozenset({"outlook"}),
    frozenset({"youtube"}),
    frozenset({"google_ads"}),
    frozenset({"google_drive"}),
    frozenset({"google_docs"}),
    frozenset({"google_analytics"}),
    frozenset({"cloud_run"}),
    frozenset({"stream_analytics"}),
    frozenset({"microsoft_lists"}),
    frozenset({"exchange_online"}),
    frozenset({"github_enterprise"}),
    frozenset({"bq_dts"}),
    frozenset({"bq_linked_dataset"}),
    frozenset({"cloud_kms"}),
    frozenset({"app_engine"}),
    frozenset({"data_catalog"}),
    frozenset({"cloud_build"}),
    frozenset({"artifact_registry"}),
    frozenset({"cloud_functions"}),
    frozenset({"azure_files"}),
    frozenset({"log_analytics"}),
    frozenset({"azure_ai_search"}),
    frozenset({"analysis_services"}),
    frozenset({"cloud_sql_auth_proxy"}),
    frozenset({"synapse_link"}),
    frozenset({"azure_table", "table_storage"}),
    frozenset({"azure_queue", "queue_storage"}),
    frozenset({"sqlserver_azure_vm"}),
    frozenset({"splunk"}),
    frozenset({"tableau"}),
    frozenset({"data_lake_gen2"}),
    frozenset({"azure_sql_sp"}),
    frozenset({"dynamodb"}),
    frozenset({"elasticsearch"}),
    frozenset({"elastic_cloud"}),
    frozenset({"opensearch"}),
    frozenset({"teams_source"}),
    frozenset({"microsoft_365"}),
    frozenset({"memorystore"}),
    frozenset({"azure_sql_edge"}),
    frozenset({"intune"}),
    frozenset({"defender"}),
    frozenset({"sentinel"}),
    frozenset({"azure_ml"}),
    frozenset({"search_ads_360"}),
    frozenset({"cloud_tasks"}),
    frozenset({"vpc_sc"}),
    frozenset({"azure_firewall"}),
    frozenset({"campaign_manager"}),
    frozenset({"aurora"}),
    frozenset({"documentdb"}),
    frozenset({"cloud_armor"}),
    frozenset({"cloud_interconnect"}),
    frozenset({"cloud_vpn"}),
    frozenset({"azure_arc"}),
    frozenset({"azure_lighthouse"}),
    frozenset({"azure_monitor"}),
    frozenset({"azure_devops"}),
    frozenset({"azure_boards"}),
    frozenset({"azure_migrate"}),
    frozenset({"dv360"}),
    frozenset({"gcs_transfer_service"}),
    frozenset({"qlik"}),
    frozenset({"entra_governance"}),
    frozenset({"cloud_sql_mysql"}),
    frozenset({"hdinsight"}),
    frozenset({"expressroute"}),
    frozenset({"ssis"}),
    frozenset({"ssrs"}),
    frozenset({"gmail"}),
    frozenset({"google_calendar"}),
    frozenset({"azure_cdn"}),
    frozenset({"azure_blueprints"}),
    frozenset({"azure_automation"}),
    frozenset({"bigquery_ml"}),
    frozenset({"rds_postgresql"}),
    frozenset({"rds_mysql"}),
    frozenset({"cloud_sql_postgresql"}),
    frozenset({"filestore"}),
    frozenset({"persistent_disk"}),
    frozenset({"datastream"}),
    frozenset({"dataplex"}),
    frozenset({"informatica"}),
    frozenset({"talend"}),
    frozenset({"matillion"}),
    frozenset({"google_workspace"}),
    frozenset({"aks"}),
    frozenset({"azure_functions"}),
    frozenset({"azure_batch"}),
    frozenset({"cloud_scheduler"}),
    frozenset({"vertex_ai_search"}),
    frozenset({"business_central"}),
    frozenset({"application_insights"}),
    frozenset({"site_recovery"}),
    frozenset({"entra_external_id"}),
    frozenset({"azure_ad_b2c"}),
    frozenset({"google_meet"}),
    frozenset({"google_chat"}),
    frozenset({"yammer"}),
    frozenset({"viva"}),
    frozenset({"copilot_studio"}),
    frozenset({"logic_apps"}),
    frozenset({"eventarc"}),
    frozenset({"dialogflow"}),
    frozenset({"azure_policy"}),
    frozenset({"planner"}),
    frozenset({"microsoft_todo"}),
    frozenset({"bookings"}),
    frozenset({"google_classroom"}),
    frozenset({"azure_signalr"}),
    frozenset({"azure_service_fabric"}),
    frozenset({"google_keep"}),
    frozenset({"appsheet"}),
    frozenset({"azure_communication"}),
    frozenset({"azure_apim"}),
    frozenset({"cloud_workflows"}),
    frozenset({"chronicle"}),
    frozenset({"microsoft_forms"}),
    frozenset({"microsoft_project"}),
    frozenset({"container_apps"}),
    frozenset({"azure_front_door"}),
    frozenset({"identity_platform"}),
    frozenset({"pubsub_lite"}),
    frozenset({"rds_sqlserver"}),
    frozenset({"cloud_dns"}),
    frozenset({"cloud_domains"}),
    frozenset({"natural_language_api"}),
    frozenset({"azure_mariadb"}),
    frozenset({"azure_relay"}),
    frozenset({"azure_remote_rendering"}),
    frozenset({"azure_quantum"}),
    frozenset({"azure_orbital"}),
    frozenset({"azure_local"}),
    frozenset({"windows_365"}),
    frozenset({"api_gateway"}),
    frozenset({"azure_test_plans"}),
    frozenset({"azure_dedicated_sql_pool"}),
    frozenset({"cloud_cdn"}),
    frozenset({"cloud_nat"}),
    frozenset({"cloud_iap"}),
    frozenset({"cloud_hsm"}),
    frozenset({"certificate_manager"}),
    frozenset({"binary_authorization"}),
    frozenset({"service_mesh"}),
    frozenset({"apigee"}),
    frozenset({"iot_central"}),
    frozenset({"time_series_insights"}),
    frozenset({"azure_maps"}),
    frozenset({"notification_hubs"}),
    frozenset({"azure_web_pubsub"}),
    frozenset({"azure_repos"}),
    frozenset({"azure_pipelines"}),
    frozenset({"bing_ads"}),
    frozenset({"retail_api"}),
    frozenset({"healthcare_api"}),
    frozenset({"cloud_endpoints"}),
    frozenset({"tag_manager"}),
    frozenset({"search_console"}),
    frozenset({"cloud_load_balancing"}),
    frozenset({"assured_workloads"}),
    frozenset({"config_connector"}),
    frozenset({"app_hub"}),
    frozenset({"microsoft_advertising"}),
    frozenset({"azure_artifacts"}),
    frozenset({"document_ai"}),
    frozenset({"fhir"}),
    frozenset({"github_copilot"}),
    frozenset({"adls"}),
    frozenset({"privatelink"}),
    frozenset({"synapse", "azure_synapse", "synaps"}),
    frozenset({"slot_quota"}),
    frozenset({"hosted"}),
    frozenset({"gcp"}),
    frozenset({"workload_identity"}),
    frozenset({"column_lineage"}),
    frozenset({"goldengate"}),
    frozenset({"sharing"}),
)


def _env_on(name: str, default: bool = True) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"0", "false", "off", "no"}:
        return False
    if raw in {"1", "true", "on", "yes"}:
        return True
    return default


def rewrite_enabled() -> bool:
    return _env_on("DATAFLOW_FP_REWRITE", default=True)


def narrate_enabled() -> bool:
    return _env_on("DATAFLOW_FP_NARRATE", default=True)


@lru_cache(maxsize=1)
def load_model(path: str | None = None) -> FirstPartyCheckpoint | None:
    """Load the shipped checkpoint, or ``None`` if serve must stay extractive."""
    try:
        import numpy  # noqa: F401
    except ImportError:
        return None
    artifact = Path(path) if path else default_artifact_path()
    if not artifact.is_file():
        return None
    try:
        return load_checkpoint(artifact)
    except Exception:
        return None


def reset_model_cache() -> None:
    """Test helper — drop the process-wide checkpoint."""
    clear = getattr(load_model, "cache_clear", None)
    if callable(clear):
        clear()


def _subject_terms(text: str) -> set[str]:
    from src.ai.rag.evidence_policy import is_subject_term
    from src.ai.rag.lexical_index import content_terms

    return {term for term in content_terms(text) if is_subject_term(term)}


def _src_closure(text: str) -> set[str]:
    """Typed terms plus phrase/loose expansions — not a recursive rewrite."""
    from src.ai.rag.lexical_index import content_terms, normalize
    from src.ai.rag.query_analysis import expand_terms_tiered

    terms = set(content_terms(text))
    raw = tuple(terms)
    phrase, loose = expand_terms_tiered(text, raw)
    terms.update(phrase)
    terms.update(loose)
    terms.update(normalize(t) for t in word_tokens(text))
    return terms


def _same_family(new_term: str, src_terms: set[str]) -> bool:
    for family in _SUBJECT_FAMILIES:
        if new_term in family and src_terms & family:
            return True
    return False


def _lexical_variant(new_term: str, src_terms: set[str]) -> bool:
    for src in src_terms:
        if not src or not new_term:
            continue
        if new_term == src:
            return True
        if len(src) >= 3 and (src in new_term or new_term in src):
            return True
    return False


def drops_distinctive_subjects(source: str, gold: str) -> bool:
    """Whether snapping to ``gold`` would drop a subject the operator named.

    ``can I bring Iceberg with a Glue catalog`` must not become a generic
    Iceberg question — that is how merge-on-read stole the Glue catalog
    lead after a high-cosine snap.
    """
    src = _subject_terms(source)
    if not src:
        return False
    kept = _subject_terms(gold) | _src_closure(gold)
    lost = (src - kept) - _GENERIC_OK_NEW
    return bool(lost)


def introduces_unrelated_subjects(source: str, gold: str) -> bool:
    """Whether snapping to ``gold`` would name a new product subject.

    Allowed additions: generic engine words, lexical variants
    (``wal`` → ``wal_level``), and family members of a source term.
    Everything else is a neighbor steal and is refused.
    """
    src_subjects = _subject_terms(source)
    if not src_subjects:
        return True
    gold_subjects = _subject_terms(gold)
    closure = _src_closure(source) | src_subjects
    new = gold_subjects - closure
    for term in new:
        if term in _GENERIC_OK_NEW:
            continue
        if _same_family(term, closure):
            continue
        if _lexical_variant(term, closure):
            continue
        return True
    return False


def semantic_rewrite(question: str) -> str | None:
    """Nearest gold question, or ``None`` when the snap is not safe.

    Callers already ran ``rewrite_operator_question``. This only fires
    when the cosine is high **and** the gold does not introduce an
    unrelated product subject. Off-subject English stays off-subject.
    """
    if not rewrite_enabled():
        return None
    text = (question or "").strip()
    if not text:
        return None
    model = load_model()
    if model is None or not model.gold_questions:
        return None
    src_subjects = _subject_terms(text)
    if not src_subjects:
        return None
    gold, score = nearest_gold(
        model.encoder,
        text,
        list(model.gold_questions),
        model.gold_vectors,
    )
    if not gold or score < REWRITE_COSINE:
        return None
    if gold.strip().lower() == text.lower():
        return None
    if introduces_unrelated_subjects(text, gold):
        return None
    if drops_distinctive_subjects(text, gold):
        return None
    return gold


def retrieve_bonus(query: str, passages: list[str]) -> list[float]:
    """Optional dual-encoder scores for already-retrieved candidates.

    Not wired into fusion by default: a retrieve bonus is how a new
    heading steals a neighbor in a ``limit=4`` window. Exposed for
    experiments and tests.
    """
    model = load_model()
    if model is None:
        return [0.0] * len(passages)
    q = model.encoder.encode_query(query)
    return [float(q @ model.encoder.encode_gold(p)) for p in passages]


def narrate_answer(question: str, evidence: str, draft: str) -> str | None:
    """Copy-grounded fluency over an extractive draft, or ``None``.

    The extractive composer stays the source of facts. This may add a
    closed prefix. It may not drop ``_df_lsn``, invent dbt/SSH, or turn
    at-least-once into exactly-once.
    """
    if not narrate_enabled():
        return None
    body = (draft or "").strip()
    if not body:
        return None
    model = load_model()
    if model is None:
        return None
    from src.ai.rag.evidence import keeps_draft_facts, retains_evidence
    from src.ai.rag.lexical_index import content_terms

    generated = model.generator.narrate(
        model.encoder, question, evidence, body
    ).strip()
    if not generated or generated == body:
        return None
    if invented_claims(generated, evidence):
        return None
    if not tokens_grounded_in_evidence(generated, f"{evidence} {body}"):
        return None
    if not keeps_draft_facts(body, generated):
        return None
    matched = set(content_terms(question))
    context = set(content_terms(f"{evidence} {body}"))
    if not retains_evidence(generated, matched, context):
        return None
    return generated
