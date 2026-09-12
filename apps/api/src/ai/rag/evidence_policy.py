"""Whether the retrieved passages are enough to answer — the decision that was wrong.

The previous rule was a single absolute threshold: one passage had to cover 55%
of the question's IDF-weighted terms or the whole question was refused. That
measures the wrong thing twice over.

*It judges one passage at a time.* Real answers are assembled from several
sections — "which sync mode for a nightly load" is answered by the sync-mode
list plus the schedule cadence section, and neither covers the question alone.

*It scales with question length.* A six-word question can clear 55%; the same
question asked in a full sentence cannot, because every extra word raises the
denominator. Measured on the shipped corpus this refused 14 of 30 ordinary
operator questions, including "what happens to bad rows" and "how do I connect
to BigQuery" — both squarely documented.

What actually distinguishes an answerable question is not how much of it one
passage repeats, but whether it **names a subject the documentation is about**
and whether the retrieved set **covers that subject**. So the decision here rests
on two independent signals:

``anchor``    at least one of the question's terms (after expansion) is a subject
              the documentation has a heading for, and the retrieved passages
              cover it. This is what keeps an off-subject question refused:
              "what is the capital of France" does name a corpus term
              (``capital`` appears in one passage) but no heading is *about*
              capitals, so there is no anchor and the question is refused.

``coverage``  the share of the question's content terms the retrieved set
              accounts for. Below a floor the question is mostly about things
              the documentation never mentions.

Together they give three outcomes rather than two. The missing middle is what
made the old behaviour feel evasive: "how do I see why a job was slow" is a
question about ``job``, which is documented, plus ``slow``, which is not. The
honest response is the job-phase material *and* a sentence saying duration is not
covered — not a blanket refusal.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from functools import lru_cache

from .lexical_index import content_terms, normalize
from .query_analysis import GENERIC_QUESTION_WORDS, QueryAnalysis, phrase_evidence

#: Share of the question's content terms the retrieved set must account for
#: before the answer is offered without a caveat.
COVERAGE_FLOOR = 0.6

#: Below this the question is mostly about undocumented things, and naming one
#: documented subject is not enough to answer it. "write me a poem about the
#: sea" anchors on ``write`` — a real heading term — and is refused here.
PARTIAL_FLOOR = 0.34

#: A ``snake_case`` word an operator types is the name of one of their own
#: objects, not a concept. "What semantic patterns match subscriber_id column
#: naming" is about ``subscriber_id``; the documentation describes semantic
#: roles in general and cannot vouch for that column, so narrating the general
#: material as the answer would be exactly the invented answer this product
#: refuses to give. When the documentation does use the name — every sync mode
#: is a ``snake_case`` identifier — it is covered and nothing changes.
_MIN_OBJECT_NAME_LEN = 5

#: Shortest catalog identifier worth admitting as a subject. Below this they are
#: acronyms that collide with ordinary words (``ai``, ``box``, ``sap``).
_MIN_CATALOG_TERM_LEN = 5

# Heading words that name no subject. A heading term is the strongest available
# signal that the documentation is *about* something, but headings also contain
# ordinary English. Without this subtraction "write me a poem" anchors on
# ``write`` and "get me a beer" anchors on ``get``.
_NON_SUBJECT_HEADING_WORDS = frozenset(
    normalize(w)
    for w in """
    add create open run runs get use used see work works write receive
    first next last before after related optional different same other
    guide guides tips tip example examples checklist procedure step steps
    question questions asked frequently concept concepts page surface
    support supported honest exact label bind hand bridge off
    live native advanced ready order fix inspect configure manage
    """.split()
)


@lru_cache(maxsize=1)
def product_subjects() -> frozenset[str]:
    """Terms the shipped documentation has a heading about.

    Derived from the corpus rather than hand-listed, so it grows when the corpus
    does. Article and section titles are what the documentation declares itself
    to be about, which is a far better answerability signal than IDF: the core
    subjects of this product (``connector``, ``gate``, ``pipeline``) appear in
    most passages and therefore have *low* IDF, while noise words like ``tips``
    and ``first`` have high IDF.
    """
    from .product_docs import load_product_doc_chunks

    terms: set[str] = set()
    for chunk in load_product_doc_chunks():
        terms.update(content_terms(f"{chunk.doc_title} {chunk.section_title}"))
    return frozenset(terms - _NON_SUBJECT_HEADING_WORDS - GENERIC_QUESTION_WORDS)


@lru_cache(maxsize=1)
def subject_aliases() -> frozenset[str]:
    """Subjects the product has, spelled the way an operator would type them.

    A heading says "Sync modes (exact product labels)"; an operator says
    ``upsert`` or ``scd2``. These are the product's own enum values and gate
    names — shipped behaviour, not authored prose — so anchoring on them is
    still anchoring on something the product documents by doing it.
    """
    extra: set[str] = set()
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES

        for mode in CANONICAL_SYNC_MODES:
            extra.update(content_terms(mode.replace("_", " ")))
            extra.add(normalize(mode))
    except Exception:
        pass
    try:
        from services.schedule_store import SCHEMA_POLICIES

        for policy in SCHEMA_POLICIES:
            extra.update(content_terms(policy.replace("_", " ")))
            # Keep the snake_case token. ``type_locked`` tokenized to
            # ``type`` + ``locked`` and "what is type_locked" was refused
            # even though the policy enum is the product's own label.
            extra.add(normalize(policy))
    except Exception:
        pass
    try:
        from services.rbac import role_names

        extra.update(normalize(r) for r in role_names())
    except Exception:
        extra.update({"viewer", "editor", "operator", "admin"})
    extra.update(f"g{i}" for i in range(1, 10))
    extra.update(
        {
            "wal_level",
            "pgoutput",
            "toast",
            "replication",
            "publication",
            "gtid",
            "binlog",
            "preimage",
            "backfill",
            "iceberg",
            "sftp",
            "mysql",
            "redis",
            "chatgpt",
            "dbt",
            "ssh",
            "bastion",
            "debezium",
            "terraform",
            "privatelink",
            "airflow",
            "goldengate",
            "glue",
            "confirm",
            "soc2",
            "hipaa",
            "baa",
            "openlineage",
            "github",
            "undo",
            "rollback",
            "allowlist",
            "mfa",
            "principal",
            "audit_read",
            "scd1",
            "residency",
            "sqlserver",
            "databricks",
            "delta",
            "custom_domain",
            "full_refresh",
            "parallel",
            "session",
            "unity",
            "mssql",
            "redshift",
            "synapse",
            "oracle",
            "mongodb",
            "postgresql",
            "collision",
            "hosted",
            "gcp",
            "workload_identity",
            "primary_key",
            "g8",
            "reconcil",
            "salesforce_oauth",
            "salesforc_oauth",
            "slack",
            "teams_notify",
            "team_notify",
            "custom_roles",
            "custom_role",
            "field_encryption",
            "hudi",
            "kafka_group",
            "external_vault",
            "email_notify",
            "servicenow_notify",
            "slack_connector",
            "teams_dest",
            "hubspot",
            "stripe",
            "rls",
            "snowflake_dynamic",
            "pause_cdc",
            "salesforce_connect",
            "cdc_read_replica",
            "oracle_logminer",
            "sqlserver_cdc",
            "sqlserver_ct",
            "cdc_event_filter",
            "cdc_skip_deletes",
            "cdc_heartbeat_interval",
            "cdc_fetch_size",
            "oracle_xstream",
            "sqlserver_ag",
            "fabric",
            "power_bi",
            "adf",
            "azure_managed_identity",
            "cosmos",
            "event_hubs",
            "service_bus",
            "cloud_sql",
            "azure_sql",
            "gcs",
            "pubsub",
            "spanner",
            "vertex_ai",
            "sharepoint",
            "dynamics365",
            "dataverse",
            "purview",
            "excel_online",
            "aws_iam_role",
            "custom_slot_name",
            "blue_green_cutover",
            "scd2",
            "incremental_updated_at",
            "azure_database_postgresql",
            "azure_database_mysql",
            "cloud_sql_sqlserver",
            "onedrive",
            "looker",
            "looker_studio",
            "bigquery_omni",
            "google_sheets",
            "alloydb",
            "microsoft_graph",
            "azure_openai",
            "kusto",
            "event_grid",
            "dataflow_google",
            "power_platform",
            "conditional_access",
            "cloud_composer",
            "azure_blob",
            "azure_redis",
            "azure_flexible_server",
            "firebase",
            "firebase_ready",
            "firestore",
            "bigtable",
            "dataproc",
            "entra_pim",
            "gke",
            "outlook",
            "youtube",
            "google_ads",
            "google_drive",
            "google_docs",
            "google_analytics",
            "cloud_run",
            "stream_analytics",
            "microsoft_lists",
            "exchange_online",
            "github_enterprise",
            "bq_dts",
            "bq_linked_dataset",
            "cloud_kms",
            "app_engine",
            "data_catalog",
            "cloud_build",
            "artifact_registry",
            "cloud_functions",
            "azure_files",
            "log_analytics",
            "azure_ai_search",
            "analysis_services",
            "cloud_sql_auth_proxy",
            "synapse_link",
            "azure_table",
            "table_storage",
            "azure_queue",
            "queue_storage",
            "sqlserver_azure_vm",
            "splunk",
            "tableau",
            "data_lake_gen2",
            "azure_sql_sp",
            "dynamodb",
            "elasticsearch",
            "elastic_cloud",
            "opensearch",
            "teams_source",
            "microsoft_365",
            "memorystore",
            "azure_sql_edge",
            "intune",
            "defender",
            "sentinel",
            "azure_ml",
            "search_ads_360",
            "cloud_tasks",
            "vpc_sc",
            "azure_firewall",
            "campaign_manager",
            "aurora",
            "documentdb",
            "cloud_armor",
            "cloud_interconnect",
            "cloud_vpn",
            "azure_arc",
            "azure_lighthouse",
            "azure_monitor",
            "azure_devops",
            "azure_boards",
            "azure_migrate",
            "dv360",
            "gcs_transfer_service",
            "qlik",
            "entra_governance",
            "cloud_sql_mysql",
            "hdinsight",
            "expressroute",
            "ssis",
            "ssrs",
            "gmail",
            "google_calendar",
            "azure_cdn",
            "azure_blueprints",
            "azure_automation",
            "bigquery_ml",
            "rds_postgresql",
            "rds_mysql",
            "cloud_sql_postgresql",
            "filestore",
            "persistent_disk",
            "datastream",
            "dataplex",
            "informatica",
            "talend",
            "matillion",
            "google_workspace",
            "aks",
            "azure_functions",
            "azure_batch",
            "cloud_scheduler",
            "vertex_ai_search",
            "business_central",
            "application_insights",
            "site_recovery",
            "entra_external_id",
            "azure_ad_b2c",
            "google_meet",
            "google_chat",
            "yammer",
            "viva",
            "copilot_studio",
            "logic_apps",
            "eventarc",
            "dialogflow",
            "azure_policy",
            "planner",
            "microsoft_todo",
            "bookings",
            "google_classroom",
            "azure_signalr",
            "azure_service_fabric",
            "google_keep",
            "appsheet",
            "azure_communication",
            "azure_apim",
            "cloud_workflows",
            "chronicle",
            "microsoft_forms",
            "microsoft_project",
            "container_apps",
            "azure_front_door",
            "identity_platform",
            "pubsub_lite",
            "rds_sqlserver",
            "cloud_dns",
            "cloud_domains",
            "natural_language_api",
            "azure_mariadb",
            "azure_relay",
            "azure_remote_rendering",
            "azure_quantum",
            "azure_orbital",
            "azure_local",
            "windows_365",
            "api_gateway",
            "azure_test_plans",
            "azure_dedicated_sql_pool",
            "cloud_cdn",
            "cloud_nat",
            "cloud_iap",
            "cloud_hsm",
            "certificate_manager",
            "binary_authorization",
            "service_mesh",
            "apigee",
            "iot_central",
            "time_series_insights",
            "azure_maps",
            "notification_hubs",
            "azure_web_pubsub",
            "azure_repos",
            "azure_pipelines",
            "bing_ads",
            "retail_api",
            "healthcare_api",
            "cloud_endpoints",
            "tag_manager",
            "search_console",
            "cloud_load_balancing",
            "assured_workloads",
            "config_connector",
            "app_hub",
            "microsoft_advertising",
            "azure_artifacts",
            "document_ai",
            "fhir",
            "github_copilot",
            "google_photos",
            "google_contacts",
            "google_maps",
            "google_news",
            "google_play",
            "vision_ai",
            "speech_to_text",
            "earth_engine",
            "bigquery_bi_engine",
            "azure_media",
            "azure_cognitive",
            "azure_bot",
            "confidential_ledger",
            "operator_nexus",
            "microsoft_clarity",
            "anthos",
            "iot_hub",
            "merchant_center",
            "adls",
            "privatelink",
            "azure_synapse",
            "synaps",
            "slot_quota",
            "g1",
            "g2",
            "g3",
            "g4",
            "g5",
            "g6",
            "g7",
            "g9",
            "workload",
            "column_lineage",
            "sharing",
            "goldengate",
        }
    )
    try:
        # The logical type space is an enum the engine dispatches on, so a
        # question about booleans, arrays, decimals or binary names a subject
        # the product documents by doing it. Heading words alone missed all of
        # them: the generated section is titled "Destination type for each
        # logical type", so "how do you handle booleans" was read as off-subject.
        from services.type_system import CANONICAL_TYPES

        for logical in set(CANONICAL_TYPES.values()):
            extra.update(content_terms(logical.replace("_", " ")))
    except Exception:
        pass
    try:
        # The engine and format names the transfer engine dispatches on. An
        # operator's first question is almost always "can it do <my system>",
        # and the catalog is the product's own enum for that: the generated
        # catalog passage lists every one of these, but they appear in its body
        # and its heading is "Which engines you can connect", so "can it do
        # salesforce" was read as off-subject and refused.
        import registry

        for group in ("DATABASE_TYPES", "FILE_FORMATS"):
            for item in getattr(registry, group, ()) or ():
                name = str(getattr(item, "value", item))
                extra.update(content_terms(name.replace("_", " ")))
                extra.add(normalize(name))
    except Exception:
        pass
    try:
        # Every catalog tile, so "can it do <system>" is answerable for all of
        # them — including the roadmap ones, where the honest answer is that a
        # tile is not a transfer-ready driver. Identifiers only: a tile's
        # display name can be an ordinary English word ("Close", "Front",
        # "Monday"), and admitting those would make off-subject questions look
        # like product questions.
        from services.catalog_service import enriched_connectors

        for tile in enriched_connectors():
            for key in ("id", "driver_type"):
                value = str(tile.get(key) or "").strip().lower()
                if len(value) >= _MIN_CATALOG_TERM_LEN:
                    extra.update(content_terms(value.replace("_", " ")))
                    extra.add(normalize(value))
    except Exception:
        pass
    try:
        # The schema aspects every migration certificate has to account for —
        # not null, default, collation, encoding, offset label, partitioning.
        from services.schema_fidelity import REQUIRED_ASPECTS

        for aspect in REQUIRED_ASPECTS:
            extra.update(content_terms(aspect.replace("_", " ")))
    except Exception:
        pass
    return frozenset(extra - GENERIC_QUESTION_WORDS - _NON_SUBJECT_HEADING_WORDS)


def is_subject_term(term: str) -> bool:
    """Whether one term names something this product documents."""
    return term in product_subjects() or term in subject_aliases()


def names_own_object(term: str) -> bool:
    """Whether a term is the ``snake_case`` name of one of the operator's objects."""
    return "_" in term and len(term) >= _MIN_OBJECT_NAME_LEN


@dataclass(frozen=True)
class EvidenceVerdict:
    """The answerability decision, with the numbers it was made on."""

    #: ``answer`` · ``partial`` · ``refuse``
    outcome: str
    coverage: float
    anchors: tuple[str, ...] = ()
    covered_anchors: tuple[str, ...] = ()
    uncovered_terms: tuple[str, ...] = ()
    #: The subset of ``uncovered_terms`` that names something this product
    #: documents elsewhere. Only these are worth telling the operator about:
    #: that the documentation does not contain the word "low" from "why is my
    #: mapping confidence low" is true and completely uninformative.
    uncovered_subjects: tuple[str, ...] = ()
    reason: str = ""
    subjects_in_question: tuple[str, ...] = field(default=())

    @property
    def answerable(self) -> bool:
        return self.outcome in {"answer", "partial"}

    @property
    def partial(self) -> bool:
        return self.outcome == "partial"

    def as_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "coverage": round(self.coverage, 4),
            "anchors": list(self.anchors),
            "covered_anchors": list(self.covered_anchors),
            "uncovered_terms": list(self.uncovered_terms),
            "uncovered_subjects": list(self.uncovered_subjects),
            "reason": self.reason,
        }


def assess_evidence(
    analysis: QueryAnalysis,
    passage_texts: Sequence[str],
    *,
    coverage_floor: float = COVERAGE_FLOOR,
    partial_floor: float = PARTIAL_FLOOR,
    in_vocabulary: Callable[[str], bool] | None = None,
) -> EvidenceVerdict:
    """Decide whether ``passage_texts`` can answer ``analysis``.

    ``passage_texts`` is the fused top-k, judged as one body of evidence — an
    answer composed from three sections is a normal answer, not a stretch.

    ``in_vocabulary`` answers whether the *whole corpus* has ever used a term,
    which is what distinguishes a question carrying ordinary English the
    documentation also uses from one carrying words it has never seen.
    """
    question_terms = tuple(analysis.terms)
    if not question_terms:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=0.0,
            reason="The question carries no searchable terms.",
        )

    covered: set[str] = set()
    for text in passage_texts:
        covered.update(content_terms(text))
    # A passage that says ``reverse_etl`` covers the words "reverse" and "etl".
    # Splitting here rather than in the index keeps this out of ranking, where
    # it would add ``write`` and ``etl`` to every passage that lists a sync mode
    # and let those outrank the section the question was about.
    # The parts are already normalized: ``normalize`` stems a ``snake_case``
    # identifier part by part, so splitting one yields canonical tokens. Stemming
    # them a second time eroded them — ``revers_etl`` split to ``rever``, which
    # is not what the question side produces — because a suffix stripper is a
    # single pass, not a rule that converges.
    for term in tuple(covered):
        if "_" in term:
            covered.update(part for part in term.split("_") if len(part) > 1)

    subjects = tuple(t for t in analysis.search_terms if is_subject_term(t))
    anchors = tuple(dict.fromkeys(subjects))
    covered_anchors = tuple(a for a in anchors if a in covered)

    # Coverage is measured on what the operator actually typed. Expansion terms
    # widen retrieval; letting them also satisfy coverage would mean the
    # expansion table could talk itself into an answer.
    #
    # One exception, and only because the alternative is measuring nothing. A
    # word the corpus has *never* used cannot be absent from a passage
    # informatively — its absence is guaranteed, so counting it as a gap says
    # only that the documentation spells the concept differently. Measured:
    # "explain aggregation" and "what does a breakdown show me" were refused at
    # 0% coverage against a passage that defines a grouped aggregate, because
    # ``aggregation`` does not stem to ``aggregat`` and ``breakdown`` written as
    # one word shares no token with "break down".
    #
    # Conservative on both axes so the expansion table still cannot argue
    # itself into an answer: the credit reaches only corpus-foreign terms a
    # *phrase* rule consumed — the tier that matches an exact spelling and is
    # already trusted for retrieval, never the single-word guesses — and only
    # when every one of that rule's targets is in the evidence. A rule whose
    # targets are too broad for that to hold earns nothing, which is the right
    # direction for a policy whose job is to refuse.
    spoken_for: set[str] = set()
    if in_vocabulary is not None:
        for consumed, targets in phrase_evidence(analysis.text):
            if not targets or not all(t in covered for t in targets):
                continue
            spoken_for.update(
                t for t in consumed if t not in covered and not in_vocabulary(t)
            )
    hit = [t for t in question_terms if t in covered or t in spoken_for]
    uncovered = tuple(
        t for t in question_terms if t not in covered and t not in spoken_for
    )
    uncovered_subjects = tuple(t for t in uncovered if is_subject_term(t))
    coverage = len(hit) / len(question_terms)

    if not covered_anchors:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=coverage,
            anchors=anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                "The question names no subject the documentation has a section about."
                if not anchors
                else "The retrieved sections do not cover the subject the question names."
            ),
        )

    # Words the documentation has never used anywhere, not merely absent from
    # the passages that ranked. They are the difference between a question
    # phrased in this product's English and a question about something else:
    # "how do you handle very large decimals" leaves "large" uncovered and the
    # corpus does say "large", while "unmapped nonsense query zzqq" leaves
    # words the corpus has never seen and must stay refused.
    foreign = (
        tuple(t for t in uncovered if not in_vocabulary(t))
        if in_vocabulary is not None
        else ()
    )

    # Every subject the question names is covered, nothing left over names
    # anything else this product documents, and nothing left over is foreign to
    # the corpus. The coverage ratio is then measuring ordinary English — "how
    # do you *handle* *very* *large* decimals" is one part subject and three
    # parts framing — and refusing on it declined a question the corpus answers
    # outright. Anchor coverage is the stronger signal, so it wins; the floor
    # still applies whenever an anchor is missing or the remainder points
    # somewhere the documentation does not go.
    #
    # ``uncovered_subjects`` is read off the typed terms, so the escape asks only
    # that the subjects the *operator* named are covered. Requiring it of every
    # anchor let the expansion table talk the answer down the way the coverage
    # ratio above forbids it to talk one up: "truncated unmapped nonsense
    # decimals" expands ``truncated`` to the ``overwrite`` sync mode, and a
    # passage about decimals does not mention overwriting, so a question whose
    # own subject was covered was refused over a word nobody typed.
    all_subjects_covered = not uncovered_subjects and not foreign
    if coverage < partial_floor and not all_subjects_covered:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=coverage,
            anchors=anchors,
            covered_anchors=covered_anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                f"Only {coverage:.0%} of the question is covered by the documentation "
                f"— too little to answer without guessing."
            ),
        )

    unknown_objects = tuple(t for t in uncovered if names_own_object(t))
    if unknown_objects:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=coverage,
            anchors=anchors,
            covered_anchors=covered_anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                f"The question is about {', '.join(unknown_objects)}, which the "
                f"documentation does not name — that needs a live read, not an article."
            ),
        )

    # A partial answer is one that leaves part of the *subject* unanswered.
    # Keying this on any uncovered term made almost every answer partial, since
    # an ordinary question carries modifiers ("nightly", "low", "2am") that no
    # passage repeats and that nothing is missing without.
    if coverage < coverage_floor or uncovered_subjects:
        return EvidenceVerdict(
            outcome="partial",
            coverage=coverage,
            anchors=anchors,
            covered_anchors=covered_anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                f"Documented for {', '.join(covered_anchors)}; "
                f"not covered: {', '.join(uncovered_subjects)}."
                if uncovered_subjects
                else f"Partial coverage ({coverage:.0%})."
            ),
        )

    return EvidenceVerdict(
        outcome="answer",
        coverage=coverage,
        anchors=anchors,
        covered_anchors=covered_anchors,
        subjects_in_question=subjects,
        reason=f"Documentation covers {coverage:.0%} of the question.",
    )
