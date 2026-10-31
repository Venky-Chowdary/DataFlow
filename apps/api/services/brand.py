"""The product's brand: the name people read, and the names it leaves behind.

Two different things carry the brand and they change under different rules.

* **Presentation** (``PRODUCT_NAME``): text a person reads — messages, file
  names, e-mail, MCP identity. It is always "Datawrap".

* **Artifacts**: objects this product writes into a customer's systems — a
  table, a column, a comment, a session name. Customers already hold these
  under the old "dataflow" / ``_df_`` names, and the next run must keep
  working against them. So:

  - an artifact that already exists under either name is the one in use, and
    both names are recognised as the product's own; nothing is ever renamed;
  - only a NEW artifact may take the new name, and only when the operator
    sets ``DATAWRAP_BRAND_OBJECT_NAMES=1`` (off by default; ``DATAFLOW_``
    spelling also read). Each one created that way writes an audit event
    ``brand.object_name.created``.

``ARTIFACTS`` is the inventory of every artifact name the product writes into
customer systems. ``switchable`` says whether the flag changes it in this
build; the others keep the old name even with the flag on (their run-to-run
identity is threaded through code that does not resolve it per destination
yet). ``services.brand_artifacts`` lists them in a destination (dry run).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Callable

from services.brand_env import getenv_brand_bool

logger = logging.getLogger(__name__)

PRODUCT_NAME = "Datawrap"
LEGACY_PRODUCT_NAME = "DataFlow"
#: Lower-case product name for file names and identifiers people see
#: (downloads, the MCP server name). Not used for objects in customer systems.
PRODUCT_SLUG = "datawrap"

#: ``DATAWRAP_BRAND_OBJECT_NAMES`` (``DATAFLOW_BRAND_OBJECT_NAMES`` also read).
OBJECT_NAMES_FLAG = "BRAND_OBJECT_NAMES"


@dataclass(frozen=True)
class Artifact:
    kind: str
    legacy: str
    new: str
    #: table | column | column_prefix | table_prefix | table_suffix | comment
    #: | session | external
    shape: str
    where: str
    switchable: bool = False


def _a(kind: str, legacy: str, new: str, shape: str, where: str, switchable: bool = False):
    return kind, Artifact(kind, legacy, new, shape, where, switchable)


ARTIFACTS: dict[str, Artifact] = dict([
    # -- switched by the flag in this build ---------------------------------
    _a("session_application_name", "dataflow", "datawrap", "session",
       "PostgreSQL application_name on every connection", True),
    _a("managed_table_comment", "dataflow:managed", "datawrap:managed", "comment",
       "table comment marking a table this product created (PostgreSQL, MySQL)", True),
    _a("cdc_eos_watermark_table", "_df_cdc_eos_watermarks", "_dw_cdc_eos_watermarks", "table",
       "exactly-once CDC watermark table (SQLAlchemy destinations)", True),
    _a("cdc_eos_watermark_table_oracle", "df_cdc_eos_watermarks", "dw_cdc_eos_watermarks", "table",
       "exactly-once CDC watermark table on Oracle", True),
    _a("api_key_prefix", "dfk_", "dwk_", "external",
       "prefix of a newly minted workspace API key (both prefixes authenticate)", True),
    # -- recognised, listed by the dry run, NOT switched yet --------------------
    _a("cdc_lsn_column", "_df_lsn", "_dw_lsn", "column",
       "CDC monotonic-apply column on every CDC destination table"),
    _a("bookkeeping_column", "_df_", "_dw_", "column_prefix",
       "other _df_* bookkeeping columns / metadata keys (quarantine, vectors)"),
    _a("write_ledger_table", "_dataflow_write_ledger", "_datawrap_write_ledger", "table",
       "chunk ledger that makes a retried write skip committed chunks"),
    _a("dest_quarantine_table", "_df_quarantine", "_dw_quarantine", "table_suffix",
       "in-destination quarantine table <table>_df_quarantine (its columns are _df_*)"),
    _a("file_manifest_table", "dataflow_file_manifest", "datawrap_file_manifest", "table",
       "processed-file manifest on the destination (file routes)"),
    _a("vector_sidecar_table", "_df_vector_collections", "_dw_vector_collections", "table",
       "vector fingerprint sidecar (pgvector table / Qdrant collection)"),
    _a("mysql_row_backup_comment", "dataflow:overwrite-row-backup",
       "datawrap:overwrite-row-backup", "comment",
       "comment on a MySQL overwrite row-backup table"),
    _a("cdc_signal_table", "dataflow_signal", "datawrap_signal", "table",
       "CDC incremental-snapshot signal table in the SOURCE database"),
    _a("table_backup_suffix", "__df_bak", "__dw_bak", "table_suffix",
       "pre-run copy kept by overwrite on MySQL/Mongo (<table>__df_bak[_xxxxxx])"),
    _a("copy_staging_prefix", "_df_stg_", "_dw_stg_", "table_prefix",
       "COPY upsert / source-snapshot staging table"),
    _a("scd2_staging_prefix", "_dataflow_stg_", "_datawrap_stg_", "table_prefix",
       "SCD2 / mirror source spool"),
    _a("mirror_staging_prefix", "_df_mirrorkeys_", "_dw_mirrorkeys_", "table_prefix",
       "mirror key-staging table"),
    _a("pg_merge_stage_prefix", "_df_merge_stage_", "_dw_merge_stage_", "table_prefix",
       "PostgreSQL merge staging table"),
    _a("pg_upsert_stage_prefix", "_df_upsert_stage_", "_dw_upsert_stage_", "table_prefix",
       "PostgreSQL upsert staging table"),
    _a("pg_copy_upsert_prefix", "_df_copy_ups_", "_dw_copy_ups_", "table_prefix",
       "PostgreSQL COPY upsert staging table"),
    _a("snowflake_upsert_temp_prefix", "_DF_UPSERT_", "_DW_UPSERT_", "table_prefix",
       "Snowflake upsert temporary table"),
    _a("merge_temp_prefix", "df_mrg_", "dw_mrg_", "table_prefix",
       "MERGE temporary tables (#df_mrg_ / SESSION.DF_MRG_)"),
    _a("pg_enum_type_prefix", "df_enum_", "dw_enum_", "table_prefix",
       "PostgreSQL enum types created for mapped enums"),
    _a("pg_slot_prefix", "df_", "dw_", "external",
       "PostgreSQL replication slots / publications (df_*, df_pub_*) on the source"),
    _a("kafka_group_prefix", "dataflow-kafka-", "datawrap-kafka-", "external",
       "Kafka consumer group id (committed offsets live under it)"),
    _a("object_store_staging", ".__df_staging__", ".__dw_staging__", "external",
       "object-store staging key / multipart prefix (.__df_staging__, .__df_mp__/)"),
    _a("redshift_stage_prefix", "dataflow/redshift-stage", "datawrap/redshift-stage", "external",
       "S3 key prefix for Redshift COPY staging"),
    _a("iceberg_property_prefix", "dataflow.", "datawrap.", "external",
       "Iceberg snapshot summary properties (dataflow.commit-id, dataflow.eos.*)"),
    _a("vector_collection_default", "dataflow_vectors", "datawrap_vectors", "external",
       "default Qdrant / Milvus / Weaviate collection (dataflow_chunks, DataflowChunk)"),
    _a("kafka_default_topic", "dataflow.events", "datawrap.events", "external",
       "Kafka topic written when the destination names none"),
    _a("object_store_default_key", "exports/dataflow_export.json",
       "exports/datawrap_export.json", "external",
       "object-store key written when the destination names none"),
    _a("redis_default_prefix", "dataflow", "datawrap", "external",
       "Redis key prefix when the destination names no table/schema"),
    _a("file_temp_prefix", ".dataflow-", ".datawrap-", "external",
       "temporary file names on SFTP / local destinations and the preflight write probe"),
    _a("saas_idempotency_key_prefix", "dataflow-", "datawrap-", "external",
       "idempotency keys sent to Stripe / Shopify / Airtable"),
    _a("bigquery_default_dataset", "dataflow", "datawrap", "external",
       "BigQuery dataset used when the connector names none"),
    _a("snowflake_default_schema", "DATAFLOW", "DATAWRAP", "external",
       "Snowflake schema used by the COPY route when none is configured"),
])


#: Shapes of ARTIFACTS that are objects in a customer database (tables,
#: collections, staging spools) — the ones an object listing would show.
_OBJECT_SHAPES = frozenset({"table", "table_prefix", "table_suffix"})


def _internal_object_rules() -> tuple[frozenset[str], tuple[str, ...], tuple[str, ...]]:
    names: set[str] = set()
    prefixes: list[str] = []
    suffixes: list[str] = []
    for art in ARTIFACTS.values():
        if art.shape not in _OBJECT_SHAPES:
            continue
        for spelling in (art.legacy, art.new):
            low = spelling.lower()
            if art.shape == "table":
                names.add(low)
            elif art.shape == "table_prefix":
                prefixes.append(low)
            else:
                suffixes.append(low)
    return frozenset(names), tuple(prefixes), tuple(suffixes)


def is_internal_object_name(name: object) -> bool:
    """Is ``name`` a table/collection this product writes (either brand name)?

    Matches the ARTIFACTS inventory: exact internal tables (``dataflow_signal``),
    internal prefixes (``_df_stg_…``) and suffixes (``orders_df_quarantine``,
    ``orders__df_bak_ab12cd``). Case-insensitive and schema-qualifier blind,
    because Oracle/Snowflake fold identifiers upper-case and listings may carry
    ``schema.table``. User-facing listings hide these objects.
    """
    text = "" if name is None else str(name).strip().strip('"`[]')
    if not text:
        return False
    leaf = text.rsplit(".", 1)[-1].strip('"`[]').lower()
    exact, prefixes, suffixes = _internal_object_rules()
    if leaf in exact or leaf.startswith(prefixes):
        return True
    for suffix in suffixes:
        at = leaf.rfind(suffix)
        if at <= 0:
            continue
        tail = leaf[at + len(suffix):]
        # ``<table>__df_bak`` may carry a ``_<token>`` run suffix.
        if not tail or (
            suffix.endswith("_bak") and tail.startswith("_") and tail[1:].isalnum()
        ):
            return True
    return False


def object_names_flag_on() -> bool:
    """``DATAWRAP_BRAND_OBJECT_NAMES=1``: new artifacts take the new name."""
    return getenv_brand_bool(OBJECT_NAMES_FLAG, False)


def artifact(kind: str) -> Artifact:
    return ARTIFACTS[kind]


def known_names(kind: str) -> tuple[str, str]:
    """Both names of an artifact, old first: each one is recognised as ours."""
    art = ARTIFACTS[kind]
    return art.legacy, art.new


def is_own(kind: str, value: object) -> bool:
    """Is ``value`` this artifact under either of its names?"""
    text = "" if value is None else str(value)
    return text in known_names(kind)


def name_for_new(kind: str) -> str:
    """The name a NEW artifact of this kind gets now (flag-dependent)."""
    art = ARTIFACTS[kind]
    return art.new if (art.switchable and object_names_flag_on()) else art.legacy


_RESOLVED: dict[tuple[str, str], str] = {}
_AUDITED: set[tuple[str, str, str]] = set()
_LOCK = threading.Lock()


def reset_resolution_cache() -> None:
    with _LOCK:
        _RESOLVED.clear()
        _AUDITED.clear()


def resolve_existing(kind: str, target: str, exists: Callable[[str], bool]) -> str:
    """The name to use for this artifact on ``target`` (a destination identity).

    An artifact that already exists wins, old name first, so a run never forks
    state into a second object and nothing is renamed. Only when neither
    exists does the flag choose the name the run is about to create. A found
    answer is cached per process; "neither exists" is not (the caller creates
    it next, and the next resolution finds it).
    """
    key = (kind, target)
    with _LOCK:
        hit = _RESOLVED.get(key)
    if hit is not None:
        return hit
    for name in known_names(kind):
        try:
            found = bool(exists(name))
        except Exception:
            # Could not tell whether it exists: never start a new-named object
            # beside one we failed to see. The old name is today's behaviour.
            logger.warning("brand: existence probe for %s on %s failed", name, target, exc_info=True)
            return ARTIFACTS[kind].legacy
        if found:
            with _LOCK:
                _RESOLVED[key] = name
            return name
    chosen = name_for_new(kind)
    if chosen != ARTIFACTS[kind].legacy:
        audit_new_name(kind, chosen, target)
    return chosen


def audit_new_name(kind: str, name: str, target: str) -> None:
    """Audit an artifact about to be created under the new name (once per target)."""
    key = (kind, name, target)
    with _LOCK:
        if key in _AUDITED:
            return
        _AUDITED.add(key)
    art = ARTIFACTS[kind]
    try:
        from services import audit_log

        audit_log.append_audit_event(
            action="brand.object_name.created",
            resource=f"destination:{target}",
            details={
                "kind": kind,
                "name": name,
                "legacy_name": art.legacy,
                "where": art.where,
                "flag": f"DATAWRAP_{OBJECT_NAMES_FLAG}",
            },
            reason_code="brand_object_names_flag",
        )
    except Exception:
        logger.warning("brand: audit of new artifact name %s failed", name, exc_info=True)


def new_marker_comment(table: str) -> str:
    """The ownership comment the table this run is creating gets (audited)."""
    name = name_for_new("managed_table_comment")
    if name != ARTIFACTS["managed_table_comment"].legacy:
        audit_new_name("managed_table_comment", name, table)
    return name


def session_application_name() -> str:
    return name_for_new("session_application_name")
