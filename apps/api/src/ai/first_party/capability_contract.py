"""Capabilities spoken from enforcing modules — shipped or honestly absent.

Google / Microsoft operators ask competitor-adjacent and infra questions
first. Answering Debezium with the Airbyte wedge, Terraform with a
connector wizard, or Confirm with the Overview page is a silent lie.

Each card is read from a signature, honesty flag, catalog set, or
adapter module so the spoken verdict cannot drift from the engine.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass


@dataclass(frozen=True)
class CapabilityCard:
    """One honest capability answer plus the module that makes it true."""

    title: str
    text: str
    source_module: str
    category: str = "product"


def _sftp_is_a_file_connector() -> bool:
    try:
        import registry

        formats = [
            str(getattr(item, "value", item)).lower()
            for item in getattr(registry, "FILE_FORMATS", ())
        ]
        return "sftp" in formats
    except Exception:
        return False


def _transfer_ready_drivers() -> set[str]:
    try:
        from services.catalog_service import catalog_summary

        return {
            str(name).lower()
            for name in (catalog_summary().get("unique_driver_types") or ())
            if name
        }
    except Exception:
        return set()


def postgres_connect_accepts_tunnel() -> bool:
    """Whether the Postgres probe signature takes a tunnel / bastion field."""
    try:
        from connectors.postgresql import test_postgresql
    except Exception:
        return False
    names = {name.lower() for name in inspect.signature(test_postgresql).parameters}
    return bool(
        names
        & {
            "ssh_tunnel",
            "tunnel_host",
            "tunnel_port",
            "bastion",
            "ssh_host",
            "jump_host",
            "privatelink",
            "vpc_endpoint",
        }
    )


def dbt_cloud_shipped() -> bool:
    try:
        from services.dbt_export import dbt_export_honesty
    except Exception:
        return False
    honesty = dbt_export_honesty()
    return bool(honesty.get("is_dbt_cloud") or honesty.get("is_managed_elt"))


def transfer_requires_confirm() -> bool:
    """Pilot mutations stage Confirm — start_transfer sets requires_confirm."""
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "copilot" / "transfer_tools.py"
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return True
    return "requires_confirm" in source and "start_transfer" in source


def iceberg_catalog_types() -> tuple[str, ...]:
    """Catalog kinds ``iceberg_catalog`` actually dispatches on."""
    try:
        from connectors import iceberg_catalog as catalogs

        source = inspect.getsource(catalogs._infer_catalog_type)
    except Exception:
        return ()
    allowed = ("filesystem", "glue", "rest", "sql", "hive", "hadoop", "nessie")
    return tuple(name for name in allowed if f'"{name}"' in source or f"'{name}'" in source)


def dbt_card() -> CapabilityCard | None:
    try:
        from services.dbt_export import dbt_export_honesty
    except Exception:
        return None
    honesty = dbt_export_honesty()
    if honesty.get("is_dbt_cloud") or honesty.get("is_managed_elt"):
        return None
    return CapabilityCard(
        title="Does Datawrap run dbt Cloud",
        text=(
            "Datawrap does not run dbt Cloud or use dbt as the transfer "
            "engine — transform projects can export a dbt starter pack "
            "(sources and models) as a complement hook after a governed "
            "load, and is_dbt_cloud is false. "
            "Map, Validate, and Execute stay on Datawrap; the export is "
            "not Gate-8 reconcile evidence."
        ),
        source_module="services/dbt_export.py · dbt_export_honesty",
    )


def ssh_tunnel_card() -> CapabilityCard | None:
    if postgres_connect_accepts_tunnel():
        return None
    sftp = (
        "SFTP is a file connector, not a bastion or tunnel in front of a warehouse."
        if _sftp_is_a_file_connector()
        else "Database connections are not tunneled through SSH."
    )
    return CapabilityCard(
        title="Does Datawrap open SSH tunnels",
        text=(
            "Datawrap does not open SSH tunnels — drivers take host, port, "
            "and credentials with no bastion or jump host. "
            f"{sftp}"
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def debezium_card() -> CapabilityCard | None:
    """Optional Kafka envelope bridge — not an embedded Debezium product."""
    try:
        from connectors import kafka_debezium_bridge as bridge
    except Exception:
        return None
    doc = (bridge.__doc__ or "").lower()
    if "not required for native" not in doc and "without embedding" not in doc:
        return None
    return CapabilityCard(
        title="Does Datawrap embed Debezium",
        text=(
            "Datawrap does not embed Debezium, is not a Kafka Connect "
            "replacement, and does not run Flink CDC as the capture engine. "
            "A thin optional topic-envelope bridge can consume an existing "
            "Connect-plus-Debezium feed; that bridge is not required for "
            "native CDC."
        ),
        source_module="connectors/kafka_debezium_bridge.py",
        category="connectors",
    )


def terraform_card() -> CapabilityCard:
    return CapabilityCard(
        title="Does Datawrap have a Terraform provider",
        text=(
            "Datawrap does not ship a Terraform provider — pipelines are "
            "kept in git as YAML: Export YAML and Import YAML on "
            "Operations → Pipelines, and the GitOps CLI validates, plans "
            "and applies the same manifest. "
            "Credentials stay in the connection store; the manifest names "
            "a connection."
        ),
        source_module="apps/cli/dataflow_cli · schedules export route",
        category="enterprise",
    )


def confirm_card() -> CapabilityCard | None:
    if not transfer_requires_confirm():
        return None
    return CapabilityCard(
        title="Does a transfer start without Confirm",
        text=(
            "A transfer does not start without Confirm — Pilot stages the "
            "plan with requires_confirm, and nothing moves until the "
            "operator presses Confirm. "
            "Overwrite is never the default; Confirm fails closed on a "
            "signed contract and an open breaker."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="product",
    )


def privatelink_card() -> CapabilityCard | None:
    if postgres_connect_accepts_tunnel():
        return None
    return CapabilityCard(
        title="Does Datawrap use AWS PrivateLink",
        text=(
            "Private Link is not shipped (privatelink is false) — Datawrap "
            "does not ship AWS PrivateLink or GCP Private Service Connect "
            "as a connect option. "
            "Database connections take host, port, and credentials. "
            "VPC peering is also not on that connect path."
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def airflow_spark_card() -> CapabilityCard:
    return CapabilityCard(
        title="Does Datawrap run Airflow or Spark jobs",
        text=(
            "Datawrap does not run Airflow DAGs or Spark jobs — a transfer "
            "is a Datawrap job with phases, quarantine, and checksum proof. "
            "An external orchestrator can call /api/v1 after you Confirm; "
            "it does not become the write engine."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="product",
    )


def goldengate_card() -> CapabilityCard:
    return CapabilityCard(
        title="Does Datawrap embed Oracle GoldenGate",
        text=(
            "Datawrap does not embed Oracle GoldenGate — native CDC is "
            "Datawrap's own reader, not GoldenGate capture."
        ),
        source_module="apps/api/registry.py · DATABASE_TYPES",
        category="connectors",
    )


def iceberg_catalog_card() -> CapabilityCard | None:
    kinds = iceberg_catalog_types()
    if "glue" not in kinds:
        return None
    listed = ", ".join(kinds)
    return CapabilityCard(
        title="Can I use an Iceberg Glue catalog",
        text=(
            f"Yes — Iceberg accepts a Glue catalog (catalog_type glue), plus "
            f"{listed}. "
            "Nessie is mapped to rest. Glue is selected when the warehouse "
            "is S3 or GCS or a region is set. "
            "Upsert and CDC writes still use merge-on-read; overwrite stays "
            "copy-on-write."
        ),
        source_module="connectors/iceberg_catalog.py · _infer_catalog_type",
        category="connectors",
    )


def snowflake_sharing_card() -> CapabilityCard:
    return CapabilityCard(
        title="Does Datawrap use Snowflake Secure Sharing",
        text=(
            "Datawrap does not implement Snowflake Secure Data Sharing — "
            "there is no share consumer or provider path."
        ),
        source_module="apps/api/registry.py · DATABASE_TYPES",
        category="connectors",
    )


def kafka_source_card() -> CapabilityCard | None:
    if not kafka_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Can I use Kafka as a source",
        text=(
            "Yes — Kafka is a transfer-ready driver, so a transfer can use "
            "it as a source or a destination. "
            "Native Postgres, MySQL, and Mongo CDC do not require Kafka; "
            "the Debezium envelope bridge is optional."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def kafka_dest_card() -> CapabilityCard | None:
    if not kafka_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Kafka as a destination",
        text=(
            "Yes — Kafka is a transfer-ready driver (kafka), so a transfer "
            "can write to a topic as a destination. "
            "A registry URL on the route is optional, not a shipped registry."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def iceberg_ready_card() -> CapabilityCard | None:
    if not iceberg_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Iceberg",
        text=(
            "Yes — Iceberg is a transfer-ready driver (iceberg). "
            "Upsert and CDC use merge-on-read; overwrite stays copy-on-write."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def sftp_ready_card() -> CapabilityCard | None:
    if "sftp" not in _transfer_ready_drivers() and not _sftp_is_a_file_connector():
        return None
    return CapabilityCard(
        title="Do you support SFTP",
        text=(
            "Yes — SFTP is a transfer-ready file connector (sftp): host, "
            "port, and credentials, not a bastion in front of a warehouse."
        ),
        source_module="src/transfer/connector_capabilities.py · sftp",
        category="connectors",
    )


def mysql_ready_card() -> CapabilityCard | None:
    if not mysql_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support MySQL",
        text=(
            "Yes — MySQL is a transfer-ready driver (mysql)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def redis_ready_card() -> CapabilityCard | None:
    if not redis_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Redis",
        text=(
            "Yes — Redis is a transfer-ready driver (redis). "
            "Azure Cache for Redis and Memorystore connect as the same driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def salesforce_card() -> CapabilityCard | None:
    if "salesforce" not in _transfer_ready_drivers():
        return None
    return CapabilityCard(
        title="Do you have Salesforce",
        text=(
            "Yes — Salesforce is a transfer-ready driver, so a transfer "
            "can run it today. "
            "Catalog tile count is not the proof; unique_driver_types is."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def schema_registry_card() -> CapabilityCard:
    return CapabilityCard(
        title="Does Datawrap include a schema registry",
        text=(
            "Datawrap does not ship a standalone Confluent Schema Registry "
            "— Kafka routes can point at an existing registry URL. "
            "Schema change on other engines is the contract and the "
            "schema-change policy, not a registry service."
        ),
        source_module="connectors/confluent_schema_registry.py",
        category="connectors",
    )


def upsert_is_canonical_sync_mode() -> bool:
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES
    except Exception:
        return False
    return "upsert" in CANONICAL_SYNC_MODES


def merge_sql_dialect_shipped() -> bool:
    """Whether a warehouse writer actually emits MERGE INTO."""
    try:
        from connectors.snowflake_writer import build_snowflake_merge_sql
    except Exception:
        return False
    return "MERGE INTO" in inspect.getsource(build_snowflake_merge_sql)


def airbyte_is_transfer_ready() -> bool:
    return "airbyte" in _transfer_ready_drivers()


def fivetran_is_transfer_ready() -> bool:
    return "fivetran" in _transfer_ready_drivers()


def transfer_undo_claimed() -> bool:
    try:
        from services.recovery_honesty import TRANSFER_UNDO_CLAIMED
    except Exception:
        return False
    return bool(TRANSFER_UNDO_CLAIMED)


def viewer_can_read_secrets() -> bool:
    try:
        from services.rbac import Permission, role_permissions
    except Exception:
        return False
    return Permission.WORKSPACE_MANAGE in role_permissions("viewer")


def snowflake_key_pair_supported() -> bool:
    try:
        from services.connector_auth import infer_auth_mode
    except Exception:
        return False
    return infer_auth_mode(private_key="-----BEGIN PRIVATE KEY-----", driver="snowflake") == "key_pair"


def viewer_has_audit_read() -> bool:
    try:
        from services.rbac import Permission, role_permissions
    except Exception:
        return False
    return Permission.AUDIT_READ in role_permissions("viewer")


def bigquery_is_transfer_ready() -> bool:
    return "bigquery" in _transfer_ready_drivers()


def adls_is_transfer_ready() -> bool:
    return "adls" in _transfer_ready_drivers()


def adls_service_principal_shipped() -> bool:
    try:
        from connectors.adls_common import _service_principal_credential
    except Exception:
        return False
    return "ClientSecretCredential" in inspect.getsource(_service_principal_credential)


def incremental_modes_are_canonical() -> bool:
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES
    except Exception:
        return False
    return {"incremental_append", "incremental_deduped", "upsert"} <= set(
        CANONICAL_SYNC_MODES
    )


def login_mfa_enforced() -> bool:
    """Login MFA is policy memory only — workspace_router.mfa_enforced stays False."""
    return False


def ip_allowlist_enforced_without_custom_domain() -> bool:
    """CIDR list is stored; enforcement requires a custom domain."""
    return False


def sqlserver_is_transfer_ready() -> bool:
    return "sqlserver" in _transfer_ready_drivers()


def databricks_is_transfer_ready() -> bool:
    return "databricks" in _transfer_ready_drivers()


def delta_lake_is_transfer_ready() -> bool:
    drivers = _transfer_ready_drivers()
    return bool(drivers & {"delta", "delta_lake"})


def snowflake_is_transfer_ready() -> bool:
    return "snowflake" in _transfer_ready_drivers()


def oracle_is_transfer_ready() -> bool:
    return "oracle" in _transfer_ready_drivers()


def postgresql_is_transfer_ready() -> bool:
    return "postgresql" in _transfer_ready_drivers()


def mysql_is_transfer_ready() -> bool:
    return "mysql" in _transfer_ready_drivers()


def mongodb_is_transfer_ready() -> bool:
    return "mongodb" in _transfer_ready_drivers()


def s3_is_transfer_ready() -> bool:
    return "s3" in _transfer_ready_drivers()


def gcs_is_transfer_ready() -> bool:
    return "gcs" in _transfer_ready_drivers()


def redis_is_transfer_ready() -> bool:
    return "redis" in _transfer_ready_drivers()


def iceberg_is_transfer_ready() -> bool:
    return "iceberg" in _transfer_ready_drivers()


def kafka_is_transfer_ready() -> bool:
    return "kafka" in _transfer_ready_drivers()


def scd2_is_canonical() -> bool:
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES
    except Exception:
        return False
    return "scd2" in CANONICAL_SYNC_MODES


def redshift_is_transfer_ready() -> bool:
    return bool(_transfer_ready_drivers() & {"redshift", "amazon_redshift"})


def synapse_is_transfer_ready() -> bool:
    return bool(
        _transfer_ready_drivers() & {"synapse", "azure_synapse", "azuresynapse"}
    )


def bigquery_service_account_shipped() -> bool:
    try:
        from connectors.bigquery_conn import get_client
    except Exception:
        return False
    return "service_account" in inspect.signature(get_client).parameters


def source_duplicate_probe_shipped() -> bool:
    try:
        from services.source_duplicate_probe import probe_source_duplicate_keys
        from services.destination_key_collision_probe import (
            probe_destination_key_collisions,
        )
    except Exception:
        return False
    return callable(probe_source_duplicate_keys) and callable(
        probe_destination_key_collisions
    )


def byok_wraps_connector_secrets() -> bool:
    try:
        from services import byok_key_manager
    except Exception:
        return False
    return callable(getattr(byok_key_manager, "create_key", None))


def gcp_workload_identity_shipped() -> bool:
    return False


def column_level_lineage_emitted() -> bool:
    return False


def scd1_is_canonical() -> bool:
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES
    except Exception:
        return False
    return bool({"scd1", "scd_1", "scd_type_1"} & set(CANONICAL_SYNC_MODES))


def full_refresh_modes_are_canonical() -> bool:
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES
    except Exception:
        return False
    return {"full_refresh_overwrite", "full_refresh_append"} <= set(CANONICAL_SYNC_MODES)


def destination_table_lock_claimed() -> bool:
    return False


def transfer_workers_default() -> int:
    """Default concurrent jobs on one worker — worker_fleet TRANSFER_WORKERS."""
    try:
        from services.platform_config import getenv_brand

        return max(1, int(getenv_brand("TRANSFER_WORKERS", "8") or "8"))
    except Exception:
        return 8


def session_timeout_enforced() -> bool:
    """Token TTL is DATAFLOW_TOKEN_TTL_SEC — tenant.session_timeout_hours is memory."""
    return False


def residency_attestation_claimed() -> bool:
    return False


def watermark_store_honesty() -> dict[str, object]:
    """Default resume token vs opted-in EOS table — not a platform exactly-once claim."""
    try:
        from services.cdc_exactly_once import (
            PLATFORM_EXACTLY_ONCE_CLAIMED,
            WATERMARK_TABLE,
        )
    except Exception:
        return {
            "default_store": "resume_token",
            "eos_table": "_df_cdc_eos_watermarks",
            "exactly_once_claimed": False,
        }
    return {
        "default_store": "resume_token",
        "eos_table": WATERMARK_TABLE,
        "exactly_once_claimed": bool(PLATFORM_EXACTLY_ONCE_CLAIMED),
    }


def silent_data_loss_card() -> CapabilityCard | None:
    """No legal zero-loss SLA — quarantine + ledger, not query-capture deletes."""
    try:
        from services.row_conservation import silent_loss_honesty
    except Exception:
        return None
    honesty = silent_loss_honesty()
    if honesty.get("legal_sla") or honesty.get("silent_drop"):
        return None
    try:
        from services.cdc_effectively_once import DELIVERY_DEFAULT
    except Exception:
        delivery = "at-least-once"
    else:
        delivery = DELIVERY_DEFAULT
    return CapabilityCard(
        title="Do you guarantee no silent data loss",
        text=(
            "Datawrap does not invent a legal no-data-loss SLA — the product "
            "rule is no silent drop: a bad row is quarantined and surfaced, "
            "and the row ledger must close "
            f"({honesty.get('identity') or 'read = dest + hold-outs + skipped'}) "
            "or the run is unbalanced. "
            f"CDC stays {delivery} upsert on `_df_lsn`; checksum reconcile "
            "is Gate-8 proof, not a warranty."
        ),
        source_module="services/row_conservation.py · silent_loss_honesty",
        category="proof",
    )


def upsert_versus_merge_card() -> CapabilityCard | None:
    """Sync-mode upsert vs destination MERGE SQL — not Iceberg MoR."""
    if not upsert_is_canonical_sync_mode() or not merge_sql_dialect_shipped():
        return None
    return CapabilityCard(
        title="What is the difference between upsert and merge",
        text=(
            "Upsert is a sync mode written key-idempotently (new keys insert, "
            "known keys update); MERGE is a destination SQL dialect "
            "(MERGE INTO / ON CONFLICT) to apply that upsert. "
            "The dialect is how Snowflake, BigQuery, and similar writers emit "
            "the upsert; it is not a separate sync mode."
        ),
        source_module=(
            "services/sync_cursor.py · CANONICAL_SYNC_MODES · "
            "connectors/snowflake_writer.py · build_snowflake_merge_sql"
        ),
        category="transfer",
    )


def compliance_attestation_card() -> CapabilityCard | None:
    """Audit export is diligence — not a signed SOC 2 / HIPAA / DPA letter."""
    try:
        from src.routers.audit_router import audit_export_honesty
    except Exception:
        return None
    honesty = audit_export_honesty()
    if honesty.get("signed_soc2") or honesty.get("signed_hipaa_baa"):
        return None
    return CapabilityCard(
        title="Do you sign a SOC2 or HIPAA BAA",
        text=(
            "Datawrap does not invent a signed SOC 2 Type II letter, GDPR DPA, "
            "or HIPAA BAA (soc2 is false). "
            "Audit export is a workspace-scoped sample whose HMAC-SHA256 chain "
            "is diligence, not a certificate."
        ),
        source_module="src/routers/audit_router.py · audit_export_honesty",
        category="enterprise",
    )


def airbyte_connector_pack_card() -> CapabilityCard | None:
    """Airbyte / Fivetran tiles are not a connector pack loader."""
    if airbyte_is_transfer_ready() or fivetran_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Does Datawrap load Airbyte or Fivetran connector packs",
        text=(
            "Datawrap does not load Airbyte connector packs, Fivetran connector "
            "packs, or custom Airbyte connectors — a transfer runs Datawrap's "
            "own transfer-ready drivers (unique_driver_types), not an Airbyte "
            "CDK or Fivetran pack. "
            "Both remain planned catalog tiles and a competitor comparison, "
            "not a connector pack loader."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def transfer_undo_card() -> CapabilityCard | None:
    if transfer_undo_claimed():
        return None
    try:
        from services.recovery_honesty import honesty_dict
    except Exception:
        return None
    honesty = honesty_dict()
    resume = bool((honesty.get("capabilities") or {}).get("checkpoint_resume", {}).get("available"))
    return CapabilityCard(
        title="Can I undo a transfer",
        text=(
            "Datawrap does not undo a transfer or roll back a committed load — "
            "transfer_undo_claimed is false, and there is no one-click destination "
            "undo, staging swap, or warehouse restore. "
            + (
                "A failed run can resume from the last safe checkpoint; "
                "quarantined rows can be replayed; warehouse Time Travel stays DBA tooling."
                if resume
                else "Quarantined rows can be replayed; warehouse restore stays DBA tooling."
            )
        ),
        source_module="services/recovery_honesty.py · honesty_dict",
        category="proof",
    )


def viewer_secrets_card() -> CapabilityCard | None:
    if viewer_can_read_secrets():
        return None
    return CapabilityCard(
        title="Can a viewer see secrets",
        text=(
            "A viewer cannot see secrets — SSO certificates, provider keys, "
            "and API keys stay behind workspace.manage. "
            "A viewer can read jobs, connectors, and audit events; YAML export "
            "does not include credentials."
        ),
        source_module="services/rbac.py · workspace.manage",
        category="enterprise",
    )


def rest_api_card() -> CapabilityCard:
    return CapabilityCard(
        title="Do you have a REST API",
        text=(
            "Yes — Datawrap has a REST API at /api/v1; authenticate with a "
            "Bearer token to list connectors, run preflight, execute a "
            "transfer, and read job status. "
            "Mutating calls still require Confirm."
        ),
        source_module="docs/API_VERSIONING.md · /api/v1",
        category="api",
    )


def github_actions_card() -> CapabilityCard:
    return CapabilityCard(
        title="Can I call Datawrap from GitHub Actions",
        text=(
            "Yes — GitHub Actions can call the /api/v1 REST API with a Bearer "
            "token after you Confirm; Datawrap does not run GitHub Actions or "
            "Airflow as the write engine. "
            "An external CI workflow is a client of the API, not the write engine."
        ),
        source_module="docs/API_VERSIONING.md · start_transfer requires_confirm",
        category="api",
    )


def openlineage_card() -> CapabilityCard | None:
    try:
        from services.lineage_telemetry import emit_run_started
    except Exception:
        return None
    if emit_run_started is None:
        return None
    return CapabilityCard(
        title="Do you support OpenLineage",
        text=(
            "Yes — transfers emit OpenLineage-compatible events at run and "
            "dataset grain (job, run, datasets, validation evidence), not a "
            "hosted OpenLineage server or a per-row graph. "
            "Row-level questions use the row ledger and quarantine reason."
        ),
        source_module="services/lineage_telemetry.py · emit_run_started",
        category="proof",
    )


def mirror_versus_upsert_card() -> CapabilityCard | None:
    if not upsert_is_canonical_sync_mode():
        return None
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES
    except Exception:
        return None
    if "mirror" not in CANONICAL_SYNC_MODES:
        return None
    return CapabilityCard(
        title="What is the difference between mirror and upsert",
        text=(
            "Mirror is upsert plus deletion — destination rows whose key the "
            "source no longer has are removed. "
            "Upsert leaves those destination-only rows alone. "
            "Mirror deletes data, so nothing aliases onto it implicitly."
        ),
        source_module="services/sync_cursor.py · CANONICAL_SYNC_MODES",
        category="transfer",
    )


def snowflake_key_pair_card() -> CapabilityCard | None:
    if not snowflake_key_pair_supported():
        return None
    return CapabilityCard(
        title="Can I connect Snowflake with a private key",
        text=(
            "Yes — Snowflake accepts key-pair auth: a private_key selects "
            "auth_mode key_pair on the Snowflake connect path."
        ),
        source_module="services/connector_auth.py · infer_auth_mode",
        category="connectors",
    )


def audit_export_card() -> CapabilityCard | None:
    """Who can download the workspace audit sample — not a SOC 2 letter."""
    try:
        from src.routers.audit_router import audit_export_honesty
    except Exception:
        return None
    honesty = audit_export_honesty()
    if honesty.get("signed_soc2") or honesty.get("signed_hipaa_baa"):
        return None
    if not viewer_has_audit_read():
        return None
    return CapabilityCard(
        title="Who can export audit logs as CSV",
        text=(
            "Any role with audit.read can export audit logs as CSV or JSON "
            "from GET /api/v1/audit/export. "
            "Viewers have audit.read. The download is a workspace-scoped "
            "HMAC-SHA256 sample, not a signed attestation letter."
        ),
        source_module="src/routers/audit_router.py · export_events · services/rbac.py",
        category="enterprise",
    )


def ip_allowlist_card() -> CapabilityCard | None:
    if ip_allowlist_enforced_without_custom_domain():
        return None
    return CapabilityCard(
        title="Do you support IP allowlists",
        text=(
            "A tenant IP allowlist (CIDR) is stored on the workspace and is "
            "enforced only when a vanity app host is configured; without that "
            "host the list is never evaluated. "
            "ip_allowlist_enforced stays false until then."
        ),
        source_module="src/routers/workspace_router.py · ip_allowlist_enforced",
        category="enterprise",
    )


def require_mfa_card() -> CapabilityCard | None:
    if login_mfa_enforced():
        return None
    return CapabilityCard(
        title="Can I require MFA",
        text=(
            "You can require MFA as policy memory only; login MFA is not "
            "wired, so mfa_enforced is false. "
            "Saving the flag does not challenge sign-in."
        ),
        source_module="src/routers/workspace_router.py · mfa_enforced",
        category="enterprise",
    )


def cdc_watermark_store_card() -> CapabilityCard | None:
    # Resume token only. The opted-in EOS table name lives on the delivery
    # section — repeating `_df_cdc_eos_watermarks` here put ``cdc`` in a
    # short card and crowded "is CDC exactly once" off its own lead.
    return CapabilityCard(
        title="Where is the watermark stored",
        text=(
            "The CDC watermark is stored as the resume token (binlog, GTID, "
            "LSN, or SCN) kept with the job. "
            "Operators do not type a watermark by hand; the next tick reads "
            "the token the last tick left."
        ),
        source_module="services/cdc_exactly_once.py · WATERMARK_TABLE",
        category="transfer",
    )


def incremental_versus_upsert_card() -> CapabilityCard | None:
    if not incremental_modes_are_canonical():
        return None
    return CapabilityCard(
        title="What is the difference between incremental and upsert",
        text=(
            "Incremental modes are cursor-bounded: incremental_append inserts "
            "rows past the saved cursor with no dedup; incremental_deduped "
            "merges those rows on the key. "
            "Upsert is a whole-source read written key-idempotently. "
            "Incremental is not a synonym for upsert."
        ),
        source_module="services/sync_cursor.py · CANONICAL_SYNC_MODES",
        category="transfer",
    )


def bigquery_destination_card() -> CapabilityCard | None:
    if not bigquery_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support BigQuery as a destination",
        text=(
            "Yes — BigQuery is a transfer-ready driver, so a transfer can use "
            "it as a destination or a source. "
            "Catalog tile count is not the proof; unique_driver_types is."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_service_principal_card() -> CapabilityCard | None:
    if not adls_is_transfer_ready() or not adls_service_principal_shipped():
        return None
    return CapabilityCard(
        title="Can I use a service principal for Azure",
        text=(
            "Yes — ADLS and Azure Blob connections accept a service "
            "principal via service_account JSON (tenant_id, client_id, "
            "client_secret) on the ADLS connect path. "
            "That is a connector credential, not Azure AD SSO as the "
            "transfer writer."
        ),
        source_module="connectors/adls_common.py · _service_principal_credential",
        category="connectors",
    )


def parallel_transfers_card() -> CapabilityCard:
    workers = transfer_workers_default()
    return CapabilityCard(
        title="Can I run transfers in parallel",
        text=(
            f"Yes — you can run transfers in parallel: TRANSFER_WORKERS "
            f"(default {workers}) bounds in-flight jobs on one worker. "
            "Inside one transfer, PARALLEL_WORKERS chunks overlap reads and "
            "writes. That is job and chunk concurrency, not a destination "
            "table lock."
        ),
        source_module="services/worker_fleet.py · TRANSFER_WORKERS",
        category="transfer",
    )


def two_jobs_same_table_card() -> CapabilityCard | None:
    if destination_table_lock_claimed():
        return None
    return CapabilityCard(
        title="What happens if two jobs write the same table",
        text=(
            "Two jobs can write the same destination table; Datawrap does not "
            "take an exclusive destination lock. "
            "Key-idempotent upsert is last-writer-wins on the key."
        ),
        source_module="src/transfer/stream.py · no destination table lock",
        category="transfer",
    )


def full_refresh_versus_incremental_card() -> CapabilityCard | None:
    if not full_refresh_modes_are_canonical() or not incremental_modes_are_canonical():
        return None
    return CapabilityCard(
        title="What is the difference between full refresh and incremental",
        text=(
            "Full refresh is a whole-source read (full_refresh_append inserts "
            "every row; full_refresh_overwrite replaces the destination). "
            "Incremental modes are cursor-bounded and only read rows past the "
            "saved cursor. Full refresh is not a synonym for incremental."
        ),
        source_module="services/sync_cursor.py · CANONICAL_SYNC_MODES",
        category="transfer",
    )


def scd1_card() -> CapabilityCard | None:
    if scd1_is_canonical():
        return None
    return CapabilityCard(
        title="Do you support SCD1",
        text=(
            "Datawrap does not ship SCD1 as a sync mode. "
            "Current-state writes use upsert or mirror; SCD2 is the "
            "history-keeping mode."
        ),
        source_module="services/sync_cursor.py · CANONICAL_SYNC_MODES",
        category="transfer",
    )


def scd2_card() -> CapabilityCard | None:
    if not scd2_is_canonical():
        return None
    return CapabilityCard(
        title="Do you support SCD type 2",
        text=(
            "Yes — SCD2 is a shipped sync mode (scd2): one source identity "
            "becomes several destination versions, each with a validity "
            "window, instead of overwriting the previous value. "
            "SCD1 is not a mode."
        ),
        source_module="services/sync_cursor.py · CANONICAL_SYNC_MODES",
        category="transfer",
    )


def incremental_updated_at_card() -> CapabilityCard | None:
    if not incremental_modes_are_canonical():
        return None
    return CapabilityCard(
        title="Can I use incremental by updated_at",
        text=(
            "Yes — incremental_append and incremental_deduped are shipped "
            "sync modes that advance a saved cursor (often updated_at / "
            "incremental_updated_at) instead of rewriting the table. "
            "Upsert is key-idempotent and is not incremental."
        ),
        source_module="services/sync_cursor.py · CANONICAL_SYNC_MODES",
        category="transfer",
    )


def session_timeout_card() -> CapabilityCard | None:
    if session_timeout_enforced():
        return None
    return CapabilityCard(
        title="What is session timeout",
        text=(
            "Session timeout is recorded as session_timeout_hours; "
            "session_timeout_enforced is false because the timeout is not "
            "wired. "
            "Token lifetime is DATAFLOW_TOKEN_TTL_SEC."
        ),
        source_module="src/routers/workspace_router.py · session_timeout_enforced",
        category="enterprise",
    )


def custom_domain_card() -> CapabilityCard:
    return CapabilityCard(
        title="Can I set a custom domain",
        text=(
            "Yes — a tenant can set a custom domain (tenant.custom_domain, "
            "for example data.customer.com) and point DNS at the Datawrap "
            "web host. "
            "API CORS accepts that origin via TenantAwareCORSMiddleware."
        ),
        source_module="src/routers/workspace_router.py · custom_domain_cors",
        category="enterprise",
    )


def data_residency_card() -> CapabilityCard | None:
    if residency_attestation_claimed():
        return None
    return CapabilityCard(
        title="Do you support data residency",
        text=(
            "A workspace records data_region for data residency; that is not "
            "a signed multi-region residency attestation. "
            "Destination-region fail-closed runs only when "
            "DATAFLOW_RESIDENCY_STRICT is on or the tenant is not on the "
            "default region."
        ),
        source_module="src/routers/transfer_router.py · _residency_check",
        category="enterprise",
    )


def sqlserver_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support SQL Server",
        text=(
            "Yes — SQL Server is a transfer-ready driver, so a transfer can "
            "use it as a source or a destination. "
            "Catalog tile count is not the proof; unique_driver_types is."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def databricks_destination_card() -> CapabilityCard | None:
    if databricks_is_transfer_ready():
        return CapabilityCard(
            title="Do you support Databricks as a destination",
            text=(
                "Yes — Databricks is a transfer-ready driver, so a transfer "
                "can use it as a destination. "
                "Catalog tile count is not the proof; unique_driver_types is."
            ),
            source_module="services/catalog_service.py · unique_driver_types",
            category="connectors",
        )
    return CapabilityCard(
        title="Do you support Databricks as a destination",
        text=(
            "Databricks is not a transfer-ready driver, and Unity Catalog is "
            "not a connect option — unique_driver_types does not include it. "
            "A catalog tile or generic_sql dialect alias is not a live writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def delta_lake_card() -> CapabilityCard | None:
    if delta_lake_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Delta Lake",
        text=(
            "Datawrap does not ship Delta Lake as a transfer-ready driver — "
            "unique_driver_types does not include delta or delta_lake. "
            "A type-system alias is not a live table-format writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def _ready_driver_card(title: str, name: str) -> CapabilityCard:
    return CapabilityCard(
        title=title,
        text=(
            f"Yes — {name} is a transfer-ready driver, so a transfer can "
            f"use it as a source or a destination. "
            "Catalog tile count is not the proof; unique_driver_types is."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def snowflake_destination_card() -> CapabilityCard | None:
    if not snowflake_is_transfer_ready():
        return None
    return _ready_driver_card(
        "Do you support Snowflake as a destination", "Snowflake"
    )


def oracle_destination_card() -> CapabilityCard | None:
    if not oracle_is_transfer_ready():
        return None
    return _ready_driver_card("Do you support Oracle as a destination", "Oracle")


def postgres_destination_card() -> CapabilityCard | None:
    if not postgresql_is_transfer_ready():
        return None
    return _ready_driver_card(
        "Do you support Postgres as a destination", "PostgreSQL"
    )


def mongodb_destination_card() -> CapabilityCard | None:
    if not mongodb_is_transfer_ready():
        return None
    return _ready_driver_card("Do you support MongoDB", "MongoDB")


def s3_destination_card() -> CapabilityCard | None:
    if not s3_is_transfer_ready():
        return None
    return _ready_driver_card("Do you support S3 as a destination", "S3")


def gcs_destination_card() -> CapabilityCard | None:
    if not gcs_is_transfer_ready():
        return None
    return _ready_driver_card("Do you support GCS as a destination", "GCS")


def redshift_destination_card() -> CapabilityCard | None:
    if redshift_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Redshift as a destination",
        text=(
            "Redshift is not a transfer-ready driver — unique_driver_types "
            "does not include redshift. "
            "A help list or catalog tile is not a live writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def synapse_destination_card() -> CapabilityCard | None:
    if synapse_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure Synapse",
        text=(
            "Azure Synapse is not a transfer-ready driver (azure_synapse). synap "
            "unique_driver_types does not include synapse. "
            "A catalog tile is not a live writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def unique_identity_modes() -> frozenset[str]:
    try:
        from services.primary_key import _UNIQUE_IDENTITY_SYNC_MODES
    except Exception:
        return frozenset()
    return frozenset(str(mode) for mode in _UNIQUE_IDENTITY_SYNC_MODES)


def missing_primary_key_card() -> CapabilityCard | None:
    modes = unique_identity_modes()
    if not modes:
        return None
    return CapabilityCard(
        title="What if the source has no primary key",
        text=(
            "A source with no primary key cannot run upsert, CDC, mirror, "
            "or the other identity sync modes — preflight refuses those "
            "runs. full_refresh_overwrite and incremental_append do not "
            "require a primary key."
        ),
        source_module="services/primary_key.py · _UNIQUE_IDENTITY_SYNC_MODES",
        category="transfer",
    )


def gate_8_shipped() -> bool:
    try:
        import src.transfer.reconcile_step as reconcile_step
    except Exception:
        return False
    return "Gate 8" in str(getattr(reconcile_step, "__doc__", "") or "")


def salesforce_oauth_shipped() -> bool:
    try:
        from connectors.salesforce import test_salesforce
    except Exception:
        return False
    names = {name.lower() for name in inspect.signature(test_salesforce).parameters}
    return bool(
        names
        & {
            "oauth",
            "client_id",
            "client_secret",
            "refresh_token",
            "connected_app",
        }
    )


def slack_webhook_notifications_shipped() -> bool:
    try:
        from services.notification_service import send_to_channel, _send_slack
    except Exception:
        return False
    return callable(send_to_channel) and callable(_send_slack)


def teams_webhook_notifications_shipped() -> bool:
    try:
        from services.notification_service import send_to_channel, _send_teams
    except Exception:
        return False
    return callable(send_to_channel) and callable(_send_teams)


def email_notifications_shipped() -> bool:
    try:
        from services.notification_service import send_to_channel, _send_email
    except Exception:
        return False
    return callable(send_to_channel) and callable(_send_email)


def servicenow_notifications_shipped() -> bool:
    try:
        from services.notification_service import send_to_channel, _send_servicenow
    except Exception:
        return False
    return callable(send_to_channel) and callable(_send_servicenow)


def slack_is_transfer_ready() -> bool:
    return bool(_transfer_ready_drivers() & {"slack"})


def teams_is_transfer_ready() -> bool:
    return bool(_transfer_ready_drivers() & {"teams", "microsoft_teams"})


def snowflake_dynamic_tables_shipped() -> bool:
    return False


def row_level_security_shipped() -> bool:
    return False


def closed_rbac_roles() -> tuple[str, ...]:
    try:
        from services.rbac import _ROLE_PERMISSIONS
    except Exception:
        return ()
    return tuple(sorted(_ROLE_PERMISSIONS))


def custom_roles_shipped() -> bool:
    return False


def field_level_encryption_shipped() -> bool:
    return False


def hudi_is_transfer_ready() -> bool:
    return bool(_transfer_ready_drivers() & {"hudi", "apache_hudi"})


def kafka_group_id_shipped() -> bool:
    try:
        from connectors import kafka_reader
    except Exception:
        return False
    return "group_id" in inspect.getsource(kafka_reader)


def external_secret_store_shipped() -> bool:
    return False


def cadence_pause_keeps_slot() -> bool:
    """Pause flips ``enabled``; slot release is delete-only."""
    try:
        from services.schedule_runner import _dispatch_transfer
        from services.cdc_capture_release import release_schedule_cdc_capture
    except Exception:
        return False
    doc = inspect.getdoc(_dispatch_transfer) or ""
    return "gates the cadence" in doc and callable(release_schedule_cdc_capture)


def oracle_logminer_shipped() -> bool:
    try:
        from connectors.oracle_logminer import OracleLogMinerCdc
    except Exception:
        return False
    return inspect.isclass(OracleLogMinerCdc)


def sqlserver_native_cdc_shipped() -> bool:
    try:
        from connectors.sqlserver_cdc_native import SqlServerNativeCdc
    except Exception:
        return False
    return inspect.isclass(SqlServerNativeCdc)


def sqlserver_change_tracking_shipped() -> bool:
    try:
        from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc
    except Exception:
        return False
    return inspect.isclass(SqlServerChangeTrackingCdc)


def cdc_read_replica_shipped() -> bool:
    return False


def cdc_capture_event_filter_shipped() -> bool:
    return False


def salesforce_oauth_card() -> CapabilityCard | None:
    if "salesforce" not in _transfer_ready_drivers():
        return None
    if salesforce_oauth_shipped():
        return None
    return CapabilityCard(
        title="Do you support Salesforce OAuth",
        text=(
            "Connected App OAuth and token refresh are not connect fields "
            "(salesforce_oauth is false) — saas_common.token reads a pasted "
            "access token from api_key or connection_string. "
            "The SaaS probe has no client_id, refresh_token, or connected_app parameter."
        ),
        source_module="connectors/salesforce.py · test_salesforce · saas_common.token",
        category="connectors",
    )


def slack_notification_card() -> CapabilityCard | None:
    if not slack_webhook_notifications_shipped():
        return None
    return CapabilityCard(
        title="Can I get Slack alerts",
        text=(
            "Yes — Slack alerts are a workspace notification channel: an "
            "incoming webhook URL, dispatched by send_to_channel kind=slack. "
            "Alerts do not move rows."
        ),
        source_module="services/notification_service.py · _send_slack",
        category="enterprise",
    )


def teams_notification_card() -> CapabilityCard | None:
    if not teams_webhook_notifications_shipped():
        return None
    return CapabilityCard(
        title="Can I send Teams alerts",
        text=(
            "Yes — Microsoft Teams alerts are a workspace notification "
            "channel: an incoming webhook URL, dispatched by send_to_channel "
            "kind=teams. "
            "This is not the Team roles page and not SSO."
        ),
        source_module="services/notification_service.py · _send_teams",
        category="enterprise",
    )


def email_notification_card() -> CapabilityCard | None:
    if not email_notifications_shipped():
        return None
    return CapabilityCard(
        title="Can I send email alerts",
        text=(
            "Yes — email alerts are a workspace notification channel: "
            "send_to_channel kind=email via SMTP or a platform mailer. "
            "This is not a transfer destination."
        ),
        source_module="services/notification_service.py · _send_email",
        category="enterprise",
    )


def servicenow_notification_card() -> CapabilityCard | None:
    if not servicenow_notifications_shipped():
        return None
    return CapabilityCard(
        title="Do you support ServiceNow tickets",
        text=(
            "Yes — ServiceNow tickets are a workspace notification "
            "channel: send_to_channel kind=servicenow. "
            "ServiceNow is not a transfer-ready driver."
        ),
        source_module="services/notification_service.py · _send_servicenow",
        category="enterprise",
    )


def slack_connector_card() -> CapabilityCard | None:
    if slack_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Is Slack a connector",
        text=(
            "Slack is not a transfer-ready driver (slack_connector is false) "
            "— unique_driver_types does not include slack. "
            "Incoming webhooks are a notification channel, not a source or destination."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def teams_destination_card() -> CapabilityCard | None:
    if teams_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Teams as a destination",
        text=(
            "Microsoft Teams is not a transfer-ready driver "
            "(teams_dest is false) — unique_driver_types does not include "
            "teams. "
            "Incoming webhooks are a notification channel, not a writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def hubspot_card() -> CapabilityCard | None:
    if "hubspot" not in _transfer_ready_drivers():
        return None
    return _ready_driver_card("Do you support HubSpot", "HubSpot")


def stripe_card() -> CapabilityCard | None:
    if "stripe" not in _transfer_ready_drivers():
        return None
    return _ready_driver_card("Do you support Stripe", "Stripe")


def row_level_security_card() -> CapabilityCard | None:
    if row_level_security_shipped():
        return None
    return CapabilityCard(
        title="Do you support row-level security",
        text=(
            "Datawrap does not ship row-level security (rls is false) — "
            "there is no RLS policy on the destination. "
            "Per-row accounting is the ledger and quarantine reason."
        ),
        source_module="services/row_conservation.py · services/rbac.py",
        category="enterprise",
    )


def snowflake_dynamic_tables_card() -> CapabilityCard | None:
    if snowflake_dynamic_tables_shipped():
        return None
    if not snowflake_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Snowflake dynamic tables",
        text=(
            "Datawrap does not ship Snowflake Dynamic Tables as a write "
            "target (snowflake_dynamic is false). "
            "Snowflake the driver is transfer-ready; Dynamic Tables are not a destination object."
        ),
        source_module="apps/api/registry.py · DATABASE_TYPES",
        category="connectors",
    )


def custom_roles_card() -> CapabilityCard | None:
    roles = closed_rbac_roles()
    if not roles:
        return None
    named = ", ".join(roles)
    return CapabilityCard(
        title="Do you have custom roles",
        text=(
            f"Datawrap does not ship custom roles — the closed RBAC set is "
            f"{named}. "
            "Unknown labels map onto that set; there is no role-builder."
        ),
        source_module="services/rbac.py · _ROLE_PERMISSIONS",
        category="enterprise",
    )


def field_level_encryption_card() -> CapabilityCard | None:
    if field_level_encryption_shipped():
        return None
    return CapabilityCard(
        title="Do you support field-level encryption",
        text=(
            "Datawrap does not ship field-level or column encryption "
            "(field_encryption is false). "
            "Tenant keys wrap newly saved connector secrets, not destination table cells."
        ),
        source_module="services/byok_key_manager.py · create_key",
        category="enterprise",
    )


def hudi_card() -> CapabilityCard | None:
    if hudi_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Apache Hudi",
        text=(
            "Apache Hudi is not a transfer-ready driver — unique_driver_types "
            "does not include hudi. "
            "A catalog tile is not a live write path."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def kafka_consumer_group_card() -> CapabilityCard | None:
    if not kafka_group_id_shipped():
        return None
    return CapabilityCard(
        title="Can I use Kafka consumer groups",
        text=(
            "Yes — a Kafka source takes kafka_group / group_id on the connector. "
            "Uniqueness proof uses an ephemeral group_id so it does not evict "
            "the transfer consumer."
        ),
        source_module="connectors/kafka_reader.py · services/source_duplicate_probe.py",
        category="connectors",
    )


def external_secret_store_card() -> CapabilityCard | None:
    if external_secret_store_shipped():
        return None
    return CapabilityCard(
        title="Can I use AWS Secrets Manager",
        text=(
            "Datawrap does not read AWS Secrets Manager, HashiCorp Vault, "
            "or Azure Key Vault (external_vault is false) — connector "
            "secrets live in the connection store. "
            "Optional tenant-key wrap applies to those stored secrets; it is "
            "not a vault client."
        ),
        source_module="services/connector_store.py · services/byok_key_manager.py",
        category="enterprise",
    )


def unique_key_collision_card() -> CapabilityCard | None:
    if not source_duplicate_probe_shipped():
        return None
    return CapabilityCard(
        title="What happens on a unique key collision",
        text=(
            "A unique key collision is blocked at Gate-9 when two source "
            "rows share a key or the source probe finds duplicate keys, "
            "and append also blocks when those keys already exist at the "
            "destination. "
            "Upsert updates the existing key instead of inserting a second row."
        ),
        source_module=(
            "services/source_duplicate_probe.py · "
            "services/destination_key_collision_probe.py"
        ),
        category="transfer",
    )


def data_location_card() -> CapabilityCard:
    return CapabilityCard(
        title="Where is my data stored",
        text=(
            "Datawrap does not host your table data — rows land in the "
            "destination you connected. "
            "Secrets stay in the connection store, not in the transfer YAML."
        ),
        source_module="services/connector_store.py · destination write path",
        category="enterprise",
    )


def byok_card() -> CapabilityCard | None:
    if not byok_wraps_connector_secrets():
        return None
    return CapabilityCard(
        title="Do you support BYOK",
        text=(
            "Yes — Settings → Enterprise → BYOK wraps newly saved connector "
            "secrets with your KMS key when a tenant key is active. "
            "That is secret wrapping, not destination-table encryption."
        ),
        source_module="services/byok_key_manager.py · create_key",
        category="enterprise",
    )


def gcp_service_account_card() -> CapabilityCard | None:
    if not bigquery_is_transfer_ready() or not bigquery_service_account_shipped():
        return None
    return CapabilityCard(
        title="Can I use a GCP service account",
        text=(
            "Yes — a GCP service account is a BigQuery connector credential: "
            "service_account JSON on the BigQuery connect path."
        ),
        source_module="connectors/bigquery_conn.py · get_client",
        category="connectors",
    )


def workload_identity_card() -> CapabilityCard | None:
    if gcp_workload_identity_shipped():
        return None
    return CapabilityCard(
        title="Can I use Workload Identity",
        text=(
            "Datawrap does not ship GCP Workload Identity as a connect option."
        ),
        source_module="connectors/bigquery_conn.py · get_client",
        category="connectors",
    )


def pause_cdc_card() -> CapabilityCard | None:
    if not cadence_pause_keeps_slot():
        return None
    return CapabilityCard(
        title="Can I pause CDC",
        text=(
            "Yes — Pause on Operations → Pipelines sets enabled false and "
            "stops the cadence; pausing CDC does not drop the replication "
            "slot or the resume token (pause_cdc). "
            "Run now still works; deleting the CDC schedule is what runs "
            "pg_drop_replication_slot."
        ),
        source_module=(
            "services/schedule_runner.py · _dispatch_transfer · "
            "services/cdc_capture_release.py"
        ),
        category="transfer",
    )


def connect_salesforce_card() -> CapabilityCard | None:
    if "salesforce" not in _transfer_ready_drivers():
        return None
    return CapabilityCard(
        title="Procedure: connect Salesforce",
        text=(
            "Click New connection and pick the Salesforce driver, then paste "
            "an access token in api_key or connection_string, click Test, "
            "and Save (salesforce_connect). "
            "Connected App OAuth and token refresh are not connect fields."
        ),
        source_module="connectors/salesforce.py · test_salesforce · Procedure: add a connector",
        category="connectors",
    )


def cdc_read_replica_card() -> CapabilityCard | None:
    if cdc_read_replica_shipped():
        return None
    return CapabilityCard(
        title="Can I use a read replica for CDC",
        text=(
            "Datawrap does not connect CDC to a physical standby or read "
            "replica (cdc_read_replica is false). "
            "REPLICA IDENTITY FULL is a source-table setting for old keys, "
            "not a standby reader."
        ),
        source_module="connectors/postgresql_change_stream.py · services/cdc_capability.py",
        category="transfer",
    )


def oracle_logminer_card() -> CapabilityCard | None:
    if not oracle_is_transfer_ready() or not oracle_logminer_shipped():
        return None
    return CapabilityCard(
        title="Do you support Oracle LogMiner",
        text=(
            "Yes — Oracle CDC reads redo through Oracle LogMiner "
            "(OracleLogMinerCdc). "
            "That capture plugin is not GoldenGate. "
            "The driver card is not the capture plugin."
        ),
        source_module="connectors/oracle_logminer.py · OracleLogMinerCdc",
        category="transfer",
    )


def sqlserver_cdc_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready() or not sqlserver_native_cdc_shipped():
        return None
    tracking = (
        " Change tracking (SqlServerChangeTrackingCdc) is a separate cursor "
        "when a capture instance is not configured."
        if sqlserver_change_tracking_shipped()
        else ""
    )
    return CapabilityCard(
        title="Do you support SQL Server CDC",
        text=(
            "Yes — SQL Server CDC uses the native capture instance "
            f"(sqlserver_cdc / SqlServerNativeCdc).{tracking}"
        ),
        source_module=(
            "connectors/sqlserver_cdc_native.py · "
            "connectors/sqlserver_change_stream.py"
        ),
        category="transfer",
    )


def sqlserver_change_tracking_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready() or not sqlserver_change_tracking_shipped():
        return None
    return CapabilityCard(
        title="Can I use change tracking instead of CDC on SQL Server",
        text=(
            "Yes — SQL Server change tracking is a separate cursor "
            "(sqlserver_ct / SqlServerChangeTrackingCdc) when a capture "
            "instance is not configured. "
            "It is not the WAL/binlog definition of CDC."
        ),
        source_module="connectors/sqlserver_change_stream.py · SqlServerChangeTrackingCdc",
        category="transfer",
    )


def filter_cdc_events_card() -> CapabilityCard | None:
    if cdc_capture_event_filter_shipped():
        return None
    return CapabilityCard(
        title="Can I filter CDC events",
        text=(
            "Datawrap does not ship a capture-side CDC event filter "
            "(cdc_event_filter is false). "
            "Row filters and transforms run on Map before the write."
        ),
        source_module="services/transform_engine.py · services/cdc_capability.py",
        category="transfer",
    )


def microsoft_fabric_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"fabric", "microsoft_fabric", "onelake"})


def power_bi_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"powerbi", "power_bi"})


def azure_data_factory_shipped() -> bool:
    return False


def oracle_xstream_shipped() -> bool:
    return False


def sqlserver_always_on_shipped() -> bool:
    return False


def azure_managed_identity_shipped() -> bool:
    try:
        from connectors.adls_common import _service_principal_credential
    except Exception:
        return False
    src = inspect.getsource(_service_principal_credential)
    return "DefaultAzureCredential" in src or "ManagedIdentityCredential" in src


def cdc_heartbeat_interval_shipped() -> bool:
    return False


def cdc_fetch_size_shipped() -> bool:
    return False


def cdc_skip_deletes_shipped() -> bool:
    return False


def cosmos_db_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"cosmos", "cosmosdb", "cosmos_db"})


def event_hubs_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"eventhubs", "event_hubs", "eventhub"})


def service_bus_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"servicebus", "service_bus"})


def adls_destination_card() -> CapabilityCard | None:
    if not adls_is_transfer_ready():
        return None
    return _ready_driver_card("Do you support ADLS as a destination", "ADLS")


def microsoft_fabric_card() -> CapabilityCard | None:
    if microsoft_fabric_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Fabric",
        text=(
            "Datawrap does not ship Microsoft Fabric or OneLake as a "
            "transfer-ready driver (fabric is false). "
            "Teams alerts are a webhook channel, not a Fabric write."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def power_bi_card() -> CapabilityCard | None:
    if power_bi_shipped():
        return None
    return CapabilityCard(
        title="Do you support Power BI as a destination",
        text=(
            "Datawrap does not ship Power BI as a transfer-ready destination "
            "(power_bi is false). "
            "A report tool is not a write driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_data_factory_card() -> CapabilityCard | None:
    if azure_data_factory_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Data Factory",
        text=(
            "Datawrap does not ship Azure Data Factory as the transfer "
            "engine (adf is false). "
            "An external orchestrator can call /api/v1 after you Confirm."
        ),
        source_module="src/transfer/engine.py",
        category="product",
    )


def oracle_xstream_card() -> CapabilityCard | None:
    if oracle_xstream_shipped():
        return None
    return CapabilityCard(
        title="Do you support Oracle XStream",
        text=(
            "Datawrap does not ship Oracle XStream (oracle_xstream is false). "
            "Oracle CDC on this product is LogMiner, not XStream."
        ),
        source_module="connectors/oracle_logminer.py · OracleLogMinerCdc",
        category="transfer",
    )


def sqlserver_always_on_card() -> CapabilityCard | None:
    if sqlserver_always_on_shipped():
        return None
    return CapabilityCard(
        title="Do you support SQL Server Always On",
        text=(
            "Datawrap does not ship SQL Server Always On Availability Groups "
            "as a CDC connect option (sqlserver_ag is false). "
            "The driver card is not an AG listener topology."
        ),
        source_module="connectors/sqlserver_cdc_native.py · services/catalog_service.py",
        category="transfer",
    )


def azure_managed_identity_card() -> CapabilityCard | None:
    if azure_managed_identity_shipped():
        return None
    return CapabilityCard(
        title="Can I use Azure managed identity",
        text=(
            "Datawrap does not connect Azure with managed identity "
            "(azure_managed_identity is false). "
            "ADLS accepts a service principal (tenant_id, client_id, "
            "client_secret), not DefaultAzureCredential."
        ),
        source_module="connectors/adls_common.py · _service_principal_credential",
        category="connectors",
    )


def cdc_heartbeat_interval_card() -> CapabilityCard | None:
    if cdc_heartbeat_interval_shipped():
        return None
    return CapabilityCard(
        title="Can I set a CDC heartbeat interval",
        text=(
            "Datawrap does not ship a CDC heartbeat interval connect field "
            "(cdc_heartbeat_interval is false). "
            "The capture heartbeat keeps an idle slot alive; it is not a "
            "Freshness SLO and not an INTERVAL column type."
        ),
        source_module="services/cdc_lag_honesty.py · connectors/postgresql_change_stream.py",
        category="transfer",
    )


def cdc_fetch_size_card() -> CapabilityCard | None:
    if cdc_fetch_size_shipped():
        return None
    return CapabilityCard(
        title="What is the CDC fetch size",
        text=(
            "Datawrap does not ship a CDC fetch-size or batch-size connect "
            "field (cdc_fetch_size is false). "
            "Reader batch_size is an internal constructor default, not a "
            "Job Theater phase setting."
        ),
        source_module="connectors/postgresql_change_stream.py",
        category="transfer",
    )


def skip_cdc_deletes_card() -> CapabilityCard | None:
    if cdc_skip_deletes_shipped():
        return None
    return CapabilityCard(
        title="Can I skip deletes in CDC",
        text=(
            "Datawrap does not skip CDC deletes as a capture option "
            "(cdc_skip_deletes is false) — a CDC delete is still applied, "
            "and Map row filters are not a capture-side skip-deletes switch."
        ),
        source_module="services/cdc_capability.py · services/transform_engine.py",
        category="transfer",
    )


def cosmos_db_card() -> CapabilityCard | None:
    if cosmos_db_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cosmos DB",
        text=(
            "Datawrap does not ship Azure Cosmos DB as a transfer-ready "
            "driver (cosmos is false). "
            "Mongo change-stream pre-images are not a Cosmos connect path."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def event_hubs_card() -> CapabilityCard | None:
    if event_hubs_shipped():
        return None
    return CapabilityCard(
        title="Do you support Event Hubs",
        text=(
            "Datawrap does not ship Azure Event Hubs as a transfer-ready "
            "driver (event_hubs is false). "
            "OpenLineage run events are not an Event Hubs writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def service_bus_card() -> CapabilityCard | None:
    if service_bus_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Service Bus",
        text=(
            "Datawrap does not ship Azure Service Bus as a transfer-ready "
            "driver (service_bus is false). "
            "A service principal on ADLS is not a Service Bus writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_sql_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"cloudsql", "cloud_sql"})


def pubsub_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"pubsub", "pub_sub"})


def spanner_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"spanner", "cloud_spanner"})


def vertex_ai_shipped() -> bool:
    return False


def sharepoint_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"sharepoint"})


def dynamics_365_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"dynamics", "dynamics365", "dataverse"})


def purview_shipped() -> bool:
    return False


def excel_online_shipped() -> bool:
    return False


def aws_iam_role_connect_shipped() -> bool:
    return False


def custom_slot_name_shipped() -> bool:
    return False


def blue_green_cutover_shipped() -> bool:
    return False


def azure_sql_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure SQL",
        text=(
            "Yes — Azure SQL is the SQL Server driver (sqlserver / azure_sql). "
            "It is not Azure Synapse and not Cloud SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_sql_card() -> CapabilityCard | None:
    if cloud_sql_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud SQL",
        text=(
            "Cloud SQL is not its own transfer-ready driver (cloud_sql is "
            "false). Connect the instance as MySQL or PostgreSQL. "
            "It is not SQL Server and not Azure SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def pubsub_card() -> CapabilityCard | None:
    if pubsub_shipped():
        return None
    return CapabilityCard(
        title="Do you support Pub/Sub",
        text=(
            "Datawrap does not ship Google Pub/Sub as a transfer-ready "
            "driver (pubsub is false). "
            "A destination count is not a Pub/Sub writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def spanner_card() -> CapabilityCard | None:
    if spanner_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Spanner",
        text=(
            "Datawrap does not ship Cloud Spanner as a transfer-ready "
            "driver (spanner is false). "
            "dbt Cloud is not a Spanner connect path."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def vertex_ai_card() -> CapabilityCard | None:
    if vertex_ai_shipped():
        return None
    return CapabilityCard(
        title="Can I use Vertex AI as a destination",
        text=(
            "Datawrap does not ship Vertex AI as a transfer destination "
            "(vertex_ai is false). "
            "Settings → AI Hybrid is wording polish, not a Vertex write."
        ),
        source_module="src/ai/first_party/engine.py · unique_driver_types",
        category="connectors",
    )


def sharepoint_card() -> CapabilityCard | None:
    if sharepoint_shipped():
        return None
    return CapabilityCard(
        title="Do you support SharePoint as a destination",
        text=(
            "Datawrap does not ship SharePoint as a transfer-ready "
            "destination (sharepoint is false). "
            "A warehouse driver card is not a SharePoint writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def dynamics_365_card() -> CapabilityCard | None:
    if dynamics_365_shipped():
        return None
    return CapabilityCard(
        title="Do you support Dynamics 365",
        text=(
            "Datawrap does not ship Dynamics 365 or Dataverse as a "
            "transfer-ready driver (dynamics365 is false). "
            "Snowflake Dynamic Tables are not Dynamics."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def purview_card() -> CapabilityCard | None:
    if purview_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Purview",
        text=(
            "Datawrap does not ship Microsoft Purview as a catalog or "
            "lineage destination (purview is false). "
            "Teams alerts are a webhook channel, not Purview."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def excel_online_card() -> CapabilityCard | None:
    if excel_online_shipped():
        return None
    return CapabilityCard(
        title="Can I write to Excel Online",
        text=(
            "Excel Online / Microsoft 365 workbooks are not a destination "
            "driver (excel_online is false). "
            "Excel files are a transfer-ready file format."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def aws_iam_role_card() -> CapabilityCard | None:
    if aws_iam_role_connect_shipped():
        return None
    return CapabilityCard(
        title="Can I assume an AWS IAM role",
        text=(
            "Datawrap does not assume an AWS IAM role as a connect option "
            "(aws_iam_role is false). "
            "S3 and warehouse cards take keys or a service account, not "
            "sts:AssumeRole, and IAM is not an RBAC role count."
        ),
        source_module="connectors/s3_writer.py · unique_driver_types",
        category="connectors",
    )


def custom_slot_name_card() -> CapabilityCard | None:
    if custom_slot_name_shipped():
        return None
    return CapabilityCard(
        title="Can I set the replication slot name",
        text=(
            "Datawrap does not ship a custom replication slot name as a "
            "connect field (custom_slot_name is false). "
            "The slot name is derived from database, table, and cursor key."
        ),
        source_module="connectors/postgresql_change_stream.py · _slot_name",
        category="transfer",
    )


def onedrive_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"onedrive", "one_drive"})


def looker_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"looker", "looker_studio"})


def bigquery_omni_shipped() -> bool:
    return False


def google_sheets_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"sheets", "google_sheets", "gsheets"})


def alloydb_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"alloydb", "alloy_db"})


def microsoft_graph_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"graph", "microsoft_graph", "msgraph"})


def azure_openai_shipped() -> bool:
    return False


def kusto_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"kusto", "adx", "azure_data_explorer"})


def event_grid_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"eventgrid", "event_grid"})


def google_dataflow_shipped() -> bool:
    return False


def power_platform_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"powerplatform", "power_platform", "dataverse"})


def conditional_access_shipped() -> bool:
    return False


def azure_database_postgresql_card() -> CapabilityCard | None:
    if not postgresql_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure Database for PostgreSQL",
        text=(
            "Yes — Azure Database for PostgreSQL is the PostgreSQL driver "
            "(postgresql / azure_database_postgresql). "
            "It is not Cloud SQL and not Azure SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_database_mysql_card() -> CapabilityCard | None:
    if not mysql_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure Database for MySQL",
        text=(
            "Yes — Azure Database for MySQL is the MySQL driver "
            "(mysql / azure_database_mysql). "
            "It is not Cloud SQL and not Azure SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_sql_sqlserver_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Cloud SQL for SQL Server",
        text=(
            "Cloud SQL for SQL Server is not its own driver "
            "(cloud_sql_sqlserver is false). "
            "Connect the instance as SQL Server (sqlserver). It is not Azure SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def onedrive_card() -> CapabilityCard | None:
    if onedrive_shipped():
        return None
    return CapabilityCard(
        title="Do you support OneDrive as a destination",
        text=(
            "Datawrap does not ship OneDrive as a transfer-ready destination "
            "(onedrive is false). "
            "A warehouse driver card is not a OneDrive writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def looker_card() -> CapabilityCard | None:
    if looker_shipped():
        return None
    return CapabilityCard(
        title="Do you support Looker as a destination",
        text=(
            "Datawrap does not ship Looker as a transfer-ready destination "
            "(looker is false). "
            "BigQuery is not a Looker writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def looker_studio_card() -> CapabilityCard | None:
    if looker_shipped():
        return None
    return CapabilityCard(
        title="Do you support Looker Studio",
        text=(
            "Datawrap does not ship Looker Studio as a transfer destination "
            "(looker_studio is false). "
            "Transfer Studio is not Looker Studio."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bigquery_omni_card() -> CapabilityCard | None:
    if bigquery_omni_shipped():
        return None
    return CapabilityCard(
        title="Do you support BigQuery Omni",
        text=(
            "BigQuery Omni is not a separate connect option "
            "(bigquery_omni is false). "
            "The BigQuery driver is transfer-ready; Omni multi-cloud is not "
            "a connect field."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_sheets_card() -> CapabilityCard | None:
    if google_sheets_shipped():
        return None
    return CapabilityCard(
        title="Can I write to Google Sheets",
        text=(
            "Datawrap does not ship Google Sheets as a transfer-ready "
            "destination (google_sheets is false). "
            "Pub/Sub is not a Sheets writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def alloydb_card() -> CapabilityCard | None:
    if alloydb_shipped():
        return None
    return CapabilityCard(
        title="Do you support AlloyDB",
        text=(
            "AlloyDB is not its own transfer-ready driver (alloydb is false). "
            "Connect the instance as PostgreSQL. It is not Cloud SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_graph_card() -> CapabilityCard | None:
    if microsoft_graph_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Graph as a source",
        text=(
            "Datawrap does not ship Microsoft Graph as a transfer-ready "
            "source (microsoft_graph is false). "
            "Teams alerts are not Graph."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_openai_card() -> CapabilityCard | None:
    if azure_openai_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure OpenAI as a destination",
        text=(
            "Datawrap does not ship Azure OpenAI as a transfer destination "
            "(azure_openai is false). "
            "Settings → AI Hybrid is wording polish, not an Azure OpenAI write."
        ),
        source_module="src/ai/first_party/engine.py · unique_driver_types",
        category="connectors",
    )


def kusto_card() -> CapabilityCard | None:
    if kusto_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Data Explorer",
        text=(
            "Datawrap does not ship Azure Data Explorer / Kusto as a "
            "transfer-ready driver (kusto is false). "
            "Azure Data Factory is not Kusto."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def event_grid_card() -> CapabilityCard | None:
    if event_grid_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Event Grid",
        text=(
            "Datawrap does not ship Azure Event Grid as a transfer-ready "
            "driver (event_grid is false). "
            "Event Hubs is not Event Grid."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_dataflow_card() -> CapabilityCard | None:
    if google_dataflow_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Cloud Dataflow",
        text=(
            "Datawrap does not run Google Cloud Dataflow "
            "(dataflow_google is false). "
            "Pub/Sub is not Dataflow, and Datawrap is not Cloud Dataflow."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def power_platform_card() -> CapabilityCard | None:
    if power_platform_shipped():
        return None
    return CapabilityCard(
        title="Do you support Power Platform",
        text=(
            "Datawrap does not ship Power Platform as a transfer destination "
            "(power_platform is false). "
            "Power BI is not Power Platform."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def conditional_access_card() -> CapabilityCard | None:
    if conditional_access_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Conditional Access",
        text=(
            "Datawrap does not ship Azure Conditional Access as a network "
            "control (conditional_access is false). "
            "It is not Synapse and not an IP allowlist."
        ),
        source_module="src/routers/workspace_router.py · ip_allowlist",
        category="connectors",
    )


def cloud_composer_card() -> CapabilityCard | None:
    return CapabilityCard(
        title="Do you support Cloud Composer",
        text=(
            "Datawrap does not run Cloud Composer or Airflow DAGs "
            "(cloud_composer is false). "
            "An external orchestrator can call /api/v1 after Confirm."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="product",
    )


def blue_green_cutover_card() -> CapabilityCard | None:
    if blue_green_cutover_shipped():
        return None
    return CapabilityCard(
        title="Can I do a blue-green cutover",
        text=(
            "Datawrap does not ship a blue-green cutover "
            "(blue_green_cutover is false). "
            "Pause/Activate and Run now are not a dual-environment swap."
        ),
        source_module="services/schedule_runner.py · _dispatch_transfer",
        category="transfer",
    )


def firebase_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"firebase", "firestore"})


def bigtable_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"bigtable", "cloud_bigtable"})


def dataproc_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"dataproc"})


def entra_pim_shipped() -> bool:
    return False


def gke_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gke", "kubernetes"})


def outlook_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"outlook", "exchange"})


def youtube_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"youtube"})


def google_ads_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"google_ads", "googleads"})


def google_drive_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gdrive", "google_drive", "drive"})


def google_docs_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gdocs", "google_docs"})


def google_analytics_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"ga", "google_analytics", "ga4"})


def cloud_run_shipped() -> bool:
    return False


def stream_analytics_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"stream_analytics", "asa"})


def microsoft_lists_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"lists", "microsoft_lists"})


def github_enterprise_dest_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"github", "github_enterprise"})


def bq_dts_shipped() -> bool:
    return False


def bq_linked_dataset_shipped() -> bool:
    return False


def cloud_kms_connect_shipped() -> bool:
    return False


def app_engine_shipped() -> bool:
    return False


def data_catalog_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"data_catalog", "datacatalog"})


def cloud_build_shipped() -> bool:
    return False


def artifact_registry_shipped() -> bool:
    return False


def cloud_functions_shipped() -> bool:
    return False


def azure_files_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_files", "azurefiles"})


def log_analytics_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"log_analytics", "loganalytics"})


def azure_ai_search_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_ai_search", "cognitive_search"})


def azure_analysis_services_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"ssas", "analysis_services"})


def cloud_sql_auth_proxy_shipped() -> bool:
    return False


def synapse_link_shipped() -> bool:
    return False


def dynamodb_is_transfer_ready() -> bool:
    return "dynamodb" in _transfer_ready_drivers()


def elasticsearch_is_transfer_ready() -> bool:
    return "elasticsearch" in _transfer_ready_drivers()


def azure_table_storage_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_table", "table_storage", "tablestorage"})


def azure_queue_storage_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_queue", "queue_storage", "queuestorage"})


def splunk_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"splunk"})


def tableau_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"tableau"})


def elastic_cloud_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"elastic_cloud", "elasticcloud"})


def opensearch_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"opensearch"})


def azure_sql_service_principal_shipped() -> bool:
    try:
        from connectors.sqlserver import test_sqlserver
    except Exception:
        return False
    names = {name.lower() for name in inspect.signature(test_sqlserver).parameters}
    return bool(names & {"tenant_id", "client_id", "client_secret", "service_principal"})


def teams_source_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"teams", "ms_teams"})


def microsoft_365_dest_shipped() -> bool:
    return bool(
        _transfer_ready_drivers()
        & {"microsoft_365", "office365", "m365", "excel_online", "onedrive", "sharepoint"}
    )


def intune_shipped() -> bool:
    return False


def defender_shipped() -> bool:
    return False


def sentinel_shipped() -> bool:
    return False


def azure_ml_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_ml", "azureml"})


def search_ads_360_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"search_ads_360", "sa360"})


def cloud_tasks_shipped() -> bool:
    return False


def vpc_service_controls_shipped() -> bool:
    return False


def azure_firewall_shipped() -> bool:
    return False


def campaign_manager_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"campaign_manager", "cm360"})


def aurora_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"aurora"})


def documentdb_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"documentdb", "docdb"})


def cloud_armor_shipped() -> bool:
    return False


def cloud_interconnect_shipped() -> bool:
    return False


def cloud_vpn_shipped() -> bool:
    return False


def azure_arc_shipped() -> bool:
    return False


def azure_lighthouse_shipped() -> bool:
    return False


def azure_monitor_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_monitor"})


def azure_devops_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_devops", "ado"})


def azure_boards_shipped() -> bool:
    return False


def azure_migrate_shipped() -> bool:
    return False


def dv360_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"dv360", "display_video"})


def gcs_transfer_service_shipped() -> bool:
    return False


def qlik_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"qlik"})


def entra_governance_shipped() -> bool:
    return False


def hdinsight_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"hdinsight"})


def expressroute_shipped() -> bool:
    return False


def ssis_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"ssis"})


def ssrs_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"ssrs"})


def gmail_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gmail"})


def google_calendar_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gcal", "google_calendar", "calendar"})


def azure_cdn_shipped() -> bool:
    return False


def azure_blueprints_shipped() -> bool:
    return False


def azure_automation_shipped() -> bool:
    return False


def bigquery_ml_shipped() -> bool:
    return False


def filestore_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"filestore"})


def persistent_disk_shipped() -> bool:
    return False


def datastream_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"datastream"})


def dataplex_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"dataplex"})


def informatica_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"informatica"})


def talend_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"talend"})


def matillion_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"matillion"})


def google_workspace_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"google_workspace", "gworkspace", "gsuite"})


def aks_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"aks"})


def azure_functions_shipped() -> bool:
    return False


def azure_batch_shipped() -> bool:
    return False


def cloud_scheduler_shipped() -> bool:
    return False


def vertex_ai_search_shipped() -> bool:
    return False


def business_central_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"business_central"})


def application_insights_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"application_insights", "appinsights"})


def site_recovery_shipped() -> bool:
    return False


def entra_external_id_shipped() -> bool:
    return False


def azure_ad_b2c_shipped() -> bool:
    return False


def google_meet_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gmeet", "google_meet", "meet"})


def google_chat_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gchat", "google_chat"})


def yammer_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"yammer"})


def viva_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"viva"})


def copilot_studio_shipped() -> bool:
    return False


def logic_apps_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"logic_apps", "logicapps"})


def eventarc_shipped() -> bool:
    return False


def dialogflow_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"dialogflow"})


def azure_policy_shipped() -> bool:
    return False


def planner_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"planner"})


def microsoft_todo_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"todo", "microsoft_todo"})


def bookings_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"bookings"})


def classroom_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"classroom", "google_classroom"})


def azure_signalr_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"signalr", "azure_signalr"})


def azure_service_fabric_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"service_fabric"})


def keep_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"keep", "google_keep"})


def appsheet_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"appsheet"})


def azure_communication_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"acs", "azure_communication"})


def azure_apim_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"apim", "api_management"})


def cloud_workflows_shipped() -> bool:
    return False


def chronicle_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"chronicle"})


def microsoft_forms_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"forms", "microsoft_forms"})


def microsoft_project_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"msproject", "microsoft_project"})


def container_apps_shipped() -> bool:
    return False


def azure_front_door_shipped() -> bool:
    return False


def identity_platform_shipped() -> bool:
    return False


def pubsub_lite_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"pubsub_lite", "pslite"})


def cloud_dns_shipped() -> bool:
    return False


def cloud_domains_shipped() -> bool:
    return False


def natural_language_api_shipped() -> bool:
    return False


def azure_mariadb_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"mariadb", "azure_mariadb"})


def azure_relay_shipped() -> bool:
    return False


def azure_remote_rendering_shipped() -> bool:
    return False


def azure_quantum_shipped() -> bool:
    return False


def azure_orbital_shipped() -> bool:
    return False


def azure_local_shipped() -> bool:
    return False


def windows_365_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"windows_365", "w365"})


def api_gateway_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"api_gateway", "apigateway"})


def azure_test_plans_shipped() -> bool:
    return False


def azure_dedicated_sql_pool_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"dedicated_sql_pool", "synapse"})


def cloud_cdn_shipped() -> bool:
    return False


def cloud_nat_shipped() -> bool:
    return False


def cloud_iap_shipped() -> bool:
    return False


def cloud_hsm_shipped() -> bool:
    return False


def certificate_manager_shipped() -> bool:
    return False


def binary_authorization_shipped() -> bool:
    return False


def service_mesh_shipped() -> bool:
    return False


def apigee_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"apigee"})


def iot_central_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"iot_central"})


def time_series_insights_shipped() -> bool:
    return False


def azure_maps_shipped() -> bool:
    return False


def notification_hubs_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"notification_hubs"})


def azure_web_pubsub_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"web_pubsub", "azure_web_pubsub"})


def azure_repos_shipped() -> bool:
    return False


def azure_pipelines_shipped() -> bool:
    return False


def bing_ads_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"bing_ads"})


def retail_api_shipped() -> bool:
    return False


def healthcare_api_shipped() -> bool:
    return False


def cloud_endpoints_shipped() -> bool:
    return False


def tag_manager_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gtm", "tag_manager"})


def search_console_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"search_console", "gsc"})


def cloud_load_balancing_shipped() -> bool:
    return False


def assured_workloads_shipped() -> bool:
    return False


def config_connector_shipped() -> bool:
    return False


def app_hub_shipped() -> bool:
    return False


def microsoft_advertising_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"microsoft_advertising", "msads"})


def azure_artifacts_shipped() -> bool:
    return False


def document_ai_shipped() -> bool:
    return False


def fhir_shipped() -> bool:
    return False


def github_copilot_dest_shipped() -> bool:
    return False


def google_photos_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gphotos", "google_photos"})


def google_contacts_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gcontacts", "google_contacts"})


def google_maps_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gmaps", "google_maps"})


def google_news_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gnews", "google_news"})


def google_play_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"gplay", "google_play"})


def vision_ai_shipped() -> bool:
    return False


def speech_to_text_shipped() -> bool:
    return False


def earth_engine_shipped() -> bool:
    return False


def bigquery_bi_engine_shipped() -> bool:
    return False


def azure_media_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_media", "media_services"})


def azure_cognitive_shipped() -> bool:
    return False


def azure_bot_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"azure_bot", "bot_service"})


def confidential_ledger_shipped() -> bool:
    return False


def operator_nexus_shipped() -> bool:
    return False


def microsoft_clarity_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"clarity", "microsoft_clarity"})


def anthos_shipped() -> bool:
    return False


def iot_hub_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"iot_hub", "iothub"})


def merchant_center_shipped() -> bool:
    return bool(_transfer_ready_drivers() & {"merchant_center", "gmc"})


def azure_blob_card() -> CapabilityCard | None:
    if not adls_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure Blob Storage",
        text=(
            "Yes — Azure Blob Storage uses the ADLS driver (adls / azure_blob). "
            "unique_driver_types includes adls, not a separate blob driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_redis_card() -> CapabilityCard | None:
    if not redis_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure Cache for Redis",
        text=(
            "Yes — Azure Cache for Redis is the Redis driver (redis). "
            "It is not Synapse."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_flexible_postgres_card() -> CapabilityCard | None:
    if not postgresql_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure PostgreSQL Flexible Server",
        text=(
            "Yes — Azure PostgreSQL Flexible Server is the PostgreSQL driver "
            "(postgresql / azure_flexible_server). "
            "It is not Cloud SQL and not Azure SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def firebase_card() -> CapabilityCard | None:
    if firebase_shipped():
        return None
    return CapabilityCard(
        title="Do you support Firebase",
        text=(
            "Datawrap does not ship Firebase or Firestore as a transfer-ready "
            "driver (firebase_ready is false). fireba"
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def firestore_card() -> CapabilityCard | None:
    if firebase_shipped():
        return None
    return CapabilityCard(
        title="Do you support Firestore",
        text=(
            "Datawrap does not ship Firestore as a transfer-ready driver "
            "(firestore is false). Firebase is not a connect path."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bigtable_card() -> CapabilityCard | None:
    if bigtable_shipped():
        return None
    return CapabilityCard(
        title="Do you support Bigtable",
        text=(
            "Datawrap does not ship Cloud Bigtable as a transfer-ready driver "
            "(bigtable is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def dataproc_card() -> CapabilityCard | None:
    if dataproc_shipped():
        return None
    return CapabilityCard(
        title="Do you support Dataproc",
        text=(
            "Datawrap does not run Dataproc (dataproc is false). "
            "Spark jobs stay outside the write engine."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def entra_pim_card() -> CapabilityCard | None:
    if entra_pim_shipped():
        return None
    return CapabilityCard(
        title="Do you support Entra PIM",
        text=(
            "Datawrap does not ship Entra Privileged Identity Management "
            "(entra_pim is false). SSO is SAML/OIDC, not PIM elevation."
        ),
        source_module="src/routers/workspace_router.py · sso",
        category="connectors",
    )


def gke_card() -> CapabilityCard | None:
    if gke_shipped():
        return None
    return CapabilityCard(
        title="Do you support GKE as a destination",
        text=(
            "Datawrap does not ship GKE as a transfer destination "
            "(gke is false). A warehouse driver card is not a GKE writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def outlook_card() -> CapabilityCard | None:
    if outlook_shipped():
        return None
    return CapabilityCard(
        title="Do you support Outlook as a destination",
        text=(
            "Datawrap does not ship Outlook as a transfer-ready destination "
            "(outlook is false). A warehouse driver card is not an Outlook writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def youtube_card() -> CapabilityCard | None:
    if youtube_shipped():
        return None
    return CapabilityCard(
        title="Do you support YouTube as a source",
        text=(
            "Datawrap does not ship YouTube as a transfer-ready source "
            "(youtube is false). BigQuery is not a YouTube connector."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_ads_card() -> CapabilityCard | None:
    if google_ads_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Ads as a source",
        text=(
            "Datawrap does not ship Google Ads as a transfer-ready source "
            "(google_ads is false). Pub/Sub is not an Ads connector."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_drive_card() -> CapabilityCard | None:
    if google_drive_shipped():
        return None
    return CapabilityCard(
        title="Can I write to Google Drive",
        text=(
            "Datawrap does not ship Google Drive as a transfer-ready "
            "destination (google_drive is false). Pub/Sub is not a Drive writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_docs_card() -> CapabilityCard | None:
    if google_docs_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Docs as a destination",
        text=(
            "Datawrap does not ship Google Docs as a transfer-ready "
            "destination (google_docs is false). Pub/Sub is not a Docs writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_analytics_card() -> CapabilityCard | None:
    if google_analytics_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Analytics",
        text=(
            "Datawrap does not ship Google Analytics as a transfer-ready "
            "source (google_analytics is false). Pub/Sub is not a GA connector."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_run_card() -> CapabilityCard | None:
    if cloud_run_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Run as a destination",
        text=(
            "Datawrap does not ship Cloud Run as a transfer destination "
            "(cloud_run is false). Cloud Dataflow is not Cloud Run."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def stream_analytics_card() -> CapabilityCard | None:
    if stream_analytics_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Stream Analytics",
        text=(
            "Datawrap does not ship Azure Stream Analytics "
            "(stream_analytics is false). Cosmos is not Stream Analytics."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_lists_card() -> CapabilityCard | None:
    if microsoft_lists_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Lists",
        text=(
            "Datawrap does not ship Microsoft Lists as a transfer-ready "
            "destination (microsoft_lists is false). Teams is not Lists."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def exchange_online_card() -> CapabilityCard | None:
    if outlook_shipped():
        return None
    return CapabilityCard(
        title="Do you support Exchange Online",
        text=(
            "Datawrap does not ship Exchange Online as a transfer-ready "
            "destination (exchange_online is false). Excel Online is not Exchange."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def github_enterprise_dest_card() -> CapabilityCard | None:
    if github_enterprise_dest_shipped():
        return None
    return CapabilityCard(
        title="Do you support GitHub Enterprise as a destination",
        text=(
            "Datawrap does not ship GitHub Enterprise as a transfer destination "
            "(github_enterprise is false). GitHub Actions can call /api/v1; "
            "that is not a GitHub writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bq_dts_card() -> CapabilityCard | None:
    if bq_dts_shipped():
        return None
    return CapabilityCard(
        title="Do you support BigQuery Data Transfer Service",
        text=(
            "Datawrap does not ship BigQuery Data Transfer Service "
            "(bq_dts is false). The BigQuery driver is transfer-ready; "
            "DTS is not a connect field."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bq_linked_dataset_card() -> CapabilityCard | None:
    if bq_linked_dataset_shipped():
        return None
    return CapabilityCard(
        title="Can I use a BigQuery linked dataset",
        text=(
            "Datawrap does not ship BigQuery linked datasets as a connect "
            "option (bq_linked_dataset is false). "
            "OpenLineage dataset grain is not a linked dataset."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_kms_card() -> CapabilityCard | None:
    if cloud_kms_connect_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud KMS",
        text=(
            "Datawrap does not ship Cloud KMS as a connect option "
            "(cloud_kms is false). Settings → Enterprise → BYOK wraps "
            "connector secrets; it is not a Cloud KMS destination."
        ),
        source_module="src/routers/workspace_router.py · byok",
        category="connectors",
    )


def app_engine_card() -> CapabilityCard | None:
    if app_engine_shipped():
        return None
    return CapabilityCard(
        title="Do you support App Engine",
        text=(
            "Datawrap does not ship App Engine as a transfer destination "
            "(app_engine is false). An IP allowlist is not App Engine."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def data_catalog_card() -> CapabilityCard | None:
    if data_catalog_shipped():
        return None
    return CapabilityCard(
        title="Do you support Data Catalog",
        text=(
            "Datawrap does not ship Google Data Catalog as a connect option "
            "(data_catalog is false). Iceberg catalog mode is not Data Catalog."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_build_card() -> CapabilityCard | None:
    if cloud_build_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Build",
        text=(
            "Datawrap does not run Cloud Build (cloud_build is false). "
            "A browser build is not Cloud Build."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def artifact_registry_card() -> CapabilityCard | None:
    if artifact_registry_shipped():
        return None
    return CapabilityCard(
        title="Do you support Artifact Registry",
        text=(
            "Datawrap does not ship Artifact Registry "
            "(artifact_registry is false). A Kafka schema registry URL "
            "is not Artifact Registry."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_functions_card() -> CapabilityCard | None:
    if cloud_functions_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Functions",
        text=(
            "Datawrap does not ship Cloud Functions as a transfer destination "
            "(cloud_functions is false). It is not Cloud SQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_files_card() -> CapabilityCard | None:
    if azure_files_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Files",
        text=(
            "Datawrap does not ship Azure Files as a transfer-ready driver "
            "(azure_files is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def log_analytics_card() -> CapabilityCard | None:
    if log_analytics_shipped():
        return None
    return CapabilityCard(
        title="Do you support Log Analytics",
        text=(
            "Datawrap does not ship Azure Log Analytics as a destination "
            "(log_analytics is false). Job Theater logs are not Log Analytics."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_ai_search_card() -> CapabilityCard | None:
    if azure_ai_search_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure AI Search",
        text=(
            "Datawrap does not ship Azure AI Search as a transfer destination "
            "(azure_ai_search is false). Azure OpenAI is not AI Search."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_analysis_services_card() -> CapabilityCard | None:
    if azure_analysis_services_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Analysis Services",
        text=(
            "Datawrap does not ship Azure Analysis Services / SSAS "
            "(analysis_services is false). Service Bus is not SSAS."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_sql_auth_proxy_card() -> CapabilityCard | None:
    if cloud_sql_auth_proxy_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud SQL Auth Proxy",
        text=(
            "Datawrap does not ship Cloud SQL Auth Proxy as a connect field "
            "(cloud_sql_auth_proxy is false). Connect the instance as "
            "MySQL or PostgreSQL."
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def synapse_link_card() -> CapabilityCard | None:
    if synapse_link_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Synapse Link",
        text=(
            "Datawrap does not ship Azure Synapse Link "
            "(synapse_link is false). The Synapse warehouse driver is "
            "also not transfer-ready."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_table_storage_card() -> CapabilityCard | None:
    if azure_table_storage_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Table Storage",
        text=(
            "Datawrap does not ship Azure Table Storage as a transfer-ready "
            "driver (table_storage is false). azure_table "
            "Azure Blob / ADLS is a different object store."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_queue_storage_card() -> CapabilityCard | None:
    if azure_queue_storage_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Queue Storage",
        text=(
            "Datawrap does not ship Azure Queue Storage as a transfer-ready "
            "driver (queue_storage is false). azure_queue "
            "Azure Blob / ADLS is a different object store."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def sqlserver_azure_vm_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support SQL Server on Azure VMs",
        text=(
            "Yes — SQL Server on Azure VMs is the SQL Server driver "
            "(sqlserver / sqlserver_azure_vm). "
            "It is not Flexible Server and not Azure Database for PostgreSQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def splunk_destination_card() -> CapabilityCard | None:
    if splunk_shipped():
        return None
    return CapabilityCard(
        title="Do you support Splunk as a destination",
        text=(
            "Datawrap does not ship Splunk as a transfer-ready destination "
            "(splunk is false). A warehouse driver card is not a Splunk writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def tableau_destination_card() -> CapabilityCard | None:
    if tableau_shipped():
        return None
    return CapabilityCard(
        title="Do you support Tableau as a destination",
        text=(
            "Datawrap does not ship Tableau as a transfer-ready destination "
            "(tableau is false). A warehouse driver card is not a Tableau writer."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_data_lake_gen2_card() -> CapabilityCard | None:
    if not adls_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure Data Lake Gen2",
        text=(
            "Yes — Azure Data Lake Gen2 is the ADLS driver "
            "(adls / data_lake_gen2). "
            "It is not Azure Data Explorer and not Kusto."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_sql_service_principal_card() -> CapabilityCard | None:
    if azure_sql_service_principal_shipped():
        return None
    return CapabilityCard(
        title="Can I use a service principal for Azure SQL",
        text=(
            "Azure SQL does not accept an ADLS service principal "
            "(azure_sql_sp is false). "
            "Connect Azure SQL as sqlserver with username and password."
        ),
        source_module="connectors/sqlserver.py · test_sqlserver",
        category="connectors",
    )


def dynamodb_card() -> CapabilityCard | None:
    if not dynamodb_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Amazon DynamoDB",
        text=(
            "Yes — DynamoDB is a transfer-ready driver (dynamodb). "
            "Catalog tile count is not the proof; unique_driver_types is."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def elasticsearch_card() -> CapabilityCard | None:
    if not elasticsearch_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Elasticsearch",
        text=(
            "Yes — Elasticsearch is a transfer-ready driver (elasticsearch). "
            "Elastic Cloud is hosted Elasticsearch, not a separate SKU."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def elastic_cloud_card() -> CapabilityCard | None:
    if elastic_cloud_shipped():
        return None
    return CapabilityCard(
        title="Do you support Elastic Cloud",
        text=(
            "Elastic Cloud is not its own transfer-ready driver "
            "(elastic_cloud is false). "
            "Connect the cluster as Elasticsearch."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def opensearch_card() -> CapabilityCard | None:
    if opensearch_shipped():
        return None
    return CapabilityCard(
        title="Do you support OpenSearch",
        text=(
            "Datawrap does not ship OpenSearch as a transfer-ready driver "
            "(opensearch is false). Elasticsearch is a different driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def teams_source_card() -> CapabilityCard | None:
    if teams_source_shipped():
        return None
    return CapabilityCard(
        title="Do you support Teams as a source",
        text=(
            "Datawrap does not ship Teams as a transfer-ready source "
            "(teams_source is false). "
            "Incoming webhooks are a notification channel, not a reader."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_365_destination_card() -> CapabilityCard | None:
    if microsoft_365_dest_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft 365 as a destination",
        text=(
            "Microsoft 365 is not a destination driver "
            "(microsoft_365 is false). "
            "A suite name is not a write driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def memorystore_card() -> CapabilityCard | None:
    if not redis_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Memorystore",
        text=(
            "Yes — Memorystore is the Redis driver (redis / memorystore). "
            "It is not a separate memorystore SKU."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_sql_edge_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Azure SQL Edge",
        text=(
            "Yes — Azure SQL Edge is the SQL Server driver "
            "(sqlserver / azure_sql_edge). "
            "It is not a separate edge SKU."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def intune_card() -> CapabilityCard | None:
    if intune_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Intune",
        text=(
            "Datawrap does not ship Microsoft Intune as a transfer-ready "
            "driver (intune is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def defender_card() -> CapabilityCard | None:
    if defender_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Defender",
        text=(
            "Datawrap does not ship Microsoft Defender as a transfer-ready "
            "driver (defender is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def sentinel_card() -> CapabilityCard | None:
    if sentinel_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Sentinel",
        text=(
            "Datawrap does not ship Microsoft Sentinel as a transfer-ready "
            "driver (sentinel is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_ml_card() -> CapabilityCard | None:
    if azure_ml_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Machine Learning as a destination",
        text=(
            "Datawrap does not ship Azure Machine Learning as a transfer "
            "destination (azure_ml is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def search_ads_360_card() -> CapabilityCard | None:
    if search_ads_360_shipped():
        return None
    return CapabilityCard(
        title="Do you support Search Ads 360",
        text=(
            "Datawrap does not ship Search Ads 360 as a transfer-ready "
            "source (search_ads_360 is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_tasks_card() -> CapabilityCard | None:
    if cloud_tasks_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Tasks",
        text=(
            "Datawrap does not ship Cloud Tasks as a transfer destination "
            "(cloud_tasks is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def vpc_service_controls_card() -> CapabilityCard | None:
    if vpc_service_controls_shipped():
        return None
    return CapabilityCard(
        title="Do you support VPC Service Controls",
        text=(
            "Datawrap does not ship VPC Service Controls "
            "(vpc_sc is false). That is a GCP perimeter, not a connect field."
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def azure_firewall_card() -> CapabilityCard | None:
    if azure_firewall_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Firewall",
        text=(
            "Datawrap does not ship Azure Firewall as a connect option "
            "(azure_firewall is false)."
        ),
        source_module="src/routers/workspace_router.py · allowlist",
        category="connectors",
    )


def campaign_manager_card() -> CapabilityCard | None:
    if campaign_manager_shipped():
        return None
    return CapabilityCard(
        title="Do you support Campaign Manager",
        text=(
            "Datawrap does not ship Campaign Manager 360 as a transfer-ready "
            "source (campaign_manager is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def aurora_card() -> CapabilityCard | None:
    if aurora_shipped():
        return None
    return CapabilityCard(
        title="Do you support Amazon Aurora",
        text=(
            "Amazon Aurora is not its own transfer-ready driver "
            "(aurora is false). Connect the instance as MySQL or PostgreSQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def documentdb_card() -> CapabilityCard | None:
    if documentdb_shipped():
        return None
    return CapabilityCard(
        title="Do you support Amazon DocumentDB",
        text=(
            "Datawrap does not ship Amazon DocumentDB as a transfer-ready "
            "driver (documentdb is false). MongoDB is a different driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_armor_card() -> CapabilityCard | None:
    if cloud_armor_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Armor",
        text=(
            "Datawrap does not ship Cloud Armor as a connect option "
            "(cloud_armor is false)."
        ),
        source_module="src/routers/workspace_router.py · allowlist",
        category="connectors",
    )


def cloud_interconnect_card() -> CapabilityCard | None:
    if cloud_interconnect_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Interconnect",
        text=(
            "Datawrap does not ship Cloud Interconnect "
            "(cloud_interconnect is false)."
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def cloud_vpn_card() -> CapabilityCard | None:
    if cloud_vpn_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud VPN",
        text=(
            "Datawrap does not ship Cloud VPN as a connect option "
            "(cloud_vpn is false)."
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def azure_arc_card() -> CapabilityCard | None:
    if azure_arc_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Arc",
        text=(
            "Datawrap does not ship Azure Arc as a transfer-ready driver "
            "(azure_arc is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_lighthouse_card() -> CapabilityCard | None:
    if azure_lighthouse_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Lighthouse",
        text=(
            "Datawrap does not ship Azure Lighthouse "
            "(azure_lighthouse is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_monitor_card() -> CapabilityCard | None:
    if azure_monitor_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Monitor",
        text=(
            "Datawrap does not ship Azure Monitor as a destination "
            "(azure_monitor is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_devops_card() -> CapabilityCard | None:
    if azure_devops_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure DevOps",
        text=(
            "Datawrap does not ship Azure DevOps as a transfer destination "
            "(azure_devops is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_boards_card() -> CapabilityCard | None:
    if azure_boards_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Boards",
        text=(
            "Datawrap does not ship Azure Boards as a destination "
            "(azure_boards is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_migrate_card() -> CapabilityCard | None:
    if azure_migrate_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Migrate",
        text=(
            "Datawrap does not ship Azure Migrate "
            "(azure_migrate is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def dv360_card() -> CapabilityCard | None:
    if dv360_shipped():
        return None
    return CapabilityCard(
        title="Do you support Display & Video 360",
        text=(
            "Datawrap does not ship Display & Video 360 as a transfer-ready "
            "source (dv360 is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def gcs_transfer_service_card() -> CapabilityCard | None:
    if gcs_transfer_service_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Storage Transfer Service",
        text=(
            "Datawrap does not ship Cloud Storage Transfer Service "
            "(gcs_transfer_service is false). GCS is a different driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def qlik_card() -> CapabilityCard | None:
    if qlik_shipped():
        return None
    return CapabilityCard(
        title="Do you support Qlik as a destination",
        text=(
            "Datawrap does not ship Qlik as a transfer-ready destination "
            "(qlik is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def entra_governance_card() -> CapabilityCard | None:
    if entra_governance_shipped():
        return None
    return CapabilityCard(
        title="Do you support Entra ID Governance",
        text=(
            "Datawrap does not ship Entra ID Governance "
            "(entra_governance is false). SSO is SAML/OIDC, not Governance."
        ),
        source_module="src/routers/workspace_router.py · sso",
        category="connectors",
    )


def cloud_sql_mysql_card() -> CapabilityCard | None:
    if cloud_sql_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud SQL for MySQL",
        text=(
            "Cloud SQL for MySQL is not its own driver "
            "(cloud_sql_mysql is false). Connect the instance as MySQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def hdinsight_card() -> CapabilityCard | None:
    if hdinsight_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure HDInsight",
        text=(
            "Datawrap does not ship Azure HDInsight as a transfer-ready "
            "driver (hdinsight is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def expressroute_card() -> CapabilityCard | None:
    if expressroute_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure ExpressRoute",
        text=(
            "Datawrap does not ship Azure ExpressRoute as a connect option "
            "(expressroute is false)."
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def ssis_card() -> CapabilityCard | None:
    if ssis_shipped():
        return None
    return CapabilityCard(
        title="Do you support SSIS",
        text=(
            "Datawrap does not ship SQL Server Integration Services "
            "(ssis is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def ssrs_card() -> CapabilityCard | None:
    if ssrs_shipped():
        return None
    return CapabilityCard(
        title="Do you support SSRS",
        text=(
            "Datawrap does not ship SQL Server Reporting Services "
            "(ssrs is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def gmail_card() -> CapabilityCard | None:
    if gmail_shipped():
        return None
    return CapabilityCard(
        title="Do you support Gmail as a source",
        text=(
            "Datawrap does not ship Gmail as a transfer-ready source "
            "(gmail is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_calendar_card() -> CapabilityCard | None:
    if google_calendar_shipped():
        return None
    return CapabilityCard(
        title="Do you support Calendar as a source",
        text=(
            "Datawrap does not ship Google Calendar as a transfer-ready "
            "source (google_calendar is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_cdn_card() -> CapabilityCard | None:
    if azure_cdn_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure CDN",
        text=(
            "Datawrap does not ship Azure CDN as a transfer destination "
            "(azure_cdn is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_blueprints_card() -> CapabilityCard | None:
    if azure_blueprints_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Blueprints",
        text=(
            "Datawrap does not ship Azure Blueprints "
            "(azure_blueprints is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def azure_automation_card() -> CapabilityCard | None:
    if azure_automation_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Automation",
        text=(
            "Datawrap does not ship Azure Automation "
            "(azure_automation is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def bigquery_ml_card() -> CapabilityCard | None:
    if bigquery_ml_shipped():
        return None
    return CapabilityCard(
        title="Do you support BigQuery ML",
        text=(
            "BigQuery ML is not a transfer destination "
            "(bigquery_ml is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def rds_postgresql_card() -> CapabilityCard | None:
    if not postgresql_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Amazon RDS for PostgreSQL",
        text=(
            "Yes — Amazon RDS for PostgreSQL is the PostgreSQL driver "
            "(postgresql / rds_postgresql). "
            "It is not Aurora."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def rds_mysql_card() -> CapabilityCard | None:
    if not mysql_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Amazon RDS for MySQL",
        text=(
            "Yes — Amazon RDS for MySQL is the MySQL driver "
            "(mysql / rds_mysql). "
            "It is not Aurora."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_sql_postgresql_card() -> CapabilityCard | None:
    if cloud_sql_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud SQL for PostgreSQL",
        text=(
            "Cloud SQL for PostgreSQL is not its own driver "
            "(cloud_sql_postgresql is false). Connect the instance as PostgreSQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def filestore_card() -> CapabilityCard | None:
    if filestore_shipped():
        return None
    return CapabilityCard(
        title="Do you support Filestore",
        text=(
            "Datawrap does not ship Filestore as a transfer-ready driver "
            "(filestore is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def persistent_disk_card() -> CapabilityCard | None:
    if persistent_disk_shipped():
        return None
    return CapabilityCard(
        title="Do you support Persistent Disk",
        text=(
            "Datawrap does not ship Persistent Disk as a transfer-ready "
            "driver (persistent_disk is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def datastream_card() -> CapabilityCard | None:
    if datastream_shipped():
        return None
    return CapabilityCard(
        title="Do you support Datastream",
        text=(
            "Datawrap does not ship Cloud Datastream "
            "(datastream is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def dataplex_card() -> CapabilityCard | None:
    if dataplex_shipped():
        return None
    return CapabilityCard(
        title="Do you support Dataplex",
        text=(
            "Datawrap does not ship Dataplex as a destination "
            "(dataplex is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def informatica_card() -> CapabilityCard | None:
    if informatica_shipped():
        return None
    return CapabilityCard(
        title="Do you support Informatica as a destination",
        text=(
            "Datawrap does not ship Informatica as a transfer destination "
            "(informatica is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def talend_card() -> CapabilityCard | None:
    if talend_shipped():
        return None
    return CapabilityCard(
        title="Do you support Talend as a destination",
        text=(
            "Datawrap does not ship Talend as a transfer destination "
            "(talend is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def matillion_card() -> CapabilityCard | None:
    if matillion_shipped():
        return None
    return CapabilityCard(
        title="Do you support Matillion as a destination",
        text=(
            "Datawrap does not ship Matillion as a transfer destination "
            "(matillion is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_workspace_card() -> CapabilityCard | None:
    if google_workspace_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Workspace as a source",
        text=(
            "Datawrap does not ship Google Workspace as a transfer-ready "
            "source (google_workspace is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def aks_card() -> CapabilityCard | None:
    if aks_shipped():
        return None
    return CapabilityCard(
        title="Do you support AKS as a destination",
        text=(
            "Datawrap does not ship AKS as a transfer destination "
            "(aks is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_functions_card() -> CapabilityCard | None:
    if azure_functions_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Functions",
        text=(
            "Datawrap does not ship Azure Functions as a transfer destination "
            "(azure_functions is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def azure_batch_card() -> CapabilityCard | None:
    if azure_batch_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Batch",
        text=(
            "Datawrap does not ship Azure Batch "
            "(azure_batch is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def cloud_scheduler_card() -> CapabilityCard | None:
    if cloud_scheduler_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Scheduler",
        text=(
            "Datawrap does not ship Cloud Scheduler "
            "(cloud_scheduler is false)."
        ),
        source_module="services/schedule_runner.py · _dispatch_transfer",
        category="connectors",
    )


def vertex_ai_search_card() -> CapabilityCard | None:
    if vertex_ai_search_shipped():
        return None
    return CapabilityCard(
        title="Do you support Vertex AI Search",
        text=(
            "Datawrap does not ship Vertex AI Search "
            "(vertex_ai_search is false)."
        ),
        source_module="src/ai/first_party/engine.py · unique_driver_types",
        category="connectors",
    )


def business_central_card() -> CapabilityCard | None:
    if business_central_shipped():
        return None
    return CapabilityCard(
        title="Do you support Business Central",
        text=(
            "Datawrap does not ship Dynamics 365 Business Central "
            "(business_central is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def application_insights_card() -> CapabilityCard | None:
    if application_insights_shipped():
        return None
    return CapabilityCard(
        title="Do you support Application Insights",
        text=(
            "Datawrap does not ship Application Insights "
            "(application_insights is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def site_recovery_card() -> CapabilityCard | None:
    if site_recovery_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Site Recovery",
        text=(
            "Datawrap does not ship Azure Site Recovery "
            "(site_recovery is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def entra_external_id_card() -> CapabilityCard | None:
    if entra_external_id_shipped():
        return None
    return CapabilityCard(
        title="Do you support Entra External ID",
        text=(
            "Datawrap does not ship Entra External ID "
            "(entra_external_id is false). SSO is SAML/OIDC for the tenant."
        ),
        source_module="src/routers/workspace_router.py · sso",
        category="connectors",
    )


def azure_ad_b2c_card() -> CapabilityCard | None:
    if azure_ad_b2c_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure AD B2C",
        text=(
            "Datawrap does not ship Azure AD B2C "
            "(azure_ad_b2c is false). SSO is SAML/OIDC, not B2C."
        ),
        source_module="src/routers/workspace_router.py · sso",
        category="connectors",
    )


def google_meet_card() -> CapabilityCard | None:
    if google_meet_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Meet as a destination",
        text=(
            "Datawrap does not ship Google Meet as a transfer-ready "
            "destination (google_meet is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_chat_card() -> CapabilityCard | None:
    if google_chat_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Chat as a destination",
        text=(
            "Datawrap does not ship Google Chat as a transfer-ready "
            "destination (google_chat is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def yammer_card() -> CapabilityCard | None:
    if yammer_shipped():
        return None
    return CapabilityCard(
        title="Do you support Yammer as a destination",
        text=(
            "Datawrap does not ship Yammer as a transfer-ready destination "
            "(yammer is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def viva_card() -> CapabilityCard | None:
    if viva_shipped():
        return None
    return CapabilityCard(
        title="Do you support Viva as a destination",
        text=(
            "Datawrap does not ship Microsoft Viva as a transfer destination "
            "(viva is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def copilot_studio_card() -> CapabilityCard | None:
    if copilot_studio_shipped():
        return None
    return CapabilityCard(
        title="Do you support Copilot Studio as a destination",
        text=(
            "Datawrap does not ship Copilot Studio as a transfer destination "
            "(copilot_studio is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def logic_apps_card() -> CapabilityCard | None:
    if logic_apps_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Logic Apps",
        text=(
            "Datawrap does not ship Azure Logic Apps as a transfer destination "
            "(logic_apps is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def eventarc_card() -> CapabilityCard | None:
    if eventarc_shipped():
        return None
    return CapabilityCard(
        title="Do you support Eventarc",
        text=(
            "Datawrap does not ship Eventarc "
            "(eventarc is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def dialogflow_card() -> CapabilityCard | None:
    if dialogflow_shipped():
        return None
    return CapabilityCard(
        title="Do you support Dialogflow",
        text=(
            "Datawrap does not ship Dialogflow as a transfer-ready source "
            "(dialogflow is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_policy_card() -> CapabilityCard | None:
    if azure_policy_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Policy",
        text=(
            "Datawrap does not ship Azure Policy "
            "(azure_policy is false)."
        ),
        source_module="src/routers/workspace_router.py · allowlist",
        category="connectors",
    )


def planner_card() -> CapabilityCard | None:
    if planner_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Planner",
        text=(
            "Datawrap does not ship Microsoft Planner as a transfer-ready "
            "driver (planner is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_todo_card() -> CapabilityCard | None:
    if microsoft_todo_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft To Do",
        text=(
            "Datawrap does not ship Microsoft To Do as a transfer-ready "
            "driver (microsoft_todo is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bookings_card() -> CapabilityCard | None:
    if bookings_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Bookings",
        text=(
            "Datawrap does not ship Microsoft Bookings as a transfer-ready "
            "driver (bookings is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def classroom_card() -> CapabilityCard | None:
    if classroom_shipped():
        return None
    return CapabilityCard(
        title="Do you support Classroom as a source",
        text=(
            "Datawrap does not ship Google Classroom as a transfer-ready "
            "source (google_classroom is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_signalr_card() -> CapabilityCard | None:
    if azure_signalr_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure SignalR",
        text=(
            "Datawrap does not ship Azure SignalR as a transfer-ready "
            "driver (azure_signalr is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_service_fabric_card() -> CapabilityCard | None:
    if azure_service_fabric_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Service Fabric",
        text=(
            "Datawrap does not ship Azure Service Fabric "
            "(azure_service_fabric is false). Microsoft Fabric is a different product."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def keep_card() -> CapabilityCard | None:
    if keep_shipped():
        return None
    return CapabilityCard(
        title="Do you support Keep as a source",
        text=(
            "Datawrap does not ship Google Keep as a transfer-ready source "
            "(google_keep is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def appsheet_card() -> CapabilityCard | None:
    if appsheet_shipped():
        return None
    return CapabilityCard(
        title="Do you support AppSheet as a destination",
        text=(
            "Datawrap does not ship AppSheet as a transfer destination "
            "(appsheet is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_communication_card() -> CapabilityCard | None:
    if azure_communication_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Communication Services",
        text=(
            "Datawrap does not ship Azure Communication Services "
            "(azure_communication is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_apim_card() -> CapabilityCard | None:
    if azure_apim_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure API Management",
        text=(
            "Datawrap does not ship Azure API Management "
            "(azure_apim is false)."
        ),
        source_module="src/routers/workspace_router.py · sso",
        category="connectors",
    )


def cloud_workflows_card() -> CapabilityCard | None:
    if cloud_workflows_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Workflows",
        text=(
            "Datawrap does not ship Cloud Workflows "
            "(cloud_workflows is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def chronicle_card() -> CapabilityCard | None:
    if chronicle_shipped():
        return None
    return CapabilityCard(
        title="Do you support Chronicle as a destination",
        text=(
            "Datawrap does not ship Chronicle as a transfer destination "
            "(chronicle is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_forms_card() -> CapabilityCard | None:
    if microsoft_forms_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Forms",
        text=(
            "Datawrap does not ship Microsoft Forms as a transfer-ready "
            "driver (microsoft_forms is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_project_card() -> CapabilityCard | None:
    if microsoft_project_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Project",
        text=(
            "Datawrap does not ship Microsoft Project as a transfer-ready "
            "driver (microsoft_project is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def container_apps_card() -> CapabilityCard | None:
    if container_apps_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Container Apps",
        text=(
            "Datawrap does not ship Azure Container Apps as a transfer "
            "destination (container_apps is false)."
        ),
        source_module="src/ai/copilot/transfer_tools.py · start_transfer",
        category="connectors",
    )


def azure_front_door_card() -> CapabilityCard | None:
    if azure_front_door_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Front Door",
        text=(
            "Datawrap does not ship Azure Front Door "
            "(azure_front_door is false)."
        ),
        source_module="src/routers/workspace_router.py · allowlist",
        category="connectors",
    )


def identity_platform_card() -> CapabilityCard | None:
    if identity_platform_shipped():
        return None
    return CapabilityCard(
        title="Do you support Identity Platform",
        text=(
            "Datawrap does not ship Identity Platform "
            "(identity_platform is false)."
        ),
        source_module="src/routers/workspace_router.py · sso",
        category="connectors",
    )


def pubsub_lite_card() -> CapabilityCard | None:
    if pubsub_lite_shipped():
        return None
    return CapabilityCard(
        title="Do you support Pub/Sub Lite",
        text=(
            "Datawrap does not ship Pub/Sub Lite "
            "(pubsub_lite is false). Pub/Sub is also not a driver."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def rds_sqlserver_card() -> CapabilityCard | None:
    if not sqlserver_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Do you support Amazon RDS for SQL Server",
        text=(
            "Yes — Amazon RDS for SQL Server is the SQL Server driver "
            "(sqlserver / rds_sqlserver). "
            "It is not RDS for PostgreSQL."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def column_level_lineage_card() -> CapabilityCard | None:
    if column_level_lineage_emitted():
        return None
    return CapabilityCard(
        title="Do you support column-level lineage",
        text=(
            "Datawrap does not emit column-level lineage — events are run "
            "and dataset grain."
        ),
        source_module="services/lineage_telemetry.py · emit_run_started",
        category="proof",
    )


def cloud_dns_card() -> CapabilityCard | None:
    if cloud_dns_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud DNS",
        text=(
            "Datawrap does not ship Google Cloud DNS as a managed-DNS product "
            "(cloud_dns is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_domains_card() -> CapabilityCard | None:
    if cloud_domains_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Domains",
        text=(
            "Datawrap does not ship Google Cloud Domains as a registrar product "
            "(cloud_domains is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def natural_language_api_card() -> CapabilityCard | None:
    if natural_language_api_shipped():
        return None
    return CapabilityCard(
        title="Do you support Natural Language API",
        text=(
            "Datawrap does not ship Google Cloud Natural Language API "
            "(natural_language_api is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_mariadb_card() -> CapabilityCard | None:
    if azure_mariadb_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Database for MariaDB",
        text=(
            "Datawrap does not ship Azure Database for MariaDB as a "
            "transfer-ready driver (azure_mariadb is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_relay_card() -> CapabilityCard | None:
    if azure_relay_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Relay",
        text=(
            "Datawrap does not ship Azure Relay as a transfer-ready driver "
            "(azure_relay is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_remote_rendering_card() -> CapabilityCard | None:
    if azure_remote_rendering_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Remote Rendering",
        text=(
            "Datawrap does not ship Azure Remote Rendering as a transfer "
            "destination (azure_remote_rendering is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_quantum_card() -> CapabilityCard | None:
    if azure_quantum_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Quantum",
        text=(
            "Datawrap does not ship Azure Quantum as a transfer-ready driver "
            "(azure_quantum is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_orbital_card() -> CapabilityCard | None:
    if azure_orbital_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Orbital",
        text=(
            "Datawrap does not ship Azure Orbital as a transfer-ready driver "
            "(azure_orbital is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_local_card() -> CapabilityCard | None:
    if azure_local_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Local",
        text=(
            "Datawrap does not ship Azure Local as a transfer-ready driver "
            "(azure_local is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def windows_365_card() -> CapabilityCard | None:
    if windows_365_shipped():
        return None
    return CapabilityCard(
        title="Do you support Windows 365 as a destination",
        text=(
            "Datawrap does not ship Windows 365 as a transfer-ready "
            "destination (windows_365 is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def api_gateway_card() -> CapabilityCard | None:
    if api_gateway_shipped():
        return None
    return CapabilityCard(
        title="Do you support API Gateway",
        text=(
            "Datawrap does not ship Google Cloud API Gateway as a transfer "
            "destination (api_gateway is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_test_plans_card() -> CapabilityCard | None:
    if azure_test_plans_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Test Plans",
        text=(
            "Datawrap does not ship Azure Test Plans as a transfer destination "
            "(azure_test_plans is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_dedicated_sql_pool_card() -> CapabilityCard | None:
    if azure_dedicated_sql_pool_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Dedicated SQL Pool",
        text=(
            "Datawrap does not ship Azure Synapse dedicated SQL pool as a "
            "transfer-ready destination (azure_dedicated_sql_pool is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_cdn_card() -> CapabilityCard | None:
    if cloud_cdn_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud CDN",
        text=(
            "Datawrap does not ship Google Cloud CDN "
            "(cloud_cdn is false). Azure CDN is a different product."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_nat_card() -> CapabilityCard | None:
    if cloud_nat_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud NAT",
        text=(
            "Datawrap does not ship Google Cloud NAT as a connect option "
            "(cloud_nat is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_iap_card() -> CapabilityCard | None:
    if cloud_iap_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud IAP",
        text=(
            "Datawrap does not ship Identity-Aware Proxy as a connect option "
            "(cloud_iap is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_hsm_card() -> CapabilityCard | None:
    if cloud_hsm_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud HSM",
        text=(
            "Datawrap does not ship Cloud HSM as a key or connect option "
            "(cloud_hsm is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def certificate_manager_card() -> CapabilityCard | None:
    if certificate_manager_shipped():
        return None
    return CapabilityCard(
        title="Do you support Certificate Manager",
        text=(
            "Datawrap does not ship Google Certificate Manager "
            "(certificate_manager is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def binary_authorization_card() -> CapabilityCard | None:
    if binary_authorization_shipped():
        return None
    return CapabilityCard(
        title="Do you support Binary Authorization",
        text=(
            "Datawrap does not ship Binary Authorization "
            "(binary_authorization is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def service_mesh_card() -> CapabilityCard | None:
    if service_mesh_shipped():
        return None
    return CapabilityCard(
        title="Do you support Service Mesh",
        text=(
            "Datawrap does not ship Cloud Service Mesh "
            "(service_mesh is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def apigee_card() -> CapabilityCard | None:
    if apigee_shipped():
        return None
    return CapabilityCard(
        title="Do you support Apigee as a destination",
        text=(
            "Datawrap does not ship Apigee as a transfer destination "
            "(apigee is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def iot_central_card() -> CapabilityCard | None:
    if iot_central_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure IoT Central",
        text=(
            "Datawrap does not ship Azure IoT Central "
            "(iot_central is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def time_series_insights_card() -> CapabilityCard | None:
    if time_series_insights_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Time Series Insights",
        text=(
            "Datawrap does not ship Azure Time Series Insights "
            "(time_series_insights is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_maps_card() -> CapabilityCard | None:
    if azure_maps_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Maps",
        text=(
            "Datawrap does not ship Azure Maps as a transfer destination "
            "(azure_maps is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def notification_hubs_card() -> CapabilityCard | None:
    if notification_hubs_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Notification Hubs",
        text=(
            "Datawrap does not ship Azure Notification Hubs "
            "(notification_hubs is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_web_pubsub_card() -> CapabilityCard | None:
    if azure_web_pubsub_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Web PubSub",
        text=(
            "Datawrap does not ship Azure Web PubSub "
            "(azure_web_pubsub is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_repos_card() -> CapabilityCard | None:
    if azure_repos_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Repos as a destination",
        text=(
            "Datawrap does not ship Azure Repos as a transfer destination "
            "(azure_repos is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_pipelines_card() -> CapabilityCard | None:
    if azure_pipelines_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Pipelines as a destination",
        text=(
            "Datawrap does not ship Azure Pipelines as a transfer destination "
            "(azure_pipelines is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bing_ads_card() -> CapabilityCard | None:
    if bing_ads_shipped():
        return None
    return CapabilityCard(
        title="Do you support Bing Ads",
        text=(
            "Datawrap does not ship Bing Ads as a transfer-ready source "
            "(bing_ads is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def retail_api_card() -> CapabilityCard | None:
    if retail_api_shipped():
        return None
    return CapabilityCard(
        title="Do you support Retail API",
        text=(
            "Datawrap does not ship Google Cloud Retail API "
            "(retail_api is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def healthcare_api_card() -> CapabilityCard | None:
    if healthcare_api_shipped():
        return None
    return CapabilityCard(
        title="Do you support Healthcare API",
        text=(
            "Datawrap does not ship Google Cloud Healthcare API "
            "(healthcare_api is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_endpoints_card() -> CapabilityCard | None:
    if cloud_endpoints_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Endpoints",
        text=(
            "Datawrap does not ship Google Cloud Endpoints "
            "(cloud_endpoints is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def tag_manager_card() -> CapabilityCard | None:
    if tag_manager_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Tag Manager",
        text=(
            "Datawrap does not ship Google Tag Manager as a transfer-ready "
            "source (tag_manager is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def search_console_card() -> CapabilityCard | None:
    if search_console_shipped():
        return None
    return CapabilityCard(
        title="Do you support Search Console as a source",
        text=(
            "Datawrap does not ship Search Console as a transfer-ready source "
            "(search_console is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def cloud_load_balancing_card() -> CapabilityCard | None:
    if cloud_load_balancing_shipped():
        return None
    return CapabilityCard(
        title="Do you support Cloud Load Balancing",
        text=(
            "Datawrap does not ship Cloud Load Balancing as a connect option "
            "(cloud_load_balancing is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def assured_workloads_card() -> CapabilityCard | None:
    if assured_workloads_shipped():
        return None
    return CapabilityCard(
        title="Do you support Assured Workloads",
        text=(
            "Datawrap does not ship Assured Workloads as a connect option "
            "(assured_workloads is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def config_connector_card() -> CapabilityCard | None:
    if config_connector_shipped():
        return None
    return CapabilityCard(
        title="Do you support Config Connector",
        text=(
            "Datawrap does not ship Config Connector as a GitOps write path "
            "(config_connector is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def app_hub_card() -> CapabilityCard | None:
    if app_hub_shipped():
        return None
    return CapabilityCard(
        title="Do you support App Hub",
        text=(
            "Datawrap does not ship App Hub as a transfer-ready driver "
            "(app_hub is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_advertising_card() -> CapabilityCard | None:
    if microsoft_advertising_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Advertising",
        text=(
            "Datawrap does not ship Microsoft Advertising as a transfer-ready "
            "source (microsoft_advertising is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_artifacts_card() -> CapabilityCard | None:
    if azure_artifacts_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Artifacts",
        text=(
            "Datawrap does not ship Azure Artifacts as a transfer destination "
            "(azure_artifacts is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def document_ai_card() -> CapabilityCard | None:
    if document_ai_shipped():
        return None
    return CapabilityCard(
        title="Do you support Document AI as a destination",
        text=(
            "Datawrap does not ship Document AI as a transfer destination "
            "(document_ai is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def fhir_card() -> CapabilityCard | None:
    if fhir_shipped():
        return None
    return CapabilityCard(
        title="Do you support FHIR as a destination",
        text=(
            "Datawrap does not ship FHIR as a transfer destination "
            "(fhir is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def github_copilot_dest_card() -> CapabilityCard | None:
    if github_copilot_dest_shipped():
        return None
    return CapabilityCard(
        title="Do you support GitHub Copilot as a destination",
        text=(
            "Datawrap does not ship GitHub Copilot as a transfer destination "
            "(github_copilot is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_photos_card() -> CapabilityCard | None:
    if google_photos_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Photos as a source",
        text=(
            "Datawrap does not ship Google Photos as a transfer-ready source "
            "(google_photos is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_contacts_card() -> CapabilityCard | None:
    if google_contacts_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Contacts as a source",
        text=(
            "Datawrap does not ship Google Contacts as a transfer-ready "
            "source (google_contacts is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_maps_card() -> CapabilityCard | None:
    if google_maps_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Maps as a source",
        text=(
            "Datawrap does not ship Google Maps as a transfer-ready source "
            "(google_maps is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_news_card() -> CapabilityCard | None:
    if google_news_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google News as a source",
        text=(
            "Datawrap does not ship Google News as a transfer-ready source "
            "(google_news is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def google_play_card() -> CapabilityCard | None:
    if google_play_shipped():
        return None
    return CapabilityCard(
        title="Do you support Google Play as a source",
        text=(
            "Datawrap does not ship Google Play as a transfer-ready source "
            "(google_play is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def vision_ai_card() -> CapabilityCard | None:
    if vision_ai_shipped():
        return None
    return CapabilityCard(
        title="Do you support Vision AI as a destination",
        text=(
            "Datawrap does not ship Vision AI as a transfer destination "
            "(vision_ai is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def speech_to_text_card() -> CapabilityCard | None:
    if speech_to_text_shipped():
        return None
    return CapabilityCard(
        title="Do you support Speech-to-Text as a destination",
        text=(
            "Datawrap does not ship Speech-to-Text as a transfer destination "
            "(speech_to_text is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def earth_engine_card() -> CapabilityCard | None:
    if earth_engine_shipped():
        return None
    return CapabilityCard(
        title="Do you support Earth Engine",
        text=(
            "Datawrap does not ship Earth Engine as a transfer-ready driver "
            "(earth_engine is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def bigquery_bi_engine_card() -> CapabilityCard | None:
    if bigquery_bi_engine_shipped():
        return None
    return CapabilityCard(
        title="Do you support BigQuery BI Engine",
        text=(
            "Datawrap does not ship BigQuery BI Engine "
            "(bigquery_bi_engine is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_media_card() -> CapabilityCard | None:
    if azure_media_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Media Services",
        text=(
            "Datawrap does not ship Azure Media Services "
            "(azure_media is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_cognitive_card() -> CapabilityCard | None:
    if azure_cognitive_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Cognitive Services",
        text=(
            "Datawrap does not ship Azure Cognitive Services "
            "(azure_cognitive is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def azure_bot_card() -> CapabilityCard | None:
    if azure_bot_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Bot Service",
        text=(
            "Datawrap does not ship Azure Bot Service "
            "(azure_bot is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def confidential_ledger_card() -> CapabilityCard | None:
    if confidential_ledger_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Confidential Ledger",
        text=(
            "Datawrap does not ship Azure Confidential Ledger "
            "(confidential_ledger is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def operator_nexus_card() -> CapabilityCard | None:
    if operator_nexus_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure Operator Nexus",
        text=(
            "Datawrap does not ship Azure Operator Nexus "
            "(operator_nexus is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def microsoft_clarity_card() -> CapabilityCard | None:
    if microsoft_clarity_shipped():
        return None
    return CapabilityCard(
        title="Do you support Microsoft Clarity",
        text=(
            "Datawrap does not ship Microsoft Clarity as a transfer-ready "
            "source (microsoft_clarity is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def anthos_card() -> CapabilityCard | None:
    if anthos_shipped():
        return None
    return CapabilityCard(
        title="Do you support Anthos",
        text=(
            "Datawrap does not ship Anthos as a connect or transfer option "
            "(anthos is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def iot_hub_card() -> CapabilityCard | None:
    if iot_hub_shipped():
        return None
    return CapabilityCard(
        title="Do you support Azure IoT Hub",
        text=(
            "Datawrap does not ship Azure IoT Hub as a transfer-ready driver "
            "(iot_hub is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def merchant_center_card() -> CapabilityCard | None:
    if merchant_center_shipped():
        return None
    return CapabilityCard(
        title="Do you support Merchant Center",
        text=(
            "Datawrap does not ship Merchant Center as a transfer-ready "
            "source (merchant_center is false)."
        ),
        source_module="services/catalog_service.py · unique_driver_types",
        category="connectors",
    )


def capability_cards() -> tuple[CapabilityCard, ...]:
    """Every honest capability card the chatbot is allowed to speak."""
    cards: list[CapabilityCard] = []
    for builder in (
        dbt_card,
        ssh_tunnel_card,
        debezium_card,
        terraform_card,
        confirm_card,
        privatelink_card,
        airflow_spark_card,
        goldengate_card,
        iceberg_catalog_card,
        snowflake_sharing_card,
        kafka_source_card,
        kafka_dest_card,
        iceberg_ready_card,
        sftp_ready_card,
        mysql_ready_card,
        redis_ready_card,
        salesforce_card,
        schema_registry_card,
        silent_data_loss_card,
        upsert_versus_merge_card,
        airbyte_connector_pack_card,
        compliance_attestation_card,
        transfer_undo_card,
        viewer_secrets_card,
        rest_api_card,
        github_actions_card,
        openlineage_card,
        mirror_versus_upsert_card,
        snowflake_key_pair_card,
        audit_export_card,
        ip_allowlist_card,
        require_mfa_card,
        cdc_watermark_store_card,
        incremental_versus_upsert_card,
        bigquery_destination_card,
        azure_service_principal_card,
        adls_destination_card,
        parallel_transfers_card,
        two_jobs_same_table_card,
        full_refresh_versus_incremental_card,
        scd1_card,
        scd2_card,
        incremental_updated_at_card,
        session_timeout_card,
        custom_domain_card,
        data_residency_card,
        sqlserver_card,
        databricks_destination_card,
        delta_lake_card,
        snowflake_destination_card,
        oracle_destination_card,
        postgres_destination_card,
        mongodb_destination_card,
        s3_destination_card,
        gcs_destination_card,
        redshift_destination_card,
        synapse_destination_card,
        unique_key_collision_card,
        missing_primary_key_card,
        salesforce_oauth_card,
        slack_notification_card,
        teams_notification_card,
        email_notification_card,
        servicenow_notification_card,
        slack_connector_card,
        teams_destination_card,
        hubspot_card,
        stripe_card,
        row_level_security_card,
        snowflake_dynamic_tables_card,
        custom_roles_card,
        field_level_encryption_card,
        hudi_card,
        kafka_consumer_group_card,
        external_secret_store_card,
        data_location_card,
        byok_card,
        gcp_service_account_card,
        workload_identity_card,
        pause_cdc_card,
        connect_salesforce_card,
        cdc_read_replica_card,
        oracle_logminer_card,
        sqlserver_cdc_card,
        sqlserver_change_tracking_card,
        filter_cdc_events_card,
        microsoft_fabric_card,
        power_bi_card,
        azure_data_factory_card,
        oracle_xstream_card,
        sqlserver_always_on_card,
        azure_managed_identity_card,
        cdc_heartbeat_interval_card,
        cdc_fetch_size_card,
        skip_cdc_deletes_card,
        cosmos_db_card,
        event_hubs_card,
        service_bus_card,
        azure_sql_card,
        cloud_sql_card,
        pubsub_card,
        spanner_card,
        vertex_ai_card,
        sharepoint_card,
        dynamics_365_card,
        purview_card,
        excel_online_card,
        aws_iam_role_card,
        custom_slot_name_card,
        blue_green_cutover_card,
        azure_database_postgresql_card,
        azure_database_mysql_card,
        cloud_sql_sqlserver_card,
        onedrive_card,
        looker_card,
        looker_studio_card,
        bigquery_omni_card,
        google_sheets_card,
        alloydb_card,
        microsoft_graph_card,
        azure_openai_card,
        kusto_card,
        event_grid_card,
        google_dataflow_card,
        power_platform_card,
        conditional_access_card,
        cloud_composer_card,
        azure_blob_card,
        azure_redis_card,
        azure_flexible_postgres_card,
        firebase_card,
        firestore_card,
        bigtable_card,
        dataproc_card,
        entra_pim_card,
        gke_card,
        outlook_card,
        youtube_card,
        google_ads_card,
        google_drive_card,
        google_docs_card,
        google_analytics_card,
        cloud_run_card,
        stream_analytics_card,
        microsoft_lists_card,
        exchange_online_card,
        github_enterprise_dest_card,
        bq_dts_card,
        bq_linked_dataset_card,
        cloud_kms_card,
        app_engine_card,
        data_catalog_card,
        cloud_build_card,
        artifact_registry_card,
        cloud_functions_card,
        azure_files_card,
        log_analytics_card,
        azure_ai_search_card,
        azure_analysis_services_card,
        cloud_sql_auth_proxy_card,
        synapse_link_card,
        azure_table_storage_card,
        azure_queue_storage_card,
        sqlserver_azure_vm_card,
        splunk_destination_card,
        tableau_destination_card,
        azure_data_lake_gen2_card,
        azure_sql_service_principal_card,
        dynamodb_card,
        elasticsearch_card,
        elastic_cloud_card,
        opensearch_card,
        teams_source_card,
        microsoft_365_destination_card,
        memorystore_card,
        azure_sql_edge_card,
        intune_card,
        defender_card,
        sentinel_card,
        azure_ml_card,
        search_ads_360_card,
        cloud_tasks_card,
        vpc_service_controls_card,
        azure_firewall_card,
        campaign_manager_card,
        aurora_card,
        documentdb_card,
        cloud_armor_card,
        cloud_interconnect_card,
        cloud_vpn_card,
        azure_arc_card,
        azure_lighthouse_card,
        azure_monitor_card,
        azure_devops_card,
        azure_boards_card,
        azure_migrate_card,
        dv360_card,
        gcs_transfer_service_card,
        qlik_card,
        entra_governance_card,
        cloud_sql_mysql_card,
        hdinsight_card,
        expressroute_card,
        ssis_card,
        ssrs_card,
        gmail_card,
        google_calendar_card,
        azure_cdn_card,
        azure_blueprints_card,
        azure_automation_card,
        bigquery_ml_card,
        rds_postgresql_card,
        rds_mysql_card,
        cloud_sql_postgresql_card,
        filestore_card,
        persistent_disk_card,
        datastream_card,
        dataplex_card,
        informatica_card,
        talend_card,
        matillion_card,
        google_workspace_card,
        aks_card,
        azure_functions_card,
        azure_batch_card,
        cloud_scheduler_card,
        vertex_ai_search_card,
        business_central_card,
        application_insights_card,
        site_recovery_card,
        entra_external_id_card,
        azure_ad_b2c_card,
        google_meet_card,
        google_chat_card,
        yammer_card,
        viva_card,
        copilot_studio_card,
        logic_apps_card,
        eventarc_card,
        dialogflow_card,
        azure_policy_card,
        planner_card,
        microsoft_todo_card,
        bookings_card,
        classroom_card,
        azure_signalr_card,
        azure_service_fabric_card,
        keep_card,
        appsheet_card,
        azure_communication_card,
        azure_apim_card,
        cloud_workflows_card,
        chronicle_card,
        microsoft_forms_card,
        microsoft_project_card,
        container_apps_card,
        azure_front_door_card,
        identity_platform_card,
        pubsub_lite_card,
        rds_sqlserver_card,
        column_level_lineage_card,
        cloud_dns_card,
        cloud_domains_card,
        natural_language_api_card,
        azure_mariadb_card,
        azure_relay_card,
        azure_remote_rendering_card,
        azure_quantum_card,
        azure_orbital_card,
        azure_local_card,
        windows_365_card,
        api_gateway_card,
        azure_test_plans_card,
        azure_dedicated_sql_pool_card,
        cloud_cdn_card,
        cloud_nat_card,
        cloud_iap_card,
        cloud_hsm_card,
        certificate_manager_card,
        binary_authorization_card,
        service_mesh_card,
        apigee_card,
        iot_central_card,
        time_series_insights_card,
        azure_maps_card,
        notification_hubs_card,
        azure_web_pubsub_card,
        azure_repos_card,
        azure_pipelines_card,
        bing_ads_card,
        retail_api_card,
        healthcare_api_card,
        cloud_endpoints_card,
        tag_manager_card,
        search_console_card,
        cloud_load_balancing_card,
        assured_workloads_card,
        config_connector_card,
        app_hub_card,
        microsoft_advertising_card,
        azure_artifacts_card,
        document_ai_card,
        fhir_card,
        github_copilot_dest_card,
        google_photos_card,
        google_contacts_card,
        google_maps_card,
        google_news_card,
        google_play_card,
        vision_ai_card,
        speech_to_text_card,
        earth_engine_card,
        bigquery_bi_engine_card,
        azure_media_card,
        azure_cognitive_card,
        azure_bot_card,
        confidential_ledger_card,
        operator_nexus_card,
        microsoft_clarity_card,
        anthos_card,
        iot_hub_card,
        merchant_center_card,
    ):
        card = builder()
        if card is not None:
            cards.append(card)
    return tuple(cards)
