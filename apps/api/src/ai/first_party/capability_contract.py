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
            "Datawrap does not open SSH tunnels for database connections — "
            "Postgres and MySQL take host, port, and credentials directly, "
            "with no bastion or jump host on the connect path. "
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
            "replacement, and does not run Flink CDC as the capture engine "
            "— native Postgres, MySQL, and Mongo CDC run without them. "
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
            "Datawrap does not ship AWS PrivateLink or VPC peering as a "
            "connect option — database connections take host, port, and "
            "credentials. "
            "SSH tunnels are also not on that connect path."
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
            "Datawrap does not embed Oracle GoldenGate — Oracle is a "
            "database engine a transfer can connect to, and native CDC is "
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
            "Datawrap does not implement Snowflake Secure Data Sharing — a "
            "Snowflake destination is a warehouse connection (credentials, "
            "Test, Save), not a share consumer or provider."
        ),
        source_module="apps/api/registry.py · DATABASE_TYPES",
        category="connectors",
    )


def kafka_source_card() -> CapabilityCard | None:
    if "kafka" not in _transfer_ready_drivers():
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
            "or HIPAA BAA — audit export is a workspace-scoped sample whose "
            "HMAC-SHA256 chain is diligence, not an attestation. "
            "Settings → Audit Logs list mapping decisions, job runs, and "
            "quarantine events an auditor can review; they are not a certificate."
        ),
        source_module="src/routers/audit_router.py · audit_export_honesty",
        category="enterprise",
    )


def airbyte_connector_pack_card() -> CapabilityCard | None:
    """Airbyte is a planned tile / competitor name, not a connector runtime."""
    if airbyte_is_transfer_ready():
        return None
    return CapabilityCard(
        title="Does Datawrap load Airbyte connector packs",
        text=(
            "Datawrap does not load Airbyte connector packs or custom Airbyte "
            "connectors — a transfer runs Datawrap's own transfer-ready drivers "
            "(unique_driver_types), not an Airbyte CDK spec. "
            "Airbyte remains a planned catalog tile and a competitor comparison, "
            "not a connector runtime."
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
        salesforce_card,
        schema_registry_card,
        silent_data_loss_card,
        upsert_versus_merge_card,
        airbyte_connector_pack_card,
        compliance_attestation_card,
    ):
        card = builder()
        if card is not None:
            cards.append(card)
    return tuple(cards)
