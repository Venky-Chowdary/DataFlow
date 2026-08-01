"""Reverse-ETL destination architecture (activation / operational sync).

DataFlow's primary path is source→warehouse. Reverse-ETL flips that: curated
warehouse tables activate into operational SaaS systems (CRM, support, etc.).

This module defines the contract and a registry of activation adapters. Concrete
SaaS writers land as connectors; the transfer engine routes ``sync_mode=reverse_etl``
through :func:`plan_activation` before the standard write path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ActivationPlan:
    """Declarative plan for a reverse-ETL run."""

    destination_kind: str
    object_name: str
    primary_key: list[str]
    field_map: dict[str, str] = field(default_factory=dict)
    mode: str = "upsert"  # upsert | insert | update
    batch_size: int = 200
    notes: list[str] = field(default_factory=list)


# Registry of destination_kind → planner callables (optional specialization).
_ACTIVATION_PLANNERS: dict[str, Callable[..., ActivationPlan]] = {}


def register_activation_planner(kind: str, planner: Callable[..., ActivationPlan]) -> None:
    _ACTIVATION_PLANNERS[kind.strip().lower()] = planner


def plan_activation(
    *,
    destination_kind: str,
    object_name: str,
    primary_key: str | list[str],
    field_map: dict[str, str] | None = None,
    mode: str = "upsert",
) -> ActivationPlan:
    """Build an activation plan; unknown kinds still get a generic upsert plan."""
    kind = (destination_kind or "").strip().lower()
    pk = (
        [c.strip() for c in primary_key if c and str(c).strip()]
        if isinstance(primary_key, list)
        else [p.strip() for p in str(primary_key or "").split(",") if p.strip()]
    )
    if not pk:
        raise ValueError("reverse-ETL requires a primary_key for idempotent activation")
    if not object_name:
        raise ValueError("reverse-ETL requires an object_name (table/collection/resource)")

    planner = _ACTIVATION_PLANNERS.get(kind)
    if planner:
        return planner(
            destination_kind=kind,
            object_name=object_name,
            primary_key=pk,
            field_map=field_map or {},
            mode=mode,
        )

    notes = []
    if kind in {"pgvector", "qdrant", "weaviate", "pinecone", "milvus"}:
        notes.append("vector destinations use embedding writers; treat as RAG activation")
    elif kind in {"postgresql", "mysql", "sqlserver", "snowflake", "bigquery"}:
        notes.append("SQL operational sync via upsert writers (warehouse→OLTP)")
    else:
        notes.append(f"Generic reverse-ETL plan for kind={kind}; SaaS-specific adapter pending")

    return ActivationPlan(
        destination_kind=kind,
        object_name=object_name,
        primary_key=pk,
        field_map=dict(field_map or {}),
        mode=mode or "upsert",
        notes=notes,
    )


def _plan_salesforce(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "Salesforce Collections upsert (External Id or Id)",
        "Failed records quarantine via rejected_details — never silently dropped",
        "Batch size capped at 200 per Composite/sObjects call",
    ]
    return ActivationPlan(
        destination_kind="salesforce",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=200,
        notes=notes,
    )


def _plan_hubspot(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "HubSpot CRM batch upsert by idProperty (default email)",
        "Failed records quarantine via rejected_details — never silently dropped",
        "Batch size capped at 100 per CRM batch/upsert call",
    ]
    return ActivationPlan(
        destination_kind="hubspot",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=100,
        notes=notes,
    )


def _plan_zendesk(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "Zendesk Support API create/update (tickets, users, orgs)",
        "tagger/multiselect use option tag values — not display names",
        "Failed records quarantine via rejected_details — never silently dropped",
        "Batch size capped at 100 per request group",
    ]
    return ActivationPlan(
        destination_kind="zendesk",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=100,
        notes=notes,
    )


def _plan_notion(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "Notion database page create/update — property types from live schema",
        "select/status/multi_select quarantine unknown option names",
        "Failed records quarantine via rejected_details — never silently dropped",
        "Batch size capped at 25 (Notion rate limits)",
    ]
    return ActivationPlan(
        destination_kind="notion",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=25,
        notes=notes,
    )


def _plan_airtable(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "Airtable REST batch create/upsert (10 records/request)",
        "singleSelect/multipleSelects use choice names from Meta schema",
        "Failed records quarantine via rejected_details — never silently dropped",
    ]
    return ActivationPlan(
        destination_kind="airtable",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=10,
        notes=notes,
    )


def _plan_stripe(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "Stripe API object create/update (customers, products, prices, …)",
        "Closed enums (tax_exempt, billing_scheme, coupon duration) fail-closed",
        "Failed records quarantine via rejected_details — never silently dropped",
        "Batch size capped at 100",
    ]
    return ActivationPlan(
        destination_kind="stripe",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=100,
        notes=notes,
    )


def _plan_shopify(**kwargs: Any) -> ActivationPlan:
    pk = kwargs["primary_key"]
    notes = [
        "Shopify Admin REST/GraphQL upsert (customers, products, variants, …)",
        "Metafield list.*/money/measurement polarity via shopify_metafield_type_to_carrier",
        "Failed records quarantine via rejected_details — never silently dropped",
        "Batch size capped at 50 (Shopify REST rate limits)",
    ]
    return ActivationPlan(
        destination_kind="shopify",
        object_name=kwargs["object_name"],
        primary_key=pk,
        field_map=dict(kwargs.get("field_map") or {}),
        mode=kwargs.get("mode") or "upsert",
        batch_size=50,
        notes=notes,
    )


# Register first-class SaaS activation planners at import time.
register_activation_planner("salesforce", _plan_salesforce)
register_activation_planner("hubspot", _plan_hubspot)
register_activation_planner("zendesk", _plan_zendesk)
register_activation_planner("notion", _plan_notion)
register_activation_planner("airtable", _plan_airtable)
register_activation_planner("stripe", _plan_stripe)
register_activation_planner("shopify", _plan_shopify)


def supported_activation_kinds() -> list[str]:
    """Kinds with first-class or generic reverse-ETL support."""
    base = sorted(
        {
            "postgresql",
            "mysql",
            "sqlserver",
            "oracle",
            "snowflake",
            "bigquery",
            "salesforce",
            "hubspot",
            "zendesk",
            "notion",
            "airtable",
            "stripe",
            "shopify",
            "pgvector",
            "qdrant",
            "weaviate",
            "pinecone",
            "milvus",
            *(_ACTIVATION_PLANNERS.keys()),
        }
    )
    return base
