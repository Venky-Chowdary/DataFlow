# SaaS Connector Honesty (Phase E3)

**Decision:** Do **not** market a SaaS category fleet until incremental sync, OAuth refresh, and `Retry-After` are proven for claimed connectors. Until then SaaS is an **activation / reverse-ETL** niche, not Airbyte/Fivetran territory.

## Certified today (PRODUCTION_SKU / transfer_ready)

| Connector | Role |
|-----------|------|
| Salesforce | Reverse-ETL + read (SKU-backed) |
| HubSpot | Reverse-ETL + read (SKU-backed) |

## Writers exist but Planned (not transfer_ready)

Stripe, Shopify, Airtable, Zendesk, Notion — code paths exist; **stay Planned** until SKU + incremental/OAuth/Retry-After bar is met. Catalog enrichment demotes them even if JSON says `live`.

## Roadmap tiles

Hundreds of SaaS brand stubs (`rest_api` aliases) are **roadmap only**. They must never appear in `unique_drivers` / public “live” counts.

## Non-claims

- No incremental cursor per stream
- No OAuth refresh in `saas_common` as a general capability
- No production-grade `Retry-After` budget across SaaS
- Catalog tile count ≠ live SaaS count

When the three capability gates land for a brand, promote via `PRODUCTION_SKU` + capability registry — never by flipping JSON `status` alone.
