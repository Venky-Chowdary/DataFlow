# Public API versioning & deprecation policy

**Status:** Active for Datawrap control-plane HTTP API
**Canonical mount:** `/api/v1` only (see `apps/api/src/main.py`)

## Stability contract

1. **Additive by default.** New optional fields, endpoints, and query params may
   ship on `/api/v1` without a major bump.
2. **Breaking changes** (removed fields, changed semantics, renamed paths)
   require `/api/v2` (or a later major). Dual-run for **at least 6 months** when
   a major is introduced.
3. **Deprecation signal.** Deprecated surfaces return:
   - Response header `Deprecation: true`
   - Response header `Sunset: <HTTP-date>` when a removal date is known
   - JSON body field `deprecation` (when the payload is an object) with
     `{ "since": "...", "sunset": "...", "replacement": "..." }` when practical
4. **MCP and Pilot** are production API surfaces forever - same auth, rate
   limits, and receipt expectations as REST.
5. **Honesty.** Delivery semantics for CDC/resume remain **at-least-once** unless
   a future documented contract proves stronger semantics per writer.

## What is not covered

- Warehouse DDL / connector dialect quirks (documented per connector SKU)
- Internal worker RPCs and debug endpoints
- GitOps `MappingBundle` imports (always land as DRAFT contracts)

## Changelog location

Ship breaking notes in release notes and keep this policy file as the SSOT for
buyer diligence. OpenAPI/export tooling should cite `/api/v1` as the only
supported public prefix until `v2` exists.
