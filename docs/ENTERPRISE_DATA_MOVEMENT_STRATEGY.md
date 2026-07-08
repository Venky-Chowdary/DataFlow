# Enterprise Data Movement Strategy

This project should behave like a universal data plane: every source and
destination is normalized through a small set of durable contracts, then routed
through connector-specific adapters.

## Product Bar

Current enterprise references point to four capabilities we should treat as
non-negotiable:

- Google Datastream: CDC, backfill, stream lifecycle, private connectivity,
  monitoring, and resilience to schema changes.
- Microsoft Azure Data Factory: schema drift support with late-binding, inferred
  drifted types, auto-mapping, and rule-based mapping.
- AWS DMS: full-load plus CDC validation, table-level validation statistics, and
  row-by-row mismatch reporting.
- Airbyte: source/destination protocol contracts, JSON-schema-based stream
  catalogs, and a compact type system every destination must handle.

Useful references:

- https://docs.cloud.google.com/datastream/docs/overview
- https://learn.microsoft.com/en-us/azure/data-factory/concepts-data-flow-schema-drift
- https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html
- https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol
- https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types

## Core Architecture

1. Endpoint intelligence
   - Probe connection health, auth scope, read/write permissions, object lists,
     sample rows, row counts, source schema, and destination DDL capability.
   - Keep capabilities separate from credentials so UI, preflight, and runtime
     can reason about the same connector contract.

2. Universal pivot schema
   - Normalize all source types into logical types: string, text, integer,
     decimal, boolean, date, datetime, time, uuid, json, array, binary.
   - Map from the pivot to destination-native DDL types.
   - Prefer lossless JSON/text/binary fallbacks over fragile casts.

3. Intelligent mapping
   - Combine exact matching, abbreviation expansion, BM25/token overlap,
     semantic role detection, value-pattern analysis, type compatibility, and
     learned lexicon boosts.
   - Enforce one-to-one target assignment unless the user explicitly overrides.
   - Treat low-confidence matches as review items, not silent failures.

4. Transform execution
   - Use deterministic transforms for decimal, integer, boolean, date,
     datetime, JSON, binary, IDs, and text cleanup.
   - Support configurable error policy:
     - `quarantine`: default; skip invalid rows and report them.
     - `coerce_null`: keep rows and null invalid cells.
     - `fail`: strict mode for regulated workflows.

5. Preflight gates
   - Validate source readability, destination reachability, type coercions,
     mapping confidence, dry-run conversion, DDL compatibility, staging capacity,
     and post-transfer reconciliation readiness.
   - Preflight must use the same transform logic as runtime.

6. Runtime transfer
   - Use streaming/chunked transfer by default for files and databases.
   - Keep checkpoint progress and retry boundaries per batch.
   - Add CDC as a separate mode with resumable cursor state and validation.

7. Reconciliation
   - Compare source rows, written rows, rejected rows, and destination row count.
   - Persist mismatch samples and validation metrics for audit.
   - Treat cross-engine checksum differences as warnings when row counts prove
     no loss, because drivers render decimals/timestamps differently.

8. Enterprise UI
   - First screen should be the actual transfer cockpit: source, destination,
     schema map, preflight, execution, reconciliation.
   - Show confidence, drift, rejected rows, warnings, and audit trail as primary
     operational signals.
   - Keep connector setup dense, searchable, and role-oriented.

## Implemented In This Hardening Pass

- Added a shared universal type system for all transfer planners and writers.
- Mapped JSON, arrays, UUIDs, binary, wide decimals, timestamps, and text to
  destination-native DDL across PostgreSQL, MySQL, Snowflake, BigQuery, and
  MongoDB.
- Added deterministic transforms for JSON, boolean, integer, binary, and richer
  timestamps.
- Added quarantine/coerce/fail transform error policies.
- Threaded rejected-row diagnostics through writers, streaming summaries, and
  reconciliation.
- Fixed preflight dry-run validation so transform failures are caught before
  execution.
- Added regression tests for type mapping, transform validation, quarantine
  behavior, preflight blocking, and reconciliation accounting.

## Next Hardening Rounds

1. Connector contract registry
   - Each connector declares supported read modes, write modes, DDL support,
     upsert support, CDC support, batch limits, auth modes, and private network
     requirements.

2. Schema drift engine
   - Detect added, removed, renamed, and type-changed fields between runs.
   - Generate migration plans and safe destination alterations.

3. CDC/resumability
   - Add checkpointed cursor state, idempotent batch writes, retry policies, and
     resume-after-failure behavior.

4. Mapping workbench
   - Add explainable confidence, competing candidates, value previews, rule
     mappings, and user-approved mapping memory.

5. Validation reports
   - Store row-level rejects, mismatch samples, validation statistics, and export
     audit reports per job.
