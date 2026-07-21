"""Preflight rulebook and user-facing remediation guidance.

Defines the data-governance rules for DataFlow preflight: what blocks a
transfer, why it blocks, and what the user can do to fix it.  Aligned with the
universal data-transfer orchestration framework: hard gates block commits,
soft gates inform confidence, and every blocker carries a concrete
remediation.
"""

from __future__ import annotations

import re
from typing import Any

from services.db_type_utils import SCHEMALESS_DESTS

# ═══════════════════════════════════════════════════════════════════════════
# Gate taxonomy
# ═══════════════════════════════════════════════════════════════════════════
HARD_GATE_IDS = {
    "g1_source",
    "g2_destination",
    "g3_schema_contract",
    "g5_dry_run",
    "g6_target_ddl",
    "g9_data_integrity",
    "g9_sync_contract",
    "schema_drift",
    "proof_bundle",
}

SOFT_GATE_IDS = {
    "g4_mapping_confidence",
    "g7_capacity",
    "g8_reconciliation",
    "g10_schema_policy",
    "g11_validation_posture",
}

# ═══════════════════════════════════════════════════════════════════════════
# Gate rules: what the gate checks, why it blocks, and what to do
# ═══════════════════════════════════════════════════════════════════════════
PREFLIGHT_GATE_RULES: dict[str, dict[str, Any]] = {
    "g1_source": {
        "title": "Source connectivity",
        "category": "hard",
        "why": "DataFlow cannot read the source. Without a readable source there is nothing to transfer.",
        "fix": "Check the host/port/credentials, ensure the source is online, and that the chosen table/file/collection exists and is accessible.",
        "examples": [
            "PostgreSQL connection refused → whitelist the DataFlow host or start the database.",
            "S3 bucket not found → verify the bucket name, region, and access key.",
            "CSV parse error → open the file in a text editor and remove malformed quoted lines.",
        ],
    },
    "g2_destination": {
        "title": "Destination connectivity",
        "category": "hard",
        "why": "DataFlow can read the source but cannot reach the destination. Writes would fail silently.",
        "fix": "Check the destination host, port, credentials, and write permissions. For object stores, verify the bucket/container exists and the account can write.",
        "examples": [
            "MongoDB auth failed → use the correct username/password and auth database.",
            "Snowflake warehouse suspended → resume the warehouse or choose an active one.",
        ],
    },
    "g3_schema_contract": {
        "title": "Schema contract / type coercion",
        "category": "hard",
        "why": "A source value cannot be stored in the target type without losing precision or failing. This is a data-loss risk.",
        "fix": "Change the target column type to a wider or more compatible type, or add a transform that safely casts the value. For schemaless destinations this is normally allowed.",
        "examples": [
            "VARCHAR 'abc' → INTEGER is impossible. Keep the source as VARCHAR or skip that row.",
            "TIMESTAMP → DATE truncates the time. Use DATE only if the time is not needed.",
            "DECIMAL → INTEGER drops fractional cents. Use DECIMAL or NUMERIC for money.",
        ],
    },
    "g4_mapping_confidence": {
        "title": "Mapping confidence",
        "category": "soft",
        "why": "The automatic column mapper is not certain a source column means the same thing as the target column. Low confidence increases the risk of wrong joins, wrong aggregations, or compliance errors.",
        "fix": "Review the mapping in the UI, confirm or re-map the column, and click approve. You can also improve the source or target schema names.",
        "examples": [
            "'amt' mapped to 'amount' with low confidence → approve if they are the same concept.",
            "'created' mapped to 'updated' → verify which timestamp the business actually needs.",
        ],
    },
    "g5_dry_run": {
        "title": "Transform dry-run / integrity",
        "category": "hard",
        "why": "A sample row could not be transformed or violates a data-integrity rule. This means the real transfer would fail or produce bad data.",
        "fix": "Fix the source values, choose a less strict transform, or adjust the validation mode. For schemaless destinations the transform is stored as-is and may not block.",
        "examples": [
            "'2024-13-01' cannot be parsed as a date → correct the date or use a string target.",
            "A required identifier is null → remove the row or relax the source constraint.",
        ],
    },
    "g6_target_ddl": {
        "title": "Target DDL compatibility",
        "category": "hard",
        "why": "The target table or collection cannot accept the data as mapped. This could be a duplicate primary key, a missing column, a width overflow, or a NOT-NULL constraint.",
        "fix": (
            "Fix every listed issue before Run: remap columns, enable create-new / "
            "backfill so DataFlow can ADD COLUMN, widen types, or clean source samples. "
            "Do not Execute until this gate is green — warehouse errors after Run are too late."
        ),
        "examples": [
            "Duplicate '_id' values in MongoDB → remove duplicates or choose a different primary key.",
            "VARCHAR(10) target cannot fit a 50-character email → widen to VARCHAR(255) or TEXT.",
            "Target column does not exist → enable create-new / backfill or remap.",
            "Decimal capacity overflow → widen NUMBER/DECIMAL or map to VARCHAR.",
            "Could not load destination schema → refresh Map destination columns.",
        ],
    },
    "g7_capacity": {
        "title": "Capacity / staging",
        "category": "soft",
        "why": "The local staging volume may not have enough free space to stage the transfer.",
        "fix": "Free disk space on the DataFlow host, reduce the batch size, or write directly to the destination without local staging.",
        "examples": [
            "A 100 GB export needs at least 300 GB staging free space. Clear old exports or mount a larger volume.",
        ],
    },
    "g9_data_integrity": {
        "title": "Data integrity audit",
        "category": "hard",
        "why": "The sample violates one or more integrity rules: duplicate keys, required nulls, financial precision loss, or encoding anomalies.",
        "fix": "Clean the source data, adjust the mapping, or switch validation mode to 'balanced' for non-critical fields.",
        "examples": [
            "Primary key has duplicates → deduplicate the source or use a composite key.",
            "An amount field lost a decimal place → use DECIMAL with the same precision as the source.",
            "A required 'user_id' is 50% null → fix the source or exclude the rows.",
        ],
    },
    "proof_bundle": {
        "title": "Proof bundle decision",
        "category": "hard",
        "why": "The composite readiness score, semantic confidence, or reconciliation proof is below the configured threshold.",
        "fix": "Review the proof panel, improve mapping confidence, fix data-quality issues, or lower the validation threshold for non-critical transfers.",
        "examples": [
            "Semantic confidence 0.61 < threshold 0.75 → approve mappings manually or improve column naming.",
            "Reconciliation mismatch → investigate the row count or key set difference.",
        ],
    },
    "schema_drift": {
        "title": "Schema drift",
        "category": "hard",
        "why": "The live source or destination schema no longer matches the saved mapping contract.",
        "fix": "Re-run the mapping step, approve the new contract, or set the schema policy to propagate safe changes.",
        "examples": [
            "A new 'deleted_at' column appeared in the source → add it to the mapping or ignore it.",
            "A target column type changed from VARCHAR to INTEGER → re-run preflight and update the contract.",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# Issue catalog: patterns and remediation
# ═══════════════════════════════════════════════════════════════════════════
ISSUE_CATALOG: list[dict[str, Any]] = [
    {
        "keywords": ["semantic mapping confidence"],
        "gate": "proof_bundle",
        "why": "The average confidence of the automatic column mappings is below the threshold.",
        "fix": "Review each mapping in the mapping panel. Approve the ones that are correct and re-map the ones that are wrong. If the columns are intentionally unusual, lower the validation mode or confidence threshold.",
        "examples": ["Semantic confidence 0.61 < 0.75."],
    },
    {
        "keywords": ["dry-run / integrity failed"],
        "gate": "g5_dry_run",
        "why": "The sample rows could not be transformed or failed an integrity check.",
        "fix": "Open the issue list and fix the source values or the target types. For schemaless destinations (MongoDB, DynamoDB, Redis) DataFlow can store values as-is, but if a typed target is used the transform must succeed.",
        "examples": ["'abc' could not be parsed as integer."],
    },
    {
        "keywords": ["target ddl incompatible"],
        "gate": "g6_target_ddl",
        "why": "The target table or collection cannot accept the mapped data as-is.",
        "fix": "Allow table creation, widen the target column, change the target type, remove duplicate primary keys, or map to a compatible existing column.",
        "examples": ["VARCHAR(10) cannot store a 50-char string."],
    },
    {
        "keywords": ["ambiguous mapping"],
        "gate": "g4_mapping_confidence",
        "why": "Two or more target columns are equally plausible for a source column, so the winner is uncertain.",
        "fix": "Manually select the correct target column in the mapping panel. If the ambiguity is a false positive because the target uses '_id' vs 'id' or another common alias, DataFlow will learn the alias.",
        "examples": ["'_id' mapped to 'id' with a low gap between alternatives."],
    },
    {
        "keywords": ["low confidence"],
        "gate": "g4_mapping_confidence",
        "why": "The mapper cannot find a strong semantic or lexical match for the source column.",
        "fix": "Rename the source or target column to a more standard name, provide a sample, or manually map the column.",
        "examples": ["'col_4' -> 'amount' confidence 0.55."],
    },
    {
        "keywords": ["duplicate key values"],
        "gate": "g9_data_integrity",
        "why": "The primary-key or unique-key column contains repeated values. The target would reject the second occurrence.",
        "fix": "Deduplicate the source, use a composite key, or choose a different primary key. For schemaless stores only the real '_id' target is enforced.",
        "examples": ["user_id 'u1' appears twice."],
    },
    {
        "keywords": ["null/empty", "null rate"],
        "gate": "g9_data_integrity",
        "why": "A required identifier or key column is mostly null. The target would reject these rows or create a broken key.",
        "fix": "Fix the source data, exclude the rows, or use a non-required field for the key. For schemaless destinations optional fields are allowed.",
        "examples": ["user_id is 60% null but is required for the target key."],
    },
    {
        "keywords": ["lossy type coercion"],
        "gate": "g3_schema_contract",
        "why": "Converting the source type to the target type may lose precision, range, or information.",
        "fix": "Use a wider or lossless target type (VARCHAR, TEXT, JSON, DECIMAL, TIMESTAMP) or cast only when the business accepts the loss.",
        "examples": [
            "DECIMAL -> INTEGER drops fractional cents.",
            "TIMESTAMP -> DATE drops the time-of-day.",
            "JSON -> BOOLEAN is invalid for non-boolean JSON.",
        ],
    },
    {
        "keywords": ["value width overflow"],
        "gate": "g6_target_ddl",
        "why": "A sample value is longer than the target column allows.",
        "fix": "Widen the target column (VARCHAR/TEXT) or truncate the source value in a transform only if the tail is not needed.",
        "examples": ["A 60-char address in a VARCHAR(50) target."],
    },
    {
        "keywords": ["fractional source values"],
        "gate": "g6_target_ddl",
        "why": "A source numeric value has a decimal part but the target column is an integer.",
        "fix": "Change the target column to DECIMAL/NUMERIC or round the value explicitly with a transform.",
        "examples": ["12.34 cannot fit into an INTEGER target."],
    },
    {
        "keywords": ["could not load destination schema", "cannot prove mapped columns"],
        "gate": "g6_target_ddl",
        "why": (
            "Validate could not introspect the destination table schema, so it cannot "
            "prove that mapped columns exist. Running now would risk invalid-identifier "
            "or missing-column failures after the transfer starts."
        ),
        "fix": (
            "Confirm the destination table/schema name, refresh destination columns on "
            "Map, fix credentials, or choose full_refresh_overwrite if you intend to "
            "recreate the table from the mapping."
        ),
        "examples": [
            "Could not load destination schema for existing target — Validate cannot prove mapped columns exist.",
        ],
    },
    {
        "keywords": ["decimal capacity overflow"],
        "gate": "g6_target_ddl",
        "why": (
            "A sample value is larger than the destination DECIMAL/NUMBER precision or "
            "scale. The warehouse will reject or truncate the value at write time."
        ),
        "fix": (
            "Widen the destination column (higher precision/scale), map to VARCHAR/TEXT, "
            "or round/scale the source value with an explicit transform before Run."
        ),
        "examples": [
            "Decimal capacity overflow: amount (NUMBER(10,2)) cannot hold sample value '12345678901.99'",
        ],
    },
    {
        "keywords": ["000904", "invalid identifier", "sql compilation error"],
        "gate": "g6_target_ddl",
        "why": (
            "Snowflake rejected a column name in the write SQL. Usually the mapping "
            "targets a create-new column (for example id_text / _id) that does not "
            "exist on the destination table yet."
        ),
        "fix": (
            "Go back to Map and confirm create-new columns. Re-run so DataFlow can "
            "ADD COLUMN for create_compatible_new mappings, or remap onto an existing "
            "compatible column. Enable 'backfill new fields' if you added columns manually."
        ),
        "examples": [
            'invalid identifier \'"id_text"\'',
            "SQL compilation error: error line 1 at position 25",
        ],
    },
    {
        "keywords": [
            "undefinedcolumn",
            "undefined column",
            "column does not exist",
            "of relation",
        ],
        "gate": "g6_target_ddl",
        "why": (
            "PostgreSQL (or another SQL engine) rejected a column that is not on the "
            "destination table. Same class as Snowflake 000904: create-new / drift "
            "column was referenced without ADD COLUMN."
        ),
        "fix": (
            "Remap create-new fields, enable backfill new fields / propagate columns, "
            "or map onto an existing column. Re-run Validate then Execute."
        ),
        "examples": [
            'column "_id" of relation "customers" does not exist',
            "UndefinedColumn: column \"id_text\" does not exist",
        ],
    },
    {
        "keywords": ["1054", "unknown column", "er_bad_field_error"],
        "gate": "g6_target_ddl",
        "why": (
            "MySQL/MariaDB rejected an unknown column in INSERT/UPDATE. Usually a "
            "create_compatible_new target was written before ALTER TABLE ADD COLUMN."
        ),
        "fix": (
            "Confirm create-new mappings on Map, re-run so DataFlow ADDs the column, "
            "or enable backfill new fields / remap to an existing column."
        ),
        "examples": [
            "Unknown column '_id' in 'field list'",
            "ERROR 1054 (42S22): Unknown column 'id_text' in 'field list'",
        ],
    },
    {
        "keywords": ["not found in table", "unrecognized name", "name not found inside"],
        "gate": "g6_target_ddl",
        "why": (
            "BigQuery rejected a column name that is not in the table schema — the "
            "create-new / drift column class across warehouses."
        ),
        "fix": (
            "Re-run with create-new backfill enabled (automatic for create_compatible_new), "
            "or remap onto an existing BigQuery field."
        ),
        "examples": [
            "Name _id not found inside customers",
            "Unrecognized name: id_text",
        ],
    },
    {
        "keywords": ["207", "invalid column name", "s0022"],
        "gate": "g6_target_ddl",
        "why": (
            "SQL Server rejected an invalid/missing column name during write — same "
            "missing ADD COLUMN failure mode as other SQL destinations."
        ),
        "fix": (
            "Ensure create-new mappings trigger ADD COLUMN (automatic), enable backfill "
            "new fields, or remap to an existing column."
        ),
        "examples": [
            "Invalid column name '_id'",
            "Msg 207, Level 16, State 1, Line 1",
        ],
    },
    {
        "keywords": ["ora-00904", "invalid identifier"],
        "gate": "g6_target_ddl",
        "why": (
            "Oracle rejected an identifier that is not a column on the target table — "
            "typically a create-new mapping without schema evolution."
        ),
        "fix": (
            "Re-run so DataFlow can ADD the missing column for create_compatible_new, "
            "enable backfill new fields, or remap onto an existing Oracle column."
        ),
        "examples": [
            'ORA-00904: "_ID": invalid identifier',
            "ORA-00904: invalid identifier",
        ],
    },
    {
        "keywords": ["target column", "does not exist"],
        "gate": "g6_target_ddl",
        "why": "The mapping references a target column that does not exist and the destination cannot be altered.",
        "fix": (
            "Enable backfill new fields / create-new so DataFlow can ADD COLUMN, allow "
            "create table for new targets, or map only to existing columns."
        ),
        "examples": ["Target 'email' does not exist in the existing table."],
    },
    {
        "keywords": ["duplicate target column"],
        "gate": "g6_target_ddl",
        "why": "Two source columns map to the same target column.",
        "fix": "Remove the duplicate mapping or pick a different target for one of the sources.",
        "examples": ["Both 'user_id' and 'customer_id' map to 'id'."],
    },
    {
        "keywords": ["unmapped"],
        "gate": "g4_mapping_confidence",
        "why": "A source column has no target, or a target column has no source.",
        "fix": "Map the column explicitly, or leave it unmapped if it is not needed. For an unknown target, DataFlow will create identity columns automatically.",
        "examples": ["'notes' source column has no mapped target."],
    },
    {
        "keywords": ["pii/compliance", "compliance review"],
        "gate": "proof_bundle",
        "why": "The sample contains fields that look like PII (email, phone, name, etc.) and compliance review is required.",
        "fix": "Mask, tokenize, or drop the PII fields, or approve the transfer after confirming your data governance policy allows the move.",
        "examples": ["Email and phone columns detected."],
    },
    {
        "keywords": ["source schema changed", "destination schema changed"],
        "gate": "schema_drift",
        "why": "The schema has changed since the mapping was last approved.",
        "fix": "Re-run the mapping step and approve the new contract, or set the schema policy to propagate safe changes.",
        "examples": ["A new 'status' column appeared in the source."],
    },
    {
        "keywords": ["replacement character", "encoding", "format-control character"],
        "gate": "g5_dry_run",
        "why": "The sample contains replacement characters or invisible format/control characters (zero-width spaces, null bytes, etc.) that warehouses often reject.",
        "fix": "Open Fix bad data and choose Strip control characters (applies strip_controls), or quarantine affected rows. In balanced mode this is a warning; strict mode blocks until sanitized.",
        "examples": [
            "Zero-width space U+200B in a MongoDB string field.",
            "Null byte U+0000 in a scraped HTML description (also breaks Postgres COPY).",
        ],
    },
    {
        "keywords": ["magnitude shift"],
        "gate": "g9_data_integrity",
        "why": "A financial value changed by more than 100x or less than 1x after parsing, indicating a currency/decimal parsing error.",
        "fix": "Check the source format (currency symbols, commas, decimal separators) and use a DECIMAL transform with the correct locale.",
        "examples": ["'$1,234.56' parsed as '1.234'."],
    },
    {
        "keywords": ["sync mode contract", "missing cursor", "missing primary key"],
        "gate": "g9_sync_contract",
        "why": "The selected sync mode requires a cursor or primary key that is not configured.",
        "fix": "Set a cursor field for incremental/CDC sync, or choose a full-refresh mode. For deduped sync, set a primary key.",
        "examples": ["CDC mode requires a primary key and a change log."],
    },
    {
        "keywords": ["insufficient staging"],
        "gate": "g7_capacity",
        "why": "The local staging volume may not have enough free space for the transfer.",
        "fix": "Free disk space, mount a larger volume, or reduce the batch size.",
        "examples": ["Need 300 GB, have 50 GB free."],
    },
    {
        "keywords": ["source not connected", "source error"],
        "gate": "g1_source",
        "why": "DataFlow cannot connect to or read the source.",
        "fix": "Check the connection settings, credentials, network, and that the source object exists.",
        "examples": ["PostgreSQL connection refused."],
    },
    {
        "keywords": ["destination not reachable", "destination error"],
        "gate": "g2_destination",
        "why": "DataFlow cannot reach the destination.",
        "fix": "Check the destination connection, credentials, and write permissions.",
        "examples": ["MongoDB auth failed."],
    },
]


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════
def _match_issue(message: str) -> dict[str, Any] | None:
    """Return the first catalog entry that matches the issue message."""
    lower = message.lower()
    for entry in ISSUE_CATALOG:
        for kw in entry.get("keywords", []):
            if kw.lower() in lower:
                return entry
        pattern = entry.get("pattern")
        if pattern and re.search(pattern, message, re.IGNORECASE):
            return entry
    return None


def explain_issue(
    message: str,
    *,
    dest_kind: str = "",
    validation_mode: str = "balanced",
    source_type: str = "",
    target_type: str = "",
) -> dict[str, Any]:
    """Return human-readable why/why/guidance for a preflight issue string."""
    entry = _match_issue(message) or {}
    guidance = entry.get("why", "A preflight gate detected a condition that violates a transfer-safety rule.")
    if dest_kind.lower() in SCHEMALESS_DESTS and "key" in message.lower():
        guidance += " For schemaless destinations, only the real '_id' key is enforced; other identifier-like columns may repeat."
    return {
        "message": message,
        "gate": entry.get("gate", "general"),
        "why": guidance,
        "fix": entry.get(
            "fix",
            "Review the issue and adjust the source data, mapping, or target schema.",
        ),
        "examples": entry.get("examples", []),
        "severity": entry.get("severity", "warning"),
    }


def explain_gate(gate_id: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the rule for a gate failure."""
    rule = PREFLIGHT_GATE_RULES.get(gate_id)
    if not rule:
        return explain_issue(message, dest_kind="", validation_mode="balanced")
    return {
        "gate": gate_id,
        "title": rule["title"],
        "category": rule["category"],
        "why": rule["why"],
        "fix": rule["fix"],
        "examples": rule["examples"],
    }


def enrich_blockers(
    blockers: list[dict[str, Any]],
    *,
    dest_kind: str = "",
    validation_mode: str = "balanced",
) -> list[dict[str, Any]]:
    """Attach guidance to each blocker dict."""
    enriched: list[dict[str, Any]] = []
    for b in blockers:
        gate_id = b.get("id") or b.get("gate") or "general"
        guidance = explain_gate(gate_id, b.get("message", ""), b.get("details"))
        # Also explain nested issues if present
        details = b.get("details") or {}
        nested = details.get("issues") or details.get("errors") or []
        if nested and isinstance(nested, list):
            guidance["details"] = [
                explain_issue(str(item), dest_kind=dest_kind, validation_mode=validation_mode)
                for item in nested
            ]
        enriched.append({**b, "guidance": guidance})
    return enriched


def enrich_issues(
    issues: list[str],
    *,
    dest_kind: str = "",
    validation_mode: str = "balanced",
) -> list[dict[str, Any]]:
    """Attach guidance to each issue string."""
    return [explain_issue(msg, dest_kind=dest_kind, validation_mode=validation_mode) for msg in issues]
