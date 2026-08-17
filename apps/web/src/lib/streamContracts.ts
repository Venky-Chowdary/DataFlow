/** Per-stream cursor / primary-key overrides for multi-stream Advanced settings. */

import { evaluateCursorSemantics } from "./cursorSemantics";

export interface StreamFieldContract {
  cursorField: string;
  primaryKeyField: string;
  /** Declared meaning of the cursor column — never inferred from its name. */
  cursorSemantics?: string;
}

export interface BuildStreamContractsInput {
  streamNames: string[];
  syncMode: string;
  schemaPolicy: string;
  validationMode: string;
  fieldCount: number;
  requiresCursor: boolean;
  requiresPrimaryKey: boolean;
  /** Shared defaults (single-stream or seed for new streams). */
  defaultCursor: string;
  defaultPrimaryKey: string;
  defaultCursorSemantics?: string;
  /** Per-stream overrides; missing keys fall back to defaults. */
  streamFields: Record<string, StreamFieldContract>;
  /** Debezium-compatible snapshot mode (CDC only). */
  snapshotMode?: string;
}

export function resolveStreamFields(
  name: string,
  streamFields: Record<string, StreamFieldContract>,
  defaultCursor: string,
  defaultPrimaryKey: string,
  defaultCursorSemantics = "",
): StreamFieldContract {
  const override = streamFields[name];
  return {
    cursorField: override?.cursorField ?? defaultCursor,
    primaryKeyField: override?.primaryKeyField ?? defaultPrimaryKey,
    cursorSemantics: override?.cursorSemantics ?? defaultCursorSemantics,
  };
}

/** Build API `stream_contracts` with per-stream cursor/PK when multi-stream. */
export function buildStreamContracts(input: BuildStreamContractsInput & {
  streamMappings?: Record<string, { source: string; target: string; confidence?: number; transform?: string }[]>;
}): Record<string, unknown>[] {
  return input.streamNames.map((name) => {
    const fields = resolveStreamFields(
      name,
      input.streamFields,
      input.defaultCursor,
      input.defaultPrimaryKey,
      input.defaultCursorSemantics || "",
    );
    const maps = input.streamMappings?.[name];
    return {
      name,
      selected: true,
      sync_mode: input.syncMode,
      cursor_field: input.requiresCursor ? fields.cursorField : "",
      cursor_semantics: input.requiresCursor ? fields.cursorSemantics || "" : "",
      primary_key: fields.primaryKeyField || "",
      schema_policy: input.schemaPolicy,
      field_count: input.fieldCount,
      validation_mode: input.validationMode,
      ...(input.syncMode === "cdc" && input.snapshotMode
        ? { snapshot_mode: input.snapshotMode }
        : {}),
      ...(maps && maps.length
        ? {
            mappings: maps.map((m) => ({
              source: m.source,
              target: m.target,
              confidence: m.confidence ?? 0,
              transform: m.transform || "none",
            })),
          }
        : {}),
    };
  });
}

export interface StreamContractReviewInput {
  streamNames: string[];
  sourceColumns: string[];
  /** Per-stream columns when schemas diverge — falls back to sourceColumns. */
  sourceColumnsByStream?: Record<string, string[]>;
  requiresCursor: boolean;
  requiresPrimaryKey: boolean;
  defaultCursor: string;
  defaultPrimaryKey: string;
  defaultCursorSemantics?: string;
  streamFields: Record<string, StreamFieldContract>;
  /** Sync mode and validation mode decide whether a declaration is required. */
  syncMode?: string;
  validationMode?: string;
}

export interface StreamContractIssue {
  /** The stream to change — empty when the whole schema has not loaded. */
  stream: string;
  /** Why the run cannot proceed, in the operator's terms. */
  reason: string;
  /** The single thing to change. */
  action: string;
}

/**
 * The first stream whose contract blocks a run, with one action to fix it.
 *
 * The engine refuses per stream, so the operator is told which stream and which
 * single change — not a list of everything the contract could be missing.
 */
export function firstStreamContractIssue(
  input: StreamContractReviewInput,
): StreamContractIssue | null {
  const anyColumns =
    input.sourceColumns.length > 0
    || Object.values(input.sourceColumnsByStream || {}).some((c) => c.length > 0);
  if (!anyColumns) return null;
  for (const name of input.streamNames) {
    const cols = input.sourceColumnsByStream?.[name]?.length
      ? input.sourceColumnsByStream[name]
      : input.sourceColumns;
    if (!cols.length) {
      return {
        stream: name,
        reason: `${name} is selected but its schema has not loaded.`,
        action: "Reload the source schema before running.",
      };
    }
    const fields = resolveStreamFields(
      name,
      input.streamFields,
      input.defaultCursor,
      input.defaultPrimaryKey,
      input.defaultCursorSemantics || "",
    );
    if (input.requiresCursor && !fields.cursorField) {
      return {
        stream: name,
        reason: `${name} has no cursor column, so an incremental read has no watermark.`,
        action: `Select a cursor column for ${name}.`,
      };
    }
    if (input.requiresCursor && !cols.includes(fields.cursorField)) {
      return {
        stream: name,
        reason: `${name}.${fields.cursorField} is not in the source schema.`,
        action: `Select a cursor column that exists in ${name}.`,
      };
    }
    // A cursor that exists is not a cursor that is safe: what it means decides
    // whether the read can lose rows, and the engine refuses an undeclared one.
    if (input.requiresCursor && input.syncMode) {
      const verdict = evaluateCursorSemantics({
        syncMode: input.syncMode,
        cursorField: fields.cursorField,
        declared: fields.cursorSemantics || "",
        validationMode: input.validationMode,
      });
      if (verdict.status === "block") {
        return {
          stream: name,
          reason: `${name}: ${verdict.reason}`,
          action: verdict.primaryAction,
        };
      }
    }
    if (input.requiresPrimaryKey && !fields.primaryKeyField) {
      return {
        stream: name,
        reason: `${name} has no primary key, so a changed row cannot be matched to the row it replaces.`,
        action: `Select a primary key for ${name}.`,
      };
    }
    if (input.requiresPrimaryKey && !cols.includes(fields.primaryKeyField)) {
      return {
        stream: name,
        reason: `${name}.${fields.primaryKeyField} is not in the source schema.`,
        action: `Select a primary key that exists in ${name}.`,
      };
    }
  }
  return null;
}

/** True when any selected stream's cursor / primary-key contract blocks a run. */
export function streamContractsNeedReview(input: StreamContractReviewInput): boolean {
  return firstStreamContractIssue(input) !== null;
}

/** Merge auto-detected candidates into each stream that lacks a field. */
export function seedStreamFieldsFromCandidates(
  streamNames: string[],
  prev: Record<string, StreamFieldContract>,
  cursorCandidate: string,
  primaryKeyCandidate: string,
  sourceColumns: string[],
): Record<string, StreamFieldContract> {
  const next = { ...prev };
  let changed = false;
  for (const name of streamNames) {
    const cur = next[name] ?? { cursorField: "", primaryKeyField: "" };
    let cursorField = cur.cursorField;
    let primaryKeyField = cur.primaryKeyField;
    if (cursorCandidate && (!cursorField || !sourceColumns.includes(cursorField))) {
      cursorField = cursorCandidate;
    } else if (cursorField && !sourceColumns.includes(cursorField)) {
      cursorField = "";
    }
    if (primaryKeyCandidate && (!primaryKeyField || !sourceColumns.includes(primaryKeyField))) {
      primaryKeyField = primaryKeyCandidate;
    } else if (primaryKeyField && !sourceColumns.includes(primaryKeyField)) {
      primaryKeyField = "";
    }
    // A declaration describes one column. It survives a primary-key change and
    // dies with the column it described — never carried onto a different cursor.
    const cursorSemantics =
      cursorField && cursorField === cur.cursorField ? cur.cursorSemantics ?? "" : "";
    if (
      cursorField !== cur.cursorField
      || primaryKeyField !== cur.primaryKeyField
      || cursorSemantics !== (cur.cursorSemantics ?? "")
      || !next[name]
    ) {
      next[name] = { cursorField, primaryKeyField, cursorSemantics };
      changed = true;
    }
  }
  // Drop stale stream keys
  for (const key of Object.keys(next)) {
    if (!streamNames.includes(key)) {
      delete next[key];
      changed = true;
    }
  }
  return changed ? next : prev;
}
