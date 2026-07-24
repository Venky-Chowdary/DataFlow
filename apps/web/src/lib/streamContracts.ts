/** Per-stream cursor / primary-key overrides for multi-stream Advanced settings. */

export interface StreamFieldContract {
  cursorField: string;
  primaryKeyField: string;
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
): StreamFieldContract {
  const override = streamFields[name];
  return {
    cursorField: override?.cursorField ?? defaultCursor,
    primaryKeyField: override?.primaryKeyField ?? defaultPrimaryKey,
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
    );
    const maps = input.streamMappings?.[name];
    return {
      name,
      selected: true,
      sync_mode: input.syncMode,
      cursor_field: input.requiresCursor ? fields.cursorField : "",
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

/** True when any selected stream is missing a required cursor or primary key. */
export function streamContractsNeedReview(input: {
  streamNames: string[];
  sourceColumns: string[];
  requiresCursor: boolean;
  requiresPrimaryKey: boolean;
  defaultCursor: string;
  defaultPrimaryKey: string;
  streamFields: Record<string, StreamFieldContract>;
}): boolean {
  if (!input.sourceColumns.length) return false;
  for (const name of input.streamNames) {
    const fields = resolveStreamFields(
      name,
      input.streamFields,
      input.defaultCursor,
      input.defaultPrimaryKey,
    );
    if (input.requiresCursor && !fields.cursorField) return true;
    if (input.requiresPrimaryKey && !fields.primaryKeyField) return true;
  }
  return false;
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
    if (cursorField !== cur.cursorField || primaryKeyField !== cur.primaryKeyField || !next[name]) {
      next[name] = { cursorField, primaryKeyField };
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
