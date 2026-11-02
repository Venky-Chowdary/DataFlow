/** Pure helpers for Transfer Studio (Phase F9). */

import {
  mappingRequiresRiskAck,
  type EditableMapping,
} from "../../lib/mapping";
import type { EnhancedAnalysis } from "../../lib/types";
import { STEP_DESTINATION, STEP_MAP, STEP_SOURCE, STEP_VALIDATE } from "./studioConstants";

/** Remediations must not clear fidelity/STRUCT risk without Accept risk. */
export function sealRemediationApproval(m: EditableMapping): EditableMapping {
  if (mappingRequiresRiskAck(m) && !m.riskAcknowledged) {
    return { ...m, approved: false, requiresReview: true };
  }
  return m;
}

export function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function findColumn(columns: string[], patterns: RegExp[]) {
  return columns.find((col) => patterns.some((pattern) => pattern.test(col))) || "";
}

export function fileExtension(name: string) {
  return name.split(".").pop()?.toLowerCase() ?? "";
}

export type StudioSourceKind = "file" | "database" | "cloud";

/** Run-panel and chrome share this — never prefer a leftover file name on a DB route. */
export function studioSourceLabel(opts: {
  sourceKind: StudioSourceKind;
  fileName?: string | null;
  cloudPath?: string;
  sourceConnectorName?: string | null;
}): string {
  if (opts.sourceKind === "file") return opts.fileName?.trim() || "Choose source";
  if (opts.sourceKind === "cloud") {
    return opts.cloudPath?.trim() || opts.sourceConnectorName || "Cloud source";
  }
  return opts.sourceConnectorName || "Database source";
}

export function analysisFromPipeline(
  columns: string[],
  schema: Record<string, string>,
  pipelineColumns: {
    source: string;
    target: string;
    confidence: number;
    reasoning?: string;
  }[],
): EnhancedAnalysis {
  const bySource = Object.fromEntries(pipelineColumns.map((m) => [m.source, m]));
  return {
    columns: columns.map((column_name) => ({
      column_name,
      inferred_type: schema[column_name] || "string",
      confidence: bySource[column_name]?.confidence ?? 0.7,
      is_pii: /email|phone|ssn|name/i.test(column_name),
      compliance: [],
      reasoning_steps: [bySource[column_name]?.reasoning || "Semantic mapping pipeline"],
      method: "mapping_pipeline",
    })),
    pii_columns: columns.filter((c) => /email|phone|ssn/i.test(c)),
    quality_score: pipelineColumns.length
      ? Math.round(
          (pipelineColumns.reduce((s, m) => s + m.confidence, 0) / pipelineColumns.length) *
            100,
        )
      : 70,
    recommendations: ["Review column mappings before executing."],
    method: "mapping_pipeline",
  };
}

/** Destination object list loads on Destination, Map, and Validate.
 *  Connect keeps the operator on Source, so the flagged studio must load it there too.
 */
export function destinationCatalogLoadsOnStep(step: number, canvas: boolean): boolean {
  return (
    step === STEP_DESTINATION
    || step === STEP_MAP
    || step === STEP_VALIDATE
    || (canvas && step === STEP_SOURCE)
  );
}

/** Automatic retry of a failed destination probe. Validate probes on demand. */
export function destinationCatalogRetriesOnStep(step: number, canvas: boolean): boolean {
  return step === STEP_DESTINATION || step === STEP_MAP || (canvas && step === STEP_SOURCE);
}

/** Engines whose "database" is a local file path, not a server. */
export const FILE_DATABASE_ENGINES = new Set(["sqlite", "duckdb"]);

/** `sqlite:////abs/path.db` / `duckdb:///rel.duckdb` → the file path, else "". */
export function fileDatabasePathFromUrl(url: string): string {
  const m = /^(?:sqlite|duckdb)(?:\+\w+)?:\/\/\/(.+)$/i.exec(url.trim());
  if (!m) return "";
  const path = m[1].split("?")[0];
  return path === ":memory:" ? "" : path;
}

/** Why "Continue" on the Destination step is disabled, or null when it is not. */
export function destinationContinueBlocker(o: {
  sourceReady: boolean;
  destKindMode: string;
  destType: string;
  callable: boolean;
  sqlReady: boolean;
  hasConnector: boolean;
  targetDb: string;
  targetCollection: string;
  schemaLoading: boolean;
  isFileDatabase: boolean;
  objectWord: string;
}): string | null {
  if (!o.sourceReady) return "Finish the source step first.";
  if (o.destKindMode === "file_export") return null;
  if (!o.destType) return "Pick a saved connection or an engine.";
  if (o.schemaLoading) return "Reading the destination catalog…";
  if (o.callable && !o.sqlReady) return "Enter the destination statement.";
  if (!o.hasConnector && !o.targetDb.trim()) {
    return o.isFileDatabase ? "Enter the database file path." : "Enter the destination database.";
  }
  if (!o.callable && !o.targetDb.trim()) return "Enter the destination database.";
  if (!o.callable && !o.targetCollection.trim()) return `Name the destination ${o.objectWord}.`;
  return null;
}

const FORMAT_CONTROL_RE = /[\u200B-\u200F\u202A-\u202E\u2060-\u2064\uFEFF]/;

/** True when any string cell carries a zero-width / bidi format character. */
export function rowsContainFormatControlChars(rows: Record<string, unknown>[] | null | undefined): boolean {
  return (rows || []).some((row) =>
    Object.values(row || {}).some((v) => typeof v === "string" && FORMAT_CONTROL_RE.test(v)),
  );
}
