/** Pure helpers for Transfer Studio (Phase F9). */

import {
  mappingRequiresRiskAck,
  type EditableMapping,
} from "../../lib/mapping";
import type { EnhancedAnalysis } from "../../lib/types";

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
