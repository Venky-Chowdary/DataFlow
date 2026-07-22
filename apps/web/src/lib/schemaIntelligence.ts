import type { EditableMapping } from "./mapping";
import type { ColumnAnalysis, EnhancedAnalysis, PreflightResult, TransferPlan } from "./types";

export interface TypeRisk {
  id: string;
  column: string;
  severity: "info" | "warn" | "block";
  title: string;
  detail: string;
  suggestedTransform?: string;
}

export interface NestedFieldInsight {
  column: string;
  kind: "nested_object" | "json_string" | "array" | "dot_notation";
  detail: string;
  flattenTarget?: string;
}

export interface IntelligenceAdvantage {
  id: string;
  title: string;
  detail: string;
  icon: "shield" | "sparkle" | "activity" | "connectors" | "check" | "trend";
}

const NUMERIC_HINTS = /^(int|integer|bigint|decimal|numeric|float|double|number|currency|amount|qty|quantity|weight|rate)/i;
const DATE_HINTS = /^(date|time|timestamp|datetime|iso)/i;
const NESTED_NAME = /\./;
const JSON_LIKE = /^[\[{]/;

/** Detect nested JSON / document shapes Compass imports raw — we flatten and type-map. */
export function detectNestedDocumentFields(
  columns: string[],
  samples?: Record<string, unknown>[] | Record<string, string[]>,
): NestedFieldInsight[] {
  const insights: NestedFieldInsight[] = [];
  const sampleRows = Array.isArray(samples) ? samples : undefined;
  const columnSamples = !Array.isArray(samples) && samples ? samples : undefined;

  for (const col of columns) {
    if (NESTED_NAME.test(col)) {
      insights.push({
        column: col,
        kind: "dot_notation",
        detail: "Flattened path detected — DataFlow preserves structure across SQL warehouses.",
        flattenTarget: col.replace(/\./g, "_"),
      });
      continue;
    }

    let sampleVal = "";
    if (sampleRows?.length) {
      const v = sampleRows[0][col];
      if (v != null) sampleVal = typeof v === "object" ? JSON.stringify(v) : String(v);
    } else if (columnSamples?.[col]?.[0]) {
      sampleVal = columnSamples[col][0];
    }

    if (!sampleVal) continue;
    const trimmed = sampleVal.trim();
    if (JSON_LIKE.test(trimmed)) {
      const isArray = trimmed.startsWith("[");
      insights.push({
        column: col,
        kind: isArray ? "array" : "nested_object",
        detail: isArray
          ? "Array field — serialize per destination DDL (no silent row explosion)."
          : "Embedded JSON object — Map chooses JSON blob or flatten top-level keys (nested objects stay on parent).",
        flattenTarget: `${col}_json`,
      });
    }
  }

  return insights.slice(0, 8);
}

/** Why DataFlow vs single-tool import (Compass JSON, manual ETL, etc.) */
export function buildCompetitiveAdvantages(ctx: {
  sourceKind?: string;
  destType?: string;
  columnCount?: number;
  hasPreflight?: boolean;
  hasCrossDb?: boolean;
  nestedFieldCount?: number;
}): IntelligenceAdvantage[] {
  const advantages: IntelligenceAdvantage[] = [
    {
      id: "cross-destination",
      title: "Any source → any destination",
      detail: ctx.destType
        ? `Route to ${ctx.destType} with native DDL — not limited to loading JSON into one MongoDB collection.`
        : "File, MongoDB, S3, Snowflake, Postgres, SQL Server, Oracle, and Iceberg in one governed path.",
      icon: "connectors",
    },
    {
      id: "preflight",
      title: "8-gate preflight before write",
      detail: ctx.hasPreflight
        ? "Schema contract, mapping confidence, dry-run transform, and reconciliation — failures caught pre-load."
        : "Run preflight to block unsafe casts and precision loss before a single row is written.",
      icon: "shield",
    },
    {
      id: "semantic-map",
      title: "Semantic column intelligence",
      detail: ctx.columnCount
        ? `${ctx.columnCount} fields profiled with PII detection, type inference, and BM25+ role-graph mapping.`
        : "BM25 + role graph maps messy logistics/ERP headers to warehouse contracts automatically.",
      icon: "sparkle",
    },
    {
      id: "type-safety",
      title: "Cross-engine type safety",
      detail: "Integer/decimal mismatches, date formats, and VARCHAR→NUMBER casts validated — the #1 cause of silent data corruption.",
      icon: "check",
    },
  ];

  if (ctx.nestedFieldCount && ctx.nestedFieldCount > 0) {
    advantages.unshift({
      id: "json-flatten",
      title: "Nested JSON → typed columns",
      detail: `${ctx.nestedFieldCount} nested/document field(s) detected. On Map, choose JSON blob or flatten top-level keys — nested objects stay on the parent (no invented deep explode).`,
      icon: "trend",
    });
  }

  if (ctx.sourceKind === "file" || ctx.sourceKind === "database") {
    advantages.push({
      id: "reconcile",
      title: "Post-transfer reconciliation",
      detail: "Row counts and checksums verified after load — Compass import has no cross-system proof.",
      icon: "activity",
    });
  }

  return advantages.slice(0, 5);
}

export function detectTypeRisks(
  mappings: EditableMapping[],
  analysis?: EnhancedAnalysis | null,
  transferPlan?: TransferPlan | null,
): TypeRisk[] {
  const risks: TypeRisk[] = [];
  const bySource = new Map(analysis?.columns.map((c) => [c.column_name, c]) ?? []);

  for (const m of mappings) {
    const col = bySource.get(m.source);
    const srcType = (m.inferredType ?? col?.inferred_type ?? col?.semantic_type ?? "string").toLowerCase();
    const planMap = transferPlan?.type_mappings.find(
      (t) => t.column === m.source || t.column === m.target,
    );
    const destType = planMap?.dest_type?.toLowerCase() ?? "";

    if (m.isPii && m.transform !== "hash_pii") {
      risks.push({
        id: `pii-${m.source}`,
        column: m.source,
        severity: "warn",
        title: "PII field unprotected",
        detail: "Apply Hash PII before loading to warehouse or lakehouse.",
        suggestedTransform: "hash_pii",
      });
    }

    if (!m.approved && m.confidence < 0.85) {
      risks.push({
        id: `conf-${m.source}`,
        column: m.source,
        severity: "warn",
        title: "Low-confidence mapping",
        detail: `Mapped to "${m.target}" at ${(m.confidence * 100).toFixed(0)}% — review before execute.`,
      });
    }

    if (NUMERIC_HINTS.test(srcType) && m.sample && /[^\d.,\-eE+]/.test(m.sample.replace(/\s/g, ""))) {
      risks.push({
        id: `num-${m.source}`,
        column: m.source,
        severity: "block",
        title: "Numeric type with non-numeric samples",
        detail: `Sample "${m.sample.slice(0, 40)}" may fail integer/decimal load.`,
        suggestedTransform: "cast_number",
      });
    }

    if (DATE_HINTS.test(srcType) && m.transform !== "date_iso") {
      risks.push({
        id: `date-${m.source}`,
        column: m.source,
        severity: "info",
        title: "Date normalization recommended",
        detail: "Normalize to ISO-8601 for cross-engine compatibility (Snowflake, BigQuery).",
        suggestedTransform: "date_iso",
      });
    }

    if (destType && srcType && destType !== srcType) {
      const src = srcType.toLowerCase();
      const dest = destType.toLowerCase();
      const floatToDecimal =
        /\b(float|double|real|float64)\b/.test(src) &&
        /\b(decimal|numeric|number|bignumeric)\b/.test(dest) &&
        !/\b(float|double|real|float64)\b/.test(dest);
      const decimalToInt =
        /\b(decimal|numeric|number|float|double)\b/.test(src) && /\b(int|bigint|smallint)\b/.test(dest);
      const datetimeToDate =
        /\b(timestamp|datetime|timestamptz)\b/.test(src) && /\bdate\b/.test(dest) && !/time/.test(dest);
      const stringToNumber =
        /\b(string|text|varchar|char)\b/.test(src) &&
        /\b(int|bigint|decimal|numeric|number|float|double)\b/.test(dest);
      if (floatToDecimal || decimalToInt || datetimeToDate || stringToNumber) {
        risks.push({
          id: `lossy-${m.source}`,
          column: m.source,
          severity: "block",
          title: floatToDecimal
            ? "IEEE float → fixed-point may lose precision"
            : datetimeToDate
              ? "Datetime → date drops time-of-day"
              : "Possible precision loss",
          detail: floatToDecimal
            ? `${srcType} → ${destType}: keep FLOAT/DOUBLE on the destination, or accept rounding risk and approve on Validate.`
            : `${srcType} → ${destType} may truncate, drop time, or reject values. Open Validate before Execute.`,
          suggestedTransform: decimalToInt || stringToNumber ? "cast_number" : undefined,
        });
      }
    }

    const mappingDest = (m.destType || destType || "").toLowerCase();
    const enumLike =
      m.semanticRole === "string_enum"
      || /active|inactive|pending|invalidated|approved|draft/i.test(m.sample || "");
    if (
      enumLike
      && (mappingDest.includes("bool") || m.transform === "cast_boolean")
    ) {
      risks.push({
        id: `enum-bool-${m.source}`,
        column: m.source,
        severity: "block",
        title: "String enum cannot map to BOOLEAN",
        detail: m.existsInDestination
          ? `Sample "${(m.sample || "").slice(0, 40)}" is a status label but destination column already exists as BOOLEAN — remap to a VARCHAR column or ALTER the destination. Mapping Widen alone will not change DDL.`
          : `Sample "${(m.sample || "").slice(0, 40)}" looks like a status label — use VARCHAR (Widen → VARCHAR), not Cast boolean.`,
      });
    }

    if (m.sample && JSON_LIKE.test(m.sample.trim()) && m.transform !== "parse_json") {
      risks.push({
        id: `json-${m.source}`,
        column: m.source,
        severity: "warn",
        title: "JSON document in scalar column",
        detail: "Compass stores as-is; warehouses need flatten or VARIANT mapping.",
        suggestedTransform: "parse_json",
      });
    }
  }

  return risks.slice(0, 14);
}

export function intelligenceScore(
  analysis?: EnhancedAnalysis | null,
  preflight?: PreflightResult | null,
  typeRisks?: TypeRisk[],
): number {
  if (preflight) return preflight.readiness_score;
  if (!analysis) return 0;
  const penalty = (typeRisks?.filter((r) => r.severity === "block").length ?? 0) * 8
    + (typeRisks?.filter((r) => r.severity === "warn").length ?? 0) * 3;
  return Math.max(0, Math.min(100, Math.round(analysis.quality_score - penalty)));
}

export function summarizeColumns(analysis?: EnhancedAnalysis | null): {
  total: number;
  pii: number;
  highConfidence: number;
  lowConfidence: number;
} {
  const cols = analysis?.columns ?? [];
  return {
    total: cols.length,
    pii: analysis?.pii_columns.length ?? 0,
    highConfidence: cols.filter((c: ColumnAnalysis) => c.confidence >= 0.9).length,
    lowConfidence: cols.filter((c: ColumnAnalysis) => c.confidence < 0.75).length,
  };
}

export function inferSourceFormatLabel(
  analysis?: EnhancedAnalysis | null,
  fileType?: string,
): string {
  if (fileType) return fileType.toUpperCase();
  const method = analysis?.method ?? "";
  if (/json/i.test(method)) return "JSON";
  if (analysis?.columns.some((c) => c.semantic_type === "payment_amount")) return "Payment feed";
  return "Tabular";
}
