import type { EditableMapping } from "./mapping";
import type { ColumnAnalysis, EnhancedAnalysis, PreflightResult, TransferPlan } from "./types";
import {
  decimalWouldCollapse,
  effectiveDestCarrier,
  parseDecimalPrecisionScale,
  parseStringCarrierWidth,
  sampleExceedsStringWidth,
  stringWidthWouldNarrow,
} from "./typeCarrierFidelity";

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

export type FidelityRiskOptions = {
  /** Destination connector id (stripe, shopify, hubspot, …) for SaaS width defaults. */
  destConnector?: string | null;
};

/** Short badge label for Map pair chips. */
export function fidelityChipLabel(risk: TypeRisk): string {
  if (risk.id.startsWith("width-") || risk.id.startsWith("sample-width-")) return "width";
  if (risk.id.startsWith("scale-")) return "scale";
  if (risk.id.startsWith("enum-bool-")) return "type";
  return "fidelity";
}

/** Pair-row fidelity chip — same rules as detectTypeRisks, keyed by source. */
export function fidelityRiskForMapping(
  mapping: EditableMapping,
  opts?: FidelityRiskOptions,
): TypeRisk | null {
  const risks = detectTypeRisks([mapping], null, null, opts);
  const fidelity = risks.find(
    (r) =>
      r.id.startsWith("lossy-") ||
      r.id.startsWith("width-") ||
      r.id.startsWith("scale-") ||
      r.id.startsWith("sample-width-") ||
      r.id.startsWith("enum-bool-") ||
      (r.severity === "block" && /precision|lossy|timezone|datetime|ieee|float|width|scale/i.test(r.title)),
  );
  return fidelity ?? risks.find((r) => r.severity === "block") ?? null;
}

export function detectTypeRisks(
  mappings: EditableMapping[],
  analysis?: EnhancedAnalysis | null,
  transferPlan?: TransferPlan | null,
  opts?: FidelityRiskOptions,
): TypeRisk[] {
  const risks: TypeRisk[] = [];
  const bySource = new Map(analysis?.columns.map((c) => [c.column_name, c]) ?? []);
  const destConnector = opts?.destConnector ?? null;

  for (const m of mappings) {
    const col = bySource.get(m.source);
    const srcTypeRaw = m.inferredType ?? col?.inferred_type ?? col?.semantic_type ?? "string";
    const srcType = srcTypeRaw.toLowerCase();
    const planMap = transferPlan?.type_mappings?.find(
      (t) => t.column === m.source || t.column === m.target,
    );
    // Prefer live Map destType (operator/pipeline stamp) over transfer-plan snapshot.
    // SaaS catalog fills width when stamped type is bare string/varchar.
    const stampedDest = m.destType || planMap?.dest_type || "";
    const destCarrier = effectiveDestCarrier(stampedDest, destConnector, m.target);
    const destType = destCarrier.toLowerCase();

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
      const decimalToFloat =
        /\b(decimal|numeric|number|bignumeric)\b/.test(src) &&
        /\b(float|double|real|float64)\b/.test(dest);
      const decimalToInt =
        /\b(decimal|numeric|number|float|double)\b/.test(src) &&
        /\b(int|integer|bigint|smallint|tinyint)\b/.test(dest);
      const datetimeToDate =
        /\b(timestamp|datetime|timestamptz)\b/.test(src) && /\bdate\b/.test(dest) && !/time/.test(dest);
      const stringToNumber =
        /\b(string|text|varchar|char)\b/.test(src) &&
        /\b(int|integer|bigint|decimal|numeric|number|float|double)\b/.test(dest);
      const tzToNtz =
        /\b(timestamptz|timestamp with time zone|timestamp_tz|timestamp_ltz)\b/.test(src) &&
        /\b(timestamp_ntz|datetime|timestamp without time zone)\b/.test(dest);
      const ntzToTz =
        /\b(timestamp_ntz|datetime|timestamp without time zone)\b/.test(src) &&
        /\b(timestamptz|timestamp with time zone|timestamp_tz|timestamp_ltz|datetimeoffset)\b/.test(dest);
      const timeToTimetz =
        /\btime\b/.test(src) && !/time\s*zone|timetz/.test(src) &&
        /\b(timetz|time with time zone)\b/.test(dest);
      const dateToTz =
        /\bdate\b/.test(src) && !/time/.test(src) &&
        /\b(timestamptz|timestamp with time zone|timestamp_tz|timestamp_ltz|datetimeoffset)\b/.test(dest);
      const jsonToString =
        /\b(jsonb?|variant|super)\b/.test(src) &&
        /\b(string|text|varchar|nvarchar|char)\b/.test(dest);
      const stringToJson =
        /\b(string|text|varchar|nvarchar|char)\b/.test(src) &&
        /\b(jsonb?|variant|super)\b/.test(dest);
      const intWidthNarrow =
        /\b(int64|bigint|long|int8)\b/.test(src) &&
        /\b(int32|integer|int4|int\b|smallint|tinyint)\b/.test(dest) &&
        !/\b(int64|bigint|long)\b/.test(dest);
      if (
        floatToDecimal || decimalToFloat || decimalToInt || datetimeToDate || stringToNumber
        || tzToNtz || ntzToTz || timeToTimetz || dateToTz || jsonToString || stringToJson
        || intWidthNarrow
      ) {
        risks.push({
          id: `lossy-${m.source}`,
          column: m.source,
          severity: "block",
          title: floatToDecimal
            ? "IEEE float → fixed-point may lose precision"
            : decimalToFloat
              ? "Fixed-point → IEEE float may lose magnitude/scale"
              : datetimeToDate
                ? "Datetime → date drops time-of-day"
                : tzToNtz
                  ? "Timestamptz → NTZ drops timezone polarity"
                  : ntzToTz || timeToTimetz || dateToTz
                    ? "Wall-clock → timezone-aware invents an offset"
                    : jsonToString
                      ? "Document → open string drops JSON validation domain"
                      : stringToJson
                        ? "Open string → document invents JSON validation domain"
                        : intWidthNarrow
                          ? "Integer width narrows (e.g. INT64 → INT) — values may truncate"
                          : "Possible precision loss",
          detail: floatToDecimal
            ? `${srcTypeRaw} → ${destCarrier}: keep FLOAT/DOUBLE on the destination, or accept rounding risk and approve on Validate.`
            : decimalToFloat
              ? `${srcTypeRaw} → ${destCarrier}: keep DECIMAL/NUMERIC on the destination — IEEE cannot prove exact scale.`
              : `${srcTypeRaw} → ${destCarrier} may truncate, drop time, or reject values. Open Validate before Execute.`,
          suggestedTransform: decimalToInt || stringToNumber ? "cast_number" : undefined,
        });
      }
    }

    // Width / scale attributes (HVR class) — SaaS VARCHAR(n) and DECIMAL(p,s).
    if (stringWidthWouldNarrow(srcTypeRaw, destCarrier)) {
      const srcW = parseStringCarrierWidth(srcTypeRaw);
      const tgtW = parseStringCarrierWidth(destCarrier);
      risks.push({
        id: `width-${m.source}`,
        column: m.source,
        severity: "block",
        title: "String width narrows — truncate risk",
        detail:
          srcW != null && tgtW != null
            ? `${srcTypeRaw} → ${destCarrier}: source capacity ${srcW} exceeds destination ${tgtW}. Widen the dest field, remap, or expect quarantine on Validate/write.`
            : `${srcTypeRaw} → ${destCarrier}: unbounded/TEXT source into bounded VARCHAR — overflow rows quarantine (no silent truncate).`,
      });
    } else if (sampleExceedsStringWidth(m.sample, destCarrier)) {
      const tgtW = parseStringCarrierWidth(destCarrier);
      risks.push({
        id: `sample-width-${m.source}`,
        column: m.source,
        severity: "block",
        title: "Sample exceeds destination width",
        detail: `Sample length ${[...String(m.sample || "")].length} > ${destCarrier}${tgtW != null ? ` (${tgtW})` : ""}. Fix source value or widen destination before Execute.`,
      });
    }

    if (decimalWouldCollapse(srcTypeRaw, destCarrier)) {
      const srcPs = parseDecimalPrecisionScale(srcTypeRaw);
      const tgtPs = parseDecimalPrecisionScale(destCarrier);
      risks.push({
        id: `scale-${m.source}`,
        column: m.source,
        severity: "block",
        title: "Decimal precision/scale collapses",
        detail:
          srcPs && tgtPs
            ? `${srcTypeRaw} → ${destCarrier}: source needs ${srcPs.precision - srcPs.scale} integer digits / scale ${srcPs.scale}; dest allows ${tgtPs.precision - tgtPs.scale} / ${tgtPs.scale}. Expect quarantine or rounding loss.`
            : `${srcTypeRaw} → ${destCarrier}: destination DECIMAL cannot hold source precision/scale.`,
      });
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
): number | null {
  if (preflight) return preflight.readiness_score;
  // No measured analysis — never invent 0% as a quality score.
  if (!analysis) return null;
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
