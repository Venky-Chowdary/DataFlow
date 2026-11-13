/**
 * Map-step remapping API (`/api/v1/transfer/mapping/*`).
 *
 * Type grades and auto-map suggestions come from the server so the Map step
 * reads the same rules Validate enforces — this client never re-derives them.
 */
import { apiFetch, parseApiError } from "./api";
import { API_BASE } from "./types";
import { catalogFromApi, type DestColumnCatalog } from "./destColumnCatalog";
import type { DocumentMapping } from "./mappingCode";
import type { ShapeStepWire } from "./shape";

export type TypeVerdict = "safe" | "lossy" | "incompatible" | "unknown";

export interface TypeGrade {
  sourceType: string;
  targetType: string;
  verdict: TypeVerdict;
  conversionClass: string;
  reasons: Array<{ kind: string; message: string }>;
  requiresRiskContract: boolean;
}

export interface AutoMapSuggestion {
  source: string;
  target: string;
  confidence: number;
  kind: "exact" | "case_insensitive" | "normalized" | "learned" | "synonym" | "concept" | "key_suffix" | "token" | string;
  reason: string;
  selected: boolean;
  alternatives: Array<{ target: string; confidence: number; kind: string }>;
  compat?: { verdict: TypeVerdict; reasons: Array<{ kind: string; message: string }> };
}

async function postJson<T>(url: string, body: unknown, fallback: string, timeoutMs?: number): Promise<T> {
  const res = await apiFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    ...(timeoutMs ? { timeoutMs } : {}),
  });
  if (!res.ok) throw new Error(await parseApiError(res, fallback));
  return res.json() as Promise<T>;
}

/** The destination table's columns, read off the destination's own catalog. */
export async function fetchDestinationColumns(
  destination: Record<string, unknown>,
  schemaPolicy = "",
): Promise<DestColumnCatalog> {
  const raw = await postJson<Record<string, unknown>>(
    `${API_BASE}/transfer/mapping/destination-columns`,
    { destination, schema_policy: schemaPolicy },
    "Could not read the destination's columns",
    // The server bounds the read by the connector deadline and answers 504.
    120_000,
  );
  return catalogFromApi(raw);
}

export async function fetchTypeCompatibility(
  pairs: Array<{ sourceType: string; targetType: string; transform?: string }>,
  destType: string,
  destTableExists: boolean | null = true,
): Promise<TypeGrade[]> {
  const out = await postJson<{ results: Array<Record<string, unknown>> }>(
    `${API_BASE}/transfer/mapping/type-compatibility`,
    {
      dest_type: destType,
      dest_table_exists: destTableExists,
      pairs: pairs.map((p) => ({
        source_type: p.sourceType,
        target_type: p.targetType,
        transform: p.transform || "none",
      })),
    },
    "Could not grade column types",
  );
  return (out.results ?? []).map((r) => ({
    sourceType: String(r.source_type ?? ""),
    targetType: String(r.target_type ?? ""),
    verdict: (String(r.verdict ?? "unknown") as TypeVerdict),
    conversionClass: String(r.conversion_class ?? ""),
    reasons: Array.isArray(r.reasons) ? (r.reasons as TypeGrade["reasons"]) : [],
    requiresRiskContract: r.requires_risk_contract === true,
  }));
}

export async function fetchAutoMap(body: {
  sourceColumns: Array<{ name: string; type?: string }>;
  destinationColumns: Array<{ name: string; type?: string }>;
  mapped: Array<{ source: string; target: string }>;
  mode: "all" | "remaining";
  sourceTable?: string;
  destTable?: string;
  destType?: string;
}): Promise<AutoMapSuggestion[]> {
  const out = await postJson<{ suggestions: AutoMapSuggestion[] }>(
    `${API_BASE}/transfer/mapping/auto-map`,
    {
      source_columns: body.sourceColumns.map((c) => ({ name: c.name, type: c.type ?? "" })),
      destination_columns: body.destinationColumns.map((c) => ({ name: c.name, type: c.type ?? "" })),
      mapped: body.mapped,
      mode: body.mode,
      source_table: body.sourceTable ?? "",
      dest_table: body.destTable ?? "",
      dest_type: body.destType ?? "",
    },
    "Auto-map failed",
  );
  return out.suggestions ?? [];
}

/** Remember confirmed pairs for this workspace's learned auto-map tier. */
export async function recordMappingMemory(pairs: Array<{ source: string; target: string }>): Promise<void> {
  if (pairs.length === 0) return;
  await postJson(`${API_BASE}/transfer/mapping/memory`, { pairs: pairs.slice(0, 2000) }, "Could not record confirmed mappings");
}

export interface MappingTemplateSummary {
  id: string;
  name: string;
  description: string;
  created_at: string;
  created_by: string;
  mapping_count: number;
  computed_count: number;
}

export interface MappingDocumentBody {
  mappings: DocumentMapping[];
  computed: ShapeStepWire[];
  name?: string;
  source?: { table?: string; type?: string };
  destination?: { table?: string; type?: string };
}

/** The mapping as a YAML/JSON document (schema version + checksum), as text. */
export async function exportMappingDocument(
  body: MappingDocumentBody,
  format: "yaml" | "json",
): Promise<{ text: string; filename: string }> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping/export`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...body, format }),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Export failed"));
  const disposition = res.headers.get("Content-Disposition") || "";
  const filename = /filename="([^"]+)"/.exec(disposition)?.[1] || `mapping.mapping.${format}`;
  return { text: await res.text(), filename };
}

export async function importMappingDocument(document: string): Promise<{
  mappings: DocumentMapping[];
  computed: ShapeStepWire[];
  checksum: string;
  checksum_verified: boolean;
  warnings: string[];
  metadata: { name?: string };
}> {
  return postJson(`${API_BASE}/transfer/mapping/import`, { document }, "Import failed");
}

export async function listMappingTemplates(): Promise<MappingTemplateSummary[]> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping/templates`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not list mapping templates"));
  return ((await res.json()) as { templates: MappingTemplateSummary[] }).templates ?? [];
}

export async function createMappingTemplate(body: MappingDocumentBody & { description?: string }): Promise<MappingTemplateSummary> {
  return postJson(`${API_BASE}/transfer/mapping/templates`, body, "Could not save the template");
}

export async function applyMappingTemplate(
  templateId: string,
  sourceColumns: string[],
  destinationColumns: string[],
): Promise<{ suggestions: AutoMapSuggestion[]; unbound: Array<{ source: string; target: string; missing: string }>; computed: ShapeStepWire[] }> {
  return postJson(
    `${API_BASE}/transfer/mapping/templates/${encodeURIComponent(templateId)}/apply`,
    { source_columns: sourceColumns, destination_columns: destinationColumns },
    "Could not apply the template",
  );
}

export async function deleteMappingTemplate(templateId: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping/templates/${encodeURIComponent(templateId)}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not delete the template"));
}
