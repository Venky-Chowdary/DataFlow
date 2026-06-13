import {
  endpointSummary,
  inferOperation,
  type DatabaseConnection,
  type EndpointConfig,
} from "./types";

export type { WizardStep } from "./types";
export {
  DATABASE_OPTIONS,
  emptyEndpoint,
  endpointSummary,
  inferOperation,
  operationLabel,
  type DatabaseConnection,
  type EndpointConfig,
} from "./types";

export interface ColumnSchema {
  name: string;
  inferred_type: string;
  nullable: boolean;
  samples: string[];
}

export interface MappingResult {
  source: string;
  target: string;
  confidence: number;
  reasoning: string;
  transform?: string;
  user_override?: boolean;
}

export interface SemanticColumnAnalysis {
  name: string;
  inferred_type: string;
  semantic_role: string;
  confidence: number;
  detection_source: string;
  description: string;
  samples: string[];
}

export interface MappingPipelineResult {
  mappings: MappingResult[];
  transforms: { source: string; target: string; expression: string }[];
  validation: { passed: boolean; issues: string[] };
  semantic_analysis?: SemanticColumnAnalysis[];
  classification?: { format: string; confidence: number };
}

export interface SavedConnector {
  id: string;
  name: string;
  type: string;
  role: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema: string;
  connection_string: string;
  ssl: boolean;
  warehouse: string;
  last_tested_at: string | null;
  last_test_ok: boolean;
  created_at: string;
}

export interface GateStatTile {
  id: string;
  label: string;
  status: string;
  count: number;
}

export interface GateResult {
  gate_id: string;
  status: string;
  message: string;
  duration_ms: number;
}

export interface PreflightResponse {
  passed: boolean;
  passed_count: number;
  total_gates: number;
  gates: GateResult[];
  blockers: GateResult[];
}

export interface ReconciliationData {
  passed: boolean;
  source_rows: number;
  target_rows: number;
  source_checksum: string;
  target_checksum: string;
  message: string;
}

export interface TransferResponse {
  job_id: string;
  status: string;
  operation: string;
  message: string;
  async?: boolean;
  rows_processed?: number;
  table?: string;
  reconciliation?: ReconciliationData;
  driver?: string;
}

export interface JobDetail {
  job_id: string;
  status: string;
  operation: string;
  source: string;
  destination: string;
  rows_processed: number;
  total_rows: number;
  current_chunk: number;
  total_chunks: number;
  table_name: string;
  driver: string;
  workflow_phase: string;
  message: string;
  created_at: string;
  checkpoints: { chunk: number; total: number; rows: number; at: string }[];
  reconciliation: ReconciliationData | null;
}

export interface PlatformStats {
  total_jobs: number;
  completed: number;
  active: number;
  failed: number;
  rows_transferred: number;
}

export interface UploadResponse {
  file_id: string;
  filename: string;
  format: string;
  encoding?: string;
  delimiter?: string;
  row_count: number;
  columns: ColumnSchema[];
  preview_rows: string[][];
}

export interface JobListItem {
  job_id: string;
  status: string;
  operation: string;
  source: string;
  destination: string;
  rows_processed: number;
  created_at: string;
}

const DEFAULT_TARGET_COLUMNS = [
  "customer_id",
  "payment_amount",
  "transaction_date",
  "account_number",
  "currency_code",
  "reference_number",
  "status",
  "description",
];

export async function uploadFile(file: File): Promise<UploadResponse> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch("/api/v1/files/upload", { method: "POST", body: form });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail ?? "Upload failed");
  }
  return res.json();
}

export async function fetchSemanticMappings(
  sourceColumns: string[],
  targetColumns: string[] = DEFAULT_TARGET_COLUMNS,
  options: {
    fileFormat?: string | null;
    sourceSchemas?: ColumnSchema[];
    targetSchemas?: ColumnSchema[];
  } = {}
): Promise<MappingPipelineResult> {
  const res = await fetch("/api/v1/map", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      source_columns: sourceColumns,
      target_columns: targetColumns,
      source_schemas: options.sourceSchemas ?? [],
      target_schemas: options.targetSchemas ?? [],
      file_format: options.fileFormat ?? undefined,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail ?? "Semantic mapping failed");
  }
  return res.json();
}

export async function analyzeColumnSchema(columns: ColumnSchema[]): Promise<{ columns: SemanticColumnAnalysis[] }> {
  const res = await fetch("/api/v1/analyze/schema", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns }),
  });
  if (!res.ok) throw new Error("Semantic analysis failed");
  return res.json();
}

export async function fetchConnectorCatalog(params: {
  q?: string;
  category?: string;
  status?: string;
  offset?: number;
  limit?: number;
} = {}): Promise<{
  total: number;
  offset: number;
  limit: number;
  categories: string[];
  live_count: number;
  catalog_total: number;
  connectors: CatalogConnector[];
}> {
  const sp = new URLSearchParams();
  if (params.q) sp.set("q", params.q);
  if (params.category) sp.set("category", params.category);
  if (params.status) sp.set("status", params.status);
  if (params.offset != null) sp.set("offset", String(params.offset));
  if (params.limit != null) sp.set("limit", String(params.limit));
  const res = await fetch(`/api/v1/connectors/catalog?${sp}`);
  if (!res.ok) throw new Error("Connector catalog unavailable");
  return res.json();
}

export interface CatalogConnector {
  id: string;
  name: string;
  category: string;
  status: "live" | "beta" | "planned" | "ai";
  description: string;
}

export async function fetchSavedConnectors(role?: string): Promise<SavedConnector[]> {
  const q = role ? `?role=${encodeURIComponent(role)}` : "";
  const res = await fetch(`/api/v1/connectors/saved${q}`);
  if (!res.ok) throw new Error("Could not load saved connectors");
  const data = await res.json();
  return data.connectors;
}

export async function fetchSavedConnector(id: string): Promise<SavedConnector> {
  const res = await fetch(`/api/v1/connectors/saved/${id}`);
  if (!res.ok) throw new Error("Connector not found");
  return res.json();
}

export async function saveConnector(payload: {
  name: string;
  type: string;
  role: string;
  connection_string: string;
  warehouse?: string;
}): Promise<SavedConnector> {
  const res = await fetch("/api/v1/connectors/saved", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail ?? "Could not save connector");
  }
  return res.json();
}

export async function deleteSavedConnector(id: string): Promise<void> {
  const res = await fetch(`/api/v1/connectors/saved/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Could not delete connector");
}

export async function testSavedConnector(
  id: string
): Promise<{ ok: boolean; error?: string; tables?: string[]; message?: string }> {
  const res = await fetch(`/api/v1/connectors/saved/${id}/test`, { method: "POST" });
  return res.json();
}

export async function introspectSchema(
  db: DatabaseConnection,
  table?: string
): Promise<{ ok: boolean; columns: { name: string; inferred_type: string; nullable: boolean }[]; tables: string[]; error?: string }> {
  const res = await fetch("/api/v1/schema/introspect", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: db.type,
      host: db.host,
      port: db.port,
      database: db.database,
      username: db.username,
      password: db.password,
      schema: db.schema,
      connection_string: db.connectionString,
      ssl: db.ssl,
      warehouse: db.warehouse,
      table,
    }),
  });
  if (!res.ok) throw new Error("Schema introspection failed");
  return res.json();
}

export async function fetchGateStats(): Promise<{ gates: GateStatTile[]; sample_size: number }> {
  const res = await fetch("/api/v1/stats/gates");
  if (!res.ok) throw new Error("Gate stats unavailable");
  return res.json();
}

export async function fetchPlatformStats(): Promise<PlatformStats> {
  const res = await fetch("/api/v1/stats");
  if (!res.ok) throw new Error("Stats unavailable");
  return res.json();
}

export async function fetchJobs(limit = 20): Promise<JobListItem[]> {
  const res = await fetch(`/api/v1/jobs?limit=${limit}`);
  if (!res.ok) throw new Error("Jobs unavailable");
  const data = await res.json();
  return data.jobs;
}

export async function fetchJob(jobId: string): Promise<JobDetail> {
  const res = await fetch(`/api/v1/jobs/${jobId}`);
  if (!res.ok) throw new Error("Job not found");
  return res.json();
}

export async function generateConnector(
  openapi: object
): Promise<{
  connector_id: string;
  name: string;
  version: string;
  base_url: string;
  auth_type: string;
  endpoint_count: number;
  plugin_code: string;
  certification: { status: string; next_step: string };
}> {
  const res = await fetch("/api/v1/connectors/generate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ openapi }),
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail ?? "Connector generation failed");
  }
  return res.json();
}

export async function testConnection(
  db: DatabaseConnection
): Promise<{ ok: boolean; error?: string; tables?: string[]; message?: string; driver?: string }> {
  const res = await fetch("/api/v1/connect/test", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: db.type,
      host: db.host,
      port: db.port,
      database: db.database,
      username: db.username,
      password: db.password,
      schema: db.schema,
      connection_string: db.connectionString,
      ssl: db.ssl,
      warehouse: db.warehouse,
    }),
  });
  return res.json();
}

export async function resolveMappings(
  source: EndpointConfig,
  dest: EndpointConfig
): Promise<MappingPipelineResult> {
  let sourceCols: string[] = [];
  if (source.kind === "file" && source.file.columns?.length) {
    sourceCols = source.file.columns.map((c) => c.name);
  } else if (source.kind === "database" && source.database?.targetColumns.length) {
    sourceCols = source.database.targetColumns.map((c) => c.name);
  } else if (source.kind === "database" && source.database?.tables.length) {
    sourceCols = source.database.tables;
  }

  if (sourceCols.length === 0) {
    throw new Error("No source columns detected — upload a file or connect a database source");
  }

  const targetCols =
    dest.kind === "database" && dest.database?.targetColumns.length
      ? dest.database.targetColumns.map((c) => c.name)
      : dest.kind === "database" && dest.database?.tables.length
        ? dest.database.tables.slice(0, 8)
        : DEFAULT_TARGET_COLUMNS;

  const sourceSchemas =
    source.kind === "file" && source.file.columns?.length
      ? source.file.columns
      : source.kind === "database" && source.database?.targetColumns.length
        ? source.database.targetColumns.map((c) => ({
            name: c.name,
            inferred_type: c.inferred_type,
            nullable: c.nullable,
            samples: [],
          }))
        : undefined;

  const targetSchemas =
    dest.kind === "database" && dest.database?.targetColumns.length
      ? dest.database.targetColumns.map((c) => ({
          name: c.name,
          inferred_type: c.inferred_type,
          nullable: c.nullable,
          samples: [],
        }))
      : undefined;

  return fetchSemanticMappings(sourceCols, targetCols, {
    fileFormat: source.file.detectedFormat,
    sourceSchemas,
    targetSchemas,
  });
}

export function buildPreflightBody(
  source: EndpointConfig,
  dest: EndpointConfig,
  mappings: MappingResult[]
) {
  const sourceDb = source.database;
  const sourceColumns =
    source.kind === "file" && source.file.columns?.length
      ? source.file.columns
      : source.kind === "database" && sourceDb?.targetColumns.length
        ? sourceDb.targetColumns.map((c) => ({
            name: c.name,
            inferred_type: c.inferred_type,
            nullable: c.nullable,
            samples: [],
          }))
        : mappings.map((m) => ({
            name: m.source,
            inferred_type: "VARCHAR",
            nullable: true,
            samples: [],
          }));

  const db = dest.database;

  return {
    operation: inferOperation(source, dest),
    source_kind: source.kind,
    source_connected: source.connected,
    source_parseable: source.kind === "file" ? !!source.file.fileName : source.connected,
    source_columns: sourceColumns,
    source_summary: endpointSummary(source),
    file_id: source.file.fileId ?? null,
    source_db_type: sourceDb?.type ?? "postgresql",
    source_host: sourceDb?.host ?? "",
    source_port: sourceDb?.port ?? 5432,
    source_database: sourceDb?.database ?? "",
    source_username: sourceDb?.username ?? "",
    source_password: sourceDb?.password ?? "",
    source_schema: sourceDb?.schema ?? "public",
    source_connection_string: sourceDb?.connectionString ?? "",
    source_ssl: sourceDb?.ssl ?? true,
    source_table: sourceDb?.sourceTable ?? "",
    dest_kind: dest.kind === "file" ? "file_export" : dest.kind,
    dest_connected: dest.connected,
    dest_can_write: dest.connected,
    dest_columns:
      dest.kind === "database" && dest.database?.targetColumns.length
        ? dest.database.targetColumns.map((c) => ({ name: c.name, inferred_type: c.inferred_type }))
        : mappings.map((m) => ({ name: m.target, inferred_type: "VARCHAR" })),
    dest_summary: endpointSummary(dest),
    dest_db_type: db?.type ?? "postgresql",
    dest_host: db?.host ?? "",
    dest_port: db?.port ?? 5432,
    dest_database: db?.database ?? "",
    dest_username: db?.username ?? "",
    dest_password: db?.password ?? "",
    dest_schema: db?.schema ?? "public",
    dest_connection_string: db?.connectionString ?? "",
    dest_ssl: db?.ssl ?? true,
    dest_warehouse: db?.warehouse ?? "",
    mappings: mappings.map((m) => ({
      source: m.source,
      target: m.target,
      confidence: m.user_override ? 1 : m.confidence,
      transform: m.transform,
      user_override: !!m.user_override,
      reasoning: m.user_override ? "User override" : m.reasoning,
    })),
    required_targets: mappings.map((m) => m.target),
  };
}

export async function runPreflight(
  source: EndpointConfig,
  dest: EndpointConfig,
  mappings: MappingResult[]
): Promise<PreflightResponse> {
  const res = await fetch("/api/v1/preflight", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildPreflightBody(source, dest, mappings)),
  });
  if (!res.ok) throw new Error("Preflight request failed");
  return res.json();
}

export async function startTransfer(
  source: EndpointConfig,
  dest: EndpointConfig,
  mappings: MappingResult[]
): Promise<TransferResponse> {
  const res = await fetch("/api/v1/transfer", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildPreflightBody(source, dest, mappings)),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    const detail = err.detail;
    const message =
      typeof detail === "string"
        ? detail
        : detail?.message ?? detail?.blockers?.[0]?.message ?? "Transfer blocked";
    throw new Error(message);
  }
  return res.json();
}

export { resolveMappings as getMappingsForTransfer };
