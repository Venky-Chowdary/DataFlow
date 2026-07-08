import { API_BASE, ActiveDataContext, Connector, EnhancedAnalysis, ParsedUpload, PipelineSchedule, TransferJob, TransferPlan } from "./types";

const DEFAULT_REQUEST_TIMEOUT_MS = 15000;
const LONG_REQUEST_TIMEOUT_MS = 120000;

type TimedRequestInit = RequestInit & { timeoutMs?: number };

async function apiFetch(input: RequestInfo | URL, init: TimedRequestInit = {}): Promise<Response> {
  const { timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS, signal, ...requestInit } = init;
  if (timeoutMs <= 0) return fetch(input, { ...requestInit, signal });

  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  if (signal?.aborted) {
    controller.abort();
  } else {
    signal?.addEventListener("abort", () => controller.abort(), { once: true });
  }

  try {
    return await fetch(input, { ...requestInit, signal: controller.signal });
  } catch (error) {
    if (controller.signal.aborted) throw new Error("Request timed out");
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function requestJson<T>(urls: string[], fallbackMessage: string): Promise<T> {
  let lastError: unknown;
  for (const url of urls) {
    try {
      const res = await apiFetch(url);
      if (!res.ok) throw new Error(`Request failed with ${res.status}`);
      return (await res.json()) as T;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error(fallbackMessage);
}

/** Build column→sample values map for AI analysis */
export function buildColumnSamples(
  columns: string[],
  rows: Record<string, unknown>[] | undefined,
  limit = 50
): Record<string, string[]> {
  const samples: Record<string, string[]> = {};
  if (!rows?.length) {
    for (const col of columns) samples[col] = [];
    return samples;
  }
  for (const col of columns) {
    samples[col] = rows
      .slice(0, limit)
      .map((r) => (r[col] != null ? String(r[col]) : ""))
      .filter(Boolean);
  }
  return samples;
}

export async function analyzeSchemaEnhanced(
  columns: Record<string, string[]>
): Promise<EnhancedAnalysis> {
  const res = await apiFetch(`${API_BASE}/ai/analyze/enhanced`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns }),
  });
  if (!res.ok) throw new Error("AI analysis failed");
  return res.json();
}

export async function runPreflight(payload: {
  columns: string[];
  column_types: Record<string, string>;
  row_count: number;
  mappings: { source: string; target: string; confidence: number; reason?: string }[];
  connector_id?: string;
  source_connector_id?: string;
  sample_rows?: Record<string, unknown>[];
  estimated_bytes?: number;
  sync_mode?: string;
  schema_policy?: string;
  validation_mode?: string;
  backfill_new_fields?: boolean;
  stream_contracts?: Record<string, unknown>[];
}): Promise<import("./types").PreflightResult> {
  const res = await apiFetch(`${API_BASE}/preflight/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error("Preflight failed");
  return res.json();
}

export async function analyzeDbTransfer(payload: {
  sourceConnectorId: string;
  sourceFormat: string;
  sourceDatabase?: string;
  sourceTable?: string;
  sourceCollection?: string;
  destFormat: string;
  destDatabase: string;
  destTable?: string;
  destCollection?: string;
  destConnectorId?: string;
}): Promise<TransferPlan & { source_columns?: string[]; source_schema?: Record<string, string> }> {
  const isMongo = payload.sourceFormat === "mongodb";
  const isDestMongo = payload.destFormat === "mongodb";
  const body = {
    source: {
      kind: "database",
      format: payload.sourceFormat,
      connector_id: payload.sourceConnectorId,
      database: payload.sourceDatabase || "",
      table: isMongo ? "" : payload.sourceTable || "",
      collection: isMongo ? payload.sourceCollection || payload.sourceTable || "" : "",
    },
    destination: {
      kind: "database",
      format: payload.destFormat,
      connector_id: payload.destConnectorId || "",
      database: payload.destDatabase,
      table: isDestMongo ? "" : payload.destTable || payload.destCollection || "",
      collection: isDestMongo ? payload.destCollection || payload.destTable || "" : "",
    },
  };
  const res = await apiFetch(`${API_BASE}/transfer/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error("Database route analysis failed");
  return res.json();
}

export async function queryRAG(query: string): Promise<{ answer: string; response?: string }> {
  const res = await apiFetch(`${API_BASE}/ai/rag/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error("RAG query failed");
  return res.json();
}

export interface CopilotChatMessage {
  role: "user" | "assistant";
  content: string;
}

export interface CopilotAction {
  type?: string;
  screen?: string;
  route?: string;
  label?: string;
}

export interface CopilotChatResponse {
  answer: string;
  intent: string;
  confidence: number;
  method: string;
  reasoning?: string;
  suggested_actions?: CopilotAction[];
  suggested_prompts?: string[];
  data_insight?: {
    dataset: string;
    columns: number;
    rows: number;
    pii_count: number;
    quality_score: number;
  };
  tools_used?: { name: string; success: boolean; summary: string }[];
}

export interface PilotToolRegistry {
  tool_count: number;
  generated_action_count: number;
  total_routable_actions: number;
  families: {
    id: string;
    label: string;
    tools: string[];
    tool_count: number;
    generated_actions: number;
  }[];
  tools: {
    name: string;
    description: string;
    parameters: Record<string, unknown>;
  }[];
}

export interface ModelCapabilities {
  active_provider: string;
  active_model: string;
  agent_mode: string;
  fallback_order: string[];
  providers: {
    provider: string;
    label: string;
    default_model: string;
    tier: string;
    roles: string[];
    best_for: string;
    configured: boolean;
    package_installed: boolean;
    available: boolean;
    status: string;
  }[];
  guarantees: string[];
}

export async function copilotChat(
  message: string,
  history: CopilotChatMessage[] = [],
  dataContext?: ActiveDataContext | null
): Promise<CopilotChatResponse> {
  const body: Record<string, unknown> = { message, history };
  if (dataContext) {
    body.data_context = {
      name: dataContext.name,
      filename: dataContext.filename,
      columns: dataContext.columns,
      row_count: dataContext.row_count,
      samples: dataContext.samples,
    };
  }
  const res = await apiFetch(`${API_BASE}/copilot/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error("Copilot chat failed");
  return res.json();
}

export async function fetchCopilotPrompts(): Promise<string[]> {
  const res = await apiFetch(`${API_BASE}/copilot/prompts`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.prompts || [];
}

export async function fetchPilotTools(): Promise<PilotToolRegistry> {
  const res = await apiFetch(`${API_BASE}/copilot/tools`);
  if (!res.ok) throw new Error("Data Pilot tools failed");
  return res.json();
}

export async function fetchModelCapabilities(): Promise<ModelCapabilities> {
  const res = await apiFetch(`${API_BASE}/copilot/models`);
  if (!res.ok) throw new Error("Model capabilities failed");
  return res.json();
}

export async function fetchMcpManifest() {
  const res = await apiFetch(`${API_BASE}/mcp/manifest`);
  if (!res.ok) throw new Error("MCP manifest failed");
  return res.json();
}

export async function fetchMcpStatus() {
  const res = await apiFetch(`${API_BASE}/mcp/status`);
  if (!res.ok) throw new Error("MCP status failed");
  return res.json();
}

export async function fetchCopilotStatus() {
  const res = await apiFetch(`${API_BASE}/copilot/status`);
  if (!res.ok) throw new Error("Copilot status failed");
  return res.json();
}

export interface CatalogConnector {
  id: string;
  name: string;
  category: string;
  status: string;
  description: string;
}

export async function fetchCatalogConnectors(options: {
  q?: string;
  role?: string;
  category?: string;
  status?: string;
  limit?: number;
} = {}) {
  const params = new URLSearchParams();
  if (options.q) params.set("q", options.q);
  if (options.role) params.set("role", options.role);
  if (options.category) params.set("category", options.category);
  if (options.status) params.set("status", options.status);
  if (options.limit) params.set("limit", String(options.limit));
  const res = await apiFetch(`${API_BASE}/catalog/connectors?${params}`);
  if (!res.ok) throw new Error("Catalog fetch failed");
  return res.json();
}

export async function fetchCatalogStats(): Promise<{
  total: number;
  live: number;
  beta: number;
  planned: number;
  categories: number;
}> {
  const res = await apiFetch(`${API_BASE}/catalog/stats`);
  if (!res.ok) throw new Error("Catalog stats fetch failed");
  return res.json();
}

export async function fetchConnectors(): Promise<Connector[]> {
  const normalize = (c: Record<string, unknown>): Connector => ({
    id: String(c.id ?? c._id ?? ""),
    name: String(c.name ?? ""),
    type: String(c.type ?? ""),
    host: String(c.host ?? ""),
    port: Number(c.port ?? 0),
    database: String(c.database ?? ""),
    status: String(c.status ?? "configured"),
    created_at: String(c.created_at ?? new Date().toISOString()),
  });

  const seen = new Map<string, Connector>();
  for (const url of [`${API_BASE}/connectors/saved`, `${API_BASE}/connectors/`]) {
    try {
      const res = await apiFetch(url);
      if (!res.ok) continue;
      const data = await res.json();
      for (const raw of data.connectors || []) {
        const c = normalize(raw as Record<string, unknown>);
        if (c.id) seen.set(c.id, c);
      }
    } catch {
      /* try next */
    }
  }
  return Array.from(seen.values());
}

export async function fetchJobs(): Promise<TransferJob[]> {
  const data = await requestJson<{ jobs?: TransferJob[] }>(
    [`${API_BASE}/connectors/jobs`, `${API_BASE}/jobs`],
    "Failed to load jobs"
  );
  return data.jobs || [];
}

export type JobProgress = import("./types").JobProgress;

export async function fetchJob(jobId: string): Promise<JobProgress> {
  return requestJson<JobProgress>(
    [`${API_BASE}/connectors/jobs/${jobId}`, `${API_BASE}/jobs/${jobId}`],
    "Job not found"
  );
}

export async function retryJob(jobId: string): Promise<{ job_id: string; retry_of: string }> {
  const urls = [
    `${API_BASE}/connectors/jobs/${jobId}/retry`,
    `${API_BASE}/jobs/${jobId}/retry`,
  ];
  let lastError: unknown;
  for (const url of urls) {
    try {
      const res = await apiFetch(url, { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(typeof data.detail === "string" ? data.detail : "Retry failed");
      }
      return data as { job_id: string; retry_of: string };
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Retry failed");
}

/** Subscribe to live job progress via SSE with polling fallback. Returns cleanup function. */
export function streamJobProgress(
  jobId: string,
  onUpdate: (job: JobProgress) => void,
  onError?: (err: Error) => void,
): () => void {
  let stopped = false;
  let pollTimer: ReturnType<typeof setInterval> | null = null;

  const normalize = (raw: Record<string, unknown>): JobProgress => ({
    _id: String(raw._id ?? raw.job_id ?? jobId),
    source_type: String(raw.source_type ?? ""),
    source_name: String(raw.source_name ?? raw.source ?? ""),
    destination_type: String(raw.destination_type ?? ""),
    destination_database: String(raw.destination_database ?? ""),
    destination_collection: String(raw.destination_collection ?? ""),
    status: String(raw.status ?? "pending"),
    records_processed: Number(raw.records_processed ?? raw.rows_processed ?? 0),
    total_rows: Number(raw.total_rows ?? 0),
    progress_pct: Number(raw.progress_pct ?? 0),
    phase: raw.phase ? String(raw.phase) : undefined,
    message: raw.message ? String(raw.message) : undefined,
    operation: raw.operation ? String(raw.operation) : undefined,
    error: raw.error ? String(raw.error) : undefined,
    chunk_current: raw.chunk_current != null ? Number(raw.chunk_current) : undefined,
    chunk_total: raw.chunk_total != null ? Number(raw.chunk_total) : undefined,
    created_at: String(raw.created_at ?? new Date().toISOString()),
  });

  const startPolling = () => {
    if (pollTimer) return;
    const poll = async () => {
      if (stopped) return;
      try {
        const job = await fetchJob(jobId);
        onUpdate(job);
        if (job.status === "completed" || job.status === "failed") {
          if (pollTimer) clearInterval(pollTimer);
        }
      } catch (e) {
        onError?.(e instanceof Error ? e : new Error("Poll failed"));
      }
    };
    poll();
    pollTimer = setInterval(poll, 800);
  };

  try {
    const es = new EventSource(`${API_BASE}/connectors/jobs/${jobId}/stream`);
    es.onmessage = (ev) => {
      if (stopped) return;
      try {
        const job = normalize(JSON.parse(ev.data));
        onUpdate(job);
        if (job.status === "completed" || job.status === "failed") {
          es.close();
        }
      } catch {
        /* ignore parse errors */
      }
    };
    es.onerror = () => {
      es.close();
      startPolling();
    };
    return () => {
      stopped = true;
      es.close();
      if (pollTimer) clearInterval(pollTimer);
    };
  } catch {
    startPolling();
    return () => {
      stopped = true;
      if (pollTimer) clearInterval(pollTimer);
    };
  }
}

export async function deleteConnector(id: string): Promise<void> {
  for (const url of [`${API_BASE}/connectors/saved/${id}`, `${API_BASE}/connectors/${id}`]) {
    const res = await apiFetch(url, { method: "DELETE" });
    if (res.ok) return;
  }
  throw new Error("Failed to delete connector");
}

export async function testSavedConnector(id: string): Promise<{ success: boolean; message: string }> {
  const res = await apiFetch(`${API_BASE}/connectors/saved/${id}/test`, { method: "POST" });
  if (!res.ok) throw new Error("Connection test failed");
  return res.json();
}

export async function fetchSchedules(): Promise<PipelineSchedule[]> {
  const res = await apiFetch(`${API_BASE}/schedules/`);
  if (!res.ok) throw new Error("Failed to fetch schedules");
  return res.json();
}

export async function createSchedule(payload: {
  name: string;
  source_connector_id: string;
  source_table: string;
  dest_connector_id: string;
  dest_table: string;
  interval: "hourly" | "daily" | "weekly";
  enabled?: boolean;
}): Promise<PipelineSchedule> {
  const res = await apiFetch(`${API_BASE}/schedules/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error("Failed to create schedule");
  return res.json();
}

export async function updateSchedule(
  id: string,
  payload: Partial<{
    name: string;
    source_connector_id: string;
    source_table: string;
    dest_connector_id: string;
    dest_table: string;
    interval: "hourly" | "daily" | "weekly";
    enabled: boolean;
  }>,
): Promise<PipelineSchedule> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error("Failed to update schedule");
  return res.json();
}

export async function deleteSchedule(id: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete schedule");
}

export async function runScheduleNow(id: string): Promise<{ job_id: string }> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}/run`, { method: "POST" });
  if (!res.ok) throw new Error("Failed to run schedule");
  return res.json();
}

export async function testConnection(payload: {
  type: string;
  host?: string;
  port?: number;
  database: string;
  schema?: string;
  username?: string;
  password?: string;
  connection_string?: string;
}): Promise<{ success: boolean; message: string }> {
  const res = await apiFetch(`${API_BASE}/connectors/test`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function saveConnector(payload: {
  name: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username?: string;
  password?: string;
  schema?: string;
  connection_string?: string;
  warehouse?: string;
}): Promise<Connector> {
  for (const url of [`${API_BASE}/connectors/saved`, `${API_BASE}/connectors/`]) {
    const res = await apiFetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.ok) return res.json();
  }
  throw new Error("Failed to save connector");
}

export async function updateConnector(
  id: string,
  payload: {
    name: string;
    type: string;
    host: string;
    port: number;
    database: string;
    username?: string;
    password?: string;
    schema?: string;
    connection_string?: string;
    warehouse?: string;
  },
): Promise<Connector> {
  for (const url of [`${API_BASE}/connectors/saved/${id}`, `${API_BASE}/connectors/${id}`]) {
    const res = await apiFetch(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.ok) return res.json();
  }
  throw new Error("Failed to update connector");
}

export async function uploadFile(file: File): Promise<ParsedUpload> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await apiFetch(`${API_BASE}/connectors/upload`, { method: "POST", body: formData, timeoutMs: LONG_REQUEST_TIMEOUT_MS });
  if (!res.ok) throw new Error("Upload failed");
  return res.json();
}

export async function fetchTransferCapabilities() {
  const res = await apiFetch(`${API_BASE}/transfer/capabilities`);
  if (!res.ok) throw new Error("Failed to load capabilities");
  return res.json();
}

export async function analyzeFileTransfer(
  file: File,
  options: {
    destKind?: string;
    destFormat?: string;
    destDatabase?: string;
    destTable?: string;
    destCollection?: string;
  } = {}
): Promise<TransferPlan> {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("dest_kind", options.destKind || "database");
  formData.append("dest_format", options.destFormat || "mongodb");
  formData.append("dest_database", options.destDatabase || "test_db");
  if (options.destTable) formData.append("dest_table", options.destTable);
  if (options.destCollection) formData.append("dest_collection", options.destCollection);
  const res = await apiFetch(`${API_BASE}/transfer/analyze-file`, { method: "POST", body: formData, timeoutMs: LONG_REQUEST_TIMEOUT_MS });
  if (!res.ok) throw new Error("Analysis failed");
  return res.json();
}

export async function runUniversalTransfer(options: {
  file?: File;
  sourceKind?: string;
  sourceFormat?: string;
  sourceConnectorId?: string;
  sourceDatabase?: string;
  sourceTable?: string;
  sourceCollection?: string;
  destKind?: string;
  destFormat?: string;
  destDatabase?: string;
  destSchema?: string;
  destTable?: string;
  destCollection?: string;
  destConnectorId?: string;
  destHost?: string;
  destPort?: number;
  destUsername?: string;
  destPassword?: string;
  destWarehouse?: string;
  skipPreflight?: boolean;
  mappings?: { source: string; target: string; confidence: number; reason?: string }[];
  syncMode?: string;
  schemaPolicy?: string;
  validationMode?: string;
  backfillNewFields?: boolean;
  streamContracts?: Record<string, unknown>[];
}) {
  const formData = new FormData();
  if (options.file) formData.append("file", options.file);
  formData.append("source_kind", options.sourceKind || "file");
  if (options.sourceFormat) formData.append("source_format", options.sourceFormat);
  formData.append("dest_kind", options.destKind || "database");
  formData.append("dest_format", options.destFormat || "mongodb");
  formData.append("dest_database", options.destDatabase || "test_db");
  formData.append("dest_schema", options.destSchema || "public");
  formData.append("sync_mode", options.syncMode || "full_refresh_overwrite");
  formData.append("schema_policy", options.schemaPolicy || "manual_review");
  formData.append("validation_mode", options.validationMode || "strict");
  formData.append("backfill_new_fields", options.backfillNewFields === true ? "true" : "false");
  if (options.destTable) formData.append("dest_table", options.destTable);
  if (options.destCollection) formData.append("dest_collection", options.destCollection);
  formData.append("skip_preflight", options.skipPreflight === true ? "true" : "false");
  if (options.sourceConnectorId) formData.append("source_connector_id", options.sourceConnectorId);
  if (options.sourceDatabase) formData.append("source_database", options.sourceDatabase);
  if (options.sourceTable) formData.append("source_table", options.sourceTable);
  if (options.sourceCollection) formData.append("source_collection", options.sourceCollection);
  if (options.destConnectorId) formData.append("dest_connector_id", options.destConnectorId);
  if (options.destHost) formData.append("dest_host", options.destHost);
  if (options.destPort) formData.append("dest_port", String(options.destPort));
  if (options.destUsername) formData.append("dest_username", options.destUsername);
  if (options.destPassword) formData.append("dest_password", options.destPassword);
  if (options.destWarehouse) formData.append("dest_warehouse", options.destWarehouse);
  if (options.mappings?.length) {
    formData.append("mappings_json", JSON.stringify(options.mappings));
  }
  if (options.streamContracts?.length) {
    formData.append("stream_contracts_json", JSON.stringify(options.streamContracts));
  }
  const res = await apiFetch(`${API_BASE}/transfer/run`, { method: "POST", body: formData, timeoutMs: LONG_REQUEST_TIMEOUT_MS });
  const data = await res.json();
  if (!res.ok) {
    const detail = data.detail;
    const errMsg = typeof detail === "string"
      ? detail
      : detail?.error || detail?.message || JSON.stringify(detail) || "Transfer failed";
    return { success: false, error: errMsg };
  }
  return { success: true, async: data.async === true, ...data };
}

export async function transferFile(
  file: File,
  destinationDatabase: string,
  destinationCollection: string,
  options: {
    connectorId?: string;
    skipPreflight?: boolean;
    destType?: string;
    destHost?: string;
    destPort?: number;
    destSchema?: string;
    destUsername?: string;
    destPassword?: string;
    destWarehouse?: string;
    syncMode?: string;
    schemaPolicy?: string;
    validationMode?: string;
    backfillNewFields?: boolean;
    streamContracts?: Record<string, unknown>[];
  } = {}
) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("destination_database", destinationDatabase);
  formData.append("destination_collection", destinationCollection);
  formData.append("skip_preflight", options.skipPreflight === true ? "true" : "false");
  formData.append("dest_type", options.destType || "mongodb");
  formData.append("sync_mode", options.syncMode || "full_refresh_overwrite");
  formData.append("schema_policy", options.schemaPolicy || "manual_review");
  formData.append("validation_mode", options.validationMode || "strict");
  formData.append("backfill_new_fields", options.backfillNewFields === true ? "true" : "false");
  if (options.connectorId) formData.append("connector_id", options.connectorId);
  if (options.destHost) formData.append("dest_host", options.destHost);
  if (options.destPort) formData.append("dest_port", String(options.destPort));
  if (options.destSchema) formData.append("dest_schema", options.destSchema);
  if (options.destUsername) formData.append("dest_username", options.destUsername);
  if (options.destPassword) formData.append("dest_password", options.destPassword);
  if (options.destWarehouse) formData.append("dest_warehouse", options.destWarehouse);
  if (options.streamContracts?.length) {
    formData.append("stream_contracts_json", JSON.stringify(options.streamContracts));
  }
  const res = await apiFetch(`${API_BASE}/connectors/transfer`, { method: "POST", body: formData, timeoutMs: LONG_REQUEST_TIMEOUT_MS });
  const data = await res.json();
  if (!res.ok) {
    const detail = data.detail;
    const errMsg = typeof detail === "string"
      ? detail
      : detail?.error || detail?.message || JSON.stringify(detail) || "Transfer failed";
    return { success: false, error: errMsg, preflight: detail?.preflight };
  }
  return { success: true, async: data.async === true, ...data };
}
