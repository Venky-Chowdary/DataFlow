import { API_BASE, ActiveDataContext, Connector, EnhancedAnalysis, ParsedUpload, PipelineSchedule, TransferJob, TransferPlan } from "./types";
import { clearSession, getAuthToken } from "./session";

const DEFAULT_REQUEST_TIMEOUT_MS = 15000;
const LONG_REQUEST_TIMEOUT_MS = 120000;

type TimedRequestInit = RequestInit & { timeoutMs?: number };

/** Fired when the API rejects a request with 401 — AppShell should return to login. */
export const AUTH_REQUIRED_EVENT = "df2:auth-required";

let _authNotifyAt = 0;

function notifyAuthRequired(requestUrl: string) {
  if (/\/auth\/(login|bootstrap|sso)/i.test(requestUrl)) return;
  // Debounce so a burst of 401s (connectors + catalog + jobs) only redirects once.
  const now = Date.now();
  if (now - _authNotifyAt < 1500) return;
  _authNotifyAt = now;
  clearSession();
  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent(AUTH_REQUIRED_EVENT, { detail: { url: requestUrl } }));
  }
}

async function parseApiError(res: Response, fallback: string): Promise<string> {
  try {
    const data = await res.json();
    const detail = data.detail ?? data.error ?? data.message;
    if (typeof detail === "string") return detail;
    if (detail && typeof detail === "object") {
      if (typeof detail.error === "string") return detail.error;
      if (typeof detail.message === "string") return detail.message;
    }
    return fallback;
  } catch {
    return fallback;
  }
}

async function apiFetch(input: RequestInfo | URL, init: TimedRequestInit = {}): Promise<Response> {
  const { timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS, signal, ...requestInit } = init;
  const headers = new Headers(requestInit.headers);
  const token = getAuthToken();
  if (token && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  const mergedInit = { ...requestInit, headers };
  if (timeoutMs <= 0) return fetch(input, { ...mergedInit, signal });

  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  if (signal?.aborted) {
    controller.abort();
  } else {
    signal?.addEventListener("abort", () => controller.abort(), { once: true });
  }

  try {
    const res = await fetch(input, { ...mergedInit, signal: controller.signal });
    if (res.status === 401) {
      notifyAuthRequired(typeof input === "string" ? input : String(input));
    }
    return res;
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
  columns: Record<string, string[]>,
  options?: { timeoutMs?: number },
): Promise<EnhancedAnalysis> {
  const res = await apiFetch(`${API_BASE}/ai/analyze/enhanced`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns }),
    timeoutMs: options?.timeoutMs ?? LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "AI analysis failed"));
  return res.json();
}

export async function runPreflight(payload: {
  columns: string[];
  column_types: Record<string, string>;
  row_count: number;
  mappings: {
    source: string;
    target: string;
    confidence: number;
    reason?: string;
    transform?: string;
    target_type?: string;
    requires_review?: boolean;
    score_gap?: number;
    user_override?: boolean;
  }[];
  connector_id?: string;
  source_connector_id?: string;
  dest_type?: string;
  dest_host?: string;
  dest_port?: number;
  dest_database?: string;
  dest_username?: string;
  dest_password?: string;
  dest_connection_string?: string;
  dest_schema?: string;
  dest_warehouse?: string;
  dest_kind?: string;
  destination_column_types?: Record<string, string>;
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
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Preflight failed"));
  return res.json();
}

/**
 * AI-assisted "explain & suggest fix" for a preflight result. Works
 * deterministically offline; the backend reuses an LLM only to add a friendlier
 * narrative when a provider is configured (see `assistant_provider`).
 */
export async function explainPreflight(payload: {
  preflight: import("./types").PreflightResult;
  dest_type?: string;
  validation_mode?: string;
  use_llm?: boolean;
}): Promise<import("./types").ValidationExplanation> {
  const res = await apiFetch(`${API_BASE}/preflight/explain`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ use_llm: true, ...payload }),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Explain failed"));
  return res.json();
}

export type CellPreviewResult = {
  quarantine_count: number;
  coerce_count: number;
  ok_count: number;
  sample_rows_scanned: number;
  cells: Array<{
    row: number;
    source: string;
    target: string;
    raw: string;
    coerced?: string;
    status: "quarantine" | "coerced" | string;
    message?: string;
    transform?: string;
  }>;
};

/** Cell-level will-quarantine / will-coerce preview before run. */
export async function previewQuarantineCells(payload: {
  headers: string[];
  sample_rows: string[][];
  mappings: Array<{ source: string; target: string; transform?: string | null; target_type?: string | null }>;
  column_types?: Record<string, string>;
  sample_size?: number;
}): Promise<CellPreviewResult> {
  const res = await apiFetch(`${API_BASE}/preflight/preview-cells`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Cell preview failed"));
  return res.json();
}

export async function analyzeDbTransfer(payload: {
  sourceConnectorId: string;
  sourceFormat: string;
  sourceDatabase?: string;
  sourceTable?: string;
  sourceCollection?: string;
  sourceHost?: string;
  sourcePort?: number;
  sourceUsername?: string;
  sourcePassword?: string;
  sourceSchema?: string;
  sourceConnectionString?: string;
  destFormat: string;
  destDatabase: string;
  destTable?: string;
  destCollection?: string;
  destConnectorId?: string;
}): Promise<TransferPlan & { source_columns?: string[]; source_schema?: Record<string, string> }> {
  const isMongo = payload.sourceFormat === "mongodb";
  const isDestMongo = payload.destFormat === "mongodb";
  const body: Record<string, unknown> = {
    source: {
      kind: "database",
      format: payload.sourceFormat,
      connector_id: payload.sourceConnectorId,
      database: payload.sourceDatabase || "",
      table: isMongo ? "" : payload.sourceTable || "",
      collection: isMongo ? payload.sourceCollection || payload.sourceTable || "" : "",
      host: payload.sourceHost || "",
      port: payload.sourcePort || 0,
      username: payload.sourceUsername || "",
      password: payload.sourcePassword || "",
      schema: payload.sourceSchema || "",
      connection_string: payload.sourceConnectionString || "",
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
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Route analysis failed"));
  return res.json();
}

/** Build transfer plan from known schema — no file re-upload (fast for large CSVs). */
export async function analyzeTransferRoute(payload: {
  source: Record<string, unknown>;
  destination: Record<string, unknown>;
  source_columns?: string[];
  source_schema?: Record<string, string>;
}): Promise<TransferPlan> {
  const res = await apiFetch(`${API_BASE}/transfer/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Route analysis failed"));
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
  /** Studio remediation kind when type === "studio". */
  kind?: string;
  run_id?: string;
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

/** Map Pilot / API failures to an actionable message (not a generic URL blame). */
export function formatPilotReachError(error: unknown, apiBase: string = API_BASE): string {
  const raw = error instanceof Error ? error.message : String(error || "Unknown error");
  const lower = raw.toLowerCase();
  if (lower.includes("timed out") || lower.includes("abort")) {
    return "Data Pilot timed out waiting for the API. Retry once — if it keeps happening, set ANTHROPIC_API_KEY or OPENAI_API_KEY on the API service (or leave them unset so the local Pilot agent answers immediately), then redeploy.";
  }
  if (lower.includes("401") || lower.includes("authentication required") || lower.includes("not authenticated")) {
    return "Sign-in required or session expired. Sign in again, then retry Data Pilot.";
  }
  if (lower.includes("403") || lower.includes("forbidden")) {
    return "Your account is not allowed to use Data Pilot. Check workspace role / RBAC.";
  }
  if (lower.includes("failed to fetch") || lower.includes("networkerror") || lower.includes("load failed")) {
    return `Could not reach the API at ${apiBase}. On Railway, set DATAFLOW_API_BASE (or VITE_API_BASE) on the web service to your API URL ending in /api/v1, redeploy web, and set DATAFLOW_WEB_DOMAIN on the API for CORS.`;
  }
  if (lower.includes("503") || lower.includes("no ai") || lower.includes("provider")) {
    return raw;
  }
  return `Data Pilot error: ${raw}`;
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
      preflight_run_id: dataContext.preflight_run_id,
      job_id: dataContext.job_id,
      validation_status: dataContext.validation_status,
      route: dataContext.route,
      blockers: dataContext.blockers,
    };
  }
  let res: Response;
  try {
    res = await apiFetch(`${API_BASE}/copilot/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      // Agent + tool loops routinely exceed the default 15s API timeout.
      timeoutMs: LONG_REQUEST_TIMEOUT_MS,
    });
  } catch (error) {
    throw new Error(formatPilotReachError(error, API_BASE));
  }
  if (!res.ok) {
    const detail = await parseApiError(res, `Copilot chat failed (${res.status})`);
    throw new Error(formatPilotReachError(new Error(`${res.status}: ${detail}`), API_BASE));
  }
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
  driver_type?: string;
  effective_status?: string;
  transfer_ready?: boolean;
  connect_only?: boolean;
  capability_label?: string;
  /** Honest tier: certified | source_only | connect_only | planned */
  certification_tier?: string;
  capabilities?: {
    test?: boolean;
    read?: boolean;
    write?: boolean;
    file_source?: boolean;
  };
}

export async function fetchCatalogConnectors(options: {
  q?: string;
  role?: string;
  category?: string;
  status?: string;
  limit?: number;
  transferOnly?: boolean;
} = {}) {
  const params = new URLSearchParams();
  if (options.q) params.set("q", options.q);
  if (options.role) params.set("role", options.role);
  if (options.category) params.set("category", options.category);
  if (options.status) params.set("status", options.status);
  if (options.limit) params.set("limit", String(options.limit));
  if (options.transferOnly) params.set("transfer_only", "true");
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
  transfer_live?: number;
  connect_only?: number;
  roadmap?: number;
}> {
  const res = await apiFetch(`${API_BASE}/catalog/stats`);
  if (!res.ok) throw new Error("Catalog stats fetch failed");
  return res.json();
}

export async function fetchConnectors(): Promise<Connector[]> {
  const normalize = (c: Record<string, unknown>): Connector | null => {
    const id = String(c.id ?? c._id ?? "");
    if (!id) return null;
    return {
      id,
      name: String(c.name ?? ""),
      type: String(c.type ?? ""),
      host: String(c.host ?? ""),
      port: Number(c.port ?? 0),
      database: String(c.database ?? ""),
      status: c.last_test_ok === false ? "error" : String(c.status ?? "configured"),
      role: c.role ? String(c.role) : undefined,
      username: c.username ? String(c.username) : undefined,
      password: c.password ? String(c.password) : undefined,
      schema: c.schema ? String(c.schema) : undefined,
      connection_string: c.connection_string ? String(c.connection_string) : undefined,
      warehouse: c.warehouse ? String(c.warehouse) : undefined,
      ssl: c.ssl === true,
      auth_mode: c.auth_mode ? String(c.auth_mode) : undefined,
      auth_role: c.auth_role ? String(c.auth_role) : undefined,
      api_key: c.api_key ? String(c.api_key) : undefined,
      service_account: c.service_account ? String(c.service_account) : undefined,
      created_at: String(c.created_at ?? new Date().toISOString()),
      last_test_ok: c.last_test_ok === true,
    };
  };

  // File-backed store is canonical — always prefer /connectors/saved
  const savedRes = await apiFetch(`${API_BASE}/connectors/saved`);
  if (savedRes.status === 401) {
    throw new Error("Authentication required — sign in again to load connectors");
  }
  if (savedRes.ok) {
    const data = await savedRes.json();
    return (data.connectors || [])
      .map((raw: Record<string, unknown>) => normalize(raw))
      .filter(Boolean) as Connector[];
  }

  // Legacy MongoDB list (optional fallback when saved route unavailable)
  const legacyRes = await apiFetch(`${API_BASE}/connectors/`);
  if (legacyRes.status === 401) {
    throw new Error("Authentication required — sign in again to load connectors");
  }
  if (legacyRes.ok) {
    const data = await legacyRes.json();
    return (data.connectors || [])
      .map((raw: Record<string, unknown>) => normalize(raw))
      .filter(Boolean) as Connector[];
  }
  return [];
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

export async function resumeJob(jobId: string): Promise<{ job_id: string; status: string }> {
  const urls = [
    `${API_BASE}/connectors/jobs/${jobId}/resume`,
    `${API_BASE}/jobs/${jobId}/resume`,
  ];
  let lastError: unknown;
  for (const url of urls) {
    try {
      const res = await apiFetch(url, { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(typeof data.detail === "string" ? data.detail : "Resume failed");
      }
      return data as { job_id: string; status: string };
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Resume failed");
}

export async function cancelJob(jobId: string): Promise<{ success: boolean; job_id: string; status: string; message?: string }> {
  const urls = [
    `${API_BASE}/connectors/jobs/${jobId}/cancel`,
    `${API_BASE}/jobs/${jobId}/cancel`,
  ];
  let lastError: unknown;
  for (const url of urls) {
    try {
      const res = await apiFetch(url, { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(typeof data.detail === "string" ? data.detail : "Cancel failed");
      }
      return data as { success: boolean; job_id: string; status: string; message?: string };
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Cancel failed");
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
    rejected_rows: raw.rejected_rows != null ? Number(raw.rejected_rows) : undefined,
    coerced_null_rows: raw.coerced_null_rows != null ? Number(raw.coerced_null_rows) : undefined,
    rejected_details: Array.isArray(raw.rejected_details) ? raw.rejected_details as JobProgress["rejected_details"] : undefined,
    destination_summary: raw.destination_summary && typeof raw.destination_summary === "object"
      ? raw.destination_summary as Record<string, unknown>
      : undefined,
    preflight: raw.preflight && typeof raw.preflight === "object"
      ? raw.preflight as JobProgress["preflight"]
      : undefined,
    phases: Array.isArray(raw.phases)
      ? raw.phases
          .map((p) => {
            if (!p || typeof p !== "object") return null;
            const phase = p as Record<string, unknown>;
            const name = String(phase.name ?? "").trim();
            const rawStatus = String(phase.status ?? "pending").toLowerCase();
            const status: "pending" | "active" | "done" | "failed" | "skipped" =
              rawStatus === "active" || rawStatus === "done" || rawStatus === "failed" || rawStatus === "skipped"
                ? rawStatus
                : "pending";
            if (!name) return null;
            return {
              name,
              status,
              message: phase.message ? String(phase.message) : undefined,
            };
          })
          .filter((p): p is NonNullable<typeof p> => Boolean(p))
      : undefined,
    created_at: String(raw.created_at ?? new Date().toISOString()),
    updated_at: raw.updated_at ? String(raw.updated_at) : undefined,
    started_at: raw.started_at ? String(raw.started_at) : undefined,
    completed_at: raw.completed_at ? String(raw.completed_at) : undefined,
  });

  const startPolling = () => {
    if (pollTimer) return;
    const poll = async () => {
      if (stopped) return;
      try {
        const job = await fetchJob(jobId);
        onUpdate(job);
        if (
          job.status === "completed"
          || job.status === "completed_with_quarantine"
          || job.status === "failed"
          || job.status === "cancelled"
        ) {
          if (pollTimer) clearInterval(pollTimer);
        }
      } catch (e) {
        onError?.(e instanceof Error ? e : new Error("Poll failed"));
      }
    };
    poll();
    pollTimer = setInterval(poll, 3000);
  };

  try {
    const token = getAuthToken();
    const streamUrl = token
      ? `${API_BASE}/connectors/jobs/${jobId}/stream?token=${encodeURIComponent(token)}`
      : `${API_BASE}/connectors/jobs/${jobId}/stream`;
    const es = new EventSource(streamUrl);
    es.onmessage = (ev) => {
      if (stopped) return;
      try {
        const job = normalize(JSON.parse(ev.data));
        onUpdate(job);
        if (
          job.status === "completed"
          || job.status === "completed_with_quarantine"
          || job.status === "failed"
          || job.status === "cancelled"
        ) {
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
  const res = await apiFetch(`${API_BASE}/connectors/saved/${id}`, { method: "DELETE" });
  if (res.ok) return;
  throw new Error(await parseApiError(res, "Failed to delete connector"));
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

/** Preset cadences and available sync modes for the schedule editor dropdowns. */
export async function fetchScheduleIntervals(): Promise<import("./types").ScheduleIntervals> {
  const res = await apiFetch(`${API_BASE}/schedules/intervals`);
  if (!res.ok) throw new Error("Failed to fetch schedule intervals");
  return res.json();
}

export async function fetchSchedule(id: string): Promise<PipelineSchedule> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}`);
  if (!res.ok) throw new Error("Failed to fetch schedule");
  return res.json();
}

/** Run history for a schedule, most-recent-first. */
export async function fetchScheduleHistory(
  id: string,
  limit = 25,
): Promise<import("./types").ScheduleHistory> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}/history?limit=${limit}`);
  if (!res.ok) throw new Error("Failed to fetch schedule history");
  return res.json();
}

export async function createSchedule(
  payload: Partial<import("./types").ScheduleInput> & {
    name: string;
    source_connector_id: string;
    source_table: string;
    dest_connector_id: string;
    dest_table: string;
  },
): Promise<PipelineSchedule> {
  const res = await apiFetch(`${API_BASE}/schedules/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to create schedule"));
  return res.json();
}

export async function updateSchedule(
  id: string,
  payload: Partial<import("./types").ScheduleInput>,
): Promise<PipelineSchedule> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to update schedule"));
  return res.json();
}

export async function deleteSchedule(id: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete schedule");
}

export async function runScheduleNow(id: string): Promise<{ job_id: string }> {
  const res = await apiFetch(`${API_BASE}/schedules/${id}/run`, { method: "POST" });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to run schedule"));
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
  warehouse?: string;
  ssl?: boolean;
  auth_mode?: string;
  auth_role?: string;
  auth_source?: string;
  api_key?: string;
  service_account?: string;
  private_key?: string;
  endpoint_url?: string;
  path_style?: boolean;
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
  role?: string;
  username?: string;
  password?: string;
  schema?: string;
  connection_string?: string;
  warehouse?: string;
  ssl?: boolean;
  auth_mode?: string;
  auth_role?: string;
  auth_source?: string;
  api_key?: string;
  service_account?: string;
  private_key?: string;
  endpoint_url?: string;
  path_style?: boolean;
}): Promise<Connector> {
  const body = { role: "both", ssl: false, ...payload };
  const res = await apiFetch(`${API_BASE}/connectors/saved`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to save connector"));
  const data = await res.json();
  return {
    id: String(data.id ?? ""),
    name: String(data.name ?? payload.name),
    type: String(data.type ?? payload.type),
    host: String(data.host ?? payload.host),
    port: Number(data.port ?? payload.port),
    database: String(data.database ?? payload.database),
    status: String(data.status ?? "configured"),
    created_at: String(data.created_at ?? new Date().toISOString()),
    last_test_ok: data.last_test_ok === true,
  };
}

export async function updateConnector(
  id: string,
  payload: {
    name: string;
    type: string;
    host: string;
    port: number;
    database: string;
    role?: string;
    username?: string;
    password?: string;
    schema?: string;
    connection_string?: string;
    warehouse?: string;
    ssl?: boolean;
    auth_mode?: string;
    auth_role?: string;
    api_key?: string;
    service_account?: string;
    private_key?: string;
    endpoint_url?: string;
    path_style?: boolean;
  },
): Promise<Connector> {
  const body = { role: "both", ssl: false, ...payload };
  const res = await apiFetch(`${API_BASE}/connectors/saved/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to update connector"));
  return res.json();
}

export async function uploadFile(file: File): Promise<ParsedUpload> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await apiFetch(`${API_BASE}/connectors/upload`, { method: "POST", body: formData, timeoutMs: LONG_REQUEST_TIMEOUT_MS });
  if (!res.ok) throw new Error(await parseApiError(res, "Upload failed"));
  return res.json();
}

export async function fetchTransferCapabilities() {
  const res = await apiFetch(`${API_BASE}/transfer/capabilities`);
  if (!res.ok) throw new Error("Failed to load capabilities");
  return res.json();
}

export interface EndpointIntrospection {
  connected: boolean;
  columns: string[];
  schema: Record<string, string>;
  objects?: { name: string; type: string }[];
  row_estimate?: number;
  table_exists?: boolean;
  data?: Record<string, unknown>[];
  sample_data?: Record<string, unknown>[];
  message: string;
}

export async function introspectTransferEndpoints(
  payload: {
    source: Record<string, unknown>;
    destination: Record<string, unknown>;
  },
  options?: { timeoutMs?: number },
): Promise<{ source: EndpointIntrospection; destination: EndpointIntrospection }> {
  const res = await apiFetch(`${API_BASE}/transfer/introspect`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    // Destination probes should fail soft quickly — never block the wizard for 2+ minutes.
    timeoutMs: options?.timeoutMs ?? 45_000,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Schema introspection failed"));
  return res.json();
}

export async function fetchRouteAnalysis(payload: {
  source: Record<string, unknown>;
  destination: Record<string, unknown>;
}): Promise<{
  supported: boolean;
  score: number;
  operation: string;
  conversion_needed?: boolean;
  conversion_supported?: boolean;
  hints?: string[];
  warnings?: string[];
  alternatives?: Array<{ dest_kind: string; dest_format: string; reason: string }>;
}> {
  const res = await apiFetch(`${API_BASE}/transfer/route`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Route analysis failed"));
  return res.json();
}

export async function fetchPlatformStatus(): Promise<{
  catalog_total: number;
  transfer_ready: number;
  connect_test_only: number;
  roadmap: number;
  live_route_combinations: number;
  llm_mapping_available: boolean;
  live_drivers: string[];
  preflight_gates: number;
}> {
  const res = await apiFetch(`${API_BASE}/transfer/platform`);
  if (!res.ok) throw new Error("Failed to load platform status");
  return res.json();
}

export async function fetchTransferReadiness(): Promise<{
  ready: boolean;
  drivers_total: number;
  drivers_ready: number;
  drivers_failed: number;
  production_note?: string;
}> {
  const res = await apiFetch(`${API_BASE}/transfer/readiness`);
  if (!res.ok) throw new Error("Failed to load transfer readiness");
  return res.json();
}

export async function mapTransferColumns(payload: {
  source_columns: string[];
  source_schema?: Record<string, string>;
  target_columns?: string[];
  target_schema?: Record<string, string>;
  validation_mode?: string;
  file_format?: string;
  use_llm?: boolean;
  source_samples?: Record<string, string[]>;
}): Promise<{
  mappings: Array<{
    source: string;
    target: string;
    confidence: number;
    reasoning?: string;
    requires_review?: boolean;
    score_gap?: number;
  }>;
  validation: { passed: boolean; issues: string[] };
  destination_aware: boolean;
  confidence_threshold: number;
  llm?: { llm_used?: boolean; llm_provider?: string; strategy?: string };
  plan_summary?: Record<string, unknown>;
  coercion_issues?: Array<Record<string, unknown>>;
}> {
  const res = await apiFetch(`${API_BASE}/transfer/map`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Column mapping failed"));
  return res.json();
}

export async function createTransferPlan(payload: {
  name?: string;
  source?: Record<string, unknown>;
  destination?: Record<string, unknown>;
  source_columns: string[];
  source_schema?: Record<string, string>;
  target_columns?: string[];
  target_schema?: Record<string, string>;
  row_count_estimate?: number;
  sample_rows?: Record<string, unknown>[];
  policies?: Record<string, unknown>;
}): Promise<{ plan: { id: string; status: string } }> {
  const res = await apiFetch(`${API_BASE}/transfer/plans`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to create transfer plan"));
  return res.json();
}

export async function mapTransferPlan(
  planId: string,
  payload: { validation_mode?: string; use_llm?: boolean; source_samples?: Record<string, string[]> } = {},
) {
  const res = await apiFetch(`${API_BASE}/transfer/plans/${planId}/map`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Plan mapping failed"));
  return res.json();
}

export async function preflightTransferPlan(planId: string) {
  const res = await apiFetch(`${API_BASE}/transfer/plans/${planId}/preflight`, {
    method: "POST",
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Plan preflight failed"));
  return res.json();
}

export async function approveTransferPlan(planId: string, version?: number) {
  const qs = version != null ? `?version=${version}` : "";
  const res = await apiFetch(`${API_BASE}/transfer/plans/${planId}/approve${qs}`, { method: "POST" });
  if (!res.ok) throw new Error(await parseApiError(res, "Plan approval failed"));
  return res.json();
}

export async function updateTransferPlan(
  planId: string,
  payload: Record<string, unknown>,
) {
  const res = await apiFetch(`${API_BASE}/transfer/plans/${planId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to update transfer plan"));
  return res.json();
}

export async function syncTransferPlanMappings(
  planId: string,
  mappings: { source: string; target: string; confidence: number; reason?: string; transform?: string }[],
) {
  const res = await apiFetch(`${API_BASE}/transfer/plans/${planId}/mappings`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mappings }),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to sync plan mappings"));
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
  sourceHost?: string;
  sourcePort?: number;
  sourceUsername?: string;
  sourcePassword?: string;
  sourceDatabase?: string;
  sourceSchema?: string;
  sourceTable?: string;
  sourceCollection?: string;
  sourceConnectionString?: string;
  sourceAuthSource?: string;
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
  destConnectionString?: string;
  destOutputPath?: string;
  destWarehouse?: string;
  destAuthSource?: string;
  skipPreflight?: boolean;
  mappings?: { source: string; target: string; confidence: number; reason?: string }[];
  syncMode?: string;
  schemaPolicy?: string;
  validationMode?: string;
  backfillNewFields?: boolean;
  streamContracts?: Record<string, unknown>[];
  planId?: string;
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
  if (options.sourceHost) formData.append("source_host", options.sourceHost);
  if (options.sourcePort) formData.append("source_port", String(options.sourcePort));
  if (options.sourceUsername) formData.append("source_username", options.sourceUsername);
  if (options.sourcePassword) formData.append("source_password", options.sourcePassword);
  if (options.sourceDatabase) formData.append("source_database", options.sourceDatabase);
  if (options.sourceSchema) formData.append("source_schema", options.sourceSchema);
  if (options.sourceTable) formData.append("source_table", options.sourceTable);
  if (options.sourceCollection) formData.append("source_collection", options.sourceCollection);
  if (options.sourceConnectionString) formData.append("source_connection_string", options.sourceConnectionString);
  if (options.sourceAuthSource) formData.append("source_auth_source", options.sourceAuthSource);
  if (options.destConnectorId) formData.append("dest_connector_id", options.destConnectorId);
  if (options.destHost) formData.append("dest_host", options.destHost);
  if (options.destPort) formData.append("dest_port", String(options.destPort));
  if (options.destUsername) formData.append("dest_username", options.destUsername);
  if (options.destPassword) formData.append("dest_password", options.destPassword);
  if (options.destConnectionString) formData.append("dest_connection_string", options.destConnectionString);
  if (options.destOutputPath) formData.append("dest_output_path", options.destOutputPath);
  if (options.destWarehouse) formData.append("dest_warehouse", options.destWarehouse);
  if (options.destAuthSource) formData.append("dest_auth_source", options.destAuthSource);
  if (options.mappings?.length) {
    formData.append("mappings_json", JSON.stringify(options.mappings));
  }
  if (options.streamContracts?.length) {
    formData.append("stream_contracts_json", JSON.stringify(options.streamContracts));
  }
  if (options.planId) formData.append("plan_id", options.planId);
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

/** @deprecated Use runUniversalTransfer — legacy /connectors/transfer route */
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
    destConnectionString?: string;
    destWarehouse?: string;
    syncMode?: string;
    schemaPolicy?: string;
    validationMode?: string;
    backfillNewFields?: boolean;
    streamContracts?: Record<string, unknown>[];
    mappings?: { source: string; target: string; confidence: number; reason?: string }[];
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
  if (options.destConnectionString) formData.append("dest_connection_string", options.destConnectionString);
  if (options.destWarehouse) formData.append("dest_warehouse", options.destWarehouse);
  if (options.streamContracts?.length) {
    formData.append("stream_contracts_json", JSON.stringify(options.streamContracts));
  }
  if (options.mappings?.length) {
    formData.append("mappings_json", JSON.stringify(options.mappings));
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

export async function fetchMcpLogs(limit = 50): Promise<Array<{
  id: string;
  time: string;
  tool: string;
  client: string;
  status: string;
  ms: number;
  error?: string | null;
}>> {
  const res = await apiFetch(`${API_BASE}/mcp/logs?limit=${limit}`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.logs ?? [];
}

export async function fetchWorkspaceSettings(): Promise<{
  org_name: string;
  timezone: string;
  retention_days: number;
  updated_at?: string | null;
  updated_by?: string | null;
}> {
  const res = await apiFetch(`${API_BASE}/workspace/settings`);
  if (!res.ok) {
    return { org_name: "DataFlow", timezone: "UTC", retention_days: 90 };
  }
  return res.json();
}

export async function updateWorkspaceSettings(body: {
  org_name?: string;
  timezone?: string;
  retention_days?: number;
}): Promise<{
  org_name: string;
  timezone: string;
  retention_days: number;
}> {
  const res = await apiFetch(`${API_BASE}/workspace/settings`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(await parseApiError(res, "Failed to save workspace settings"));
  }
  return res.json();
}

export async function fetchAuditEvents(limit = 50, level?: string): Promise<Array<{
  id: string;
  time: string;
  actor: string;
  action: string;
  resource: string;
  level: string;
  details?: Record<string, unknown>;
}>> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (level && level !== "all") params.set("level", level);
  const res = await apiFetch(`${API_BASE}/audit/events?${params}`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.events ?? [];
}

export type SsoType = "saml" | "oidc" | "azure_ad";

export type SsoConfig = {
  enabled: boolean;
  entity_id?: string;
  sso_url?: string;
  x509_cert?: string;
  email_attribute?: string;
  issuer?: string;
  client_id?: string;
  client_secret?: string;
  redirect_uri?: string;
  scopes?: string;
  tenant_id?: string;
};

export function resolveApiBase(): string {
  if (API_BASE.startsWith("http")) return API_BASE.replace(/\/$/, "");
  return `${window.location.origin}${API_BASE}`.replace(/\/$/, "");
}

export function ssoStartUrl(type: SsoType): string {
  return `${resolveApiBase()}/auth/sso/${type}/start`;
}

export async function fetchSsoConfigs(): Promise<Record<SsoType, SsoConfig>> {
  const res = await apiFetch(`${API_BASE}/workspace/sso`);
  if (!res.ok) throw new Error("Failed to load SSO settings");
  const data = await res.json();
  return data.providers;
}

export async function updateSsoConfig(type: SsoType, body: Partial<SsoConfig>): Promise<{ config: SsoConfig; validation: { ok: boolean; message: string; missing_fields: string[] } }> {
  const res = await apiFetch(`${API_BASE}/workspace/sso/${type}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to save SSO settings"));
  return res.json();
}

export async function testSsoConfig(type: SsoType): Promise<{ ok: boolean; message: string; missing_fields: string[] }> {
  const res = await apiFetch(`${API_BASE}/workspace/sso/${type}/test`, { method: "POST" });
  if (!res.ok) throw new Error(await parseApiError(res, "SSO test failed"));
  return res.json();
}

export async function fetchSsoProviders(): Promise<Array<{ type: SsoType; label: string; login_path: string }>> {
  const res = await fetch(`${resolveApiBase()}/auth/sso/providers`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.providers ?? [];
}

export async function fetchAiProviderSettings(): Promise<Record<string, {
  enabled: boolean;
  api_key: string;
  model: string;
  base_url?: string;
  configured: boolean;
}>> {
  const res = await apiFetch(`${API_BASE}/workspace/ai-providers`);
  if (!res.ok) throw new Error("Failed to load AI provider settings");
  const data = await res.json();
  return data.providers;
}

export async function updateAiProviderSettings(provider: string, body: {
  enabled?: boolean;
  api_key?: string;
  model?: string;
  base_url?: string;
}): Promise<Record<string, unknown>> {
  const res = await apiFetch(`${API_BASE}/workspace/ai-providers/${provider}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to save AI provider settings"));
  return res.json();
}

export type WorkspaceApiKey = {
  id: string;
  name: string;
  prefix: string;
  created_at?: string;
  created_by?: string;
  last_used_at?: string | null;
};

export async function fetchWorkspaceApiKeys(): Promise<WorkspaceApiKey[]> {
  const res = await apiFetch(`${API_BASE}/workspace/api-keys`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.keys ?? [];
}

export async function createWorkspaceApiKey(name: string): Promise<WorkspaceApiKey & { key: string }> {
  const res = await apiFetch(`${API_BASE}/workspace/api-keys`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to create API key"));
  return res.json();
}

export async function revokeWorkspaceApiKey(keyId: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/workspace/api-keys/${keyId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to revoke API key"));
}

export type Workspace = {
  id: string;
  name: string;
  created_at: string;
  created_by: string;
};

export type WorkspaceMember = {
  workspace_id: string;
  email: string;
  role: "owner" | "editor" | "viewer";
  added_at: string;
  added_by: string;
};

export async function fetchWorkspaces(): Promise<Workspace[]> {
  const res = await apiFetch(`${API_BASE}/workspace/workspaces`);
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to load workspaces"));
  const data = await res.json();
  return data.workspaces ?? [];
}

export async function createWorkspace(name: string): Promise<Workspace> {
  const res = await apiFetch(`${API_BASE}/workspace/workspaces`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to create workspace"));
  return res.json();
}

export async function fetchWorkspaceMembers(workspaceId: string): Promise<WorkspaceMember[]> {
  const res = await apiFetch(`${API_BASE}/workspace/workspaces/${workspaceId}/members`);
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to load members"));
  const data = await res.json();
  return data.members ?? [];
}

export async function addWorkspaceMember(workspaceId: string, email: string, role: string): Promise<WorkspaceMember> {
  const res = await apiFetch(`${API_BASE}/workspace/workspaces/${workspaceId}/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, role }),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to add member"));
  return res.json();
}

export async function removeWorkspaceMember(workspaceId: string, email: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/workspace/workspaces/${workspaceId}/members/${encodeURIComponent(email)}`, { method: "DELETE" });
  if (!res.ok) throw new Error(await parseApiError(res, "Failed to remove member"));
}

export async function loginWorkspace(email: string, password: string): Promise<{
  token: string;
  expires_at: number;
  user: { email: string; name: string; role: string };
}> {
  const res = await apiFetch(`${API_BASE}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    throw new Error(await parseApiError(res, "Sign-in failed"));
  }
  return res.json();
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  column_schema: Record<string, string>;
  row_count: number;
  truncated: boolean;
}

export interface QueryExportResult {
  success: boolean;
  filename?: string;
  download_url?: string;
  path?: string;
  row_count?: number;
  format?: string;
  error?: string;
}

export async function executeQuery(payload: {
  connector_id: string;
  query: string;
  database?: string;
  collection?: string;
  limit?: number;
}): Promise<QueryResult> {
  const res = await apiFetch(`${API_BASE}/query/execute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) {
    const detail = await parseApiError(res, "Query failed");
    if (res.status === 400) {
      throw new Error(detail || "Query rejected — use a safe read-only SQL / Mongo filter");
    }
    throw new Error(detail);
  }
  return res.json();
}

export async function exportQuery(payload: {
  connector_id: string;
  query: string;
  database?: string;
  collection?: string;
  limit?: number;
  format: string;
  output_path?: string;
  destination_connector_id?: string;
  destination?: string;
  sync_mode?: string;
  conflict_columns?: string[];
}): Promise<QueryExportResult> {
  const res = await apiFetch(`${API_BASE}/query/export`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Export failed"));
  return res.json();
}

export interface QuarantineInfo {
  job_id: string;
  rejected_rows: number;
  issue_count?: number;
  /** write = load-time rejects; preflight = Validate/Run integrity findings */
  source?: "write" | "preflight" | "none" | string;
  quarantine: {
    row?: number;
    column?: string;
    target?: string;
    value?: string;
    reason?: string;
    policy?: string;
    values?: Record<string, string>;
    chars?: string[];
    suggested_transform?: string;
  }[];
}

export async function fetchJobQuarantine(jobId: string): Promise<QuarantineInfo> {
  const res = await apiFetch(`${API_BASE}/connectors/jobs/${jobId}/quarantine`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load quarantine"));
  return res.json();
}

export async function exportJobQuarantine(jobId: string): Promise<{ success: boolean; row_count?: number; download_url?: string; filename?: string }> {
  const res = await apiFetch(`${API_BASE}/connectors/jobs/${jobId}/quarantine/export`, { method: "POST" });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not export quarantine"));
  return res.json();
}

/** Resolve API-relative download paths (e.g. /api/v1/transfer/download/x.csv) to an absolute URL. */
export function resolveApiAssetUrl(pathOrUrl: string): string {
  if (!pathOrUrl) return "";
  if (/^https?:\/\//i.test(pathOrUrl)) return pathOrUrl;
  if (pathOrUrl.startsWith("/api/")) {
    if (API_BASE.startsWith("http")) {
      const origin = API_BASE.replace(/\/api\/v1\/?$/, "");
      return `${origin}${pathOrUrl}`;
    }
    return `${window.location.origin}${pathOrUrl}`;
  }
  const base = resolveApiBase().replace(/\/$/, "");
  return `${base}/${pathOrUrl.replace(/^\//, "")}`;
}

/**
 * Download quarantine CSV as a browser Blob.
 * Prefers an authenticated fetch of the export file (fixes "file wasn't available on site"
 * when the web app and API are on different origins). Falls back to client-built CSV.
 */
export async function downloadJobQuarantineCsv(
  jobId: string,
  fallbackRows?: QuarantineInfo["quarantine"],
): Promise<{ filename: string; row_count: number; blob: Blob }> {
  const escape = (v: unknown) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  const toCsv = (rows: QuarantineInfo["quarantine"]) => {
    const lines = ["row,column,target,value,reason,policy"];
    for (const r of rows) {
      lines.push(
        [r.row, r.column, r.target, r.value, r.reason, r.policy].map(escape).join(","),
      );
    }
    return `${lines.join("\n")}\n`;
  };

  // 1) Client-side CSV when rows are already loaded — never depends on download_url origin.
  if (fallbackRows && fallbackRows.length > 0) {
    const filename = `quarantine-${jobId}.csv`;
    return {
      filename,
      row_count: fallbackRows.length,
      blob: new Blob([toCsv(fallbackRows)], { type: "text/csv;charset=utf-8" }),
    };
  }

  // 2) Server export + authenticated fetch of the file from the API host.
  const meta = await exportJobQuarantine(jobId);
  if (meta.download_url) {
    const url = resolveApiAssetUrl(meta.download_url);
    const fileRes = await apiFetch(url);
    if (fileRes.ok) {
      return {
        filename: meta.filename || `quarantine-${jobId}.csv`,
        row_count: meta.row_count ?? 0,
        blob: await fileRes.blob(),
      };
    }
  }

  // 3) Last resort: fetch quarantine JSON and build CSV locally.
  const data = await fetchJobQuarantine(jobId);
  const rows = data.quarantine || [];
  if (!rows.length) {
    throw new Error("No quarantine findings to export for this job");
  }
  return {
    filename: `quarantine-${jobId}.csv`,
    row_count: rows.length,
    blob: new Blob([toCsv(rows)], { type: "text/csv;charset=utf-8" }),
  };
}

export interface QuarantineReplayResult {
  success: boolean;
  job_id: string;
  parent_job_id: string;
  rows_written: number;
  rejected: number;
  rows_attempted: number;
  status: string;
  destination_summary?: Record<string, unknown>;
}

export async function replayJobQuarantine(
  jobId: string,
  body: { rows?: QuarantineInfo["quarantine"]; transform_overrides?: Record<string, string> } = {},
): Promise<QuarantineReplayResult> {
  const res = await apiFetch(`${API_BASE}/connectors/jobs/${jobId}/quarantine/replay`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Quarantine replay failed"));
  return res.json();
}

export interface NotificationChannel {
  id: string;
  workspace_id: string;
  kind: "slack" | "teams" | "email" | "servicenow" | "webhook";
  label: string;
  enabled: boolean;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  created_by: string;
}

export async function fetchNotificationChannels(workspaceId?: string): Promise<{ channels: NotificationChannel[] }> {
  const params = workspaceId ? `?workspace_id=${encodeURIComponent(workspaceId)}` : "";
  const res = await apiFetch(`${API_BASE}/workspace/notifications${params}`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load channels"));
  return res.json();
}

export async function createNotificationChannel(channel: Omit<NotificationChannel, "id" | "created_at" | "updated_at" | "created_by">): Promise<NotificationChannel> {
  const res = await apiFetch(`${API_BASE}/workspace/notifications`, { method: "POST", body: JSON.stringify(channel) });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not create channel"));
  return res.json();
}

export async function updateNotificationChannel(id: string, updates: Partial<NotificationChannel>): Promise<NotificationChannel> {
  const res = await apiFetch(`${API_BASE}/workspace/notifications/${id}`, { method: "PATCH", body: JSON.stringify(updates) });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not update channel"));
  return res.json();
}

export async function deleteNotificationChannel(id: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/workspace/notifications/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not delete channel"));
}

export async function testNotificationChannel(id: string): Promise<{ success: boolean; detail: unknown }> {
  const res = await apiFetch(`${API_BASE}/workspace/notifications/${id}/test`, { method: "POST" });
  if (!res.ok) throw new Error(await parseApiError(res, "Test failed"));
  return res.json();
}

export type Tenant = {
  id: string;
  workspace_id: string;
  name: string;
  custom_domain: string;
  data_region: string;
  byok_key_id: string;
  security_contact_email: string;
  mfa_required: boolean;
  session_timeout_hours: number;
  ip_allowlist: string[];
  created_at: string;
  updated_at: string;
};

export type ByokKey = {
  id: string;
  tenant_id: string;
  label: string;
  provider: "local" | "wrapped" | "aws_kms" | "azure_keyvault" | "gcp_kms";
  key_reference: string;
  status: "active" | "rotated" | "revoked";
  created_at: string;
  updated_at: string;
};

export type BenchmarkReport = {
  target: string;
  rows: number;
  success: boolean;
  elapsed_seconds: number;
  records_per_second: number;
  peak_memory_bytes: number;
  peak_memory_mb: number;
  destination_summary: Record<string, unknown>;
  error: string;
  timestamp: string;
  competitors: {
    product: string;
    typical_rps: number;
    memory_mb: number;
    resume_from_checkpoint: boolean;
    observed_max_rows: number;
    notes: string;
  }[];
};

export type SecurityPosture = {
  tenant_id: string | null;
  workspace_id: string | null;
  custom_domain: string | null;
  data_region: string;
  environment: "production" | "development";
  encryption_at_rest: boolean;
  byok: {
    configured: boolean;
    active_count: number;
    total_count: number;
    providers: string[];
    rotated: boolean;
  };
  audit_logging: boolean;
  pii_detection: boolean;
  ip_allowlist_enabled: boolean;
  mfa_required: boolean;
  session_timeout_hours: number;
  tls_version: string;
  compliance: Array<{ framework: string; status: string; evidence: string }>;
  attestations: Array<{ name: string; last_completed?: string | null; next_due?: string | null; status?: string }>;
};

export async function fetchTenant(): Promise<Tenant | null> {
  const res = await apiFetch(`${API_BASE}/workspace/tenant`);
  if (!res.ok) return null;
  return res.json();
}

export async function createTenant(body: Omit<Tenant, "id" | "created_at" | "updated_at">): Promise<Tenant> {
  const res = await apiFetch(`${API_BASE}/workspace/tenant`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not create tenant"));
  return res.json();
}

export async function updateTenant(
  tenantId: string,
  body: Partial<Omit<Tenant, "id" | "created_at" | "updated_at">>,
): Promise<Tenant> {
  const res = await apiFetch(`${API_BASE}/workspace/tenant/${tenantId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not update tenant"));
  return res.json();
}

export async function downloadSecurityReport(): Promise<Blob> {
  const res = await apiFetch(`${API_BASE}/workspace/security/report`);
  if (!res.ok) throw new Error("Could not download compliance report");
  return res.blob();
}

export async function fetchSecurityPosture(): Promise<SecurityPosture> {
  const res = await apiFetch(`${API_BASE}/workspace/security/posture`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load security posture"));
  return res.json();
}

export async function fetchByokKeys(): Promise<{ keys: ByokKey[] }> {
  const res = await apiFetch(`${API_BASE}/workspace/tenant/byok-keys`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load BYOK keys"));
  return res.json();
}

export async function createByokKey(body: { label: string; provider: ByokKey["provider"]; key_material?: string }): Promise<ByokKey> {
  const res = await apiFetch(`${API_BASE}/workspace/tenant/byok-keys`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not create BYOK key"));
  return res.json();
}

export type ProofLedger = {
  generated_at: string;
  headline: string;
  metrics: {
    unique_transfer_drivers: number;
    transfer_live_drivers: string[];
    catalog_transfer_ready_aliases?: number;
    live_route_combinations?: number;
    production_sku_routes: number;
    fidelity_proofs_on_disk: number;
    fidelity_proofs_passed: number;
    planned_catalog_entries?: number;
  };
  production_sku: {
    source_kind: string;
    source_format: string;
    dest_kind: string;
    dest_format: string;
    route: string;
    status: string;
  }[];
  recent_proofs: {
    id: string;
    path: string;
    mtime: string;
    tier?: string;
    route?: string;
    success?: boolean;
    rows?: number;
    checks?: string[];
    elapsed_ms?: number;
  }[];
  vs_airbyte: {
    dimension: string;
    dataflow: string;
    airbyte: string;
    proof: string;
  }[];
  how_to_verify: string[];
};

export type FidelityProofResult = {
  success: boolean;
  tier: string;
  route: string;
  rows: number;
  records_transferred?: number;
  elapsed_ms?: number;
  error?: string;
  checks?: string[];
  check_detail?: Record<string, boolean>;
  spot?: Record<string, unknown>;
  proof_id?: string;
  proof_file?: string;
  vs_airbyte?: string;
};

export async function fetchProofLedger(): Promise<ProofLedger> {
  const res = await apiFetch(`${API_BASE}/workspace/proofs/ledger`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load proof ledger"));
  return res.json();
}

export async function runFidelityProof(): Promise<FidelityProofResult> {
  const res = await apiFetch(`${API_BASE}/workspace/proofs/fidelity`, {
    method: "POST",
    timeoutMs: 120_000,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Fidelity proof failed"));
  return res.json();
}

export async function runBenchmark(rows = 100_000): Promise<BenchmarkReport> {
  const res = await apiFetch(`${API_BASE}/workspace/benchmark`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rows, format: "json" }),
    timeoutMs: 120_000,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Benchmark failed"));
  return res.json();
}

export async function downloadBenchmarkReport(rows = 100_000): Promise<Blob> {
  const res = await apiFetch(`${API_BASE}/workspace/benchmark`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rows, format: "md" }),
    timeoutMs: 120_000,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not download benchmark report"));
  return res.blob();
}

/* ── Data contracts ─────────────────────────────────────────────── */

export interface DataContractSummary {
  id: string;
  name: string;
  version: number;
  status: string;
  source: Record<string, unknown>;
  destination: Record<string, unknown>;
  columns: Record<string, unknown>[];
  mappings: Record<string, unknown>[];
  quality_rules: Record<string, unknown>[];
  strict: boolean;
  created_at: string;
  updated_at: string;
  metadata: Record<string, unknown>;
  preflight_gates?: Record<string, unknown>[];
}

export interface ContractBreaker {
  contract_id: string;
  state: string;
  failure_count: number;
  success_count: number;
  failure_threshold: number;
  recovery_timeout_seconds: number;
}

export async function fetchContracts(): Promise<DataContractSummary[]> {
  const res = await apiFetch(`${API_BASE}/contracts`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load contracts"));
  const data = await res.json();
  return data.contracts || [];
}

export async function createContractFromTransfer(body: {
  name: string;
  source?: Record<string, unknown>;
  destination?: Record<string, unknown>;
  mappings?: Record<string, unknown>[];
  columns?: Record<string, unknown>[];
  quality_rules?: Record<string, unknown>[];
  preflight_gates?: Record<string, unknown>[];
  column_types?: Record<string, string>;
  strict?: boolean;
  metadata?: Record<string, unknown>;
}): Promise<DataContractSummary> {
  const res = await apiFetch(`${API_BASE}/contracts/from-transfer`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not save contract"));
  return res.json();
}

export async function signContract(id: string, strict = true): Promise<DataContractSummary> {
  const res = await apiFetch(`${API_BASE}/contracts/${encodeURIComponent(id)}/sign`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ strict }),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not sign contract"));
  return res.json();
}

export async function deprecateContract(id: string): Promise<DataContractSummary> {
  const res = await apiFetch(`${API_BASE}/contracts/${encodeURIComponent(id)}/deprecate`, {
    method: "POST",
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not deprecate contract"));
  return res.json();
}

export async function fetchContractBreaker(id: string): Promise<ContractBreaker> {
  const res = await apiFetch(`${API_BASE}/contracts/${encodeURIComponent(id)}/breaker`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load breaker"));
  return res.json();
}

export async function resetContractBreaker(id: string): Promise<ContractBreaker> {
  const res = await apiFetch(`${API_BASE}/contracts/${encodeURIComponent(id)}/breaker/reset`, {
    method: "POST",
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not reset breaker"));
  return res.json();
}

export async function exportContract(id: string): Promise<Blob> {
  const res = await apiFetch(`${API_BASE}/contracts/${encodeURIComponent(id)}/export`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not export contract"));
  return res.blob();
}

export async function fetchUsageSummary(days = 30): Promise<{
  days?: number;
  rows_written?: number;
  bytes_processed?: number;
  event_count?: number;
  totals?: { rows_written?: number; bytes_processed?: number; event_count?: number };
  daily?: { date: string; rows_written: number; bytes_processed: number; event_count: number }[];
}> {
  const res = await apiFetch(`${API_BASE}/usage/summary?days=${days}`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load usage"));
  return res.json();
}

export interface SchemaDriftReport {
  severity: string;
  additive: { kind: string; column?: string; to_type?: string }[];
  breaking: { kind: string; column?: string; to?: string; to_type?: string }[];
  summary?: string;
}

export async function classifySchemaDrift(
  oldSchema: Record<string, unknown>,
  newSchema: Record<string, unknown>,
): Promise<SchemaDriftReport> {
  const res = await apiFetch(`${API_BASE}/preflight/schema-drift`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ old_schema: oldSchema, new_schema: newSchema }),
  });
  if (!res.ok) {
    // Fallback: local classify via a thin mirror if endpoint missing
    throw new Error(await parseApiError(res, "Schema drift classify failed"));
  }
  return res.json();
}
