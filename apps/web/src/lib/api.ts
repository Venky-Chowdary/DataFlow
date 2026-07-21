import { API_BASE, ActiveDataContext, Connector, EnhancedAnalysis, ParsedUpload, PipelineSchedule, TransferJob, TransferPlan } from "./types";
import { clearSession, getAuthToken } from "./session";

const DEFAULT_REQUEST_TIMEOUT_MS = 15000;
const LONG_REQUEST_TIMEOUT_MS = 120000;

/** Unauthenticated liveness probe — used so a 401 on connectors is not "API offline". */
let _healthFailStreak = 0;
const HEALTH_OFFLINE_THRESHOLD = 5;
let _lastApiOkAt = 0;

export function noteApiSuccess(): void {
  _healthFailStreak = 0;
  _lastApiOkAt = Date.now();
}

export async function probeApiHealth(): Promise<boolean> {
  const origin = API_BASE.replace(/\/api\/v1\/?$/i, "") || "";
  // Prefer nginx → API /health (no auth). Never lead with /api/v1/health — it was
  // auth-gated and returned 401, which burned the fail streak during demos.
  const candidates = [
    origin ? `${origin}/health-api` : "/health-api",
    origin ? `${origin}/api/v1/health` : "/api/v1/health",
    `${API_BASE.replace(/\/$/, "")}/health`,
    origin && /^https?:\/\//i.test(origin) ? `${origin}/health` : "",
  ].filter((v, i, a) => v && a.indexOf(v) === i);

  for (const url of candidates) {
    try {
      const controller = new AbortController();
      const timer = window.setTimeout(() => controller.abort(), 4000);
      try {
        const res = await fetch(url, { method: "GET", cache: "no-store", signal: controller.signal });
        // 401 on a misconfigured health path is not an outage — try next URL.
        if (res.ok) {
          noteApiSuccess();
          return true;
        }
      } finally {
        window.clearTimeout(timer);
      }
    } catch {
      // try next candidate
    }
  }
  // If any API call succeeded recently, the control plane is busy (introspect/map),
  // not down — do not increment the offline streak.
  if (Date.now() - _lastApiOkAt < 60_000) {
    return true;
  }
  _healthFailStreak += 1;
  return false;
}

/** True only after repeated health failures — avoids flicker when one request times out. */
export function shouldMarkApiOffline(healthOk: boolean): boolean {
  if (healthOk) {
    _healthFailStreak = 0;
    return false;
  }
  if (Date.now() - _lastApiOkAt < 60_000) {
    return false;
  }
  return _healthFailStreak >= HEALTH_OFFLINE_THRESHOLD;
}

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
    } else if (res.ok) {
      noteApiSuccess();
    }
    return res;
  } catch (error) {
    if (controller.signal.aborted) throw new Error("Request timed out");
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function requestJson<T>(
  urls: string[],
  fallbackMessage: string,
  options?: { timeoutMs?: number },
): Promise<T> {
  let lastError: unknown;
  for (const url of urls) {
    try {
      const res = await apiFetch(url, { timeoutMs: options?.timeoutMs });
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

export interface VectorFieldRouting {
  column: string;
  action: "embed" | "metadata" | "exclude_pii" | "skip";
  confidence: number;
  reason: string;
  semantic_role?: string;
  is_pii?: boolean;
}

export interface VectorRoutingPlan {
  fields: VectorFieldRouting[];
  content_column: string | null;
  embedding_column: string | null;
  metadata_columns: string[];
  exclude_pii_columns: string[];
  skip_columns: string[];
  summary?: {
    embed?: string | null;
    metadata_count?: number;
    exclude_pii_count?: number;
    skip_count?: number;
  };
}

/** Recommend embed / metadata / exclude_pii / skip for vector destinations. */
export async function fetchVectorRouting(payload: {
  columns: string[];
  samples?: Record<string, string[]>;
  schema_types?: Record<string, string>;
  analysis_columns?: Array<{ column_name: string; is_pii?: boolean; semantic_type?: string }>;
}): Promise<VectorRoutingPlan> {
  const res = await apiFetch(`${API_BASE}/ai/vector-routing`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Vector routing failed"));
  return res.json();
}

export interface EmbeddingCacheStats {
  path: string;
  entries: number;
  models: number;
  approx_bytes: number;
  session_hits: number;
  session_misses: number;
  session_writes: number;
  durable_default: boolean;
  hit_rate?: number | null;
}

/** Durable SQLite embedding cache status. */
export async function fetchEmbeddingCacheStats(): Promise<EmbeddingCacheStats> {
  const res = await apiFetch(`${API_BASE}/ai/embedding-cache`);
  if (!res.ok) throw new Error(await parseApiError(res, "Embedding cache status failed"));
  return res.json();
}

/** Clear durable embedding cache (and process L1 by default). */
export async function clearEmbeddingCache(options?: {
  model?: string;
  clearMemory?: boolean;
}): Promise<{ deleted: number; model: string; memory_cleared: number }> {
  const params = new URLSearchParams();
  if (options?.model) params.set("model", options.model);
  if (options?.clearMemory === false) params.set("clear_memory", "false");
  const qs = params.toString();
  const res = await apiFetch(`${API_BASE}/ai/embedding-cache${qs ? `?${qs}` : ""}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Clear embedding cache failed"));
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
    source_type?: string;
    requires_review?: boolean;
    score_gap?: number;
    user_override?: boolean;
    create_new?: boolean;
    assignment_strategy?: string;
    semantic_role?: string;
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
  dest_auth_source?: string;
  dest_auth_mode?: string;
  dest_auth_role?: string;
  dest_kind?: string;
  dest_table?: string;
  dest_collection?: string;
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
  risk?: "safe" | "mutate" | string;
  job_id?: string;
  schedule_id?: string;
}

export interface CopilotPendingAction {
  id: string;
  type: string;
  label?: string;
  risk?: string;
  kind?: string;
  run_id?: string;
  payload?: Record<string, unknown>;
}

export interface CopilotChatResponse {
  answer: string;
  intent: string;
  confidence: number;
  method: string;
  reasoning?: string;
  suggested_actions?: CopilotAction[];
  pending_actions?: CopilotPendingAction[];
  needs_clarification?: string;
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
    return "Data Pilot took too long to respond. Please try again in a moment.";
  }
  if (lower.includes("401") || lower.includes("authentication required") || lower.includes("not authenticated")) {
    return "Your session expired. Sign in again, then retry.";
  }
  if (lower.includes("403") || lower.includes("forbidden")) {
    return "You don’t have permission to use Data Pilot in this workspace.";
  }
  if (lower.includes("failed to fetch") || lower.includes("networkerror") || lower.includes("load failed")) {
    return "Couldn’t reach Data Pilot right now. Check that the app is online, then try again.";
  }
  if (lower.includes("503") || lower.includes("no ai") || lower.includes("provider")) {
    return "Data Pilot isn’t available right now. Please try again shortly.";
  }
  return "Something went wrong with Data Pilot. Please try again.";
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
  unique_drivers?: number;
  catalog_tiles?: number;
  transfer_live_tiles?: number;
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
      // Preserve tri-state: true / false / undefined (never tested).
      // Coercing null→false made brand-new saves look like "Test failed".
      last_test_ok:
        c.last_test_ok === true ? true : c.last_test_ok === false ? false : undefined,
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
    "Failed to load jobs",
    { timeoutMs: 45_000 },
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

export async function fetchJobMappingProof(jobId: string): Promise<{
  job_id: string;
  status?: string;
  plan_id?: string;
  mapping_version?: number;
  mapping_hash?: string;
  mapping_proof: Record<string, unknown>;
  honesty?: string;
}> {
  return requestJson(
    [`${API_BASE}/transfer/${encodeURIComponent(jobId)}/mapping-proof`],
    "Mapping proof not found"
  );
}

export async function renameJob(jobId: string, name: string): Promise<JobProgress> {
  const urls = [
    `${API_BASE}/connectors/jobs/${jobId}`,
    `${API_BASE}/jobs/${jobId}`,
  ];
  let lastError: unknown;
  for (const url of urls) {
    try {
      const res = await apiFetch(url, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = data?.detail;
        const message =
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((d: { msg?: string }) => d?.msg).filter(Boolean).join("; ") || "Rename failed"
              : "Rename failed";
        throw new Error(message);
      }
      return data as JobProgress;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Rename failed");
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

  const normalize = (raw: Record<string, unknown>): JobProgress => {
    const ds = raw.destination_summary && typeof raw.destination_summary === "object"
      ? raw.destination_summary as Record<string, unknown>
      : undefined;
    const rpsFromRoot = raw.records_per_second != null ? Number(raw.records_per_second) : undefined;
    const rpsFromDs = ds?.records_per_second != null ? Number(ds.records_per_second) : undefined;
    return {
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
      progress_indeterminate: Boolean(raw.progress_indeterminate),
      phase: raw.phase ? String(raw.phase) : undefined,
      message: raw.message ? String(raw.message) : undefined,
      operation: raw.operation ? String(raw.operation) : undefined,
      error: raw.error ? String(raw.error) : undefined,
      error_details: raw.error_details && typeof raw.error_details === "object"
        ? raw.error_details as JobProgress["error_details"]
        : undefined,
      error_code: raw.error_code ? String(raw.error_code) : undefined,
      error_title: raw.error_title ? String(raw.error_title) : undefined,
      error_fix: raw.error_fix ? String(raw.error_fix) : undefined,
      error_confidence: raw.error_confidence ? String(raw.error_confidence) : undefined,
      failed_at_phase: raw.failed_at_phase ? String(raw.failed_at_phase) : undefined,
      chunk_current: raw.chunk_current != null ? Number(raw.chunk_current) : undefined,
      chunk_total: raw.chunk_total != null ? Number(raw.chunk_total) : undefined,
      chunk_size: raw.chunk_size != null
        ? Number(raw.chunk_size)
        : ds?.chunk_size != null
          ? Number(ds.chunk_size)
          : undefined,
      rejected_rows: raw.rejected_rows != null ? Number(raw.rejected_rows) : undefined,
      coerced_null_rows: raw.coerced_null_rows != null ? Number(raw.coerced_null_rows) : undefined,
      rejected_details: Array.isArray(raw.rejected_details) ? raw.rejected_details as JobProgress["rejected_details"] : undefined,
      destination_summary: ds,
      load_history_report: raw.load_history_report && typeof raw.load_history_report === "object"
        ? raw.load_history_report as JobProgress["load_history_report"]
        : (
          ds?.load_history_report && typeof ds.load_history_report === "object"
            ? ds.load_history_report as JobProgress["load_history_report"]
            : undefined
        ),
      preflight: raw.preflight && typeof raw.preflight === "object"
        ? raw.preflight as JobProgress["preflight"]
        : undefined,
      reconciliation: raw.reconciliation && typeof raw.reconciliation === "object"
        ? raw.reconciliation as JobProgress["reconciliation"]
        : undefined,
      explanation: raw.explanation ? String(raw.explanation) : undefined,
      mapping_proof: raw.mapping_proof && typeof raw.mapping_proof === "object"
        ? raw.mapping_proof as JobProgress["mapping_proof"]
        : undefined,
      plan_id: raw.plan_id ? String(raw.plan_id) : undefined,
      mapping_version: raw.mapping_version != null ? Number(raw.mapping_version) : undefined,
      mapping_hash: raw.mapping_hash ? String(raw.mapping_hash) : undefined,
      ddl_executed: Array.isArray(raw.ddl_executed)
        ? raw.ddl_executed.map(String)
        : Array.isArray(raw.ddl_log)
          ? raw.ddl_log.map(String)
          : undefined,
      ddl_log: Array.isArray(raw.ddl_log)
        ? raw.ddl_log.map(String)
        : Array.isArray(raw.ddl_executed)
          ? raw.ddl_executed.map(String)
          : undefined,
      event_log: Array.isArray(raw.event_log) ? raw.event_log.map(String) : undefined,
      sync_mode: raw.sync_mode ? String(raw.sync_mode) : undefined,
      schema_policy: raw.schema_policy ? String(raw.schema_policy) : undefined,
      validation_mode: raw.validation_mode ? String(raw.validation_mode) : undefined,
      triggered_by: raw.triggered_by
        ? String(raw.triggered_by)
        : raw.created_by
          ? String(raw.created_by)
          : undefined,
      created_by: raw.created_by ? String(raw.created_by) : undefined,
      records_per_second: Number.isFinite(rpsFromRoot as number)
        ? rpsFromRoot
        : Number.isFinite(rpsFromDs as number)
          ? rpsFromDs
          : undefined,
      cdc_lag_seconds: raw.cdc_lag_seconds != null ? Number(raw.cdc_lag_seconds) : null,
      replication_lag_bytes: raw.replication_lag_bytes != null ? Number(raw.replication_lag_bytes) : null,
      cdc_heartbeat_at: raw.cdc_heartbeat_at ? String(raw.cdc_heartbeat_at) : null,
      cdc_last_ddl_at: raw.cdc_last_ddl_at ? String(raw.cdc_last_ddl_at) : null,
      cdc_plugin: raw.cdc_plugin ? String(raw.cdc_plugin) : null,
      cdc_slot_name: raw.cdc_slot_name ? String(raw.cdc_slot_name) : null,
      cdc_delivery: raw.cdc_delivery ? String(raw.cdc_delivery) : null,
      cdc_row_filter: raw.cdc_row_filter ? String(raw.cdc_row_filter) : null,
      watermark: raw.watermark != null ? String(raw.watermark) : null,
      cdc_shared_reader: raw.cdc_shared_reader == null ? null : Boolean(raw.cdc_shared_reader),
      snapshot_mode: raw.snapshot_mode ? String(raw.snapshot_mode) : null,
      cdc_lease_holder: raw.cdc_lease_holder ? String(raw.cdc_lease_holder) : null,
      cdc_lease_resource: raw.cdc_lease_resource ? String(raw.cdc_lease_resource) : null,
      cdc_lease_stale: raw.cdc_lease_stale == null ? null : Boolean(raw.cdc_lease_stale),
      cdc_lease_heartbeat_age_sec:
        raw.cdc_lease_heartbeat_age_sec != null ? Number(raw.cdc_lease_heartbeat_age_sec) : null,
      cdc_lease_backend: raw.cdc_lease_backend ? String(raw.cdc_lease_backend) : null,
      cdc_lease_generation:
        raw.cdc_lease_generation != null ? Number(raw.cdc_lease_generation) : null,
      cdc_lease_cursor_key: raw.cdc_lease_cursor_key ? String(raw.cdc_lease_cursor_key) : null,
      cdc_lease_conflict: raw.cdc_lease_conflict == null ? null : Boolean(raw.cdc_lease_conflict),
      cdc_cursor_gap: raw.cdc_cursor_gap == null ? null : Boolean(raw.cdc_cursor_gap),
      cdc_cursor_gap_code: raw.cdc_cursor_gap_code ? String(raw.cdc_cursor_gap_code) : null,
      cdc_cursor_gap_dialect: raw.cdc_cursor_gap_dialect ? String(raw.cdc_cursor_gap_dialect) : null,
      cdc_cursor_gap_resume: raw.cdc_cursor_gap_resume != null ? String(raw.cdc_cursor_gap_resume) : null,
      cdc_cursor_gap_retained: raw.cdc_cursor_gap_retained != null ? String(raw.cdc_cursor_gap_retained) : null,
      source_ha_role: raw.source_ha_role != null ? String(raw.source_ha_role) : null,
      source_ha_topology: raw.source_ha_topology != null ? String(raw.source_ha_topology) : null,
      source_ha_enabled: raw.source_ha_enabled == null ? null : Boolean(raw.source_ha_enabled),
      source_ha_group: raw.source_ha_group != null ? String(raw.source_ha_group) : null,
      source_ha_replica: raw.source_ha_replica != null ? String(raw.source_ha_replica) : null,
      source_ha_message: raw.source_ha_message != null ? String(raw.source_ha_message) : null,
      cdc_retention_status: raw.cdc_retention_status != null ? String(raw.cdc_retention_status) : null,
      cdc_retention_resume: raw.cdc_retention_resume != null ? String(raw.cdc_retention_resume) : null,
      cdc_retention_retained: raw.cdc_retention_retained != null ? String(raw.cdc_retention_retained) : null,
      cdc_retention_message: raw.cdc_retention_message != null ? String(raw.cdc_retention_message) : null,
      cdc_retention_dialect: raw.cdc_retention_dialect != null ? String(raw.cdc_retention_dialect) : null,
      cdc_append_only_sink: raw.cdc_append_only_sink == null ? null : Boolean(raw.cdc_append_only_sink),
      trust_score: raw.trust_score != null ? Number(raw.trust_score) : null,
      trust: raw.trust && typeof raw.trust === "object" ? raw.trust as JobProgress["trust"] : null,
      streams: Array.isArray(raw.streams) ? raw.streams as JobProgress["streams"] : undefined,
      notifications: Array.isArray(raw.notifications)
        ? raw.notifications as JobProgress["notifications"]
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
                started_at: phase.started_at ? String(phase.started_at) : undefined,
                ended_at: phase.ended_at ? String(phase.ended_at) : undefined,
                elapsed_ms: phase.elapsed_ms != null ? Number(phase.elapsed_ms) : undefined,
              };
            })
            .filter((p): p is NonNullable<typeof p> => Boolean(p))
        : undefined,
      created_at: String(raw.created_at ?? new Date().toISOString()),
      updated_at: raw.updated_at ? String(raw.updated_at) : undefined,
      started_at: raw.started_at ? String(raw.started_at) : undefined,
      completed_at: raw.completed_at ? String(raw.completed_at) : undefined,
    };
  };

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

export async function exportDataflowManifest(): Promise<Blob> {
  const res = await apiFetch(`${API_BASE}/schedules/export/dataflow?format=yaml`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not export dataflow.yaml"));
  return res.blob();
}

export async function exportScheduleYaml(id: string): Promise<Blob> {
  const res = await apiFetch(`${API_BASE}/schedules/${encodeURIComponent(id)}/export?format=yaml`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not export pipeline YAML"));
  return res.blob();
}

export async function planGitopsManifest(payload: Record<string, unknown>): Promise<{
  dry_run: boolean;
  resource_count: number;
  creates: number;
  updates: number;
  skips: number;
  actions: Array<{ kind: string; action: string; id?: string | null; name?: string | null; reason?: string }>;
}> {
  const res = await apiFetch(`${API_BASE}/schedules/gitops/plan`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "GitOps plan failed"));
  return res.json();
}

export async function applyGitopsManifest(
  payload: Record<string, unknown>,
  dryRun = false,
  opts?: { requireSignedContracts?: boolean },
): Promise<{
  dry_run: boolean;
  resource_count: number;
  applied?: number;
  failed?: number;
  creates?: number;
  updates?: number;
  require_signed_contracts?: boolean;
  results?: Array<{ kind: string; action: string; ok?: boolean; id?: string; name?: string; error?: string }>;
  actions?: Array<{ kind: string; action: string }>;
}> {
  const params = new URLSearchParams();
  if (dryRun) params.set("dry_run", "true");
  if (opts?.requireSignedContracts) params.set("require_signed_contracts", "true");
  const q = params.toString() ? `?${params.toString()}` : "";
  const res = await apiFetch(`${API_BASE}/schedules/gitops/apply${q}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "GitOps apply failed"));
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
}): Promise<{ success: boolean; message: string; source_ha?: Record<string, unknown> }> {
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
  /** Persist form Test result so the list matches (true/false). */
  last_test_ok?: boolean;
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
    role: String(data.role ?? body.role ?? "both"),
    status: String(data.status ?? "configured"),
    created_at: String(data.created_at ?? new Date().toISOString()),
    last_test_ok:
      data.last_test_ok === true ? true : data.last_test_ok === false ? false : undefined,
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
    last_test_ok?: boolean;
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

export async function uploadFile(
  file: File,
  options?: { enableOcr?: boolean },
): Promise<ParsedUpload> {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("enable_ocr", options?.enableOcr === true ? "true" : "false");
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
  schema_intelligence?: Record<
    string,
    { logical_type?: string; semantic_role?: string; confidence?: number; notes?: string[] }
  >;
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
    // Destination schema can take 1–2 minutes on Snowflake cold start — allow it.
    // Timeout is not "API down"; TransferPage shows schema loading, not offline banner.
    timeoutMs: options?.timeoutMs ?? 180_000,
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

export async function fetchOpsFreshness(warnSeconds = 60): Promise<{
  worst_lag_seconds: number | null;
  warn_threshold_seconds: number;
  critical_threshold_seconds?: number;
  heartbeat_stale_seconds?: number;
  stale_count?: number;
  critical_count?: number;
  slo_status?: "ok" | "warn" | "critical" | "unknown" | string;
  alerts?: Array<{
    severity: string;
    code: string;
    title: string;
    detail: string;
    schedule_id?: string | null;
    job_id?: string | null;
    stream?: string | null;
    lag_seconds?: number;
  }>;
  pipelines: Array<{
    schedule_id: string;
    stream: string;
    job_id: string;
    lag_seconds: number;
    polls_total: number;
    heartbeat_at?: number;
    heartbeat_age_seconds?: number | null;
    stale: boolean;
    severity?: string;
  }>;
  counters: Record<string, number>;
  gauges: Record<string, number>;
}> {
  const res = await apiFetch(`${API_BASE}/ops/freshness?warn_seconds=${warnSeconds}`);
  if (!res.ok) throw new Error("Failed to load pipeline freshness");
  return res.json();
}

export async function fetchOpsDlq(limit = 50): Promise<{
  events: Array<Record<string, unknown>>;
  count: number;
  by_action: Record<string, number>;
  open_rows: number;
}> {
  const res = await apiFetch(`${API_BASE}/ops/dlq?limit=${limit}`);
  if (!res.ok) throw new Error("Failed to load quarantine DLQ");
  return res.json();
}

export async function fetchCdcLease(cursorKey: string): Promise<{
  found: boolean;
  cursor_key: string;
  backend?: string;
  holder_job_id?: string | null;
  lease?: {
    holder_id?: string;
    resource?: string;
    generation?: number;
    stale?: boolean;
    age_sec?: number;
    backend?: string;
  };
}> {
  const res = await apiFetch(
    `${API_BASE}/ops/cdc-leases?cursor_key=${encodeURIComponent(cursorKey)}`,
  );
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load CDC lease"));
  return res.json();
}

export async function forceReleaseCdcLease(body: {
  cursor_key: string;
  expected_generation?: number | null;
  reason?: string;
}): Promise<{
  released: boolean;
  reason: string;
  cursor_key: string;
  prior?: Record<string, unknown>;
  holder_job_id?: string | null;
  backend?: string;
}> {
  const res = await apiFetch(`${API_BASE}/ops/cdc-leases/force-release`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not force-release CDC lease"));
  return res.json();
}

export async function fetchCdcCursor(cursorKey: string): Promise<{
  cursor_key: string;
  found: boolean;
  watermark: string | null;
}> {
  const res = await apiFetch(
    `${API_BASE}/ops/cdc-cursors?cursor_key=${encodeURIComponent(cursorKey)}`,
  );
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load CDC cursor"));
  return res.json();
}

export async function clearCdcCursor(body: {
  cursor_key: string;
  reason?: string;
}): Promise<{
  cleared: boolean;
  cursor_key: string;
  prior_watermark?: string | null;
  reason?: string;
}> {
  const res = await apiFetch(`${API_BASE}/ops/cdc-cursors/clear`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not clear CDC watermark"));
  return res.json();
}

export interface CdcRetentionProbe {
  ok: boolean;
  cdc_retention_status: string;
  cdc_retention_resume?: string | null;
  cdc_retention_retained?: string | null;
  cdc_retention_message?: string | null;
  cdc_retention_dialect?: string | null;
  retention?: {
    status: string;
    dialect: string;
    resume?: string;
    retained?: string;
    cursor_key?: string;
    message?: string;
  };
}

/** Probe watermark vs live CDC retention (SQL Server min_lsn / Oracle oldest SCN). */
export async function probeCdcRetention(body: {
  type: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  schema?: string;
  connection_string?: string;
  table?: string;
  cursor_key?: string;
  watermark?: string | null;
  multi_subnet_failover?: boolean;
}): Promise<CdcRetentionProbe> {
  const res = await apiFetch(`${API_BASE}/ops/cdc-retention/probe`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "CDC retention probe failed"));
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
  destination_db_type?: string;
  sync_mode?: string;
  schema_policy?: string;
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
  mapping_proof?: Record<string, unknown>;
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
  writeViaStaging?: boolean;
  /** Opt-in Tesseract OCR for scanned/image-only PDF sources. */
  enableOcr?: boolean;
  /** Destination writer options (vector embed fields, allow_append_only, …). */
  destExtra?: Record<string, unknown>;
  /** Source options (multi_subnet_failover, enable_ocr already separate, …). */
  sourceExtra?: Record<string, unknown>;
  streamContracts?: Record<string, unknown>[];
  planId?: string;
  priorityColumn?: string;
  priorityDirection?: "asc" | "desc";
  limit?: number;
}) {
  const formData = new FormData();
  if (options.file) formData.append("file", options.file);
  formData.append("source_kind", options.sourceKind || "file");
  if (options.sourceFormat) formData.append("source_format", options.sourceFormat);
  formData.append("dest_kind", options.destKind || "database");
  formData.append("dest_format", options.destFormat || "mongodb");
  formData.append("dest_database", options.destDatabase || "test_db");
  // Empty schema is OK — API normalize_schema fills dialect default (never force Postgres public).
  formData.append("dest_schema", options.destSchema || "");
  formData.append("sync_mode", options.syncMode || "full_refresh_append");
  formData.append("schema_policy", options.schemaPolicy || "manual_review");
  formData.append("validation_mode", options.validationMode || "strict");
  formData.append("backfill_new_fields", options.backfillNewFields === true ? "true" : "false");
  formData.append("write_via_staging", options.writeViaStaging === true ? "true" : "false");
  formData.append("enable_ocr", options.enableOcr === true ? "true" : "false");
  if (options.destExtra && Object.keys(options.destExtra).length) {
    formData.append("dest_extra_json", JSON.stringify(options.destExtra));
  }
  if (options.sourceExtra && Object.keys(options.sourceExtra).length) {
    formData.append("source_extra_json", JSON.stringify(options.sourceExtra));
  }
  if (options.priorityColumn) formData.append("priority_column", options.priorityColumn);
  if (options.priorityDirection) formData.append("priority_direction", options.priorityDirection);
  if (options.limit && options.limit > 0) formData.append("limit", String(options.limit));
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

/** JSON transfer execute (SDK / GitOps) — Form upload remains on runUniversalTransfer. */
export async function executeTransferJson(payload: {
  source: Record<string, unknown>;
  destination: Record<string, unknown>;
  mappings?: { source: string; target: string; confidence?: number }[];
  syncMode?: string;
  validationMode?: string;
  schemaPolicy?: string;
  skipPreflight?: boolean;
  asyncMode?: boolean;
  planId?: string;
}) {
  const res = await apiFetch(`${API_BASE}/transfer/execute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      source: payload.source,
      destination: payload.destination,
      mappings: payload.mappings || [],
      sync_mode: payload.syncMode || "full_refresh_append",
      validation_mode: payload.validationMode || "strict",
      schema_policy: payload.schemaPolicy || "manual_review",
      skip_preflight: payload.skipPreflight === true,
      async_mode: payload.asyncMode !== false,
      plan_id: payload.planId || undefined,
    }),
    timeoutMs: LONG_REQUEST_TIMEOUT_MS,
  });
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
  formData.append("sync_mode", options.syncMode || "full_refresh_append");
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
    _df_qid?: string;
  }[];
  /** Destination-side DLQ table (`{table}_df_quarantine`) when written. */
  dest_dlq?: {
    table?: string | null;
    rows_written?: number | null;
    open_rows?: number | null;
    ok?: boolean | null;
    skipped?: boolean | null;
    reason?: string | null;
    error?: string | null;
    supported?: boolean | null;
  };
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
    const lines = ["row,column,target,value,reason,policy,suggested_transform"];
    const mark = (v: unknown) => {
      let text = String(v ?? "");
      text = text
        .replace(/\u200B/g, "[U+200B]")
        .replace(/\u200C/g, "[U+200C]")
        .replace(/\u200D/g, "[U+200D]")
        .replace(/\uFEFF/g, "[U+FEFF]")
        .replace(/\u0000/g, "[U+0000]")
        .replace(/\uFFFD/g, "[U+FFFD]");
      return text;
    };
    for (const r of rows) {
      lines.push(
        [r.row, r.column, r.target, mark(r.value), r.reason, r.policy, r.suggested_transform]
          .map(escape)
          .join(","),
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
  dest_dlq_promoted?: {
    updated?: number;
    table?: string;
    promoted_at?: string;
    error?: string;
    skipped?: boolean;
  };
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

/** Agentic repair — durable propose → human decide → apply mappings. */
export interface RepairProposal {
  id: string;
  job_id?: string;
  source?: string;
  status: string;
  confidence?: string;
  auto_applicable?: boolean;
  summary?: string;
  actions: Record<string, unknown>[];
  diagnosis?: Record<string, unknown>;
  created_at?: number;
  decided_at?: number;
  decided_by?: string;
  apply_result?: {
    applied?: boolean;
    mappings?: RepairMapping[];
    error?: string;
  };
}

export interface RepairMapping {
  source?: string;
  destination?: string;
  destination_type?: string;
  target_type?: string;
  transform?: string;
  transforms?: { type?: string }[];
  [key: string]: unknown;
}

export async function proposeRepairFromPreflight(body: {
  preflight: Record<string, unknown>;
  coercion_report?: Record<string, unknown>;
  job_id?: string;
}): Promise<RepairProposal> {
  const res = await apiFetch(`${API_BASE}/repair/propose/preflight`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not propose repair"));
  return res.json();
}

export async function proposeRepairFromQuarantine(body: {
  rejected_details: Record<string, unknown>[];
  job_id?: string;
}): Promise<RepairProposal> {
  const res = await apiFetch(`${API_BASE}/repair/propose/quarantine`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Could not propose quarantine repair"));
  return res.json();
}

export async function listRepairProposals(opts?: {
  job_id?: string;
  status?: string;
}): Promise<RepairProposal[]> {
  const q = new URLSearchParams();
  if (opts?.job_id) q.set("job_id", opts.job_id);
  if (opts?.status) q.set("status", opts.status);
  const suffix = q.toString() ? `?${q}` : "";
  const res = await apiFetch(`${API_BASE}/repair/proposals${suffix}`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not list repair proposals"));
  const data = await res.json();
  return Array.isArray(data.proposals) ? data.proposals : [];
}

export async function fetchRepairProposal(proposalId: string): Promise<RepairProposal> {
  const res = await apiFetch(`${API_BASE}/repair/proposals/${encodeURIComponent(proposalId)}`);
  if (!res.ok) throw new Error(await parseApiError(res, "Could not load repair proposal"));
  return res.json();
}

export async function decideRepairProposal(
  proposalId: string,
  body: { approve: boolean; actor?: string; mappings?: RepairMapping[] },
): Promise<RepairProposal> {
  const res = await apiFetch(
    `${API_BASE}/repair/proposals/${encodeURIComponent(proposalId)}/decide`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        approve: body.approve,
        actor: body.actor || "operator",
        mappings: body.mappings || [],
      }),
    },
  );
  if (!res.ok) throw new Error(await parseApiError(res, "Could not decide repair proposal"));
  return res.json();
}

/** Debezium-style CDC incremental snapshot (job-scoped). */
export interface CdcSnapshotSignal {
  id: string;
  source_key: string;
  table: string;
  status: string;
  primary_key?: string;
  chunk_size?: number;
  last_pk?: string;
  rows_snapshotted?: number;
  created_at?: number;
  updated_at?: number;
  error?: string;
  resolved_source_key?: string;
  context?: Record<string, unknown>;
}

export async function listJobCdcSnapshots(
  jobId: string,
  status = "",
): Promise<{
  job_id: string;
  signals: CdcSnapshotSignal[];
  context?: {
    source_key?: string;
    source_keys?: string[];
    table?: string;
    primary_key?: string;
    driver?: string;
    honesty?: string;
  };
}> {
  const q = status ? `?status=${encodeURIComponent(status)}` : "";
  const res = await apiFetch(
    `${API_BASE}/transfer/${encodeURIComponent(jobId)}/cdc/snapshots${q}`,
  );
  if (!res.ok) throw new Error(await parseApiError(res, "Could not list CDC snapshots"));
  return res.json();
}

export async function requestJobCdcSnapshot(
  jobId: string,
  body?: { table?: string; primary_key?: string; chunk_size?: number },
): Promise<CdcSnapshotSignal> {
  const res = await apiFetch(
    `${API_BASE}/transfer/${encodeURIComponent(jobId)}/cdc/snapshots`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    },
  );
  if (!res.ok) throw new Error(await parseApiError(res, "Could not request CDC snapshot"));
  return res.json();
}

export async function cancelJobCdcSnapshot(
  jobId: string,
  signalId: string,
): Promise<CdcSnapshotSignal> {
  const res = await apiFetch(
    `${API_BASE}/transfer/${encodeURIComponent(jobId)}/cdc/snapshots/${encodeURIComponent(signalId)}/cancel`,
    { method: "POST" },
  );
  if (!res.ok) throw new Error(await parseApiError(res, "Could not cancel CDC snapshot"));
  return res.json();
}
