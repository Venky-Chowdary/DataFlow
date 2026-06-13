import { API_BASE, ActiveDataContext, Connector, EnhancedAnalysis, ParsedUpload, TransferJob, TransferPlan } from "./types";

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
  const res = await fetch(`${API_BASE}/ai/analyze/enhanced`, {
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
  sample_rows?: Record<string, unknown>[];
  estimated_bytes?: number;
}): Promise<import("./types").PreflightResult> {
  const res = await fetch(`${API_BASE}/preflight/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error("Preflight failed");
  return res.json();
}

export async function queryRAG(query: string): Promise<{ answer: string; response?: string }> {
  const res = await fetch(`${API_BASE}/ai/rag/query`, {
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
  const res = await fetch(`${API_BASE}/copilot/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error("Copilot chat failed");
  return res.json();
}

export async function fetchCopilotPrompts(): Promise<string[]> {
  const res = await fetch(`${API_BASE}/copilot/prompts`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.prompts || [];
}

export async function fetchMcpManifest() {
  const res = await fetch(`${API_BASE}/mcp/manifest`);
  if (!res.ok) throw new Error("MCP manifest failed");
  return res.json();
}

export async function fetchMcpStatus() {
  const res = await fetch(`${API_BASE}/mcp/status`);
  if (!res.ok) throw new Error("MCP status failed");
  return res.json();
}

export async function fetchCopilotStatus() {
  const res = await fetch(`${API_BASE}/copilot/status`);
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
  limit?: number;
} = {}) {
  const params = new URLSearchParams();
  if (options.q) params.set("q", options.q);
  if (options.role) params.set("role", options.role);
  if (options.limit) params.set("limit", String(options.limit));
  const res = await fetch(`${API_BASE}/catalog/connectors?${params}`);
  if (!res.ok) throw new Error("Catalog fetch failed");
  return res.json();
}

export async function fetchConnectors(): Promise<Connector[]> {
  const res = await fetch(`${API_BASE}/connectors/`);
  if (!res.ok) throw new Error("Failed to load connectors");
  const data = await res.json();
  return data.connectors || [];
}

export async function fetchJobs(): Promise<TransferJob[]> {
  const res = await fetch(`${API_BASE}/connectors/jobs`);
  if (!res.ok) throw new Error("Failed to load jobs");
  const data = await res.json();
  return data.jobs || [];
}

export async function deleteConnector(id: string): Promise<void> {
  const res = await fetch(`${API_BASE}/connectors/${id}`, { method: "DELETE" });
  if (!res.ok) throw new Error("Failed to delete connector");
}

export async function testConnection(payload: {
  type: string;
  host: string;
  port: number;
  database: string;
}): Promise<{ success: boolean; message: string }> {
  const res = await fetch(`${API_BASE}/connectors/test`, {
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
}): Promise<Connector> {
  const res = await fetch(`${API_BASE}/connectors/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error("Failed to save connector");
  return res.json();
}

export async function uploadFile(file: File): Promise<ParsedUpload> {
  const formData = new FormData();
  formData.append("file", file);
  const res = await fetch(`${API_BASE}/connectors/upload`, { method: "POST", body: formData });
  if (!res.ok) throw new Error("Upload failed");
  return res.json();
}

export async function fetchTransferCapabilities() {
  const res = await fetch(`${API_BASE}/transfer/capabilities`);
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
  const res = await fetch(`${API_BASE}/transfer/analyze-file`, { method: "POST", body: formData });
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
}) {
  const formData = new FormData();
  if (options.file) formData.append("file", options.file);
  formData.append("source_kind", options.sourceKind || "file");
  if (options.sourceFormat) formData.append("source_format", options.sourceFormat);
  formData.append("dest_kind", options.destKind || "database");
  formData.append("dest_format", options.destFormat || "mongodb");
  formData.append("dest_database", options.destDatabase || "test_db");
  formData.append("dest_schema", options.destSchema || "public");
  if (options.destTable) formData.append("dest_table", options.destTable);
  if (options.destCollection) formData.append("dest_collection", options.destCollection);
  formData.append("skip_preflight", options.skipPreflight !== false ? "true" : "false");
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
  const res = await fetch(`${API_BASE}/transfer/run`, { method: "POST", body: formData });
  const data = await res.json();
  if (!res.ok) {
    const detail = data.detail;
    const errMsg = typeof detail === "string"
      ? detail
      : detail?.error || detail?.message || JSON.stringify(detail) || "Transfer failed";
    return { success: false, error: errMsg };
  }
  return { success: true, ...data };
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
  } = {}
) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("destination_database", destinationDatabase);
  formData.append("destination_collection", destinationCollection);
  formData.append("skip_preflight", options.skipPreflight !== false ? "true" : "false");
  formData.append("dest_type", options.destType || "mongodb");
  if (options.connectorId) formData.append("connector_id", options.connectorId);
  if (options.destHost) formData.append("dest_host", options.destHost);
  if (options.destPort) formData.append("dest_port", String(options.destPort));
  if (options.destSchema) formData.append("dest_schema", options.destSchema);
  if (options.destUsername) formData.append("dest_username", options.destUsername);
  if (options.destPassword) formData.append("dest_password", options.destPassword);
  if (options.destWarehouse) formData.append("dest_warehouse", options.destWarehouse);
  const res = await fetch(`${API_BASE}/connectors/transfer`, { method: "POST", body: formData });
  const data = await res.json();
  if (!res.ok) {
    const detail = data.detail;
    const errMsg = typeof detail === "string"
      ? detail
      : detail?.error || detail?.message || JSON.stringify(detail) || "Transfer failed";
    return { success: false, error: errMsg, preflight: detail?.preflight };
  }
  return { success: true, ...data };
}
