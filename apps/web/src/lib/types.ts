/** Normalize API origin so login and all routes hit `/api/v1/...`. */
function resolveApiBase(): string {
  const fromWindow =
    typeof window !== "undefined"
      ? (window as { DATAFLOW_API_BASE?: string }).DATAFLOW_API_BASE
      : undefined;
  const raw = fromWindow || import.meta.env.VITE_API_BASE || "/api/v1";
  // Strip quotes/whitespace (Railway paste / "white" blank env values).
  let trimmed = String(raw).trim().replace(/^['"]|['"]$/g, "").replace(/\/+$/, "");
  if (!trimmed) return "/api/v1";
  if (trimmed === "/api/v1" || trimmed.endsWith("/api/v1")) return trimmed;
  if (!/^https?:\/\//i.test(trimmed) && !trimmed.startsWith("/")) {
    trimmed = `https://${trimmed}`;
  }
  // Operators often set the Railway host without the version prefix.
  return `${trimmed}/api/v1`;
}

export const API_BASE = resolveApiBase();

/** Human-readable API target for Pilot / Settings diagnostics. */
export function describeApiBase(): string {
  return API_BASE;
}

export type Screen = "landing" | "dashboard" | "pilot" | "transfer" | "query" | "connectors" | "schedules" | "jobs" | "contracts" | "mcp" | "settings" | "docs" | "benchmarks";

export interface DataContract {
  id: string;
  name: string;
  version: number;
  status: "draft" | "signed" | "broken" | "deprecated" | string;
  source: Record<string, unknown>;
  destination: Record<string, unknown>;
  columns: {
    source_name: string;
    target_name: string;
    source_type: string;
    target_type: string;
    transform?: string | null;
    nullable?: boolean;
    primary_key?: boolean;
  }[];
  mappings: Record<string, unknown>[];
  quality_rules: { name: string; expectation: string; threshold?: number | null; severity?: string }[];
  preflight_gates?: Record<string, unknown>[];
  strict: boolean;
  created_at: string;
  updated_at: string;
  metadata: Record<string, unknown>;
}

export interface Connector {
  id: string;
  name: string;
  type: string;
  host: string;
  port: number;
  database: string;
  status: string;
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
  created_at: string;
  last_test_ok?: boolean;
}

export interface TransferCheckpoint {
  chunk_index?: number;
  chunk_total?: number;
  rows_processed?: number;
  offset?: number;
  cursor_value?: unknown;
  cursor_column?: string;
  status?: string;
}

/**
 * Canonical transfer-job lifecycle vocabulary (mirrors `services/job_status.py`).
 * `completed_with_quarantine` is a SUCCESS-with-warnings terminal state — the
 * data landed, but rows were rejected and/or values were coerced to NULL.
 */
export type JobStatus =
  | "pending"
  | "running"
  | "completed"
  | "completed_with_quarantine"
  | "failed"
  | "cancelled";

/** A single quarantined/rejected cell, emitted by every writer. */
export interface RejectedDetail {
  row?: number;
  column?: string;
  target?: string;
  value?: string;
  reason?: string;
  policy?: string;
}

export interface CdcStreamHealth {
  name: string;
  status?: string;
  records_processed?: number;
  cdc_lag_seconds?: number | null;
  replication_lag_bytes?: number | null;
  watermark?: string | null;
  error?: string | null;
}

export interface TransferJob {
  _id: string;
  /** User-editable display name (defaults to source → dest on create). */
  name?: string;
  source_type: string;
  source_name: string;
  destination_type: string;
  destination_database: string;
  destination_collection: string;
  status: JobStatus | string;
  records_processed: number;
  created_at: string;
  total_rows?: number;
  progress_pct?: number;
  phase?: string;
  message?: string;
  operation?: string;
  error?: string;
  rejected_rows?: number;
  coerced_null_rows?: number;
  chunk_current?: number;
  chunk_total?: number;
  checkpoint?: TransferCheckpoint;
  retry_of?: string;
  updated_at?: string;
  started_at?: string;
  completed_at?: string;
  /** CDC heartbeat / event age in seconds (not end-to-end pipeline latency). */
  cdc_lag_seconds?: number | null;
  replication_lag_bytes?: number | null;
  cdc_heartbeat_at?: string | null;
  cdc_last_ddl_at?: string | null;
  streams?: CdcStreamHealth[];
}

export interface JobPhase {
  name: string;
  status: "pending" | "active" | "done" | "failed" | "skipped";
  message?: string;
  started_at?: string;
  ended_at?: string;
  elapsed_ms?: number;
}

export interface JobNotificationResult {
  channel_id: string;
  kind: "slack" | "teams" | "email" | "servicenow" | "webhook";
  ok?: boolean;
  error?: string;
  status?: number;
  body?: string;
}

export interface JobProgress extends TransferJob {
  progress_pct: number;
  phase?: string;
  message?: string;
  error?: string;
  rejected_rows?: number;
  /** Distinct rows where a value was coerced to NULL (kept, but fidelity lost). */
  coerced_null_rows?: number;
  rejected_details?: RejectedDetail[];
  destination_summary?: Record<string, unknown>;
  preflight?: PreflightResult;
  phases?: JobPhase[];
  notifications?: JobNotificationResult[];
  records_per_second?: number;
  chunk_size?: number;
}

export interface CsvValidationReport {
  ok: boolean;
  rows_scanned: number;
  total_rows?: number;
  full_scan?: boolean;
  issues: string[];
  issue_count: number;
}

export interface ParsedUpload {
  row_count: number;
  columns: string[];
  file_type?: string;
  sample_data?: Record<string, unknown>[];
  data?: Record<string, unknown>[];
  schema?: Record<string, string>;
  validation?: CsvValidationReport | null;
}

export interface ActiveDataContext {
  name: string;
  filename?: string;
  columns: string[];
  row_count: number;
  samples?: Record<string, string[]>;
  schema?: Record<string, string>;
  /** Validation run ID (pf_…) — Data Pilot can look this up. */
  preflight_run_id?: string;
  /** Live transfer job ID once Execute starts. */
  job_id?: string;
  validation_status?: "passed" | "blocked" | "running" | string;
  route?: string;
  blockers?: string[];
}

export interface ColumnAnalysis {
  column_name: string;
  semantic_type?: string;
  inferred_type?: string;
  confidence: number;
  is_pii: boolean;
  compliance: string[];
  canonical_form?: string;
  rag_confidence?: number;
  reasoning_steps?: string[];
  method?: string;
  rag_sources?: { title?: string; source?: string; score?: number }[];
}

export interface EnhancedAnalysis {
  columns: ColumnAnalysis[];
  pii_columns: string[];
  quality_score: number;
  recommendations: string[];
  method: string;
}

export interface PreflightGate {
  id: string;
  status: "pass" | "block" | "skip" | "running";
  message: string;
  duration_ms: number;
  details?: Record<string, unknown>;
}

export interface PreflightProofBundle {
  passed: boolean;
  semantic_mapping_score: number;
  semantic_notes: string[];
  quality_score: number;
  confidence_band?: "high" | "medium" | "low";
  quality_grade?: "excellent" | "good" | "review";
  evidence_summary?: string;
  compliance: {
    risk_score: number;
    requires_review: boolean;
    tags: string[];
    details?: Record<string, unknown>;
  };
  reconciliation: {
    passed: boolean;
    preview?: boolean;
    row_fidelity_score?: number;
    matched_key_count?: number;
    missing_key_count?: number;
    extra_key_count?: number;
    message?: string;
    sample_compare?: {
      passed: boolean;
      compared: number;
      mismatches: { row: string; source: string; target: string; source_value: string; target_value: string }[];
      skipped?: boolean;
    };
  };
  transfer_decision: {
    decision: "approve" | "review" | "block";
    blockers: string[];
    reason: string;
    warnings?: string[];
  };
}

export interface CoercionSampleFailure {
  row: number;
  value: string;
  reason: string;
}

/** Per-column value-aware coercion prediction (from `coercion_report.columns[]`). */
export interface CoercionColumn {
  source: string;
  target: string;
  source_type: string;
  target_type: string;
  target_logical?: string;
  transform?: string;
  sampled: number;
  ok: number;
  nulls: number;
  sentinel_nulls: number;
  failed: number;
  sample_failures: CoercionSampleFailure[];
  sentinel_examples?: { row: number; value: string }[];
  severity: "ok" | "warn" | "block";
  suggested_fix?: string;
  suggested_target_type?: string | null;
  suggested_transform?: string | null;
}

export interface CoercionReport {
  checked: number;
  sampled_rows: number;
  has_blocking_failures: boolean;
  columns: CoercionColumn[];
  by_source?: Record<string, CoercionColumn>;
}

export interface PreflightResult {
  passed: boolean;
  passed_count: number;
  total_gates: number;
  readiness_score: number;
  /** Stable ID for this validation run — surface in UI and feed Data Pilot. */
  run_id?: string;
  gates: PreflightGate[];
  blockers: { id: string; message: string; details?: Record<string, unknown>; guidance?: { gate?: string; title?: string; category?: string; why?: string; fix?: string; examples?: string[] } }[];
  proof_bundle?: PreflightProofBundle;
  coercion_report?: CoercionReport;
}

/** Machine-readable next step from POST /preflight/explain — mapped to Studio controls. */
export type ValidationActionKind =
  | "change_target_type"
  | "add_transform"
  | "review_mappings"
  | "rerun_mapping"
  | "check_connection"
  | "normalize_control_chars"
  | "open_bad_data_fix";

export interface ValidationSuggestedAction {
  kind: ValidationActionKind | string;
  column?: string;
  target?: string;
  to_type?: string;
  transform?: string;
  label: string;
  /** True when mapping-only type change cannot ALTER existing destination DDL. */
  requires_ddl?: boolean;
}

export interface ValidationIssue {
  gate: string;
  title: string;
  severity: "block" | "warning" | string;
  what: string;
  why: string;
  fix: string;
  examples: string[];
  columns: string[];
  detail_messages: string[];
}

export interface ValidationColumnFix {
  column: string;
  target?: string;
  source_type?: string;
  target_type?: string;
  severity: "block" | "warn" | "ok" | string;
  failed: number;
  sentinel_nulls: number;
  sampled: number;
  sample_failures: CoercionSampleFailure[];
  suggested_fix?: string;
  suggested_target_type?: string | null;
  suggested_transform?: string | null;
}

export interface ValidationExplanation {
  passed: boolean;
  summary: string;
  issues: ValidationIssue[];
  column_fixes: ValidationColumnFix[];
  suggested_actions: ValidationSuggestedAction[];
  narrative: string;
  assistant_provider: string;
}

export interface TransferResult {
  success: boolean;
  records_transferred?: number;
  destination?: { database: string; collection: string; path?: string; format?: string; filename?: string; download_url?: string };
  destination_summary?: {
    type?: string;
    schema?: string;
    table?: string;
    database?: string;
    collection?: string;
    dataset?: string;
    project?: string;
    checksum?: string;
    driver?: string;
    rejected_rows?: number;
    coerced_null_rows?: number;
    rejected_details?: RejectedDetail[];
    warnings?: string[];
    error_policy?: string;
    filename?: string;
    download_url?: string;
    load_method?: string;
    chunk_size?: number;
    batches?: number;
    records_per_second?: number;
  };
  records_per_second?: number;
  ddl_executed?: string[];
  operation?: string;
  error?: string;
  error_details?: Record<string, unknown>;
  reconciliation?: {
    passed?: boolean;
    message?: string;
    source_rows?: number;
    target_rows?: number;
    rejected_rows?: number;
    coerced_null_rows?: number;
    source_checksum?: string;
    target_checksum?: string;
  };
  job_id?: string;
  /** Full client-captured event log from live theater (persisted for result dashboard) */
  event_log?: string[];
}

export interface TransferPlan {
  supported: boolean;
  message: string;
  operation: string;
  auto_create: string[];
  type_mappings: { column: string; source_type: string; dest_type: string }[];
  source_columns?: string[];
  source_schema?: Record<string, string>;
}

export type ScheduleInterval = "hourly" | "daily" | "weekly";
export type ScheduleSyncMode =
  | "full_refresh_overwrite"
  | "full_refresh_append"
  | "incremental"
  | "cdc"
  | "scd2"
  | "mirror";

/** Editable config shared by create (POST) and partial update (PATCH). */
export interface ScheduleInput {
  name: string;
  source_connector_id: string;
  source_table: string;
  dest_connector_id: string;
  dest_table: string;
  interval: ScheduleInterval | string;
  cron: string;
  timezone: string;
  sync_mode: ScheduleSyncMode | string;
  validation_mode: string;
  schema_policy: string;
  backfill_new_fields: boolean;
  cursor_column: string;
  primary_key: string;
  mappings: Record<string, unknown>[];
  stream_contracts: Record<string, unknown>[];
  workspace_id: string;
  max_retries: number;
  retry_backoff_seconds: number;
  notify_on_failure: boolean;
  notify_on_success: boolean;
  enabled: boolean;
}

/** Full schedule record (list/detail) — config plus read-only run state. */
export interface PipelineSchedule {
  id: string;
  name: string;
  source_connector_id: string;
  source_table: string;
  dest_connector_id: string;
  dest_table: string;
  interval: ScheduleInterval | string;
  cron: string;
  timezone: string;
  sync_mode: ScheduleSyncMode | string;
  validation_mode: string;
  schema_policy: string;
  backfill_new_fields: boolean;
  cursor_column: string;
  primary_key: string;
  cursor_value: string;
  workspace_id: string;
  max_retries: number;
  retry_backoff_seconds: number;
  notify_on_failure: boolean;
  notify_on_success: boolean;
  enabled: boolean;
  last_run_at: string | null;
  next_run_at: string | null;
  last_job_id: string | null;
  last_status: string | null;
  run_count: number;
  running: boolean;
  created_at: string;
  /** Present on GET /schedules/{id}; omitted from list summaries. */
  mappings?: { source: string; target: string; confidence?: number; transform?: string | null }[];
  mapping_count?: number;
}

/** A single persisted run attempt from GET /schedules/{id}/history. */
export interface ScheduleRun {
  job_id: string;
  status: string;
  attempt: number;
  started_at: string;
  finished_at: string;
  duration_seconds: number;
  records_transferred: number;
  rejected_rows: number;
  coerced_null_rows: number;
  error: string;
  retry_scheduled?: boolean;
}

export interface ScheduleHistory {
  schedule_id: string;
  runs: ScheduleRun[];
}

export interface ScheduleIntervalOption {
  id: string;
  label: string;
}

export interface ScheduleIntervals {
  intervals: ScheduleIntervalOption[];
  sync_modes: string[];
}

export const CONNECTOR_CATALOG = [
  // Relational databases
  { id: "postgresql", label: "PostgreSQL", port: 5432 },
  { id: "mysql", label: "MySQL", port: 3306 },
  { id: "mariadb", label: "MariaDB", port: 3306 },
  { id: "sqlserver", label: "SQL Server", port: 1433 },
  { id: "oracle", label: "Oracle", port: 1521 },
  { id: "sqlite", label: "SQLite", port: 0 },
  { id: "cockroachdb", label: "CockroachDB", port: 26257 },
  { id: "singlestore", label: "SingleStore", port: 3306 },
  { id: "timescaledb", label: "TimescaleDB", port: 5432 },
  { id: "supabase", label: "Supabase", port: 5432 },
  // Document / NoSQL
  { id: "mongodb", label: "MongoDB", port: 27017 },
  { id: "dynamodb", label: "Amazon DynamoDB", port: 443 },
  { id: "cassandra", label: "Apache Cassandra", port: 9042 },
  { id: "couchbase", label: "Couchbase", port: 8091 },
  { id: "redis", label: "Redis", port: 6379 },
  { id: "neo4j", label: "Neo4j", port: 7687 },
  { id: "elasticsearch", label: "Elasticsearch", port: 9200 },
  { id: "firebase", label: "Firebase", port: 443 },
  // Cloud warehouses
  { id: "snowflake", label: "Snowflake", port: 443 },
  { id: "bigquery", label: "BigQuery", port: 443 },
  { id: "redshift", label: "Amazon Redshift", port: 5439 },
  { id: "databricks", label: "Databricks", port: 443 },
  { id: "synapse", label: "Azure Synapse", port: 1433 },
  { id: "teradata", label: "Teradata", port: 1025 },
  { id: "vertica", label: "Vertica", port: 5433 },
  { id: "firebolt", label: "Firebolt", port: 443 },
  { id: "clickhouse", label: "ClickHouse", port: 8123 },
  { id: "duckdb", label: "DuckDB", port: 0 },
  { id: "trino", label: "Trino / Presto", port: 8080 },
  { id: "hive", label: "Apache Hive", port: 10000 },
  { id: "druid", label: "Apache Druid", port: 8082 },
  // File formats
  { id: "csv", label: "CSV", port: 0 },
  { id: "tsv", label: "TSV", port: 0 },
  { id: "json", label: "JSON", port: 0 },
  { id: "jsonl", label: "JSON Lines", port: 0 },
  { id: "parquet", label: "Parquet", port: 0 },
  { id: "avro", label: "Avro", port: 0 },
  { id: "orc", label: "ORC", port: 0 },
  { id: "excel", label: "Excel", port: 0 },
  { id: "xml", label: "XML", port: 0 },
  { id: "yaml", label: "YAML", port: 0 },
  { id: "fixed_width", label: "Fixed-width", port: 0 },
  // Object storage
  { id: "s3", label: "Amazon S3", port: 443 },
  { id: "gcs", label: "Google Cloud Storage", port: 443 },
  { id: "azure_blob", label: "Azure Blob", port: 443 },
  { id: "adls", label: "Azure Data Lake", port: 443 },
  { id: "sftp", label: "SFTP", port: 22 },
  { id: "email", label: "Email (SMTP)", port: 587 },
  // Streaming
  { id: "kafka", label: "Apache Kafka", port: 9092 },
  { id: "kinesis", label: "Amazon Kinesis", port: 443 },
  { id: "pubsub", label: "Google Pub/Sub", port: 443 },
  { id: "rabbitmq", label: "RabbitMQ", port: 5672 },
  { id: "pulsar", label: "Apache Pulsar", port: 6650 },
  // SaaS
  { id: "salesforce", label: "Salesforce", port: 443 },
  { id: "hubspot", label: "HubSpot", port: 443 },
  { id: "stripe", label: "Stripe", port: 443 },
  { id: "shopify", label: "Shopify", port: 443 },
  { id: "rest_api", label: "REST / OpenAPI", port: 443 },
  { id: "graphql", label: "GraphQL", port: 443 },
] as const;

export type ConnectorCatalogId = (typeof CONNECTOR_CATALOG)[number]["id"];
