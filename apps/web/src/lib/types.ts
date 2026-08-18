/** Normalize API origin so login and all routes hit `/api/v1/...`. */
function resolveApiBase(): string {
  const w =
    typeof window !== "undefined"
      ? (window as { DATAWRAP_API_BASE?: string; DATAFLOW_API_BASE?: string })
      : undefined;
  // Prefer Datawrap; keep DATAFLOW_* for Railway cutover until vars are renamed.
  const fromWindow = w?.DATAWRAP_API_BASE || w?.DATAFLOW_API_BASE;
  // `import.meta.env` is a Vite inject. Under node:test there is no Vite
  // transform, so reading `.env` throws. Fall back cleanly instead of
  // crashing every pure helper that happens to import this module.
  const viteEnv =
    typeof import.meta !== "undefined" &&
    (import.meta as { env?: { VITE_API_BASE?: string } }).env
      ? (import.meta as { env?: { VITE_API_BASE?: string } }).env!.VITE_API_BASE
      : undefined;
  const raw = fromWindow || viteEnv || "/api/v1";
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

export type Screen = "landing" | "dashboard" | "pilot" | "transfer" | "query" | "connectors" | "schedules" | "transforms" | "jobs" | "contracts" | "mcp" | "settings" | "docs" | "benchmarks";

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
  last_used_at?: string | null;
}

export interface TransferCheckpoint {
  chunk_index?: number;
  chunk_total?: number;
  rows_processed?: number;
  offset?: number;
  cursor_value?: unknown;
  cursor_column?: string;
  status?: string;
  /** ISO timestamp from checkpoint_service — used for resume-age display. */
  updated_at?: string;
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
  values?: Record<string, string>;
  chars?: string[];
  suggested_transform?: string;
}

export interface CdcStreamHealth {
  name: string;
  status?: string;
  records_processed?: number;
  cdc_lag_seconds?: number | null;
  replication_lag_bytes?: number | null;
  watermark?: string | null;
  error?: string | null;
  row_accounting?: import("./conservationLedger").ConservationLedger | null;
}

/**
 * Server-side cutover window, projected only from throughput the engine
 * actually persisted. When `available` is false, `reason` says why — the UI
 * must show that reason rather than inventing a number.
 */
export interface RuntimeEstimate {
  available: boolean;
  reason?: string;
  basis?: string;
  rows_total?: number | null;
  rows_done?: number;
  rows_remaining?: number | null;
  rows_per_second_p50?: number | null;
  rows_per_second_p10?: number | null;
  remaining_seconds_p50?: number | null;
  remaining_seconds_p90?: number | null;
  finishes_at_p50?: string;
  finishes_at_p90?: string;
  intervals_observed?: number;
  runs_observed?: number;
  notes?: string[];
}

export interface TransferJob {
  _id: string;
  /** User-editable display name (defaults to source → dest on create). */
  name?: string;
  source_type: string;
  source_name: string;
  source_connector_id?: string;
  dest_connector_id?: string;
  destination_type: string;
  destination_database: string;
  destination_collection: string;
  status: JobStatus | string;
  records_processed: number;
  created_at: string;
  total_rows?: number;
  runtime_estimate?: RuntimeEstimate;
  progress_pct?: number;
  /** True when job has no finite row denominator (e.g. continuous CDC). */
  progress_indeterminate?: boolean;
  phase?: string;
  message?: string;
  operation?: string;
  error?: string;
  /** Structured failure details (code, title, fix, raw driver message). */
  error_details?: Record<string, unknown>;
  error_code?: string;
  error_title?: string;
  error_fix?: string;
  error_confidence?: string;
  /** Phase active when the job failed (e.g. load) — distinct from status phase=failed. */
  failed_at_phase?: string;
  rejected_rows?: number;
  coerced_null_rows?: number;
  /** Quarantine evidence dropped past the sample cap (exact count still in rejected_rows). */
  rejected_details_truncated?: number;
  chunk_current?: number;
  chunk_total?: number;
  checkpoint?: TransferCheckpoint;
  retry_of?: string;
  updated_at?: string;
  started_at?: string;
  completed_at?: string;
  /** Proven CDC lag seconds (commit_ts or WAL catch-up) — never heartbeat age. */
  cdc_lag_seconds?: number | null;
  /** Lag proof basis: commit_ts | wal_bytes | legacy_seconds | unknown. */
  cdc_lag_basis?: string | null;
  cdc_heartbeat_age_sec?: number | null;
  /** Logical decoding plugin (pgoutput / test_decoding) or binlog engine. */
  cdc_plugin?: string | null;
  cdc_slot_name?: string | null;
  cdc_delivery?: string | null;
  /** Route-scoped dest-owned watermark EOS — never a platform-wide claim. */
  exactly_once_active?: boolean | null;
  exactly_once_claimed_platform?: boolean | null;
  exactly_once_algorithm?: string | null;
  exactly_once_protocol?: string | null;
  delivery_semantics?: string | null;
  eos_committed_lsn?: string | null;
  eos_fence_epoch?: number | null;
  eos_dest_authoritative?: boolean | null;
  /** Dest-monotonic apply sequence after post-commit verify. */
  eos_apply_seq?: number | null;
  /** Dest-owned incremental-snapshot window id (Debezium DDD-3, dest-persisted). */
  eos_window_id?: string | null;
  eos_snapshot_signal_id?: string | null;
  eos_window_hi_pk?: string | null;
  /** SQL Server CDC TVF row_filter_option actually used (all / all update old / net). */
  cdc_row_filter?: string | null;
  replication_lag_bytes?: number | null;
  /** Slot confirmed_flush_lsn — the position holding WAL retention. */
  cdc_confirmed_flush_lsn?: string | null;
  /** Slot restart_lsn — oldest WAL still needed by this slot. */
  cdc_restart_lsn?: string | null;
  /** SQL Server CDC capture min_lsn (retention edge). */
  cdc_min_lsn?: string | null;
  /** SQL Server CDC capture max_lsn (capture head). */
  cdc_max_lsn?: string | null;
  /** Wall time of max_lsn via fn_cdc_map_lsn_to_time. */
  cdc_max_lsn_time?: string | null;
  /** SQL Server CDC capture instance name. */
  cdc_capture_instance?: string | null;
  /** True when capture agent / log scan is stalled (reader-at-tip ≠ healthy). */
  cdc_capture_stall?: boolean | null;
  cdc_capture_stall_reason?: string | null;
  cdc_capture_stall_unknown?: boolean | null;
  /** DMV capture latency seconds (sys.dm_cdc_log_scan_sessions). */
  cdc_capture_latency_seconds?: number | null;
  /** Live pg_replication_slots.active for this CDC job. */
  cdc_slot_active?: boolean | null;
  cdc_slot_exists?: boolean | null;
  /** PG13+ wal_status: reserved | extended | unreserved | lost. */
  cdc_wal_status?: string | null;
  cdc_freshness_severity?: string | null;
  cdc_lag_unknown_reason?: string | null;
  cdc_heartbeat_at?: string | null;
  cdc_last_ddl_at?: string | null;
  mapping_review_required?: boolean | null;
  mapping_review_id?: string | null;
  mapping_review_reason?: string | null;
  mapping_review_honesty?: string | null;
  /** Durable CDC resume cursor (slot/LSN, GTID, change-stream token). */
  watermark?: string | null;
  /** True when multi-table shared log reader is active (one slot / server_id). */
  cdc_shared_reader?: boolean | null;
  /** Debezium-compatible snapshot mode used for this run. */
  snapshot_mode?: string | null;
  /**
   * Named CDC snapshot plan. lost_window means purged WAL/binlog/redo events
   * are gone — recovery is at-least-once upsert of current keys, not continuous CDC.
   */
  snapshot_plan?: {
    kind?: string | null;
    snapshot_mode?: string | null;
    lost_window?: boolean | null;
    resume_broken?: boolean | null;
    run_snapshot?: boolean | null;
    run_stream?: boolean | null;
    next_action?: string | null;
    reason?: string | null;
    migration_proven?: boolean | null;
  } | null;
  /** Active CDC lease holder (multi-worker fail-fast). */
  cdc_lease_holder?: string | null;
  cdc_lease_resource?: string | null;
  cdc_lease_stale?: boolean | null;
  cdc_lease_heartbeat_age_sec?: number | null;
  /** Lease store backend: memory | file | redis. */
  cdc_lease_backend?: string | null;
  /** Fencing generation — increments on steal. */
  cdc_lease_generation?: number | null;
  /** Cursor key for the contested CDC lease (force-release). */
  cdc_lease_cursor_key?: string | null;
  /** True when this job failed because another worker holds the CDC resource. */
  cdc_lease_conflict?: boolean | null;
  /** True when resume LSN/SCN is before retained redo (AG / archive purge gap). */
  cdc_cursor_gap?: boolean | null;
  cdc_cursor_gap_code?: string | null;
  cdc_cursor_gap_dialect?: string | null;
  cdc_cursor_gap_resume?: string | null;
  cdc_cursor_gap_retained?: string | null;
  source_ha_role?: string | null;
  source_ha_topology?: string | null;
  source_ha_enabled?: boolean | null;
  source_ha_group?: string | null;
  source_ha_replica?: string | null;
  source_ha_message?: string | null;
  cdc_retention_status?: string | null;
  cdc_retention_resume?: string | null;
  cdc_retention_retained?: string | null;
  cdc_retention_message?: string | null;
  cdc_retention_dialect?: string | null;
  /** True when CDC was blocked because dest is append-only without opt-in. */
  cdc_append_only_sink?: boolean | null;
  /** Composite trust score 0–100 (persisted on terminal). */
  trust_score?: number | null;
  /**
   * Independent dest COUNT(*) conservation stamped on terminal jobs.
   * Display only — never close dest with records_processed / writer ack.
   */
  row_accounting?: import("./conservationLedger").ConservationLedger | null;
  trust?: {
    score: number;
    grade: string;
    tone: string;
    confidence: string;
    factors: Array<{
      id: string;
      label: string;
      score: number | null;
      weight: number;
      note: string;
      present?: boolean;
    }>;
    next_action: { code: string; label: string; detail: string };
    lease_conflict?: boolean;
  } | null;
  streams?: CdcStreamHealth[];
  sync_mode?: string;
  schema_policy?: string;
  validation_mode?: string;
  triggered_by?: string;
  created_by?: string;
  explanation?: string;
  ddl_executed?: string[];
  ddl_log?: string[];
  event_log?: string[];
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

/**
 * Gate-8 reconcile payload as the engine emits it
 * (`services/reconciliation.py` → `src/transfer/reconcile_step.py`).
 *
 * Every honesty qualifier the engine produces belongs here: dropping
 * `verification_mode` / `identity` / `alignment` at the type boundary is what
 * let a positional-only comparison render as an exact match.
 */
export interface Gate8ReconciliationPayload {
  passed?: boolean;
  message?: string;
  phase?: string;
  /**
   * Proof population vs sample honesty:
   * full_checksum | sample | writer_ack | none
   */
  coverage?: string;
  /**
   * Population the dest digest covers. ``whole_table_not_comparable`` means
   * append/upsert into a larger sink — dest-before delta is the identity.
   */
  checksum_scope?: string;
  assurance_level?: string;
  checksum_match?: boolean | null;
  preview?: boolean;
  post_write_pending?: boolean;
  /** File/object export Gate-8: operational pass without read-back proof. */
  unproven?: boolean;
  skipped_readback?: boolean;
  migration_proven?: boolean;
  /** key_aligned | positional_only | unproven_identity */
  verification_mode?: string;
  identity?: {
    column?: string | null;
    proven?: boolean;
    reason?: string;
    provenance?: string;
  };
  source_rows?: number;
  target_rows?: number;
  /** Pre-write dest COUNT(*) — append identity is dest_after − dest_before. */
  target_rows_before?: number | null;
  rejected_rows?: number;
  coerced_null_rows?: number;
  /** Intentional LSN-guard / redelivery skips — not a shortfall. */
  rows_skipped?: number;
  source_checksum?: string;
  /** writer_ack | write_pass_fingerprints | remapped_source_rows | engine_population | independent_source_reread */
  source_checksum_provenance?: string;
  target_checksum?: string;
  missing_key_count?: number;
  extra_key_count?: number;
  matched_key_count?: number;
  row_fidelity_score?: number;
  sample_compare?: {
    passed?: boolean;
    compared?: number;
    skipped?: boolean;
    /** Set when the destination read-back itself failed. */
    error?: string;
    alignment?: string;
    /**
     * Why a sample was not compared. ``alignment: "declined"`` means the engine
     * had no key that identifies a row, so pairing them by position would have
     * compared unrelated rows — it says so instead of reporting corruption.
     */
    reason?: string;
    identity_warning?: string;
    /** Deterministic sample set for auditor replay. */
    sample_seed?: {
      method?: string;
      size?: number;
      sort_key?: string;
      source_sort_key?: string;
      stratify_by?: string;
      auto_selected?: boolean;
      /** Always sample for Gate-8 keyed compare — never invent population proof. */
      coverage?: string;
      population_proof?: boolean;
      note?: string;
      pk_values?: string[];
      content_sha256?: string;
    };
    mismatches?: {
      row?: string | number;
      source?: string;
      target?: string;
      source_value?: string;
      target_value?: string;
      column?: string;
    }[];
  };
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
  /** Last-N load intelligence for this source→destination route. */
  load_history_report?: LoadHistoryReport;
  preflight?: PreflightResult;
  /** Gate-8 reconcile payload persisted on the job at terminal status. */
  reconciliation?: Gate8ReconciliationPayload;
  /** Plain-language pipeline explanation from the engine. */
  explanation?: string;
  /** Persisted per-mapping evidence (confidence, fidelity, risks) for Theater/Jobs. */
  mapping_proof?: Record<string, unknown>;
  /** Transfer plan id when the job was started from Studio plan flow. */
  plan_id?: string;
  mapping_version?: number;
  mapping_hash?: string;
  /** DDL / stream log lines (aliased as ddl_log for Jobs UI). */
  ddl_executed?: string[];
  ddl_log?: string[];
  /** Durable operator event lines (phase / message / row milestones). */
  event_log?: string[];
  sync_mode?: string;
  source_read_mode?: string;
  schema_policy?: string;
  validation_mode?: string;
  /** Operator who started the job. */
  triggered_by?: string;
  created_by?: string;
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
  /** True when chunks came from Tesseract OCR (scanned PDF). */
  ocr_used?: boolean;
  ocr_page_count?: number;
  ocr_status?: {
    available?: boolean;
    message?: string;
    missing?: string[];
  };
}

export interface ActiveDataContext {
  name: string;
  filename?: string;
  columns: string[];
  row_count: number;
  samples?: Record<string, string[]>;
  schema?: Record<string, string>;
  /** Validation run ID (pf_…) — Datawrap Pilot can look this up. */
  preflight_run_id?: string;
  /** Live transfer job ID once Execute starts. */
  job_id?: string;
  validation_status?: "passed" | "blocked" | "running" | string;
  route?: string;
  blockers?: string[];
  /** Pilot chat session id — scopes durable query result refs. */
  pilot_session_id?: string;
  /** Last sampled/query result_id for follow-up analyze/filter. */
  last_result_id?: string;
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
  status: "pass" | "block" | "skip" | "running" | "warn";
  message: string;
  duration_ms: number;
  details?: Record<string, unknown>;
}

export interface PreflightProofBundle {
  passed: boolean;
  semantic_mapping_score: number;
  semantic_notes: string[];
  /** null when sample quality was not profiled — never treat as 0%. */
  quality_score: number | null;
  confidence_band?: "high" | "medium" | "low";
  quality_grade?: "excellent" | "good" | "review" | "not_profiled";
  evidence_summary?: string;
  compliance: {
    risk_score: number;
    requires_review: boolean;
    tags: string[];
    details?: Record<string, unknown>;
    acknowledged?: boolean;
    review_status?: string;
    acknowledgment?: {
      actor?: string;
      at?: string;
      reason?: string;
    };
  };
  reconciliation: {
    passed: boolean;
    preview?: boolean;
    phase?: string;
    post_write_pending?: boolean;
    /** key_aligned | positional_only | unproven_identity */
    verification_mode?: string;
    identity?: { column?: string | null; proven?: boolean; reason?: string };
    source_rows?: number;
    target_rows?: number;
    rejected_rows?: number;
    coerced_null_rows?: number;
    rows_skipped?: number;
    source_checksum?: string | null;
    target_checksum?: string | null;
    row_fidelity_score?: number | null;
    matched_key_count?: number;
    missing_key_count?: number;
    extra_key_count?: number;
    message?: string;
    sample_compare?: {
      passed: boolean;
      compared: number;
      mismatches: { row: string; source: string; target: string; source_value: string; target_value: string }[];
      skipped?: boolean;
      error?: string;
      alignment?: string;
      /** Why a sample was not compared (see the payload shape above). */
      reason?: string;
      identity_warning?: string;
    };
  };
  transfer_decision: {
    decision: "approve" | "review" | "block";
    blockers: string[];
    reason: string;
    warnings?: string[];
    /** True when the only pending item is PII governance acknowledgment. */
    compliance_only?: boolean;
  };
  /** Module 1 — incomplete Risk Contracts block Execute-approve. */
  risk_contracts?: {
    incomplete?: boolean;
    missing_columns?: string[];
    note?: string;
  };
  /**
   * Module 8 — true only after post-write Gate-8 full_checksum.
   * Never infer from Validate / sample / writer-ack alone.
   */
  migration_proven?: boolean;
  /** Module 12 — Map→DDL identity fingerprint. */
  ddl_identity?: {
    ddl_identity_hash?: string;
    matches_approved?: boolean;
    note?: string;
  };
  /** Phase C11 — Decision Artifact content hash from Validate. */
  decision_artifact_hash?: string;
  decision_artifact?: {
    schema_version?: string;
    content_hash?: string;
    artifact_id?: string;
    ddl?: { ddl_identity_hash?: string };
  };
  /** Phase C8 — Validation Orchestrator class buckets. */
  validation_orchestrator?: {
    orchestrator?: string;
    decision_artifact_hash?: string | null;
    decision_artifact_present?: boolean;
    blocked_classes?: string[];
    population_note?: string;
    by_class?: Record<string, Array<{ id?: string; status?: string; message?: string }>>;
  };
  /** Module 12 — per-column ConversionClass stamps. */
  conversion_contract?: {
    version?: string;
    columns?: Array<{
      source?: string;
      target?: string;
      conversion_class?: string;
      invents_capacity?: boolean;
      requires_risk_contract?: boolean;
      reason?: string;
      risk_level?: string;
    }>;
  };
}

export interface CoercionSampleFailure {
  row: number;
  value: string;
  reason: string;
  wire_form?: string | null;
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
  wire_normalize?: number;
  wire_failures?: number;
  /** Bare scalars wrapped as JSON string literals (domain change — Accept risk). */
  json_scalar_wraps?: number;
  sample_failures: CoercionSampleFailure[];
  sentinel_examples?: { row: number; value: string }[];
  wire_examples?: { row: number; value: string; wire_form?: string | null; reason?: string }[];
  wrap_examples?: { row: number; value: string; wire_form?: string | null; reason?: string }[];
  sample_wire_form?: string | null;
  severity: "ok" | "warn" | "block";
  /** Declared type path collapses fidelity even when preview samples coerce. */
  fidelity_collapse?: boolean;
  suggested_fix?: string;
  suggested_target_type?: string | null;
  suggested_transform?: string | null;
  framing?: {
    kind?: string;
    label?: string;
    source_shape?: string;
    target_shape?: string;
    shape_preserved?: boolean;
    elements_preserved?: boolean;
    sample_round_trip?: boolean;
  } | null;
}

export interface CoercionReport {
  checked: number;
  sampled_rows: number;
  has_blocking_failures: boolean;
  columns: CoercionColumn[];
  by_source?: Record<string, CoercionColumn>;
}

/** Wall-clock time the engine spent in one phase of a transfer. */
export interface PhaseTiming {
  phase: string;
  label: string;
  seconds: number;
  calls: number;
  rows: number;
  /** Fraction of total busy time, 0–1. */
  share_of_busy: number;
  rows_per_second: number;
}

/**
 * Per-phase timing breakdown emitted by the transfer engine.
 *
 * `busy_seconds` sums each phase's own time, so it exceeds `elapsed_seconds`
 * when phases overlap across worker threads. `overlap_factor` is that ratio —
 * roughly 1.0 means serial, higher means real concurrency.
 */
export interface PhaseProfileReport {
  phases: PhaseTiming[];
  busy_seconds: number;
  elapsed_seconds: number;
  dominant_phase: string;
  overlap_factor: number;
}

/**
 * Per-run connection and metadata reuse. Proves the engine reused pools and
 * schema lookups across chunks instead of rebuilding them per batch — the
 * defect that used to cost a TCP+TLS+auth round-trip on every chunk.
 */
export interface ReuseCounters {
  hits: number;
  misses: number;
  reuse_ratio: number;
  connections_saved?: number;
  metadata_queries_saved?: number;
  live?: number;
  evictions?: number;
  invalidations?: number;
}

export interface ConnectionReuseReport {
  engine_pool?: ReuseCounters;
  schema_cache?: ReuseCounters;
}

/**
 * Whether an interrupted write can be retried in place without duplicating rows.
 * Surfaced so the operator can tell "resume is automatic" from "resume needs a
 * decision" without reading engine logs.
 */
export interface ReplaySafetyReport {
  safe: boolean;
  mechanism: "idempotent_upsert" | "chunk_ledger" | "keyed_document" | "none" | string;
  reason: string;
  destination?: string;
  write_mode?: string;
  duplicate_risk?: boolean;
  evidence?: string[];
}

/** One data test evaluated against a materialized transformation model. */
export interface TransformTestResult {
  model: string;
  test_type: string;
  column: string;
  severity: "error" | "warn";
  passed: boolean;
  failing_rows: number;
  message: string;
  sql?: string;
}

/** One model built by a post-load transformation project. */
export interface TransformModelResult {
  name: string;
  materialization: string;
  status: "success" | "failed" | "skipped";
  relation: string;
  strategy: string;
  rows_affected: number;
  seconds: number;
  sql: string;
  error: string;
  tests: TransformTestResult[];
}

/** Post-load transformation outcome attached to a finished transfer. */
export interface TransformationsReport {
  ran: boolean;
  status: "success" | "partial" | "failed" | "skipped";
  message: string;
  projects: {
    project_id: string;
    project_name: string;
    status: string;
    message?: string;
    seconds?: number;
    failed_model_count?: number;
    failed_test_count?: number;
    warnings?: string[];
    models: TransformModelResult[];
  }[];
}

/** Last-N load comparison for the same source→destination route. */
export interface LoadHistoryReport {
  prior_load_count?: number;
  compare_last_k?: number;
  passed?: boolean;
  anomalies?: string[];
  column_findings?: { column: string; signals?: { kind?: string; message?: string }[] }[];
  novel_quarantine_patterns?: {
    column: string;
    reason?: string;
    count?: number;
    prior_count?: number;
    kind?: string;
  }[];
  volume_note?: string | null;
  prior_runs_summary?: {
    captured_at?: string;
    job_id?: string | null;
    row_count?: number;
    rejected_rows?: number;
    quarantine_keys?: number;
  }[];
  warning?: string;
}

export interface PreflightResult {
  passed: boolean;
  passed_count: number;
  total_gates: number;
  readiness_score: number;
  /** Stable ID for this validation run — surface in UI and feed Datawrap Pilot. */
  run_id?: string;
  gates: PreflightGate[];
  blockers: { id: string; message: string; details?: Record<string, unknown>; guidance?: { gate?: string; title?: string; category?: string; why?: string; fix?: string; examples?: string[]; suggested_actions?: ValidationSuggestedAction[] } }[];
  /**
   * Engine-level Root Cause Engine output — one explainable problem, many gates.
   * Prefer this over client-side collapse when present.
   */
  root_causes?: Array<{
    root_id: string;
    kind: string;
    title: string;
    summary: string;
    business_impact: string;
    affected_columns?: string[];
    affected_rows_sample?: number | null;
    estimated_total_rows?: number | null;
    risk_level?: string;
    recommended_fix?: string;
    alternative_fixes?: string[];
    recovery_strategy?: string;
    expected_runtime_impact?: string;
    quarantine_policy?: string;
    rollback_policy?: string;
    documentation?: string;
    impacted_gates?: string[];
    absorbed_blocker_ids?: string[];
    severity?: string;
  }>;
  /** Top-level privilege probe from destination inspect (also on g2_destination.details). */
  privilege_probe?: {
    status?: string;
    method?: string;
    engine?: string;
    detail?: string;
    can_write?: boolean | null;
    can_create_table?: boolean | null;
  };
  /** Redshift COPY FROM S3 staging-bucket probe (also on g2_destination.details). */
  redshift_staging_probe?: {
    status?: string;
    method?: string;
    engine?: string;
    detail?: string;
    bucket?: string;
  };
  proof_bundle?: PreflightProofBundle;
  coercion_report?: CoercionReport;
  load_history_report?: LoadHistoryReport;
  /** Soft FK / relational hints — structured findings; block via constraint_fk when severity=block. */
  constraint_hints?: Array<Record<string, unknown> | string>;
  /** Structured FK findings (same payloads as constraint_hints when present). */
  constraint_findings?: Array<Record<string, unknown>>;
  /** Honesty stamp — schema FK coverage ≠ population RI proof. */
  referential_integrity?: {
    proven?: boolean;
    coverage?: string;
    population_orphan_probe_ran?: boolean;
    population_orphan_count?: number | null;
    sample_orphan_probe_ran?: boolean;
    sample_orphan_count?: number | null;
    finding_count?: number;
    note?: string;
    fk_risk_acknowledged?: boolean;
  };
  /** Sample-scoped orphan probe report (never population proof). */
  sample_orphan_probe?: {
    ran?: boolean;
    coverage?: string;
    population_proof?: boolean;
    orphan_count?: number;
    checked_values?: number;
    note?: string;
  };
  /** Module 11 — opt-in full-table orphan scan (only path to RI proven). */
  population_orphan_probe?: {
    ran?: boolean;
    coverage?: string;
    population_proof?: boolean;
    complete?: boolean;
    orphan_count?: number;
    child_table?: string;
    note?: string;
  };
  /** Soft Snowflake warehouse sizing from G7 volume — never a GateId. */
  snowflake_warehouse_advice?: {
    kind?: string;
    recommended_size?: string;
    credit_band?: string;
    estimated_bytes?: number;
    estimated_gib?: number;
    message?: string;
    rationale?: string;
    honesty?: string;
    current_warehouse?: string | null;
  };
  /**
   * Signed Migration Risk Contracts echoed from Validate hydrate.
   * Merge onto Map so Execute sees risk_id + signature (not unsigned drafts).
   */
  signed_mappings?: Array<{
    source?: string;
    target?: string;
    risk_contract?: Record<string, unknown>;
    risk_acknowledged?: boolean;
  }>;
  /**
   * Decision Kernel target_type stamps from Validate (additive / create-new).
   * Merge onto Map so Studio destType matches what Execute will refuse without.
   */
  stamped_mappings?: Array<{
    source?: string;
    target?: string;
    target_type?: string;
    create_new?: boolean;
    assignment_strategy?: string;
  }>;
  /** Canonical Kernel ValidationFinding dicts (coercion → findings SSOT). */
  validation_findings?: Array<Record<string, unknown>>;
  /** G13/G14/G15 mapping contract — extras, dest-only, write_by=name. */
  source_coverage?: {
    unaccounted?: string[];
    omitted?: string[];
    written?: string[];
    shape_contract?: {
      shape?: string;
      headline?: string;
      detail?: string;
      primary_action?: string;
      unaccounted_sources?: string[];
      extra_source_columns?: string[];
      dest_only?: Array<{ target?: string; kind?: string }>;
      counts?: Record<string, number>;
      write_by?: string;
    };
  };
  /** Callable extract: FK catalog was not probed against a fake relation. */
  source_fk_catalog?: {
    ran?: boolean;
    skipped?: boolean;
    reason?: string;
    note?: string;
  };
  /**
   * Bounded destination carriers decided before the write (g3f_population_fit).
   * `evidence` is the strength of the walk, never the wish: `exact` only when
   * every source row was scanned, `sampled` for a preview, `partial` when a
   * budget stopped it, `unmeasured` when no rows were available.
   */
  population_fit?: {
    evidence?: "exact" | "partial" | "sampled" | "unmeasured";
    rows_scanned?: number;
    rows_total?: number;
    scanned_population?: boolean;
    unfit_rows?: number;
    note?: string;
    error?: string;
    bounded_columns?: Array<{
      source?: string;
      target?: string;
      target_type?: string;
      carrier?: string;
      write_action?: string;
      execution_policy?: string;
      aborts_job?: boolean;
    }>;
    findings?: Array<{
      source?: string;
      target?: string;
      target_type?: string;
      unfit_rows?: number;
      example_rows?: number[];
      example_values?: string[];
      aborts_job?: boolean;
      reason?: string;
    }>;
    undecidable_columns?: string[];
    safe_by_declaration?: string[];
  };
  /** Procedure / SQL extract honesty — catalog probes skipped, CDC/SCD2/mirror refused. */
  callable_extract?: {
    mode?: string;
    catalog_probes?: string;
    note?: string;
    cdc?: string;
    history_sync?: string;
    incremental?: string;
    honesty?: string;
  };
}

/** Machine-readable next step from POST /preflight/explain — mapped to Studio controls. */
export type ValidationActionKind =
  | "change_target_type"
  | "add_transform"
  | "map_column"
  | "review_mappings"
  | "rerun_mapping"
  | "check_connection"
  | "normalize_control_chars"
  | "quarantine_and_rerun"
  | "open_bad_data_fix"
  | "fix_source_keys"
  | "confirm_or_remap"
  | "reload_dest_schema"
  | "confirm_add"
  | "continue_validate"
  | "fix_orphans"
  | "run_population_orphan_scan";

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
    /** How many quarantine details were dropped past the sample cap. */
    rejected_details_truncated?: number;
    warnings?: string[];
    /** How many distinct warnings were suppressed past the display sample. */
    warnings_suppressed?: number;
    error_policy?: string;
    /** Pre-ingestion staging table when write_via_staging was used. */
    staging_table?: string;
    staged_rows?: number;
    promoted_rows?: number;
    promote_blocked?: boolean;
    pre_ingestion_staging?: Record<string, unknown>;
    filename?: string;
    download_url?: string;
    load_method?: string;
    chunk_size?: number;
    batches?: number;
    records_per_second?: number;
    load_history_report?: LoadHistoryReport;
    phase_profile?: PhaseProfileReport;
    transformations?: TransformationsReport;
    connection_reuse?: ConnectionReuseReport;
    /** Whether an interrupted write could have been retried without duplicating rows. */
    replay_safety?: ReplaySafetyReport;
    /** OpenTelemetry trace id when DATAFLOW_ENABLE_TRACING=1. */
    trace_id?: string;
    /** Inbound X-Correlation-ID bridged into the transfer root span. */
    correlation_id?: string;
  };
  records_per_second?: number;
  ddl_executed?: string[];
  operation?: string;
  error?: string;
  error_details?: Record<string, unknown>;
  reconciliation?: Gate8ReconciliationPayload;
  explanation?: string;
  mapping_proof?: Record<string, unknown>;
  job_id?: string;
  /**
   * Independent dest COUNT(*) conservation from execute_tracked.
   * Display only — dest is never records_transferred.
   */
  row_accounting?: import("./conservationLedger").ConservationLedger | null;
  /** CDC operator signals copied from the completed job. */
  cdc_lag_seconds?: number | null;
  cdc_plugin?: string | null;
  cdc_delivery?: string | null;
  cdc_row_filter?: string | null;
  cdc_shared_reader?: boolean | null;
  snapshot_mode?: string | null;
  snapshot_plan?: TransferJob["snapshot_plan"];
  watermark?: string | null;
  cdc_lease_holder?: string | null;
  cdc_lease_backend?: string | null;
  /** Source Always On / Data Guard role when probed. */
  source_ha_role?: string | null;
  source_ha_topology?: string | null;
  source_ha_group?: string | null;
  source_ha_message?: string | null;
  cdc_retention_status?: string | null;
  cdc_retention_resume?: string | null;
  cdc_retention_retained?: string | null;
  cdc_retention_message?: string | null;
  cdc_retention_dialect?: string | null;
  cdc_cursor_gap?: boolean | null;
  cdc_cursor_gap_code?: string | null;
  cdc_cursor_gap_dialect?: string | null;
  cdc_cursor_gap_resume?: string | null;
  cdc_cursor_gap_retained?: string | null;
  cdc_lease_cursor_key?: string | null;
  error_code?: string | null;
  /** Workspace notification dispatch results copied from the completed job. */
  notifications?: JobNotificationResult[];
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
  delivery_guarantee?: string;
  cursor_column: string;
  primary_key: string;
  source_read_mode?: string;
  procedure_call?: string;
  source_query?: string;
  procedure_params?: Record<string, string>;
  mappings: Record<string, unknown>[];
  stream_contracts: Record<string, unknown>[];
  workspace_id: string;
  max_retries: number;
  retry_backoff_seconds: number;
  notify_on_failure: boolean;
  notify_on_success: boolean;
  enabled: boolean;
  /** Optional signed data contract enforced on each scheduled run. */
  contract_id?: string;
  /** When true (default if contract_id set), refuse to schedule/enable until SIGNED. */
  require_signed_contract?: boolean;
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
  delivery_guarantee?: string;
  cursor_column: string;
  primary_key: string;
  cursor_value: string;
  source_read_mode?: string;
  procedure_call?: string;
  source_query?: string;
  procedure_params?: Record<string, string>;
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
  /** Data contract bound to this pipeline (enforced when require_signed_contract). */
  contract_id?: string;
  require_signed_contract?: boolean;
  /** Present on GET /schedules/{id}; omitted from list summaries. */
  mappings?: { source: string; target: string; confidence?: number; transform?: string | null }[];
  mapping_count?: number;
  /** Dual Run campaign (consecutive parallel-run cycles). Display-only. */
  fidelity_campaign?: {
    verdict?: string;
    consecutive_passes?: number;
    required_consecutive?: number;
    next_action?: string;
    note?: string;
    last_check?: {
      passed?: boolean;
      message?: string;
      divergent_columns?: string[];
      assurance_level?: string;
    } | null;
  };
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
  /** Why no further attempt was queued after this failure. */
  retry_refused?: string;
  /** Whether this failure can change on a later attempt, and what fixes it. */
  failure_class?: {
    kind: "transient" | "deterministic" | "unknown";
    reason: string;
    corrective_action: string;
    retryable: boolean;
  } | null;
  /** Independent dest COUNT(*) ledger copied from the completed job. */
  row_accounting?: import("./conservationLedger").ConservationLedger | null;
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
  { id: "pgvector", label: "pgvector (PostgreSQL)", port: 5432 },
  { id: "qdrant", label: "Qdrant", port: 6333 },
  { id: "weaviate", label: "Weaviate", port: 8080 },
  { id: "pinecone", label: "Pinecone", port: 443 },
  { id: "milvus", label: "Milvus", port: 19530 },
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
  { id: "iceberg", label: "Apache Iceberg", port: 0 },
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
  { id: "pdf", label: "PDF", port: 0 },
  { id: "docx", label: "Word (DOCX)", port: 0 },
  { id: "html", label: "HTML", port: 0 },
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
