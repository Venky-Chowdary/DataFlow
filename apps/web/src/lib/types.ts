export const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8001/api/v1";

export type Screen = "dashboard" | "pilot" | "transfer" | "connectors" | "jobs" | "mcp" | "settings";

export interface Connector {
  id: string;
  name: string;
  type: string;
  host: string;
  port: number;
  database: string;
  status: string;
  created_at: string;
}

export interface TransferJob {
  _id: string;
  source_type: string;
  source_name: string;
  destination_type: string;
  destination_database: string;
  destination_collection: string;
  status: string;
  records_processed: number;
  created_at: string;
}

export interface ParsedUpload {
  row_count: number;
  columns: string[];
  file_type?: string;
  sample_data?: Record<string, unknown>[];
  data?: Record<string, unknown>[];
  schema?: Record<string, string>;
}

export interface ActiveDataContext {
  name: string;
  filename?: string;
  columns: string[];
  row_count: number;
  samples?: Record<string, string[]>;
  schema?: Record<string, string>;
}

export interface ColumnAnalysis {
  column_name: string;
  semantic_type?: string;
  inferred_type?: string;
  confidence: number;
  is_pii: boolean;
  compliance: string[];
  canonical_form?: string;
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
  status: "pass" | "block" | "skip";
  message: string;
  duration_ms: number;
}

export interface PreflightResult {
  passed: boolean;
  passed_count: number;
  total_gates: number;
  readiness_score: number;
  gates: PreflightGate[];
  blockers: { id: string; message: string }[];
}

export interface TransferResult {
  success: boolean;
  records_transferred?: number;
  destination?: { database: string; collection: string; path?: string; format?: string };
  ddl_executed?: string[];
  operation?: string;
  error?: string;
}

export interface TransferPlan {
  supported: boolean;
  message: string;
  operation: string;
  auto_create: string[];
  type_mappings: { column: string; source_type: string; dest_type: string }[];
  source_columns?: string[];
}

export const CONNECTOR_CATALOG = [
  { id: "mongodb", label: "MongoDB", port: 27017 },
  { id: "postgresql", label: "PostgreSQL", port: 5432 },
  { id: "mysql", label: "MySQL", port: 3306 },
  { id: "snowflake", label: "Snowflake", port: 443 },
  { id: "bigquery", label: "BigQuery", port: 443 },
  { id: "s3", label: "Amazon S3", port: 443 },
  { id: "salesforce", label: "Salesforce", port: 443 },
  { id: "kafka", label: "Apache Kafka", port: 9092 },
  { id: "redis", label: "Redis", port: 6379 },
  { id: "csv", label: "CSV Files", port: 0 },
  { id: "json", label: "JSON Files", port: 0 },
] as const;
