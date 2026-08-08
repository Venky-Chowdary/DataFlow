/** Transfer Studio step / upload constants (Phase F9 extraction from TransferPage). */

export const FILE_FORMAT_SOURCE_TYPES = new Set([
  "csv",
  "tsv",
  "json",
  "jsonl",
  "ndjson",
  "excel",
  "parquet",
  "avro",
  "orc",
  "xml",
  "pdf",
  "docx",
  "html",
]);

export const STEP_SOURCE = 1;
export const STEP_DESTINATION = 2;
export const STEP_MAP = 3;
export const STEP_VALIDATE = 4;
export const STEP_RUN = 5;

export const STEPS: { n: number; label: string; shortLabel: string; icon: string }[] = [
  { n: STEP_SOURCE, label: "Source", shortLabel: "Src", icon: "upload" },
  { n: STEP_DESTINATION, label: "Destination", shortLabel: "Dest", icon: "connectors" },
  { n: STEP_MAP, label: "Map", shortLabel: "Map", icon: "sparkle" },
  { n: STEP_VALIDATE, label: "Validate", shortLabel: "Gate", icon: "gate" },
  { n: STEP_RUN, label: "Run", shortLabel: "Run", icon: "transfer" },
];

export const RUN_LAUNCH_STAGES = [
  "Submitting governed job request",
  "Locking approved mapping revision",
  "Provisioning destination writer",
  "Opening live telemetry stream",
] as const;

export const CLOUD_SOURCE_TYPES = new Set([
  "s3",
  "gcs",
  "google_cloud_storage",
  "azure_blob",
  "adls",
]);

export const FALLBACK_DEST_TYPES = [
  "mongodb",
  "postgresql",
  "mysql",
  "snowflake",
  "bigquery",
] as const;

export const FALLBACK_EXPORT_FORMATS = ["csv", "json", "jsonl"] as const;

export const ACCEPTED_UPLOAD_EXTENSIONS = new Set([
  "csv",
  "json",
  "jsonl",
  "tsv",
  "parquet",
  "pdf",
  "docx",
  "html",
  "htm",
  "xlsx",
  "xls",
  "xml",
]);

export const MAX_UPLOAD_BYTES = 250 * 1024 * 1024;
export const UPLOAD_FORMATS = [
  "JSON",
  "CSV",
  "JSONL",
  "TSV",
  "Excel",
  "Parquet",
  "PDF",
  "DOCX",
  "HTML",
] as const;
