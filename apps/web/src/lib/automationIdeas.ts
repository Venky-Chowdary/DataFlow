export interface AutomationIdea {
  id: string;
  category: string;
  title: string;
  prompt: string;
  description: string;
}

export interface AutomationCategory {
  id: string;
  label: string;
}

export const AUTOMATION_CATEGORIES: AutomationCategory[] = [
  { id: "all", label: "All" },
  { id: "logistics", label: "Logistics" },
  { id: "finance", label: "Finance" },
  { id: "retail", label: "Retail" },
  { id: "compliance", label: "Compliance" },
  { id: "analytics", label: "Analytics" },
  { id: "data_ops", label: "Data Ops" },
];

export const AUTOMATION_IDEAS: AutomationIdea[] = [
  {
    id: "logistics_mongo",
    category: "logistics",
    title: "Logistics CSV → MongoDB",
    prompt: "Move my logistics CSV to MongoDB with auto-created collection and typed columns",
    description: "Upload freight data — CUST_ID, AMT, TRACK_NO detected automatically.",
  },
  {
    id: "pii_scan_hr",
    category: "compliance",
    title: "PII scan on HR data",
    prompt: "What PII is in my HR data and what compliance frameworks apply?",
    description: "GDPR, HIPAA, PCI-DSS tags before transfer.",
  },
  {
    id: "payments_snowflake",
    category: "finance",
    title: "Payments → Snowflake",
    prompt: "Transfer payments data to Snowflake with auto DDL and typed columns",
    description: "CREATE TABLE IF NOT EXISTS with warehouse-native types.",
  },
  {
    id: "retail_compare",
    category: "retail",
    title: "Compare retail vs logistics",
    prompt: "Compare retail and logistics datasets — shared columns and differences",
    description: "Schema diff before merging datasets.",
  },
  {
    id: "pg_migration",
    category: "data_ops",
    title: "PostgreSQL → MongoDB",
    prompt: "Migrate data from PostgreSQL table to MongoDB collection",
    description: "Universal DB-to-DB — any source, any destination.",
  },
  {
    id: "job_audit",
    category: "data_ops",
    title: "Transfer job audit",
    prompt: "Show my recent transfer jobs and flag any failures",
    description: "Job history with record counts and status.",
  },
  {
    id: "quality_report",
    category: "analytics",
    title: "Data quality report",
    prompt: "Analyze all my datasets and give me quality scores and PII summary",
    description: "Cross-dataset quality from Data Pilot.",
  },
  {
    id: "file_export",
    category: "data_ops",
    title: "JSON to CSV export",
    prompt: "Convert my JSON file to CSV export",
    description: "File-to-file via universal engine.",
  },
  {
    id: "semantic_map",
    category: "analytics",
    title: "Semantic column mapping",
    prompt: "Map logistics columns to PostgreSQL with AI semantic types",
    description: "AMT→amount, cust_id→customer_id at 92%+ confidence.",
  },
];
