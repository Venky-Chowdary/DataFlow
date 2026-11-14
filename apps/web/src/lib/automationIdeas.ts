export interface AutomationIdea {
  id: string;
  category: string;
  title: string;
  prompt: string;
  description: string;
}

/**
 * UX-18: the Pilot start page's tiles. Each is answered from this workspace's
 * own connectors, jobs and schedules, so none dead-ends on a demo dataset
 * (logistics CSV, HR data, "count airports") the workspace never had.
 */
export const WORKSPACE_IDEAS: AutomationIdea[] = [
  {
    id: "briefing",
    category: "workspace",
    title: "Workspace briefing",
    prompt: "Give me a workspace briefing",
    description: "Connections, recent runs and paused schedules, counted from this workspace.",
  },
  {
    id: "connections",
    category: "connections",
    title: "My connections",
    prompt: "Show my connectors",
    description: "Each saved connection with its last test result.",
  },
  {
    id: "recent_jobs",
    category: "jobs",
    title: "Recent runs",
    prompt: "Show my recent jobs",
    description: "What ran, how many rows, and which runs need attention.",
  },
  {
    id: "capabilities",
    category: "help",
    title: "What Pilot can do",
    prompt: "What can you do?",
    description: "The questions and actions Pilot supports, with examples.",
  },
];

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
    title: "Workspace briefing",
    prompt: "Give me a workspace briefing",
    description: "Live sitrep of connectors, jobs, and parked pipelines — counts from stores, not a demo.",
  },
  {
    id: "quality_report",
    category: "analytics",
    title: "Data quality report",
    prompt: "Analyze all my datasets and give me quality scores and PII summary",
    description: "Cross-dataset quality from Datawrap Pilot.",
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
