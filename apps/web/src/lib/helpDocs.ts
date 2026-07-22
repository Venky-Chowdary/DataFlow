export type HelpDocId =
  | "help-getting-started"
  | "help-installation"
  | "help-transfer-studio"
  | "help-preflight-gates"
  | "help-semantic-mapping"
  | "help-connectors"
  | "help-pipelines"
  | "help-data-pilot"
  | "help-mcp"
  | "help-query"
  | "help-job-theater"
  | "help-enterprise"
  | "help-api"
  | "help-faq";

export interface HelpDocFigure {
  src: string;
  alt: string;
  caption: string;
}

export interface HelpDocSection {
  id: string;
  title: string;
  body: string;
  steps?: string[];
  code?: string;
  tip?: string;
  figure?: HelpDocFigure;
}

export interface HelpDocArticle {
  id: HelpDocId;
  slug: string;
  category: string;
  title: string;
  description: string;
  readTime: string;
  icon: string;
  sections: HelpDocSection[];
}

export interface HelpDocCategory {
  id: string;
  title: string;
  description: string;
  docs: HelpDocId[];
}

export const HELP_DOC_IDS: HelpDocId[] = [
  "help-getting-started",
  "help-installation",
  "help-transfer-studio",
  "help-preflight-gates",
  "help-semantic-mapping",
  "help-connectors",
  "help-pipelines",
  "help-data-pilot",
  "help-mcp",
  "help-query",
  "help-job-theater",
  "help-enterprise",
  "help-api",
  "help-faq",
];

export const HELP_DOC_CATEGORIES: HelpDocCategory[] = [
  {
    id: "start",
    title: "Getting started",
    description: "Install, connect, and run your first governed transfer.",
    docs: ["help-getting-started", "help-installation"],
  },
  {
    id: "transfer",
    title: "Transfer Studio",
    description: "Map schemas, run preflight, write with proof.",
    docs: ["help-transfer-studio", "help-preflight-gates", "help-semantic-mapping"],
  },
  {
    id: "connect",
    title: "Connectors & pipelines",
    description: "Drivers, catalogs, schedules, and incremental sync.",
    docs: ["help-connectors", "help-pipelines"],
  },
  {
    id: "ai",
    title: "Pilot, MCP & Query",
    description: "Agent-native and ad-hoc SQL surfaces.",
    docs: ["help-data-pilot", "help-mcp", "help-query"],
  },
  {
    id: "ops",
    title: "Operations",
    description: "Job Theater, quarantine, and reconciliation.",
    docs: ["help-job-theater"],
  },
  {
    id: "enterprise",
    title: "Enterprise & API",
    description: "SSO, tenants, security, and REST reference.",
    docs: ["help-enterprise", "help-api", "help-faq"],
  },
];

const ARTICLES: Record<HelpDocId, HelpDocArticle> = {
  "help-getting-started": {
    id: "help-getting-started",
    slug: "getting-started",
    category: "Getting started",
    title: "Introduction to DataFlow",
    description: "Learn the platform surfaces and run your first checksum-proven transfer in under an hour.",
    readTime: "8 min",
    icon: "book",
    sections: [
      {
        id: "overview",
        title: "What is DataFlow?",
        body: "DataFlow is a universal data transfer platform. Transfer Studio maps schemas across systems, runs eight preflight gates before any production write, and proves every load with checksum reconciliation. Data Pilot and MCP bring the same governed engine to chat and agent workflows.",
      },
      {
        id: "surfaces",
        title: "Product surfaces",
        body: "Most teams start in Transfer Studio, then expand to scheduled pipelines, Data Pilot for triage, and MCP for Cursor or Claude integrations.",
        steps: [
          "Transfer Studio — map, preflight, write, reconcile",
          "Jobs & Schedules — recurring sync with quarantine",
          "Data Pilot — natural-language job triage",
          "MCP Server — agent-native governed transfers",
          "Query Playground — ad-hoc SQL before you transfer",
        ],
      },
      {
        id: "first-transfer",
        title: "Your first transfer (step by step)",
        body: "Follow this exact path inside the signed-in workspace. Every screenshot below is from the live application — not a marketing mock.",
        steps: [
          "Open Overview — confirm connections and recent jobs load for your workspace.",
          "Go to Connectors — add or test Local Postgres / MySQL / MongoDB (or upload a file in Transfer).",
          "Open Transfer → Source — Load sample orders CSV (or pick a connector) and review Detected structure.",
          "Continue Destination → Map → Validate — review confidence scores; fix any blocked preflight gates.",
          "Run the load — open Jobs (Job Theater) and confirm reconcile shows row fidelity matched.",
        ],
        tip: "File-to-file demo transfers work offline. Live database connectors need the API on port 8001.",
        figure: {
          src: "/docs/screenshots/app-overview.png",
          alt: "DataFlow workspace Overview with live metrics, throughput, and connections",
          caption: "Step 1 — Overview shows live rows moved, success rate, and connection health from your workspace.",
        },
      },
      {
        id: "workspace",
        title: "Workspace concepts",
        body: "Every transfer runs inside a workspace with connectors, saved routes, synonym dictionaries, and audit history. Team members inherit RBAC from your IdP in enterprise tenants.",
        steps: [
          "Connectors — encrypted credentials scoped to workspace",
          "Saved routes — reuse mapping + write mode presets",
          "Synonym dictionary — accepted semantic pairs for auto-map",
          "Job Theater — unified run history with quarantine and proof",
        ],
      },
    ],
  },
  "help-installation": {
    id: "help-installation",
    slug: "installation",
    category: "Getting started",
    title: "Installation & workspace setup",
    description: "Deploy DataFlow locally, in your cloud, or as enterprise SaaS with SSO.",
    readTime: "10 min",
    icon: "server",
    sections: [
      {
        id: "local",
        title: "Local development",
        body: "Run the web app and API from the monorepo. File transfers and the connector catalog work against a local API.",
        code: "npm install\nnpm run dev",
        figure: {
          src: "/docs/screenshots/app-overview.png",
          alt: "DataFlow workspace after local login showing Overview metrics",
          caption: "After local setup — Overview loads with live workspace metrics.",
        },
      },
      {
        id: "cloud",
        title: "Cloud & enterprise",
        body: "Enterprise tenants deploy at your-company.dataflow.io with SAML/OIDC SSO, BYOK encryption, and region pinning for residency.",
        steps: [
          "Contact sales for tenant provisioning",
          "Configure IdP metadata for SSO",
          "Upload KMS key for BYOK (optional)",
          "Invite workspace members with RBAC roles",
        ],
      },
      {
        id: "requirements",
        title: "System requirements",
        body: "Modern Chromium, Firefox, or Safari. API server requires PostgreSQL for job metadata. Connector drivers may need outbound network access to source and destination systems.",
      },
      {
        id: "env",
        title: "Environment variables",
        body: "Configure the API and web app with standard environment variables for database URL, encryption keys, and optional SSO metadata.",
        code: "DATABASE_URL=postgresql://...\nDATAFLOW_SECRET_KEY=...\nDATAFLOW_SSO_METADATA_URL=...",
      },
    ],
  },
  "help-transfer-studio": {
    id: "help-transfer-studio",
    slug: "transfer-studio",
    category: "Transfer Studio",
    title: "Transfer Studio guide",
    description: "End-to-end wizard for any→any loads with semantic mapping and proof.",
    readTime: "12 min",
    icon: "transfer",
    sections: [
      {
        id: "wizard",
        title: "Wizard flow",
        body: "Transfer Studio walks you through source selection, destination selection, column mapping, preflight, write mode, and post-load reconciliation.",
        steps: [
          "Choose source connector or file",
          "Choose destination connector or file",
          "Review auto-generated semantic maps",
          "Select write mode: append, overwrite, upsert, incremental",
          "Run preflight — fix any blocked gates",
          "Execute and monitor in Job Theater",
        ],
        figure: {
          src: "/docs/screenshots/app-transfer-source.png",
          alt: "Transfer Studio Source step with sample-orders.csv profiled rows and typed columns",
          caption: "Transfer Studio · Source — upload or connect, then review detected structure (types + sample rows) before destination.",
        },
      },
      {
        id: "write-modes",
        title: "Write modes",
        body: "Append adds rows. Overwrite replaces target tables. Upsert merges on keys where the destination supports it. Incremental uses watermark columns for recurring sync.",
      },
      {
        id: "quarantine",
        title: "Quarantine",
        body: "Rows that fail validation during load are isolated with column, value, and reason — never silently dropped. Inspect quarantine in Job Theater before reprocessing.",
      },
      {
        id: "proof",
        title: "Post-load proof",
        body: "After every write, DataFlow reconciles row counts and content hashes between source sample and destination. Export proof reports for finance and compliance reviewers.",
        tip: "Proof runs automatically — no separate reconciliation job required.",
      },
    ],
  },
  "help-preflight-gates": {
    id: "help-preflight-gates",
    slug: "preflight-gates",
    category: "Transfer Studio",
    title: "Preflight gates explained",
    description: "Eight fail-fast checks that block dangerous writes before production.",
    readTime: "9 min",
    icon: "gate",
    sections: [
      {
        id: "list",
        title: "The eight gates",
        body: "Every production write passes through these gates. Failures block the write and surface actionable errors.",
        steps: [
          "Schema compatibility — columns and types align",
          "Nullability — required fields won't be null",
          "Type coercion — safe casts only",
          "Row volume — within destination capacity",
          "Permission probe — write access confirmed",
          "Key uniqueness — upsert keys valid",
          "Sample validation — spot-check mapped rows",
          "Policy check — workspace rules satisfied",
        ],
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater showing completed e2e_customers transfer with reconcile timeline",
          caption: "Job Theater — every phase from queue through reconcile, with row fidelity proof on the selected job.",
        },
      },
      {
        id: "fix",
        title: "Fixing gate failures",
        body: "Open the gate detail panel for the failing check. Adjust mappings, filters, or write mode. Re-run preflight until all gates pass.",
        tip: "Data Pilot can explain gate failures in plain language.",
      },
      {
        id: "when",
        title: "When gates run",
        body: "Preflight executes before every production write and on schedule for recurring pipelines. Dry-run mode lets you validate without writing — useful in CI/CD before promoting to production.",
      },
    ],
  },
  "help-semantic-mapping": {
    id: "help-semantic-mapping",
    slug: "semantic-mapping",
    category: "Transfer Studio",
    title: "Semantic column mapping",
    description: "How DataFlow infers roles, synonyms, and confidence scores.",
    readTime: "11 min",
    icon: "sparkle",
    sections: [
      {
        id: "roles",
        title: "Semantic roles",
        body: "DataFlow detects roles like amount, email, identifier, timestamp, and address — not just string name matching.",
        figure: {
          src: "/docs/screenshots/app-transfer-source.png",
          alt: "Detected structure panel showing order_id, customer_email, order_amt typed columns",
          caption: "Semantic roles start from profiled columns — INTEGER, VARCHAR, DECIMAL, DATE — before name matching.",
        },
      },
      {
        id: "synonyms",
        title: "Synonym dictionary",
        body: "Accept or reject mapping suggestions. Accepted pairs enter your workspace synonym dictionary for future auto-maps.",
      },
      {
        id: "confidence",
        title: "Confidence scores",
        body: "Each map shows a confidence percentage. Review anything below your threshold before production write.",
      },
      {
        id: "drift",
        title: "Schema drift detection",
        body: "DataFlow compares current source and destination schemas against your saved route. Drift warnings appear in Transfer Studio and Data Pilot before the next scheduled run.",
        steps: [
          "New columns in source — review suggested maps",
          "Removed columns — confirm destination still valid",
          "Type changes — preflight type-coercion gate validates casts",
        ],
      },
    ],
  },
  "help-connectors": {
    id: "help-connectors",
    slug: "connectors",
    category: "Connectors",
    title: "Connector catalog & drivers",
    description: "Native drivers, SQLAlchemy generics, and honest transfer-ready labels.",
    readTime: "7 min",
    icon: "connectors",
    sections: [
      {
        id: "labels",
        title: "Transfer-ready labels",
        body: "Each connector shows live, beta, planned, or connect-only status. We don't inflate marketplace counts — labels reflect what works for governed transfer today.",
        figure: {
          src: "/docs/screenshots/app-connectors.png",
          alt: "Connectors page listing Local Postgres, MySQL, MongoDB with healthy and error states",
          caption: "Connectors — saved connections with Test passed / Test failed status from your live workspace.",
        },
      },
      {
        id: "native",
        title: "Native drivers",
        body: "PostgreSQL, MySQL, MongoDB, Snowflake, BigQuery, Redshift, S3, CSV, JSON, and more ship with upsert and incremental where supported.",
      },
      {
        id: "sqlalchemy",
        title: "SQLAlchemy generics",
        body: "Generic SQLAlchemy URLs extend reach to additional databases with standard read/write paths and preflight validation.",
      },
      {
        id: "credentials",
        title: "Credential storage",
        body: "Connector credentials are encrypted at rest with workspace-scoped keys. Enterprise tenants can bring your own KMS key (BYOK). DataFlow never logs raw passwords or connection strings in Job Theater.",
        tip: "Test connectivity from the connector panel before saving to Transfer Studio.",
      },
    ],
  },
  "help-pipelines": {
    id: "help-pipelines",
    slug: "pipelines",
    category: "Connectors",
    title: "Pipelines & recurring sync",
    description: "Schedule hourly, daily, or weekly loads with watermark incremental and quarantine.",
    readTime: "10 min",
    icon: "activity",
    sections: [
      {
        id: "schedules",
        title: "Schedules",
        body: "Create pipelines from saved Transfer Studio routes. Cron-style schedules trigger governed loads on interval.",
        figure: {
          src: "/docs/screenshots/app-pipelines.png",
          alt: "Pipelines page empty state with Create pipeline action in the workspace",
          caption: "Pipelines — create recurring syncs that reuse Transfer Studio’s gates on every tick.",
        },
      },
      {
        id: "incremental",
        title: "Incremental modes",
        body: "Watermark incremental tracks a monotonic column. Upsert merges on primary keys. Failed rows quarantine without stopping the pipeline.",
      },
      {
        id: "monitoring",
        title: "Pipeline monitoring",
        body: "Each scheduled run appears in Job Theater with the same preflight and proof stages as ad-hoc transfers. Email and webhook alerts fire on gate failure or quarantine threshold breaches.",
        steps: [
          "Open Jobs → select pipeline",
          "Review last run status and quarantine count",
          "Drill into Job Theater for row-level detail",
          "Re-run or pause schedule from the pipeline drawer",
        ],
      },
    ],
  },
  "help-data-pilot": {
    id: "help-data-pilot",
    slug: "data-pilot",
    category: "Pilot & MCP",
    title: "Data Pilot",
    description: "Natural-language triage for transfers, jobs, and schema questions.",
    readTime: "6 min",
    icon: "sparkle",
    sections: [
      {
        id: "use",
        title: "When to use Pilot",
        body: "Ask about failed preflight gates, job status, connector readiness, or mapping suggestions. Pilot uses the same workspace context as Transfer Studio.",
        figure: {
          src: "/docs/screenshots/app-pilot.png",
          alt: "Data Pilot chat workspace with suggested prompts and logistics/finance scenarios",
          caption: "Data Pilot — natural-language triage on the same governed engine, with chats saved in the browser.",
        },
      },
      {
        id: "handoff",
        title: "Hand off to Transfer Studio",
        body: "When you need the full wizard, open Transfer Studio from Pilot suggestions — maps and gates carry over.",
      },
      {
        id: "examples",
        title: "Example prompts",
        body: "Pilot understands workspace context — ask about specific jobs, connectors, or mapping decisions.",
        steps: [
          "Why did preflight gate 3 fail on job DF-8842?",
          "Which connectors are transfer-ready for Snowflake upsert?",
          "Suggest a map for order_amt → payment_amount",
          "Show quarantined rows from last night's pipeline",
        ],
      },
    ],
  },
  "help-mcp": {
    id: "help-mcp",
    slug: "mcp",
    category: "Pilot & MCP",
    title: "MCP Server for agents",
    description: "Governed transfer tools for Cursor, Claude, and VS Code.",
    readTime: "8 min",
    icon: "zap",
    sections: [
      {
        id: "tools",
        title: "Available tools",
        body: "MCP exposes connectors, transfer preflight, job status, and catalog queries. Agents inherit workspace RBAC — no raw destination passwords.",
        figure: {
          src: "/docs/screenshots/app-pilot.png",
          alt: "Data Pilot and agent-adjacent workspace surface",
          caption: "MCP tools drive the same governed engine you see in Pilot and Transfer Studio.",
        },
      },
      {
        id: "setup",
        title: "Setup",
        body: "Add the DataFlow MCP server to your agent config. Authenticate with workspace token. All runs appear in Job Theater with full audit trail.",
        code: '{\n  "mcpServers": {\n    "dataflow": {\n      "url": "https://api.dataflow.io/mcp"\n    }\n  }\n}',
      },
      {
        id: "security",
        title: "Agent security",
        body: "MCP tools inherit workspace RBAC. Agents cannot read raw connector secrets — only trigger governed operations your role allows. Every agent-initiated transfer is tagged in audit logs.",
      },
    ],
  },
  "help-query": {
    id: "help-query",
    slug: "query-playground",
    category: "Pilot & MCP",
    title: "Query Playground",
    description: "Multi-dialect SQL editor for ad-hoc checks before transfer.",
    readTime: "5 min",
    icon: "code",
    sections: [
      {
        id: "dialects",
        title: "Supported dialects",
        body: "Switch between PostgreSQL, MySQL, Snowflake, BigQuery, and generic SQL. Syntax highlighting adapts to the selected dialect.",
        figure: {
          src: "/docs/screenshots/app-query.png",
          alt: "Query Playground editor with SQL SELECT against saved connectors",
          caption: "Query Playground — pick a saved connector, write read-only SQL, and export results before you transfer.",
        },
      },
      {
        id: "tips",
        title: "Tips",
        body: "Use Query Playground to validate source data shape before mapping. Format JSON results, clear editor, and use snippet chips for common patterns.",
      },
      {
        id: "transfer-bridge",
        title: "Bridge to Transfer Studio",
        body: "Validated queries can inform column filters and preflight sample checks. Copy result schemas into Transfer Studio mapping notes for team review.",
      },
    ],
  },
  "help-job-theater": {
    id: "help-job-theater",
    slug: "job-theater",
    category: "Operations",
    title: "Job Theater & reconciliation",
    description: "Monitor runs from queue to checksum proof.",
    readTime: "7 min",
    icon: "jobs",
    sections: [
      {
        id: "stages",
        title: "Job stages",
        body: "Jobs progress through queued, preflight, writing, reconciling, and complete. Failed gates stop before write.",
        steps: ["Queued", "Preflight", "Writing", "Reconciling", "Complete"],
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater detail with timeline: queued, preflight, extract, load, reconcile, completed",
          caption: "Job stages in Theater — queue → preflight → extract → load → reconcile → complete, with checksum proof.",
        },
      },
      {
        id: "proof",
        title: "Checksum proof",
        body: "Post-load reconciliation compares row counts and content hashes. Finance and analytics teams can export proof reports.",
      },
      {
        id: "quarantine-ui",
        title: "Quarantine inspection",
        body: "Open any job in Job Theater to inspect quarantined rows — column name, offending value, validation rule, and timestamp. Reprocess after fixing mappings or export for offline review.",
      },
    ],
  },
  "help-enterprise": {
    id: "help-enterprise",
    slug: "enterprise",
    category: "Enterprise",
    title: "Enterprise setup",
    description: "SSO, RBAC, tenants, BYOK, and audit for regulated teams.",
    readTime: "9 min",
    icon: "shield",
    sections: [
      {
        id: "sso",
        title: "SSO & RBAC",
        body: "SAML and OIDC integrate with Okta, Azure AD, and Google Workspace. Roles control connector access, transfer execution, and admin settings.",
        figure: {
          src: "/docs/screenshots/app-connectors.png",
          alt: "Connectors scoped inside an enterprise workspace",
          caption: "Workspace-scoped connectors with encrypted credentials — RBAC gates who can test and run.",
        },
      },
      {
        id: "audit",
        title: "Audit trails",
        body: "Every job, mapping decision, quarantine row, and MCP call is logged immutably for SOC 2, GDPR, and HIPAA review.",
      },
      {
        id: "residency",
        title: "Data residency",
        body: "Enterprise tenants pin workspace metadata and job history to a chosen region. Transfer execution can run in customer VPC or air-gapped environments with the same Transfer Studio UI.",
      },
    ],
  },
  "help-api": {
    id: "help-api",
    slug: "api-reference",
    category: "Enterprise",
    title: "API reference",
    description: "REST endpoints for connectors, transfers, jobs, and MCP.",
    readTime: "12 min",
    icon: "code",
    sections: [
      {
        id: "auth",
        title: "Authentication",
        body: "Bearer tokens scoped to workspace. Enterprise uses SSO-backed service accounts.",
        code: 'curl -H "Authorization: Bearer $TOKEN" https://api.dataflow.io/v1/connectors',
      },
      {
        id: "endpoints",
        title: "Core endpoints",
        body: "Catalog, preflight, run, and job status endpoints mirror Transfer Studio behavior.",
        steps: [
          "GET /v1/connectors — list with transfer-ready status",
          "POST /v1/transfers/preflight — run eight gates",
          "POST /v1/transfers/run — execute governed load",
          "GET /v1/jobs/{id} — status, quarantine, reconciliation",
        ],
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater for API-started transfers",
          caption: "API runs surface in Job Theater with the same stage timeline and checksum proof.",
        },
      },
      {
        id: "webhooks",
        title: "Webhooks",
        body: "Subscribe to job.completed, job.failed, and pipeline.quarantine_threshold events. Payloads include job ID, gate results, and reconciliation summary — no row payloads unless explicitly configured.",
        code: 'POST /v1/webhooks\n{ "url": "https://your.app/hooks/dataflow", "events": ["job.completed"] }',
      },
    ],
  },
  "help-faq": {
    id: "help-faq",
    slug: "faq",
    category: "Enterprise",
    title: "Frequently asked questions",
    description: "Common questions from data engineers and platform leads.",
    readTime: "6 min",
    icon: "book",
    sections: [
      {
        id: "q1",
        title: "What is quarantine?",
        body: "Rows that fail validation during load are isolated with the column, value, and reason — never silently dropped.",
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater showing quarantine and reconcile evidence",
          caption: "Quarantine and proof live on the job — open Theater to inspect failed rows.",
        },
      },
      {
        id: "q2",
        title: "Do I need the API online?",
        body: "File-to-file demo transfers work locally. Connectors and Job Theater need the API for live runs.",
      },
      {
        id: "q3",
        title: "Can I self-host?",
        body:
          "Yes. Run the same Transfer Studio engine in your VPC with Docker Compose (or your orchestrator). " +
          "Air-gapped installs load offline images and use your Postgres/object store — no DataFlow SaaS egress required. " +
          "SaaS multi-tenant and customer-VPC self-host are both supported; AWS Private Link is not first-class yet (use VPN/peering you control).",
      },
      {
        id: "q3b",
        title: "Will data be lost moving over the internet?",
        body:
          "Transfers use connector TLS (database SSL, HTTPS object stores, rediss). " +
          "Bytes pass through the DataFlow worker (source → worker → destination) with preflight gates, quarantine instead of silent drops, and post-load checksum reconciliation. " +
          "Empty string and SQL NULL stay distinct. We do not claim zero-copy or bank-attested Private Link — ask for the Security posture report in Workspace settings.",
      },
      {
        id: "q4",
        title: "How is DataFlow different from ETL scripts?",
        body: "Preflight gates and post-load reconciliation prove every transfer before and after write — with audit trails and quarantine built in.",
      },
    ],
  },
};

export function getHelpDoc(id: HelpDocId): HelpDocArticle {
  return ARTICLES[id];
}

/** Full-text search across help articles for the docs portal. */
export function searchHelpDocs(query: string): HelpDocArticle[] {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  return Object.values(ARTICLES).filter(
    (a) =>
      a.title.toLowerCase().includes(q) ||
      a.description.toLowerCase().includes(q) ||
      a.category.toLowerCase().includes(q) ||
      a.sections.some(
        (s) =>
          s.title.toLowerCase().includes(q) ||
          s.body.toLowerCase().includes(q) ||
          s.steps?.some((step) => step.toLowerCase().includes(q)),
      ),
  );
}

export function listAllHelpDocs(): HelpDocArticle[] {
  return HELP_DOC_IDS.map((id) => ARTICLES[id]);
}

export function helpDocFromSlug(slug: string): HelpDocId | null {
  const entry = Object.values(ARTICLES).find((a) => a.slug === slug);
  return entry?.id ?? null;
}

export function isHelpDocRoute(route: string): route is HelpDocId {
  return (HELP_DOC_IDS as readonly string[]).includes(route);
}

export function hashForHelpDoc(id: HelpDocId): string {
  return `#/help/${ARTICLES[id].slug}`;
}

export function helpDocNeighbors(id: HelpDocId): { prev: HelpDocId | null; next: HelpDocId | null } {
  const idx = HELP_DOC_IDS.indexOf(id);
  return {
    prev: idx > 0 ? HELP_DOC_IDS[idx - 1] : null,
    next: idx < HELP_DOC_IDS.length - 1 ? HELP_DOC_IDS[idx + 1] : null,
  };
}

export const HELP_VIDEO_TUTORIALS = [
  { title: "Transfer Studio in 6 minutes", duration: "6:12", topic: "Getting started" },
  { title: "Preflight gates walkthrough", duration: "4:45", topic: "Transfer Studio" },
  { title: "Semantic mapping deep dive", duration: "8:30", topic: "Mapping" },
  { title: "MCP setup for Cursor", duration: "5:20", topic: "Agents" },
];

export const HELP_PRODUCT_CARDS = [
  {
    title: "Transfer Studio",
    body: "Map, preflight, and prove any→any loads with semantic intelligence.",
    doc: "help-transfer-studio" as HelpDocId,
    icon: "transfer",
  },
  {
    title: "Data Pilot",
    body: "Natural-language triage grounded in your workspace.",
    doc: "help-data-pilot" as HelpDocId,
    icon: "sparkle",
  },
  {
    title: "MCP Server",
    body: "Governed transfers from Cursor, Claude, and VS Code.",
    doc: "help-mcp" as HelpDocId,
    icon: "zap",
  },
];
