/** SEO + browser tab metadata for every app screen / category. */

import { CATALOG_TOTAL, PROVEN_DRIVERS, cdcSourcesPhrase } from "./provenEvidence";
import { PUBLIC_PAGE_META } from "./publicNavigation";
import type { Screen } from "./types";

export interface PageMeta {
  title: string;
  description: string;
  keywords: string;
  robots?: string;
  ogType?: "website" | "article";
  /** Hash or path for canonical / og:url (e.g. "#/pricing"). */
  canonicalPath?: string;
}

const BASE_TITLE = "Datawrap";
/** The home page's own description: the site states it once, everything else reuses it. */
const DEFAULT_DESCRIPTION = PUBLIC_PAGE_META.home.description;
const DEFAULT_KEYWORDS =
  "data transfer, data migration, ETL, database migration, CSV to PostgreSQL, Snowflake migration, semantic mapping, preflight validation, data pipeline, Datawrap";

export const PAGE_META: Record<Screen | "login", PageMeta> = {
  landing: {
    title: PUBLIC_PAGE_META.home.title,
    description: PUBLIC_PAGE_META.home.description,
    keywords:
      "data transfer platform, database migration tool, file to database, PostgreSQL to Snowflake, data sync, ETL platform, semantic mapping",
    ogType: "website",
  },
  login: {
    title: "Sign In",
    description: "Sign in to Datawrap to manage connectors, run transfers, and monitor pipelines.",
    keywords: "Datawrap login, data platform sign in, enterprise data transfer",
    robots: "noindex, nofollow",
  },
  dashboard: {
    title: "Overview",
    description:
      "Platform overview — live topology, connector health, recent jobs, and pipeline status at a glance.",
    keywords: "data platform dashboard, migration overview, connector health, job monitoring",
    robots: "noindex",
  },
  transfer: {
    title: "Transfer Studio",
    description:
      "One-click data transfer wizard — connect source and destination, map columns semantically, run preflight gates, and reconcile results.",
    keywords: "data transfer wizard, column mapping, preflight gates, CSV upload, database write",
    robots: "noindex",
  },
  query: {
    title: "Query Playground",
    description:
      "Run safe, read-only SQL and MongoDB queries against saved connectors and export results to CSV, JSON, or Excel.",
    keywords: "SQL query playground, MongoDB query, ad-hoc query, data export, connector query",
    robots: "noindex",
  },
  pilot: {
    title: "Datawrap Pilot",
    description:
      "Copilot on Datawrap's own local engine — inspect schemas, run transfers, and troubleshoot pipelines, with Confirm before anything moves.",
    keywords: "AI data agent, natural language ETL, Datawrap Pilot, schema inspection",
    robots: "noindex",
  },
  connectors: {
    title: "Connectors",
    description:
      "Manage sources and destinations — PostgreSQL, MySQL, MongoDB, SQL Server, Oracle, Snowflake, BigQuery, S3, Iceberg, and the rest of your stack.",
    keywords: "data connectors, PostgreSQL connector, Snowflake connector, S3 connector, database connection",
    robots: "noindex",
  },
  schedules: {
    title: "Schedules",
    description:
      "Schedule recurring data syncs — hourly, daily, or weekly pipelines with monitoring and failure alerts.",
    keywords: "scheduled data sync, recurring ETL, pipeline scheduler, cron data transfer",
    robots: "noindex",
  },
  transforms: {
    title: "Transformations",
    description:
      "Post-load SQL models that run at the destination after a transfer lands — views, tables, incremental rollups, and data tests.",
    keywords: "dbt alternative, post-load SQL, data transformation, warehouse models, data tests",
    robots: "noindex",
  },
  "transform-library": {
    title: "Transform Library",
    description:
      "Named, versioned pre-load transforms — built once, bound from the Transform step of any transfer with pinned or latest versions and parameter overrides.",
    keywords: "transform library, reusable transforms, data cleansing, PII masking, versioned recipes",
    robots: "noindex",
  },
  jobs: {
    title: "Job Theater",
    description:
      "Live transfer progress — batch tracking, phase timeline, error triage, and reconciliation reports.",
    keywords: "data migration jobs, transfer progress, batch monitoring, reconciliation report",
    robots: "noindex",
  },
  mcp: {
    title: "MCP Server",
    description:
      "Model Context Protocol server for Cursor, Claude, and VS Code — same Datawrap tools inside your IDE.",
    keywords: "MCP server, Cursor integration, Claude data tools, VS Code data agent",
    robots: "noindex",
  },
  settings: {
    title: "Settings",
    description: "Security, team access, and workspace configuration for your Datawrap deployment.",
    keywords: "Datawrap settings, team security, workspace configuration",
    robots: "noindex",
  },
  docs: {
    title: "Documentation",
    description:
      "How Datawrap moves, maps, and validates data — architecture, preflight rules, connector coverage, and security.",
    keywords:
      "Datawrap documentation, data transfer architecture, ETL documentation, preflight gates, data migration guide",
    robots: "noindex",
  },
  benchmarks: {
    title: "Benchmarks",
    description:
      "Reproducible scale proofs — Datawrap throughput, memory, and correctness on named routes and fixtures you can re-run.",
    keywords:
      "Datawrap benchmark, ETL throughput, data transfer performance, rows per second",
    robots: "noindex",
  },
  exit: {
    title: "Exit readiness",
    description:
      "Declared systems, rehearsed exits measured against their windows, completeness evidence per table, and signed Exit Evidence Packs.",
    keywords: "exit plan, exit rehearsal, data portability, EU Data Act switching, DORA exit strategy, evidence pack",
    robots: "noindex",
  },
  contracts: {
    title: "Data contracts",
    description:
      "Signed schema agreements that gate transfers, enforce quality rules, and detect breaking drift before cutover.",
    keywords: "Datawrap data contracts, schema agreement, schema drift, preflight gates",
    robots: "noindex",
  },
};

export function resolveSiteUrl(): string {
  const env = import.meta.env.VITE_SITE_URL as string | undefined;
  if (env?.trim()) return env.replace(/\/$/, "");
  if (typeof window !== "undefined" && window.location.origin) {
    return window.location.origin;
  }
  return "https://www.datawrap.io";
}

export function formatDocumentTitle(pageTitle: string): string {
  if (pageTitle === BASE_TITLE || pageTitle.startsWith(`${BASE_TITLE} —`)) return pageTitle;
  return `${BASE_TITLE} — ${pageTitle}`;
}

export function metaForScreen(screen: Screen): PageMeta {
  return PAGE_META[screen] ?? PAGE_META.landing;
}

export function metaForLogin(): PageMeta {
  return PAGE_META.login;
}

export const DEFAULT_PAGE_META: PageMeta = {
  title: BASE_TITLE,
  description: DEFAULT_DESCRIPTION,
  keywords: DEFAULT_KEYWORDS,
  ogType: "website",
};

/**
 * What the structured data (JSON-LD) lists as features. Each line is a claim a
 * public page already makes: nine core gates (Transfer, helpDocs), the
 * read-back (home), CDC sources from CI evidence, quarantine with replay,
 * typed watermarks (Schedules), and the Enterprise controls. `index.html`
 * carries the same list for crawlers that do not run scripts; a test keeps
 * the two equal.
 */
export const SOFTWARE_FEATURES: readonly string[] = [
  "Database migration",
  "File to database transfer",
  "Semantic column mapping",
  "Nine core preflight gates before any write",
  "Read-back of the destination after the write",
  `Native CDC for ${cdcSourcesPhrase()}`,
  "Scheduled pipelines with typed watermarks",
  "Quarantine with replay",
  "SSO, SCIM, and tamper-evident audit log",
];

/**
 * The share card. The query string changes when the card is regenerated, so
 * Slack, LinkedIn and X fetch the new card instead of their cached copy.
 * `index.html` carries the same URL for crawlers that do not run scripts.
 */
export const OG_IMAGE_PATH = "/og-image.png?v=every-row-1";
export const OG_IMAGE_SIZE = { width: 1200, height: 630 } as const;

/**
 * Copy for the share card (`og-image.png`) and the social covers, which
 * `scripts/export-brand-icons.mjs` and `scripts/export-social-brand.mjs`
 * render. Every line is one the site already says: the home hero, the home
 * statement's label, the Transfer page's gates, and the connector count from
 * CI evidence. Tests hold each line to its source.
 */
export const SHARE_CARD = {
  headline: "Every row, accounted for.",
  statementLabel: "Example statement for one run",
  facts: [
    "Nine core gates before any write.",
    `${PROVEN_DRIVERS} of ${CATALOG_TOTAL} drivers proven in CI.`,
  ],
  /** og:image:alt — says what the picture shows. */
  alt: "Datawrap. Every row, accounted for. An example run: rows read equal rows landed, plus held, plus skipped.",
} as const;

export { BASE_TITLE, DEFAULT_DESCRIPTION, DEFAULT_KEYWORDS };
