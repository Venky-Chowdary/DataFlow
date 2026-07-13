/** SEO + browser tab metadata for every app screen / category. */

import type { Screen } from "./types";

export interface PageMeta {
  title: string;
  description: string;
  keywords: string;
  robots?: string;
  ogType?: "website" | "article";
}

const BASE_TITLE = "DataFlow";
const DEFAULT_DESCRIPTION =
  "Universal data transfer platform — migrate databases, sync files, and move data between 650+ systems with AI semantic mapping and 8 preflight gates.";
const DEFAULT_KEYWORDS =
  "data transfer, data migration, ETL, database migration, CSV to PostgreSQL, Snowflake migration, semantic mapping, preflight validation, data pipeline, DataFlow";

export const PAGE_META: Record<Screen | "login", PageMeta> = {
  landing: {
    title: "Universal Data Transfer Platform",
    description:
      "Move data between any source and destination — databases, files, warehouses, and APIs. AI column mapping, 8 preflight gates, and live reconciliation.",
    keywords:
      "data transfer platform, database migration tool, file to database, PostgreSQL to Snowflake, data sync, ETL platform, semantic mapping",
    ogType: "website",
  },
  login: {
    title: "Sign In",
    description: "Sign in to DataFlow to manage connectors, run transfers, and monitor pipelines.",
    keywords: "DataFlow login, data platform sign in, enterprise data transfer",
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
  pilot: {
    title: "Data Pilot",
    description:
      "AI agent for natural-language data operations — inspect schemas, run transfers, and troubleshoot pipelines.",
    keywords: "AI data agent, natural language ETL, Data Pilot, schema inspection",
    robots: "noindex",
  },
  connectors: {
    title: "Connectors",
    description:
      "Manage sources and destinations — PostgreSQL, MySQL, MongoDB, Snowflake, BigQuery, S3, Redis, Elasticsearch, and 650+ catalog systems.",
    keywords: "data connectors, PostgreSQL connector, Snowflake connector, S3 connector, database connection",
    robots: "noindex",
  },
  schedules: {
    title: "Pipelines",
    description:
      "Schedule recurring data syncs — hourly, daily, or weekly pipelines with monitoring and failure alerts.",
    keywords: "scheduled data sync, recurring ETL, pipeline scheduler, cron data transfer",
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
      "Model Context Protocol server for Cursor, Claude, and VS Code — same DataFlow tools inside your IDE.",
    keywords: "MCP server, Cursor integration, Claude data tools, VS Code data agent",
    robots: "noindex",
  },
  settings: {
    title: "Settings",
    description: "Security, team access, and workspace configuration for your DataFlow deployment.",
    keywords: "DataFlow settings, team security, workspace configuration",
    robots: "noindex",
  },
  docs: {
    title: "Documentation",
    description:
      "How DataFlow moves, maps, and validates any data — architecture, preflight rules, connector coverage, and security.",
    keywords:
      "DataFlow documentation, data transfer architecture, ETL documentation, preflight gates, data migration guide",
    robots: "noindex",
  },
};

export function resolveSiteUrl(): string {
  const env = import.meta.env.VITE_SITE_URL as string | undefined;
  if (env?.trim()) return env.replace(/\/$/, "");
  if (typeof window !== "undefined" && window.location.origin) {
    return window.location.origin;
  }
  return "https://dataflow.app";
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

export { BASE_TITLE, DEFAULT_DESCRIPTION, DEFAULT_KEYWORDS };
