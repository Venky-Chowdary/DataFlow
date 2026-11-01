/** Public marketing routes — never require auth. Includes help/* article routes. */

import {
  HELP_DOC_IDS,
  getHelpDoc,
  hashForHelpDoc,
  helpDocFromSlug,
  isHelpDocRoute,
  type HelpDocId,
} from "./helpDocs";
import { TRANSFER_READY_DRIVERS } from "./provenEvidence";

export type PublicRoute =
  | "home"
  | "pricing"
  | "enterprise"
  | "customers"
  | "contact"
  | "privacy"
  | "terms"
  | "security"
  | "help"
  | "product-transfer"
  | "product-pilot"
  | "product-mcp"
  | "product-jobs"
  | "product-pipelines"
  | "product-query"
  | "integrations"
  | "demos"
  | "solution-migrations"
  | "solution-warehouse"
  | "solution-sync"
  | HelpDocId;

const BASE_HASH_TO_ROUTE: Record<string, Exclude<PublicRoute, HelpDocId>> = {
  "": "home",
  home: "home",
  landing: "home",
  pricing: "pricing",
  enterprise: "enterprise",
  customers: "customers",
  contact: "contact",
  privacy: "privacy",
  terms: "terms",
  security: "security",
  help: "help",
  docs: "help",
  guide: "help",
  documentation: "help",
  integrations: "integrations",
  connectors: "integrations",
  catalog: "integrations",
  demos: "demos",
  videos: "demos",
  demo: "demos",
  watch: "demos",
  "product/transfer": "product-transfer",
  "product/pilot": "product-pilot",
  "product/mcp": "product-mcp",
  "product/jobs": "product-jobs",
  "product/pipelines": "product-pipelines",
  "product/query": "product-query",
  "solutions/migrations": "solution-migrations",
  "solutions/warehouse": "solution-warehouse",
  "solutions/sync": "solution-sync",
};

const BASE_ROUTE_TO_HASH: Record<Exclude<PublicRoute, HelpDocId>, string> = {
  home: "#/",
  pricing: "#/pricing",
  enterprise: "#/enterprise",
  customers: "#/customers",
  contact: "#/contact",
  privacy: "#/privacy",
  terms: "#/terms",
  security: "#/security",
  help: "#/help",
  "product-transfer": "#/product/transfer",
  "product-pilot": "#/product/pilot",
  "product-mcp": "#/product/mcp",
  "product-jobs": "#/product/jobs",
  "product-pipelines": "#/product/pipelines",
  "product-query": "#/product/query",
  integrations: "#/integrations",
  demos: "#/demos",
  "solution-migrations": "#/solutions/migrations",
  "solution-warehouse": "#/solutions/warehouse",
  "solution-sync": "#/solutions/sync",
};

export type PublicPageMeta = {
  title: string;
  description: string;
  keywords?: string;
  canonicalPath?: string;
};

export const PUBLIC_PAGE_META: Record<PublicRoute, PublicPageMeta> = {
  home: {
    title: "Universal Data Transfer Platform",
    description:
      "Move data between databases, warehouses, files, streams, and APIs. Semantic mapping, 9 preflight gates, native CDC, and every load verified value by value.",
    keywords:
      "Datawrap, data transfer platform, database migration, ETL, CDC, semantic mapping, preflight gates, data verification, PostgreSQL Snowflake SQL Server",
    canonicalPath: "#/",
  },
  pricing: {
    title: "Pricing",
    description: "Starter, Team, and Enterprise plans — verification, preflight, and quarantine in every plan; CDC, Pilot, SSO, and SCIM as you scale.",
    keywords: "Datawrap pricing, ETL pricing, data migration plans, enterprise data transfer cost",
    canonicalPath: "#/pricing",
  },
  enterprise: {
    title: "Enterprise",
    description: "SSO (OIDC / SAML), SCIM, RBAC with capped API keys, workspace isolation, BYOK, and a tamper-evident audit chain for governed data movement.",
    keywords: "Datawrap enterprise, SSO SAML OIDC, SCIM provisioning, RBAC data platform, workspace isolation, BYOK, audit trail",
    canonicalPath: "#/enterprise",
  },
  customers: {
    title: "Customers",
    description: "Measured on live engines: schema drift, identity and key carry, retry safety, and destination read-back — the proof you get on your own data.",
    keywords: "Datawrap customers, data migration proof, warehouse loading, schema drift testing",
    canonicalPath: "#/customers",
  },
  contact: {
    title: "Contact sales",
    description: "Talk to a Datawrap solutions engineer about migrations, CDC, warehouse loading, and a scoped pilot on your own stack.",
    keywords: "contact Datawrap, data migration sales, enterprise ETL demo",
    canonicalPath: "#/contact",
  },
  privacy: {
    title: "Privacy",
    description: "How Datawrap handles workspace data, encrypted credentials, retention, subprocessors, and your rights.",
    keywords: "Datawrap privacy policy, data processor, credential encryption",
    canonicalPath: "#/privacy",
  },
  terms: {
    title: "Terms of service",
    description: "Terms of service for the Datawrap platform — accounts, customer data, acceptable use, and enterprise agreements.",
    keywords: "Datawrap terms of service, acceptable use",
    canonicalPath: "#/terms",
  },
  security: {
    title: "Security",
    description: "Workspace isolation, encrypted credentials with BYOK, SSO and SCIM, signed webhooks, and a hash-chained audit log in Datawrap.",
    keywords: "Datawrap security, BYOK, workspace isolation, SSO SAML OIDC, SCIM, signed webhooks, audit log",
    canonicalPath: "#/security",
  },
  help: {
    title: "Docs & help",
    description: "Guides for Transfer Studio, connectors, preflight, verification, pipelines, CDC, Pilot, and MCP.",
    keywords: "Datawrap docs, Transfer Studio guide, preflight gates documentation",
    canonicalPath: "#/help",
  },
  "product-transfer": {
    title: "Transfer Studio",
    description: "Map columns by meaning, clear nine preflight gates, and verify every load against the destination, value by value.",
    keywords: "Transfer Studio, column mapping, data transfer wizard, preflight validation",
    canonicalPath: "#/product/transfer",
  },
  "product-pilot": {
    title: "Datawrap Pilot",
    description: "A copilot on Datawrap's own local engine — no cloud AI key needed. Triage transfers, jobs, and schemas; Confirm before anything moves.",
    keywords: "Datawrap Pilot, AI data agent, natural language ETL, schema inspection chat",
    canonicalPath: "#/product/pilot",
  },
  "product-mcp": {
    title: "MCP Server",
    description: "Governed transfers from Cursor, Claude, and VS Code via MCP — RBAC, capped API keys, and secret references on every call.",
    keywords: "Datawrap MCP, Cursor MCP server, Claude data tools, IDE ETL",
    canonicalPath: "#/product/mcp",
  },
  "product-jobs": {
    title: "Job Theater",
    description: "Live phases, quarantine with replay, and Gate-8 verification reports for every transfer.",
    keywords: "Job Theater, transfer progress, quarantine report, data reconciliation",
    canonicalPath: "#/product/jobs",
  },
  "product-pipelines": {
    title: "Pipelines",
    description: "Schedules with typed watermarks, native CDC, mirror with soft delete, and drift review — every run gated and verified.",
    keywords: "Datawrap pipelines, scheduled ETL, incremental sync, CDC pipelines",
    canonicalPath: "#/product/pipelines",
  },
  "product-query": {
    title: "Query Playground",
    description: "Ad-hoc SQL and document queries against saved connectors with export paths.",
    keywords: "Query Playground, SQL against connectors, MongoDB query, data export",
    canonicalPath: "#/product/query",
  },
  integrations: {
    title: "Connectors",
    description: `${TRANSFER_READY_DRIVERS} transfer-ready drivers: PostgreSQL, SQL Server, Oracle, MySQL, MongoDB, Snowflake, BigQuery, S3, Kafka, pgvector, and more.`,
    keywords: "Datawrap connectors, PostgreSQL MySQL SQL Server Oracle MongoDB Snowflake BigQuery S3 Iceberg Kafka pgvector",
    canonicalPath: "#/integrations",
  },
  demos: {
    title: "Video demos",
    description:
      "Watch real Datawrap transfers prove themselves: quarantine and replay, schema drift review, native CDC, files and stored procedures, and the checksum match at the end.",
    keywords: "Datawrap demo, data migration video, ETL demo, CDC demo, quarantine replay proof",
    canonicalPath: "#/demos",
  },
  "solution-migrations": {
    title: "Migrations",
    description: "Cross-schema migrations with semantic mapping, identity and key carry, Unicode safety, and a signed proof pack at cutover.",
    keywords: "database migration, schema migration, PostgreSQL to Snowflake migration",
    canonicalPath: "#/solutions/migrations",
  },
  "solution-warehouse": {
    title: "Warehouse loading",
    description: "Load Snowflake, BigQuery, Iceberg, and cloud storage with MERGE upserts and read-back verification.",
    keywords: "warehouse loading, Snowflake load, BigQuery ETL, Iceberg, S3 ADLS GCS",
    canonicalPath: "#/solutions/warehouse",
  },
  "solution-sync": {
    title: "Recurring sync",
    description: "CDC and incremental pipelines with typed watermarks, quarantine, and verification on every run.",
    keywords: "recurring data sync, incremental ETL, upsert CDC, scheduled sync",
    canonicalPath: "#/solutions/sync",
  },
  ...Object.fromEntries(
    HELP_DOC_IDS.map((id) => {
      const doc = getHelpDoc(id);
      return [
        id,
        {
          title: doc.title,
          description: doc.description,
          keywords: `Datawrap help, ${doc.title}, data transfer documentation`,
          canonicalPath: hashForHelpDoc(id),
        },
      ] as const;
    }),
  ),
} as Record<PublicRoute, PublicPageMeta>;

export function publicRouteFromHash(hash: string): PublicRoute | null {
  const raw = hash.replace(/^#\/?/, "").split("?")[0].trim().toLowerCase();
  if (!raw) return "home";

  // Doc articles: #/help/<slug>  (must run before the bare "help" map)
  const helpMatch = raw.match(/^help\/([a-z0-9-]+)$/);
  if (helpMatch) {
    return helpDocFromSlug(helpMatch[1]);
  }

  const appOnly = new Set([
    "dashboard",
    "transfer",
    "pilot",
    "schedules",
    "jobs",
    "mcp",
    "settings",
    "query",
    "benchmarks",
    "login",
  ]);
  if (appOnly.has(raw)) return null;

  if (raw in BASE_HASH_TO_ROUTE) return BASE_HASH_TO_ROUTE[raw];
  if (isHelpDocRoute(raw)) return raw;
  return null;
}

export function hashForPublicRoute(route: PublicRoute): string {
  if (isHelpDocRoute(route)) return hashForHelpDoc(route);
  return BASE_ROUTE_TO_HASH[route];
}

export function readPublicHash(): PublicRoute | null {
  if (typeof window === "undefined") return null;
  return publicRouteFromHash(window.location.hash);
}

export function writePublicHash(route: PublicRoute, replace = false) {
  if (typeof window === "undefined") return;
  const next = hashForPublicRoute(route);
  if (window.location.hash === next || (route === "home" && (!window.location.hash || window.location.hash === "#"))) {
    return;
  }
  if (replace) {
    window.history.replaceState(null, "", next === "#/" ? window.location.pathname : next);
  } else if (route === "home") {
    window.history.pushState(null, "", window.location.pathname);
  } else {
    window.location.hash = next;
  }
}

export function isPublicHash(hash: string): boolean {
  return publicRouteFromHash(hash) !== null;
}
