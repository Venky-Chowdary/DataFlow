/** Public marketing routes — never require auth. Includes help/* article routes. */

import {
  HELP_DOC_IDS,
  getHelpDoc,
  hashForHelpDoc,
  helpDocFromSlug,
  isHelpDocRoute,
  type HelpDocId,
} from "./helpDocs";

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
      "Move any data anywhere with AI semantic mapping, 8 preflight gates, quarantine, and checksum proof. Databases, files, warehouses, and APIs.",
    keywords:
      "Datawrap, data transfer platform, database migration, ETL, semantic mapping, preflight gates, PostgreSQL Snowflake",
    canonicalPath: "#/",
  },
  pricing: {
    title: "Pricing",
    description: "Plans for teams moving data with Transfer Studio, pipelines, and MCP — Starter, Team, and Enterprise.",
    keywords: "Datawrap pricing, ETL pricing, data migration plans, enterprise data transfer cost",
    canonicalPath: "#/pricing",
  },
  enterprise: {
    title: "Enterprise",
    description: "SSO, RBAC, audit trails, tenant isolation, and dedicated support for governed Datawrap deployments.",
    keywords: "Datawrap enterprise, SSO ETL, RBAC data platform, tenant isolation, audit trail",
    canonicalPath: "#/enterprise",
  },
  customers: {
    title: "Customers",
    description: "Load Snowflake, BigQuery, and your lake with mapping, preflight, quarantine, and a checksum finance can archive.",
    keywords: "Datawrap customers, data migration proof, warehouse loading, schema drift testing",
    canonicalPath: "#/customers",
  },
  contact: {
    title: "Contact sales",
    description: "Talk to Datawrap about enterprise migrations, governed sync, and dedicated support.",
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
    description: "Encryption, isolation, residency, and governance controls in Datawrap.",
    keywords: "Datawrap security, Fernet encryption, workspace isolation, SSO SAML",
    canonicalPath: "#/security",
  },
  help: {
    title: "Docs & help",
    description: "Guides for Transfer Studio, connectors, preflight, pipelines, and MCP.",
    keywords: "Datawrap docs, Transfer Studio guide, preflight gates documentation",
    canonicalPath: "#/help",
  },
  "product-transfer": {
    title: "Transfer Studio",
    description: "Map, preflight, and prove any-to-any data loads with semantic column mapping.",
    keywords: "Transfer Studio, column mapping, data transfer wizard, preflight validation",
    canonicalPath: "#/product/transfer",
  },
  "product-pilot": {
    title: "Datawrap Pilot",
    description: "Natural-language triage for transfers, jobs, schemas, and connectors — Confirm before anything moves.",
    keywords: "Datawrap Pilot, AI data agent, natural language ETL, schema inspection chat",
    canonicalPath: "#/product/pilot",
  },
  "product-mcp": {
    title: "MCP Server",
    description: "Governed transfers from Cursor, Claude, and VS Code via Model Context Protocol.",
    keywords: "Datawrap MCP, Cursor MCP server, Claude data tools, IDE ETL",
    canonicalPath: "#/product/mcp",
  },
  "product-jobs": {
    title: "Job Theater",
    description: "Live batch progress, phases, quarantine, and proof reports for every transfer.",
    keywords: "Job Theater, transfer progress, quarantine report, data reconciliation",
    canonicalPath: "#/product/jobs",
  },
  "product-pipelines": {
    title: "Pipelines",
    description: "Scheduled sync with watermarks, upsert modes, and governed preflight.",
    keywords: "Datawrap pipelines, scheduled ETL, incremental sync, CDC pipelines",
    canonicalPath: "#/product/pipelines",
  },
  "product-query": {
    title: "Query Playground",
    description: "Ad-hoc SQL and document queries against live connectors with export paths.",
    keywords: "Query Playground, SQL against connectors, MongoDB query, data export",
    canonicalPath: "#/product/query",
  },
  integrations: {
    title: "Connectors",
    description: "Snowflake, BigQuery, S3, ADLS, GCS, PostgreSQL, and the rest of your stack — one catalog.",
    keywords: "Datawrap connectors, PostgreSQL MySQL MongoDB Snowflake BigQuery S3 Iceberg",
    canonicalPath: "#/integrations",
  },
  "solution-migrations": {
    title: "Migrations",
    description: "Cross-schema migrations with semantic mapping and checksum proof.",
    keywords: "database migration, schema migration, PostgreSQL to Snowflake migration",
    canonicalPath: "#/solutions/migrations",
  },
  "solution-warehouse": {
    title: "Warehouse loading",
    description: "Load Snowflake, BigQuery, and Redshift with reconciliation.",
    keywords: "warehouse loading, Snowflake load, BigQuery ETL, Redshift sync",
    canonicalPath: "#/solutions/warehouse",
  },
  "solution-sync": {
    title: "Recurring sync",
    description: "Incremental pipelines with quarantine and upsert modes.",
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
