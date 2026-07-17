/** Public marketing routes — never require auth. */

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
  | "integrations"
  | "solution-migrations"
  | "solution-warehouse"
  | "solution-sync";

const HASH_TO_ROUTE: Record<string, PublicRoute> = {
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
  integrations: "integrations",
  connectors: "integrations",
  catalog: "integrations",
  "product/transfer": "product-transfer",
  "product/pilot": "product-pilot",
  "product/mcp": "product-mcp",
  "solutions/migrations": "solution-migrations",
  "solutions/warehouse": "solution-warehouse",
  "solutions/sync": "solution-sync",
};

const ROUTE_TO_HASH: Record<PublicRoute, string> = {
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
  integrations: "#/integrations",
  "solution-migrations": "#/solutions/migrations",
  "solution-warehouse": "#/solutions/warehouse",
  "solution-sync": "#/solutions/sync",
};

export const PUBLIC_PAGE_META: Record<PublicRoute, { title: string; description: string }> = {
  home: {
    title: "Universal Data Transfer Platform",
    description: "Move any data anywhere with semantic mapping, preflight gates, and proof.",
  },
  pricing: {
    title: "Pricing",
    description: "Plans for teams moving data with Transfer Studio, pipelines, and MCP.",
  },
  enterprise: {
    title: "Enterprise",
    description: "SSO, RBAC, audit trails, tenant isolation, and dedicated support.",
  },
  customers: {
    title: "Customers",
    description: "How data teams use DataFlow for migrations, sync, and warehouse loads.",
  },
  contact: {
    title: "Contact sales",
    description: "Talk to DataFlow about enterprise migrations and governed sync.",
  },
  privacy: {
    title: "Privacy",
    description: "How DataFlow handles workspace data, credentials, and audit logs.",
  },
  terms: {
    title: "Terms of service",
    description: "Terms governing use of the DataFlow platform.",
  },
  security: {
    title: "Security",
    description: "Encryption, isolation, residency, and governance controls.",
  },
  help: {
    title: "Docs & help",
    description: "Guides for Transfer Studio, connectors, preflight, and MCP.",
  },
  "product-transfer": {
    title: "Transfer Studio",
    description: "Map, preflight, and prove any-to-any data loads.",
  },
  "product-pilot": {
    title: "Data Pilot",
    description: "Natural-language triage for transfers and jobs.",
  },
  "product-mcp": {
    title: "MCP Server",
    description: "Governed transfers from Cursor, Claude, and VS Code.",
  },
  integrations: {
    title: "Connectors",
    description: "Native drivers and SQLAlchemy generics with honest transfer-ready labels.",
  },
  "solution-migrations": {
    title: "Migrations",
    description: "Cross-schema migrations with semantic mapping and checksum proof.",
  },
  "solution-warehouse": {
    title: "Warehouse loading",
    description: "Load Snowflake, BigQuery, and Redshift with reconciliation.",
  },
  "solution-sync": {
    title: "Recurring sync",
    description: "Incremental pipelines with quarantine and upsert modes.",
  },
};

export function publicRouteFromHash(hash: string): PublicRoute | null {
  const raw = hash.replace(/^#\/?/, "").split("?")[0].trim().toLowerCase();
  // App screens handled elsewhere — not public marketing
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
  if (raw in HASH_TO_ROUTE) return HASH_TO_ROUTE[raw];
  return null;
}

export function hashForPublicRoute(route: PublicRoute): string {
  return ROUTE_TO_HASH[route];
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
