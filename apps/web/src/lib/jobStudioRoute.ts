/**
 * Jobs → Studio: plan how to reopen a job's route (Source + Destination) from
 * its stored transfer_request. Pure, so every source/destination shape is
 * covered by tests rather than by one clicked-through route.
 *
 * Job documents come back with secrets masked, so an inline connection (MCP,
 * API) cannot be replayed as-is; it is matched to a saved connector by
 * type + host + port + database instead.
 */
import type { Connector } from "./types";
import { fileExportFormulaProtection } from "./fileExportDestination";

type Endpoint = Record<string, unknown>;

export type JobRouteSource =
  | { kind: "connector"; connectorId: string; connectorType: string; table: string; collection: string }
  | { kind: "journal"; journalId: string; stream: string }
  | { kind: "stored_file"; fileId: string; filename: string }
  | { kind: "missing"; reason: "inline_file" | "unsaved_connection" | "ambiguous_connection"; label: string };

export type JobRouteDestination =
  | {
    kind: "file_export";
    format: string;
    outputPath: string;
    database: string;
    /** The destination wrote CSV/TSV with formula_protection "prefix" (G7-F2). */
    formulaProtection: boolean;
  }
  | {
    kind: "database";
    connectorId: string;
    type: string;
    connectionString: string;
    database: string;
    table: string;
    /** No saved connector and no replayable connection: the operator must pick one. */
    missingLabel: string;
  };

export interface JobStudioRoutePlan {
  syncMode: string;
  source: JobRouteSource;
  destination: JobRouteDestination;
}

const text = (v: unknown): string => (typeof v === "string" ? v : "");

const TYPE_ALIASES: Record<string, string> = {
  postgres: "postgresql",
  pg: "postgresql",
  mssql: "sqlserver",
  sql_server: "sqlserver",
  mongo: "mongodb",
  mariadb: "mysql",
};

function normType(t: string): string {
  const base = t.trim().toLowerCase().split("+")[0];
  return TYPE_ALIASES[base] ?? base;
}

const DEFAULT_PORTS: Record<string, number> = {
  postgresql: 5432,
  mysql: 3306,
  sqlserver: 1433,
  oracle: 1521,
  mongodb: 27017,
};

interface Address {
  type: string;
  host: string;
  port: number;
  database: string;
}

function portOf(v: unknown): number {
  return typeof v === "number" ? v : Number(text(v)) || 0;
}

/**
 * Host/port/database of an endpoint: its fields, then the API's `address`
 * projection (stored connection strings are ciphertext), then its masked URL.
 */
export function endpointAddress(ep: Endpoint): Address {
  const projected = (ep.address && typeof ep.address === "object" ? ep.address : {}) as Endpoint;
  let type = normType(text(ep.format) || text(ep.type) || text(ep.db_type) || text(projected.type));
  let host = text(ep.host) || text(projected.host);
  let port = portOf(ep.port) || portOf(projected.port);
  let database = text(ep.database) || text(projected.database);
  const url = text(ep.connection_string);
  if (url.includes("://")) {
    try {
      // Masked passwords ("****") still parse; only the address is read.
      const u = new URL(url.replace(/^([a-z0-9+.-]+):\/\//i, "http://"));
      const scheme = normType(url.split("://")[0]);
      if (!type) type = scheme;
      if (!host) host = u.hostname;
      if (!port && u.port) port = Number(u.port);
      if (!database) database = decodeURIComponent(u.pathname.replace(/^\//, "").split("/")[0] || "");
    } catch {
      // Not a URL (DSN / key=value): the explicit fields are all we have.
    }
  }
  return { type, host: host.toLowerCase(), port: port || DEFAULT_PORTS[type] || 0, database };
}

function sameAddress(a: Address, c: Connector): boolean {
  const ca = endpointAddress({
    type: c.type,
    host: c.host,
    port: c.port,
    database: c.database,
    connection_string: c.connection_string,
  });
  if (!a.type || a.type !== ca.type) return false;
  if (!a.host || a.host !== ca.host) return false;
  if (a.port && ca.port && a.port !== ca.port) return false;
  return !a.database || !ca.database || a.database === ca.database;
}

/** The saved connector this endpoint ran against, by id, else by address. */
export function matchSavedConnector(
  ep: Endpoint,
  connectors: readonly Connector[],
): { connector?: Connector; ambiguous: boolean } {
  const id = text(ep.connector_id);
  if (id) {
    const byId = connectors.find((c) => c.id === id);
    if (byId) return { connector: byId, ambiguous: false };
  }
  const addr = endpointAddress(ep);
  let hits = connectors.filter((c) => sameAddress(addr, c));
  if (hits.length > 1) {
    const user = text(ep.username);
    const exactDb = hits.filter((c) => (c.database || "") === addr.database);
    if (exactDb.length) hits = exactDb;
    if (hits.length > 1 && user) {
      const byUser = hits.filter((c) => (c.username || "") === user);
      if (byUser.length) hits = byUser;
    }
  }
  if (hits.length === 1) return { connector: hits[0], ambiguous: false };
  return { ambiguous: hits.length > 1 };
}

function addressLabel(ep: Endpoint): string {
  const a = endpointAddress(ep);
  const where = [a.host, a.database].filter(Boolean).join("/");
  return [a.type, where].filter(Boolean).join(" at ") || "the job's connection";
}

/** A URL the browser cannot replay: masked password or vault ciphertext. */
const isMasked = (url: string) => url.includes("****") || /^(enc:|sm:|byok:)/.test(url);

export function planJobStudioRoute(job: unknown, connectors: readonly Connector[]): JobStudioRoutePlan {
  const tr = ((job as { transfer_request?: Endpoint } | null)?.transfer_request ?? {}) as Endpoint;
  const src = (tr.source ?? {}) as Endpoint;
  const dst = (tr.destination ?? {}) as Endpoint;
  const srcExtra = (src.extra ?? {}) as Endpoint;

  let destination: JobRouteDestination;
  const dstKind = text(dst.kind);
  if (dstKind === "file_export" || dstKind === "file" || (text(dst.output_path) && dstKind !== "database")) {
    destination = {
      kind: "file_export",
      format: text(dst.format) || "csv",
      outputPath: text(dst.output_path),
      database: text(dst.database),
      formulaProtection: fileExportFormulaProtection(dst),
    };
  } else {
    const { connector, ambiguous } = matchSavedConnector(dst, connectors);
    const url = text(dst.connection_string);
    const replayable = Boolean(url) && !isMasked(url) && !text(dst.password);
    destination = {
      kind: "database",
      connectorId: connector?.id ?? "",
      type: connector?.type ?? (text(dst.format) || text(dst.type)),
      connectionString: !connector && replayable ? url : "",
      database: text(dst.database) || connector?.database || endpointAddress(dst).database,
      table: text(dst.table) || text(dst.collection),
      missingLabel: connector || replayable
        ? ""
        : `${addressLabel(dst)}${ambiguous ? " (several saved connectors match)" : ""}`,
    };
  }

  let source: JobRouteSource;
  const srcKind = text(src.kind) || "database";
  if (srcKind === "file") {
    if (text(srcExtra.journal_id)) {
      source = { kind: "journal", journalId: text(srcExtra.journal_id), stream: text(srcExtra.journal_stream) };
    } else if (text(srcExtra.file_id)) {
      source = {
        kind: "stored_file",
        fileId: text(srcExtra.file_id),
        filename: text(tr.source_filename) || text(srcExtra.filename) || "",
      };
    } else {
      source = { kind: "missing", reason: "inline_file", label: text(tr.source_filename) || "the job's file" };
    }
  } else {
    const { connector, ambiguous } = matchSavedConnector(src, connectors);
    source = connector
      ? {
        kind: "connector",
        connectorId: connector.id,
        connectorType: connector.type,
        table: text(src.table),
        collection: text(src.collection),
      }
      : { kind: "missing", reason: ambiguous ? "ambiguous_connection" : "unsaved_connection", label: addressLabel(src) };
  }

  return { syncMode: text(tr.sync_mode), source, destination };
}

/** The route a schedule-fired job came from, for reopening it in Studio. */
export interface ScheduleRecoveryRoute {
  scheduleId: string;
  sourceConnectorId: string;
  destConnectorId: string;
  sourceTable?: string;
  destTable?: string;
}

/**
 * A job the scheduler fired names its schedule on its endpoints
 * (``extra.schedule_id``, services/schedule_runner.build_schedule_request) and
 * the saved connectors it ran on. Reopening that job in Studio has to load
 * exactly this route and carry the schedule id, so Validate re-signs the
 * schedule's own contract (the recovery a "content_hash mismatch" asks for).
 *
 * Opening it as a plain job left Studio on whatever route it already held
 * (QA 2026-11-06: a failed schedule run opened an unrelated 1M-row transfer),
 * and a Validate there could never re-sign the paused schedule.
 *
 * ``null`` when the job was not fired by a schedule or does not name both
 * saved connectors; the caller keeps the plain job path.
 */
export function scheduleRecoveryRoute(job: unknown): ScheduleRecoveryRoute | null {
  const rec = (job ?? {}) as Endpoint;
  const tr = (rec.transfer_request ?? {}) as Endpoint;
  const src = (tr.source ?? {}) as Endpoint;
  const dst = (tr.destination ?? {}) as Endpoint;
  const srcExtra = (src.extra ?? {}) as Endpoint;
  const dstExtra = (dst.extra ?? {}) as Endpoint;
  const scheduleId = (
    text(srcExtra.schedule_id) || text(dstExtra.schedule_id) || text(rec.schedule_id)
  ).trim();
  const sourceConnectorId = (text(src.connector_id) || text(rec.source_connector_id)).trim();
  const destConnectorId = (text(dst.connector_id) || text(rec.dest_connector_id)).trim();
  if (!scheduleId || !sourceConnectorId || !destConnectorId) return null;
  const sourceTable = (text(src.table) || text(src.collection)).trim();
  const destTable = (text(dst.table) || text(dst.collection)).trim();
  return {
    scheduleId,
    sourceConnectorId,
    destConnectorId,
    ...(sourceTable ? { sourceTable } : {}),
    ...(destTable ? { destTable } : {}),
  };
}
