import { Connector, PipelineSchedule, TransferJob } from "./types";

/** Capability / usage role shown to operators. */
export type TopologyRole = "source" | "destination" | "both";

export interface HubEdge {
  id: string;
  sourceNodeId: string;
  destNodeId: string;
  label?: string;
  active?: boolean;
}

export interface TopologyNode {
  id: string;
  label: string;
  type: string;
  active?: boolean;
  role: TopologyRole;
  linked?: boolean;
  isVirtual?: boolean;
}

export interface DataPlaneTopology {
  nodes: TopologyNode[];
  edges: HubEdge[];
}

/** File / extract-oriented — typically read as source only. */
const SOURCE_ONLY_TYPES = new Set([
  "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet", "avro", "orc", "xml", "yaml",
  "fixed_width", "stripe", "singer_tap", "rest_api", "graphql", "shopify",
]);

/** Sink-oriented — usually destination (can still be dual in rare cases). */
const DESTINATION_LEAN_TYPES = new Set([
  "email", "sftp",
]);

/**
 * Databases and warehouses that work as both source and destination.
 * Defaulting these to "source" is what confused operators after file→MySQL loads.
 */
const BIDIRECTIONAL_TYPES = new Set([
  "mysql", "mariadb", "singlestore",
  "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb", "supabase",
  "sqlserver", "mssql", "synapse", "oracle", "db2", "generic_sql",
  "sqlite", "duckdb", "h2",
  "mongodb", "dynamodb", "cassandra", "couchbase", "elasticsearch", "redis",
  "snowflake", "bigquery", "databricks", "clickhouse", "trino", "presto", "questdb",
  "s3", "amazon_s3", "gcs", "google_cloud_storage", "adls", "azure_blob", "azure_blob_storage",
  "kafka", "apache_kafka", "iceberg", "apache_iceberg",
  "salesforce", "hubspot",
]);

export function formatConnectorRoleLabel(role: TopologyRole | string | undefined): string {
  const r = (role || "").toLowerCase();
  if (r === "destination" || r === "dest") return "Destination";
  if (r === "both" || r === "source_and_destination" || r === "bidirectional") {
    return "Source & destination";
  }
  if (r === "source") return "Source";
  return "Source & destination";
}

export function connectorMatchesRoleFilter(
  role: TopologyRole,
  filter: "all" | "source" | "destination",
): boolean {
  if (filter === "all") return true;
  if (role === "both") return true;
  return role === filter;
}

/**
 * Infer the *capability* role for a saved connection (not last-transfer usage).
 * MySQL/Postgres/etc. are always "both" — catalog clicks that saved role=source
 * must not lock a dual-use system into a Source-only badge after file→DB loads.
 */
export function inferTopologyRole(type: string, name = "", connectorRole?: string): TopologyRole {
  const t = type.toLowerCase();
  const n = name.toLowerCase();

  // Type wins for dual-use systems. Persisted role is often "where I clicked in
  // the catalog," not a capability lock.
  if (BIDIRECTIONAL_TYPES.has(t)) return "both";
  if (SOURCE_ONLY_TYPES.has(t)) return "source";
  if (DESTINATION_LEAN_TYPES.has(t)) return "destination";

  const r = (connectorRole ?? "").toLowerCase().trim();
  if (r === "source") return "source";
  if (r === "destination" || r === "dest") return "destination";
  if (r === "both" || r === "source_and_destination" || r === "bidirectional") return "both";

  if (/\b(dest|target|warehouse|sink|output|archive)\b/.test(n)) return "destination";
  if (/\b(source|src|input|origin|extract)\b/.test(n)) return "source";
  // Unknown DB-like types: prefer both over source-only to avoid destination confusion.
  return "both";
}

/** How this connection has actually been used in jobs/schedules. */
export function resolveConnectorUsage(
  connector: Connector,
  jobs: TransferJob[] = [],
  schedules: PipelineSchedule[] = [],
): { asSource: boolean; asDestination: boolean; hint: string | null } {
  const id = connector.id;
  const name = (connector.name || "").toLowerCase();
  const type = (connector.type || "").toLowerCase();
  const db = (connector.database || "").toLowerCase();

  let asSource = schedules.some((s) => s.source_connector_id === id);
  let asDestination = schedules.some((s) => s.dest_connector_id === id);

  for (const job of jobs) {
    const src = (job.source_name ?? "").toLowerCase();
    if (src && (src === name || src === id.toLowerCase())) asSource = true;
    if (job.source_type?.toLowerCase() === type && src && src === name) asSource = true;

    if (job.destination_type?.toLowerCase() === type) {
      const dest = (job.destination_collection || job.destination_database || "").toLowerCase();
      if (
        dest
        && (dest === db || dest === name || name.includes(dest) || dest.includes(name))
      ) {
        asDestination = true;
      }
      // Also match when transfer used this connector id as destination name.
      if (src === id.toLowerCase()) {
        /* no-op */
      }
      if ((job.destination_collection || "").toLowerCase() === id.toLowerCase()) {
        asDestination = true;
      }
    }
  }

  let hint: string | null = null;
  if (asSource && asDestination) hint = "Used as source and destination";
  else if (asDestination) hint = "Used as destination";
  else if (asSource) hint = "Used as source";

  return { asSource, asDestination, hint };
}

/**
 * Display role for lists: capability first, refined by usage when only one side
 * has been observed (so a MySQL used only as dest reads clearly).
 */
export function resolveDisplayRole(
  connector: Connector,
  jobs: TransferJob[] = [],
  schedules: PipelineSchedule[] = [],
): TopologyRole {
  const capability = inferTopologyRole(connector.type, connector.name, connector.role);
  const { asSource, asDestination } = resolveConnectorUsage(connector, jobs, schedules);
  if (asSource && asDestination) return "both";
  if (asDestination && !asSource) {
    return capability === "source" ? "destination" : capability === "both" ? "both" : "destination";
  }
  if (asSource && !asDestination) {
    return capability === "destination" ? "source" : capability === "both" ? "both" : "source";
  }
  return capability;
}

export function connectorNode(conn: Connector): TopologyNode {
  return {
    id: conn.id,
    label: conn.name,
    type: conn.type,
    active: conn.status !== "error" && conn.last_test_ok !== false,
    role: inferTopologyRole(conn.type, conn.name, conn.role),
    linked: false,
    isVirtual: false,
  };
}

/** @deprecated Use buildDataPlaneTopology */
export function connectorsToHubNodes(connectors: Connector[]) {
  return connectors.map((c) => connectorNode(c));
}

function jobDestLabel(job: TransferJob): string {
  return job.destination_collection || job.destination_database || "destination";
}

function findConnectorForJobSource(connectors: Connector[], job: TransferJob): Connector | undefined {
  const sourceName = job.source_name ?? "";
  if (!sourceName) return undefined;
  return connectors.find(
    (c) => c.id === sourceName
      || c.name === sourceName
      || (c.type === job.source_type && c.name.toLowerCase() === sourceName.toLowerCase()),
  );
}

function findConnectorForJobDest(connectors: Connector[], job: TransferJob): Connector | undefined {
  const destLabel = jobDestLabel(job).toLowerCase();
  return connectors.find(
    (c) => c.type === job.destination_type
      && (
        (job.destination_database && c.database?.toLowerCase() === job.destination_database.toLowerCase())
        || c.name.toLowerCase() === destLabel
        || destLabel.length > 2 && c.name.toLowerCase().includes(destLabel)
      ),
  );
}

function mergeRole(current: TopologyRole, next: TopologyRole): TopologyRole {
  if (current === next) return current;
  if (current === "both" || next === "both") return "both";
  if (
    (current === "source" && next === "destination")
    || (current === "destination" && next === "source")
  ) {
    return "both";
  }
  return next;
}

/**
 * Real saved connectors always appear as nodes.
 * Edges come only from enabled schedules and transfer job history.
 */
export function buildDataPlaneTopology(
  connectors: Connector[],
  jobs: TransferJob[],
  schedules: PipelineSchedule[] = [],
): DataPlaneTopology {
  const nodeById = new Map<string, TopologyNode>();
  const edges: HubEdge[] = [];
  const edgeKeys = new Set<string>();

  for (const conn of connectors) {
    nodeById.set(conn.id, connectorNode(conn));
  }

  const touchNode = (id: string, role: TopologyRole): string => {
    const n = nodeById.get(id);
    if (n) {
      n.linked = true;
      n.role = mergeRole(n.role, role);
    }
    return id;
  };

  // Dedupe virtual endpoints by role+type+label so 20 file→mongo jobs
  // collapse into one "file" node and one "mongo" node instead of 40 nodes.
  const ensureVirtualNode = (
    label: string,
    type: string,
    role: TopologyRole,
    active: boolean,
  ) => {
    const id = `virtual:${role}:${type}:${label.toLowerCase()}`;
    if (!nodeById.has(id)) {
      nodeById.set(id, { id, label, type, role, active, linked: true, isVirtual: true });
    } else {
      const n = nodeById.get(id)!;
      n.linked = true;
      n.role = mergeRole(n.role, role);
      if (active) n.active = true;
    }
    return id;
  };

  for (const sched of schedules) {
    if (!sched.enabled) continue;
    const src = connectors.find((c) => c.id === sched.source_connector_id);
    const dst = connectors.find((c) => c.id === sched.dest_connector_id);
    if (!src || !dst) continue;

    touchNode(src.id, "source");
    touchNode(dst.id, "destination");
    const key = `${src.id}→${dst.id}`;
    edgeKeys.add(key);
    edges.push({
      id: `schedule-${sched.id}`,
      sourceNodeId: src.id,
      destNodeId: dst.id,
      label: sched.name,
      active: true,
    });
  }

  for (const job of jobs) {
    const srcConn = findConnectorForJobSource(connectors, job);
    const dstConn = findConnectorForJobDest(connectors, job);
    const running = job.status === "running" || job.status === "pending";

    const srcId = srcConn
      ? touchNode(srcConn.id, "source")
      : ensureVirtualNode(
          job.source_name || job.source_type || "Source",
          job.source_type || "file",
          "source",
          job.status !== "failed",
        );

    const dstId = dstConn
      ? touchNode(dstConn.id, "destination")
      : ensureVirtualNode(
          jobDestLabel(job) || job.destination_type || "Destination",
          job.destination_type || "database",
          "destination",
          job.status === "completed" || job.status === "completed_with_quarantine" || running,
        );

    const key = `${srcId}→${dstId}`;
    if (edgeKeys.has(key)) {
      const existing = edges.find((e) => `${e.sourceNodeId}→${e.destNodeId}` === key);
      if (existing && running) existing.active = true;
      continue;
    }
    edgeKeys.add(key);
    edges.push({
      id: `job-${job._id}`,
      sourceNodeId: srcId,
      destNodeId: dstId,
      label: job.status,
      active: running,
    });
  }

  return { nodes: [...nodeById.values()], edges };
}
