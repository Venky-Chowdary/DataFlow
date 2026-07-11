import { Connector, PipelineSchedule, TransferJob } from "./types";

export type TopologyRole = "source" | "destination";

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

const SOURCE_LEAN_TYPES = new Set([
  "csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet",
  "s3", "gcs", "google_cloud_storage", "dynamodb", "elasticsearch",
]);

const DEST_LEAN_TYPES = new Set([
  "snowflake", "bigquery", "redshift",
]);

export function inferTopologyRole(type: string, name = "", connectorRole?: string): TopologyRole {
  const r = (connectorRole ?? "").toLowerCase();
  if (r === "source") return "source";
  if (r === "destination" || r === "dest") return "destination";

  const t = type.toLowerCase();
  const n = name.toLowerCase();
  if (SOURCE_LEAN_TYPES.has(t)) return "source";
  if (DEST_LEAN_TYPES.has(t)) return "destination";
  if (/\b(dest|target|warehouse|sink|output|archive)\b/.test(n)) return "destination";
  if (/\b(source|src|input|origin|extract)\b/.test(n)) return "source";
  return "source";
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
      if (role === "destination") n.role = "destination";
      else if (role === "source" && n.role !== "destination") n.role = "source";
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
          job.status === "completed" || running,
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
