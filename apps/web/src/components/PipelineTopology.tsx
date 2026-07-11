/** Airbyte-style pipeline topology — sources → platform → destinations. */

import { useMemo } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtLogo } from "./DtLogo";
import type { HubEdge } from "../lib/topologyUtils";

export interface PipelineNode {
  id: string;
  label: string;
  type: string;
  active?: boolean;
  linked?: boolean;
}

interface PipelineTopologyProps {
  nodes: PipelineNode[];
  edges: HubEdge[];
  emptyHint?: string;
}

function shortLabel(label: string, max = 18) {
  return label.length > max ? `${label.slice(0, max - 1)}…` : label;
}

export function PipelineTopology({ nodes, edges, emptyHint }: PipelineTopologyProps) {
  const { sources, destinations, routePairs } = useMemo(() => {
    const linkedIds = new Set<string>();
    for (const e of edges) {
      linkedIds.add(e.sourceNodeId);
      linkedIds.add(e.destNodeId);
    }

    const activeNodes = edges.length
      ? nodes.filter((n) => linkedIds.has(n.id))
      : nodes.filter((n) => n.linked !== false).slice(0, 12);

    const src: PipelineNode[] = [];
    const dst: PipelineNode[] = [];
    for (const n of activeNodes) {
      const inSource = edges.some((e) => e.sourceNodeId === n.id);
      const inDest = edges.some((e) => e.destNodeId === n.id);
      if (inDest && !inSource) dst.push(n);
      else src.push(n);
    }

    const pairs = edges.slice(0, 8).map((e) => {
      const s = nodes.find((n) => n.id === e.sourceNodeId);
      const d = nodes.find((n) => n.id === e.destNodeId);
      return {
        id: e.id,
        source: s?.label ?? e.sourceNodeId,
        dest: d?.label ?? e.destNodeId,
        active: e.active,
      };
    });

    return { sources: src.slice(0, 6), destinations: dst.slice(0, 6), routePairs: pairs };
  }, [nodes, edges]);

  if (!nodes.length) {
    return (
      <div className="df2-pipeline-topology df2-pipeline-topology-empty">
        <p>{emptyHint ?? "Add saved connectors to build your data plane."}</p>
      </div>
    );
  }

  return (
    <div className="df2-pipeline-topology">
      <div className="df2-pipeline-topology-stats">
        <span>{nodes.length} connections</span>
        <span aria-hidden>·</span>
        <span>{edges.length} sync route{edges.length === 1 ? "" : "s"}</span>
        {edges.filter((e) => e.active).length > 0 && (
          <>
            <span aria-hidden>·</span>
            <span className="df2-pipeline-live">{edges.filter((e) => e.active).length} active</span>
          </>
        )}
      </div>

      <div className="df2-pipeline-lanes">
        <div className="df2-pipeline-lane">
          <span className="df2-pipeline-lane-label">Sources</span>
          <div className="df2-pipeline-node-stack">
            {sources.length === 0 ? (
              <span className="df2-pipeline-muted">No sources in routes</span>
            ) : sources.map((n) => (
              <div key={n.id} className={`df2-pipeline-node ${n.active === false ? "err" : ""}`} title={n.label}>
                <ConnectorIcon id={n.type} size={20} />
                <span>{shortLabel(n.label)}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="df2-pipeline-hub" aria-label="DataFlow sync engine">
          <div className="df2-pipeline-hub-connector left" aria-hidden />
          <div className="df2-pipeline-hub-inner">
            <DtLogo size={28} />
            <span>DataFlow</span>
          </div>
          <div className="df2-pipeline-hub-connector right" aria-hidden />
        </div>

        <div className="df2-pipeline-lane">
          <span className="df2-pipeline-lane-label">Destinations</span>
          <div className="df2-pipeline-node-stack">
            {destinations.length === 0 ? (
              <span className="df2-pipeline-muted">No destinations in routes</span>
            ) : destinations.map((n) => (
              <div key={n.id} className={`df2-pipeline-node ${n.active === false ? "err" : ""}`} title={n.label}>
                <ConnectorIcon id={n.type} size={20} />
                <span>{shortLabel(n.label)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {routePairs.length > 0 && (
        <div className="df2-pipeline-routes">
          <span className="df2-pipeline-routes-label">Active pipelines</span>
          <div className="df2-pipeline-route-list">
            {routePairs.map((r) => (
              <div key={r.id} className={`df2-pipeline-route ${r.active ? "live" : ""}`}>
                <span className="df2-pipeline-route-src">{shortLabel(r.source, 14)}</span>
                <span className="df2-pipeline-route-arrow" aria-hidden>→</span>
                <span className="df2-pipeline-route-dst">{shortLabel(r.dest, 14)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {edges.length === 0 && (
        <p className="df2-pipeline-footnote">
          Run a transfer from Transfer Studio or enable a schedule to connect sources to destinations.
        </p>
      )}
    </div>
  );
}
