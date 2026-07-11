import { useMemo, type CSSProperties } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { HubEdge } from "../../lib/topologyUtils";
import { DtLogo } from "../DtLogo";
import type { HubNode } from "../ConnectionHub";

const ROUTE_CARD_LIMIT = 8;
const ICON_ORBIT_MAX = 5;

interface DataPlaneFlowProps {
  nodes: HubNode[];
  edges: HubEdge[];
  connectionCount: number;
  onOpenConnectors?: () => void;
}

function uniqueTypes(nodes: HubNode[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const n of nodes) {
    if (n.type === "more" || seen.has(n.type)) continue;
    seen.add(n.type);
    out.push(n.type);
    if (out.length >= ICON_ORBIT_MAX) break;
  }
  return out;
}

export function DataPlaneFlow({ nodes, edges, connectionCount, onOpenConnectors }: DataPlaneFlowProps) {
  const sources = useMemo(
    () => nodes.filter((n) => n.role !== "destination" && n.linked && n.type !== "more"),
    [nodes],
  );
  const destinations = useMemo(
    () => nodes.filter((n) => n.role === "destination" && n.linked && n.type !== "more"),
    [nodes],
  );
  const liveCount = edges.filter((e) => e.active).length;
  const nodeById = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  const routes = useMemo(
    () =>
      edges.map((edge) => {
        const src = nodeById.get(edge.sourceNodeId);
        const dst = nodeById.get(edge.destNodeId);
        return {
          id: edge.id,
          source: src?.label ?? "Source",
          dest: dst?.label ?? "Destination",
          sourceType: src?.type ?? "file",
          destType: dst?.type ?? "database",
          active: edge.active,
        };
      }),
    [edges, nodeById],
  );

  const sourceTypes = uniqueTypes(sources.length ? sources : nodes.filter((n) => n.role !== "destination"));
  const destTypes = uniqueTypes(destinations.length ? destinations : nodes.filter((n) => n.role === "destination"));
  const hasRoutes = routes.length > 0;

  if (!connectionCount) {
    return (
      <div className="df2-flow-empty">
        <div className="df2-flow-empty-orb" aria-hidden>
          <DtLogo size={36} />
        </div>
        <p>Connect sources and destinations to activate your data plane.</p>
      </div>
    );
  }

  return (
    <div className="df2-flow-panel">
      <div className="df2-flow-stage" aria-hidden>
        <div className="df2-flow-stage-glow" />
        <FlowCanvas
          sourceTypes={sourceTypes}
          destTypes={destTypes}
          sourceCount={sources.length || connectionCount}
          destCount={destinations.length || connectionCount}
          animated={hasRoutes}
        />
      </div>

      <div className="df2-flow-meta">
        <span>{connectionCount} connection{connectionCount === 1 ? "" : "s"}</span>
        <span className="df2-flow-meta-dot" aria-hidden>·</span>
        <span>{edges.length} route{edges.length === 1 ? "" : "s"}</span>
        {liveCount > 0 && (
          <>
            <span className="df2-flow-meta-dot" aria-hidden>·</span>
            <span className="df2-flow-meta-live">{liveCount} streaming</span>
          </>
        )}
      </div>

      {hasRoutes ? (
        <div className="df2-flow-routes">
          <div className="df2-flow-routes-track dt-stagger">
            {routes.slice(0, ROUTE_CARD_LIMIT).map((route, i) => (
              <article
                key={route.id}
                className={`df2-flow-route-card ${route.active ? "is-live" : ""}`}
                style={{ animationDelay: `${i * 40}ms` }}
              >
                <div className="df2-flow-route-end">
                  <ConnectorIcon id={route.sourceType} size={20} />
                  <span title={route.source}>{route.source}</span>
                </div>
                <div className="df2-flow-route-pipe" aria-hidden>
                  <span className="df2-flow-route-line" />
                  {route.active && <span className="df2-flow-route-pulse" />}
                </div>
                <div className="df2-flow-route-end">
                  <ConnectorIcon id={route.destType} size={20} />
                  <span title={route.dest}>{route.dest}</span>
                </div>
                <span className={`df2-flow-route-badge ${route.active ? "live" : ""}`}>
                  {route.active ? "Live" : "Idle"}
                </span>
              </article>
            ))}
          </div>
          {routes.length > ROUTE_CARD_LIMIT && onOpenConnectors && (
            <button type="button" className="df2-flow-routes-more" onClick={onOpenConnectors}>
              +{routes.length - ROUTE_CARD_LIMIT} more · View all connectors
            </button>
          )}
        </div>
      ) : (
        <p className="df2-flow-hint">No routes yet — run a transfer or enable a scheduled pipeline.</p>
      )}
    </div>
  );
}

function FlowCanvas({
  sourceTypes,
  destTypes,
  sourceCount,
  destCount,
  animated,
}: {
  sourceTypes: string[];
  destTypes: string[];
  sourceCount: number;
  destCount: number;
  animated: boolean;
}) {
  return (
    <div className="df2-flow-canvas">
      <div className="df2-flow-cluster df2-flow-cluster-source">
        <span className="df2-flow-cluster-label">Sources</span>
        <strong className="df2-flow-cluster-count">{sourceCount}</strong>
        <ul className="df2-flow-icon-orbit">
          {sourceTypes.map((type, i) => (
            <li key={type} style={{ "--i": i } as CSSProperties}>
              <ConnectorIcon id={type} size={22} />
            </li>
          ))}
        </ul>
      </div>

      <svg className="df2-flow-svg" viewBox="0 0 400 120" preserveAspectRatio="xMidYMid meet">
        <defs>
          <linearGradient id="df-flow-grad" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#0f766e" stopOpacity="0.35" />
            <stop offset="50%" stopColor="#14b8a6" stopOpacity="0.9" />
            <stop offset="100%" stopColor="#6366f1" stopOpacity="0.5" />
          </linearGradient>
        </defs>
        {[36, 60, 84].map((y, idx) => {
          const d = `M 48 ${y} C 130 ${y - 10}, 270 ${y + 10}, 352 ${y}`;
          return (
            <g key={y}>
              <path
                d={d}
                fill="none"
                stroke="url(#df-flow-grad)"
                strokeWidth={idx === 1 ? 2.5 : 1.5}
                strokeLinecap="round"
                className={animated ? "df2-flow-path-animated" : "df2-flow-path-idle"}
                style={{ animationDelay: `${idx * 0.35}s` }}
              />
              {animated && (
                <circle r="3.5" fill="#14b8a6" className="df2-flow-packet">
                  <animateMotion
                    dur={`${2.2 + idx * 0.4}s`}
                    repeatCount="indefinite"
                    path={d}
                    begin={`${idx * 0.5}s`}
                  />
                </circle>
              )}
            </g>
          );
        })}
      </svg>

      <div className="df2-flow-hub">
        <span className="df2-flow-hub-ring" />
        <span className="df2-flow-hub-ring df2-flow-hub-ring-2" />
        <div className="df2-flow-hub-core">
          <DtLogo size={32} />
          <span>DataFlow</span>
        </div>
      </div>

      <div className="df2-flow-cluster df2-flow-cluster-dest">
        <span className="df2-flow-cluster-label">Destinations</span>
        <strong className="df2-flow-cluster-count">{destCount}</strong>
        <ul className="df2-flow-icon-orbit df2-flow-icon-orbit-dest">
          {destTypes.map((type, i) => (
            <li key={type} style={{ "--i": i } as CSSProperties}>
              <ConnectorIcon id={type} size={22} />
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
