import { useEffect, useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { HubEdge, TopologyRole } from "../lib/topologyUtils";
import { DtLogo } from "./DtLogo";

export interface HubNode {
  id: string;
  label: string;
  type: string;
  active?: boolean;
  role?: TopologyRole;
  linked?: boolean;
  isVirtual?: boolean;
}

interface ConnectionHubProps {
  nodes: HubNode[];
  edges?: HubEdge[];
  centerLabel?: string;
  emptyHint?: string;
  variant?: "default" | "hero";
  /** Fixed-height viewport — compresses rows instead of growing the page */
  bounded?: boolean;
  maxViewportHeight?: number;
  /** Show only connectors that participate in routes (Airbyte-style clarity) */
  routesOnly?: boolean;
  layout?: "hub" | "pipeline";
}

const GLASS_HALF = 28;
const HUB_RADIUS = 38;
const BOUNDED_VIEWPORT_HEIGHT = 360;
const COMPACT_NODE_ROW_HEIGHT = 52;

interface Point {
  x: number;
  y: number;
}

const NODE_ROW_HEIGHT = 84;
const MAX_PER_COLUMN = 7;
const MAX_CANVAS_HEIGHT = 560;
const MIN_CANVAS_HEIGHT = 260;

function maxPerColumnForWidth(w: number, bounded?: boolean): number {
  if (bounded) {
    if (w < 420) return 3;
    if (w < 640) return 4;
    return 5;
  }
  if (w < 420) return 4;
  if (w < 640) return 5;
  if (w < 900) return 6;
  return MAX_PER_COLUMN;
}

function layoutColumn(
  count: number,
  index: number,
  side: "left" | "right",
  w: number,
  h: number,
  rowHeight?: number,
): Point {
  const narrow = w < 640;
  const xInset = narrow ? Math.min(56, w * 0.14) : Math.max(w * 0.12, 70);
  const x = side === "left" ? xInset : Math.max(xInset, w - xInset);
  const pad = rowHeight && rowHeight < NODE_ROW_HEIGHT ? 28 : Math.max(narrow ? 36 : 48, h * 0.08);
  const usable = Math.max(h - pad * 2, 72);
  const y = count <= 1 ? h / 2 : pad + (usable / Math.max(count - 1, 1)) * index;
  return { x, y };
}

interface ColumnResult {
  visible: HubNode[];
  overflow: HubNode | null;
  overflowIds: Set<string>;
}

/**
 * Cap a column at MAX_PER_COLUMN so the topology never grows unbounded.
 * Remaining nodes collapse into a single "+N more" chip.
 */
function capColumn(nodes: HubNode[], role: TopologyRole, limit: number): ColumnResult {
  if (nodes.length <= limit) {
    return { visible: nodes, overflow: null, overflowIds: new Set() };
  }
  const keep = limit - 1;
  const visible = nodes.slice(0, keep);
  const rest = nodes.slice(keep);
  const overflow: HubNode = {
    id: `overflow:${role}`,
    label: `+${rest.length} more`,
    type: "more",
    role,
    linked: rest.some((n) => n.linked),
    isVirtual: true,
  };
  return { visible, overflow, overflowIds: new Set(rest.map((n) => n.id)) };
}

function smoothRoutePath(from: Point, to: Point, spread: number): string {
  const dx = to.x - from.x;
  const c1x = from.x + dx * 0.42;
  const c2x = to.x - dx * 0.42;
  const c1y = from.y + spread;
  const c2y = to.y + spread;
  return `M ${from.x} ${from.y} C ${c1x} ${c1y}, ${c2x} ${c2y}, ${to.x} ${to.y}`;
}

function anchorForNode(pos: Point, side: "left" | "right"): Point {
  return {
    x: pos.x + (side === "left" ? GLASS_HALF : -GLASS_HALF),
    y: pos.y,
  };
}

export function ConnectionHub({
  nodes,
  edges = [],
  centerLabel = "DataFlow",
  emptyHint,
  variant = "default",
  bounded = false,
  maxViewportHeight = BOUNDED_VIEWPORT_HEIGHT,
  routesOnly = false,
  layout = "hub",
}: ConnectionHubProps) {
  const canvasRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 520, h: 320 });
  const columnLimit = maxPerColumnForWidth(size.w, bounded);
  const pipelineLayout = layout === "pipeline" || variant === "hero";

  const displayNodes = useMemo(() => {
    if (!routesOnly || edges.length === 0) return nodes;
    const linkedIds = new Set<string>();
    for (const e of edges) {
      linkedIds.add(e.sourceNodeId);
      linkedIds.add(e.destNodeId);
    }
    const filtered = nodes.filter((n) => linkedIds.has(n.id));
    return filtered.length > 0 ? filtered : nodes;
  }, [nodes, edges, routesOnly]);
  const hasNodes = displayNodes.length > 0;
  const hasRoutes = edges.length > 0;
  const linkedCount = displayNodes.filter((n) => n.linked).length;
  const liveRoutes = edges.filter((e) => e.active).length;

  const { sources, destinations, positions, remap, canvasHeight, hiddenCount, rowHeight, viewportHeight } = useMemo(() => {
    const srcAll: HubNode[] = [];
    const dstAll: HubNode[] = [];
    const pos = new Map<string, Point>();

    displayNodes.forEach((n) => {
      if (n.role === "destination") dstAll.push(n);
      else srcAll.push(n);
    });

    const srcCap = capColumn(srcAll, "source", columnLimit);
    const dstCap = capColumn(dstAll, "destination", columnLimit);

    const src = srcCap.overflow ? [...srcCap.visible, srcCap.overflow] : srcCap.visible;
    const dst = dstCap.overflow ? [...dstCap.visible, dstCap.overflow] : dstCap.visible;

    const rowCount = Math.max(src.length, dst.length);
    const viewportH = bounded ? maxViewportHeight : null;
    const rowH = viewportH
      ? Math.min(NODE_ROW_HEIGHT, Math.max(COMPACT_NODE_ROW_HEIGHT, Math.floor((viewportH - 40) / Math.max(rowCount, 1))))
      : NODE_ROW_HEIGHT;
    const h = viewportH
      ? viewportH
      : Math.min(
          MAX_CANVAS_HEIGHT,
          Math.max(MIN_CANVAS_HEIGHT, rowCount * NODE_ROW_HEIGHT + (size.w < 640 ? 32 : 48)),
        );

    src.forEach((n, i) => pos.set(n.id, layoutColumn(src.length, i, "left", size.w, h, rowH)));
    dst.forEach((n, i) => pos.set(n.id, layoutColumn(dst.length, i, "right", size.w, h, rowH)));

    const remapId = new Map<string, string>();
    if (srcCap.overflow) srcCap.overflowIds.forEach((id) => remapId.set(id, srcCap.overflow!.id));
    if (dstCap.overflow) dstCap.overflowIds.forEach((id) => remapId.set(id, dstCap.overflow!.id));

    const hidden = srcCap.overflowIds.size + dstCap.overflowIds.size;

    return {
      sources: src,
      destinations: dst,
      positions: pos,
      remap: remapId,
      canvasHeight: h,
      hiddenCount: hidden,
      rowHeight: rowH,
      viewportHeight: viewportH,
    };
  }, [displayNodes, size.w, columnLimit, bounded, maxViewportHeight]);

  const hub: Point = { x: size.w / 2, y: canvasHeight / 2 };

  const routePaths = useMemo(() => {
    if (!hasRoutes) return [];
    const paths: { id: string; d: string; active?: boolean }[] = [];
    const spreadStep = edges.length > 1 ? Math.min(18, 72 / Math.max(edges.length - 1, 1)) : 0;
    let edgeIndex = 0;
    for (const edge of edges) {
      const srcNodeId = remap.get(edge.sourceNodeId) ?? edge.sourceNodeId;
      const dstNodeId = remap.get(edge.destNodeId) ?? edge.destNodeId;
      const srcPos = positions.get(srcNodeId);
      const dstPos = positions.get(dstNodeId);
      if (!srcPos || !dstPos) continue;

      const spread = spreadStep ? (edgeIndex - (edges.length - 1) / 2) * spreadStep : 0;
      const from = anchorForNode(srcPos, "left");
      const to = anchorForNode(dstPos, "right");

      if (pipelineLayout) {
        paths.push({
          id: edge.id,
          d: smoothRoutePath(from, to, spread),
          active: edge.active,
        });
      } else {
        const ySpread = spread;
        const midY = hub.y + ySpread;
        const midIn = { x: hub.x - HUB_RADIUS, y: midY };
        const midOut = { x: hub.x + HUB_RADIUS, y: midY };
        paths.push({ id: `${edge.id}-in`, d: smoothRoutePath(from, midIn, 0), active: edge.active });
        paths.push({ id: `${edge.id}-bridge`, d: `M ${midIn.x} ${midIn.y} L ${midOut.x} ${midOut.y}`, active: edge.active });
        paths.push({ id: `${edge.id}-out`, d: smoothRoutePath(midOut, to, 0), active: edge.active });
      }
      edgeIndex += 1;
    }
    return paths;
  }, [hasRoutes, edges, positions, remap, hub.x, hub.y, pipelineLayout]);

  useEffect(() => {
    const el = canvasRef.current;
    if (!el) return;
    const measure = () => {
      const rect = el.getBoundingClientRect();
      if (rect.width > 0) {
        setSize((prev) =>
          Math.abs(prev.w - rect.width) < 1 ? prev : { w: rect.width, h: prev.h },
        );
      }
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, [hasNodes, canvasHeight]);

  useEffect(() => {
    setSize((prev) => (prev.h === canvasHeight ? prev : { ...prev, h: canvasHeight }));
  }, [canvasHeight]);

  if (!hasNodes) {
    return (
      <div className={`dt-connection-hub dt-connection-hub-real dt-connection-hub-empty-state ${variant === "hero" ? "dt-connection-hub-hero" : ""}`}>
        <div className="dt-topology-empty">
          <DtIconPlaceholder />
          <p>{emptyHint || "Add saved connectors to populate the data plane"}</p>
        </div>
      </div>
    );
  }

  return (
    <div
      className={`dt-connection-hub dt-connection-hub-real ${variant === "hero" ? "dt-connection-hub-hero" : ""} ${bounded ? "dt-connection-hub-bounded" : ""} ${pipelineLayout ? "dt-connection-hub-airbyte" : ""}`}
      aria-label="Connection topology"
    >
      <div className="dt-topology-meta">
        <span>{displayNodes.length} connection{displayNodes.length === 1 ? "" : "s"}</span>
        {hasRoutes ? (
          <>
            <span aria-hidden>·</span>
            <span>{edges.length} sync route{edges.length === 1 ? "" : "s"}</span>
            {liveRoutes > 0 && (
              <>
                <span aria-hidden>·</span>
                <span className="dt-topology-live">{liveRoutes} active</span>
              </>
            )}
          </>
        ) : (
          <>
            <span aria-hidden>·</span>
            <span className="dt-topology-muted">No routes — run a transfer to connect</span>
          </>
        )}
        {bounded && hiddenCount > 0 && (
          <>
            <span aria-hidden>·</span>
            <span className="dt-topology-muted">{hiddenCount} collapsed</span>
          </>
        )}
      </div>

      {!pipelineLayout && hasRoutes && edges.length > 0 && (
        <div className="dt-topology-routes" aria-label="Active routes">
          {edges.slice(0, 6).map((edge) => {
            const srcNode = displayNodes.find((n) => n.id === edge.sourceNodeId);
            const dstNode = displayNodes.find((n) => n.id === edge.destNodeId);
            const label = `${srcNode?.label ?? edge.sourceNodeId} → ${dstNode?.label ?? edge.destNodeId}`;
            return (
              <span
                key={edge.id}
                className={`dt-route-chip ${edge.active ? "live" : ""}`}
                title={label}
              >
                {label}
              </span>
            );
          })}
          {edges.length > 6 && (
            <span className="dt-route-chip muted">+{edges.length - 6} more</span>
          )}
        </div>
      )}

      {hasRoutes && (
        <div className="dt-connection-hub-flow-labels" aria-hidden>
          <span className="dt-flow-col-label">Sources</span>
          {pipelineLayout && <span className="dt-flow-col-label dt-flow-col-label-center">{centerLabel}</span>}
          <span className="dt-flow-col-label">Destinations</span>
        </div>
      )}

      <div
        className={`dt-topology-viewport ${bounded ? "dt-topology-viewport-bounded" : ""}`}
        style={bounded && viewportHeight ? { height: viewportHeight } : undefined}
      >
        <div
          className={`dt-connection-hub-canvas dt-connection-hub-canvas-aligned ${rowHeight < NODE_ROW_HEIGHT ? "dt-connection-hub-compact" : ""}`}
          ref={canvasRef}
          style={{ height: canvasHeight, minHeight: bounded ? viewportHeight ?? canvasHeight : canvasHeight }}
        >
        <svg
          className="dt-connection-hub-svg dt-connection-hub-svg-aligned"
          width={size.w}
          height={canvasHeight}
          viewBox={`0 0 ${size.w} ${canvasHeight}`}
          preserveAspectRatio="xMidYMid meet"
          aria-hidden={!hasRoutes}
        >
          <defs>
            <linearGradient id="hub-line-grad" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="#0f766e" />
              <stop offset="100%" stopColor="#6366f1" />
            </linearGradient>
            <linearGradient id="hub-line-grad-live" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="#14b8a6" />
              <stop offset="100%" stopColor="#818cf8" />
            </linearGradient>
          </defs>

          {hasRoutes && !pipelineLayout && (
            <>
              <circle cx={hub.x} cy={hub.y} r={HUB_RADIUS + 10} className="dt-connection-hub-core-glow" fill="rgba(15,118,110,0.06)" />
              <circle cx={hub.x} cy={hub.y} r={HUB_RADIUS} className="dt-connection-hub-core-ring" fill="none" />
            </>
          )}

          {routePaths.map((p) => (
            <path
              key={p.id}
              d={p.d}
              className={`dt-connection-hub-path dt-connection-hub-path-linked ${p.active ? "active live" : ""}`}
              fill="none"
              stroke={p.active ? "url(#hub-line-grad-live)" : "url(#hub-line-grad)"}
              strokeWidth={p.active ? 2.5 : 2}
              strokeLinecap="round"
            />
          ))}
        </svg>

        {hasRoutes && (
          <div className="dt-connection-hub-center" style={{ left: hub.x, top: hub.y }}>
            <div className="dt-connection-hub-center-inner">
              <DtLogo size={size.w < 640 ? 26 : 32} />
              <span>{centerLabel}</span>
            </div>
          </div>
        )}

        {sources.map((node) => {
          const pos = positions.get(node.id);
          if (!pos) return null;
          return <HubNodeView key={node.id} node={node} pos={pos} side="source" compact={rowHeight < NODE_ROW_HEIGHT} />;
        })}

        {destinations.map((node) => {
          const pos = positions.get(node.id);
          if (!pos) return null;
          return <HubNodeView key={node.id} node={node} pos={pos} side="destination" compact={rowHeight < NODE_ROW_HEIGHT} />;
        })}
        </div>
      </div>

      {hiddenCount > 0 && (
        <p className="dt-topology-overflow-note">
          {hiddenCount} more connection{hiddenCount === 1 ? "" : "s"} in routes — open Connectors for the full list.
        </p>
      )}

      {!hasRoutes && linkedCount === 0 && (
        <p className="dt-topology-footnote">
          Run a transfer from Transfer Studio or enable a scheduled pipeline to draw routes between connectors.
        </p>
      )}
    </div>
  );
}

function DtIconPlaceholder() {
  return (
    <span className="dt-topology-empty-icon" aria-hidden>
      <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
        <circle cx="6" cy="6" r="3" />
        <circle cx="18" cy="18" r="3" />
        <path d="M8.5 8.5l7 7" />
      </svg>
    </span>
  );
}

function HubNodeView({
  node,
  pos,
  side,
  compact = false,
}: {
  node: HubNode;
  pos: Point;
  side: "source" | "destination";
  compact?: boolean;
}) {
  const offline = node.active === false;
  const unlinked = !node.linked;
  const isOverflow = node.type === "more";
  const iconSize = compact ? 22 : 28;

  if (isOverflow) {
    return (
      <div
        className={`dt-connection-hub-node dt-hub-node-anchor dt-flow-node dt-flow-node-${side === "source" ? "source" : "dest"} dt-flow-node-overflow`}
        style={{ left: pos.x, top: pos.y }}
        title={`${node.label} connection${node.label === "+1 more" ? "" : "s"} not shown`}
      >
        <div className="dt-connection-hub-node-glass dt-node-glass-overflow">
          <span className="dt-node-overflow-count">{node.label.replace(" more", "")}</span>
        </div>
        <span className="dt-connection-hub-node-label">more</span>
      </div>
    );
  }

  return (
    <div
      className={`dt-connection-hub-node dt-hub-node-anchor dt-flow-node dt-flow-node-${side === "source" ? "source" : "dest"} ${offline ? "offline" : ""} ${unlinked ? "unlinked" : "linked"} ${node.isVirtual ? "virtual" : "saved"}`}
      style={{ left: pos.x, top: pos.y }}
      title={`${node.label} (${side}${unlinked ? ", not routed" : ""})`}
    >
      <div className="dt-connection-hub-node-glass">
        <ConnectorIcon id={node.type} size={iconSize} />
        {offline && <span className="dt-node-status-dot err" aria-label="Connection error" />}
        {!offline && node.linked && <span className="dt-node-status-dot ok" aria-label="Routed" />}
      </div>
      <span className="dt-connection-hub-node-label">{node.label}</span>
      {unlinked && !node.isVirtual && <span className="dt-flow-node-role">idle</span>}
    </div>
  );
}
