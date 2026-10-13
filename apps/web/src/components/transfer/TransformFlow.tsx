import { useEffect, useLayoutEffect, useMemo, useRef, useState, type DragEvent } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  ReactFlowProvider,
  applyNodeChanges,
  useReactFlow,
  useStore,
  type Connection,
  type Edge,
  type Node,
  type NodeProps,
  type OnConnect,
  type OnConnectEnd,
  type OnNodesChange,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { DtIcon } from "../DtIcon";
import {
  classifyPortLink,
  insertIndexAtPoint,
  FIT_MAX_ZOOM,
  FIT_MIN_ZOOM,
  LAYOUT_PITCH,
  layoutFor,
  NODE_BODY,
  NODE_WIDTH,
  viewportForChain,
  spliceIndexAtPoint,
  type FlowEdgeModel,
  type FlowNodeModel,
  type FlowPoint,
  type PendingStepModel,
} from "../../lib/canvasDocument";
import {
  categoryLabel,
  iconFor,
  nodeStatus,
  shortName,
  statusLabel,
  streamPorts,
  stripeFor,
  type CanvasPort,
  type NodeStatus,
  type NodeStripe,
} from "../../lib/canvasRegistry";

export interface StudioNodeData extends Record<string, unknown> {
  index: number;
  title: string;
  summary: string;
  op: string;
  icon: string;
  stripe: NodeStripe;
  status: NodeStatus;
  ports: CanvasPort[];
  family: string;
  category: string;
  kind: string;
  expanded: boolean;
  onToggleFields: (id: string) => void;
  onPreview: (index: number) => void;
  dimmed: boolean;
  disabled: boolean;
  changesRowCount: boolean;
  invalid: boolean;
  invalidReason: string;
}

const nodeTypes = { studio: StudioFlowNode };
const PITCH = LAYOUT_PITCH;

function positive(value: number | undefined): number {
  return value != null && value > 0 ? value : 0;
}

/**
 * xyflow's fitView drops any node whose measured box is still 0, so the last
 * card in a new chain is framed out and lands under the minimap. Declared
 * width and height are the card size we already render.
 */
function boundsOf(nodes: Node[]): { x: number; y: number; width: number; height: number } | null {
  if (nodes.length === 0) return null;
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const node of nodes) {
    const width = positive(node.measured?.width) || positive(node.width) || NODE_WIDTH;
    const height = positive(node.measured?.height) || positive(node.height) || NODE_BODY;
    minX = Math.min(minX, node.position.x);
    minY = Math.min(minY, node.position.y);
    maxX = Math.max(maxX, node.position.x + width);
    maxY = Math.max(maxY, node.position.y + height);
  }
  return {
    x: minX,
    y: minY,
    width: Math.max(1, maxX - minX),
    height: Math.max(1, maxY - minY),
  };
}

interface TransformFlowProps {
  nodes: FlowNodeModel[];
  edges: FlowEdgeModel[];
  pending: PendingStepModel | null;
  placed: { index: number; x: number; y: number } | null;
  onSelect: (index: number) => void;
  onReorder: (fromIndex: number, toIndex: number) => void;
  onInvalidLink: (reason: string) => void;
  onAcceptLink: (message: string) => void;
  onDropOp: (op: string, at: FlowPoint, index: number) => void;
  onPreviewNode: (index: number) => void;
  fitTick: number;
}

function stepIndex(id: string | null | undefined): number {
  const index = Number(String(id ?? "").replace("step-", ""));
  return Number.isInteger(index) ? index : -1;
}

function readTheme(): "light" | "dark" {
  return document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
}

/** xyflow surface. Loaded only when the canvas flag is on. */
export default function TransformFlow(props: TransformFlowProps) {
  return (
    <ReactFlowProvider>
      <FlowCanvas {...props} />
    </ReactFlowProvider>
  );
}

function FlowCanvas({
  nodes,
  edges,
  pending,
  placed,
  onSelect,
  onReorder,
  onInvalidLink,
  onAcceptLink,
  onDropOp,
  onPreviewNode,
  fitTick,
}: TransformFlowProps) {
  const { screenToFlowPosition, setViewport } = useReactFlow();
  const paneWidth = useStore((state) => state.width);
  const paneHeight = useStore((state) => state.height);
  const [hot, setHot] = useState(false);
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set());
  const [theme, setTheme] = useState<"light" | "dark">(readTheme);
  const toggleFields = (id: string) => {
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };
  const built = useMemo(
    () => buildNodes(nodes, pending, placed, expanded, toggleFields, onPreviewNode),
    [nodes, pending, placed, expanded, onPreviewNode],
  );
  const [flowNodes, setFlowNodes] = useState(built);
  const placedRef = useRef(placed);
  placedRef.current = placed;
  const nodesRef = useRef(flowNodes);
  nodesRef.current = flowNodes;
  const paneRef = useRef({ w: paneWidth, h: paneHeight });
  paneRef.current = { w: paneWidth, h: paneHeight };
  const autoFitted = useRef(false);
  const fittedPane = useRef({ w: 0, h: 0 });
  useEffect(() => {
    setFlowNodes(built);
  }, [built]);
  useLayoutEffect(() => {
    const explicit = fitTick > 0;
    // A drop lands on the pointer. Fitting that frame would slide the node
    // off the point the test and the operator just chose. An explicit Fit or
    // Arrange still frames every node, including one that was dropped.
    if (!explicit && placed) return;
    if (nodesRef.current.length === 0) return;
    const pane = paneRef.current;
    if (pane.w < 40 || pane.h < 48) return;
    const paneChanged = Math.abs(pane.w - fittedPane.current.w) > 16
      || Math.abs(pane.h - fittedPane.current.h) > 16;
    // The first measure is often the stage before the column finishes
    // layout. Fit again when the pane actually grows, or the chain stays
    // small in the corner. Declared card size is enough, so this runs
    // before paint instead of waiting on xyflow's measured box.
    if (!explicit && autoFitted.current && !paneChanged) return;
    if (!explicit && placedRef.current) return;
    const bounds = boundsOf(nodesRef.current);
    if (!bounds) return;
    if (!explicit) autoFitted.current = true;
    const viewport = viewportForChain(bounds, pane.w, pane.h);
    fittedPane.current = { w: pane.w, h: pane.h };
    void setViewport(viewport, { duration: 0 });
  }, [fitTick, flowNodes.length, placed, paneWidth, paneHeight, setViewport]);
  useEffect(() => {
    const read = () => setTheme(readTheme());
    const observer = new MutationObserver(read);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme", "class"] });
    return () => observer.disconnect();
  }, []);

  const linkColor = theme === "dark" ? "#2dd4bf" : "#0f766e";
  const flowEdges: Edge[] = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: edge.sourcePort,
    targetHandle: edge.targetPort,
    type: "default",
    markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16, color: linkColor },
  }));

  const onNodesChange: OnNodesChange<Node<StudioNodeData>> = (changes) => {
    setFlowNodes((current) => applyNodeChanges(changes, current));
  };

  const verdictFor = (
    source: string | null | undefined,
    sourcePort: string | null | undefined,
    target: string | null | undefined,
    targetPort: string | null | undefined,
  ) => classifyPortLink({
    nodes,
    source: source ?? "",
    sourcePort: sourcePort || "out",
    target: target ?? "",
    targetPort: targetPort || "in",
  });

  const linkOk = (connection: Connection | Edge) => (
    verdictFor(connection.source, connection.sourceHandle, connection.target, connection.targetHandle).ok
  );

  const onConnect: OnConnect = (connection) => {
    const verdict = verdictFor(connection.source, connection.sourceHandle, connection.target, connection.targetHandle);
    if (!verdict.ok) {
      onInvalidLink(verdict.reason);
      return;
    }
    const from = stepIndex(connection.source) + 1;
    const to = stepIndex(connection.target) + 1;
    onAcceptLink(`Connected step ${from} to step ${to}.`);
  };

  const onConnectEnd: OnConnectEnd = (_event, state) => {
    if (state.isValid !== false || !state.fromNode || !state.toNode) return;
    const verdict = verdictFor(state.fromNode.id, state.fromHandle?.id, state.toNode.id, state.toHandle?.id);
    if (!verdict.ok) onInvalidLink(verdict.reason);
  };

  const onDrop = (event: DragEvent) => {
    event.preventDefault();
    setHot(false);
    const op = event.dataTransfer.getData("application/x-df-op");
    if (!op) return;
    const at = screenToFlowPosition({ x: event.clientX, y: event.clientY });
    const positions = flowNodes
      .filter((node) => node.id !== "pending")
      .sort((a, b) => a.data.index - b.data.index)
      .map((node) => node.position);
    onDropOp(op, at, insertIndexAtPoint(nodes.length, at, positions));
  };

  return (
    <div
      className={hot ? "df2-canvas-stage is-drop-target" : "df2-canvas-stage"}
      data-testid="transform-canvas-stage"
    >
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        fitView={false}
        minZoom={FIT_MIN_ZOOM}
        maxZoom={FIT_MAX_ZOOM}
        panOnScroll
        zoomOnScroll={false}
        proOptions={{ hideAttribution: true }}
        nodesDraggable
        nodesConnectable
        elementsSelectable
        connectionRadius={28}
        colorMode={theme}
        deleteKeyCode={null}
        isValidConnection={linkOk}
        onConnect={onConnect}
        onConnectEnd={onConnectEnd}
        onDragOver={(event) => {
          event.preventDefault();
          if (event.dataTransfer) event.dataTransfer.dropEffect = "copy";
          setHot(true);
        }}
        onDragLeave={() => setHot(false)}
        onDrop={onDrop}
        onNodeClick={(_, node) => {
          if (node.id === "pending") return;
          onSelect(node.data.index);
        }}
        onNodeDragStop={(_, node) => {
          if (node.id === "pending" || node.data.disabled) return;
          const from = node.data.index;
          const others = flowNodes
            .filter((item) => item.id !== node.id && item.id !== "pending")
            .sort((a, b) => a.data.index - b.data.index);
          const hit = spliceIndexAtPoint(
            others.map((item) => item.position),
            { x: node.position.x + NODE_WIDTH / 2, y: node.position.y + NODE_BODY / 2 },
          );
          if (hit != null) {
            if (hit !== from) onReorder(from, hit);
            return;
          }
          const to = Math.max(0, Math.min(nodes.length - 1, Math.round(node.position.x / PITCH)));
          if (to !== from) onReorder(from, to);
        }}
      >
        <Background variant={BackgroundVariant.Dots} gap={18} size={1.5} color="var(--df-border-strong)" />
        <Controls showInteractive={false} showFitView={false} />
        <MiniMap
          pannable={false}
          zoomable={false}
          ariaLabel="Canvas overview"
          className="df2-canvas-minimap"
          style={{ width: 96, height: 68 }}
          bgColor="var(--df-surface)"
          nodeColor="var(--df-border-strong)"
          maskColor="color-mix(in srgb, var(--df-brand-tile) 16%, transparent)"
        />
      </ReactFlow>
    </div>
  );
}

function buildNodes(
  nodes: FlowNodeModel[],
  pending: PendingStepModel | null,
  placed: { index: number; x: number; y: number } | null,
  expanded: ReadonlySet<string>,
  onToggleFields: (id: string) => void,
  onPreview: (index: number) => void,
): Node<StudioNodeData>[] {
  const shift = pending ? 1 : 0;
  const positions = layoutFor(nodes.length + shift);
  const flowNodes: Node<StudioNodeData>[] = nodes.map((node) => {
    const slot = pending && node.index >= pending.atIndex ? node.index + 1 : node.index;
    const anchored = placed && placed.index === node.index ? { x: placed.x, y: placed.y } : null;
    return {
      id: node.id,
      type: "studio",
      position: anchored ?? positions[slot] ?? { x: 24, y: 28 },
      width: NODE_WIDTH,
      height: NODE_BODY,
      initialWidth: NODE_WIDTH,
      initialHeight: NODE_BODY,
      style: { width: NODE_WIDTH },
      data: {
        index: node.index,
        title: node.title,
        summary: node.summary,
        op: node.op,
        icon: node.icon,
        stripe: node.stripe,
        status: node.status,
        ports: node.ports,
        family: node.family,
        category: node.category,
        kind: node.short,
        expanded: expanded.has(node.id),
        onToggleFields,
        onPreview,
        dimmed: node.dimmed,
        disabled: node.disabled,
        changesRowCount: node.changesRowCount,
        invalid: false,
        invalidReason: "",
      },
      draggable: !node.disabled,
      connectable: !node.disabled,
    };
  });
  if (pending) {
    const slot = Math.max(0, Math.min(nodes.length, pending.atIndex));
    const anchored = placed && placed.index < 0 ? { x: placed.x, y: placed.y } : null;
    flowNodes.push({
      id: "pending",
      type: "studio",
      position: anchored ?? positions[slot] ?? { x: 24, y: 28 },
      width: NODE_WIDTH,
      height: NODE_BODY,
      initialWidth: NODE_WIDTH,
      initialHeight: NODE_BODY,
      style: { width: NODE_WIDTH },
      data: {
        index: -1,
        title: shortName(pending.op, pending.title),
        summary: pending.title,
        op: pending.op,
        icon: iconFor(pending.op, pending.family),
        stripe: stripeFor(pending.family),
        status: nodeStatus({ invalid: pending.invalid }),
        ports: streamPorts(),
        family: pending.family,
        category: categoryLabel(pending.family),
        kind: shortName(pending.op, pending.title),
        expanded: false,
        onToggleFields,
        onPreview,
        dimmed: false,
        disabled: false,
        changesRowCount: false,
        invalid: pending.invalid,
        invalidReason: pending.reason,
      },
      draggable: false,
      connectable: false,
      selectable: true,
    });
  }
  return flowNodes;
}

function shownPorts(ports: CanvasPort[], expanded: boolean, role: "in" | "out"): CanvasPort[] {
  return ports.filter((port) => port.role === role && (port.visible || (expanded && port.kind === "field")));
}

function StudioFlowNode({ id, data, selected }: NodeProps<Node<StudioNodeData>>) {
  const className = [
    "df2-canvas-node",
    selected ? "is-selected" : "",
    data.expanded ? "is-expanded" : "",
    data.dimmed ? "is-dimmed" : "",
    data.disabled ? "is-disabled" : "",
    data.invalid ? "is-invalid" : "",
  ].filter(Boolean).join(" ");
  const inputs = shownPorts(data.ports, data.expanded, "in");
  const outputs = shownPorts(data.ports, data.expanded, "out");
  const fields = data.ports.filter((port) => port.kind === "field");
  return (
    <div className={className} data-op={data.op} data-index={data.index} data-stripe={data.stripe} data-icon={data.icon} data-status={data.status}>
      {inputs.map((port, index) => (
        <Handle
          key={port.id}
          id={port.id}
          type="target"
          position={Position.Left}
          className={`df2-canvas-port is-in${port.kind === "field" ? " is-field" : ""}`}
          style={{ top: `${((index + 1) / (inputs.length + 1)) * 100}%` }}
          aria-label={port.label}
        />
      ))}
      <div className="df2-canvas-node-band">
        <span className="df2-canvas-node-mark" aria-hidden>
          <DtIcon name={data.invalid ? "alert" : data.icon} size={24} chrome="product" />
        </span>
        <div className="df2-canvas-node-id">
          <p className="df2-canvas-node-title">{data.title}</p>
          <p className="df2-canvas-node-type">{data.kind} · {data.category}</p>
        </div>
        <span className={`df2-canvas-badge is-${data.status}`}>{statusLabel(data.status)}</span>
      </div>
      <div className="df2-canvas-node-body">
        <p className="df2-canvas-node-summary">{data.summary}</p>
        {data.invalid && <p className="df2-canvas-node-invalid">{data.invalidReason}</p>}
        {data.index >= 0 && (
          <div className="df2-canvas-node-tools" role="toolbar" aria-label={`Actions for ${data.title}`}>
            <button type="button" className="df2-canvas-tool" onClick={() => data.onPreview(data.index)}>
              Preview step
            </button>
            <button
              type="button"
              className="df2-canvas-tool"
              aria-expanded={data.expanded}
              aria-controls={`${id}-fields`}
              onClick={() => data.onToggleFields(id)}
            >
              {data.expanded ? "Hide fields" : "Show fields"}
            </button>
          </div>
        )}
        {data.expanded && (
          <ul className="df2-canvas-fields" id={`${id}-fields`}>
            {fields.map((port) => (
              <li key={port.id} className="df2-canvas-field">{port.role === "in" ? "In" : "Out"} · {port.label}</li>
            ))}
          </ul>
        )}
      </div>
      {outputs.map((port, index) => (
        <Handle
          key={port.id}
          id={port.id}
          type="source"
          position={Position.Right}
          className={`df2-canvas-port${port.kind === "field" ? " is-field" : data.changesRowCount ? " is-count" : " is-out"}`}
          style={{ top: `${((index + 1) / (outputs.length + 1)) * 100}%` }}
          aria-label={data.changesRowCount && port.kind === "row" ? `${port.label}. This node changes row count` : port.label}
        />
      ))}
    </div>
  );
}
