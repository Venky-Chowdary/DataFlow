import { useEffect, useMemo, useState, type DragEvent } from "react";
import {
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  ReactFlowProvider,
  applyNodeChanges,
  useReactFlow,
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
  classifyChainLink,
  familyIcon,
  insertIndexAtPoint,
  LAYOUT_PITCH,
  layoutFor,
  NODE_BODY,
  NODE_WIDTH,
  spliceIndexAtPoint,
  type FlowEdgeModel,
  type FlowNodeModel,
  type FlowPoint,
  type PendingStepModel,
} from "../../lib/canvasDocument";

export interface StudioNodeData extends Record<string, unknown> {
  index: number;
  title: string;
  op: string;
  family: string;
  dimmed: boolean;
  disabled: boolean;
  changesRowCount: boolean;
  invalid: boolean;
  invalidReason: string;
}

const nodeTypes = { studio: StudioFlowNode };
const PITCH = LAYOUT_PITCH;

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
}: TransformFlowProps) {
  const { screenToFlowPosition } = useReactFlow();
  const built = useMemo(() => buildNodes(nodes, pending, placed), [nodes, pending, placed]);
  const [flowNodes, setFlowNodes] = useState(built);
  const [hot, setHot] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark">(readTheme);
  useEffect(() => {
    setFlowNodes(built);
  }, [built]);
  useEffect(() => {
    const read = () => setTheme(readTheme());
    const observer = new MutationObserver(read);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme", "class"] });
    return () => observer.disconnect();
  }, []);

  const flowEdges: Edge[] = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: "smoothstep",
  }));

  const onNodesChange: OnNodesChange<Node<StudioNodeData>> = (changes) => {
    setFlowNodes((current) => applyNodeChanges(changes, current));
  };

  const verdictFor = (source: string | null | undefined, target: string | null | undefined) => (
    classifyChainLink(nodes.length, stepIndex(source), stepIndex(target))
  );

  const linkOk = (connection: Connection | Edge) => verdictFor(connection.source, connection.target).ok;

  const onConnect: OnConnect = (connection) => {
    const verdict = verdictFor(connection.source, connection.target);
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
    const verdict = verdictFor(state.fromNode.id, state.toNode.id);
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
          const to = Math.max(0, Math.min(nodes.length - 1, Math.round(node.position.y / PITCH)));
          if (to !== from) onReorder(from, to);
        }}
      >
        <Background gap={16} color="var(--df-border)" />
        <Controls showInteractive={false} />
        <MiniMap pannable={false} zoomable={false} />
      </ReactFlow>
    </div>
  );
}

function buildNodes(
  nodes: FlowNodeModel[],
  pending: PendingStepModel | null,
  placed: { index: number; x: number; y: number } | null,
): Node<StudioNodeData>[] {
  const shift = pending ? 1 : 0;
  const positions = layoutFor(nodes.length + shift);
  const flowNodes: Node<StudioNodeData>[] = nodes.map((node) => {
    const slot = pending && node.index >= pending.atIndex ? node.index + 1 : node.index;
    const anchored = placed && placed.index === node.index ? { x: placed.x, y: placed.y } : null;
    return {
      id: node.id,
      type: "studio",
      position: anchored ?? positions[slot] ?? { x: 48, y: slot * PITCH },
      data: {
        index: node.index,
        title: node.title,
        op: node.op,
        family: node.family,
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
      position: anchored ?? positions[slot] ?? { x: 48, y: slot * PITCH },
      data: {
        index: -1,
        title: pending.title,
        op: pending.op,
        family: pending.family,
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

function StudioFlowNode({ data, selected }: NodeProps<Node<StudioNodeData>>) {
  const className = [
    "df2-canvas-node",
    selected ? "is-selected" : "",
    data.dimmed ? "is-dimmed" : "",
    data.disabled ? "is-disabled" : "",
    data.invalid ? "is-invalid" : "",
  ].filter(Boolean).join(" ");
  return (
    <div className={className} data-op={data.op} data-index={data.index}>
      <Handle type="target" position={Position.Top} className="df2-canvas-port is-in" aria-label="Previous step" />
      <div className="df2-canvas-node-head">
        <span className="df2-canvas-node-mark" aria-hidden>
          <DtIcon name={data.invalid ? "alert" : familyIcon(data.family)} size={16} chrome="product" />
        </span>
        <p className="df2-canvas-node-title">{data.title}</p>
      </div>
      <p className="df2-canvas-node-op">{data.op}</p>
      {data.invalid && <p className="df2-canvas-node-invalid">{data.invalidReason}</p>}
      <Handle
        type="source"
        position={Position.Bottom}
        className={`df2-canvas-port${data.changesRowCount ? " is-count" : " is-out"}`}
        aria-label={data.changesRowCount ? "Next step, this node changes row count" : "Next step"}
      />
    </div>
  );
}
