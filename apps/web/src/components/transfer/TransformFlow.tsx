import { useEffect, useMemo, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  applyNodeChanges,
  type Connection,
  type Edge,
  type Node,
  type NodeProps,
  type OnConnectEnd,
  type OnNodesChange,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { DtIcon } from "../DtIcon";
import {
  classifyChainLink,
  familyIcon,
  LAYOUT_PITCH,
  layoutFor,
  type FlowEdgeModel,
  type FlowNodeModel,
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
  onSelect: (index: number) => void;
  onReorder: (fromIndex: number, toIndex: number) => void;
  onInvalidLink: (reason: string) => void;
}

function stepIndex(id: string | null | undefined): number {
  const index = Number(String(id ?? "").replace("step-", ""));
  return Number.isInteger(index) ? index : -1;
}

/** xyflow surface. Loaded only when the canvas flag is on. */
export default function TransformFlow({
  nodes,
  edges,
  pending,
  onSelect,
  onReorder,
  onInvalidLink,
}: TransformFlowProps) {
  const built = useMemo(() => buildNodes(nodes, pending), [nodes, pending]);
  const [flowNodes, setFlowNodes] = useState(built);
  useEffect(() => {
    setFlowNodes(built);
  }, [built]);

  const flowEdges: Edge[] = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: "smoothstep",
  }));

  const onNodesChange: OnNodesChange<Node<StudioNodeData>> = (changes) => {
    setFlowNodes((current) => applyNodeChanges(changes, current));
  };

  const linkOk = (connection: Connection | Edge) => (
    classifyChainLink(nodes.length, stepIndex(connection.source), stepIndex(connection.target)).ok
  );

  const onConnectEnd: OnConnectEnd = (_event, state) => {
    if (state.isValid !== false || !state.fromNode || !state.toNode) return;
    const verdict = classifyChainLink(
      nodes.length,
      stepIndex(state.fromNode.id),
      stepIndex(state.toNode.id),
    );
    if (!verdict.ok) onInvalidLink(verdict.reason);
  };

  return (
    <ReactFlow
      nodes={flowNodes}
      edges={flowEdges}
      nodeTypes={nodeTypes}
      onNodesChange={onNodesChange}
      fitView
      fitViewOptions={{ duration: 0, padding: 0.2 }}
      proOptions={{ hideAttribution: true }}
      nodesDraggable
      isValidConnection={linkOk}
      onConnectEnd={onConnectEnd}
      onNodeClick={(_, node) => {
        if (node.id === "pending") return;
        onSelect(node.data.index);
      }}
      onNodeDragStop={(_, node) => {
        if (node.id === "pending" || node.data.disabled) return;
        const from = node.data.index;
        const to = Math.max(0, Math.min(nodes.length - 1, Math.round(node.position.y / PITCH)));
        if (to !== from) onReorder(from, to);
      }}
      deleteKeyCode={null}
    >
      <Background gap={16} color="var(--df-border)" />
      <Controls showInteractive={false} />
      <MiniMap pannable={false} zoomable={false} />
    </ReactFlow>
  );
}

function buildNodes(nodes: FlowNodeModel[], pending: PendingStepModel | null): Node<StudioNodeData>[] {
  const shift = pending ? 1 : 0;
  const positions = layoutFor(nodes.length + shift);
  const flowNodes: Node<StudioNodeData>[] = nodes.map((node) => {
    const slot = pending && node.index >= pending.atIndex ? node.index + 1 : node.index;
    return {
      id: node.id,
      type: "studio",
      position: positions[slot] ?? { x: 48, y: slot * PITCH },
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
    flowNodes.push({
      id: "pending",
      type: "studio",
      position: positions[slot] ?? { x: 48, y: slot * PITCH },
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
    <div className={className}>
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
