import { useMemo } from "react";
import {
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { FlowEdgeModel, FlowNodeModel } from "../../lib/canvasDocument";
import { layoutFor } from "../../lib/canvasDocument";

export interface StudioNodeData extends Record<string, unknown> {
  index: number;
  title: string;
  op: string;
  dimmed: boolean;
  disabled: boolean;
  changesRowCount: boolean;
}

const nodeTypes = { studio: StudioFlowNode };

interface TransformFlowProps {
  nodes: FlowNodeModel[];
  edges: FlowEdgeModel[];
  onSelect: (index: number) => void;
  onDisconnect: (sourceIndex: number) => void;
}

/** xyflow surface. Loaded only when the canvas flag is on. */
export default function TransformFlow({ nodes, edges, onSelect, onDisconnect }: TransformFlowProps) {
  const reduceMotion = useMemo(
    () => typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches,
    [],
  );
  const positions = layoutFor(nodes.length);
  const flowNodes: Node<StudioNodeData>[] = nodes.map((node, index) => ({
    id: node.id,
    type: "studio",
    position: positions[index] ?? { x: 48, y: index * 108 },
    data: {
      index: node.index,
      title: node.title,
      op: node.op,
      dimmed: node.dimmed,
      disabled: node.disabled,
      changesRowCount: node.changesRowCount,
    },
    draggable: false,
    connectable: !node.disabled,
  }));
  const flowEdges: Edge[] = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: "smoothstep",
  }));

  return (
    <ReactFlow
      nodes={flowNodes}
      edges={flowEdges}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ duration: reduceMotion ? 0 : 240, padding: 0.2 }}
      proOptions={{ hideAttribution: true }}
      nodesDraggable={false}
      onNodeClick={(_, node) => onSelect(node.data.index)}
      onEdgesDelete={(deleted) => {
        for (const edge of deleted) {
          const index = Number(String(edge.source).replace("step-", ""));
          if (Number.isInteger(index)) onDisconnect(index);
        }
      }}
      deleteKeyCode={null}
    >
      <Background gap={16} color="var(--df-border)" />
      <Controls showInteractive={false} />
      <MiniMap pannable={false} zoomable={false} />
    </ReactFlow>
  );
}

function StudioFlowNode({ data, selected }: NodeProps<Node<StudioNodeData>>) {
  const className = [
    "df2-canvas-node",
    selected ? "is-selected" : "",
    data.dimmed ? "is-dimmed" : "",
    data.disabled ? "is-disabled" : "",
  ].filter(Boolean).join(" ");
  return (
    <div className={className}>
      <Handle type="target" position={Position.Top} className="df2-canvas-port is-in" aria-label="Previous step" />
      <p className="df2-canvas-node-title">{data.title}</p>
      <p className="df2-canvas-node-op">{data.op}</p>
      <Handle
        type="source"
        position={Position.Bottom}
        className={`df2-canvas-port${data.changesRowCount ? " is-count" : " is-out"}`}
        aria-label={data.changesRowCount ? "Next step, this node changes row count" : "Next step"}
      />
    </div>
  );
}
