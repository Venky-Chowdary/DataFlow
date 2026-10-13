/**
 * The canvas document is an ordered recipe plus viewport and selection.
 *
 * Edges mean order. The model is a list, so a cycle cannot be stored.
 * Post-load operations are plan cards and are not steps.
 */

import {
  categoryLabel,
  iconFor,
  nodeStatus,
  shortName,
  streamPorts,
  stripeFor,
  type CanvasPort,
  type NodeStatus,
  type NodeStripe,
} from "./canvasRegistry";
import {
  fieldsFor,
  type ShapeOperation,
  type ShapeStepWire,
} from "./shape";

export interface CanvasViewport {
  x: number;
  y: number;
  zoom: number;
}

export interface CanvasSelection {
  stepIndex: number | null;
}

export interface CanvasDocument {
  steps: ShapeStepWire[];
  viewport: CanvasViewport;
  selection: CanvasSelection;
}

export interface FlowNodeModel {
  id: string;
  index: number;
  op: string;
  title: string;
  summary: string;
  short: string;
  icon: string;
  stripe: NodeStripe;
  status: NodeStatus;
  ports: CanvasPort[];
  family: string;
  category: string;
  dimmed: boolean;
  disabled: boolean;
  changesRowCount: boolean;
  executable: boolean;
}

export interface ChainLink {
  ok: boolean;
  reason: string;
}

/** A drop that is visible on the canvas and not yet in the recipe. */
export interface PendingStepModel {
  op: string;
  title: string;
  family: string;
  reason: string;
  atIndex: number;
  invalid: boolean;
}

export interface FlowEdgeModel {
  id: string;
  source: string;
  target: string;
  sourcePort: string;
  targetPort: string;
}

export interface PlanCardModel {
  op: string;
  caption: "Runs after load as SQL";
  href: "#/transforms";
}

export const DEFAULT_VIEWPORT: CanvasViewport = { x: 0, y: 0, zoom: 1 };

export function stepNodeId(index: number): string {
  return `step-${index}`;
}

export function stepsToFlow(
  steps: ShapeStepWire[],
  operations: ShapeOperation[],
  activeTable: string,
  syncRefused: boolean,
  runningIndex: number | null = null,
): { nodes: FlowNodeModel[]; edges: FlowEdgeModel[] } {
  const byOp = new Map(operations.map((operation) => [operation.op, operation]));
  const nodes = steps.map((step, index) => {
    const operation = byOp.get(step.op);
    const otherTable = Boolean(activeTable && step.source_table && step.source_table !== activeTable);
    const family = operation?.family || "cleanse";
    const disabled = syncRefused || step.enabled === false || otherTable;
    const short = shortName(step.op, operation?.summary);
    const named = typeof step.options?.to === "string" ? step.options.to : "";
    const column = step.column || named;
    return {
      id: stepNodeId(index),
      index,
      op: step.op,
      title: step.label?.trim() || (column ? `${short} · ${column}` : short),
      summary: operation?.summary || step.op,
      short,
      icon: iconFor(step.op, family),
      stripe: stripeFor(family),
      status: nodeStatus({ disabled, dimmed: otherTable, running: runningIndex === index }),
      ports: streamPorts(),
      family,
      category: categoryLabel(family),
      dimmed: otherTable,
      disabled,
      changesRowCount: Boolean(operation?.active),
      executable: true,
    };
  });
  const edges: FlowEdgeModel[] = [];
  for (let index = 0; index < nodes.length - 1; index += 1) {
    edges.push({
      id: `order-${index}`,
      source: nodes[index].id,
      target: nodes[index + 1].id,
      sourcePort: "out",
      targetPort: "in",
    });
  }
  return { nodes, edges };
}

/**
 * A recipe is one chain. The only legal new link is the one the list
 * already implies: output of step i to input of step i+1.
 * Self, backward, and skip links are refused and must not be stored.
 */
export function classifyChainLink(
  stepCount: number,
  sourceIndex: number,
  targetIndex: number,
): ChainLink {
  if (
    !Number.isInteger(sourceIndex)
    || !Number.isInteger(targetIndex)
    || sourceIndex < 0
    || targetIndex < 0
    || sourceIndex >= stepCount
    || targetIndex >= stepCount
  ) {
    return { ok: false, reason: "That port is not on this recipe." };
  }
  if (sourceIndex === targetIndex) {
    return { ok: false, reason: "A step cannot connect to itself." };
  }
  if (targetIndex === sourceIndex + 1) {
    return { ok: true, reason: "" };
  }
  if (targetIndex < sourceIndex) {
    return {
      ok: false,
      reason: "Each step has one input and one output. Data moves forward. Connect an earlier step's output to a later step.",
    };
  }
  return {
    ok: false,
    reason: "Each step has one input and one output. This recipe is one chain. Drag a node to reorder, or drop a step between two steps. A skip would leave the steps in between unconnected.",
  };
}

/** Drag-reorder. The list stays the only order; layout snaps back to it. */
export function reorderByDrag(
  steps: ShapeStepWire[],
  fromIndex: number,
  toIndex: number,
): ShapeStepWire[] {
  if (
    fromIndex === toIndex
    || fromIndex < 0
    || toIndex < 0
    || fromIndex >= steps.length
    || toIndex >= steps.length
  ) {
    return steps;
  }
  const next = steps.slice();
  const [moved] = next.splice(fromIndex, 1);
  next.splice(toIndex, 0, moved);
  return next;
}

/** Horizontal pitch. Wider than a node so the link between steps stays visible. */
export const LAYOUT_PITCH = 320;

/** Where a palette drop lands in the chain, from the flow-axis coordinate. */
export function insertIndexForDrop(stepCount: number, localY: number, pitch = LAYOUT_PITCH): number {
  if (stepCount <= 0) return 0;
  const index = Math.round(Math.max(0, localY) / pitch);
  return Math.max(0, Math.min(stepCount, index));
}

/** Node box the chain layout and the edge-hit test share. Wide enough to read at zoom 1. */
export const NODE_WIDTH = 248;
export const NODE_BODY = 160;

export interface FlowPoint {
  x: number;
  y: number;
}

/**
 * Insert index when the pointer sits in the gap between two consecutive nodes.
 * Null when the point is on a node or in empty space.
 */
export function spliceIndexAtPoint(positions: FlowPoint[], point: FlowPoint): number | null {
  for (let index = 0; index < positions.length - 1; index += 1) {
    const left = positions[index];
    const right = positions[index + 1];
    const gapLeft = left.x + NODE_WIDTH;
    const gapRight = right.x;
    if (gapRight <= gapLeft) continue;
    const top = Math.min(left.y, right.y) - 8;
    const bottom = Math.max(left.y, right.y) + NODE_BODY + 8;
    if (point.x >= gapLeft && point.x <= gapRight && point.y >= top && point.y <= bottom) {
      return index + 1;
    }
  }
  return null;
}

/** Chain index for a flow-space drop. An edge hit wins over a Y sort. */
export function insertIndexAtPoint(stepCount: number, point: FlowPoint, positions: FlowPoint[]): number {
  const spliced = spliceIndexAtPoint(positions, point);
  if (spliced != null) return spliced;
  if (stepCount <= 0 || positions.length === 0) return 0;
  let index = 0;
  for (const position of positions) {
    if (point.x > position.x + NODE_WIDTH / 2) index += 1;
  }
  return Math.max(0, Math.min(stepCount, index));
}

/**
 * Defaults the inspector can open on. A required text or expression stays
 * blank — the engine does not invent a predicate.
 */
export function draftWithDefaults(
  operation: ShapeOperation,
  columns: string[],
): { column: string; options: Record<string, unknown> } {
  const options: Record<string, unknown> = {};
  for (const field of fieldsFor(operation)) {
    if (field.kind === "boolean") options[field.name] = false;
    else if (field.kind === "choice" && field.choices && field.choices.length > 0) {
      options[field.name] = field.choices[0];
    } else if (field.kind === "number" && field.required) options[field.name] = 0;
  }
  return {
    column: operation.needs_column ? (columns[0] ?? "") : "",
    options,
  };
}

/** One icon per family. The palette and the node share this map. */
export function familyIcon(family: string): "edit" | "layers" | "activity" | "code" {
  if (family === "structural") return "layers";
  if (family === "rows") return "activity";
  if (family === "nested") return "code";
  return "edit";
}

export function planCards(postLoadOps: readonly string[]): PlanCardModel[] {
  return postLoadOps.map((op) => ({
    op,
    caption: "Runs after load as SQL" as const,
    href: "#/transforms" as const,
  }));
}

/** A plan card is not a shape step. Drop any op the stream engine refuses. */
export function stepsWithoutPlanOps(
  steps: ShapeStepWire[],
  postLoadOps: readonly string[],
): ShapeStepWire[] {
  const refused = new Set(postLoadOps);
  return steps.filter((step) => !refused.has(step.op));
}

export function layoutFor(count: number): { x: number; y: number }[] {
  return Array.from({ length: count }, (_, index) => ({ x: 24 + index * LAYOUT_PITCH, y: 28 }));
}

export interface GraphNodeRecord {
  id: string;
  op: string;
  ports: CanvasPort[];
  /** Present when this node is also an ordered recipe step. */
  step?: ShapeStepWire;
}

export interface GraphEdgeRecord {
  id: string;
  source: string;
  sourcePort: string;
  target: string;
  targetPort: string;
}

/** A chain (today's recipe) or a DAG (multi-input nodes held beside it). */
export interface CanvasGraphDocument {
  nodes: GraphNodeRecord[];
  edges: GraphEdgeRecord[];
}

/** The ordered recipe as a single-port chain. Edges mean order. */
export function chainDocument(steps: ShapeStepWire[]): CanvasGraphDocument {
  const nodes = steps.map((step, index) => ({
    id: stepNodeId(index),
    op: step.op,
    ports: streamPorts(),
    step,
  }));
  const edges: GraphEdgeRecord[] = [];
  for (let index = 0; index < nodes.length - 1; index += 1) {
    edges.push({
      id: `order-${index}`,
      source: nodes[index].id,
      sourcePort: "out",
      target: nodes[index + 1].id,
      targetPort: "in",
    });
  }
  return { nodes, edges };
}

/** Set-engine nodes only. A chain of row steps hashes as an empty set document. */
export function setDocumentOf(
  document: CanvasGraphDocument,
  setOps: ReadonlySet<string>,
): { nodes: Record<string, unknown>[]; edges?: Record<string, unknown>[] } {
  const nodes = document.nodes.filter((node) => setOps.has(node.op));
  if (nodes.length === 0) return { nodes: [] };
  const ids = new Set(nodes.map((node) => node.id));
  return {
    nodes: nodes.map((node) => ({ id: node.id, op: node.op })),
    edges: document.edges
      .filter((edge) => ids.has(edge.source) && ids.has(edge.target))
      .map((edge) => ({
        id: edge.id,
        source: edge.source,
        sourcePort: edge.sourcePort,
        target: edge.target,
        targetPort: edge.targetPort,
      })),
  };
}

export function documentKind(document: CanvasGraphDocument): "chain" | "dag" {
  const incoming = new Map<string, number>();
  const outgoing = new Map<string, number>();
  for (const node of document.nodes) {
    incoming.set(node.id, 0);
    outgoing.set(node.id, 0);
    const rowIn = node.ports.filter((port) => port.role === "in" && port.kind === "row").length;
    const rowOut = node.ports.filter((port) => port.role === "out" && port.kind === "row").length;
    if (rowIn > 1 || rowOut > 1) return "dag";
  }
  for (const edge of document.edges) {
    incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1);
    outgoing.set(edge.source, (outgoing.get(edge.source) ?? 0) + 1);
  }
  for (const node of document.nodes) {
    if ((incoming.get(node.id) ?? 0) > 1 || (outgoing.get(node.id) ?? 0) > 1) return "dag";
  }
  const order = document.nodes.map((node) => node.id);
  for (const edge of document.edges) {
    if (order.indexOf(edge.target) !== order.indexOf(edge.source) + 1) return "dag";
  }
  return "chain";
}

/**
 * Steps the row engine can run. A non-executable node stays in the document
 * and blocks every node that depends on it, so a later trim is not run as if
 * the join had happened.
 */
export function executableRecipe(
  document: CanvasGraphDocument,
  executableOps: ReadonlySet<string>,
): ShapeStepWire[] {
  const byId = new Map(document.nodes.map((node) => [node.id, node]));
  const incoming = new Map<string, string[]>();
  for (const node of document.nodes) incoming.set(node.id, []);
  for (const edge of document.edges) {
    const list = incoming.get(edge.target);
    if (list) list.push(edge.source);
  }
  const runnable = new Set<string>();
  const pending = document.nodes.map((node) => node.id);
  let moved = true;
  while (moved) {
    moved = false;
    for (const id of pending) {
      if (runnable.has(id)) continue;
      const node = byId.get(id);
      if (!node || !executableOps.has(node.op) || !node.step) continue;
      const preds = incoming.get(id) ?? [];
      if (preds.every((pred) => runnable.has(pred))) {
        runnable.add(id);
        moved = true;
      }
    }
  }
  return document.nodes.filter((node) => runnable.has(node.id)).map((node) => node.step as ShapeStepWire);
}

export interface PortLinkInput {
  nodes: FlowNodeModel[];
  source: string;
  sourcePort: string;
  target: string;
  targetPort: string;
}

/**
 * Chain links stay the only legal link between recipe steps.
 * A multi-port link is legal only when both nodes are executable and the
 * ports exist. Field ports connect to field ports.
 */
export function classifyPortLink(input: PortLinkInput): ChainLink {
  const source = input.nodes.find((node) => node.id === input.source);
  const target = input.nodes.find((node) => node.id === input.target);
  if (!source || !target) return { ok: false, reason: "That port is not on this recipe." };
  if (source.id === target.id) return { ok: false, reason: "A step cannot connect to itself." };
  const sourcePort = source.ports.find((port) => port.id === input.sourcePort);
  const targetPort = target.ports.find((port) => port.id === input.targetPort);
  if (!sourcePort || !targetPort) return { ok: false, reason: "That port is not on this recipe." };
  if (sourcePort.role !== "out" || targetPort.role !== "in") {
    return { ok: false, reason: "Connect an output to an input." };
  }
  if (sourcePort.kind !== targetPort.kind) {
    return { ok: false, reason: "A field port connects to a field port. A row port connects to a row port." };
  }
  if (!source.executable || !target.executable) {
    return { ok: false, reason: "That operation is not executable yet, so it cannot be connected." };
  }
  const chain = source.ports.filter((port) => port.kind === "row").length <= 2
    && target.ports.filter((port) => port.kind === "row").length <= 2
    && source.index >= 0
    && target.index >= 0;
  if (chain && sourcePort.kind === "row") {
    return classifyChainLink(input.nodes.length, source.index, target.index);
  }
  return { ok: true, reason: "" };
}
