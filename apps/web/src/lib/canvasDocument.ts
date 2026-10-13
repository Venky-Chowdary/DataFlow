/**
 * The canvas document is an ordered recipe plus viewport and selection.
 *
 * Edges mean order. The model is a list, so a cycle cannot be stored.
 * Post-load operations are plan cards and are not steps.
 */

import {
  describeStep,
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
  family: string;
  dimmed: boolean;
  disabled: boolean;
  changesRowCount: boolean;
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
): { nodes: FlowNodeModel[]; edges: FlowEdgeModel[] } {
  const byOp = new Map(operations.map((operation) => [operation.op, operation]));
  const nodes = steps.map((step, index) => {
    const operation = byOp.get(step.op);
    const otherTable = Boolean(activeTable && step.source_table && step.source_table !== activeTable);
    return {
      id: stepNodeId(index),
      index,
      op: step.op,
      title: describeStep(step, operation),
      family: operation?.family || "cleanse",
      dimmed: otherTable,
      disabled: syncRefused || step.enabled === false || otherTable,
      changesRowCount: Boolean(operation?.active),
    };
  });
  const edges: FlowEdgeModel[] = [];
  for (let index = 0; index < nodes.length - 1; index += 1) {
    edges.push({
      id: `order-${index}`,
      source: nodes[index].id,
      target: nodes[index + 1].id,
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

/** Vertical pitch. Taller than a node so an invalid reason cannot cover the next port. */
export const LAYOUT_PITCH = 148;

/** Where a palette drop lands in the chain, from the stage-local Y. */
export function insertIndexForDrop(stepCount: number, localY: number, pitch = LAYOUT_PITCH): number {
  if (stepCount <= 0) return 0;
  const index = Math.round(Math.max(0, localY) / pitch);
  return Math.max(0, Math.min(stepCount, index));
}

/** Node box the chain layout and the edge-hit test share. */
export const NODE_WIDTH = 200;
export const NODE_BODY = 88;

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
    const above = positions[index];
    const below = positions[index + 1];
    const gapTop = above.y + NODE_BODY;
    const gapBottom = below.y;
    if (gapBottom <= gapTop) continue;
    const left = Math.min(above.x, below.x) - 32;
    const right = Math.max(above.x, below.x) + NODE_WIDTH + 32;
    if (point.y >= gapTop && point.y <= gapBottom && point.x >= left && point.x <= right) {
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
    if (point.y > position.y + NODE_BODY / 2) index += 1;
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
  return Array.from({ length: count }, (_, index) => ({ x: 48, y: index * LAYOUT_PITCH }));
}
