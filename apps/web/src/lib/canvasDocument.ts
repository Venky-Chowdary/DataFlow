/**
 * The canvas document is an ordered recipe plus viewport and selection.
 *
 * Edges mean order. The model is a list, so a cycle cannot be stored.
 * Post-load operations are plan cards and are not steps.
 */

import {
  describeStep,
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
  dimmed: boolean;
  disabled: boolean;
  changesRowCount: boolean;
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
 * Removing the edge between a step and the next one swaps them.
 * The chain is rebuilt from the list, so the graph cannot cycle.
 */
export function reorderOnDisconnect(steps: ShapeStepWire[], sourceIndex: number): ShapeStepWire[] {
  if (sourceIndex < 0 || sourceIndex >= steps.length - 1) return steps;
  const next = steps.slice();
  const [moved] = next.splice(sourceIndex + 1, 1);
  next.splice(sourceIndex, 0, moved);
  return next;
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
  return Array.from({ length: count }, (_, index) => ({ x: 48, y: index * 108 }));
}
