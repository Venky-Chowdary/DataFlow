import { columnsBeforeStep } from "./canvasDocument";
import type { ShapeStepWire } from "./shape";

/** Prefix on steps this board writes. The engine still sees rename and keep. */
export const COLUMN_LAYOUT_LABEL = "Column layout";

export interface ColumnLayoutRow {
  source: string;
  name: string;
  kept: boolean;
}

export function isColumnLayoutStep(step: ShapeStepWire): boolean {
  return (step.label ?? "").startsWith(`${COLUMN_LAYOUT_LABEL}:`);
}

/** Columns the board edits: everything the non-layout steps have already produced. */
export function columnsEnteringColumnLayout(
  steps: readonly ShapeStepWire[],
  sourceColumns: readonly string[],
): string[] {
  const prior = steps.filter((step) => step.enabled !== false && !isColumnLayoutStep(step));
  return columnsBeforeStep(prior, prior.length, sourceColumns);
}

function sameRows(left: ColumnLayoutRow[], right: ColumnLayoutRow[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((row, index) => {
    const other = right[index];
    return row.source === other.source && row.name === other.name && row.kept === other.kept;
  });
}

/** Rebuild the board from steps already applied, so a reload is not a second editor. */
export function layoutRowsFromSteps(
  steps: readonly ShapeStepWire[],
  columns: readonly string[],
): ColumnLayoutRow[] {
  let rows: ColumnLayoutRow[] = columns.map((source) => ({ source, name: source, kept: true }));
  for (const step of steps) {
    if (!isColumnLayoutStep(step) || step.enabled === false) continue;
    if (step.op === "drop_column" && step.column) {
      rows = rows.map((row) => (row.source === step.column ? { ...row, kept: false } : row));
    }
    if (step.op === "rename_column" && step.column) {
      const target = String(step.options?.to ?? "").trim();
      rows = rows.map((row) => (
        row.source === step.column && target ? { ...row, name: target } : row
      ));
    }
    if (step.op === "keep_columns") {
      const order = Array.isArray(step.options?.columns)
        ? step.options.columns.map((name) => String(name))
        : [];
      const byName = new Map(rows.map((row) => [row.name, row]));
      const next: ColumnLayoutRow[] = [];
      for (const name of order) {
        const row = byName.get(name);
        if (row) next.push({ ...row, kept: true });
      }
      for (const row of rows) {
        if (!order.includes(row.name)) next.push({ ...row, kept: false });
      }
      rows = next;
    }
  }
  return rows;
}

export function columnLayoutIssue(rows: readonly ColumnLayoutRow[]): string {
  const seen = new Set<string>();
  for (const row of rows) {
    if (!row.kept) continue;
    const name = row.name.trim();
    if (!name) return "A kept column needs a name.";
    if (seen.has(name)) return `'${name}' is used more than once.`;
    seen.add(name);
  }
  if (seen.size === 0) return "Keep at least one column.";
  return "";
}

/**
 * Replace any earlier column-layout steps and append the new ones.
 * Layout runs after the other steps, so a trim still sees the source name.
 */
export function applyColumnLayout(
  steps: readonly ShapeStepWire[],
  rows: readonly ColumnLayoutRow[],
  columns: readonly string[],
): { steps: ShapeStepWire[]; error: string } {
  const error = columnLayoutIssue(rows);
  if (error) return { steps: steps.slice(), error };
  const baseline = layoutRowsFromSteps([], columns);
  const rest = steps.filter((step) => !isColumnLayoutStep(step));
  if (sameRows(rows.slice(), baseline)) return { steps: rest, error: "" };

  const kept = rows.filter((row) => row.kept);
  const layout: ShapeStepWire[] = [];
  for (const row of rows) {
    if (row.kept) continue;
    layout.push({
      op: "drop_column",
      column: row.source,
      label: `${COLUMN_LAYOUT_LABEL}: drop ${row.source}`,
    });
  }
  const slots = kept.map((row) => ({
    source: row.source,
    current: row.source,
    target: row.name.trim(),
  }));
  while (slots.some((slot) => slot.current !== slot.target)) {
    const ready = slots.find((slot) => (
      slot.current !== slot.target
      && !slots.some((other) => other !== slot && other.current === slot.target)
    ));
    if (!ready) {
      return {
        steps: steps.slice(),
        error: "These renames trade names. Rename one column to a temporary name, apply, then finish the swap. A rename never overwrites a column that is still in the row.",
      };
    }
    layout.push({
      op: "rename_column",
      column: ready.current,
      options: { to: ready.target },
      label: `${COLUMN_LAYOUT_LABEL}: rename ${ready.source}`,
    });
    ready.current = ready.target;
  }
  const natural = columns
    .filter((source) => kept.some((row) => row.source === source))
    .map((source) => kept.find((row) => row.source === source)?.name.trim() ?? source);
  const finalOrder = kept.map((row) => row.name.trim());
  const orderChanged = natural.length !== finalOrder.length
    || natural.some((name, index) => name !== finalOrder[index]);
  if (orderChanged) {
    layout.push({
      op: "keep_columns",
      options: { columns: finalOrder },
      label: `${COLUMN_LAYOUT_LABEL}: keep`,
    });
  }
  return { steps: [...rest, ...layout], error: "" };
}
