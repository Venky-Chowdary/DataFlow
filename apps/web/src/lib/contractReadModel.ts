/**
 * Contract status / version / columns as the Contracts list and drawer show them.
 *
 * The server's read model (apps/api/services/contract_read_model.py) is the one
 * source of truth. QA saw "Draft v1 (Signed 0)" stay on screen after a 200 Sign:
 * the page put the copy saved in sessionStorage at Save-as-contract time in
 * front of the server row on every load, so a stale Draft v1 beat Signed v3.
 */
import type { DataContractSummary } from "./api";

const STATUS_WORDS: Record<string, string> = {
  signed: "Signed",
  draft: "Draft",
  broken: "Broken",
  deprecated: "Deprecated",
};

export function contractStatus(c: Pick<DataContractSummary, "status"> | null | undefined): string {
  return String(c?.status || "draft").trim().toLowerCase();
}

/** "Signed v3" — the server's ``status_label`` when present, else derived the same way. */
export function contractStatusLabel(
  c: Pick<DataContractSummary, "status" | "version"> & { status_label?: string },
): string {
  if (c.status_label) return c.status_label;
  const status = contractStatus(c);
  const word = STATUS_WORDS[status] || (status ? status[0].toUpperCase() + status.slice(1) : "Draft");
  return `${word} v${Math.max(Number(c.version) || 1, 1)}`;
}

export interface ContractReconcile {
  rows: DataContractSummary[];
  /** The client copy is no longer needed (the server has that contract). */
  dropOptimistic: boolean;
}

/**
 * Merge the server list with the copy kept from Save-as-contract.
 *
 * The server row always wins once the server lists that id. The client copy is
 * only a placeholder for a just-saved contract the list has not caught up with.
 */
export function reconcileContracts(
  serverRows: DataContractSummary[],
  optimistic: DataContractSummary | null,
): ContractReconcile {
  if (!optimistic?.id) return { rows: serverRows, dropOptimistic: false };
  if (serverRows.some((c) => c.id === optimistic.id)) {
    return { rows: serverRows, dropOptimistic: true };
  }
  return { rows: [optimistic, ...serverRows], dropOptimistic: false };
}

/** Replace one row with a fresher server response (Sign / Deprecate / Reset). */
export function applyContractResponse(
  rows: DataContractSummary[],
  next: DataContractSummary,
): DataContractSummary[] {
  if (!next?.id) return rows;
  const at = rows.findIndex((c) => c.id === next.id);
  if (at < 0) return [next, ...rows];
  const copy = rows.slice();
  copy[at] = next;
  return copy;
}

export function contractStatusCounts(rows: DataContractSummary[]): Record<string, number> {
  const counts: Record<string, number> = { signed: 0, draft: 0, broken: 0, deprecated: 0 };
  for (const c of rows) {
    const s = contractStatus(c);
    counts[s] = (counts[s] || 0) + 1;
  }
  return counts;
}

export interface ContractColumnRow {
  name: string;
  type: string;
  nullable: boolean | null;
}

function text(v: unknown): string {
  return v == null ? "" : String(v).trim();
}

/**
 * Column rows from the contract's bound schema. A stored rule carries
 * ``source_name``/``target_name``/``source_type``/``target_type``; the drawer
 * looked only for ``name`` and showed ``col_0..col_7``.
 */
export function contractColumnRows(c: Pick<DataContractSummary, "columns">): ContractColumnRow[] {
  return (c.columns || []).map((col) => {
    const row = col as Record<string, unknown>;
    const name =
      text(row.name) || text(row.target_name) || text(row.source_name)
      || text(row.column_name) || text(row.target) || text(row.source);
    const type =
      text(row.type) || text(row.target_type) || text(row.source_type)
      || text(row.inferred_type) || text(row.data_type);
    return { name, type, nullable: row.nullable == null ? null : Boolean(row.nullable) };
  });
}
