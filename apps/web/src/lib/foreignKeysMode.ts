/**
 * Foreign keys on a single-table route (QA round 11: "no override" for FK
 * child tables).
 *
 * The engine probes each referenced parent at plan time. The route option
 * `foreign_keys_mode` (destination option; MCP `foreign_keys`) decides what
 * happens next:
 *   carry  — child rows are checked against destination parents that exist and
 *            the constraint is added after the load (default);
 *   guard  — child rows are checked, no constraint is added;
 *   ignore — rows are copied as read: no parent check, no constraint.
 * A parent that is not at the destination is never guarded or carried; Validate
 * says so in G22. Sample orphans against a parent that exists block strict
 * Validate until the operator acknowledges FK risk (`fk_risk_acknowledged`):
 * the orphan rows are then quarantined, not written.
 *
 * The wording mirrors apps/api/services/fk_parent_preflight.py.
 */

export type ForeignKeysMode = "carry" | "guard" | "ignore";

export const FOREIGN_KEYS_MODE_OPTIONS: ReadonlyArray<{
  id: ForeignKeysMode;
  label: string;
  detail: string;
}> = [
  {
    id: "carry",
    label: "Carry and verify (default)",
    detail:
      "Child rows are checked against destination parents that exist, and the foreign key is added after the load. A parent missing at the destination is reported and not carried.",
  },
  {
    id: "guard",
    label: "Guard only",
    detail:
      "Child rows are checked against destination parents; orphans are held out. No foreign key is added to the destination table.",
  },
  {
    id: "ignore",
    label: "Ignore",
    detail:
      "Rows are copied as read. No parent check and no foreign key: orphan child rows can land.",
  },
];

export const FK_ORPHAN_ACK_TEXT =
  "Acknowledge FK risk for this run: child rows whose parent is missing at the destination are quarantined (held out for review), not written. This does not prove referential integrity.";

/** Destination option keys for the request (empty for the default). */
export function foreignKeysDestExtra(mode: ForeignKeysMode | string | undefined): Record<string, string> {
  const m = String(mode ?? "").trim().toLowerCase();
  return m === "guard" || m === "ignore" ? { foreign_keys_mode: m } : {};
}

export function normalizeForeignKeysMode(value: unknown): ForeignKeysMode {
  const m = String(value ?? "").trim().toLowerCase();
  return m === "guard" || m === "ignore" ? m : "carry";
}

type Blocker = { id?: string; message?: string };

/** Validate refused the run on sample orphans against a destination parent. */
export function fkOrphansNeedAcknowledgment(preflight: unknown): boolean {
  const blockers = (preflight as { blockers?: Blocker[] } | null | undefined)?.blockers;
  if (!Array.isArray(blockers)) return false;
  return blockers.some(
    (b) =>
      String(b?.id ?? "") === "constraint_fk" &&
      /reference a parent row the destination does not hold/i.test(String(b?.message ?? "")),
  );
}

export interface RouteNotice {
  id: "fk_parent_absent" | "catalog_probe_budget";
  title: string;
  message: string;
}

/** G22 parent-not-at-destination and catalog-probe-budget warnings, readable. */
export function fkRouteNotices(preflight: unknown): RouteNotice[] {
  const pf = (preflight ?? {}) as {
    gates?: Array<{ id?: string; status?: string; message?: string }>;
    warnings?: unknown[];
  };
  const out: RouteNotice[] = [];
  for (const g of pf.gates ?? []) {
    if (
      g?.id === "g22_dest_referential_integrity" &&
      g.status === "warn" &&
      /not at the destination/i.test(String(g.message ?? ""))
    ) {
      out.push({
        id: "fk_parent_absent",
        title: "Parent table not at destination — foreign key not carried",
        message: String(g.message),
      });
    }
  }
  for (const w of pf.warnings ?? []) {
    if (w && typeof w === "object" && (w as { id?: string }).id === "catalog_probe_budget") {
      out.push({
        id: "catalog_probe_budget",
        title: "Catalog probe stopped at its time budget",
        message: String((w as { message?: string }).message ?? ""),
      });
    }
  }
  return out;
}
