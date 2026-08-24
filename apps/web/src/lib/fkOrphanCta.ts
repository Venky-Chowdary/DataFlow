/**
 * FK orphan suggested-action SSOT.
 *
 * Engine emits these from ``preflight_rules`` (sample + population).
 * DataFlow cannot invent parent rows and never claims RI proven from sample
 * Validate. The population scan flag is the same opt-in as the honesty checkbox.
 */

export type FkOrphanCtaKind = "run_population_orphan_scan" | "fix_orphans";

export function isFkOrphanCtaKind(kind: string | null | undefined): kind is FkOrphanCtaKind {
  return kind === "run_population_orphan_scan" || kind === "fix_orphans";
}

/** Engine sample / population orphan copy — not schema-FK-unmapped. */
export function isFkOrphanBlockerText(text: string | null | undefined): boolean {
  const t = String(text || "").toLowerCase();
  return t.includes("fk_orphan_in_sample")
    || t.includes("fk_orphan_in_population")
    || t.includes("sample orphan")
    || t.includes("population orphan")
    || t.includes("sample_orphan_probe")
    || t.includes("population_orphan_probe")
    || t.includes("population ri not proven");
}

export interface FkOrphanCtaPlan {
  kind: FkOrphanCtaKind;
  /** Same flag as Validate honesty “Run population orphan scan on next Validate”. */
  enablePopulationScan: boolean;
  /**
   * Re-run Validate immediately with the scan flag.
   * Must not wait on React setState — pass an explicit preflight override.
   */
  rerunValidateWithPopulationScan: boolean;
  /** Open Map. Remap is the in-product door; parent rows are source work. */
  goToMap: boolean;
  focusSource?: string;
  toastTitle: string;
  toastMessage: string;
  toastTone: "info" | "warning";
}

/** Prefer an explicit CTA override so the first click actually scans. */
export function resolvePopulationOrphanScanFlag(
  override: boolean | undefined,
  current: boolean,
): boolean {
  return override ?? current;
}

export function planFkOrphanSuggestedAction(opts: {
  kind: string;
  column?: string | null;
}): FkOrphanCtaPlan | null {
  const kind = String(opts.kind || "");
  const column = String(opts.column || "").trim();

  if (kind === "run_population_orphan_scan") {
    return {
      kind,
      enablePopulationScan: true,
      rerunValidateWithPopulationScan: true,
      goToMap: false,
      toastTitle: "Running population orphan scan",
      toastMessage:
        "Full-table anti-join is the only path to RI proven. Sample Validate never claims referential integrity.",
      toastTone: "info",
    };
  }

  if (kind === "fix_orphans") {
    return {
      kind,
      enablePopulationScan: false,
      rerunValidateWithPopulationScan: false,
      goToMap: true,
      focusSource: column || undefined,
      toastTitle: "Parent rows are source work",
      toastMessage: column
        ? `DataFlow cannot invent missing parents for ${column}. Load parent rows first or remap the FK on Map. Acknowledging FK risk never claims RI proven.`
        : "DataFlow cannot invent missing parent rows. Load parents first or remap the FK on Map. Acknowledging FK risk never claims RI proven.",
      toastTone: "warning",
    };
  }

  return null;
}
