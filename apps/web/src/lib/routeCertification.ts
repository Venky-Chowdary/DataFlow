/** Route certification evidence from the proof ledger.
 *
 * `sold` on a PRODUCTION_SKU row means the route is available on this host.
 * Certification is separate: it comes from test results graded per sync mode
 * by `scripts/route_certification_report.py`. Only `verified` is certified.
 */

export type RouteCertificationStatus =
  | "verified"
  | "refused"
  | "partial"
  | "failed"
  | "skipped"
  | "unverified";

export type RouteCertificationMode = {
  sync_mode: string;
  status: RouteCertificationStatus;
  checks_missing: string[];
  cases_missing: string[];
  skip_reasons: string[];
  refusal_reason: string | null;
};

export type RouteCertification = {
  status: RouteCertificationStatus;
  modes: RouteCertificationMode[];
  generated_at: string | null;
};

const LABELS: Record<RouteCertificationStatus, string> = {
  verified: "Verified",
  refused: "Refused (proven)",
  partial: "Partly proven",
  failed: "Failed",
  skipped: "Skipped",
  unverified: "No evidence",
};

const BADGES: Record<RouteCertificationStatus, string> = {
  verified: "df2-badge df2-badge-success",
  refused: "df2-badge df2-badge-muted",
  partial: "df2-badge df2-badge-warning",
  failed: "df2-badge df2-badge-error",
  skipped: "df2-badge df2-badge-muted",
  unverified: "df2-badge df2-badge-muted",
};

const MODE_LABELS: Record<string, string> = {
  full_refresh_overwrite: "Overwrite",
  full_refresh_append: "Append",
  upsert: "Upsert",
};

const CHECK_LABELS: Record<string, string> = {
  preflight: "preflight",
  write: "write",
  independent_readback: "independent read-back",
  rerun: "rerun",
  zero_writes: "zero-write proof",
};

function knownStatus(raw: unknown): RouteCertificationStatus {
  return typeof raw === "string" && raw in LABELS ? (raw as RouteCertificationStatus) : "unverified";
}

export function certificationLabel(status: unknown): string {
  return LABELS[knownStatus(status)];
}

export function certificationBadgeClass(status: unknown): string {
  return BADGES[knownStatus(status)];
}

export function syncModeLabel(mode: string): string {
  return MODE_LABELS[mode] ?? mode;
}

/** One line per sync mode saying what is proven and what is not. */
export function certificationModeDetail(mode: RouteCertificationMode): string {
  const name = syncModeLabel(mode.sync_mode);
  const status = knownStatus(mode.status);
  if (status === "verified") return `${name}: verified`;
  if (status === "refused") return `${name}: refused — ${mode.refusal_reason ?? "reason not recorded"}`;
  if (status === "failed") return `${name}: a test failed`;
  if (status === "skipped") {
    const why = mode.skip_reasons.join("; ");
    return `${name}: skipped${why ? ` — ${why}` : ""}`;
  }
  if (status === "partial") {
    const missing = mode.checks_missing.map((c) => CHECK_LABELS[c] ?? c).join(", ");
    return `${name}: not proven — ${missing}`;
  }
  return `${name}: no test evidence`;
}

/** Normalize a ledger row that may predate the certification field. */
export function routeCertificationOf(row: { certification?: RouteCertification | null }): RouteCertification {
  const cert = row.certification;
  if (!cert || !Array.isArray(cert.modes)) {
    return { status: "unverified", modes: [], generated_at: null };
  }
  return { ...cert, status: knownStatus(cert.status) };
}
