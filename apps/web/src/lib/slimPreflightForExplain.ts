/**
 * POST /preflight/explain only needs blockers, coercion, and population-fit.
 * Sending the full 1M-row Validate result spills nginx to client_temp (warn).
 */

const FINDING_CAP = 20;
const EXAMPLE_CAP = 3;

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function slimPreflightForExplain(preflight: unknown): Record<string, unknown> {
  const pf = asRecord(preflight);
  const proof = asRecord(pf.proof_bundle);
  const decision = asRecord(proof.transfer_decision);
  const pop = asRecord(pf.population_fit);
  const findings: Record<string, unknown>[] = [];
  const rawFindings = Array.isArray(pop.findings) ? pop.findings : [];
  for (const raw of rawFindings.slice(0, FINDING_CAP)) {
    const f = asRecord(raw);
    findings.push({
      source: f.source,
      target: f.target,
      target_type: f.target_type,
      unfit_rows: f.unfit_rows,
      example_values: Array.isArray(f.example_values) ? f.example_values.slice(0, EXAMPLE_CAP) : [],
      suggested_target_type: f.suggested_target_type,
      suggested_fix: f.suggested_fix,
      reason: f.reason || f.unfit_reason,
      unfit_reason: f.unfit_reason,
    });
  }
  const coercion = asRecord(pf.coercion_report);
  const coercionCols: Record<string, unknown>[] = [];
  const rawCols = Array.isArray(coercion.columns) ? coercion.columns : [];
  for (const raw of rawCols.slice(0, FINDING_CAP)) {
    const col = asRecord(raw);
    if (col.severity === "ok") continue;
    coercionCols.push({
      source: col.source,
      target: col.target,
      source_type: col.source_type,
      target_type: col.target_type,
      severity: col.severity,
      failed: col.failed ?? 0,
      sentinel_nulls: col.sentinel_nulls ?? 0,
      sampled: col.sampled ?? 0,
      suggested_fix: col.suggested_fix,
    });
  }
  const blockers: Record<string, unknown>[] = [];
  for (const raw of Array.isArray(pf.blockers) ? pf.blockers : []) {
    const b = asRecord(raw);
    const details = asRecord(b.details);
    const issuesDetail = Array.isArray(details.issues_detail) ? details.issues_detail : [];
    blockers.push({
      id: b.id || b.gate,
      gate: b.gate || b.id,
      message: b.message ?? "",
      guidance: b.guidance && typeof b.guidance === "object" ? b.guidance : {},
      details: {
        issues: Array.isArray(details.issues) ? details.issues.slice(0, 10) : [],
        errors: Array.isArray(details.errors) ? details.errors.slice(0, 10) : [],
        issues_detail: issuesDetail.slice(0, 20).map((d) => {
          const row = asRecord(d);
          return { source: row.source, column: row.column };
        }),
      },
    });
  }
  return {
    passed: Boolean(pf.passed),
    run_id: pf.run_id,
    blockers,
    coercion_report: coercionCols.length ? { columns: coercionCols } : {},
    population_fit: Object.keys(pop).length
      ? {
          evidence: pop.evidence,
          rows_scanned: pop.rows_scanned,
          rows_total: pop.rows_total,
          unfit_rows: pop.unfit_rows,
          findings,
        }
      : {},
    proof_bundle: decision.decision ? { transfer_decision: { decision: decision.decision } } : {},
    destination_table_exists: Boolean(pf.destination_table_exists || proof.destination_table_exists),
  };
}
