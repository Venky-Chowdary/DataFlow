/**
 * Composite per-job trust score — mirrors apps/api/services/job_trust.py.
 * Prefer server `job.trust` when present; otherwise compute client-side.
 */

export type JobTrustFactor = {
  id: string;
  label: string;
  score: number | null;
  weight: number;
  note: string;
  present?: boolean;
};

export type JobTrustScore = {
  score: number;
  grade: string;
  tone: "ok" | "warn" | "danger" | "muted" | string;
  confidence: "high" | "medium" | "low" | string;
  factors: JobTrustFactor[];
  next_action: { code: string; label: string; detail: string };
  lease_conflict?: boolean;
  cursor_gap?: boolean;
  source_ha_role?: string | null;
};

type TrustJobInput = {
  status?: string | null;
  records_processed?: number | null;
  rejected_rows?: number | null;
  coerced_null_rows?: number | null;
  destination_summary?: Record<string, unknown> | null;
  reconciliation?: Record<string, unknown> | null;
  cdc_lag_seconds?: number | null;
  cdc_lease_conflict?: boolean | null;
  cdc_cursor_gap?: boolean | null;
  error_code?: string | null;
  source_ha_role?: string | null;
  trust?: JobTrustScore | null;
  trust_score?: number | null;
};
function num(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function gradeOf(score: number): string {
  if (score >= 90) return "A";
  if (score >= 80) return "B";
  if (score >= 70) return "C";
  if (score >= 55) return "D";
  return "F";
}

export function computeJobTrustScore(job: TrustJobInput | null | undefined): JobTrustScore {
  if (job?.trust && typeof job.trust.score === "number") {
    return job.trust;
  }

  const status = String(job?.status || "").toLowerCase();
  const processed = num(job?.records_processed);
  let rejected = num(job?.rejected_rows);
  if (rejected <= 0) rejected = num(job?.destination_summary?.rejected_rows);
  let coerced = num(job?.coerced_null_rows);
  if (coerced <= 0) coerced = num(job?.destination_summary?.coerced_null_rows);
  const recon = (job?.reconciliation || null) as Record<string, unknown> | null;
  const lag = job?.cdc_lag_seconds;
  const leaseConflict = Boolean(job?.cdc_lease_conflict);
  const cursorGap =
    Boolean(job?.cdc_cursor_gap)
    || ["cdc_cursor_gap", "cdc_lsn_gap", "cdc_scn_gap"].includes(String(job?.error_code || ""));
  const sourceHaRole = String(job?.source_ha_role || "").trim().toUpperCase() || null;

  const factors: JobTrustFactor[] = [];

  let outcome = 55;
  let outcomeNote = "In progress — score is provisional.";
  if (status === "failed" || status === "error") {
    outcome = processed > 0 ? 12 : 0;
    outcomeNote = "Transfer failed — rows may be partial; fix cause before Resume.";
  } else if (status === "cancelled") {
    outcome = 35;
    outcomeNote = "Cancelled before completion.";
  } else if (status === "completed_with_quarantine") {
    outcome = 78;
    outcomeNote = "Completed with quarantine — not full fidelity.";
  } else if (status === "completed" || status === "success") {
    outcome = 100;
    outcomeNote = "Terminal success.";
  }
  factors.push({ id: "completeness", label: "Completeness", score: outcome, weight: 0.25, note: outcomeNote });

  const denom = Math.max(processed, rejected, 1);
  const rejectRate = Math.min(1, rejected / denom);
  const quarantineScore = rejected <= 0 ? 100 : Math.max(0, 100 - rejectRate * 400);
  factors.push({
    id: "quarantine",
    label: "Quarantine",
    score: quarantineScore,
    weight: 0.25,
    note: rejected <= 0 ? "No quarantined rows." : `${rejected.toLocaleString()} quarantined (${(rejectRate * 100).toFixed(1)}% of processed).`,
  });

  const coerceRate = Math.min(1, coerced / Math.max(processed, 1));
  const coerceScore = coerced > 0 ? Math.max(0, 100 - coerceRate * 200) : 100;
  factors.push({
    id: "coercion",
    label: "Coercion",
    score: coerceScore,
    weight: 0.1,
    note: coerced > 0 ? `${coerced.toLocaleString()} rows with coerced nulls.` : "No coerced-null rows.",
  });

  if (recon) {
    const passed = recon.passed;
    let reconScore = 70;
    const fidelity = recon.row_fidelity_score;
    if (typeof fidelity === "number" && Number.isFinite(fidelity)) {
      reconScore = fidelity <= 1 ? fidelity * 100 : Math.max(0, Math.min(100, fidelity));
    } else if (passed === true) reconScore = 100;
    else if (passed === false) reconScore = 18;
    const missing = num(recon.missing_key_count);
    const extra = num(recon.extra_key_count);
    let rNote = passed === false
      ? String(recon.message || "Gate-8 reconcile failed.")
      : missing || extra
        ? `Keys missing=${missing} extra=${extra}.`
        : "Gate-8 reconcile passed.";
    if (missing || extra) reconScore = Math.min(reconScore, 70);
    factors.push({
      id: "reconcile",
      label: "Reconcile",
      score: reconScore,
      weight: 0.3,
      note: rNote,
      present: true,
    });
  } else {
    factors.push({
      id: "reconcile",
      label: "Reconcile",
      score: null,
      weight: 0.3,
      note: "No Gate-8 report on this job yet.",
      present: false,
    });
  }

  if (lag != null && Number.isFinite(Number(lag)) && Number(lag) >= 0) {
    const lagF = Number(lag);
    let fresh = 100;
    if (lagF > 60 && lagF < 600) fresh = Math.max(0, 100 * (1 - (lagF - 60) / 540));
    else if (lagF >= 600) fresh = 0;
    factors.push({
      id: "freshness",
      label: "Freshness",
      score: fresh,
      weight: 0.1,
      note: `CDC lag ${lagF.toFixed(1)}s (warn 60s).`,
      present: true,
    });
  } else {
    factors.push({
      id: "freshness",
      label: "Freshness",
      score: null,
      weight: 0.1,
      note: "No CDC lag on this job (batch or not reported).",
      present: false,
    });
  }

  const present = factors.filter((f) => f.score != null);
  const weightSum = present.reduce((s, f) => s + f.weight, 0) || 1;
  let score = present.reduce((s, f) => s + (f.score as number) * (f.weight / weightSum), 0);
  score = Math.max(0, Math.min(100, score));
  if (leaseConflict) {
    score = Math.min(score, 35);
  }
  if (cursorGap) {
    score = Math.min(score, 28);
  }

  const covered = 3 + factors.filter((f) => f.present === true).length;
  const confidence = covered >= 5 ? "high" : covered >= 4 ? "medium" : "low";
  const tone =
    !["completed", "completed_with_quarantine", "failed", "cancelled", "success", "error"].includes(status)
      ? "muted"
      : score >= 85
        ? "ok"
        : score >= 60
          ? "warn"
          : "danger";

  let next_action = { code: "ok", label: "Trust posture healthy", detail: "No action required from composite factors." };
  if (cursorGap) {
    next_action = {
      code: "cursor_gap",
      label: "Reset CDC watermark",
      detail: "Clear the cursor, then re-run with snapshot when_needed or initial.",
    };
  } else if (leaseConflict) {
    next_action = { code: "lease", label: "Resolve CDC lease", detail: "Force-release or stop the holder, then Resume." };
  } else if (status === "failed" || status === "error") {
    next_action = { code: "resume", label: "Fix failure then Resume", detail: "Use the failure hint and event log before retrying." };
  } else if (present.length) {
    const weakest = present.reduce((a, b) => ((a.score as number) <= (b.score as number) ? a : b));
    if (weakest.id === "quarantine" || (rejected > 0 && (weakest.score as number) < 90)) {
      next_action = { code: "quarantine", label: "Review quarantine", detail: "Replay or export rejected rows — nothing was silently dropped." };
    } else if (weakest.id === "reconcile") {
      next_action = { code: "reconcile", label: "Investigate Gate-8", detail: "Export proof JSON or re-run Validate after fixing drift." };
    } else if (weakest.id === "freshness") {
      next_action = { code: "freshness", label: "Check CDC freshness", detail: "Open the pipeline — lag may need capacity or lease attention." };
    } else if (weakest.id === "coercion") {
      next_action = { code: "map", label: "Tighten mapping types", detail: "Coerced nulls reduce fidelity — adjust Map / transforms." };
    }
  }

  return {
    score: Math.round(score),
    grade: gradeOf(score),
    tone,
    confidence,
    factors: factors.map((f) => ({
      ...f,
      score: f.score == null ? null : Math.round(f.score),
    })),
    next_action,
    lease_conflict: leaseConflict,
    cursor_gap: cursorGap,
    source_ha_role: sourceHaRole,
  };
}
