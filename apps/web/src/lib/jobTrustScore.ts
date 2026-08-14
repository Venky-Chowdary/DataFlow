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
  snapshot_mode?: string | null;
  snapshot_plan?: { snapshot_mode?: string | null } | null;
  source_ha_role?: string | null;
  trust?: JobTrustScore | null;
  trust_score?: number | null;
  quarantine_closure?: {
    verdict?: string | null;
    open_count?: number | null;
    promoted_count?: number | null;
  } | null;
};
function num(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export const CDC_GAP_ERROR_CODES = [
  "cdc_cursor_gap",
  "cdc_lsn_gap",
  "cdc_scn_gap",
  "cdc_binlog_gap",
  "cdc_slot_gap",
  "cdc_ct_gap",
  "cdc_oplog_gap",
] as const;

export function isCdcGapErrorCode(code?: string | null): boolean {
  return Boolean(code && (CDC_GAP_ERROR_CODES as readonly string[]).includes(String(code)));
}

export function snapshotModeRecoversGap(mode?: string | null): boolean {
  const m = String(mode || "").trim().toLowerCase().replace(/-/g, "_");
  return m === "when_needed" || m === "always" || m === "initial_only";
}

export function cursorGapNextAction(snapshotMode?: string | null): {
  code: string;
  label: string;
  detail: string;
} {
  const mode = String(snapshotMode || "").trim().toLowerCase().replace(/-/g, "_");
  if (snapshotModeRecoversGap(mode)) {
    return {
      code: "cursor_gap",
      label: "Resume — engine will snapshot",
      detail:
        "Purged-window events are gone. Resume re-upserts current source keys, then streams from the new tip. Not continuous CDC. Not migration_proven.",
    };
  }
  if (mode === "never") {
    return {
      code: "cursor_gap",
      label: "Set snapshot when_needed",
      detail:
        "snapshot_mode=never forbids a recovery snapshot. Change the mode, then Resume. Purged-window events are gone.",
    };
  }
  return {
    code: "cursor_gap",
    label: "Reset CDC watermark",
    detail:
      "snapshot_mode=initial will not snapshot again. Reset the cursor or set when_needed, then re-run. Purged-window events are gone.",
  };
}

function gradeOf(score: number): string {
  if (score >= 90) return "A";
  if (score >= 80) return "B";
  if (score >= 70) return "C";
  if (score >= 55) return "D";
  return "F";
}

/** Matches Gate-8 ``isGate8AppendDelta`` — dest-before delta, not whole-table hashes. */
function isAppendDeltaProof(recon: Record<string, unknown> | null | undefined): boolean {
  if (!recon) return false;
  if (String(recon.checksum_scope || "").toLowerCase() === "whole_table_not_comparable") return true;
  const coverage = String(recon.assurance_level || recon.coverage || "").toLowerCase();
  const phase = String(recon.phase || "").toLowerCase();
  const src = String(recon.source_checksum || "");
  const tgt = String(recon.target_checksum || "");
  return (
    (coverage === "row_count" || phase.includes("post_write_row_count"))
    && Boolean(src)
    && Boolean(tgt)
    && src !== tgt
    && recon.checksum_match !== true
  );
}

export function computeJobTrustScore(job: TrustJobInput | null | undefined): JobTrustScore {
  if (job?.trust && typeof job.trust.score === "number") {
    return job.trust;
  }

  const status = String(job?.status || "").toLowerCase();
  const processed = num(job?.records_processed);
  const historicalRejected = (() => {
    let r = num(job?.rejected_rows);
    if (r <= 0) r = num(job?.destination_summary?.rejected_rows);
    return r;
  })();
  const closure = (job?.quarantine_closure && typeof job.quarantine_closure === "object"
    ? job.quarantine_closure
    : (job?.destination_summary?.quarantine_closure as TrustJobInput["quarantine_closure"])) || null;
  const closureVerdict = String(closure?.verdict || "");
  let rejected = historicalRejected;
  if (closure && closure.open_count != null && Number.isFinite(Number(closure.open_count))) {
    rejected = num(closure.open_count);
  }
  let coerced = num(job?.coerced_null_rows);
  if (coerced <= 0) coerced = num(job?.destination_summary?.coerced_null_rows);
  const recon = (job?.reconciliation || null) as Record<string, unknown> | null;
  const lag = job?.cdc_lag_seconds;
  const leaseConflict = Boolean(job?.cdc_lease_conflict);
  const cursorGap =
    Boolean(job?.cdc_cursor_gap)
    || isCdcGapErrorCode(job?.error_code);
  const snapshotMode =
    String(job?.snapshot_mode || job?.snapshot_plan?.snapshot_mode || "").trim();
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
    // Terminal success without Gate-8 is not a perfect completeness score.
    outcome = recon ? 100 : 82;
    outcomeNote = recon
      ? "Terminal success."
      : "Terminal success — Gate-8 reconcile not on this job yet.";
  }
  factors.push({ id: "completeness", label: "Completeness", score: outcome, weight: 0.25, note: outcomeNote });

  const denom = Math.max(processed, rejected, historicalRejected, 1);
  const rejectRate = Math.min(1, rejected / denom);
  const quarantineScore = rejected <= 0 ? 100 : Math.max(0, 100 - rejectRate * 400);
  const quarantineNote =
    rejected <= 0 && closureVerdict === "closed"
      ? `${historicalRejected.toLocaleString()} original hold-out(s) remediations landed (child Gate-8) — not a rewrite of the parent checksum.`
      : rejected <= 0
        ? "No quarantined rows."
        : `${rejected.toLocaleString()} open quarantined (${(rejectRate * 100).toFixed(1)}% of processed).`;
  factors.push({
    id: "quarantine",
    label: "Quarantine",
    score: quarantineScore,
    weight: 0.25,
    note: quarantineNote,
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
    const phase = String(recon.phase || "").toLowerCase();
    const msg = String(recon.message || "").toLowerCase();
    const assurance = String(recon.assurance_level || recon.coverage || "").toLowerCase();
    const preview = recon.preview === true || recon.post_write_pending === true;
    const unproven =
      recon.unproven === true
      || recon.skipped_readback === true
      || phase.includes("skipped")
      || (assurance === "none" && /file\/object|file export|unproven/i.test(msg));
    const writerAck =
      assurance === "writer_ack"
      || phase.includes("writer_ack")
      || /verified by writer|read-back verifier not available/i.test(msg)
      || (passed === true && Boolean(recon.source_checksum) && !recon.target_checksum && !unproven);
    const sample =
      assurance === "sample"
      || phase.includes("sample_verified")
      || /sample-verified|sample verified/i.test(msg);
    const appendDelta = isAppendDeltaProof(recon);
    const fullChecksum =
      assurance === "full_checksum"
      || (
        passed === true
        && Boolean(recon.source_checksum)
        && Boolean(recon.target_checksum)
        && String(recon.source_checksum) === String(recon.target_checksum)
        && !writerAck
        && !sample
        && !unproven
        && !appendDelta
      );
    const preWrite =
      preview
      || phase.includes("pre_write")
      || phase.includes("post_write_pending")
      || /writer checksum|still pending|may still be loading|compare still pending/i.test(msg);

    let reconScore = 70;
    const fidelity = recon.row_fidelity_score;
    if (typeof fidelity === "number" && Number.isFinite(fidelity)) {
      reconScore = fidelity <= 1 ? fidelity * 100 : Math.max(0, Math.min(100, fidelity));
    } else if (passed === false) {
      reconScore = 18;
    } else if (unproven || preWrite) {
      reconScore = 45;
    } else if (writerAck) {
      reconScore = 58;
    } else if (sample) {
      reconScore = 68;
    } else if (appendDelta) {
      reconScore = 70;
    } else if (fullChecksum) {
      reconScore = 100;
    } else if (passed === true) {
      // passed without full_checksum assurance — do not invent Verified.
      reconScore = 70;
    }

    const missing = num(recon.missing_key_count);
    const extra = num(recon.extra_key_count);
    let rNote: string;
    if (passed === false) {
      rNote = String(recon.message || "Gate-8 reconcile failed.");
    } else if (unproven) {
      rNote = "Gate-8 cell fidelity unproven (file/object export or skipped read-back) — operational pass only.";
    } else if (preWrite) {
      rNote = "Pre-write / pending Gate-8 — not independent post-write proof.";
    } else if (writerAck) {
      rNote = "Writer acknowledgment only — independent read-back not captured.";
    } else if (sample) {
      rNote = "Sample-verified Gate-8 — not full independent checksum.";
    } else if (appendDelta) {
      rNote = "Gate-8 append delta verified — whole-table checksums are not comparable; per-cell fidelity is not proven.";
    } else if (missing || extra) {
      rNote = `Keys missing=${missing} extra=${extra}.`;
    } else if (fullChecksum) {
      rNote = "Gate-8 full checksum reconcile passed.";
    } else if (passed === true) {
      rNote = "Gate-8 passed without full_checksum assurance — incomplete proof.";
    } else {
      rNote = "Gate-8 reconcile pending.";
    }
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
  // Never grade-A without independent full checksum proof (mirrors job_trust.py).
  const assurance = String(recon?.assurance_level || recon?.coverage || "").toLowerCase();
  const phase = String(recon?.phase || "").toLowerCase();
  const fullProof =
    assurance === "full_checksum"
    || (
      recon?.passed === true
      && Boolean(recon?.source_checksum)
      && Boolean(recon?.target_checksum)
      && String(recon?.source_checksum) === String(recon?.target_checksum)
      && !phase.includes("writer_ack")
      && !phase.includes("sample")
      && !phase.includes("skipped")
      && recon?.unproven !== true
      && recon?.skipped_readback !== true
      && assurance !== "row_count"
      && !phase.includes("row_count")
      && String(recon?.checksum_scope || "") !== "whole_table_not_comparable"
    );
  if (!recon) {
    score = Math.min(score, 84);
  } else if (!fullProof) {
    score = Math.min(score, 89);
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
    next_action = cursorGapNextAction(snapshotMode);
  } else if (leaseConflict) {
    next_action = { code: "lease", label: "Resolve CDC lease", detail: "Force-release or stop the holder, then Resume." };
  } else if (status === "failed" || status === "error") {
    next_action = { code: "resume", label: "Fix failure then Resume", detail: "Use the failure hint and event log before retrying." };
  } else if (present.length) {
    const weakest = present.reduce((a, b) => ((a.score as number) <= (b.score as number) ? a : b));
    if ((weakest.id === "quarantine" || weakest.id === "completeness") && closureVerdict === "closed" && rejected <= 0) {
      next_action = {
        code: "quarantine_closed",
        label: "Quarantine ledger closed",
        detail: "Remediations landed with child Gate-8. Parent checksum is historical — not migration_proven.",
      };
    } else if (weakest.id === "quarantine" || (rejected > 0 && (weakest.score as number) < 90)) {
      next_action = { code: "quarantine", label: "Review quarantine", detail: "Replay or export remaining open rows — nothing was silently dropped." };
    } else if (weakest.id === "reconcile") {
      next_action = recon?.passed === true && isAppendDeltaProof(recon)
        ? {
            code: "append_delta",
            label: "Append delta closed — not a dest replace",
            detail: "Dest grew by this run. Overwrite to replace existing rows, or add a PK and upsert.",
          }
        : { code: "reconcile", label: "Investigate Gate-8", detail: "Export proof JSON or re-run Validate after fixing drift." };
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
