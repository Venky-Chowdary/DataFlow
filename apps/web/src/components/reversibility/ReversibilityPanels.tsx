/**
 * Where Page C lives: the Validate step of a transfer plan (editable switch,
 * the g24 gate on preflight) and a job's detail (what that run required).
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../ui/Button";
import { EmptyState } from "../ui/EmptyState";
import { LoadingBlock } from "../LoadingState";
import { DtIcon } from "../DtIcon";
import { ReversibilityReportView } from "./ReversibilityReportView";
import { fetchJobReversibility, fetchPlanReversibility, setPlanRequireReversibility } from "../../lib/exitApi";
import { exitErrorMessage } from "../../lib/exitReadiness";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import type { ReversibilityReport } from "../../lib/reversibility";

export { reversibilityFromPreflight } from "../../lib/reversibility";

export interface PlanReversibilityCardProps {
  /** The persisted plan; null before Validate has saved one. */
  planId: string | null;
  /** The report plan preflight returned with its gates, when it did. */
  preflightReport: ReversibilityReport | null;
  /** The Studio's policy value, sent with the plan on every Validate. */
  requireReversibility: boolean;
  onRequireChange: (next: boolean) => void;
  /** Re-run Validate so the gate is applied (or lifted) on this plan's preflight. */
  onRevalidate?: () => void;
}

export function PlanReversibilityCard({ planId, preflightReport, requireReversibility, onRequireChange, onRevalidate }: PlanReversibilityCardProps) {
  const gate = useWriteGate(PERMISSIONS.jobPlan, "change whether this transfer must be reversible");
  const [report, setReport] = useState<ReversibilityReport | null>(preflightReport);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [changing, setChanging] = useState(false);
  const [stale, setStale] = useState(false);
  const planRef = useRef(planId);
  planRef.current = planId;

  // A fresh preflight is the newest answer: it replaces whatever was read before.
  useEffect(() => {
    if (preflightReport) {
      setReport(preflightReport);
      setStale(false);
      setError("");
    }
  }, [preflightReport]);

  const load = useCallback(async () => {
    const forPlan = planId;
    if (!forPlan) return;
    setLoading(true);
    try {
      const next = await fetchPlanReversibility(forPlan);
      if (planRef.current !== forPlan) return;
      setReport(next);
      setError("");
    } catch (err) {
      if (planRef.current === forPlan) setError(exitErrorMessage(err, "Could not analyse the reversibility of this plan."));
    } finally {
      if (planRef.current === forPlan) setLoading(false);
    }
  }, [planId]);

  useEffect(() => {
    if (!preflightReport && planId) void load();
  }, [planId, preflightReport, load]);

  const change = async (next: boolean) => {
    if (!gate.allowed) return;
    onRequireChange(next);
    if (!planId) {
      // Not saved yet: the policy travels with the plan on the next Validate.
      setReport((r) => (r ? { ...r, require_reversibility: next } : r));
      setStale(true);
      return;
    }
    setChanging(true);
    setError("");
    try {
      await setPlanRequireReversibility(planId, next);
      await load();
      setStale(true);
    } catch (err) {
      onRequireChange(!next);
      setError(exitErrorMessage(err, "Could not change the reversibility requirement."));
    } finally {
      setChanging(false);
    }
  };

  if (!report) {
    if (!planId) return null;
    if (loading) return <LoadingBlock size="sm" title="Analysing reversibility" hint="Checking every mapping and transform for information it would lose." />;
    return (
      <EmptyState
        compact
        icon="alert"
        role={error ? "alert" : "status"}
        title="Reversibility not analysed"
        description={error || "Run Validate to analyse whether this transfer can be reversed."}
        action={<Button size="sm" onClick={() => void load()}>Try again</Button>}
      />
    );
  }

  const shown = report.require_reversibility === requireReversibility ? report : { ...report, require_reversibility: requireReversibility };
  return (
    <div className="df2-rev-host">
      <ReversibilityReportView
        report={shown}
        canChangeRequirement={gate.allowed}
        requirementDeniedReason={gate.reason}
        changingRequirement={changing}
        onRequireChange={(next) => void change(next)}
        onRefresh={planId ? () => void load() : undefined}
        refreshing={loading}
        notice={stale ? "Validate has not run with this setting yet. Re-run Validate to apply the reversibility gate to this plan." : undefined}
        error={error || undefined}
      />
      {stale && onRevalidate && (
        <div className="df2-parity-actions">
          <Button size="sm" variant="primary" onClick={onRevalidate} leadingIcon={<DtIcon name="refresh" size={14} />}>
            Re-run Validate
          </Button>
        </div>
      )}
    </div>
  );
}

export function JobReversibilityTab({ jobId }: { jobId: string }) {
  const [report, setReport] = useState<ReversibilityReport | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const jobRef = useRef(jobId);
  jobRef.current = jobId;

  const load = useCallback(async (signal?: AbortSignal) => {
    const forJob = jobId;
    setLoading(true);
    try {
      const next = await fetchJobReversibility(forJob, { signal });
      if (jobRef.current !== forJob) return;
      setReport(next);
      setError("");
    } catch (err) {
      if (signal?.aborted || jobRef.current !== forJob) return;
      setError(exitErrorMessage(err, "Could not analyse the reversibility of this run."));
    } finally {
      if (!signal?.aborted && jobRef.current === forJob) setLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    setReport(null);
    setError("");
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load]);

  if (!report && loading) {
    return <LoadingBlock size="sm" title="Analysing reversibility" hint="Reading this run's mappings and transforms; no rows are read." />;
  }
  if (!report) {
    return (
      <EmptyState
        compact
        icon="alert"
        role="alert"
        title="Could not analyse reversibility"
        description={error || "The analysis is not available. Try again."}
        action={<Button size="sm" onClick={() => void load()}>Try again</Button>}
      />
    );
  }
  return (
    <div className="df2-rev-host">
      <ReversibilityReportView report={report} onRefresh={() => void load()} refreshing={loading} error={error || undefined} />
    </div>
  );
}
