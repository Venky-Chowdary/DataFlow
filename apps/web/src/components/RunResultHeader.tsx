import { useEffect, useId, useState } from "react";
import { DtIcon, type IconName } from "./DtIcon";
import { Button } from "./ui/Button";
import { fetchParityReport } from "../lib/api";
import { hashForScreen } from "../lib/appNavigation";
import { plural } from "../lib/plural";
import type { LedgerCarrier } from "../lib/conservationLedger";
import {
  presentRunStatus,
  presentRunVerification,
  runHeadlineTone,
  runReadStatement,
  type RunVerification,
  type VerificationLoad,
} from "../lib/runResult";

/**
 * Reads the Trust Report verdict for a finished job. The same request the
 * Trust Report tab makes, so the Run step and Jobs can never disagree.
 */
export function useRunVerification(
  jobId: string,
  job: { status?: string | null; reconciliation?: unknown; completed_at?: string | null },
  enabled: boolean,
): { verification: RunVerification; load: VerificationLoad; retry: () => void } {
  const [load, setLoad] = useState<VerificationLoad>({ kind: "loading" });
  const [attempt, setAttempt] = useState(0);
  const hasRecon = Boolean(job.reconciliation);
  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    setLoad({ kind: "loading" });
    fetchParityReport(jobId, { signal: controller.signal })
      .then((report) => {
        if (!controller.signal.aborted) setLoad({ kind: "ready", report });
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return;
        setLoad({ kind: "error", message: err instanceof Error ? err.message : "" });
      });
    return () => controller.abort();
    // A late reconciliation (written just after the terminal status) re-reads the verdict.
  }, [jobId, enabled, attempt, hasRecon, job.status, job.completed_at]);
  return {
    verification: presentRunVerification(load, job),
    load,
    retry: () => setAttempt((n) => n + 1),
  };
}

interface RunResultHeaderProps {
  jobId: string;
  job: LedgerCarrier & {
    status?: string | null;
    reconciliation?: unknown;
    completed_at?: string | null;
    rejected_rows?: number | null;
    total_rows?: number | null;
    message?: string | null;
  };
  /** From useRunVerification, shared with the rest of the Run step. */
  verification: RunVerification;
  load: VerificationLoad;
  onRetry: () => void;
  /** Open Trust Report is the primary action (a finished run); otherwise it is secondary. */
  trustReportPrimary?: boolean;
  /** Sentence under the verdict, e.g. why a run was cancelled. */
  detail?: string;
}

const STATUS_ICON: Record<string, IconName> = {
  danger: "error",
  warning: "warning",
  success: "success",
  neutral: "check",
};

/**
 * One result for a finished run: the job status, one verification verdict
 * (the Trust Report's five words), and read = landed + held + skipped.
 */
export function RunResultHeader({ jobId, job, verification, load, onRetry, trustReportPrimary = true, detail }: RunResultHeaderProps) {
  const titleId = useId();
  const status = presentRunStatus(job.status);
  const tone = runHeadlineTone(status, verification);
  const read = runReadStatement(job);
  const cancelled = status.title === "Transfer cancelled";
  const icon = cancelled ? "x" : STATUS_ICON[tone] ?? "check";
  const trustHref = hashForScreen("jobs", { jobId, panel: "trust-report" });

  return (
    <section className={`df2-run-result is-${tone}`} aria-labelledby={titleId} data-verdict={verification.verdict ?? "none"}>
      <span className="df2-run-result-icon" aria-hidden>
        <DtIcon name={icon} size={16} />
      </span>
      <div className="df2-run-result-body">
        <div className="df2-run-result-head">
          <h2 id={titleId} className="df2-run-result-title">{status.title}</h2>
          <span className={`df2-run-chip is-${verification.tone}`} aria-live="polite">
            Verification: <strong>{verification.word}</strong>
          </span>
        </div>
        {verification.reason && <p className="df2-run-result-reason">{verification.reason}</p>}
        {detail && <p className="df2-run-result-reason">{detail}</p>}
        {read && (
          <p className="df2-run-result-read">
            <span className="df2-sr-only">{read.text}</span>
            <span aria-hidden className="df2-run-result-terms">
              {read.terms.map((term, i) => (
                <span key={term.label} className="df2-run-term-wrap">
                  {i > 0 && <span className="df2-run-op">{read.identity ? (i === 1 ? "=" : "+") : "·"}</span>}
                  <span className={`df2-run-term is-${term.tone}`}>
                    <span className="df2-run-term-label">{term.label}</span>
                    <strong>{term.value}</strong>
                  </span>
                </span>
              ))}
            </span>
            {read.writerReported != null && (
              <span className="df2-run-result-note" aria-hidden>
                The destination was not counted; the writer reported {plural(read.writerReported, "row")}.
              </span>
            )}
          </p>
        )}
      </div>
      <div className="df2-run-result-actions">
        <a className={`df2-btn ${trustReportPrimary ? "df2-btn-primary" : "df2-btn-secondary"}`} href={trustHref}>
          <DtIcon name="verify" size={16} /> Open Trust Report
        </a>
        {load.kind === "error" && verification.verdict === null && (
          <Button variant="ghost" onClick={onRetry} leadingIcon={<DtIcon name="refresh" size={14} />}>
            Retry
          </Button>
        )}
      </div>
    </section>
  );
}
