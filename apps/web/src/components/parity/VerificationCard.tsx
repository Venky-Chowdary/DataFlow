/**
 * The verification that runs after the write (Parity at Scale): its own state,
 * never the write's status. While it runs the Trust Report reads
 * "Verification running" and sign-off waits; when it ends each table says what
 * the comparison of every row found.
 */

import { CircleAlert, CircleCheck, Hourglass } from "lucide-react";
import { useId } from "react";
import { badgeClass, formatDateTime, type ParityVerification } from "../../lib/parity";
import {
  plural,
  verificationHeadline,
  verificationProgress,
  verificationTableState,
} from "../../lib/parityScale";

export interface VerificationCardProps {
  verification: ParityVerification;
  /** The automatic re-read stopped at its limit while verification still ran. */
  pollStopped?: boolean;
}

function StateIcon({ state }: { state: string | null }) {
  if (state === "running") return <Hourglass size={16} strokeWidth={2} aria-hidden="true" />;
  if (state === "passed") return <CircleCheck size={16} strokeWidth={2} aria-hidden="true" />;
  return <CircleAlert size={16} strokeWidth={2} aria-hidden="true" />;
}

export function VerificationCard({ verification, pollStopped = false }: VerificationCardProps) {
  const titleId = useId();
  const running = verification.state === "running";
  const tone = running ? "is-info" : verification.state === "passed" ? "is-ok" : "is-warn";
  return (
    <section className="df2-parity-card df2-pscale-verify" aria-labelledby={titleId} aria-busy={running || undefined}>
      <div className={`df2-pscale-banner ${tone}`} role="status">
        <StateIcon state={verification.state} />
        <div>
          <h3 id={titleId} className="df2-parity-h4">
            {running ? "Verification running" : "Verification after the write"}
          </h3>
          <p className="df2-pscale-banner-body">
            {verificationHeadline(verification)}
            {running && " Sign-off and cutover wait until it finishes; the write itself is complete."}
          </p>
        </div>
      </div>
      <ul className="df2-pscale-verify-list">
        {verification.tables.map((table) => {
          const state = verificationTableState(table.status);
          const progress = table.status === "running" ? verificationProgress(table) : null;
          const found = Number((table.progress as { differences_so_far?: number })?.differences_so_far ?? 0);
          return (
            <li key={table.name} className="df2-pscale-verify-row">
              <span className="df2-mono df2-pscale-verify-name">{table.name}</span>
              <span className={badgeClass(state.tone)}>{state.label}</span>
              {progress ? (
                <span className="df2-pscale-progress">
                  <span
                    className="df2-pscale-progress-track"
                    role="progressbar"
                    aria-label={`Verification of ${table.name}`}
                    aria-valuemin={0}
                    aria-valuemax={100}
                    aria-valuenow={progress.pct}
                    aria-valuetext={`${progress.done.toLocaleString("en-US")} of ${plural(progress.total, "step")} so far`}
                  >
                    <span className="df2-pscale-progress-fill" style={{ width: `${progress.pct}%` }} />
                  </span>
                  <span className="df2-parity-muted df2-parity-num">
                    {progress.done.toLocaleString("en-US")} of {plural(progress.total, "step")}
                    {found > 0 && ` · ${plural(found, "difference")} so far`}
                  </span>
                </span>
              ) : table.status === "running" ? (
                <span className="df2-parity-muted">Starting…</span>
              ) : (
                table.error && <span className="df2-parity-muted">{table.error}</span>
              )}
            </li>
          );
        })}
      </ul>
      {!running && verification.finished_at && (
        <p className="df2-parity-muted">Finished {formatDateTime(verification.finished_at)}</p>
      )}
      {running && pollStopped && (
        <p className="df2-parity-muted" role="status">
          Stopped checking automatically. Refresh the report to see whether verification has finished.
        </p>
      )}
    </section>
  );
}
