import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { computeJobTrustScore, type JobTrustScore } from "../../lib/jobTrustScore";
import type { JobProgress, TransferJob } from "../../lib/types";

interface JobTrustScoreCardProps {
  job: TransferJob | JobProgress | Record<string, unknown>;
  className?: string;
  compact?: boolean;
  onOpenQuarantine?: () => void;
  onOpenValidate?: () => void;
  onOpenMap?: () => void;
  onResume?: () => void;
  onOpenPipeline?: () => void;
}

function toneClass(tone: string): string {
  if (tone === "ok") return "is-ok";
  if (tone === "warn") return "is-warn";
  if (tone === "danger") return "is-danger";
  return "is-muted";
}

/**
 * Composite trust posture: completeness · quarantine · Gate-8 · freshness.
 * Honesty: not a certificate of exactly-once delivery.
 */
export function JobTrustScoreCard({
  job,
  className = "",
  compact = false,
  onOpenQuarantine,
  onOpenValidate,
  onOpenMap,
  onResume,
  onOpenPipeline,
}: JobTrustScoreCardProps) {
  const trust: JobTrustScore = computeJobTrustScore(job as Parameters<typeof computeJobTrustScore>[0]);
  if (!trust || Number.isNaN(trust.score)) return null;

  const action = trust.next_action;
  const cta =
    action.code === "quarantine" && onOpenQuarantine
      ? { label: "Open quarantine", onClick: onOpenQuarantine }
      : action.code === "reconcile" && onOpenValidate
        ? { label: "Open Validate", onClick: onOpenValidate }
        : action.code === "map" && onOpenMap
          ? { label: "Back to Map", onClick: onOpenMap }
          : action.code === "resume" && onResume
            ? { label: "Resume", onClick: onResume }
            : action.code === "freshness" && onOpenPipeline
              ? { label: "Open pipeline", onClick: onOpenPipeline }
              : action.code === "lease" && onResume
                ? { label: "Resolve lease / Resume", onClick: onResume }
                : null;

  return (
    <section
      className={`df2-trust-score ${toneClass(trust.tone)} ${compact ? "is-compact" : ""} ${className}`.trim()}
      aria-label={`Job trust score ${trust.score}`}
    >
      <div className="df2-trust-score-head">
        <div className="df2-trust-score-ring" aria-hidden>
          <strong>{trust.score}</strong>
          <span>{trust.grade}</span>
        </div>
        <div>
          <h3>Trust score</h3>
          <p>
            Composite of completeness, quarantine, Gate-8 reconcile, and CDC freshness
            ({trust.confidence} confidence). Not exactly-once proof.
          </p>
        </div>
      </div>
      {!compact && (
        <ul className="df2-trust-score-factors">
          {trust.factors.map((f) => (
            <li key={f.id}>
              <span>{f.label}</span>
              <strong>{f.score == null ? "—" : f.score}</strong>
              <em>{f.note}</em>
            </li>
          ))}
        </ul>
      )}
      <div className="df2-trust-score-next">
        <DtIcon name={action.code === "ok" ? "check" : "alert"} size={14} />
        <div>
          <strong>Next step · {action.label}</strong>
          <span>{action.detail}</span>
        </div>
        {cta && (
          <Button size="sm" variant={action.code === "ok" ? "ghost" : "secondary"} onClick={cta.onClick}>
            {cta.label}
          </Button>
        )}
      </div>
    </section>
  );
}
