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
  /** Multi-table Gate-8 is the last stream. Open that proof; do not leave the next step as prose. */
  onOpenGate8?: () => void;
  onOpenMap?: () => void;
  onResume?: () => void;
  onOpenPipeline?: () => void;
  /**
   * The Trust Report verdict, when the caller has it: true for proven /
   * explained, false for any other verdict (or not known yet). The verdict is
   * the authority: the card then never contradicts it. Green only when the
   * verdict supports it and the score agrees; otherwise neutral. With a
   * supporting verdict the Gate-8 factor and next step defer to it instead of
   * calling the proof incomplete. Omitted keeps the score's own tone.
   */
  verified?: boolean;
}

/** Next steps about Gate-8 assurance; a supporting verdict already answers them. */
const GATE8_ACTIONS = new Set(["reconcile", "identity_key", "last_stream_checksum"]);

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
  onOpenGate8,
  onOpenMap,
  onResume,
  onOpenPipeline,
  verified,
}: JobTrustScoreCardProps) {
  const trust: JobTrustScore = computeJobTrustScore(job as Parameters<typeof computeJobTrustScore>[0]);
  if (!trust || Number.isNaN(trust.score)) return null;

  const tone = verified === undefined ? trust.tone : verified && trust.tone === "ok" ? "ok" : "muted";
  const factors = verified
    ? trust.factors.map((f) =>
        f.id === "reconcile" && f.present ? { ...f, note: "Verified: the Trust Report covers this run." } : f,
      )
    : trust.factors;
  const action =
    verified && GATE8_ACTIONS.has(trust.next_action.code)
      ? { code: "ok", label: "No action needed", detail: "The Trust Report verdict covers this run." }
      : trust.next_action;
  const cta =
    action.code === "quarantine" && onOpenQuarantine
      ? { label: "Open quarantine", onClick: onOpenQuarantine }
      : action.code === "reconcile" && onOpenValidate
        ? { label: "Open Validate", onClick: onOpenValidate }
        : action.code === "append_delta" && onOpenValidate
          ? { label: "Review Gate-8 proof", onClick: onOpenValidate }
        : action.code === "last_stream_checksum" && onOpenGate8
          ? { label: "Open Gate-8", onClick: onOpenGate8 }
        : action.code === "identity_key" && onOpenMap
          ? { label: "Open Map / Transform", onClick: onOpenMap }
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
      className={`df2-trust-score ${toneClass(tone)} ${compact ? "is-compact" : ""} ${className}`.trim()}
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
          {factors.map((f) => (
            <li key={f.id}>
              <span>{f.label}</span>
              <strong>{f.score == null ? "—" : f.score}</strong>
              <em>{f.note}</em>
            </li>
          ))}
        </ul>
      )}
      <div className="df2-trust-score-next">
        <DtIcon name={action.code === "ok" || action.code === "append_delta" || action.code === "identity_key" ? "check" : "alert"} size={14} />
        <div>
          <strong>Next step · {action.label}</strong>
          <span>{action.detail}</span>
        </div>
        {cta && (
          <Button size="sm" variant={action.code === "ok" || action.code === "append_delta" || action.code === "identity_key" ? "ghost" : "secondary"} onClick={cta.onClick}>
            {cta.label}
          </Button>
        )}
      </div>
    </section>
  );
}
