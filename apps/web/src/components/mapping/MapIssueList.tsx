import { useId, useState } from "react";
import { DtIcon } from "../DtIcon";

export interface MapIssue {
  id: string;
  /** block: Validate cannot start; warn: a decision is still open. */
  tone: "block" | "warn";
  /** One plain line: the column and what is wrong. Never truncated. */
  title: string;
  /** What clears it, read on hover and by screen readers. */
  detail?: string;
  /** The one action that clears (or starts clearing) it. */
  action?: { label: string; onClick: () => void; disabled?: boolean };
}

export interface MapStatusCounts {
  total: number;
  mapped: number;
  /** Rows (and destination columns) that need a decision before Validate. */
  attention: number;
  /** Rows that only wait for Approve. */
  toApprove: number;
}

/** The Map header's one status line: "5 of 7 mapped · 1 needs your review". */
export function mapStatusLine({ total, mapped, attention, toApprove }: MapStatusCounts): string {
  const head = `${mapped.toLocaleString("en-US")} of ${total.toLocaleString("en-US")} mapped`;
  if (attention > 0) return `${head} · ${attention.toLocaleString("en-US")} ${attention === 1 ? "needs" : "need"} your review`;
  if (toApprove > 0) return `${head} · ${toApprove.toLocaleString("en-US")} ready to approve`;
  return total > 0 ? `${head} · all approved` : head;
}

const VISIBLE = 4;

/**
 * Issue triage for the Map step: the items that hold Validate, first, each
 * with one plain line and one action. Replaces the stacked warning banners.
 */
export function MapIssueList({
  issues,
  summary,
  testId = "map-issues",
}: {
  issues: MapIssue[];
  /** e.g. "2 destination column issues block Validate". */
  summary: string;
  testId?: string;
}) {
  const [showAll, setShowAll] = useState(false);
  const listId = useId();
  if (issues.length === 0) return null;
  const ordered = [...issues].sort((a, b) => (a.tone === b.tone ? 0 : a.tone === "block" ? -1 : 1));
  const shown = showAll ? ordered : ordered.slice(0, VISIBLE);
  const blocking = ordered.some((i) => i.tone === "block");
  return (
    <section
      className={`df2-map-issues${blocking ? " is-block" : " is-warn"}`}
      aria-label="Needs attention"
      data-testid={testId}
    >
      <p className="df2-map-issues-summary" role="status">
        <DtIcon name="alert" size={14} />
        <span>{summary}</span>
      </p>
      <ul className="df2-map-issues-list" id={listId}>
        {shown.map((issue) => (
          <li key={issue.id} className={`df2-map-issue is-${issue.tone}`}>
            <span className="df2-map-issue-dot" aria-hidden="true" />
            <span className="df2-map-issue-text">
              <span className="df2-map-issue-title">{issue.title}</span>
              {issue.detail ? <span className="df2-map-issue-detail">{issue.detail}</span> : null}
            </span>
            {issue.action ? (
              <button
                type="button"
                className="df2-btn df2-btn-sm df2-btn-ghost df2-map-issue-action"
                onClick={issue.action.onClick}
                disabled={issue.action.disabled}
              >
                {issue.action.label}
              </button>
            ) : null}
          </li>
        ))}
      </ul>
      {ordered.length > VISIBLE && (
        <button
          type="button"
          className="df2-link-btn df2-map-issues-more"
          aria-expanded={showAll}
          aria-controls={listId}
          onClick={() => setShowAll((v) => !v)}
        >
          {showAll ? "Show fewer" : `Show all ${ordered.length}`}
        </button>
      )}
    </section>
  );
}
