import { useId, useState } from "react";
import { DtIcon } from "../DtIcon";
import type { MapStepCounts } from "../../lib/columnWorkbench";

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

/** The Map header's one status line, from the same counts as the filter and Review. */
export function mapStatusLine(counts: MapStepCounts): string {
  const n = (v: number) => v.toLocaleString("en-US");
  const parts = [`${n(counts.mapped)} of ${n(counts.total)} mapped`];
  if (counts.attention > 0) parts.push(`${n(counts.attention)} ${counts.attention === 1 ? "needs" : "need"} your review`);
  if (counts.destinationIssues > 0) {
    parts.push(`${n(counts.destinationIssues)} destination column ${counts.destinationIssues === 1 ? "issue" : "issues"}`);
  }
  if (counts.attention === 0 && counts.approvable > 0) parts.push(`${n(counts.approvable)} ready to approve`);
  if (counts.attention === 0 && counts.approvable === 0 && counts.destinationIssues === 0 && counts.total > 0) {
    parts.push("all approved");
  }
  return parts.join(" · ");
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
