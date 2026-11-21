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
  /** The column(s) it is about, named on the banner's one line. */
  subjects?: string[];
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

/**
 * The banner's one sentence for everything still open, in plain counts with
 * real plurals: destination issues, false-friend pairs (which Approve
 * eligible never clears) and the other rows that need a decision.
 */
export function mapIssueSummary(input: { destinationIssues: number; falseFriends: number; review: number }): string {
  const n = (v: number) => v.toLocaleString("en-US");
  const parts: string[] = [];
  if (input.destinationIssues > 0) {
    parts.push(input.destinationIssues === 1
      ? "1 destination column issue blocks Validate"
      : `${n(input.destinationIssues)} destination column issues block Validate`);
  }
  if (input.falseFriends > 0) {
    parts.push(input.falseFriends === 1
      ? "1 false-friend pair needs confirming"
      : `${n(input.falseFriends)} false-friend pairs need confirming`);
  }
  if (input.review > 0) {
    parts.push(input.review === 1 ? "1 column needs review" : `${n(input.review)} columns need review`);
  }
  return parts.join(" · ");
}

/** Names shown on the banner's one line before it says "+N more". */
const SUBJECTS_SHOWN = 3;

/**
 * The Map step's one place for what holds Validate (W7): a single compact
 * line — how many, which columns, and Show — with the per-issue fix behind
 * Details. The header only counts; the destination panel only marks the
 * columns (Required). Nothing else restates these sentences.
 */
export function MapIssueList({
  issues,
  summary,
  testId = "map-issues",
  primaryAction,
}: {
  issues: MapIssue[];
  /** e.g. "2 destination column issues block Validate". */
  summary: string;
  testId?: string;
  /** The one action on the line, e.g. Show: open the panel on the columns. */
  primaryAction?: { label: string; onClick: () => void; title?: string };
}) {
  const [open, setOpen] = useState(false);
  const listId = useId();
  if (issues.length === 0) return null;
  const ordered = [...issues].sort((a, b) => (a.tone === b.tone ? 0 : a.tone === "block" ? -1 : 1));
  const blocking = ordered.some((i) => i.tone === "block");
  const subjects = Array.from(new Set(ordered.flatMap((i) => i.subjects ?? [])));
  const shownSubjects = subjects.slice(0, SUBJECTS_SHOWN);
  const moreSubjects = subjects.length - shownSubjects.length;
  return (
    <section
      className={`df2-map-issues${blocking ? " is-block" : " is-warn"}${open ? " is-open" : ""}`}
      aria-label="Needs attention"
      data-testid={testId}
    >
      <div className="df2-map-issues-bar">
        <span className="df2-map-issues-icon">
          <DtIcon name={blocking ? "error" : "warning"} size={16} chrome="product" />
        </span>
        <p className="df2-map-issues-summary" role="status" title={ordered.map((i) => i.title).join("\n")}>
          <span className="df2-map-issues-count">{summary}</span>
          {shownSubjects.length > 0 ? (
            <span className="df2-map-issues-subjects">
              {shownSubjects.join(", ")}
              {moreSubjects > 0 ? ` +${moreSubjects} more` : ""}
            </span>
          ) : null}
        </p>
        <span className="df2-map-issues-actions">
          {primaryAction ? (
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-map-issues-show"
              onClick={primaryAction.onClick}
              title={primaryAction.title}
            >
              {primaryAction.label}
            </button>
          ) : null}
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-ghost df2-map-issues-toggle"
            aria-expanded={open}
            aria-controls={listId}
            onClick={() => setOpen((v) => !v)}
          >
            {open ? "Hide details" : "Details"}
          </button>
        </span>
      </div>
      <ul className="df2-map-issues-list" id={listId} hidden={!open}>
        {ordered.map((issue) => (
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
    </section>
  );
}
