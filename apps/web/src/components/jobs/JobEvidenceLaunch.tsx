/**
 * Jobs evidence launch cards — open right-side Drawers for deep evidence.
 * Keeps tab panes as clear overviews (DataFlow operator UX).
 */
import type { ReactNode } from "react";
import { DtIcon } from "../DtIcon";

export interface JobEvidenceLaunchItem {
  id: string;
  title: string;
  description: string;
  icon: string;
  meta?: string;
  disabled?: boolean;
  tone?: "default" | "ok" | "warn" | "danger";
  onOpen: () => void;
}

interface JobEvidenceLaunchGridProps {
  items: JobEvidenceLaunchItem[];
  label?: string;
}

export function JobEvidenceLaunchGrid({ items, label = "Open evidence" }: JobEvidenceLaunchGridProps) {
  const visible = items.filter((i) => !i.disabled);
  if (!visible.length) return null;
  return (
    <section className="df2-jobs-evidence" aria-label={label}>
      <header className="df2-jobs-evidence-head">
        <strong>{label}</strong>
        <span>Opens a right-side panel — keep the overview scannable</span>
      </header>
      <div className="df2-jobs-evidence-grid">
        {visible.map((item) => (
          <button
            key={item.id}
            type="button"
            className={`df2-jobs-evidence-card tone-${item.tone || "default"}`}
            onClick={item.onOpen}
          >
            <span className="df2-jobs-evidence-icon" aria-hidden>
              <DtIcon name={item.icon} size={18} />
            </span>
            <span className="df2-jobs-evidence-copy">
              <strong>{item.title}</strong>
              <span>{item.description}</span>
              {item.meta ? <em>{item.meta}</em> : null}
            </span>
            <span className="df2-jobs-evidence-chevron" aria-hidden>
              <DtIcon name="chevron-right" size={16} />
            </span>
          </button>
        ))}
      </div>
    </section>
  );
}

interface JobLogTableProps {
  lines: string[];
  empty?: string;
}

/** Tabular log view — clearer than stacked terminal dumps for operators. */
export function JobLogTable({ lines, empty = "No lines recorded" }: JobLogTableProps) {
  if (!lines.length) {
    return <p className="df2-muted df2-jobs-log-empty">{empty}</p>;
  }
  return (
    <div className="df2-jobs-log-table-wrap">
      <table className="df2-table df2-jobs-log-table">
        <thead>
          <tr>
            <th scope="col" className="df2-jobs-log-col-n">#</th>
            <th scope="col">Entry</th>
          </tr>
        </thead>
        <tbody>
          {lines.map((line, i) => {
            const stamped = splitLogStamp(line);
            return (
              <tr key={`${i}-${line.slice(0, 24)}`}>
                <td className="df2-jobs-log-col-n df2-cell-mono">{i + 1}</td>
                <td>
                  {stamped.time ? (
                    <span className="df2-jobs-log-stamp">{stamped.time}</span>
                  ) : null}
                  <code className="df2-jobs-log-line">{stamped.body}</code>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function splitLogStamp(line: string): { time?: string; body: string } {
  const m = line.match(/^(\d{1,2}:\d{2}:\d{2}(?:\s*[AP]M)?)\s*[—\-–]\s*(.+)$/i);
  if (m) return { time: m[1], body: m[2] };
  return { body: line };
}

interface JobOverviewNoteProps {
  children: ReactNode;
}

export function JobOverviewNote({ children }: JobOverviewNoteProps) {
  return <p className="df2-jobs-overview-note">{children}</p>;
}
