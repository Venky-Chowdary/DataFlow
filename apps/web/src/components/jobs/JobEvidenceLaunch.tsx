/**
 * Jobs evidence launch — compact single-row chips that open right-side Drawers.
 * Keeps tab panes as clear overviews (not a wall of large cards).
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
  /** Compact single-row chips (default). Pass "cards" only when needed. */
  layout?: "row" | "cards";
}

export function JobEvidenceLaunchGrid({
  items,
  label = "Open evidence",
  layout = "row",
}: JobEvidenceLaunchGridProps) {
  const visible = items.filter((i) => !i.disabled);
  if (!visible.length) return null;

  if (layout === "cards") {
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
              title={item.description}
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

  return (
    <section className="df2-jobs-evidence is-row" aria-label={label}>
      <header className="df2-jobs-evidence-head">
        <strong>{label}</strong>
        <span>Click to open on the right</span>
      </header>
      <div className="df2-jobs-evidence-row" role="list">
        {visible.map((item) => (
          <button
            key={item.id}
            type="button"
            role="listitem"
            className={`df2-jobs-evidence-chip tone-${item.tone || "default"}`}
            onClick={item.onOpen}
            title={item.description}
          >
            <DtIcon name={item.icon} size={14} />
            <span className="df2-jobs-evidence-chip-label">{item.title}</span>
            {item.meta ? <em>{item.meta}</em> : null}
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

/** Format engine pipeline explanation into readable sections (not one wall of text). */
export function JobExplanationView({ text }: { text: string }) {
  const blocks = splitExplanation(text);
  if (!blocks.length) {
    return <p className="df2-jobs-explanation-prose df2-muted">No explanation recorded.</p>;
  }
  return (
    <div className="df2-jobs-explanation-view">
      {blocks.map((b, i) =>
        b.kind === "heading" ? (
          <h3 key={`h-${i}`} className="df2-jobs-explanation-heading">{b.text}</h3>
        ) : (
          <p key={`p-${i}`} className="df2-jobs-explanation-prose">{b.text}</p>
        ),
      )}
    </div>
  );
}

function splitExplanation(raw: string): Array<{ kind: "heading" | "body"; text: string }> {
  const text = String(raw || "").replace(/\r\n/g, "\n").trim();
  if (!text) return [];
  const parts = text.split(/\n{2,}|\n(?=#{1,3}\s|[A-Z][A-Za-z0-9 /&-]{2,40}:\s*$)/);
  const out: Array<{ kind: "heading" | "body"; text: string }> = [];
  for (const part of parts) {
    const chunk = part.trim();
    if (!chunk) continue;
    const lines = chunk.split("\n").map((l) => l.trim()).filter(Boolean);
    if (!lines.length) continue;
    const first = lines[0].replace(/^#+\s*/, "").replace(/:$/, "");
    const looksHeading =
      lines.length > 1
      && (
        /^#{1,3}\s/.test(lines[0])
        || (/^[A-Z][A-Za-z0-9 /&-]{2,48}:?$/.test(lines[0]) && lines[0].length < 56)
      );
    if (looksHeading) {
      out.push({ kind: "heading", text: first });
      const body = lines.slice(1).join("\n").trim();
      if (body) out.push({ kind: "body", text: body });
    } else {
      out.push({ kind: "body", text: chunk });
    }
  }
  return out.length ? out : [{ kind: "body", text }];
}
