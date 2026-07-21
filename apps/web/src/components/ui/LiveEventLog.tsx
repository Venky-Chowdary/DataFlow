import { useEffect, useRef } from "react";
import { classifyJobLogLine } from "../../lib/transferFailure";

export type LiveLogEntry = { id: number; text: string };

type LiveEventLogProps = {
  lines: LiveLogEntry[] | string[];
  /** Live pulse in the header while the job is running. */
  live?: boolean;
  title?: string;
  empty?: string;
  className?: string;
  /** Outer shell class (theater vs jobs vs result). */
  variant?: "theater" | "jobs" | "result";
};

function toEntries(lines: LiveLogEntry[] | string[]): LiveLogEntry[] {
  if (lines.length === 0) return [];
  if (typeof lines[0] === "string") {
    return (lines as string[]).map((text, i) => ({ id: i + 1, text }));
  }
  return lines as LiveLogEntry[];
}

/**
 * Continuous terminal-style event stream: sticky head, stick-to-bottom scroll,
 * stable row keys, and a gentle enter motion — no full-panel flicker.
 */
export function LiveEventLog({
  lines,
  live = false,
  title = "Live event log",
  empty = "Waiting for job events…",
  className = "",
  variant = "theater",
}: LiveEventLogProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);
  const entries = toEntries(lines);
  const lastId = entries.length ? entries[entries.length - 1].id : 0;

  useEffect(() => {
    const el = scrollRef.current;
    if (!el || !stickRef.current) return;
    // Instant stick — smooth scroll makes lines feel like they jump in/out.
    el.scrollTop = el.scrollHeight;
  }, [entries.length, lastId]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 56;
  };

  const shell =
    variant === "jobs"
      ? "df2-live-log is-jobs"
      : variant === "result"
        ? "df2-live-log is-result"
        : "df2-live-log is-theater";

  return (
    <div className={`${shell} ${live ? "is-live" : ""} ${className}`.trim()}>
      <div className="df2-live-log-head">
        <strong>
          <span className={`df2-live-log-dot ${live ? "is-pulse" : ""}`} aria-hidden />
          {title}
        </strong>
        <span>{entries.length ? `${entries.length} events` : "Waiting…"}</span>
      </div>
      <div
        className="df2-live-log-scroll"
        ref={scrollRef}
        onScroll={onScroll}
        role="log"
        aria-live="off"
        aria-relevant="additions"
      >
        {entries.length === 0 ? (
          <div className="df2-live-log-empty">{empty}</div>
        ) : (
          entries.map((entry, i) => {
            const isNewest = i === entries.length - 1;
            return (
              <div
                key={entry.id}
                className={`df2-live-log-line is-${classifyJobLogLine(entry.text)}${isNewest ? " is-enter" : ""}`}
              >
                {entry.text}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
