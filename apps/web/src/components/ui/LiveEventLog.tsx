import { useEffect, useRef, useState } from "react";
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
  /** Show expand/collapse control (Run step drawer). */
  collapsible?: boolean;
  /** Initial open state when no session preference exists. */
  defaultOpen?: boolean;
  /** sessionStorage key for remembering open/closed. */
  storageKey?: string;
};

function toEntries(lines: LiveLogEntry[] | string[]): LiveLogEntry[] {
  if (lines.length === 0) return [];
  if (typeof lines[0] === "string") {
    return (lines as string[]).map((text, i) => ({ id: i + 1, text }));
  }
  return lines as LiveLogEntry[];
}

function readStoredOpen(storageKey: string | undefined, fallback: boolean): boolean {
  if (!storageKey || typeof window === "undefined") return fallback;
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (raw === "0") return false;
    if (raw === "1") return true;
  } catch {
    /* private mode */
  }
  return fallback;
}

/**
 * Continuous terminal-style event stream: sticky head, stick-to-bottom scroll,
 * stable row keys, and a gentle enter motion — no full-panel flicker.
 * Collapsible on Run: collapse to a bottom bar; expand upward into the theater.
 */
export function LiveEventLog({
  lines,
  live = false,
  title = "Live event log",
  empty = "Waiting for job events…",
  className = "",
  variant = "theater",
  collapsible = false,
  defaultOpen = true,
  storageKey,
}: LiveEventLogProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);
  const entries = toEntries(lines);
  const lastId = entries.length ? entries[entries.length - 1].id : 0;
  const [open, setOpen] = useState(() =>
    collapsible ? readStoredOpen(storageKey, defaultOpen) : true,
  );

  useEffect(() => {
    if (!collapsible || !storageKey) return;
    try {
      sessionStorage.setItem(storageKey, open ? "1" : "0");
    } catch {
      /* private mode */
    }
  }, [collapsible, storageKey, open]);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el || !stickRef.current || !open) return;
    // Instant stick — smooth scroll makes lines feel like they jump in/out.
    el.scrollTop = el.scrollHeight;
  }, [entries.length, lastId, open]);

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

  const toggle = () => {
    if (!collapsible) return;
    setOpen((v) => !v);
  };

  return (
    <div
      className={`${shell} ${live ? "is-live" : ""} ${collapsible ? "is-collapsible" : ""} ${
        open ? "is-open" : "is-collapsed"
      } ${className}`.trim()}
    >
      <div className="df2-live-log-head">
        <strong>
          <span className={`df2-live-log-dot ${live ? "is-pulse" : ""}`} aria-hidden />
          {title}
        </strong>
        <div className="df2-live-log-head-actions">
          <span>{entries.length ? `${entries.length} events` : "Waiting…"}</span>
          {collapsible && (
            <button
              type="button"
              className="df2-live-log-toggle"
              onClick={toggle}
              aria-expanded={open}
              aria-controls="df2-live-log-body"
              title={open ? "Collapse log" : "Expand log"}
            >
              {open ? "Collapse" : "Expand"}
              <span className="df2-live-log-chevron" aria-hidden>
                {open ? "▾" : "▴"}
              </span>
            </button>
          )}
        </div>
      </div>
      {open && (
        <div
          id="df2-live-log-body"
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
      )}
      {!open && collapsible && (
        <button
          type="button"
          className="df2-live-log-collapsed-hint"
          onClick={toggle}
        >
          Log collapsed — {entries.length || 0} event{entries.length === 1 ? "" : "s"}
          {live ? " · live" : ""} · click to expand upward
        </button>
      )}
    </div>
  );
}
