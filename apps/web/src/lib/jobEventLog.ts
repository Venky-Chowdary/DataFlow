/** Persist transfer job event logs across live theater → result dashboard (session-scoped). */

const PREFIX = "df2-job-event-log:";
const MAX_LINES = 5000;

export function jobEventLogKey(jobId: string): string {
  return `${PREFIX}${jobId}`;
}

export function readJobEventLog(jobId: string): string[] {
  if (!jobId || typeof sessionStorage === "undefined") return [];
  try {
    const raw = sessionStorage.getItem(jobEventLogKey(jobId));
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}

export function writeJobEventLog(jobId: string, lines: string[]): void {
  if (!jobId || typeof sessionStorage === "undefined") return;
  try {
    const clipped = lines.length > MAX_LINES ? lines.slice(-MAX_LINES) : lines;
    sessionStorage.setItem(jobEventLogKey(jobId), JSON.stringify(clipped));
  } catch {
    /* quota / private mode — ignore */
  }
}

export function appendJobEventLog(jobId: string, line: string): string[] {
  const next = [...readJobEventLog(jobId), line];
  writeJobEventLog(jobId, next);
  return next;
}

export function formatJobLogLine(message: string, at = new Date()): string {
  return `${at.toLocaleTimeString()} — ${message}`;
}
