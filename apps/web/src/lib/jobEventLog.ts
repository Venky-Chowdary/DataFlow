/** Persist transfer job event logs across live theater → result dashboard (session-scoped). */

const PREFIX = "df2-job-event-log:";
/** Soft cap. Prefer start + tail over dropping the beginning of a run. */
export const JOB_EVENT_LOG_MAX_LINES = 20000;

export function jobEventLogKey(jobId: string): string {
  return `${PREFIX}${jobId}`;
}

export function eventLogMessageBody(line: string): string {
  const sep = " — ";
  const i = line.indexOf(sep);
  return i >= 0 ? line.slice(i + sep.length) : line;
}

/** Keep the opening of the run and the live tail. Never drop start-only. */
export function keepEventLogStartAndTail(lines: string[], max = JOB_EVENT_LOG_MAX_LINES): string[] {
  if (lines.length <= max) return lines;
  const head = Math.min(Math.max(1, Math.floor(max * 0.3)), Math.floor((max - 1) / 2));
  const tail = Math.max(1, max - head - 1);
  const omitted = lines.length - head - tail;
  return [
    ...lines.slice(0, head),
    `… ${omitted} events in the live buffer (session store clipped — start and tail kept)`,
    ...lines.slice(-tail),
  ];
}

export function mergeEventLogLines(local: string[], incoming: string[]): string[] {
  if (!incoming.length) return local;
  if (!local.length) return incoming;
  const localBodies = new Set(local.map(eventLogMessageBody));
  const incomingBodies = new Set(incoming.map(eventLogMessageBody));
  const localCoversIncoming = [...incomingBodies].every((b) => localBodies.has(b));
  const preferLocal = local.length >= incoming.length || localCoversIncoming;
  const out: string[] = [];
  const seen = new Set<string>();
  const push = (text: string) => {
    const body = eventLogMessageBody(text);
    if (!body || seen.has(body)) return;
    seen.add(body);
    out.push(text);
  };
  if (preferLocal) {
    local.forEach(push);
    incoming.forEach(push);
  } else {
    incoming.forEach(push);
    local.forEach(push);
  }
  return out;
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
  const keep = keepEventLogStartAndTail(lines);
  try {
    sessionStorage.setItem(jobEventLogKey(jobId), JSON.stringify(keep));
  } catch {
    try {
      sessionStorage.setItem(
        jobEventLogKey(jobId),
        JSON.stringify(keepEventLogStartAndTail(lines, 2000)),
      );
    } catch {
      /* quota / private mode — ignore */
    }
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
