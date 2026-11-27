/**
 * "Next 5 runs" for the schedule form (UI pass 2, item 5).
 *
 * The server decides when a schedule fires (apps/api/services/cron_schedule.py
 * and schedule_store.compute_next_run). This preview must say the same thing:
 *
 * - The grammar is the server's: exactly five fields, `*`, steps, ranges,
 *   lists, JAN-DEC / SUN-SAT, day-of-week 0-7. Anything it refuses is refused
 *   here with a plain sentence, never previewed.
 * - Fields match the wall clock in the schedule's time zone, so a time a DST
 *   change skips does not run that day, and a time that happens twice runs
 *   twice (the server walks real instants and tests each one's local time).
 * - Day-of-month and day-of-week follow Vixie: when both are restricted,
 *   either one matching is enough.
 * - `*\/N * * * *` with N not dividing 60 is not a clock cron on the server:
 *   it runs every N minutes from the previous run (R12-002).
 * - Presets are rolling intervals from the previous run, not clock times.
 *
 * The times are found the way the server finds them, so they agree exactly
 * (a differential test replays a corpus the scheduler itself computed:
 * lib/__fixtures__/cron_next_runs.json): each local day in the zone whose
 * month / day fields match, each matching wall time on it, and every real
 * instant that wall time names (none in a DST gap, two in an overlap).
 */

const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const DAYS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
const BOUNDS: [number, number][] = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 6]];
const FIELD_NAMES = ["minute", "hour", "day of month", "month", "day of week"];

export interface ParsedCron {
  minutes: Set<number>;
  hours: Set<number>;
  days: Set<number>;
  months: Set<number>;
  dow: Set<number>;
  domRestricted: boolean;
  dowRestricted: boolean;
}

export class CronPreviewError extends Error {}

function alias(token: string, index: number): string {
  const low = token.toLowerCase();
  if (index === 3 && MONTHS.includes(low)) return String(MONTHS.indexOf(low) + 1);
  if (index === 4 && DAYS.includes(low)) return String(DAYS.indexOf(low));
  return token;
}

const INT = /^-?\d+$/;

function parseField(field: string, index: number): Set<number> {
  const [lo, hi] = BOUNDS[index];
  const values = new Set<number>();
  for (const raw of field.split(",")) {
    let part = raw.trim();
    if (!part) throw new CronPreviewError(`The ${FIELD_NAMES[index]} field has an empty term.`);
    let step = 1;
    if (part.includes("/")) {
      const [base, stepText] = part.split("/", 2);
      if (!/^\d+$/.test(stepText) || Number(stepText) < 1) throw new CronPreviewError(`"${raw}" has an invalid step.`);
      step = Number(stepText);
      part = base || "*";
    }
    let start: number;
    let end: number;
    if (part === "*") {
      [start, end] = [lo, hi];
    } else if (part.includes("-")) {
      const [a, b] = part.split("-", 2).map((t) => alias(t, index));
      if (!INT.test(a) || !INT.test(b)) throw new CronPreviewError(`"${raw}" is not a valid range.`);
      [start, end] = [Number(a), Number(b)];
    } else {
      const token = alias(part, index);
      if (!INT.test(token)) throw new CronPreviewError(`"${raw}" is not a valid ${FIELD_NAMES[index]}.`);
      start = end = Number(token);
    }
    if (index === 4) {
      start = start === 7 ? 0 : start;
      end = end === 7 ? 0 : end;
    }
    if (start > end) throw new CronPreviewError(`"${raw}" starts after it ends.`);
    if (start < lo || end > hi) throw new CronPreviewError(`The ${FIELD_NAMES[index]} must be between ${lo} and ${hi}.`);
    for (let v = start; v <= end; v += step) values.add(v);
  }
  return values;
}

/** Parse with the server's grammar; throws CronPreviewError with a plain sentence. */
export function parseCron(expr: string): ParsedCron {
  const fields = expr.trim().split(/\s+/).filter(Boolean);
  if (fields.length !== 5) {
    throw new CronPreviewError(`A cron expression has 5 fields (minute hour day month weekday); this one has ${fields.length}.`);
  }
  return {
    minutes: parseField(fields[0], 0),
    hours: parseField(fields[1], 1),
    days: parseField(fields[2], 2),
    months: parseField(fields[3], 3),
    dow: parseField(fields[4], 4),
    domRestricted: fields[2] !== "*",
    dowRestricted: fields[4] !== "*",
  };
}

interface Wall { minute: number; hour: number; day: number; month: number; dow: number; offset: string }

const wallFormatters = new Map<string, Intl.DateTimeFormat>();

function wallClock(at: Date, timeZone: string): Wall {
  let fmt = wallFormatters.get(timeZone);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat("en-US", {
      timeZone, hourCycle: "h23", year: "numeric", month: "numeric", day: "numeric",
      hour: "numeric", minute: "numeric", weekday: "short", timeZoneName: "longOffset",
    });
    wallFormatters.set(timeZone, fmt);
  }
  const parts = Object.fromEntries(fmt.formatToParts(at).map((p) => [p.type, p.value]));
  return {
    minute: Number(parts.minute),
    hour: Number(parts.hour) % 24,
    day: Number(parts.day),
    month: Number(parts.month),
    dow: DAYS.indexOf(String(parts.weekday).slice(0, 3).toLowerCase()),
    offset: String(parts.timeZoneName),
  };
}

function matches(cron: ParsedCron, wall: Wall): boolean {
  if (!cron.minutes.has(wall.minute) || !cron.hours.has(wall.hour) || !cron.months.has(wall.month)) return false;
  const domOk = cron.days.has(wall.day);
  const dowOk = cron.dow.has(wall.dow);
  if (cron.domRestricted && cron.dowRestricted) return domOk || dowOk;
  if (cron.domRestricted) return domOk;
  if (cron.dowRestricted) return dowOk;
  return true;
}

/** `*\/N * * * *` where N does not divide 60: the server's rolling every-N-minutes. */
export function unevenMinuteStep(expr: string): number | null {
  const m = /^\*\/(\d+) \* \* \* \*$/.exec(expr.trim());
  if (!m) return null;
  const n = Number(m[1]);
  return n >= 1 && n <= 59 && 60 % n !== 0 ? n : null;
}

export function isValidTimeZone(timeZone: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone });
    return true;
  } catch {
    return false;
  }
}

export type NextRunsPreview =
  | { kind: "clock"; runs: Date[]; timeZone: string; dstChange: Date | null; source?: PreviewSource }
  | { kind: "rolling"; everyMinutes: number; runs: Date[]; fromLastRun: boolean; source?: PreviewSource }
  | { kind: "paused" }
  | { kind: "invalid"; message: string; never?: boolean }
  | { kind: "empty" };

const PRESET_MINUTES: Record<string, number> = { hourly: 60, daily: 24 * 60, weekly: 7 * 24 * 60 };
/** The server's search horizon (cron_schedule.FIRE_HORIZON_DAYS). */
const HORIZON_DAYS = 4 * 366;
const DAY_MS = 86_400_000;

function offsetMinutes(at: Date, timeZone: string): number {
  const label = wallClock(at, timeZone).offset; // "GMT+10:30", "GMT-05:00", "GMT"
  const m = /GMT([+-])(\d{1,2})(?::(\d{2}))?/.exec(label);
  if (!m) return 0;
  return (m[1] === "-" ? -1 : 1) * (Number(m[2]) * 60 + Number(m[3] || 0));
}

function matchesDay(cron: ParsedCron, month: number, day: number, dow: number): boolean {
  if (!cron.months.has(month)) return false;
  const domOk = cron.days.has(day);
  const dowOk = cron.dow.has(dow);
  if (cron.domRestricted && cron.dowRestricted) return domOk || dowOk;
  if (cron.domRestricted) return domOk;
  if (cron.dowRestricted) return dowOk;
  return true;
}

/**
 * Every instant strictly after `after` whose wall clock in `timeZone`
 * matches `cron`, the first `count` of them, in time order. Mirrors
 * cron_schedule.next_run chained `count` times.
 */
export function cronNextRuns(cron: ParsedCron, timeZone: string, after: Date, count: number): Date[] {
  // The server starts at the next whole minute after `after`.
  const startMs = Math.floor(after.getTime() / 60_000) * 60_000 + 60_000;
  const minutes = [...cron.minutes].sort((a, b) => a - b);
  const hours = [...cron.hours].sort((a, b) => a - b);
  const found: number[] = [];
  // Local calendar days, starting the day before the start's local date (a zone ahead of UTC).
  const startWall = wallClock(new Date(startMs), timeZone);
  const startYear = Number(new Intl.DateTimeFormat("en-US", { timeZone, year: "numeric" }).format(new Date(startMs)));
  void startWall.dow;
  let cursor = Date.UTC(startYear, startWall.month - 1, startWall.day) - DAY_MS;
  let extraDays = -1;
  // The server searches HORIZON_DAYS from each run (next_run is chained), so the
  // window restarts at every found run.
  let lastHit = 0;
  for (let i = 0; i - lastHit < HORIZON_DAYS + 3; i += 1, cursor += DAY_MS) {
    const before = found.length;
    const d = new Date(cursor);
    const y = d.getUTCFullYear(), mo = d.getUTCMonth() + 1, da = d.getUTCDate(), dow = d.getUTCDay();
    if (matchesDay(cron, mo, da, dow)) {
      // Offsets that can apply around this local day (before and after a change).
      const offsets = new Set([-DAY_MS, 0, DAY_MS, 2 * DAY_MS].map((delta) => offsetMinutes(new Date(cursor + delta), timeZone)));
      for (const h of hours) {
        for (const m of minutes) {
          const local = Date.UTC(y, mo - 1, da, h, m);
          for (const off of offsets) {
            const utc = local - off * 60_000;
            if (utc < startMs || found.includes(utc)) continue;
            const w = wallClock(new Date(utc), timeZone);
            if (w.hour === h && w.minute === m && w.day === da && w.month === mo) found.push(utc);
          }
        }
      }
    }
    if (found.length > before) lastHit = i;
    if (found.length >= count && extraDays < 0) extraDays = 2; // a day or two more: DST can reorder across midnight
    if (extraDays >= 0 && extraDays-- === 0) break;
  }
  return found.sort((a, b) => a - b).slice(0, count).map((ms) => new Date(ms));
}

/** True when no calendar day in the server's horizon matches (cron_schedule.assert_can_fire). */
function neverFires(cron: ParsedCron, after: Date): boolean {
  let day = Date.UTC(after.getUTCFullYear(), after.getUTCMonth(), after.getUTCDate());
  for (let i = 0; i < HORIZON_DAYS; i += 1, day += DAY_MS) {
    const d = new Date(day);
    if (matchesDay(cron, d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCDay())) return false;
  }
  return true;
}

/**
 * The next `count` runs, as the server will compute them.
 *
 * Rolling cadences (presets, and *\/N not dividing 60) are measured from an
 * anchor the way the server sets it: an unchanged cadence keeps the stored
 * next run (`storedNextRunAt`); a new or changed cadence is re-anchored when
 * it is saved, i.e. now. A paused schedule (or one created paused) has none.
 */
export function previewNextRuns(input: {
  mode: "preset" | "cron";
  preset: string;
  cron: string;
  timeZone: string;
  now?: Date;
  /** The schedule's stored next run, used only while its cadence is unchanged. */
  storedNextRunAt?: string | null;
  cadenceChanged?: boolean;
  paused?: boolean;
  count?: number;
}): NextRunsPreview {
  const count = input.count ?? 5;
  const now = input.now ?? new Date();
  if (input.paused) return { kind: "paused" };
  const rolling = (everyMinutes: number): NextRunsPreview => {
    const step = everyMinutes * 60_000;
    const stored = input.storedNextRunAt && !input.cadenceChanged ? Date.parse(input.storedNextRunAt) : NaN;
    // Saved now: the server computes now + interval. An unchanged cadence keeps its stored slot.
    let first = Number.isFinite(stored) ? stored : now.getTime() + step;
    while (first <= now.getTime()) first += step;
    return {
      kind: "rolling",
      everyMinutes,
      fromLastRun: Number.isFinite(stored),
      runs: Array.from({ length: count }, (_, i) => new Date(first + i * step)),
    };
  };
  if (input.mode === "preset") return rolling(PRESET_MINUTES[input.preset] ?? PRESET_MINUTES.daily);

  const expr = input.cron.trim();
  if (!expr) return { kind: "empty" };
  const uneven = unevenMinuteStep(expr);
  if (uneven != null) return rolling(uneven);
  if (!isValidTimeZone(input.timeZone)) return { kind: "invalid", message: `"${input.timeZone}" is not a time zone this browser knows.` };
  let cron: ParsedCron;
  try {
    cron = parseCron(expr);
  } catch (error) {
    return { kind: "invalid", message: error instanceof Error ? error.message : "This cron expression is not valid." };
  }
  if (neverFires(cron, now)) return { kind: "invalid", message: "No date in the next four years matches this expression.", never: true };
  const runs = cronNextRuns(cron, input.timeZone, now, count);
  if (!runs.length) return { kind: "invalid", message: "No date in the next four years matches this expression.", never: true };
  let dstChange: Date | null = null;
  for (let i = 1; i < runs.length; i += 1) {
    if (wallClock(runs[i - 1], input.timeZone).offset !== wallClock(runs[i], input.timeZone).offset) {
      dstChange = runs[i];
      break;
    }
  }
  return { kind: "clock", runs, timeZone: input.timeZone, dstChange };
}

/** "Mon 23 Nov, 09:00" in the given zone. */
export function formatRunTime(at: Date, timeZone: string): string {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone, weekday: "short", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
  }).format(at);
}

/** "every 7 minutes", "every hour", "every 24 hours", "every 7 days". */
export function describeEvery(minutes: number): string {
  if (minutes % (24 * 60) === 0) {
    const days = minutes / (24 * 60);
    return days === 1 ? "every 24 hours" : `every ${days} days`;
  }
  if (minutes % 60 === 0) {
    const hours = minutes / 60;
    return hours === 1 ? "every hour" : `every ${hours} hours`;
  }
  return minutes === 1 ? "every minute" : `every ${minutes} minutes`;
}

/**
 * Who computed the times shown: the scheduler (POST /schedules/preview, the
 * functions save and the beat use) or, only until it answers or when it
 * cannot be reached, this browser (re-review F: a browser's time-zone data can
 * differ from the server's, e.g. Africa/Casablanca).
 */
export type PreviewSource = "scheduler" | "browser";

export interface ServerPreview {
  kind: "clock" | "rolling" | "paused" | "invalid";
  runs: string[];
  timezone?: string;
  every_minutes?: number;
  from_stored?: boolean;
  message?: string;
}

/** The scheduler's answer replaces the browser's; the browser's stands only while there is none. */
export function withServerPreview(local: NextRunsPreview, server: ServerPreview | null, timeZone: string): NextRunsPreview {
  if (!server) {
    return local.kind === "clock" || local.kind === "rolling" ? { ...local, source: "browser" } : local;
  }
  if (server.kind === "paused") return { kind: "paused" };
  if (server.kind === "invalid") {
    // Keep the browser's plain sentence when both refuse; the scheduler decides that it is refused.
    return local.kind === "invalid" ? local : { kind: "invalid", message: server.message || "The scheduler cannot run this cadence." };
  }
  const runs = server.runs.map((r) => new Date(r)).filter((d) => !Number.isNaN(d.getTime()));
  if (server.kind === "rolling") {
    return { kind: "rolling", everyMinutes: Number(server.every_minutes || 0), runs, fromLastRun: Boolean(server.from_stored), source: "scheduler" };
  }
  const zone = server.timezone || timeZone;
  let dstChange: Date | null = null;
  for (let i = 1; i < runs.length; i += 1) {
    if (wallClock(runs[i - 1], zone).offset !== wallClock(runs[i], zone).offset) {
      dstChange = runs[i];
      break;
    }
  }
  return { kind: "clock", runs, timeZone: zone, dstChange, source: "scheduler" };
}
