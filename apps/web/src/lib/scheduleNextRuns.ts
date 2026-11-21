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
 * cron-parser (MIT) generates candidate instants quickly; every candidate is
 * re-checked against the server's rules before it is shown.
 */
import { CronExpressionParser } from "cron-parser";

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
  | { kind: "clock"; runs: Date[]; timeZone: string; dstChange: Date | null }
  | { kind: "rolling"; everyMinutes: number; runs: Date[]; fromLastRun: boolean }
  | { kind: "invalid"; message: string }
  | { kind: "empty" };

const PRESET_MINUTES: Record<string, number> = { hourly: 60, daily: 24 * 60, weekly: 7 * 24 * 60 };

/**
 * The next `count` runs, as the server will compute them.
 * `lastRunAt` anchors a rolling cadence; without it the cadence starts at save.
 */
export function previewNextRuns(input: {
  mode: "preset" | "cron";
  preset: string;
  cron: string;
  timeZone: string;
  now?: Date;
  lastRunAt?: string | null;
  count?: number;
}): NextRunsPreview {
  const count = input.count ?? 5;
  const now = input.now ?? new Date();
  const rolling = (everyMinutes: number): NextRunsPreview => {
    const last = input.lastRunAt ? Date.parse(input.lastRunAt) : NaN;
    const base = Number.isFinite(last) ? last : now.getTime();
    const step = everyMinutes * 60_000;
    let first = base + step;
    // A rolling cadence whose next due time is past runs on the next beat.
    while (first <= now.getTime()) first += step;
    return {
      kind: "rolling",
      everyMinutes,
      fromLastRun: Number.isFinite(last),
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

  let iterator;
  try {
    iterator = CronExpressionParser.parse(expr, { currentDate: now, tz: input.timeZone });
  } catch {
    return { kind: "invalid", message: "No date in the next four years matches this expression." };
  }
  const runs: Date[] = [];
  const seen = new Set<number>();
  const add = (at: Date) => {
    if (at.getTime() <= now.getTime() || seen.has(at.getTime())) return;
    seen.add(at.getTime());
    runs.push(at);
  };
  // Bounded: a day-of-month rule like "31" skips months, never more than a few hundred candidates.
  for (let guard = 0; runs.length < count && guard < 500; guard += 1) {
    let candidate: Date;
    try {
      candidate = iterator.next().toDate();
    } catch {
      break;
    }
    const wall = wallClock(candidate, input.timeZone);
    if (!matches(cron, wall)) continue; // shifted into a time a DST change skipped: the server does not run then
    add(candidate);
    // A wall time that happens twice (clocks go back) runs twice on the server.
    const again = new Date(candidate.getTime() + 60 * 60_000);
    const wallAgain = wallClock(again, input.timeZone);
    if (wallAgain.hour === wall.hour && wallAgain.minute === wall.minute && matches(cron, wallAgain)) add(again);
  }
  runs.sort((a, b) => a.getTime() - b.getTime());
  const shown = runs.slice(0, count);
  if (!shown.length) return { kind: "invalid", message: "No date in the next four years matches this expression." };
  let dstChange: Date | null = null;
  for (let i = 1; i < shown.length; i += 1) {
    if (wallClock(shown[i - 1], input.timeZone).offset !== wallClock(shown[i], input.timeZone).offset) {
      dstChange = shown[i];
      break;
    }
  }
  return { kind: "clock", runs: shown, timeZone: input.timeZone, dstChange };
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
