/**
 * "Next 5 runs" under the schedule's cadence (UI pass 2, item 5): the times
 * the scheduler will use, in the schedule's time zone, with the viewer's own
 * time beside each when it differs. Recomputed as the cron or zone is typed.
 */
import { useMemo } from "react";
import { DtIcon } from "../DtIcon";
import { describeEvery, formatRunTime, previewNextRuns } from "../../lib/scheduleNextRuns";

function viewerTimeZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

export function NextRunsPreview({
  mode,
  preset,
  cron,
  timeZone,
  lastRunAt,
  now,
}: {
  mode: "preset" | "cron";
  preset: string;
  cron: string;
  timeZone: string;
  lastRunAt?: string | null;
  /** Fixed clock for tests. */
  now?: Date;
}) {
  const preview = useMemo(
    () => previewNextRuns({ mode, preset, cron, timeZone, lastRunAt, now }),
    [mode, preset, cron, timeZone, lastRunAt, now],
  );
  const local = viewerTimeZone();
  const zone = preview.kind === "clock" ? preview.timeZone : local;
  const showLocal = preview.kind === "clock" && local !== preview.timeZone;

  return (
    <section className="df2-next-runs" aria-labelledby="sched-next-runs-title" aria-live="polite">
      <h4 id="sched-next-runs-title" className="df2-next-runs-title">
        <DtIcon name="calendar" size={14} chrome="product" />
        Next 5 runs
        {preview.kind === "clock" && <span className="df2-next-runs-zone">{preview.timeZone}</span>}
      </h4>
      {preview.kind === "empty" && <p className="df2-next-runs-note">Enter a cron expression to see when it runs.</p>}
      {preview.kind === "invalid" && (
        <p className="df2-next-runs-error">
          <DtIcon name="warning" size={14} chrome="product" />
          <span>{preview.message}</span>
        </p>
      )}
      {preview.kind === "rolling" && (
        <p className="df2-next-runs-note">
          Runs {describeEvery(preview.everyMinutes)}, counted from the previous run
          {preview.fromLastRun ? "" : " (the first one after you save)"}. There is no fixed clock time.
        </p>
      )}
      {(preview.kind === "clock" || preview.kind === "rolling") && (
        <ol className="df2-next-runs-list">
          {preview.runs.map((at) => {
            const there = formatRunTime(at, zone);
            const here = showLocal ? formatRunTime(at, local) : there;
            return (
              <li key={at.getTime()}>
                <time dateTime={at.toISOString()}>{there}</time>
                {here !== there && <span className="df2-next-runs-local">{here} your time</span>}
              </li>
            );
          })}
        </ol>
      )}
      {preview.kind === "clock" && preview.dstChange && (
        <p className="df2-next-runs-note">
          Clocks change in {preview.timeZone} before {formatRunTime(preview.dstChange, preview.timeZone)}; the times above include it.
        </p>
      )}
    </section>
  );
}
