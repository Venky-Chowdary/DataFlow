import { DtIcon } from "../DtIcon";
import { buildPhaseProfileView, formatSeconds } from "../../lib/phaseProfile";
import type { PhaseProfileReport } from "../../lib/types";

/**
 * Where a transfer's time actually went.
 *
 * A single "took 4m12s" tells an operator nothing about what to fix. Splitting
 * it into reading the source, transforming and writing, and verifying the
 * checksum turns a slow run into a specific next action — add source read
 * parallelism, tune the destination batch size, or accept that strict
 * verification costs a re-read.
 */
export function PhaseProfileCard({
  profile,
  engineSeconds,
  scopeNote,
}: {
  profile?: PhaseProfileReport | null;
  /** Monotonic execute clock from the engine. Distinct from the phase span. */
  engineSeconds?: number | null;
  /**
   * Set when this profile is the last stream of a multi-table run.
   * Its row totals include a re-read and are not the job population.
   */
  scopeNote?: string | null;
}) {
  const view = buildPhaseProfileView(profile);
  // Omitting the section entirely beats rendering an empty card: a ragged grid
  // of placeholder panels is worse than a dense one.
  if (!view) return null;

  return (
    <section className="df2-result-phases" aria-label="Transfer phase timing">
      <header>
        <DtIcon name="activity" size={14} />
        <strong>Where the time went</strong>
        <span>{view.headline}</span>
      </header>
      {scopeNote ? <p className="df2-result-phase-note">{scopeNote}</p> : null}

      <ul className="df2-result-phase-list">
        {view.rows.map((row) => (
          <li
            key={row.phase}
            className={`df2-result-phase${row.dominant ? " is-dominant" : ""}`}
          >
            <div className="df2-result-phase-head">
              <span className="df2-result-phase-label">{row.label}</span>
              <span className="df2-result-phase-time">{row.secondsLabel}</span>
            </div>
            <div
              className="df2-result-phase-bar"
              role="img"
              aria-label={`${row.label}: ${row.percent}% of measured phase time`}
            >
              <span style={{ width: `${Math.max(row.percent, 1.5)}%` }} />
            </div>
            <div className="df2-result-phase-meta">
              <span>{row.percent}%</span>
              {row.rows > 0 && (
                <>
                  <span aria-hidden="true">·</span>
                  <span>{row.rows.toLocaleString()} {scopeNote ? "phase rows" : "rows"}</span>
                  <span aria-hidden="true">·</span>
                  <span>{row.throughputLabel}</span>
                </>
              )}
            </div>
          </li>
        ))}
      </ul>

      <footer className="df2-result-phase-foot">
        <span>
          Measured phases {formatSeconds(view.busySeconds)}
          {engineSeconds != null && Number.isFinite(engineSeconds)
            ? ` · execute ${formatSeconds(engineSeconds)}`
            : view.elapsedSeconds > 0
              ? ` · profile span ${formatSeconds(view.elapsedSeconds)}`
              : ""}
        </span>
        {view.overlapNote && <span className="df2-result-phase-note">{view.overlapNote}</span>}
      </footer>
    </section>
  );
}
