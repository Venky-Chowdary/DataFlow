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
export function PhaseProfileCard({ profile }: { profile?: PhaseProfileReport | null }) {
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
              aria-label={`${row.label}: ${row.percent}% of engine time`}
            >
              <span style={{ width: `${Math.max(row.percent, 1.5)}%` }} />
            </div>
            <div className="df2-result-phase-meta">
              <span>{row.percent}%</span>
              {row.rows > 0 && (
                <>
                  <span aria-hidden="true">·</span>
                  <span>{row.rows.toLocaleString()} rows</span>
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
          Engine time {formatSeconds(view.busySeconds)} · wall clock{" "}
          {formatSeconds(view.elapsedSeconds)}
        </span>
        {view.overlapNote && <span className="df2-result-phase-note">{view.overlapNote}</span>}
      </footer>
    </section>
  );
}
