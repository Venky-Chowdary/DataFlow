import type { ShapeColumnProfile, ShapeSuggestion } from "../../lib/shape";
import {
  columnFamily,
  columnFindings,
  findingShare,
  frequentValues,
  isNumericText,
  numericHistogram,
  qualityScore,
  readsAsSummary,
} from "../../lib/transformProfile";

interface TransformColumnChartProps {
  profile: ShapeColumnProfile;
  /** Destination carrier for this column, when the schema is already read. */
  targetType?: string;
  /** Sampled cells already held by the studio — the distribution is measured from these. */
  sampleValues?: unknown[];
  /** The suggestion that would repair this column, if one is still open. */
  suggestion?: ShapeSuggestion | null;
  canApply?: boolean;
  applyReason?: string;
  onApplySuggestion?: (suggestion: ShapeSuggestion) => void;
}

/**
 * Selected-column detail — DataKitchen's content analysis pane.
 *
 * Type-aware: numeric columns draw a histogram and a stat strip; text columns
 * draw frequent values and length; every family still lists hygiene findings
 * as separate bars because they overlap and a stacked share would lie.
 */
export function TransformColumnChart({
  profile,
  targetType,
  sampleValues = [],
  suggestion,
  canApply = false,
  applyReason,
  onApplySuggestion,
}: TransformColumnChartProps) {
  const findings = columnFindings(profile);
  const family = columnFamily(profile);
  const numericText = isNumericText(profile);
  const score = qualityScore(profile);
  const values = sampleValues.length ? sampleValues : profile.samples;
  const histogram = family === "numeric" ? numericHistogram(values) : [];
  const frequent = family !== "numeric" ? frequentValues(values) : [];

  return (
    <article className={`df2-xform-detail${findings.length ? " has-findings" : ""}`} aria-labelledby="xform-detail-title">
      <header className="df2-xform-detail-head">
        <div>
          <h3 id="xform-detail-title" className="df2-xform-col-name" title={profile.name}>
            {profile.name}
          </h3>
          <p className="df2-xform-col-type">{readsAsSummary(profile)}</p>
        </div>
        <div className="df2-xform-detail-head-side">
          {targetType && (
            <span className="df2-xform-col-target" title="Destination carrier this column lands in">
              → {targetType}
            </span>
          )}
          <span className="df2-xform-quality" title="Share of sampled rows without a hygiene finding">
            {score}<small>/100</small>
          </span>
        </div>
      </header>

      <dl className="df2-xform-detail-stats">
        <div>
          <dt>Rows</dt>
          <dd>{profile.rows.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Distinct</dt>
          <dd>
            {profile.distinct.toLocaleString()}
            {profile.distinct_capped ? "+" : ""}
          </dd>
        </div>
        {family === "numeric" && (
          <>
            <div>
              <dt>Min</dt>
              <dd>{profile.min || "—"}</dd>
            </div>
            <div>
              <dt>Max</dt>
              <dd>{profile.max || "—"}</dd>
            </div>
            <div>
              <dt>Scale</dt>
              <dd>{profile.max_scale.toLocaleString()}</dd>
            </div>
          </>
        )}
        {family === "text" && (
          <div>
            <dt>Longest</dt>
            <dd>{profile.max_length.toLocaleString()} char(s)</dd>
          </div>
        )}
        {numericText && (
          <div>
            <dt>Reads as</dt>
            <dd>numeric text</dd>
          </div>
        )}
      </dl>

      {histogram.length > 0 && (
        <section className="df2-xform-distribution" aria-label="Numeric distribution">
          <h4>Value distribution</h4>
          <ul className="df2-xform-bars">
            {histogram.map((bin) => (
              <li key={bin.label}>
                <span className="df2-xform-bar-label">{bin.label}</span>
                <span className="df2-xform-bar-track">
                  <span
                    className="df2-xform-bar-fill kind-distribution"
                    style={{ width: `${findingShare(bin.count, profile.rows)}%` }}
                  />
                </span>
                <span className="df2-xform-bar-count">{bin.count.toLocaleString()}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {frequent.length > 0 && (
        <section className="df2-xform-distribution" aria-label="Frequent values">
          <h4>Frequent values</h4>
          <ul className="df2-xform-bars">
            {frequent.map((item) => (
              <li key={item.value}>
                <span className="df2-xform-bar-label" title={item.value}>{item.value}</span>
                <span className="df2-xform-bar-track">
                  <span
                    className="df2-xform-bar-fill kind-distribution"
                    style={{ width: `${findingShare(item.count, profile.rows)}%` }}
                  />
                </span>
                <span className="df2-xform-bar-count">{item.count.toLocaleString()}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      <section className="df2-xform-hygiene" aria-label="Hygiene findings">
        <h4>Hygiene</h4>
        {findings.length === 0 ? (
          <p className="df2-xform-col-clean">No findings in the sample.</p>
        ) : (
          <ul className="df2-xform-bars">
            {findings.map((finding) => (
              <li key={finding.kind} title={finding.hint}>
                <span className="df2-xform-bar-label">{finding.label}</span>
                <span className="df2-xform-bar-track">
                  <span
                    className={`df2-xform-bar-fill kind-${finding.kind.split(":")[0]}`}
                    style={{ width: `${findingShare(finding.count, profile.rows)}%` }}
                  />
                </span>
                <span className="df2-xform-bar-count">
                  {finding.count.toLocaleString()}
                  <small>/{profile.rows.toLocaleString()}</small>
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>

      {suggestion && (
        <div className="df2-xform-next-action">
          <p>
            <strong>{suggestion.title}</strong>
            <span> — {suggestion.reason}</span>
          </p>
          <button
            type="button"
            className="df2-btn df2-btn-sm"
            disabled={!canApply}
            title={applyReason || "Add this step to the recipe"}
            onClick={() => onApplySuggestion?.(suggestion)}
          >
            Apply this step
          </button>
        </div>
      )}
    </article>
  );
}
