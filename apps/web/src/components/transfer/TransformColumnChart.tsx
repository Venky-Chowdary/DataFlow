import type { ShapeColumnProfile } from "../../lib/shape";
import {
  columnFindings,
  findingShare,
  isNumericText,
  readsAsSummary,
} from "../../lib/transformProfile";

interface TransformColumnChartProps {
  profile: ShapeColumnProfile;
  /** Destination carrier for this column, when the schema is already read. */
  targetType?: string;
}

/**
 * One column drawn: how it reads, and a bar per finding over the sampled rows.
 *
 * Separate bars rather than one stacked share bar, because the findings overlap
 * (a padded value can also hold inner whitespace) and a stacked bar would claim
 * a partition of the sample that does not exist.
 */
export function TransformColumnChart({ profile, targetType }: TransformColumnChartProps) {
  const findings = columnFindings(profile);
  const numericText = isNumericText(profile);

  return (
    <li className={`df2-xform-col${findings.length ? " has-findings" : ""}`}>
      <div className="df2-xform-col-head">
        <span className="df2-xform-col-name" title={profile.name}>{profile.name}</span>
        <span className="df2-xform-col-type">{readsAsSummary(profile)}</span>
        {targetType && (
          <span className="df2-xform-col-target" title="Destination carrier this column lands in">
            → {targetType}
          </span>
        )}
      </div>
      <div className="df2-xform-col-meta">
        <span>{profile.distinct.toLocaleString()}{profile.distinct_capped ? "+" : ""} distinct</span>
        {numericText && (
          <span className="df2-xform-col-flag" title="Every non-blank sampled value is a number, but the column is carried as text">
            numeric text
          </span>
        )}
        {profile.samples.length > 0 && (
          <span className="df2-xform-col-sample" title="Sampled values">
            e.g. {profile.samples.slice(0, 3).join(" · ")}
          </span>
        )}
      </div>
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
    </li>
  );
}
