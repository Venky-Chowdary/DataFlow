import { ConfidenceBar } from "./ConfidenceBar";
import type { MappingRow } from "../types";

interface MappingAuditTableProps {
  rows: MappingRow[];
  autoMappedCount?: number;
  reviewCount?: number;
  editable?: boolean;
  targetOptions?: string[];
  onTargetChange?: (source: string, newTarget: string) => void;
}

export function MappingAuditTable({
  rows,
  autoMappedCount,
  reviewCount,
  editable = false,
  targetOptions = [],
  onTargetChange,
}: MappingAuditTableProps) {
  const auto = autoMappedCount ?? rows.filter((r) => !r.needsReview && !r.userOverride).length;
  const review = reviewCount ?? rows.filter((r) => r.needsReview).length;

  return (
    <div className="df-mapping-audit">
      <div className="df-mapping-audit-summary">
        <span className="df-mono">
          {auto}/{rows.length} auto-mapped
        </span>
        {review > 0 && <span className="df-mapping-audit-review">{review} need review</span>}
        {editable && targetOptions.length > 0 && (
          <span className="df-file-meta">Select validated target column</span>
        )}
      </div>
      <div className="df-table-panel">
        <div className="df-table-scroll">
          <table className="df-data-table df-data-table--dense">
            <thead>
              <tr>
                <th>Source</th>
                <th aria-hidden />
                <th>Target</th>
                <th>Confidence</th>
                <th>Reasoning</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={row.source}
                  className={[
                    row.needsReview ? "df-data-table-row--review" : "",
                    row.userOverride ? "df-data-table-row--override" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                >
                  <td className="df-mono">{row.source}</td>
                  <td className="df-map-arrow" aria-hidden>
                    →
                  </td>
                  <td className="df-mono">
                    {editable && onTargetChange && targetOptions.length > 0 ? (
                      <select
                        className="df-map-select"
                        value={row.target}
                        onChange={(e) => onTargetChange(row.source, e.target.value)}
                        aria-label={`Target for ${row.source}`}
                      >
                        {targetOptions.map((opt) => (
                          <option key={opt} value={opt}>
                            {opt}
                          </option>
                        ))}
                        {!targetOptions.includes(row.target) && (
                          <option value={row.target}>{row.target}</option>
                        )}
                      </select>
                    ) : editable && onTargetChange ? (
                      <input
                        className="df-map-input"
                        value={row.target}
                        onChange={(e) => onTargetChange(row.source, e.target.value)}
                        aria-label={`Target for ${row.source}`}
                      />
                    ) : (
                      row.target
                    )}
                  </td>
                  <td>
                    <ConfidenceBar value={row.userOverride ? 1 : row.confidence} />
                  </td>
                  <td className="df-map-reasoning">
                    {row.userOverride ? "User override" : (row.reasoning ?? "—")}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
