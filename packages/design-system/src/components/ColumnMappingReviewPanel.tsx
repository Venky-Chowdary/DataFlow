import { useMemo, useState } from "react";
import { ConfidenceBar } from "./ConfidenceBar";
import type { MappingRow } from "../types";

export type ColumnReviewFilter = "all" | "review" | "auto";

interface ColumnMappingReviewPanelProps {
  rows: MappingRow[];
  stats: { total: number; autoMapped: number; needsReview: number; overridden: number };
  targetOptions: string[];
  onTargetChange: (source: string, target: string) => void;
  onConfirmRow?: (source: string) => void;
}

export function ColumnMappingReviewPanel({
  rows,
  stats,
  targetOptions,
  onTargetChange,
  onConfirmRow,
}: ColumnMappingReviewPanelProps) {
  const [filter, setFilter] = useState<ColumnReviewFilter>(stats.needsReview > 0 ? "review" : "all");

  const visible = useMemo(() => {
    if (filter === "review") return rows.filter((r) => r.needsReview);
    if (filter === "auto") return rows.filter((r) => !r.needsReview);
    return rows;
  }, [rows, filter]);

  return (
    <div className="df-column-review">
      <div className="df-column-review-stats">
        <div className="df-column-review-stat df-column-review-stat--ok">
          <strong>{stats.autoMapped + stats.overridden}</strong>
          <span>auto-mapped</span>
        </div>
        <div className={`df-column-review-stat ${stats.needsReview ? "df-column-review-stat--warn" : ""}`}>
          <strong>{stats.needsReview}</strong>
          <span>need review</span>
        </div>
        <div className="df-column-review-stat">
          <strong>{stats.total}</strong>
          <span>total</span>
        </div>
      </div>

      {stats.needsReview > 0 && (
        <p className="df-column-review-hint">
          {stats.needsReview} column{stats.needsReview === 1 ? "" : "s"} below 85% confidence — confirm or remap.
          All others are mapped automatically.
        </p>
      )}

      <div className="df-column-review-filters" role="tablist">
        {(["review", "auto", "all"] as const).map((f) => (
          <button
            key={f}
            type="button"
            role="tab"
            aria-selected={filter === f}
            className={["df-column-review-filter", filter === f ? "df-column-review-filter--active" : ""]
              .filter(Boolean)
              .join(" ")}
            onClick={() => setFilter(f)}
          >
            {f === "review" ? `Needs review (${stats.needsReview})` : f === "auto" ? "Auto-mapped" : "All columns"}
          </button>
        ))}
      </div>

      <div className="df-table-scroll df-column-review-table-wrap">
        <table className="df-data-table">
          <thead>
            <tr>
              <th>Source</th>
              <th>Target</th>
              <th>Confidence</th>
              <th>Status</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td colSpan={5} className="df-empty-inline">
                  No columns in this view.
                </td>
              </tr>
            ) : (
              visible.map((row) => (
                <tr key={row.source} className={row.needsReview ? "df-data-table-row--review" : ""}>
                  <td className="df-mono">{row.source}</td>
                  <td>
                    <select
                      className="df-select df-map-select"
                      value={row.target}
                      onChange={(e) => onTargetChange(row.source, e.target.value)}
                    >
                      {targetOptions.map((opt) => (
                        <option key={opt} value={opt}>
                          {opt}
                        </option>
                      ))}
                      {!targetOptions.includes(row.target) && <option value={row.target}>{row.target}</option>}
                    </select>
                  </td>
                  <td>
                    <div className="df-column-confidence">
                      <ConfidenceBar value={row.confidence} />
                      <span className="df-column-confidence-pct">{Math.round(row.confidence * 100)}%</span>
                    </div>
                  </td>
                  <td>
                    {row.needsReview ? (
                      <span className="df-badge df-badge--warn">Review</span>
                    ) : row.userOverride ? (
                      <span className="df-badge df-badge--success">Confirmed</span>
                    ) : (
                      <span className="df-badge df-badge--success">Auto</span>
                    )}
                  </td>
                  <td>
                    {row.needsReview && onConfirmRow && (
                      <button type="button" className="df-btn df-btn--ghost df-btn--sm" onClick={() => onConfirmRow(row.source)}>
                        Confirm
                      </button>
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
