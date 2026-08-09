import { useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { EmptyState } from "../ui/EmptyState";
import {
  classifyCell,
  filterRows,
  nextSort,
  sortRows,
  toCsv,
  toMarkdown,
  typeTone,
} from "../../lib/queryResults";

/**
 * Result grid for the query workspace — sort, per-column filter, type badges,
 * row inspection and clipboard export.
 *
 * The distinction other consoles collapse: an absent key, a JSON `null` and an
 * empty string are three different facts. The engine treats them differently
 * on write (a missing field is not an instruction to null a column), so the
 * console renders them differently rather than showing one dash for all three.
 */

export interface ResultGridProps {
  columns: string[];
  rows: Record<string, unknown>[];
  /** Logical type per column, as reported by the API. */
  columnSchema?: Record<string, string>;
  /** Provenance of columnSchema — surfaced so inference is never read as DDL. */
  typeSource?: string;
  truncated?: boolean;
  durationMs?: number;
  onCopied?: (what: string) => void;
}

export function ResultGrid({
  columns,
  rows,
  columnSchema = {},
  typeSource,
  truncated,
  durationMs,
  onCopied,
}: ResultGridProps) {
  const [sort, setSort] = useState<{ column: string; dir: "asc" | "desc" } | null>(null);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [detailRow, setDetailRow] = useState<number | null>(null);

  const filtered = useMemo(() => filterRows(rows, filters), [rows, filters]);
  const sorted = useMemo(() => sortRows(filtered, sort), [filtered, sort]);

  const copy = async (kind: "csv" | "json" | "markdown") => {
    const payload =
      kind === "csv"
        ? toCsv(columns, sorted)
        : kind === "json"
          ? JSON.stringify(sorted, null, 2)
          : toMarkdown(columns, sorted);
    try {
      await navigator.clipboard.writeText(payload);
      onCopied?.(`${sorted.length.toLocaleString()} rows as ${kind.toUpperCase()}`);
    } catch {
      onCopied?.("Clipboard unavailable in this browser context");
    }
  };

  const toggleSort = (col: string) => {
    setSort((prev) => nextSort(prev, col));
  };

  const activeFilters = Object.values(filters).filter((v) => v.trim() !== "").length;

  if (rows.length === 0) {
    return (
      <EmptyState
        icon="search"
        title="No rows"
        description="The query ran and returned zero rows."
        compact
      />
    );
  }

  return (
    <div className="df2-qw-grid">
      <div className="df2-qw-grid-bar">
        <span className="df2-qw-grid-count">
          {sorted.length.toLocaleString()}
          {sorted.length !== rows.length && ` of ${rows.length.toLocaleString()}`} rows ·{" "}
          {columns.length} col{columns.length === 1 ? "" : "s"}
        </span>
        {typeof durationMs === "number" && (
          <span className="df2-qw-grid-meta" title="Server-side execution time">
            <DtIcon name="clock" size={11} /> {durationMs < 1 ? "<1" : Math.round(durationMs)} ms
          </span>
        )}
        {truncated && (
          <span
            className="df2-qw-grid-meta df2-qw-grid-meta--warn"
            title="The row limit cut this result short — this is not the full set"
          >
            <DtIcon name="warning" size={11} /> truncated at limit
          </span>
        )}
        <div className="df2-qw-grid-actions">
          <button
            type="button"
            className="df2-qw-chip"
            data-active={filtersOpen}
            onClick={() => setFiltersOpen((v) => !v)}
            title="Per-column filters"
          >
            <DtIcon name="search" size={11} /> Filter{activeFilters ? ` (${activeFilters})` : ""}
          </button>
          {sort && (
            <button type="button" className="df2-qw-chip" onClick={() => setSort(null)}>
              Clear sort
            </button>
          )}
          <button type="button" className="df2-qw-chip" onClick={() => void copy("csv")}>
            Copy CSV
          </button>
          <button type="button" className="df2-qw-chip" onClick={() => void copy("json")}>
            Copy JSON
          </button>
          <button type="button" className="df2-qw-chip" onClick={() => void copy("markdown")}>
            Copy MD
          </button>
        </div>
      </div>

      <div className="df2-qw-grid-scroll">
        <table className="df2-query-table df2-qw-table">
          <thead>
            <tr>
              <th className="df2-qw-th-num" aria-label="Row number">
                #
              </th>
              {columns.map((c) => {
                const type = columnSchema[c];
                return (
                  <th key={c} data-sorted={sort?.column === c ? sort.dir : undefined}>
                    <button
                      type="button"
                      className="df2-qw-th-btn"
                      onClick={() => toggleSort(c)}
                      title={`Sort by ${c}`}
                    >
                      <span className="df2-qw-th-name">{c}</span>
                      {sort?.column === c && (
                        <DtIcon
                          name={sort.dir === "asc" ? "chevron-up" : "chevron-down"}
                          size={10}
                        />
                      )}
                    </button>
                    {type && (
                      <span className="df2-qw-th-type" data-tone={typeTone(type)} title={
                        typeSource === "inferred_from_values"
                          ? `${type} — inferred from returned values, not source DDL`
                          : type
                      }>
                        {type}
                      </span>
                    )}
                  </th>
                );
              })}
            </tr>
            {filtersOpen && (
              <tr className="df2-qw-filter-row">
                <th aria-hidden />
                {columns.map((c) => (
                  <th key={c}>
                    <input
                      className="df2-input df2-input-sm"
                      value={filters[c] ?? ""}
                      onChange={(e) =>
                        setFilters((prev) => ({ ...prev, [c]: e.target.value }))
                      }
                      placeholder="contains…"
                      aria-label={`Filter ${c}`}
                    />
                  </th>
                ))}
              </tr>
            )}
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr key={i} onDoubleClick={() => setDetailRow(i)}>
                <td className="df2-qw-td-num">
                  <button
                    type="button"
                    className="df2-qw-rownum"
                    onClick={() => setDetailRow(i)}
                    title="Inspect row"
                  >
                    {i + 1}
                  </button>
                </td>
                {columns.map((c) => {
                  const cell = classifyCell(row, c);
                  return (
                    <td key={c} data-cell={cell.kind} title={cell.text}>
                      <span className="df2-qw-cell">{cell.text}</span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {detailRow !== null && sorted[detailRow] && (
        <div className="df2-qw-detail" role="dialog" aria-label="Row detail">
          <div className="df2-qw-detail-head">
            <strong>Row {detailRow + 1}</strong>
            <button
              type="button"
              className="df2-qw-icon-btn"
              onClick={() => setDetailRow(null)}
              aria-label="Close row detail"
            >
              <DtIcon name="x" size={13} />
            </button>
          </div>
          <dl className="df2-qw-detail-list">
            {columns.map((c) => {
              const cell = classifyCell(sorted[detailRow], c);
              return (
                <div key={c} className="df2-qw-detail-item">
                  <dt>
                    {c}
                    {columnSchema[c] && (
                      <span className="df2-qw-th-type" data-tone={typeTone(columnSchema[c])}>
                        {columnSchema[c]}
                      </span>
                    )}
                  </dt>
                  <dd data-cell={cell.kind}>{cell.text}</dd>
                </div>
              );
            })}
          </dl>
        </div>
      )}

      <p className="df2-qw-grid-foot">
        <span data-cell="null">NULL</span> = SQL null ·{" "}
        <span data-cell="empty">(empty)</span> = zero-length string ·{" "}
        <span data-cell="missing">(missing)</span> = field absent from the document.
        {typeSource === "inferred_from_values" &&
          " Column types are inferred from the returned values, not read from source DDL."}
      </p>
    </div>
  );
}
