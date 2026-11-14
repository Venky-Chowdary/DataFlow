import { useEffect, useMemo, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import { EmptyState } from "../ui/EmptyState";
import { formatDuration } from "../../lib/queryWorkspace";
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
 *
 * Layout: the status bar and the column header stay put; rows scroll inside
 * `.df2-qw-grid-scroll` (both directions) — the one scroll region of the grid.
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
  /** The row limit the run asked for, so "cut short" can name it. */
  limit?: number;
  onCopied?: (what: string) => void;
  /**
   * The Query console's "Protect against spreadsheet formulas" toggle: Copy as
   * CSV defuses formula-looking cells when on (the default), exact values when off.
   */
  formulaProtection?: boolean;
  /** Fill the parent's height (the Query workspace's results pane). */
  fill?: boolean;
}

/** A column reads as numeric from its declared type, else from its first value. */
function numericColumns(columns: string[], rows: Record<string, unknown>[], schema: Record<string, string>): Set<string> {
  const out = new Set<string>();
  for (const c of columns) {
    if (schema[c]) {
      if (typeTone(schema[c]) === "number") out.add(c);
      continue;
    }
    const first = rows.find((r) => r[c] !== null && r[c] !== undefined)?.[c];
    if (typeof first === "number") out.add(c);
  }
  return out;
}

export function ResultGrid({
  columns,
  rows,
  columnSchema = {},
  typeSource,
  truncated,
  durationMs,
  limit,
  onCopied,
  formulaProtection = true,
  fill,
}: ResultGridProps) {
  const [sort, setSort] = useState<{ column: string; dir: "asc" | "desc" } | null>(null);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [detailRow, setDetailRow] = useState<number | null>(null);
  const detailCloseRef = useRef<HTMLButtonElement>(null);
  const openerRef = useRef<HTMLButtonElement | null>(null);

  const filtered = useMemo(() => filterRows(rows, filters), [rows, filters]);
  const sorted = useMemo(() => sortRows(filtered, sort), [filtered, sort]);
  const numeric = useMemo(() => numericColumns(columns, rows, columnSchema), [columns, rows, columnSchema]);

  useEffect(() => {
    if (detailRow !== null) detailCloseRef.current?.focus();
  }, [detailRow]);

  const writeClipboard = async (payload: string, what: string) => {
    try {
      await navigator.clipboard.writeText(payload);
      onCopied?.(what);
    } catch {
      onCopied?.("Clipboard unavailable in this browser context");
    }
  };

  const copy = async (kind: "csv" | "json" | "markdown") => {
    const payload =
      kind === "csv"
        ? toCsv(columns, sorted, { formulaProtection })
        : kind === "json"
          ? JSON.stringify(sorted, null, 2)
          : toMarkdown(columns, sorted);
    await writeClipboard(payload, `${sorted.length.toLocaleString()} rows as ${kind.toUpperCase()}`);
  };

  const closeDetail = () => {
    setDetailRow(null);
    openerRef.current?.focus();
  };

  const toggleSort = (col: string) => {
    setSort((prev) => nextSort(prev, col));
  };

  const activeFilters = Object.values(filters).filter((v) => v.trim() !== "").length;
  const duration = typeof durationMs === "number" ? formatDuration(durationMs) : "";

  if (rows.length === 0) {
    return (
      <div className="df2-qw-zero" data-testid="query-zero-rows" data-fill={fill ? "true" : undefined}>
        <EmptyState
          icon="check"
          title="Query ran, 0 rows"
          description={`The statement succeeded${duration ? ` in ${duration}` : ""} and returned no rows${
            columns.length ? ` (${columns.length} column${columns.length === 1 ? "" : "s"}: ${columns.slice(0, 6).join(", ")}${columns.length > 6 ? ", …" : ""})` : ""
          }. Loosen the WHERE clause or check the database / schema.`}
          compact
        />
      </div>
    );
  }

  const detail = detailRow !== null ? sorted[detailRow] : undefined;

  return (
    <div className="df2-qw-grid" data-fill={fill ? "true" : undefined}>
      <div className="df2-qw-grid-bar" data-testid="query-results-status">
        <span className="df2-qw-grid-count">
          {sorted.length.toLocaleString()}
          {sorted.length !== rows.length && ` of ${rows.length.toLocaleString()}`} rows ·{" "}
          {columns.length} col{columns.length === 1 ? "" : "s"}
        </span>
        {duration && (
          <span className="df2-qw-grid-meta" title="Server-side execution time">
            <DtIcon name="clock" size={12} /> {duration}
          </span>
        )}
        {truncated && (
          <span
            className="df2-qw-grid-meta df2-qw-grid-meta--warn"
            title="The row limit cut this result short — this is not the full set. Raise Row limit, or Export for every row."
          >
            <DtIcon name="warning" size={12} />{" "}
            {limit ? `Limited to ${limit.toLocaleString()} rows` : "truncated at limit"}
          </span>
        )}
        <div className="df2-qw-grid-actions">
          <button
            type="button"
            className="df2-qw-chip"
            data-active={filtersOpen}
            aria-pressed={filtersOpen}
            onClick={() => setFiltersOpen((v) => !v)}
            title="Per-column filters"
          >
            <DtIcon name="filter" size={12} /> Filter{activeFilters ? ` (${activeFilters})` : ""}
          </button>
          {sort && (
            <button type="button" className="df2-qw-chip" onClick={() => setSort(null)}>
              Clear sort
            </button>
          )}
          <button
            type="button"
            className="df2-qw-chip"
            onClick={() => void copy("csv")}
            title={
              formulaProtection
                ? "Copy as CSV — cells starting with = + - @ get a ' prefix (Protect against spreadsheet formulas is on)"
                : "Copy as CSV — exact values (Protect against spreadsheet formulas is off)"
            }
          >
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

      <div className="df2-qw-grid-body">
        <div
          className="df2-qw-grid-scroll"
          tabIndex={0}
          role="region"
          aria-label={`Query results, ${sorted.length.toLocaleString()} rows`}
        >
          <table className="df2-query-table df2-qw-table">
            <thead>
              <tr>
                <th className="df2-qw-th-num" scope="col">
                  <span className="df2-sr-only">Row</span>
                  <span aria-hidden>#</span>
                </th>
                {columns.map((c) => {
                  const type = columnSchema[c];
                  const sortedDir = sort?.column === c ? sort.dir : undefined;
                  return (
                    <th
                      key={c}
                      scope="col"
                      data-sorted={sortedDir}
                      data-align={numeric.has(c) ? "end" : undefined}
                      aria-sort={sortedDir === "asc" ? "ascending" : sortedDir === "desc" ? "descending" : undefined}
                    >
                      <button
                        type="button"
                        className="df2-qw-th-btn"
                        onClick={() => toggleSort(c)}
                        title={`Sort by ${c}`}
                      >
                        <span className="df2-qw-th-name">{c}</span>
                        {sortedDir && (
                          <DtIcon name={sortedDir === "asc" ? "chevron-up" : "chevron-down"} size={12} />
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
                <tr key={i} onDoubleClick={() => setDetailRow(i)} data-selected={detailRow === i ? "true" : undefined}>
                  <td className="df2-qw-td-num">
                    <button
                      type="button"
                      className="df2-qw-rownum"
                      onClick={(e) => {
                        openerRef.current = e.currentTarget;
                        setDetailRow(i);
                      }}
                      title="Inspect and copy this row"
                      aria-label={`Inspect row ${i + 1}`}
                    >
                      {i + 1}
                    </button>
                  </td>
                  {columns.map((c) => {
                    const cell = classifyCell(row, c);
                    return (
                      <td
                        key={c}
                        data-cell={cell.kind}
                        data-align={numeric.has(c) && cell.kind === "value" ? "end" : undefined}
                        title={cell.text.length > 40 ? cell.text.slice(0, 2000) : undefined}
                      >
                        <span className="df2-qw-cell">{cell.text}</span>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {detail && detailRow !== null && (
          <aside
            className="df2-qw-detail"
            aria-labelledby="query-row-detail-title"
            data-testid="query-row-detail"
            onKeyDown={(e) => {
              if (e.key === "Escape") {
                e.stopPropagation();
                closeDetail();
              }
            }}
          >
            <div className="df2-qw-detail-head">
              <strong id="query-row-detail-title">Row {detailRow + 1}</strong>
              <button
                type="button"
                className="df2-qw-chip"
                onClick={() => void writeClipboard(JSON.stringify(detail, null, 2), `row ${detailRow + 1} as JSON`)}
              >
                Copy row
              </button>
              <button
                ref={detailCloseRef}
                type="button"
                className="df2-close-btn df2-close-btn-sm"
                onClick={closeDetail}
                aria-label="Close row detail"
              >
                <DtIcon name="x" size={14} />
              </button>
            </div>
            <dl className="df2-qw-detail-list">
              {columns.map((c) => {
                const cell = classifyCell(detail, c);
                return (
                  <div key={c} className="df2-qw-detail-item">
                    <dt>
                      <span className="df2-qw-detail-name">{c}</span>
                      {columnSchema[c] && (
                        <span className="df2-qw-th-type" data-tone={typeTone(columnSchema[c])}>
                          {columnSchema[c]}
                        </span>
                      )}
                      <button
                        type="button"
                        className="df2-qw-link-btn"
                        onClick={() => void writeClipboard(cell.kind === "value" || cell.kind === "json" ? cell.text : "", `${c} value`)}
                        aria-label={`Copy ${c} value`}
                      >
                        Copy
                      </button>
                    </dt>
                    <dd data-cell={cell.kind}>{cell.text}</dd>
                  </div>
                );
              })}
            </dl>
          </aside>
        )}
      </div>

      <p className="df2-qw-grid-foot">
        <span data-cell="null">NULL</span> = SQL null ·{" "}
        <span data-cell="empty">(empty)</span> = zero-length string ·{" "}
        <span data-cell="missing">(missing)</span> = field absent · select a row number to inspect and copy.
        {typeSource === "inferred_from_values" &&
          " Column types are inferred from the returned values, not read from source DDL."}
      </p>
    </div>
  );
}
