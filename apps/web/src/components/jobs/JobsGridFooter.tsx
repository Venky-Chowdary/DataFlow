import { Button } from "../ui/Button";

interface JobsGridFooterProps {
  /** Rows matching the server query (not just this page). */
  filteredTotal: number;
  pageIndex: number;
  pageSize: number;
  /** Rows on this page. */
  shown: number;
  loading?: boolean;
  onPageSize: (size: number) => void;
  onPage: (index: number) => void;
}

export const JOBS_PAGE_SIZES = [25, 50, 100] as const;

/** Server paging under the grid: range, rows per page, previous / next. */
export function JobsGridFooter({ filteredTotal, pageIndex, pageSize, shown, loading, onPageSize, onPage }: JobsGridFooterProps) {
  const pageCount = Math.max(1, Math.ceil(filteredTotal / pageSize));
  const pageStart = shown ? pageIndex * pageSize + 1 : 0;
  const pageEnd = pageIndex * pageSize + shown;
  return (
    <nav className="df2-jobs-pagination df2-jobs-grid-footer" aria-label="Job list pages">
      <span className="df2-jobs-pagination-range" aria-live="polite">
        {filteredTotal === 0
          ? "No matching jobs"
          : `Showing ${pageStart.toLocaleString()}–${pageEnd.toLocaleString()} of ${filteredTotal.toLocaleString()}`}
      </span>
      <label className="df2-jobs-page-size">
        <span>Rows per page</span>
        <select
          className="df2-input df2-select"
          aria-label="Rows per page"
          value={pageSize}
          onChange={(event) => onPageSize(Number(event.target.value))}
        >
          {JOBS_PAGE_SIZES.map((size) => <option key={size} value={size}>{size}</option>)}
        </select>
      </label>
      <Button size="sm" variant="ghost" disabled={pageIndex === 0 || loading} onClick={() => onPage(Math.max(0, pageIndex - 1))}>
        Previous
      </Button>
      <span className="df2-jobs-pagination-page">Page {pageIndex + 1} of {pageCount}</span>
      <Button size="sm" variant="ghost" disabled={pageIndex + 1 >= pageCount || loading} onClick={() => onPage(pageIndex + 1)}>
        Next
      </Button>
    </nav>
  );
}
