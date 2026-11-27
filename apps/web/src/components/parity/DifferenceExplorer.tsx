/**
 * The difference explorer in the Trust Report (Parity at Scale).
 *
 * Three views of one verified table: Columns (what was compared per column and
 * how many rows differ), Keys (missing, extra, duplicate and changed keys) and
 * Values (source and destination side by side, read again live, differing
 * cells marked). Re-verify compares every row of the table again. Repair SQL is
 * shown for the reviewer to read, copy and run themselves: Datawrap never runs it.
 *
 * Protected columns never show a value anywhere here.
 */

import { Check, Copy, FileCode2, RefreshCw, TriangleAlert } from "lucide-react";
import { useCallback, useEffect, useId, useRef, useState, type KeyboardEvent } from "react";
import { Button } from "../ui/Button";
import { Dialog } from "../ui/Dialog";
import { Skeleton } from "../LoadingState";
import { badgeClass, parityErrorMessage, type ParityVerificationTable } from "../../lib/parity";
import {
  cellAccessibleName,
  cellText,
  columnStatusPresentation,
  comparedByLabel,
  differingPct,
  fetchExplorerColumns,
  fetchExplorerKeys,
  fetchExplorerValues,
  fetchRepairSql,
  plural,
  repairSummary,
  type ExplorerColumns,
  type ExplorerKeyCategory,
  type ExplorerKeys,
  type ExplorerValues,
  type RepairSql,
} from "../../lib/parityScale";

type TabId = "columns" | "keys" | "values";
const TABS: Array<{ id: TabId; label: string }> = [
  { id: "columns", label: "Columns" },
  { id: "keys", label: "Keys" },
  { id: "values", label: "Values" },
];
const PAGE = 25;

/**
 * Load one view, say what failed and offer a retry. An answer is shown only
 * for the request it belongs to (`key`), so a previous table never flashes.
 */
function useView<T>(key: string, load: ((signal: AbortSignal) => Promise<T>) | null) {
  const [state, setState] = useState<{ key: string; data: T | null; error: string }>({ key: "", data: null, error: "" });
  const [epoch, setEpoch] = useState(0);
  const loadRef = useRef(load);
  loadRef.current = load;
  const enabled = load != null;
  useEffect(() => {
    const run = loadRef.current;
    if (!enabled || !run) return;
    const controller = new AbortController();
    run(controller.signal).then(
      (data) => {
        if (!controller.signal.aborted) setState({ key, data, error: "" });
      },
      (err) => {
        if (!controller.signal.aborted) setState({ key, data: null, error: parityErrorMessage(err, "Could not load this view.") });
      },
    );
    return () => controller.abort();
  }, [key, enabled, epoch]);
  const settled = state.key === key && epoch >= 0;
  return {
    data: settled ? state.data : null,
    error: settled ? state.error : "",
    loading: enabled && !settled,
    retry: () => {
      setState((s) => ({ ...s, key: "" }));
      setEpoch((n) => n + 1);
    },
  };
}

function ViewState({ loading, error, retry, rows = 4 }: { loading: boolean; error: string; retry: () => void; rows?: number }) {
  if (error) {
    return (
      <div className="df2-pscale-banner is-danger" role="alert">
        <div>
          <p className="df2-pscale-banner-body">{error}</p>
        </div>
        <Button size="sm" onClick={retry}>
          Try again
        </Button>
      </div>
    );
  }
  if (loading) {
    return (
      <div className="df2-pscale-skeleton" aria-label="Loading" role="status">
        <Skeleton lines={rows} />
      </div>
    );
  }
  return null;
}

/* ── Columns ──────────────────────────────────────────────────── */

function ColumnsView({ data }: { data: ExplorerColumns }) {
  return (
    <div className="df2-pscale-scroll">
      <p className="df2-parity-muted">
        {plural(data.rows_compared, "row")} compared in each column.
      </p>
      <table className="df2-pscale-table">
        <caption className="df2-sr-only">Columns of {data.table}: how each was compared and how many rows differ</caption>
        <thead>
          <tr>
            <th scope="col">Column</th>
            <th scope="col">Compared</th>
            <th scope="col" className="is-num">Rows differing</th>
            <th scope="col">Share differing</th>
            <th scope="col">Status and cause</th>
          </tr>
        </thead>
        <tbody>
          {data.columns.map((col) => {
            const status = columnStatusPresentation(col);
            const pct = Math.min(100, Math.max(0, col.differing_pct));
            return (
              <tr key={col.column}>
                <th scope="row" className="df2-mono">
                  {col.column}
                  {col.protected && <span className="df2-pscale-tag">Protected</span>}
                </th>
                <td>{comparedByLabel(col.compared_by)}</td>
                <td className="is-num">{col.rows_differing.toLocaleString("en-US")}</td>
                <td>
                  <span className="df2-pscale-share">
                    <span className="df2-pscale-share-track" aria-hidden="true">
                      <span
                        className={`df2-pscale-share-fill ${col.status === "unexplained" ? "is-warn" : ""}`}
                        style={{ width: `${col.rows_differing ? Math.max(2, pct) : 0}%` }}
                      />
                    </span>
                    <span className="df2-parity-num">{differingPct(col.differing_pct, col.rows_differing)}</span>
                  </span>
                </td>
                <td>
                  <span className="df2-pscale-status">
                    <span className={badgeClass(status.tone)}>{status.label}</span>
                    {/* A cause is named only when the classifier found one; "Unexplained" is already the badge. */}
                    {col.causes.some((c) => c.code !== "unexplained") && (
                      <span className="df2-parity-muted">
                        {col.causes.map((c) => `${c.label} (${plural(c.rows, "row")})`).join("; ")}
                      </span>
                    )}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {!data.complete && (
        <p className="df2-parity-muted">Part of this table could not be compared. Re-verify it to compare every row.</p>
      )}
    </div>
  );
}

/* ── Keys ─────────────────────────────────────────────────────── */

const KEY_GROUPS: Array<{ id: keyof Pick<ExplorerKeys, "missing_in_destination" | "extra_in_destination" | "duplicate_keys" | "changed">; label: string; keyless: string }> = [
  { id: "missing_in_destination", label: "Missing in destination", keyless: "Rows missing in destination" },
  { id: "extra_in_destination", label: "Extra in destination", keyless: "Extra rows in destination" },
  { id: "duplicate_keys", label: "Duplicate keys", keyless: "Duplicate rows" },
  { id: "changed", label: "Changed values", keyless: "Changed rows" },
];

function KeyGroup({ label, group, hideKeys }: { label: string; group: ExplorerKeyCategory; hideKeys: boolean }) {
  const titleId = useId();
  const count = `${group.at_least && group.count ? "at least " : ""}${group.count.toLocaleString("en-US")}`;
  return (
    <section className="df2-pscale-keygroup" aria-labelledby={titleId}>
      <h4 id={titleId} className="df2-parity-h4">
        {label} <span className="df2-parity-num df2-pscale-count">{count}</span>
      </h4>
      {group.count === 0 ? (
        <p className="df2-parity-muted">None.</p>
      ) : hideKeys ? null : group.keys.length ? (
        <>
          <ul className="df2-pscale-keys" aria-label={`${label}: keys`}>
            {group.keys.map((k) => (
              <li key={k} className="df2-mono">{k}</li>
            ))}
          </ul>
          {group.count > group.keys_shown && (
            <p className="df2-parity-muted">
              Showing the first {plural(group.keys_shown, "key")} of {group.count.toLocaleString("en-US")}.
            </p>
          )}
        </>
      ) : (
        <p className="df2-parity-muted">The keys were not kept for this check.</p>
      )}
    </section>
  );
}

function KeysView({ data }: { data: ExplorerKeys }) {
  const hideKeys = data.keyless || data.key_protected;
  return (
    <div className="df2-pscale-keys-view">
      {data.keyless && (
        <p className="df2-parity-muted">This table has no key. Counts are rows of content; a difference cannot be attributed to a key.</p>
      )}
      {data.key_protected && <p className="df2-parity-muted">The key is a protected column, so key values are not shown.</p>}
      {!data.keyless && data.key_columns.length > 0 && (
        <p className="df2-parity-muted">
          Key: <span className="df2-mono">{data.key_columns.join(", ")}</span>
        </p>
      )}
      <div className="df2-pscale-keygrid">
        {KEY_GROUPS.map((g) => (
          <KeyGroup key={g.id} label={data.keyless ? g.keyless : g.label} group={data[g.id]} hideKeys={hideKeys} />
        ))}
      </div>
    </div>
  );
}

/* ── Values ───────────────────────────────────────────────────── */

/**
 * A grid of value pairs: arrows move between cells, Home and End go to the
 * row's ends, and each cell reads "column, source …, destination …, differs".
 */
function ValuesGrid({ data }: { data: ExplorerValues }) {
  const differing = new Set<string>();
  for (const row of data.rows) for (const cell of row.cells) if (cell.differs) differing.add(cell.column);
  const columns = data.rows[0]?.cells.map((c) => c.column).filter((c) => differing.has(c)) ?? [];
  const [active, setActive] = useState<[number, number]>([0, 0]);
  const gridRef = useRef<HTMLTableElement>(null);
  const width = columns.length + 1;
  const height = data.rows.length;

  const move = (event: KeyboardEvent<HTMLTableElement>) => {
    const [r, c] = active;
    let next: [number, number] | null = null;
    if (event.key === "ArrowRight") next = [r, Math.min(width - 1, c + 1)];
    else if (event.key === "ArrowLeft") next = [r, Math.max(0, c - 1)];
    else if (event.key === "ArrowDown") next = [Math.min(height - 1, r + 1), c];
    else if (event.key === "ArrowUp") next = [Math.max(0, r - 1), c];
    else if (event.key === "Home") next = event.ctrlKey ? [0, 0] : [r, 0];
    else if (event.key === "End") next = event.ctrlKey ? [height - 1, width - 1] : [r, width - 1];
    if (!next) return;
    event.preventDefault();
    setActive(next);
    gridRef.current?.querySelector<HTMLElement>(`[data-cell="${next[0]}-${next[1]}"]`)?.focus();
  };

  if (!data.rows.length) return null;
  return (
    <div className="df2-pscale-scroll">
      <table
        ref={gridRef}
        role="grid"
        aria-label={`Differing values in ${data.table}`}
        aria-rowcount={height + 1}
        className="df2-pscale-table df2-pscale-grid"
        onKeyDown={move}
      >
        <thead>
          <tr>
            <th scope="col" className="is-pinned">Key</th>
            {columns.map((col) => (
              <th key={col} scope="col" className="df2-mono">
                {col}
                <span className="df2-pscale-pair-head" aria-hidden="true">source · destination</span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row, r) => {
            const key = row.key.map((k) => cellText(k, false)).join(", ");
            const missing = !row.in_destination ? "Missing in destination" : !row.in_source ? "Extra in destination" : "";
            return (
              <tr key={`${key}-${r}`}>
                <th
                  scope="row"
                  className="is-pinned df2-mono"
                  data-cell={`${r}-0`}
                  tabIndex={active[0] === r && active[1] === 0 ? 0 : -1}
                  aria-label={`Key ${key}${missing ? `, ${missing.toLowerCase()}` : ""}`}
                >
                  {key}
                  {missing && <span className="df2-pscale-tag">{missing}</span>}
                </th>
                {columns.map((col, i) => {
                  const cell = row.cells.find((c) => c.column === col);
                  const c = i + 1;
                  if (!cell) return <td key={col} role="gridcell" data-cell={`${r}-${c}`} tabIndex={-1} />;
                  return (
                    <td
                      key={col}
                      role="gridcell"
                      data-cell={`${r}-${c}`}
                      tabIndex={active[0] === r && active[1] === c ? 0 : -1}
                      aria-label={cellAccessibleName(cell)}
                      className={cell.differs ? "is-diff" : undefined}
                    >
                      <span className="df2-pscale-pair" aria-hidden="true">
                        <span className={cell.protected ? "is-protected" : undefined}>{cellText(cell.source, cell.protected)}</span>
                        <span className="df2-pscale-ne">{cell.differs ? "≠" : "="}</span>
                        <span className={cell.protected ? "is-protected" : undefined}>
                          {cellText(cell.destination, cell.protected)}
                        </span>
                      </span>
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ValuesView({
  data,
  page,
  onPage,
}: {
  data: ExplorerValues;
  page: number;
  onPage: (page: number) => void;
}) {
  if (data.keyless) {
    return <p className="df2-parity-muted">{data.note || "This table has no key, so values cannot be paired row by row."}</p>;
  }
  if (!data.rows.length) {
    return <p className="df2-parity-muted">No changed values. Every row present on both sides holds the same values.</p>;
  }
  const from = page * PAGE + 1;
  const to = page * PAGE + data.rows.length;
  const total = Math.max(data.total, to);
  return (
    <div className="df2-pscale-values">
      <p className="df2-parity-muted">
        Read again from both databases just now. Showing rows {from.toLocaleString("en-US")} to {to.toLocaleString("en-US")} of{" "}
        {total.toLocaleString("en-US")}.
      </p>
      <ValuesGrid data={data} />
      {total > PAGE && (
        <div className="df2-parity-actions">
          <Button size="sm" variant="ghost" disabled={page === 0} onClick={() => onPage(page - 1)}>
            Previous rows
          </Button>
          <Button size="sm" variant="ghost" disabled={to >= (data.stored ?? total)} onClick={() => onPage(page + 1)}>
            Next rows
          </Button>
        </div>
      )}
    </div>
  );
}

/* ── Repair SQL ───────────────────────────────────────────────── */

function RepairDialog({ jobId, table, open, onClose }: { jobId: string; table: string; open: boolean; onClose: () => void }) {
  const view = useView<RepairSql>(`${jobId}|${table}`, open ? (signal) => fetchRepairSql(jobId, table, signal) : null);
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState("");
  useEffect(() => {
    if (!open) {
      setCopied(false);
      setCopyError("");
    }
  }, [open]);
  const repair = view.data && view.data.table === table ? view.data : null;
  const copy = async () => {
    if (!repair?.sql) return;
    try {
      await navigator.clipboard.writeText(repair.sql);
      setCopied(true);
      setCopyError("");
    } catch {
      setCopyError("Your browser did not allow copying. Select the statements and copy them yourself.");
    }
  };
  return (
    <Dialog
      open={open}
      onClose={onClose}
      size="lg"
      title="Repair SQL"
      subtitle={<span className="df2-mono">{table}</span>}
      footer={
        <div className="df2-parity-actions">
          <Button size="sm" variant="ghost" onClick={onClose}>
            Close
          </Button>
          <Button
            size="sm"
            variant="primary"
            disabled={!repair?.available}
            onClick={() => void copy()}
            leadingIcon={copied ? <Check size={14} strokeWidth={2} aria-hidden="true" /> : <Copy size={14} strokeWidth={2} aria-hidden="true" />}
          >
            {copied ? "Copied" : "Copy statements"}
          </Button>
        </div>
      }
    >
      <div className="df2-pscale-repair">
        <ViewState loading={view.loading} error={view.error} retry={view.retry} rows={6} />
        {repair && !view.loading && !view.error && (
          repair.available ? (
            <>
              <p className="df2-pscale-banner is-warn">
                <TriangleAlert size={16} strokeWidth={2} aria-hidden="true" />
                <span>
                  Datawrap does not run these. Read them, test them, and run them yourself on the destination.{" "}
                  {repairSummary(repair)}.
                </span>
              </p>
              <pre className="df2-pscale-sql" tabIndex={0} aria-label="Repair statements">
                {repair.sql}
              </pre>
            </>
          ) : (
            <p className="df2-parity-muted">{repair.reason || "No repair statements can be written for this table."}</p>
          )
        )}
        {copyError && (
          <p className="df2-parity-muted" role="alert">
            {copyError}
          </p>
        )}
        <span className="df2-sr-only" role="status">
          {copied ? "Repair statements copied" : ""}
        </span>
      </div>
    </Dialog>
  );
}

/* ── The explorer ─────────────────────────────────────────────── */

export interface DifferenceExplorerProps {
  jobId: string;
  /** Tables whose verification finished, in report order. */
  tables: ParityVerificationTable[];
  /** Bumps when the verification finishes again, so every view reloads. */
  revision?: string;
  canReverify: boolean;
  reverifyDeniedReason?: string;
  reverifying?: boolean;
  reverifyError?: string;
  onReverify: (table: string) => void;
}

export function DifferenceExplorer({
  jobId,
  tables,
  revision = "",
  canReverify,
  reverifyDeniedReason,
  reverifying = false,
  reverifyError,
  onReverify,
}: DifferenceExplorerProps) {
  const titleId = useId();
  const tabsId = useId();
  const [table, setTable] = useState(tables[0]?.name ?? "");
  const [tab, setTab] = useState<TabId>("columns");
  const [page, setPage] = useState(0);
  const [repairOpen, setRepairOpen] = useState(false);
  const tabRefs = useRef<Record<TabId, HTMLButtonElement | null>>({ columns: null, keys: null, values: null });

  useEffect(() => {
    if (!tables.some((t) => t.name === table)) setTable(tables[0]?.name ?? "");
  }, [tables, table]);
  useEffect(() => setPage(0), [table, revision]);

  const current = tables.find((t) => t.name === table);
  const base = `${jobId}|${table}|${revision}`;
  const columns = useView<ExplorerColumns>(
    base,
    tab === "columns" && table ? (signal) => fetchExplorerColumns(jobId, table, signal) : null,
  );
  const keys = useView<ExplorerKeys>(base, tab === "keys" && table ? (signal) => fetchExplorerKeys(jobId, table, signal) : null);
  const values = useView<ExplorerValues>(
    `${base}|${page}`,
    tab === "values" && table ? (signal) => fetchExplorerValues(jobId, table, { offset: page * PAGE, limit: PAGE }, signal) : null,
  );

  const onTabKey = useCallback((event: KeyboardEvent<HTMLDivElement>) => {
    const order = TABS.map((t) => t.id);
    const at = order.indexOf(tab);
    let next: TabId | null = null;
    if (event.key === "ArrowRight") next = order[(at + 1) % order.length];
    else if (event.key === "ArrowLeft") next = order[(at - 1 + order.length) % order.length];
    else if (event.key === "Home") next = order[0];
    else if (event.key === "End") next = order[order.length - 1];
    if (!next) return;
    event.preventDefault();
    setTab(next);
    tabRefs.current[next]?.focus();
  }, [tab]);

  if (!tables.length || !current) return null;
  const keyless = current.check_kind === "multiset_digest";

  return (
    <section className="df2-parity-card df2-pscale-explorer" aria-labelledby={titleId}>
      <header className="df2-parity-section-head">
        <div>
          <h3 id={titleId} className="df2-parity-h3">Explore differences</h3>
          <p className="df2-parity-muted">
            {keyless
              ? "Content match (no key): every row was compared as content; differences cannot be attributed to a key."
              : "Every row was compared in both databases. Open a view to see which columns, keys and values differ."}
          </p>
        </div>
        <div className="df2-parity-actions">
          {tables.length > 1 && (
            <label className="df2-pscale-picker">
              <span>Table</span>
              <select value={table} onChange={(e) => setTable(e.target.value)}>
                {tables.map((t) => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
              </select>
            </label>
          )}
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onReverify(table)}
            disabled={!canReverify || reverifying}
            loading={reverifying}
            loadingLabel="Starting…"
            title={canReverify ? "Compare every row of this table again, in both databases." : reverifyDeniedReason}
            leadingIcon={<RefreshCw size={14} strokeWidth={2} aria-hidden="true" />}
          >
            Re-verify this table
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => setRepairOpen(true)}
            disabled={keyless}
            title={keyless ? "A table with no key has no rows a statement could name." : "Preview statements to run yourself."}
            leadingIcon={<FileCode2 size={14} strokeWidth={2} aria-hidden="true" />}
          >
            Copy repair SQL
          </Button>
        </div>
      </header>
      {!canReverify && reverifyDeniedReason && <p className="df2-parity-muted">{reverifyDeniedReason}</p>}
      {reverifyError && (
        <p className="df2-pscale-banner is-danger" role="alert">
          <span>{reverifyError}</span>
        </p>
      )}

      <div className="df2-pscale-tabs" role="tablist" aria-label="Difference views" onKeyDown={onTabKey}>
        {TABS.map((t) => (
          <button
            key={t.id}
            ref={(el) => {
              tabRefs.current[t.id] = el;
            }}
            type="button"
            role="tab"
            id={`${tabsId}-${t.id}`}
            aria-selected={tab === t.id}
            aria-controls={`${tabsId}-${t.id}-panel`}
            tabIndex={tab === t.id ? 0 : -1}
            className={`df2-pscale-tab ${tab === t.id ? "is-active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>
      <div
        role="tabpanel"
        id={`${tabsId}-${tab}-panel`}
        aria-labelledby={`${tabsId}-${tab}`}
        className="df2-pscale-panel"
      >
        {tab === "columns" && (
          <>
            <ViewState loading={columns.loading} error={columns.error} retry={columns.retry} />
            {columns.data && !columns.loading && !columns.error && <ColumnsView data={columns.data} />}
          </>
        )}
        {tab === "keys" && (
          <>
            <ViewState loading={keys.loading} error={keys.error} retry={keys.retry} />
            {keys.data && !keys.loading && !keys.error && <KeysView data={keys.data} />}
          </>
        )}
        {tab === "values" && (
          <>
            <ViewState loading={values.loading} error={values.error} retry={values.retry} rows={6} />
            {values.data && !values.loading && !values.error && <ValuesView data={values.data} page={page} onPage={setPage} />}
          </>
        )}
      </div>
      <RepairDialog jobId={jobId} table={table} open={repairOpen} onClose={() => setRepairOpen(false)} />
    </section>
  );
}
