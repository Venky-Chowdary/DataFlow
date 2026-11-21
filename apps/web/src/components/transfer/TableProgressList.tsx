/**
 * Per-table progress on the Run screen (UI pass 2, item 3): one row per table
 * with three stage segments (Read · Write · Verify), each saying its state in
 * words, and the rows written. Active tables first, then queued; finished
 * tables collapse under "N done". A polite live region announces only state
 * changes ("orders: loaded"), never per-batch counts. No animation: segments
 * change state, they do not sweep (reduced motion needs nothing extra).
 */
import { useEffect, useId, useMemo, useRef, useState } from "react";
import {
  orderTableRows,
  STAGE_WORD,
  TABLE_STATE_WORD,
  tableStateAnnouncements,
  type StageState,
  type TableProgressRow,
  type TableState,
} from "../../lib/tableProgress";
import { plural } from "../../lib/plural";
import type { IconName } from "../icons/iconMap";
import { DtIcon } from "../DtIcon";

const STATE_ICON: Record<TableState, IconName> = {
  queued: "clock",
  loading: "spinner",
  verifying: "verify",
  loaded: "check",
  checked: "success",
  mismatch: "verdict-unexplained",
  failed: "error",
  rolled_back: "undo",
  stopped: "pause",
  not_run: "minus",
};

const STAGES: { id: "read" | "write" | "verify"; label: string }[] = [
  { id: "read", label: "Read" },
  { id: "write", label: "Write" },
  { id: "verify", label: "Verify" },
];

function Stage({ label, state }: { label: string; state: StageState }) {
  return (
    <span className="df2-tp-stage" data-state={state} title={`${label}: ${STAGE_WORD[state]}`}>
      <span className="df2-tp-stage-bar" aria-hidden="true" />
      <span className="df2-tp-stage-label">
        {label}
        <span className="df2-sr-only">: {STAGE_WORD[state]}</span>
      </span>
    </span>
  );
}

function Row({ row }: { row: TableProgressRow }) {
  return (
    <li className="df2-tp-row" data-state={row.state}>
      <span className="df2-tp-name" title={row.name}>{row.name}</span>
      <span className="df2-tp-state">
        <DtIcon name={STATE_ICON[row.state]} size={14} chrome="product" className={row.state === "loading" ? "df2-tp-icon is-live" : "df2-tp-icon"} />
        {TABLE_STATE_WORD[row.state]}
      </span>
      <span className="df2-tp-stages">
        {STAGES.map((s) => <Stage key={s.id} label={s.label} state={row[s.id]} />)}
      </span>
      <span className="df2-tp-rows">
        {row.rows == null ? "" : row.rowsFromEarlierPass ? `${plural(row.rows, "row")} from an earlier pass` : `${plural(row.rows, "row")} written`}
      </span>
    </li>
  );
}

export function TableProgressList({ rows }: { rows: TableProgressRow[] }) {
  const titleId = useId();
  const listId = useId();
  const [showDone, setShowDone] = useState(false);
  const { active, done } = useMemo(() => orderTableRows(rows), [rows]);
  const prev = useRef<Map<string, TableState>>(new Map());
  const [announcement, setAnnouncement] = useState("");
  useEffect(() => {
    const said = tableStateAnnouncements(prev.current, rows);
    if (said.length) setAnnouncement(said.join(". "));
    prev.current = new Map(rows.map((r) => [r.name, r.state]));
  }, [rows]);
  if (rows.length < 2) return null;
  const loaded = done.length;
  return (
    <section className="df2-tp" aria-labelledby={titleId}>
      <header className="df2-tp-head">
        <h3 id={titleId} className="df2-tp-title">Tables</h3>
        <span className="df2-tp-count">{loaded.toLocaleString()} of {plural(rows.length, "table")} loaded</span>
      </header>
      {active.length > 0 && <ul className="df2-tp-list">{active.map((row) => <Row key={row.name} row={row} />)}</ul>}
      {done.length > 0 && (
        <>
          <button type="button" className="df2-tp-done-toggle" aria-expanded={showDone} aria-controls={listId} onClick={() => setShowDone((v) => !v)}>
            <DtIcon name={showDone ? "chevron-down" : "chevron-right"} size={14} chrome="product" />
            {done.length.toLocaleString()} done
          </button>
          {showDone && <ul className="df2-tp-list" id={listId}>{done.map((row) => <Row key={row.name} row={row} />)}</ul>}
        </>
      )}
      <p className="df2-sr-only" aria-live="polite">{announcement}</p>
    </section>
  );
}
