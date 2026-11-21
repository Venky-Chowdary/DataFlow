/**
 * Validate's checks as a ranked checklist (UI pass 2, item 2): Blocking,
 * Could not run, Needs review, then Passed and Not applicable collapsed.
 * Findings that share a cause are one line with a count. A failed check and a
 * check that could not run look different (glyph, word and tone), and a check
 * that could not run is never counted as passed.
 *
 * Keyboard: j / k (or ↓ / ↑) move between checks, Enter runs the check's
 * action (Re-run Validate, else Explain), e explains.
 */
import { useId, useMemo, useRef, useState, type KeyboardEvent } from "react";
import {
  buildChecklist,
  checklistSummary,
  findingGroupLine,
  OBJECTS_SHOWN,
  OUTCOME_WORD,
  type CheckOutcome,
  type ChecklistRow,
  type GateLike,
} from "../../lib/gateChecklist";
import { explainGate } from "../../lib/explainCatalog";
import type { IconName } from "../icons/iconMap";
import { DtIcon } from "../DtIcon";
import { ExplainPopover } from "../ui/ExplainPopover";

const OUTCOME_ICON: Record<CheckOutcome, IconName> = {
  blocking: "error",
  could_not_run: "verdict-not-checked",
  review: "warning",
  passed: "success",
  after_run: "clock",
  not_applicable: "minus",
  pending: "spinner",
};

const FINDINGS_SHOWN = 5;

function CheckRow({
  row,
  tabIndex,
  onRerun,
  onFocusRow,
}: {
  row: ChecklistRow;
  tabIndex: number;
  onRerun?: () => void;
  onFocusRow: () => void;
}) {
  const labelId = useId();
  const [showAll, setShowAll] = useState(false);
  const explanation = explainGate(row.id, row.outcome, row.message);
  const findings = showAll ? row.findings : row.findings.slice(0, FINDINGS_SHOWN);
  const quiet = row.outcome === "passed" || row.outcome === "not_applicable";
  const cut = row.findings.length > FINDINGS_SHOWN || row.findings.some((g) => g.objects.length > OBJECTS_SHOWN);
  return (
    <li
      className="df2-check"
      data-outcome={row.outcome}
      data-check-row=""
      data-gate={row.id}
      tabIndex={tabIndex}
      aria-labelledby={labelId}
      onFocus={onFocusRow}
    >
      <span className="df2-check-icon" aria-hidden="true">
        <DtIcon name={OUTCOME_ICON[row.outcome]} size={16} chrome="product" />
      </span>
      <div className="df2-check-body">
        <p className="df2-check-head" id={labelId}>
          <span className="df2-check-label">{row.label}</span>
          <span className="df2-check-code">{row.code}</span>
          <span className="df2-check-word">{OUTCOME_WORD[row.outcome]}</span>
        </p>
        {!quiet && row.message && <p className="df2-check-message">{row.message}</p>}
        {row.scope && <p className="df2-check-scope">{row.scope}</p>}
        {!quiet && findings.length > 0 && (
          <ul className="df2-check-findings">
            {findings.map((group) => (
              <li key={group.cause}>{findingGroupLine(group, showAll)}</li>
            ))}
          </ul>
        )}
        {!quiet && cut && (
          <button type="button" className="df2-check-more" aria-expanded={showAll} onClick={() => setShowAll((v) => !v)}>
            {showAll ? "Show fewer" : "Show all"}
          </button>
        )}
      </div>
      <div className="df2-check-actions">
        {row.outcome === "could_not_run" && onRerun && (
          <button type="button" className="df2-check-action" data-row-action="" onClick={onRerun}>
            <DtIcon name="refresh" size={14} chrome="product" />
            Re-run Validate
          </button>
        )}
        <ExplainPopover explanation={explanation} label={`Explain ${row.label}`} />
      </div>
    </li>
  );
}

function CheckGroup({
  title,
  outcome,
  rows,
  collapsed,
  onToggle,
  focusIndex,
  baseIndex,
  onRerun,
  onFocusIndex,
}: {
  title: string;
  outcome: string;
  rows: ChecklistRow[];
  collapsed: boolean;
  onToggle: () => void;
  focusIndex: number;
  baseIndex: number;
  onRerun?: () => void;
  onFocusIndex: (index: number) => void;
}) {
  const listId = useId();
  return (
    <section className="df2-check-group" data-outcome={outcome}>
      <h4 className="df2-check-group-title">
        <button type="button" aria-expanded={!collapsed} aria-controls={listId} onClick={onToggle}>
          <DtIcon name={collapsed ? "chevron-right" : "chevron-down"} size={14} chrome="product" />
          <span>{title}</span>
          <span className="df2-check-group-count">{rows.length.toLocaleString()}</span>
        </button>
      </h4>
      {!collapsed && (
        <ul className="df2-check-list" id={listId}>
          {rows.map((row, i) => (
            <CheckRow
              key={row.id}
              row={row}
              tabIndex={baseIndex + i === focusIndex ? 0 : -1}
              onRerun={onRerun}
              onFocusRow={() => onFocusIndex(baseIndex + i)}
            />
          ))}
        </ul>
      )}
    </section>
  );
}

export function GateChecklist({
  gates,
  findingsOf,
  order,
  onRerun,
  note,
}: {
  gates: GateLike[];
  findingsOf?: (gate: GateLike) => string[];
  order?: (a: string, b: string) => number;
  onRerun?: () => void;
  /** Muted text after the counts, e.g. "confidence floor 85%". */
  note?: string;
}) {
  const checklist = useMemo(() => buildChecklist(gates, findingsOf, order), [gates, findingsOf, order]);
  const groups = useMemo(() => {
    const out = checklist.groups.map((g) => ({ id: g.id as string, title: g.title, rows: g.rows, collapsedByDefault: g.collapsedByDefault }));
    if (checklist.notApplicable.length) {
      out.push({ id: "not_applicable", title: "Not applicable", rows: checklist.notApplicable, collapsedByDefault: true });
    }
    return out;
  }, [checklist]);
  const [toggled, setToggled] = useState<Record<string, boolean>>({});
  const [focusIndex, setFocusIndex] = useState(0);
  const listRef = useRef<HTMLDivElement>(null);
  const summary = checklistSummary(checklist.counts);

  const rowsInDom = () => Array.from(listRef.current?.querySelectorAll<HTMLElement>("[data-check-row]") ?? []);
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement;
    if (!target.hasAttribute("data-check-row")) return;
    const rows = rowsInDom();
    const at = rows.indexOf(target);
    if (at < 0) return;
    const move = (next: number) => {
      event.preventDefault();
      const clamped = Math.max(0, Math.min(rows.length - 1, next));
      rows[clamped]?.focus();
    };
    if (event.key === "j" || event.key === "ArrowDown") move(at + 1);
    else if (event.key === "k" || event.key === "ArrowUp") move(at - 1);
    else if (event.key === "Home") move(0);
    else if (event.key === "End") move(rows.length - 1);
    else if (event.key === "Enter" || event.key === "e") {
      const action = event.key === "Enter" ? target.querySelector<HTMLElement>("[data-row-action]") : null;
      const explain = target.querySelector<HTMLElement>(".df2-explain-trigger");
      const button = action ?? explain;
      if (button) {
        event.preventDefault();
        button.click();
      }
    }
  };

  // The roving tab stop must land on a row that exists after a collapse or a re-run.
  const visibleRows = groups.reduce((n, g) => n + ((toggled[g.id] ?? g.collapsedByDefault) ? 0 : g.rows.length), 0);
  const tabStop = visibleRows ? Math.min(focusIndex, visibleRows - 1) : -1;
  let base = 0;
  return (
    <div className="df2-vd-rules df2-checklist" ref={listRef} onKeyDown={onKeyDown}>
      <div className="df2-vd-rules-head">
        <DtIcon name="verify" size={16} />
        <strong>Validation rules</strong>
        <span className="df2-checklist-summary">
          {summary}
          {note ? ` · ${note}` : ""}
        </span>
      </div>
      <p className="df2-sr-only">Use j and k to move between checks, Enter to act, e to explain.</p>
      {groups.map((group) => {
        const collapsed = toggled[group.id] ?? group.collapsedByDefault;
        const node = (
          <CheckGroup
            key={group.id}
            title={group.title}
            outcome={group.id}
            rows={group.rows}
            collapsed={collapsed}
            onToggle={() => setToggled((prev) => ({ ...prev, [group.id]: !collapsed }))}
            focusIndex={tabStop}
            baseIndex={base}
            onRerun={onRerun}
            onFocusIndex={setFocusIndex}
          />
        );
        if (!collapsed) base += group.rows.length;
        return node;
      })}
    </div>
  );
}
