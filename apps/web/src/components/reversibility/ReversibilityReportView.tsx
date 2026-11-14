/**
 * Page C: Reversibility. For each table, can the destination give the source
 * data back, and if not, which column says why.
 *
 * The verdicts, each column's invertibility and the gate are the API's
 * deterministic answer; this view only lays them out. Lossy columns are always
 * listed by name with their reason, a conditional column checked on a sample
 * says it is not proven for rows not sampled, and the rollback note never
 * promises an undo.
 */

import { Fragment, useId, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { StatTile, StatTileRow } from "../ui/StatTile";
import { badgeClass, formatDateTime } from "../../lib/parity";
import {
  checkText,
  columnLabel,
  columnReasonLabel,
  lossyColumnBadge,
  lossyColumnName,
  pluralizeText,
  columnPillPresentation,
  columnsForReview,
  countsText,
  dataCheckedText,
  gatePresentation,
  reversibilityVerdictPresentation,
  rollbackLabel,
  tableColumnCounts,
  type ColumnInvertibility,
  type ReversibilityReport,
  type ReversibilityTable,
} from "../../lib/reversibility";

export interface ReversibilityReportViewProps {
  report: ReversibilityReport;
  /** Plan scope: the switch is editable. Job scope shows what the run required. */
  canChangeRequirement?: boolean;
  requirementDeniedReason?: string;
  /** A switch change is in flight. */
  changingRequirement?: boolean;
  onRequireChange?: (next: boolean) => void;
  onRefresh?: () => void;
  refreshing?: boolean;
  /** A note under the header, e.g. "Re-run Validate to apply the gate." */
  notice?: string;
  error?: string;
}

function typeText(col: Pick<ColumnInvertibility, "source_type" | "target_type" | "reason_code">): string {
  // A row change is about which rows land, not a column's types.
  if (col.reason_code === "row_change") return "—";
  const s = col.source_type || "";
  const t = col.target_type || "";
  if (!s && !t) return "Types not declared";
  if (s && t) return s === t ? s : `${s} → ${t}`;
  return s ? `${s} → (not declared)` : `(not declared) → ${t}`;
}

function ColumnRow({ col }: { col: ColumnInvertibility }) {
  const pill = columnPillPresentation(col);
  // The API's reason already states the data check when it ran ("fits the N observed rows…"); say it once.
  const check = /observed row|not checked/i.test(col.reason) ? "" : checkText(col.check);
  return (
    <tr>
      <td className="df2-rev-col-name" data-label="Column">
        <span className="df2-mono">{columnLabel(col)}</span>
      </td>
      <td className={`df2-rev-col-type${col.reason_code === "row_change" ? " is-rows" : ""}`} data-label="Types">
        <span className="df2-mono">{typeText(col)}</span>
      </td>
      <td data-label="Mapping">
        <span className={badgeClass(pill.tone)}>{pill.label}</span>
        <span className="df2-rev-code">{columnReasonLabel(col)}</span>
      </td>
      <td className="df2-rev-col-why" data-label="Why">
        <span>{pluralizeText(col.reason)}</span>
        {col.transforms.length > 0 && (
          <span className="df2-parity-muted">
            Transforms: <span className="df2-mono">{col.transforms.join(" → ")}</span>
          </span>
        )}
        {check && <span className="df2-parity-muted">{pluralizeText(check)}</span>}
      </td>
    </tr>
  );
}

function TableColumns({ table }: { table: ReversibilityTable }) {
  const columns = columnsForReview(table.columns);
  if (!columns.length) return <p className="df2-parity-muted">No column mappings were analysed for this table.</p>;
  return (
    <table className="df2-table df2-table-compact df2-rev-columns">
      <caption className="df2-sr-only">Columns of {table.name}, lossy first</caption>
      <thead>
        <tr>
          <th scope="col">Column</th>
          <th scope="col">Types</th>
          <th scope="col">Mapping</th>
          <th scope="col">Why</th>
        </tr>
      </thead>
      <tbody>
        {columns.map((col, i) => (
          <ColumnRow key={`${col.source ?? ""}>${col.target ?? ""}:${i}`} col={col} />
        ))}
      </tbody>
    </table>
  );
}

export function ReversibilityReportView({
  report,
  canChangeRequirement = false,
  requirementDeniedReason,
  changingRequirement = false,
  onRequireChange,
  onRefresh,
  refreshing = false,
  notice,
  error,
}: ReversibilityReportViewProps) {
  const verdict = reversibilityVerdictPresentation(report.verdict);
  const gate = gatePresentation(report);
  const titleId = useId();
  const switchId = useId();
  // Tables that are not reversible open first, so the reasons are on screen without a click.
  const [open, setOpen] = useState<Set<string>>(
    () => new Set(report.tables.filter((t) => t.verdict !== "reversible").slice(0, 3).map((t) => t.name)),
  );
  const toggle = (name: string) =>
    setOpen((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  const editable = report.scope === "plan" && Boolean(onRequireChange);

  return (
    <section className="df2-rev" aria-labelledby={titleId}>
      <header className="df2-parity-toolbar">
        <div className="df2-parity-toolbar-verdict">
          <h3 id={titleId} className="df2-parity-h3">Reversibility</h3>
          <span className={badgeClass(verdict.tone)}>{verdict.label}</span>
          <span className="df2-parity-muted">
            Analysed {formatDateTime(report.generated_at)} · {report.scope === "plan" ? "this transfer plan" : "this run"}
          </span>
        </div>
        <div className="df2-parity-actions">
          {editable ? (
            <span className="df2-rev-switch-row">
              <label htmlFor={switchId} className="df2-rev-switch-label">Require reversibility</label>
              <button
                id={switchId}
                type="button"
                role="switch"
                aria-checked={report.require_reversibility}
                aria-describedby={`${switchId}-gate`}
                className={`df2-switch ${report.require_reversibility ? "on" : ""}`}
                disabled={!canChangeRequirement || changingRequirement}
                title={!canChangeRequirement ? requirementDeniedReason : undefined}
                onClick={() => onRequireChange?.(!report.require_reversibility)}
              >
                <span className="df2-switch-thumb" />
              </button>
              <span className="df2-rev-switch-state" aria-hidden>{report.require_reversibility ? "On" : "Off"}</span>
            </span>
          ) : (
            <span className="df2-parity-muted">
              Require reversibility: <b>{report.require_reversibility ? "on for this run" : "off for this run"}</b>
            </span>
          )}
          {onRefresh && (
            <Button
              size="sm"
              variant="ghost"
              onClick={onRefresh}
              loading={refreshing}
              loadingLabel="Refreshing…"
              leadingIcon={<DtIcon name="refresh" size={14} />}
            >
              Refresh
            </Button>
          )}
        </div>
      </header>

      <p
        id={`${switchId}-gate`}
        className={`df2-parity-notice${gate.tone === "danger" ? " is-danger" : gate.tone === "ok" ? " is-ok" : " is-muted"}`}
        role={gate.tone === "danger" ? "alert" : "status"}
      >
        <DtIcon name={gate.tone === "danger" ? "lock" : gate.tone === "ok" ? "check" : "gate"} size={14} />
        <span>
          <b>Gate G24R · {gate.label}.</b> {pluralizeText(gate.detail)}
        </span>
      </p>

      {editable && !canChangeRequirement && requirementDeniedReason && (
        <p className="df2-parity-muted">{requirementDeniedReason}</p>
      )}
      {notice && (
        <p className="df2-parity-notice" role="status">
          <DtIcon name="refresh" size={14} />
          <span>{notice}</span>
        </p>
      )}
      {error && (
        <p className="df2-parity-notice is-danger" role="alert">
          <DtIcon name="alert" size={14} />
          <span>{error}</span>
        </p>
      )}

      <div className="df2-rev-summary">
        {/* The verdict chip and the tiles say what verdict_reason's counts sentence said (W6 round 2). */}
        <StatTileRow label="Columns by invertibility" className="is-3 df2-rev-counts">
          <StatTile
            label="Lossy"
            value={report.counts.lossy.toLocaleString("en-US")}
            tone={report.counts.lossy > 0 ? "danger" : "neutral"}
            caption="cannot be given back"
          />
          <StatTile
            label="Conditional"
            value={report.counts.conditional.toLocaleString("en-US")}
            tone={report.counts.conditional > 0 ? "warn" : "neutral"}
            caption={(report.counts.enforced ?? 0) > 0
              ? `${(report.counts.enforced ?? 0).toLocaleString("en-US")} enforced on every row`
              : "reversible when values fit"}
          />
          <StatTile
            label="Invertible"
            value={report.counts.invertible.toLocaleString("en-US")}
            tone={report.counts.invertible > 0 ? "ok" : "neutral"}
            caption="round-trip exactly"
          />
        </StatTileRow>
        <p className="df2-parity-muted">{pluralizeText(dataCheckedText(report.data_checked))}</p>
      </div>

      <div className="df2-parity-scroll">
        <table className="df2-table df2-rev-tables">
          <caption className="df2-sr-only">Reversibility per table</caption>
          <thead>
            <tr>
              <th scope="col">Table</th>
              <th scope="col">Verdict</th>
              <th scope="col">Why</th>
              <th scope="col">Rollback</th>
              <th scope="col"><span className="df2-sr-only">Columns</span></th>
            </tr>
          </thead>
          <tbody>
            {report.tables.map((table) => {
              const v = reversibilityVerdictPresentation(table.verdict);
              const expanded = open.has(table.name);
              const panelId = `${titleId}-${table.name.replace(/[^A-Za-z0-9_-]/g, "_")}`;
              return (
                <Fragment key={table.name}>
                  <tr className={expanded ? "is-expanded" : undefined}>
                    <td className="df2-mono df2-rev-table-name" data-label="Table">{table.name}</td>
                    <td data-label="Verdict">
                      <span className={badgeClass(v.tone)}>{v.short}</span>
                    </td>
                    <td className="df2-rev-why" data-label="Why">
                      <span>{pluralizeText(table.reason)}</span>
                      <span className="df2-parity-muted">{countsText(tableColumnCounts(table))}</span>
                    </td>
                    <td data-label="Rollback">
                      <span className="df2-rev-rollback" title={pluralizeText(table.rollback_note)}>{rollbackLabel(table.rollback)}</span>
                    </td>
                    <td className="df2-rev-expand">
                      <Button
                        size="sm"
                        variant="ghost"
                        aria-expanded={expanded}
                        aria-controls={panelId}
                        onClick={() => toggle(table.name)}
                        trailingIcon={<DtIcon name={expanded ? "chevron-up" : "chevron-down"} size={14} />}
                      >
                        {expanded ? "Hide columns" : `Columns (${table.columns.length})`}
                      </Button>
                    </td>
                  </tr>
                  {expanded && (
                    <tr className="df2-rev-detail-row">
                      <td colSpan={5} id={panelId}>
                        <TableColumns table={table} />
                        <p className="df2-parity-muted">{pluralizeText(table.rollback_note)}</p>
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
            {report.tables.length === 0 && (
              <tr>
                <td colSpan={5} className="df2-parity-muted">No tables were analysed.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="df2-parity-grid2">
        <section className="df2-parity-card" aria-label="Lossy mappings">
          <h4 className="df2-parity-h4">Lossy mappings ({report.lossy_columns.length.toLocaleString("en-US")})</h4>
          {report.lossy_columns.length === 0 ? (
            <p className="df2-parity-muted">No column loses information in this mapping.</p>
          ) : (
            <ul className="df2-rev-lossy">
              {report.lossy_columns.map((c, i) => (
                <li key={`${c.table}.${c.column}:${i}`}>
                  <span className="df2-rev-lossy-head">
                    <span className={badgeClass("danger")}>{lossyColumnBadge(c, report)}</span>
                    <span className="df2-mono df2-rev-lossy-name">{lossyColumnName(c)}</span>
                  </span>
                  <span className="df2-rev-lossy-reason">{pluralizeText(c.reason)}</span>
                </li>
              ))}
            </ul>
          )}
        </section>
        <section className="df2-parity-card df2-rev-rollback-card" aria-label="Rollback plan">
          <h4 className="df2-parity-h4">Rollback plan for this cutover</h4>
          <p className="df2-parity-confidence">{pluralizeText(report.rollback_note)}</p>
          <p className="df2-parity-muted">
            This is evidence for a runbook: Datawrap does not undo committed rows. Where a table cannot be reversed,
            keep the source as the fallback until sign-off.
          </p>
        </section>
      </div>
    </section>
  );
}
