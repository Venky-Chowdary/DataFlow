import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { Drawer } from "../ui/Drawer";

export type BadDataIssue = {
  column?: string;
  row?: number;
  message: string;
  chars?: string[];
  sample?: string;
};

interface BadDataFixDrawerProps {
  open: boolean;
  onClose: () => void;
  issues: BadDataIssue[];
  applying?: boolean;
  onStripControls: () => void;
  onQuarantineContinue: () => void;
  onExplainWithAI: () => void;
}

/**
 * Operator remediation for sample-level integrity failures (format-control chars,
 * encoding anomalies). Mirrors warehouse ETL practice: sanitize, quarantine, or
 * explain — never silent drop.
 */
export function BadDataFixDrawer({
  open,
  onClose,
  issues,
  applying = false,
  onStripControls,
  onQuarantineContinue,
  onExplainWithAI,
}: BadDataFixDrawerProps) {
  const columns = [...new Set(issues.map((i) => i.column).filter(Boolean))] as string[];

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={520}
      title="Fix bad data"
      subtitle="Sanitize or quarantine before write — nothing is silently dropped."
      icon={<DtIcon name="shield" size={18} />}
      footer={
        <div className="df2-bad-data-footer">
          <Button variant="ghost" onClick={onClose} disabled={applying}>
            Cancel
          </Button>
          <Button
            variant="secondary"
            onClick={onExplainWithAI}
            disabled={applying}
            leadingIcon={<DtIcon name="sparkle" size={14} />}
          >
            Explain with AI
          </Button>
          <Button
            variant="primary"
            onClick={onStripControls}
            loading={applying}
            loadingLabel="Applying…"
            leadingIcon={<DtIcon name="check" size={14} />}
          >
            Strip controls &amp; re-run
          </Button>
        </div>
      }
    >
      <div className="df2-bad-data">
        <p className="df2-bad-data-lead">
          Sample rows contain invisible format/control characters (zero-width spaces, null bytes, etc.).
          Warehouses like Snowflake and Postgres often reject these. Choose how to remediate — this is the
          same class of issue that causes silent sync failures in other ETL tools.
        </p>

        {columns.length > 0 && (
          <p className="df2-bad-data-cols">
            Affected columns: <strong>{columns.join(", ")}</strong>
          </p>
        )}

        <ul className="df2-bad-data-issues">
          {issues.slice(0, 8).map((issue, idx) => (
            <li key={`${issue.column}-${issue.row}-${idx}`}>
              <strong>
                {issue.column || "column"}
                {issue.row != null ? ` · row ${issue.row}` : ""}
              </strong>
              <span>{issue.message}</span>
              {issue.chars?.length ? (
                <code>{issue.chars.join(" ")}</code>
              ) : null}
              {issue.sample != null && issue.sample !== "" && (
                <pre className="df2-bad-data-sample">{issue.sample}</pre>
              )}
            </li>
          ))}
        </ul>

        <div className="df2-bad-data-options">
          <article>
            <h4><DtIcon name="layers" size={14} /> Strip control characters</h4>
            <p>
              Applies <code>strip_controls</code> to every mapped column, removes format/control chars on
              write, then re-runs validation. Recommended for MongoDB → warehouse routes.
            </p>
            <Button variant="primary" onClick={onStripControls} disabled={applying}>
              Strip controls &amp; re-run
            </Button>
          </article>
          <article>
            <h4><DtIcon name="alert" size={14} /> Quarantine &amp; continue</h4>
            <p>
              Applies strip_controls + balanced validation, then re-runs gates. After you Execute,
              any remaining bad rows land on that job under <strong>Inspect Quarantine</strong>
              (Jobs page) — downloadable as CSV. Nothing is silently deleted.
            </p>
            <Button variant="secondary" onClick={onQuarantineContinue} disabled={applying}>
              Quarantine bad cells &amp; re-run
            </Button>
          </article>
        </div>
      </div>
    </Drawer>
  );
}
