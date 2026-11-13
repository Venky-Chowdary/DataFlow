import { Dialog } from "../ui/Dialog";
import { diffPlanMappings } from "../../lib/planConcurrency";
import type { PlanConflictState } from "../../lib/usePlanConflict";

const ORIGIN_COPY: Record<PlanConflictState["origin"], string> = {
  validate: "Validate did not run and nothing was saved.",
  map: "Map's result was not saved.",
  execute: "The transfer did not start and nothing was saved.",
  approve: "Nothing was approved: the revision Validate checked is no longer the active one.",
};

/**
 * The plan changed since this page loaded it: compare the latest saved
 * revision with the user's unsaved mappings, column by column, and choose.
 * In-page dialog (no browser dialogs): focus moves into it, Escape cancels,
 * and focus returns to the control that was focused before it opened.
 */
export function PlanConflictDialog({
  conflict,
  onUseLatest,
  onKeepMine,
  onCancel,
}: {
  conflict: PlanConflictState | null;
  onUseLatest: () => void;
  onKeepMine: () => void;
  onCancel: () => void;
}) {
  const latest = conflict?.latest ?? null;
  const rows = latest ? diffPlanMappings(latest.mappings, conflict?.mine ?? []) : [];
  // Newest revision when it is not the active one: shown as its own column.
  const headSplit = Boolean(latest?.headMappings);
  const headByTarget = new Map(
    (latest?.headMappings ? diffPlanMappings(latest.headMappings, []) : []).map((r) => [r.target.toLowerCase(), r.latestSource]),
  );
  const keepMineLabel = !latest
    ? "Keep mine"
    : headSplit
      ? `Keep mine (save as revision ${latest.head + 1}, replacing active revision ${latest.version})`
      : `Keep mine (save over revision ${latest.version})`;
  const changed = rows.filter((r) => r.differs).length;
  const saving = conflict?.status === "saving";
  const loading = conflict?.status === "loading";
  return (
    <Dialog
      open={Boolean(conflict)}
      onClose={onCancel}
      size="lg"
      title="This plan changed since you opened it"
      className="df2-plan-conflict"
      footer={
        <div className="df2-btn-row">
          <button type="button" className="df2-btn" onClick={onUseLatest} disabled={!latest || saving}>
            {latest ? `Use latest (revision ${latest.version})` : "Use latest"}
          </button>
          <button type="button" className="df2-btn df2-btn-primary" onClick={onKeepMine} disabled={!latest || saving}>
            {saving ? "Saving…" : keepMineLabel}
          </button>
          <button type="button" className="df2-btn df2-btn-ghost" onClick={onCancel} disabled={saving}>
            Cancel
          </button>
        </div>
      }
    >
      <div data-testid="plan-conflict-dialog">
        {conflict && <p>{ORIGIN_COPY[conflict.origin]}</p>}
        {loading && <p role="status">Loading the latest saved revision…</p>}
        {latest && headSplit && (
          <p>
            Revision {latest.head} is the newest saved revision, but revision {latest.version} is the active one
            (the one that runs). Both are shown; Keep mine saves your edits as revision {latest.head + 1}, which
            becomes active.
          </p>
        )}
        {latest && (
          <p>
            Revision {latest.version} was saved after this page loaded the plan. Your edits on this page are
            unsaved. {changed === 0
              ? "Both map every column the same way."
              : `${changed} of ${rows.length} destination column${rows.length === 1 ? "" : "s"} differ.`}
          </p>
        )}
        {conflict?.error && (
          <p className="df2-alert df2-alert-error" role="alert">{conflict.error}</p>
        )}
        {latest && (
          <table className="df2-table df2-plan-conflict-table">
            <caption>Source column mapped to each destination column</caption>
            <thead>
              <tr>
                <th scope="col">Destination column</th>
                <th scope="col">{headSplit ? `Active (revision ${latest.version})` : `Latest (revision ${latest.version})`}</th>
                {headSplit && <th scope="col">Newest (revision {latest.head})</th>}
                <th scope="col">Your unsaved edits</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={row.target}
                  className={row.differs ? "is-different" : undefined}
                  data-differs={row.differs ? "true" : "false"}
                >
                  <th scope="row">{row.target}</th>
                  <td>
                    {row.latestSource ?? "— not mapped"}
                    {row.latestSources.length > 1 && <em> (duplicate: {row.latestSources.length} sources)</em>}
                  </td>
                  {headSplit && <td>{headByTarget.get(row.target.toLowerCase()) ?? "— not mapped"}</td>}
                  <td>
                    {row.mineSource ?? "— not mapped"}
                    {row.mineSources.length > 1 && <em> (duplicate: {row.mineSources.length} sources)</em>}
                    {row.differs && <strong className="df2-plan-conflict-diff"> (different)</strong>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {latest && (
          <ul className="df2-plan-conflict-choices">
            <li><strong>Use latest</strong> shows revision {latest.version} in the editor and drops your unsaved edits.</li>
            <li><strong>Keep mine</strong> saves your edits as revision {latest.head + 1}, which replaces revision {latest.version} as the active mappings.</li>
            <li><strong>Cancel</strong> keeps your edits on screen, unsaved; saving later will ask again.</li>
          </ul>
        )}
      </div>
    </Dialog>
  );
}
