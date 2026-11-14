/**
 * In the Validate panel while a plan conflict is unresolved: says why there
 * is no verdict and re-opens the comparison (instead of a blank panel).
 */
export function PlanConflictNote({ note, onResolve }: { note: string; onResolve?: () => void }) {
  if (!note) return null;
  return (
    <div className="df2-vd-exec-summary" role="alert" data-testid="validate-plan-conflict">
      <p className="df2-vd-exec-summary-sub">{note}</p>
      {onResolve && (
        <button type="button" className="df2-btn df2-btn-sm" onClick={onResolve}>
          Compare with the latest revision
        </button>
      )}
    </div>
  );
}
