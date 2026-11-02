/**
 * Shown only when the workspace asked for a studio surface this browser has
 * not accepted, or when settings could not be read. Absent otherwise, so the
 * classic screens keep their tree.
 */
export function StudioFlagBanner({
  error,
  optIn,
  onOptIn,
  kind,
}: {
  error: string;
  optIn: boolean;
  onOptIn: () => void;
  kind: "canvas" | "schema";
}) {
  if (error) {
    return (
      <div className="df2-alert df2-alert-error" role="alert" data-testid="studio-flags-error">
        <div>
          <p>Workspace settings could not be loaded. Studio surfaces stay off. {error}</p>
        </div>
      </div>
    );
  }
  if (!optIn) return null;
  const reason = kind === "canvas"
    ? "This workspace uses the recipe canvas. This browser is still on the classic editor."
    : "This workspace uses the schema browser. This browser has not turned it on.";
  const action = kind === "canvas" ? "Use the recipe canvas" : "Use the schema browser";
  const testId = kind === "canvas" ? "studio-canvas-opt-in" : "studio-schema-opt-in";
  return (
    <div className="df2-alert df2-alert-info" role="status" data-testid={testId}>
      <div>
        <p>{reason}</p>
        <button type="button" className="df2-btn df2-btn-sm" onClick={onOptIn}>
          {action}
        </button>
      </div>
    </div>
  );
}
