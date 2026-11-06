import { HOLD_OUT_LABEL, type FileSourceIssueHold } from "../../lib/fileSourceIssueHold";

/** Validate-step decision for file values that do not fit (QA browser 3). */
export function FileSourceIssuePanel({
  hold,
  onHoldOut,
  onFixFile,
}: {
  hold: FileSourceIssueHold;
  onHoldOut: () => void;
  onFixFile: () => void;
}) {
  return (
    <section className="df2-alert df2-alert-warning" role="alert" data-testid="file-source-issue-hold">
      <div>
        <strong>{hold.title}</strong>
        <p>{hold.message}</p>
        {hold.issues.length > 0 && (
          <ul>
            {hold.issues.map((issue) => (
              <li key={issue}>{issue}</li>
            ))}
          </ul>
        )}
        <div className="df2-vd-exec-until">
          {hold.canHoldOut && (
            <>
              <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onHoldOut}>
                {HOLD_OUT_LABEL}
              </button>{" "}
            </>
          )}
          <button type="button" className="df2-btn df2-btn-sm" onClick={onFixFile}>
            Fix the file
          </button>
        </div>
      </div>
    </section>
  );
}
