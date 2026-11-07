/**
 * Job Theater "Discard staging" panel.
 *
 * QA (demo videos): the discard failed with a 409 and the page showed nothing —
 * the only report was a toast that was gone by the time anyone looked, and the
 * button stayed as if nothing had happened. A refusal stays on the panel until
 * the next attempt; a discard the job records replaces the button.
 */
export interface StagingRollbackPanelProps {
  stagingTable: string;
  busy: boolean;
  /** Refusal from the last attempt (API detail), shown until the next one. */
  error?: string | null;
  /** The job records the discard (``rollback_execution.status == "executed"``). */
  discarded?: boolean;
  onDiscard: () => void;
}

export function StagingRollbackPanel({
  stagingTable,
  busy,
  error,
  discarded,
  onDiscard,
}: StagingRollbackPanelProps) {
  if (discarded) {
    return (
      <section className="df2-theater-rollback" aria-label="Staging rollback">
        <div>
          <strong>Staging table discarded</strong>
          <p role="status">
            <code>{stagingTable}</code> was dropped. Promoted destination rows are unchanged.
          </p>
        </div>
      </section>
    );
  }
  return (
    <section className="df2-theater-rollback" aria-label="Staging rollback">
      <div>
        <strong>Discard staging table</strong>
        <p>
          Signed plan <code>DISCARD_STAGING</code> for{" "}
          <code>{stagingTable}</code>. Drops staging only —
          never undoes promoted destination rows (population_undo_claimed=false).
        </p>
        {error ? (
          <div className="df2-alert df2-alert-error" role="alert">
            Discard refused — the staging table is still there. {error}
          </div>
        ) : null}
      </div>
      <button
        type="button"
        className="df2-btn df2-btn-sm df2-btn-danger"
        disabled={busy}
        onClick={onDiscard}
      >
        {busy ? "Discarding…" : "Discard staging"}
      </button>
    </section>
  );
}

/** True when the job records an executed discard for this staging table. */
export function stagingDiscardRecorded(
  execution: unknown,
  stagingTable: string,
): boolean {
  if (!execution || typeof execution !== "object") return false;
  const rec = execution as { status?: unknown; strategy?: unknown; staging_table?: unknown };
  return (
    rec.status === "executed"
    && rec.strategy === "DISCARD_STAGING"
    && (!rec.staging_table || String(rec.staging_table) === stagingTable)
  );
}
