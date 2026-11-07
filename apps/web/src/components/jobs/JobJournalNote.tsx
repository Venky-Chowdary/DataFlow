import type { JobProgress } from "../../lib/types";
import { Button } from "../ui/Button";

/** Capture-once outcome on a finished job: kept (with replay) or why not. */
export function JobJournalNote({
  job,
  onReplay,
}: {
  job: Pick<JobProgress, "journal" | "journal_replay"> | null | undefined;
  onReplay?: (journalId: string, stream: string) => void;
}) {
  const journal = job?.journal ?? null;
  const replay = job?.journal_replay ?? null;
  if (!journal && !replay) return null;
  if (replay) {
    return (
      <section className="df2-result-warnings-block" role="status" aria-label="Replayed journal">
        <p className="df2-result-warnings-note">
          Replayed journal <code>{replay.journal_id}</code>{Number(replay.rows) > 0 ? ` — ${Number(replay.rows).toLocaleString()} rows` : " — the captured source file"}; the live source was not read.
        </p>
      </section>
    );
  }
  const streams = journal?.streams ?? [];
  const rows = streams.reduce((sum, s) => sum + (Number(s.rows) || 0), 0);
  return (
    <section className="df2-result-warnings-block" role="status" aria-label="Source journal">
      <p className="df2-result-warnings-note">
        {journal?.pending
          ? "Source journal is committing…"
          : journal?.captured
            ? <>Source journal <code>{journal.journal_id}</code> kept{rows > 0 ? ` — ${rows.toLocaleString()} rows` : " — source file bytes"}
                {journal.storage ? (journal.storage.durable ? " · shared storage" : ` · this host only (${journal.storage.reason || "not mirrored"})`) : ""}
                {journal.expires_at ? ` · expires ${new Date(journal.expires_at * 1000).toLocaleDateString()}` : ""}</>
            : `Source journal not kept: ${journal?.reason || "this run was not journaled"}`}
      </p>
      {journal?.captured && journal.journal_id && onReplay && (
        <div className="df2-journals-actions">
          {(streams.length ? streams : [{ name: "" }]).slice(0, 4).map((stream) => (
            <Button
              key={stream.name || "stream"}
              size="sm"
              onClick={() => onReplay(journal.journal_id as string, streams.length > 1 ? stream.name : "")}
            >
              {streams.length > 1 ? `Replay ${stream.name}` : "Replay to another destination"}
            </Button>
          ))}
        </div>
      )}
    </section>
  );
}
