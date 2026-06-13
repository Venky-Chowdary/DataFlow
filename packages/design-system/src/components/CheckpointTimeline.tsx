export interface CheckpointItem {
  chunk: number;
  total: number;
  rows: number;
  at: string;
}

interface CheckpointTimelineProps {
  checkpoints: CheckpointItem[];
  currentChunk?: number;
  totalChunks?: number;
  status?: string;
}

/** Checkpoint resume visualization — plan Part 1 architecture */
export function CheckpointTimeline({ checkpoints, currentChunk, totalChunks, status }: CheckpointTimelineProps) {
  const total = totalChunks ?? checkpoints[checkpoints.length - 1]?.total ?? 0;
  const current = currentChunk ?? checkpoints[checkpoints.length - 1]?.chunk ?? 0;

  if (total === 0 && checkpoints.length === 0) {
    return <p className="df-file-meta">No checkpoints yet</p>;
  }

  const segments = total || checkpoints.length;

  return (
    <div className="df-checkpoint-timeline">
      <div className="df-checkpoint-header">
        <span className="df-checkpoint-label">Checkpoints</span>
        <span className="df-mono df-checkpoint-count">
          {current}/{segments}
          {status && ` · ${status}`}
        </span>
      </div>
      <div className="df-checkpoint-track" aria-hidden>
        {Array.from({ length: segments }, (_, i) => {
          const idx = i + 1;
          const done = idx <= current || status === "completed";
          const active = idx === current + 1 && status === "running";
          return (
            <span
              key={idx}
              className={[
                "df-checkpoint-node",
                done ? "df-checkpoint-node--done" : "",
                active ? "df-checkpoint-node--active" : "",
              ]
                .filter(Boolean)
                .join(" ")}
              title={`Chunk ${idx}`}
            />
          );
        })}
      </div>
      {checkpoints.length > 0 && (
        <ul className="df-checkpoint-log">
          {checkpoints.slice(-5).map((cp) => (
            <li key={`${cp.chunk}-${cp.at}`} className="df-mono">
              Chunk {cp.chunk}/{cp.total} · {cp.rows.toLocaleString()} rows
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
