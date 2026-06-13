import { ProgressBar } from "./ProgressBar";

interface TransferProgressProps {
  currentChunk: number;
  totalChunks: number;
  rowsProcessed: number;
  status: "idle" | "running" | "completed" | "failed" | "blocked";
  message?: string;
}

const STATUS_LABEL: Record<TransferProgressProps["status"], string> = {
  idle: "Ready",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
  blocked: "Blocked",
};

export function TransferProgress({
  currentChunk,
  totalChunks,
  rowsProcessed,
  status,
  message,
}: TransferProgressProps) {
  const pct = totalChunks > 0 ? Math.round((currentChunk / totalChunks) * 100) : 0;
  const tone =
    status === "completed" ? "mint" : status === "failed" || status === "blocked" ? "danger" : "brand";

  return (
    <div className="df-transfer-progress">
      <ProgressBar
        value={status === "running" || status === "completed" ? pct : 0}
        indeterminate={status === "running" && currentChunk === 0}
        label="Transfer progress"
        sublabel={STATUS_LABEL[status]}
        tone={tone}
      />
      <p className="df-transfer-progress-meta df-mono">
        Chunk {currentChunk}/{totalChunks} · {rowsProcessed.toLocaleString()} rows
        {message ? ` · ${message}` : ""}
      </p>
    </div>
  );
}
