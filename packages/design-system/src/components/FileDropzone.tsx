import { useRef, useState, type DragEvent, type ReactNode } from "react";
import { Button } from "./Button";

interface FileDropzoneProps {
  title: string;
  hint: string;
  actionLabel?: string;
  onAction?: () => void;
  onFileSelect?: (file: File) => void;
  accept?: string;
  busy?: boolean;
  footer?: ReactNode;
  fileName?: string | null;
  rowCount?: number | null;
}

export function FileDropzone({
  title,
  hint,
  actionLabel,
  onAction,
  onFileSelect,
  accept = ".csv,.json,.txt,.tsv,.xlsx,.parquet",
  busy = false,
  footer,
  fileName,
  rowCount,
}: FileDropzoneProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragOver, setDragOver] = useState(false);

  function handleFile(file: File) {
    onFileSelect?.(file);
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  }

  const hasFile = !!fileName;

  return (
    <div
      className={[
        "df-dropzone",
        "df-dropzone--modern",
        dragOver ? "df-dropzone--active" : "",
        hasFile ? "df-dropzone--has-file" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      onDragOver={(e) => {
        e.preventDefault();
        setDragOver(true);
      }}
      onDragLeave={() => setDragOver(false)}
      onDrop={onDrop}
    >
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0];
          if (file) handleFile(file);
        }}
      />

      <div className="df-dropzone-body">
        <div className="df-dropzone-icon-wrap" aria-hidden>
          {hasFile ? (
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none">
              <path d="M9 12l2 2 4-4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              <circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="2" />
            </svg>
          ) : (
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none">
              <path d="M12 16V4M12 4L8 8M12 4L16 8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              <path d="M4 16V18C4 19.105 4.895 20 6 20H18C19.105 20 20 19.105 20 18V16" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
            </svg>
          )}
        </div>

        <div className="df-dropzone-copy">
          <p className="df-dropzone-title">{hasFile ? fileName : title}</p>
          <p className="df-dropzone-hint">
            {hasFile && rowCount != null ? `${rowCount.toLocaleString()} rows ready` : hint}
          </p>
        </div>
      </div>

      <div className="df-dropzone-actions">
        <Button variant="primary" disabled={busy} onClick={() => inputRef.current?.click()}>
          {busy ? "Analyzing…" : hasFile ? "Replace file" : "Choose file"}
        </Button>
        {onAction && actionLabel && (
          <Button variant="ghost" disabled={busy} onClick={onAction}>
            {actionLabel}
          </Button>
        )}
      </div>

      {footer}
    </div>
  );
}
