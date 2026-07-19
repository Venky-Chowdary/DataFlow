import { useEffect, useId, useRef } from "react";
import { Button } from "../ui/Button";
import { DtIcon } from "../DtIcon";

interface JobNameEditorProps {
  displayName: string;
  editing: boolean;
  draft: string;
  error: string | null;
  saving: boolean;
  onBegin: () => void;
  onDraftChange: (value: string) => void;
  onSave: () => void;
  onCancel: () => void;
}

/**
 * Single place to rename a job — detail pane only.
 * Save / Cancel use the canonical Button sizes so they match the rest of Jobs.
 */
export function JobNameEditor({
  displayName,
  editing,
  draft,
  error,
  saving,
  onBegin,
  onDraftChange,
  onSave,
  onCancel,
}: JobNameEditorProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const errorId = useId();
  const dirty = draft.trim() !== displayName.trim();
  const canSave = Boolean(draft.trim()) && dirty && !error && !saving;

  useEffect(() => {
    if (!editing) return;
    const t = window.requestAnimationFrame(() => {
      inputRef.current?.focus();
      inputRef.current?.select();
    });
    return () => window.cancelAnimationFrame(t);
  }, [editing]);

  if (!editing) {
    return (
      <div className="df2-job-name-editor is-view">
        <h2 className="df2-job-name-editor-title" title={displayName}>
          {displayName}
        </h2>
        <Button
          size="sm"
          variant="ghost"
          className="df2-job-name-rename-btn"
          onClick={onBegin}
          leadingIcon={<DtIcon name="edit" size={12} />}
          aria-label={`Rename ${displayName}`}
          title="Rename job"
        >
          Rename
        </Button>
      </div>
    );
  }

  return (
    <div className={`df2-job-name-editor is-edit${error ? " has-error" : ""}`}>
      <div className="df2-job-name-editor-form">
        <label className="df2-job-name-editor-label" htmlFor={`${errorId}-input`}>
          Job name
        </label>
        <div className="df2-job-name-editor-row">
          <input
            ref={inputRef}
            id={`${errorId}-input`}
            className="df2-input df2-job-name-editor-input"
            value={draft}
            onChange={(e) => onDraftChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                if (canSave) onSave();
              }
              if (e.key === "Escape") {
                e.preventDefault();
                onCancel();
              }
            }}
            maxLength={120}
            aria-invalid={Boolean(error) || undefined}
            aria-describedby={error ? errorId : undefined}
            disabled={saving}
            autoComplete="off"
            spellCheck={false}
          />
          <div className="df2-job-name-editor-actions">
            <Button
              size="sm"
              variant="primary"
              onClick={onSave}
              disabled={!canSave}
              loading={saving}
              loadingLabel="Saving…"
            >
              Save
            </Button>
            <Button size="sm" variant="ghost" onClick={onCancel} disabled={saving}>
              Cancel
            </Button>
          </div>
        </div>
        {error ? (
          <p id={errorId} className="df2-job-name-editor-error" role="alert">
            {error}
          </p>
        ) : (
          <p className="df2-job-name-editor-hint">
            Enter to save · Escape to cancel · names must be unique in this workspace
          </p>
        )}
      </div>
    </div>
  );
}
