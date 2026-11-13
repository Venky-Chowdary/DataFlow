import { useEffect, useId, useState } from "react";
import { Dialog } from "../ui/Dialog";
import {
  deleteMappingTemplate,
  listMappingTemplates,
  type MappingTemplateSummary,
} from "../../lib/mappingStudioApi";

interface TemplatesDialogProps {
  open: boolean;
  onClose: () => void;
  onSave: (name: string, description: string) => Promise<void>;
  onApply: (template: MappingTemplateSummary) => void;
}

/**
 * Save the current mapping as a workspace template, or apply one to this
 * table. Applying re-binds the template onto these columns on the server and
 * opens the usual preview — nothing changes until the operator applies it.
 */
export function TemplatesDialog({ open, onClose, onSave, onApply }: TemplatesDialogProps) {
  const nameId = useId();
  const descId = useId();
  const [templates, setTemplates] = useState<MappingTemplateSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  const load = () => {
    setLoading(true);
    setError("");
    listMappingTemplates()
      .then(setTemplates)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : "Could not list templates"))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (open) load();
  }, [open]);

  const save = async () => {
    if (!name.trim()) return;
    setError("");
    try {
      await onSave(name.trim(), description.trim());
      setStatus(`Saved “${name.trim()}”.`);
      setName("");
      setDescription("");
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not save the template");
    }
  };

  const remove = async (t: MappingTemplateSummary) => {
    setError("");
    try {
      await deleteMappingTemplate(t.id);
      setStatus(`Deleted “${t.name}”.`);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not delete the template");
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      size="lg"
      title="Mapping templates"
      subtitle="Reuse a mapping on another table: names are re-bound by exact, normalized, learned and synonym matches, then previewed."
      className="df2-ms-templates"
      footer={<button type="button" className="df2-btn" onClick={onClose}>Close</button>}
    >
      <div className="df2-ms-form">
        <h3 className="df2-ms-panel-title">Save this mapping</h3>
        <label htmlFor={nameId}>Name</label>
        <input id={nameId} className="df2-input" value={name} onChange={(e) => setName(e.target.value)} placeholder="Orders onto customers" />
        <label htmlFor={descId}>Description (optional)</label>
        <input id={descId} className="df2-input" value={description} onChange={(e) => setDescription(e.target.value)} />
        <div>
          <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" disabled={!name.trim()} onClick={() => void save()}>
            Save as template
          </button>
        </div>
      </div>
      <p className="df2-sr-only" role="status" aria-live="polite">{status}</p>
      {error ? <p className="df2-ms-check is-error" role="alert">{error}</p> : null}
      <h3 className="df2-ms-panel-title">Workspace templates</h3>
      {loading ? (
        <p role="status">Loading templates…</p>
      ) : templates.length === 0 ? (
        <p className="df2-ms-panel-note">No templates in this workspace yet.</p>
      ) : (
        <ul className="df2-ms-template-list" aria-label="Workspace templates">
          {templates.map((t) => (
            <li key={t.id} className="df2-ms-template-row">
              <div>
                <strong>{t.name}</strong>
                <span className="df2-ms-panel-note">
                  {t.mapping_count} mapping{t.mapping_count === 1 ? "" : "s"}
                  {t.computed_count ? ` · ${t.computed_count} computed` : ""}
                  {t.created_by ? ` · ${t.created_by}` : ""}
                </span>
                {t.description ? <span className="df2-ms-panel-note">{t.description}</span> : null}
              </div>
              <div className="df2-ms-toolbar-group">
                <button type="button" className="df2-btn df2-btn-sm" onClick={() => onApply(t)} aria-label={`Apply ${t.name}`}>
                  Apply…
                </button>
                <button type="button" className="df2-btn df2-btn-sm df2-btn-ghost" onClick={() => void remove(t)} aria-label={`Delete ${t.name}`}>
                  Delete
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}
    </Dialog>
  );
}
