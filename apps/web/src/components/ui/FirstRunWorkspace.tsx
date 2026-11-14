import { useCallback, useEffect, useId, useRef, useState, type FormEvent } from "react";

import { createWorkspace, fetchWorkspaces } from "../../lib/api";
import {
  FIRST_RUN_COPY,
  WORKSPACE_CREATE_REQUEST_EVENT,
  firstRunState,
  workspaceNameProblem,
  type FirstRunState,
} from "../../lib/firstRunWorkspace";
import { isUiPreviewSession } from "../../lib/session";
import { WORKSPACE_DIRECTORY_EVENT, notifyWorkspaceDirectory, setActiveWorkspaceId } from "../../lib/workspace";
import { DtIcon } from "../DtIcon";
import { Button } from "./Button";
import { Dialog } from "./Dialog";

/**
 * First-run workspace (UX-1): with no workspace, the first sign-in asks a
 * platform administrator to create one before anything can fail for want of
 * it. Closing the dialog leaves a banner with the same action; the switcher's
 * "Create workspace…" option opens the same dialog at any time.
 */
export function FirstRunWorkspace({ onCreated }: { onCreated?: (workspaceId: string) => void }) {
  const [state, setState] = useState<FirstRunState>("unknown");
  const [canCreate, setCanCreate] = useState(false);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [error, setError] = useState("");
  const [saving, setSaving] = useState(false);
  const autoOpened = useRef(false);
  const inputId = useId();
  const errorId = useId();

  const reload = useCallback(() => {
    // The dev UI preview is not an account: it has nothing to set up.
    if (isUiPreviewSession()) return;
    fetchWorkspaces()
      .then((dir) => {
        const next = firstRunState(dir);
        setState(next);
        setCanCreate(dir.platformAdmin);
        if (next === "create" && !autoOpened.current) {
          autoOpened.current = true;
          setOpen(true);
        }
      })
      .catch(() => setState("unknown"));
  }, []);

  useEffect(() => {
    reload();
    window.addEventListener(WORKSPACE_DIRECTORY_EVENT, reload);
    return () => window.removeEventListener(WORKSPACE_DIRECTORY_EVENT, reload);
  }, [reload]);

  useEffect(() => {
    const onRequest = () => {
      setError("");
      setOpen(true);
    };
    window.addEventListener(WORKSPACE_CREATE_REQUEST_EVENT, onRequest);
    return () => window.removeEventListener(WORKSPACE_CREATE_REQUEST_EVENT, onRequest);
  }, []);

  const submit = async (e?: FormEvent) => {
    e?.preventDefault();
    const problem = workspaceNameProblem(name);
    if (problem) {
      setError(problem);
      return;
    }
    setSaving(true);
    setError("");
    try {
      const ws = await createWorkspace(name.trim());
      setOpen(false);
      setName("");
      setState("ready");
      setActiveWorkspaceId(ws.id);
      notifyWorkspaceDirectory();
      onCreated?.(ws.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "The workspace could not be created. Try again.");
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      {state === "create" && !open && (
        <div className="df2-first-run-banner df2-alert df2-alert-info" role="region" aria-label={FIRST_RUN_COPY.bannerTitle} data-testid="first-run-banner">
          <DtIcon name="users" size={18} />
          <div>
            <strong>{FIRST_RUN_COPY.bannerTitle}</strong>
            <p>{FIRST_RUN_COPY.bannerBody}</p>
          </div>
          <Button size="sm" variant="primary" onClick={() => setOpen(true)}>
            {FIRST_RUN_COPY.createAction}
          </Button>
        </div>
      )}
      {state === "ask-admin" && (
        <div className="df2-first-run-banner df2-alert df2-alert-info" role="region" aria-label={FIRST_RUN_COPY.askAdminTitle} data-testid="first-run-ask-admin">
          <DtIcon name="users" size={18} />
          <div>
            <strong>{FIRST_RUN_COPY.askAdminTitle}</strong>
            <p>{FIRST_RUN_COPY.askAdminBody}</p>
          </div>
        </div>
      )}
      <Dialog
        open={open && canCreate}
        onClose={() => setOpen(false)}
        size="md"
        title={FIRST_RUN_COPY.createTitle}
        subtitle={FIRST_RUN_COPY.createBody}
        className="df2-first-run-dialog"
        footer={
          <>
            <Button variant="secondary" onClick={() => setOpen(false)} disabled={saving}>
              Not now
            </Button>
            <Button variant="primary" loading={saving} loadingLabel="Creating…" onClick={() => void submit()}>
              {FIRST_RUN_COPY.createAction}
            </Button>
          </>
        }
      >
        <form onSubmit={(e) => void submit(e)} className="df2-first-run-form" noValidate>
          <label htmlFor={inputId} className="df2-label">
            {FIRST_RUN_COPY.createLabel}
          </label>
          <input
            id={inputId}
            className="df2-input"
            value={name}
            maxLength={128}
            autoFocus
            autoComplete="organization"
            placeholder={FIRST_RUN_COPY.createPlaceholder}
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? errorId : undefined}
            onChange={(e) => {
              setName(e.target.value);
              if (error) setError("");
            }}
          />
          {error && (
            <p id={errorId} className="df2-field-error-text" role="alert">
              {error}
            </p>
          )}
        </form>
      </Dialog>
    </>
  );
}
