/**
 * Declare (or edit) a system: what you would have to take with you, from which
 * saved connection, and the window an exit must fit in (a contract clause or
 * the EU Data Act switching window). The API checks every field again (422).
 */

import { connectorDisplayName } from "../../lib/connectorTypes";
import { useEffect, useId, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { Drawer } from "../ui/Drawer";
import {
  CRITICALITIES,
  SIGNOFF_ROLES,
  criticalityPresentation,
  draftFromSystem,
  emptyDraft,
  exitErrorMessage,
  validateDraft,
  type ExitSystem,
  type ExitSystemInput,
  type SystemDraft,
} from "../../lib/exitReadiness";
import { roleLabel } from "../../lib/parity";
import type { Connector } from "../../lib/types";

export interface SystemFormDrawerProps {
  open: boolean;
  /** Editing this system; null declares a new one. */
  system: ExitSystem | null;
  connectors: Connector[];
  onClose: () => void;
  onSubmit: (input: ExitSystemInput, system: ExitSystem | null) => Promise<void>;
}

function FieldError({ id, message }: { id: string; message?: string }) {
  if (!message) return null;
  return (
    <p id={id} className="df2-exit-field-error" role="alert">
      <DtIcon name="alert" size={12} /> {message}
    </p>
  );
}

export function SystemFormDrawer({ open, system, connectors, onClose, onSubmit }: SystemFormDrawerProps) {
  const [draft, setDraft] = useState<SystemDraft>(() => (system ? draftFromSystem(system) : emptyDraft()));
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitError, setSubmitError] = useState("");
  const [saving, setSaving] = useState(false);
  const id = useId();
  const f = (name: string) => `${id}-${name}`;

  useEffect(() => {
    if (!open) return;
    setDraft(system ? draftFromSystem(system) : emptyDraft());
    setErrors({});
    setSubmitError("");
  }, [open, system]);

  const set = <K extends keyof SystemDraft>(key: K, value: SystemDraft[K]) => setDraft((d) => ({ ...d, [key]: value }));
  const sources = connectors.filter((c) => (c.role || "both") !== "destination");
  const connectorMissing = Boolean(draft.connectorId) && !sources.some((c) => c.id === draft.connectorId);

  const submit = async () => {
    const { errors: found, input } = validateDraft(draft);
    setErrors(found);
    setSubmitError("");
    if (!input) return;
    setSaving(true);
    try {
      await onSubmit(input, system);
    } catch (err) {
      setSubmitError(exitErrorMessage(err, system ? "Could not save this system." : "Could not declare this system."));
    } finally {
      setSaving(false);
    }
  };

  const describedBy = (name: string) => (errors[name] ? `${f(name)}-error` : undefined);

  return (
    <Drawer
      open={open}
      onClose={onClose}
      title={system ? `Edit ${system.name}` : "Declare a system"}
      subtitle="What you would have to take with you, and the window an exit must fit in."
      ariaLabel={system ? `Edit exit system ${system.name}` : "Declare an exit system"}
      className="df2-exit-drawer"
      footer={
        <div className="df2-exit-drawer-footer">
          <span />
          <span className="df2-exit-drawer-footer-end">
            <Button size="sm" variant="ghost" onClick={onClose} disabled={saving}>Cancel</Button>
            <Button size="sm" variant="primary" onClick={() => void submit()} loading={saving} loadingLabel="Saving…">
              {system ? "Save changes" : "Declare system"}
            </Button>
          </span>
        </div>
      }
    >
      <form
        className="df2-exit-form"
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
        noValidate
      >
        {submitError && (
          <p className="df2-parity-notice is-danger" role="alert">
            <DtIcon name="alert" size={14} />
            <span>{submitError}</span>
          </p>
        )}

        <fieldset className="df2-exit-fieldset">
          <legend>System</legend>
          <div className="df2-field">
            <label className="df2-label" htmlFor={f("name")}>Name</label>
            <input id={f("name")} className="df2-input" value={draft.name} maxLength={120} onChange={(e) => set("name", e.target.value)} aria-invalid={Boolean(errors.name) || undefined} aria-describedby={describedBy("name")} placeholder="Core ledger" />
            <FieldError id={`${f("name")}-error`} message={errors.name} />
          </div>
          <div className="df2-field">
            <label className="df2-label" htmlFor={f("description")}>Description (optional)</label>
            <textarea id={f("description")} className="df2-input df2-parity-note-input" rows={2} maxLength={500} value={draft.description} onChange={(e) => set("description", e.target.value)} />
          </div>
          <div className="df2-exit-form-row">
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("criticality")}>Criticality</label>
              <select id={f("criticality")} className="df2-select" value={draft.criticality} onChange={(e) => set("criticality", e.target.value as SystemDraft["criticality"])}>
                {CRITICALITIES.map((c) => (
                  <option key={c} value={c}>{criticalityPresentation(c).label}</option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("connector")}>Source connection</label>
              <select id={f("connector")} className="df2-select" value={draft.connectorId} onChange={(e) => set("connectorId", e.target.value)} aria-invalid={Boolean(errors.connector) || undefined} aria-describedby={describedBy("connector")}>
                <option value="">Choose a saved connection…</option>
                {connectorMissing && <option value={draft.connectorId}>{system?.source.connector_name || draft.connectorId}</option>}
                {sources.map((c) => (
                  <option key={c.id} value={c.id}>{c.name || c.id} ({connectorDisplayName(c.type)})</option>
                ))}
              </select>
              <FieldError id={`${f("connector")}-error`} message={errors.connector} />
            </div>
          </div>
        </fieldset>

        <fieldset className="df2-exit-fieldset" aria-describedby={describedBy("tables")}>
          <legend>Tables to take</legend>
          <p className="df2-parity-muted">Leave the key empty to use the source catalog's primary key. A table with no key is proven by its row digest.</p>
          {draft.tables.map((t, i) => (
            <div key={i} className="df2-exit-form-table">
              <div className="df2-field">
                <label className="df2-label" htmlFor={f(`t${i}-schema`)}>Schema</label>
                <input id={f(`t${i}-schema`)} className="df2-input" value={t.schema} placeholder="public" onChange={(e) => set("tables", draft.tables.map((x, j) => (j === i ? { ...x, schema: e.target.value } : x)))} />
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor={f(`t${i}-name`)}>Table</label>
                <input id={f(`t${i}-name`)} className="df2-input" value={t.name} placeholder="orders" onChange={(e) => set("tables", draft.tables.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))} />
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor={f(`t${i}-keys`)}>Key columns</label>
                <input id={f(`t${i}-keys`)} className="df2-input" value={t.keys} placeholder="id" onChange={(e) => set("tables", draft.tables.map((x, j) => (j === i ? { ...x, keys: e.target.value } : x)))} />
              </div>
              <Button
                size="sm"
                variant="ghost"
                className="df2-exit-form-remove"
                aria-label={`Remove table ${t.name || i + 1}`}
                disabled={draft.tables.length === 1}
                onClick={() => set("tables", draft.tables.filter((_, j) => j !== i))}
              >
                <DtIcon name="x" size={14} />
              </Button>
            </div>
          ))}
          <FieldError id={`${f("tables")}-error`} message={errors.tables} />
          <div className="df2-parity-actions">
            <Button size="sm" variant="ghost" onClick={() => set("tables", [...draft.tables, { name: "", schema: "", keys: "" }])} leadingIcon={<DtIcon name="plus" size={14} />}>
              Add table
            </Button>
          </div>
        </fieldset>

        <fieldset className="df2-exit-fieldset">
          <legend>Exit window</legend>
          <div className="df2-exit-form-row">
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("window")}>An exit must fit in</label>
              <span className="df2-exit-form-inline">
                <input id={f("window")} className="df2-input" type="number" min={1} inputMode="numeric" value={draft.windowValue} onChange={(e) => set("windowValue", e.target.value)} aria-invalid={Boolean(errors.window) || undefined} aria-describedby={describedBy("window")} />
                <select className="df2-select" aria-label="Window unit" value={draft.windowUnit} onChange={(e) => set("windowUnit", e.target.value as SystemDraft["windowUnit"])}>
                  <option value="days">days</option>
                  <option value="hours">hours</option>
                </select>
              </span>
              <FieldError id={`${f("window")}-error`} message={errors.window} />
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("basis")}>Where the window comes from</label>
              <input id={f("basis")} className="df2-input" value={draft.windowBasis} maxLength={200} placeholder="EU Data Act switching window" onChange={(e) => set("windowBasis", e.target.value)} />
            </div>
          </div>
          <div className="df2-field" aria-describedby={describedBy("regulatoryDates")}>
            <span className="df2-label">Regulatory dates (optional)</span>
            {draft.regulatoryDates.map((d, i) => (
              <div key={i} className="df2-exit-form-date">
                <input className="df2-input" type="date" aria-label={`Regulatory date ${i + 1}`} value={d.date} onChange={(e) => set("regulatoryDates", draft.regulatoryDates.map((x, j) => (j === i ? { ...x, date: e.target.value } : x)))} />
                <input className="df2-input" aria-label={`What happens on regulatory date ${i + 1}`} placeholder="Data Act switching charges end" value={d.label} onChange={(e) => set("regulatoryDates", draft.regulatoryDates.map((x, j) => (j === i ? { ...x, label: e.target.value } : x)))} />
                <Button size="sm" variant="ghost" aria-label={`Remove regulatory date ${i + 1}`} onClick={() => set("regulatoryDates", draft.regulatoryDates.filter((_, j) => j !== i))}>
                  <DtIcon name="x" size={14} />
                </Button>
              </div>
            ))}
            <FieldError id={`${f("regulatoryDates")}-error`} message={errors.regulatoryDates} />
            <div className="df2-parity-actions">
              <Button size="sm" variant="ghost" onClick={() => set("regulatoryDates", [...draft.regulatoryDates, { date: "", label: "" }])} leadingIcon={<DtIcon name="calendar" size={14} />}>
                Add a date
              </Button>
            </div>
          </div>
        </fieldset>

        <fieldset className="df2-exit-fieldset">
          <legend>Rehearsals</legend>
          <div className="df2-exit-form-row">
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("every")}>Overdue after (days)</label>
              <input id={f("every")} className="df2-input" type="number" min={1} max={3650} inputMode="numeric" value={draft.everyDays} onChange={(e) => set("everyDays", e.target.value)} aria-invalid={Boolean(errors.everyDays) || undefined} aria-describedby={describedBy("everyDays")} />
              <FieldError id={`${f("everyDays")}-error`} message={errors.everyDays} />
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("format")}>Extract format</label>
              <select id={f("format")} className="df2-select" value={draft.extractFormat} onChange={(e) => set("extractFormat", e.target.value as SystemDraft["extractFormat"])}>
                <option value="jsonl">JSON Lines</option>
                <option value="csv">CSV</option>
              </select>
            </div>
          </div>
          <div className="df2-exit-form-row">
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("cron")}>Recurring schedule (cron, optional)</label>
              <input id={f("cron")} className="df2-input df2-mono" value={draft.scheduleCron} placeholder="0 2 1 * *" onChange={(e) => set("scheduleCron", e.target.value)} aria-invalid={Boolean(errors.schedule) || undefined} aria-describedby={describedBy("schedule")} />
              <FieldError id={`${f("schedule")}-error`} message={errors.schedule} />
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor={f("tz")}>Time zone</label>
              <input id={f("tz")} className="df2-input" value={draft.scheduleTimezone} placeholder="UTC" onChange={(e) => set("scheduleTimezone", e.target.value)} />
            </div>
          </div>
        </fieldset>

        <fieldset className="df2-exit-fieldset" aria-describedby={describedBy("roles")}>
          <legend>Sign-off roles</legend>
          <p className="df2-parity-muted">Every chosen role signs a proven rehearsal before its evidence pack is signed.</p>
          <div className="df2-exit-roles">
            {SIGNOFF_ROLES.map((role) => (
              <label key={role} className="df2-exit-role">
                <input
                  type="checkbox"
                  aria-label={`Require sign-off from ${roleLabel(role)}`}
                  checked={draft.requiredRoles.includes(role)}
                  onChange={(e) =>
                    set("requiredRoles", e.target.checked ? [...draft.requiredRoles, role] : draft.requiredRoles.filter((r) => r !== role))
                  }
                />
                <span>{roleLabel(role)}</span>
              </label>
            ))}
          </div>
          <FieldError id={`${f("roles")}-error`} message={errors.roles} />
        </fieldset>
        <button type="submit" hidden aria-hidden tabIndex={-1} />
      </form>
    </Drawer>
  );
}
