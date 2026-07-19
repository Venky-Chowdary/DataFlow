import { FormEvent, useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { ConnectorSelect } from "../ui/ConnectorSelect";
import { CadenceTiles } from "../ui/CadenceTiles";
import type {
  Connector,
  PipelineSchedule,
  ScheduleInput,
  ScheduleIntervals,
} from "../../lib/types";

interface ScheduleFormProps {
  connectors: Connector[];
  intervals: ScheduleIntervals | null;
  /** When present the form edits an existing schedule; otherwise it creates one. */
  initial?: PipelineSchedule | null;
  saving?: boolean;
  onSubmit: (input: Partial<ScheduleInput>) => void;
  onCancel: () => void;
}

const SYNC_MODE_META: Record<string, { label: string; detail: string }> = {
  full_refresh_overwrite: { label: "Full overwrite", detail: "Snapshot replaces destination rows each run." },
  full_refresh_append: { label: "Full append", detail: "Snapshot appended to destination history." },
  incremental: { label: "Incremental", detail: "Cursor-based sync of new / changed rows." },
  cdc: { label: "CDC", detail: "Change data capture with cursor + key contract." },
  scd2: { label: "SCD Type 2", detail: "Versioned history with valid-from / valid-to; requires primary key." },
  mirror: { label: "Mirror", detail: "Keep destination in sync with inferred deletes; requires primary key." },
};

const VALIDATION_MODES = [
  { id: "balanced", label: "Balanced" },
  { id: "strict", label: "Strict" },
  { id: "maximum", label: "Maximum" },
];

const SCHEMA_POLICIES = [
  { id: "manual_review", label: "Manual approval" },
  { id: "propagate_columns", label: "Column changes" },
  { id: "propagate_all", label: "All changes" },
  { id: "pause_on_change", label: "Pause on change" },
];

const COMMON_TIMEZONES = [
  "UTC",
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Los_Angeles",
  "America/Sao_Paulo",
  "Europe/London",
  "Europe/Paris",
  "Europe/Berlin",
  "Asia/Kolkata",
  "Asia/Singapore",
  "Asia/Tokyo",
  "Australia/Sydney",
];

const DEFAULT_SYNC_MODES = ["full_refresh_overwrite", "full_refresh_append", "incremental", "cdc", "scd2", "mirror"];

function formatWhen(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString(undefined, {
      weekday: "short",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

export function ScheduleForm({ connectors, intervals, initial, saving, onSubmit, onCancel }: ScheduleFormProps) {
  const isEdit = Boolean(initial);
  const [name, setName] = useState(initial?.name ?? "");
  const [sourceId, setSourceId] = useState(initial?.source_connector_id ?? connectors[0]?.id ?? "");
  const [sourceTable, setSourceTable] = useState(initial?.source_table ?? "");
  const [destId, setDestId] = useState(initial?.dest_connector_id ?? connectors[1]?.id ?? connectors[0]?.id ?? "");
  const [destTable, setDestTable] = useState(initial?.dest_table ?? "");

  // Cadence
  const [cadenceMode, setCadenceMode] = useState<"preset" | "cron">(initial?.cron ? "cron" : "preset");
  const [interval, setIntervalValue] = useState<"hourly" | "daily" | "weekly">(
    (initial?.interval as "hourly" | "daily" | "weekly") ?? "daily",
  );
  const [cron, setCron] = useState(initial?.cron ?? "");
  const [timezone, setTimezone] = useState(initial?.timezone ?? "UTC");

  // Sync
  const [syncMode, setSyncMode] = useState(initial?.sync_mode ?? "full_refresh_overwrite");
  const [cursorColumn, setCursorColumn] = useState(initial?.cursor_column ?? "");
  const [primaryKey, setPrimaryKey] = useState(initial?.primary_key ?? "");
  const [validationMode, setValidationMode] = useState(initial?.validation_mode ?? "balanced");
  const [schemaPolicy, setSchemaPolicy] = useState(initial?.schema_policy ?? "manual_review");
  const [backfill, setBackfill] = useState(initial?.backfill_new_fields ?? false);

  // Retry & notifications
  const [maxRetries, setMaxRetries] = useState(initial?.max_retries ?? 2);
  const [retryBackoff, setRetryBackoff] = useState(initial?.retry_backoff_seconds ?? 30);
  const [notifyFailure, setNotifyFailure] = useState(initial?.notify_on_failure ?? true);
  const [notifySuccess, setNotifySuccess] = useState(initial?.notify_on_success ?? false);

  const syncModes = intervals?.sync_modes?.length ? intervals.sync_modes : DEFAULT_SYNC_MODES;
  const showCursor = syncMode === "incremental" || syncMode === "cdc";
  const showPrimaryKey = showCursor || syncMode === "scd2" || syncMode === "mirror";

  const sourceConnector = connectors.find((c) => c.id === sourceId);
  const destConnector = connectors.find((c) => c.id === destId);
  const sourceStreamLabel = sourceConnector?.type === "mongodb" ? "collection" : "table";
  const destStreamLabel = destConnector?.type === "mongodb" ? "collection" : "table";

  const timezoneOptions = useMemo(() => {
    const set = new Set(COMMON_TIMEZONES);
    if (timezone) set.add(timezone);
    return [...set];
  }, [timezone]);

  const backoffPreview = useMemo(() => {
    if (maxRetries <= 0) return "No retries — the run fails immediately on error.";
    const delays = Array.from({ length: Math.min(maxRetries, 3) }, (_, i) => `${retryBackoff * (i + 1)}s`);
    const suffix = maxRetries > 3 ? ", …" : "";
    return `Up to ${maxRetries} retr${maxRetries === 1 ? "y" : "ies"} — linear backoff before attempts: ${delays.join(", ")}${suffix}.`;
  }, [maxRetries, retryBackoff]);

  const canSubmit = Boolean(
    name.trim() && sourceId && destId && sourceTable.trim() && destTable.trim() && (cadenceMode === "preset" || cron.trim()),
  );

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      name: name.trim(),
      source_connector_id: sourceId,
      source_table: sourceTable.trim(),
      dest_connector_id: destId,
      dest_table: destTable.trim(),
      interval,
      cron: cadenceMode === "cron" ? cron.trim() : "",
      timezone,
      sync_mode: syncMode,
      validation_mode: validationMode,
      schema_policy: schemaPolicy,
      backfill_new_fields: backfill,
      cursor_column: showCursor ? cursorColumn.trim() : "",
      primary_key: showPrimaryKey ? primaryKey.trim() : "",
      max_retries: maxRetries,
      retry_backoff_seconds: retryBackoff,
      notify_on_failure: notifyFailure,
      notify_on_success: notifySuccess,
    });
  };

  return (
    <form className="df2-sched-form" onSubmit={submit}>
      <div className="df2-field">
        <label className="df2-label" htmlFor="sched-name">Pipeline name</label>
        <input id="sched-name" className="df2-input" value={name} onChange={(e) => setName(e.target.value)} placeholder="Nightly orders sync" required />
      </div>

      <div className="df2-form-row">
        <ConnectorSelect
          id="sched-src"
          label="Source connector"
          value={sourceId}
          onChange={setSourceId}
          connectors={connectors}
          placeholder="Add a connector first"
          required
          disabled={connectors.length === 0}
        />
        <div className="df2-field">
          <label className="df2-label" htmlFor="sched-src-table">Source {sourceStreamLabel}</label>
          <input id="sched-src-table" className="df2-input" value={sourceTable} onChange={(e) => setSourceTable(e.target.value)} placeholder="orders" required />
        </div>
        <ConnectorSelect
          id="sched-dst"
          label="Destination connector"
          value={destId}
          onChange={setDestId}
          connectors={connectors}
          placeholder="Add a connector first"
          required
          disabled={connectors.length === 0}
        />
        <div className="df2-field">
          <label className="df2-label" htmlFor="sched-dst-table">Destination {destStreamLabel}</label>
          <input id="sched-dst-table" className="df2-input" value={destTable} onChange={(e) => setDestTable(e.target.value)} placeholder="orders_warehouse" required />
        </div>
      </div>

      {/* Panel: Cadence */}
      <section className="df2-sched-panel">
        <div className="df2-sched-panel-head">
          <DtIcon name="clock" size={15} />
          <div>
            <strong>Cadence</strong>
            <span>How often this pipeline runs. Use a preset or a cron expression.</span>
          </div>
          <div className="df2-sched-toggle" role="tablist" aria-label="Cadence type">
            <button type="button" role="tab" aria-selected={cadenceMode === "preset"} className={cadenceMode === "preset" ? "active" : ""} onClick={() => setCadenceMode("preset")}>Preset</button>
            <button type="button" role="tab" aria-selected={cadenceMode === "cron"} className={cadenceMode === "cron" ? "active" : ""} onClick={() => setCadenceMode("cron")}>Cron</button>
          </div>
        </div>

        {cadenceMode === "preset" ? (
          <>
            <CadenceTiles value={interval} onChange={setIntervalValue} />
            <p className="df2-field-hint df2-sched-preset-hint">
              Presets are rolling intervals from the last run (or create) — Daily means ~24 hours later, not a fixed clock time.
              Use <strong>Cron</strong> for “every day at 10:10” (example: <code>10 10 * * *</code>).
            </p>
          </>
        ) : (
          <div className="df2-form-row df2-sched-cron-row">
            <div className="df2-field">
              <label className="df2-label" htmlFor="sched-cron">Cron expression</label>
              <input
                id="sched-cron"
                className="df2-input df2-input-mono"
                value={cron}
                onChange={(e) => setCron(e.target.value)}
                placeholder="10 10 * * *"
                spellCheck={false}
                required
              />
              <span className="df2-field-hint">5-field cron (min hour day month weekday). Example: <code>10 10 * * *</code> = 10:10 every day.</span>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="sched-tz">Timezone</label>
              <select id="sched-tz" className="df2-input" value={timezone} onChange={(e) => setTimezone(e.target.value)}>
                {timezoneOptions.map((tz) => <option key={tz} value={tz}>{tz}</option>)}
              </select>
              <span className="df2-field-hint">IANA timezone for the cron wall clock (e.g. America/New_York).</span>
            </div>
          </div>
        )}

        <p className="df2-sched-nextrun">
          <DtIcon name="activity" size={13} />
          {initial?.next_run_at
            ? <>Next run: <strong>{formatWhen(initial.next_run_at)}</strong>{cadenceMode === "cron" ? ` (${timezone})` : " · rolling interval"}</>
            : "Next run is computed once the pipeline is saved."}
        </p>
      </section>

      {/* Panel: Sync mode */}
      <section className="df2-sched-panel">
        <div className="df2-sched-panel-head">
          <DtIcon name="transfer" size={15} />
          <div>
            <strong>Sync mode</strong>
            <span>How rows are read from the source and written to the destination.</span>
          </div>
        </div>
        <div className="df2-policy-options df2-sched-sync-options">
          {syncModes.map((mode) => {
            const meta = SYNC_MODE_META[mode] ?? { label: mode, detail: "" };
            return (
              <button
                key={mode}
                type="button"
                className={`df2-policy-option ${syncMode === mode ? "active" : ""}`}
                onClick={() => setSyncMode(mode)}
                aria-pressed={syncMode === mode}
              >
                <strong>{meta.label}</strong>
                <span>{meta.detail}</span>
              </button>
            );
          })}
        </div>

        {(showCursor || showPrimaryKey) && (
          <div className="df2-form-row df2-sched-cursor-row">
            {showCursor && (
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-cursor">Cursor column</label>
                <input id="sched-cursor" className="df2-input" value={cursorColumn} onChange={(e) => setCursorColumn(e.target.value)} placeholder="updated_at" />
                <span className="df2-field-hint">Watermark column tracked between runs for incremental / CDC sync.</span>
              </div>
            )}
            {showPrimaryKey && (
              <div className="df2-field">
                <label className="df2-label" htmlFor="sched-pk">Primary key{syncMode === "scd2" || syncMode === "mirror" ? " (required)" : ""}</label>
                <input
                  id="sched-pk"
                  className="df2-input"
                  value={primaryKey}
                  onChange={(e) => setPrimaryKey(e.target.value)}
                  placeholder="id"
                  required={syncMode === "scd2" || syncMode === "mirror"}
                />
                <span className="df2-field-hint">
                  {syncMode === "scd2"
                    ? "Required for SCD2 versioning — identifies which business key to expire and reopen."
                    : syncMode === "mirror"
                      ? "Required for mirror sync — identifies rows to soft-delete when missing from source."
                      : "Enables dedupe / upsert into the destination."}
                </span>
              </div>
            )}
          </div>
        )}

        <div className="df2-form-row">
          <div className="df2-field">
            <label className="df2-label">Validation mode</label>
            <div className="df2-sched-seg" role="radiogroup" aria-label="Validation mode">
              {VALIDATION_MODES.map((v) => (
                <button
                  key={v.id}
                  type="button"
                  role="radio"
                  aria-checked={validationMode === v.id}
                  className={validationMode === v.id ? "active" : ""}
                  onClick={() => setValidationMode(v.id)}
                >
                  {v.label}
                </button>
              ))}
            </div>
          </div>
          <div className="df2-field">
            <label className="df2-label" htmlFor="sched-schema">Schema change policy</label>
            <select id="sched-schema" className="df2-input" value={schemaPolicy} onChange={(e) => setSchemaPolicy(e.target.value)}>
              {SCHEMA_POLICIES.map((p) => <option key={p.id} value={p.id}>{p.label}</option>)}
            </select>
            <label className="df2-sched-check">
              <input type="checkbox" checked={backfill} onChange={(e) => setBackfill(e.target.checked)} />
              Backfill new fields on schema change
            </label>
          </div>
        </div>
      </section>

      {/* Panel: Retry & notifications */}
      <section className="df2-sched-panel">
        <div className="df2-sched-panel-head">
          <DtIcon name="shield" size={15} />
          <div>
            <strong>Retry &amp; notifications</strong>
            <span>Resilience on failure and who gets told about run outcomes.</span>
          </div>
        </div>
        <div className="df2-form-row">
          <div className="df2-field">
            <label className="df2-label" htmlFor="sched-retries">Max retries</label>
            <input
              id="sched-retries"
              type="number"
              min={0}
              max={10}
              className="df2-input"
              value={maxRetries}
              onChange={(e) => setMaxRetries(Math.max(0, Math.min(10, Number(e.target.value) || 0)))}
            />
          </div>
          <div className="df2-field">
            <label className="df2-label" htmlFor="sched-backoff">Retry backoff (seconds)</label>
            <input
              id="sched-backoff"
              type="number"
              min={0}
              max={3600}
              className="df2-input"
              value={retryBackoff}
              onChange={(e) => setRetryBackoff(Math.max(0, Math.min(3600, Number(e.target.value) || 0)))}
            />
          </div>
        </div>
        <p className="df2-field-hint df2-sched-backoff-hint">{backoffPreview}</p>

        <div className="df2-sched-switches">
          <label className="df2-sched-switch-row">
            <span>
              <strong>Notify on failure</strong>
              <small>Alert configured channels when a run fails.</small>
            </span>
            <button type="button" role="switch" aria-checked={notifyFailure} className={`df2-switch ${notifyFailure ? "on" : ""}`} onClick={() => setNotifyFailure((v) => !v)}>
              <span className="df2-switch-thumb" />
            </button>
          </label>
          <label className="df2-sched-switch-row">
            <span>
              <strong>Notify on success</strong>
              <small>Also alert when a run completes cleanly.</small>
            </span>
            <button type="button" role="switch" aria-checked={notifySuccess} className={`df2-switch ${notifySuccess ? "on" : ""}`} onClick={() => setNotifySuccess((v) => !v)}>
              <span className="df2-switch-thumb" />
            </button>
          </label>
        </div>
      </section>

      <div className="df2-card-footer df2-card-footer--form">
        <button type="button" className="df2-btn df2-btn-ghost" onClick={onCancel}>Cancel</button>
        <Button
          type="submit"
          variant="primary"
          loading={saving}
          loadingLabel="Saving…"
          disabled={saving || !canSubmit || connectors.length < 2}
        >
          {isEdit ? "Save changes" : "Save pipeline"}
        </Button>
      </div>
    </form>
  );
}
