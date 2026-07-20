import { Drawer } from "../ui/Drawer";
import { Button } from "../ui/Button";
import { FilterBar } from "../ui/FilterBar";
import { FilterTabs } from "../ui/FilterTabs";
import { DtIcon } from "../DtIcon";
import type { StreamFieldContract } from "../../lib/streamContracts";
import { resolveStreamFields } from "../../lib/streamContracts";

export type DestSyncMode =
  | "full_refresh_overwrite"
  | "full_refresh_append"
  | "incremental_append"
  | "incremental_deduped"
  | "cdc"
  | "scd2"
  | "mirror";

export type DestSchemaPolicy =
  | "manual_review"
  | "propagate_columns"
  | "propagate_all"
  | "pause_on_change"
  | "type_locked";

export type DestValidationMode = "balanced" | "strict" | "maximum";

export interface SyncModeOption {
  id: DestSyncMode;
  label: string;
  detail: string;
}

export interface SchemaPolicyOption {
  id: DestSchemaPolicy;
  label: string;
  detail: string;
}

export interface ValidationModeOption {
  id: DestValidationMode;
  label: string;
}

interface DestinationAdvancedDrawerProps {
  open: boolean;
  onClose: () => void;
  syncModes: SyncModeOption[];
  schemaPolicies: SchemaPolicyOption[];
  validationModes: ValidationModeOption[];
  syncMode: DestSyncMode;
  schemaPolicy: DestSchemaPolicy;
  validationMode: DestValidationMode;
  backfillNewFields: boolean;
  /** Stream names (one row each when multi-stream). */
  streamNames: string[];
  /** Per-stream cursor / PK overrides. */
  streamFields: Record<string, StreamFieldContract>;
  /** Shared fallbacks when a stream has no override yet. */
  defaultCursor: string;
  defaultPrimaryKey: string;
  sourceColumns: string[];
  sourceSchema: Record<string, string>;
  syncModeLabel: string;
  schemaPolicyLabel: string;
  requiresCursor: boolean;
  requiresPrimaryKey: boolean;
  streamNeedsReview: boolean;
  onSyncModeChange: (mode: DestSyncMode) => void;
  onSchemaPolicyChange: (policy: DestSchemaPolicy) => void;
  onValidationModeChange: (mode: DestValidationMode) => void;
  onBackfillChange: (value: boolean) => void;
  onStreamCursorChange: (stream: string, value: string) => void;
  onStreamPrimaryKeyChange: (stream: string, value: string) => void;
  /** Heuristic suggestions for empty cursor / PK selects. */
  suggestedCursor?: string;
  suggestedPrimaryKey?: string;
  /** Priority-first sync: sort source by this column before write. */
  priorityColumn?: string;
  priorityDirection?: "asc" | "desc";
  /** Soft row cap (0 = no limit). */
  rowLimit?: number;
  onPriorityColumnChange?: (value: string) => void;
  onPriorityDirectionChange?: (value: "asc" | "desc") => void;
  onRowLimitChange?: (value: number) => void;
}

/**
 * Right-side drawer for sync / schema / stream contract controls.
 * Keeps the Destination step focused on picking a clear destination.
 */
export function DestinationAdvancedDrawer({
  open,
  onClose,
  syncModes,
  schemaPolicies,
  validationModes,
  syncMode,
  schemaPolicy,
  validationMode,
  backfillNewFields,
  streamNames,
  streamFields,
  defaultCursor,
  defaultPrimaryKey,
  sourceColumns,
  sourceSchema,
  syncModeLabel,
  schemaPolicyLabel,
  requiresCursor,
  requiresPrimaryKey,
  streamNeedsReview,
  onSyncModeChange,
  onSchemaPolicyChange,
  onValidationModeChange,
  onBackfillChange,
  onStreamCursorChange,
  onStreamPrimaryKeyChange,
  suggestedCursor = "",
  suggestedPrimaryKey = "",
  priorityColumn = "",
  priorityDirection = "desc",
  rowLimit = 0,
  onPriorityColumnChange,
  onPriorityDirectionChange,
  onRowLimitChange,
}: DestinationAdvancedDrawerProps) {
  const names = streamNames.length > 0 ? streamNames : ["source_stream"];
  const activeMode = syncModes.find((m) => m.id === syncMode);

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={640}
      side="right"
      ariaLabel="Advanced sync and schema settings"
      icon={<DtIcon name="settings" size={20} />}
      title="Advanced settings"
      subtitle="Sync mode, schema drift policy, validation, and per-stream contracts"
      headerExtra={
        <span className={`df2-badge ${streamNeedsReview ? "df2-badge-run" : "df2-badge-live"}`}>
          {sourceColumns.length ? (streamNeedsReview ? "Review required" : "Ready") : "Waiting for schema"}
        </span>
      }
      footer={
        <div className="df2-drawer-actions">
          <Button size="sm" variant="primary" onClick={onClose}>
            Done
          </Button>
        </div>
      }
    >
      <div className="df2-dest-advanced-drawer">
        {activeMode && (
          <aside className="df2-adv-behavior-callout" aria-label="Sync behavior">
            <strong>{activeMode.label}</strong>
            <p>{activeMode.detail}</p>
            {syncMode === "full_refresh_overwrite" && (
              <p className="is-warn">Replaces destination rows — existing data is dropped before load.</p>
            )}
            {syncMode === "full_refresh_append" && (
              <p>Keeps existing destination rows and inserts the full snapshot again.</p>
            )}
            {syncMode === "cdc" && (
              <p>Change delivery is <strong>at-least-once upsert</strong> until exactly-once is proven for your sink.</p>
            )}
            {syncMode === "mirror" && (
              <p>Missing source keys are soft-deleted on the destination (not hard-deleted).</p>
            )}
          </aside>
        )}

        <div className="df2-policy-grid">
          <div className="df2-field">
            <label className="df2-label">Sync mode</label>
            <div className="df2-policy-options">
              {syncModes.map((mode) => (
                <button
                  key={mode.id}
                  type="button"
                  className={`df2-policy-option ${syncMode === mode.id ? "active" : ""}`}
                  onClick={() => onSyncModeChange(mode.id)}
                >
                  <strong>{mode.label}</strong>
                  <span>{mode.detail}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="df2-field">
            <label className="df2-label">Schema change policy</label>
            <div className="df2-policy-options">
              {schemaPolicies.map((policy) => (
                <button
                  key={policy.id}
                  type="button"
                  className={`df2-policy-option ${schemaPolicy === policy.id ? "active" : ""}`}
                  onClick={() => onSchemaPolicyChange(policy.id)}
                >
                  <strong>{policy.label}</strong>
                  <span>{policy.detail}</span>
                </button>
              ))}
            </div>
          </div>
        </div>

        {(requiresCursor || requiresPrimaryKey) && (suggestedCursor || suggestedPrimaryKey) && (
          <div className="df2-adv-suggest-row">
            {requiresCursor && suggestedCursor && !defaultCursor && (
              <button
                type="button"
                className="df2-adv-suggest-chip"
                onClick={() => onStreamCursorChange(names[0], suggestedCursor)}
              >
                Use suggested cursor · <strong>{suggestedCursor}</strong>
              </button>
            )}
            {requiresPrimaryKey && suggestedPrimaryKey && !defaultPrimaryKey && (
              <button
                type="button"
                className="df2-adv-suggest-chip"
                onClick={() => onStreamPrimaryKeyChange(names[0], suggestedPrimaryKey)}
              >
                Use suggested primary key · <strong>{suggestedPrimaryKey}</strong>
              </button>
            )}
          </div>
        )}

        <div className="df2-policy-toolbar">
          <div className="df2-field">
            <label className="df2-label">Validation</label>
            <FilterBar ariaLabel="Validation mode">
              <FilterTabs
                ariaLabel="Validation mode"
                value={validationMode}
                onChange={(id) => onValidationModeChange(id as DestValidationMode)}
                items={validationModes.map((mode) => ({ id: mode.id, label: mode.label }))}
              />
            </FilterBar>
          </div>
          <label className="df2-policy-toggle">
            <input
              type="checkbox"
              checked={backfillNewFields || ["propagate_columns", "propagate_all"].includes(schemaPolicy)}
              disabled={!["propagate_columns", "propagate_all"].includes(schemaPolicy)}
              onChange={(e) => onBackfillChange(e.target.checked)}
            />
            <span>
              <strong>Backfill new fields</strong>
              <small>
                {["propagate_columns", "propagate_all"].includes(schemaPolicy)
                  ? "Propagate policies auto-enable additive destination columns"
                  : "Enable Propagate columns / Propagate everything first"}
              </small>
            </span>
          </label>
        </div>

        <div className="df2-stream-contract">
          <div className="df2-stream-head">
            <strong>Streams and fields</strong>
            <span>
              {names.length > 1 ? `${names.length} streams` : "1 stream"}
              {" · "}
              {sourceColumns.length} discovered fields
            </span>
          </div>
          {names.length > 1 && (
            <p className="df2-label-hint" style={{ margin: "0 0 10px" }}>
              Each stream keeps its own cursor and primary key. Sync mode and schema policy apply to all streams.
            </p>
          )}
          <div className="df2-stream-table-wrap">
            <table className="df2-stream-table">
              <thead>
                <tr>
                  <th>Stream</th>
                  <th>Mode</th>
                  <th>Cursor</th>
                  <th>Primary key</th>
                  <th>Policy</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {names.map((streamName) => {
                  const fields = resolveStreamFields(
                    streamName,
                    streamFields,
                    defaultCursor,
                    defaultPrimaryKey,
                  );
                  const rowNeeds =
                    sourceColumns.length > 0
                    && ((requiresCursor && !fields.cursorField)
                      || (requiresPrimaryKey && !fields.primaryKeyField));
                  return (
                    <tr key={streamName}>
                      <td>
                        <label className="df2-stream-name">
                          <input type="checkbox" checked readOnly aria-label={`${streamName} selected`} />
                          <span>
                            <strong>{streamName}</strong>
                            <small>
                              {sourceColumns.length ? `${sourceColumns.length} fields` : "No schema loaded"}
                            </small>
                          </span>
                        </label>
                      </td>
                      <td>{syncModeLabel}</td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={requiresCursor ? fields.cursorField : ""}
                          disabled={!requiresCursor || sourceColumns.length === 0}
                          onChange={(e) => onStreamCursorChange(streamName, e.target.value)}
                          aria-label={`Cursor field for ${streamName}`}
                        >
                          <option value="">{requiresCursor ? "Select cursor" : "Not required"}</option>
                          {sourceColumns.map((col) => (
                            <option key={col} value={col}>
                              {col}{sourceSchema[col] ? ` · ${sourceSchema[col]}` : ""}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>
                        <select
                          className="df2-input df2-select df2-stream-select"
                          value={requiresPrimaryKey ? fields.primaryKeyField : ""}
                          disabled={!requiresPrimaryKey || sourceColumns.length === 0}
                          onChange={(e) => onStreamPrimaryKeyChange(streamName, e.target.value)}
                          aria-label={`Primary key for ${streamName}`}
                        >
                          <option value="">{requiresPrimaryKey ? "Select key" : "Not required"}</option>
                          {sourceColumns.map((col) => (
                            <option key={col} value={col}>
                              {col}{sourceSchema[col] ? ` · ${sourceSchema[col]}` : ""}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td>{schemaPolicyLabel}</td>
                      <td>
                        <span className={`df2-badge ${rowNeeds ? "df2-badge-run" : "df2-badge-live"}`}>
                          {sourceColumns.length ? (rowNeeds ? "Needs contract" : "Valid") : "Pending"}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>

        {(syncMode === "scd2" || syncMode === "mirror") && (
          <p className="df2-label-hint" style={{ margin: "12px 0 0" }}>
            {syncMode === "scd2"
              ? "SCD Type 2 requires a primary key on each stream to version rows (valid-from / valid-to)."
              : "Mirror sync requires a primary key on each stream to detect and soft-delete rows missing from the source."}
            {streamNeedsReview ? " Select a primary key above before running." : ""}
          </p>
        )}

        <div className="df2-adv-load-controls">
          <h4 className="df2-adv-section-title">Load controls</h4>
          <p className="df2-label-hint">
            Priority-first ordering and optional row caps — useful for smoke tests and high-value-first migrations.
          </p>
          <div className="df2-adv-load-grid">
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-adv-priority-col">Priority column</label>
              <select
                id="df2-adv-priority-col"
                className="df2-input df2-select"
                value={priorityColumn}
                onChange={(e) => onPriorityColumnChange?.(e.target.value)}
                disabled={!onPriorityColumnChange || sourceColumns.length === 0}
              >
                <option value="">None (source order)</option>
                {sourceColumns.map((col) => (
                  <option key={col} value={col}>
                    {col}{sourceSchema[col] ? ` · ${sourceSchema[col]}` : ""}
                  </option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-adv-priority-dir">Direction</label>
              <select
                id="df2-adv-priority-dir"
                className="df2-input df2-select"
                value={priorityDirection}
                disabled={!priorityColumn || !onPriorityDirectionChange}
                onChange={(e) => onPriorityDirectionChange?.(e.target.value as "asc" | "desc")}
              >
                <option value="desc">Highest first</option>
                <option value="asc">Lowest first</option>
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-adv-row-limit">Row limit</label>
              <input
                id="df2-adv-row-limit"
                className="df2-input"
                type="number"
                min={0}
                step={1000}
                value={rowLimit || ""}
                placeholder="0 = no limit"
                onChange={(e) => onRowLimitChange?.(Math.max(0, Number(e.target.value) || 0))}
                disabled={!onRowLimitChange}
              />
              <small className="df2-label-hint">0 means transfer all rows.</small>
            </div>
          </div>
        </div>
      </div>
    </Drawer>
  );
}
