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
  /** Debezium-compatible snapshot mode (CDC). */
  snapshotMode?: string;
  onSnapshotModeChange?: (mode: string) => void;
  /** Priority-first sync: sort source by this column before write. */
  priorityColumn?: string;
  priorityDirection?: "asc" | "desc";
  /** Soft row cap (0 = no limit). */
  rowLimit?: number;
  onPriorityColumnChange?: (value: string) => void;
  onPriorityDirectionChange?: (value: "asc" | "desc") => void;
  onRowLimitChange?: (value: number) => void;
  /** CDC → append-only dest opt-in (duplicates on redelivery). */
  allowAppendOnly?: boolean;
  onAllowAppendOnlyChange?: (value: boolean) => void;
  /** SQL Server Always On listener: ODBC MultiSubnetFailover=Yes. */
  multiSubnetFailover?: boolean;
  onMultiSubnetFailoverChange?: (value: boolean) => void;
  showMultiSubnetFailover?: boolean;
  /** Stage into `{table}_df_staging`, promote only clean rows to primary. */
  writeViaStaging?: boolean;
  onWriteViaStagingChange?: (value: boolean) => void;
  /** Show vector destination embedding controls (pgvector / Qdrant / Weaviate / Pinecone / Milvus). */
  showVectorOptions?: boolean;
  vectorContentColumn?: string;
  vectorEmbeddingColumn?: string;
  vectorMetadataColumns?: string;
  vectorEmbeddingModel?: string;
  vectorChunkSize?: number;
  vectorChunkOverlap?: number;
  onVectorContentColumnChange?: (value: string) => void;
  onVectorEmbeddingColumnChange?: (value: string) => void;
  onVectorMetadataColumnsChange?: (value: string) => void;
  onVectorEmbeddingModelChange?: (value: string) => void;
  onVectorChunkSizeChange?: (value: number) => void;
  onVectorChunkOverlapChange?: (value: number) => void;
  /** Semantic routing plan (embed / metadata / exclude_pii / skip). */
  vectorRoutingFields?: Array<{
    column: string;
    action: string;
    confidence: number;
    reason: string;
    is_pii?: boolean;
  }>;
  vectorRoutingLoading?: boolean;
  vectorExcludePiiColumns?: string;
  onApplyVectorRouting?: () => void;
  /** Persist embeddings to SQLite across restarts (default on). */
  vectorDurableCache?: boolean;
  onVectorDurableCacheChange?: (value: boolean) => void;
  embeddingCacheStats?: {
    entries?: number;
    models?: number;
    approx_bytes?: number;
    session_hits?: number;
    session_misses?: number;
    hit_rate?: number | null;
    path?: string;
  } | null;
  embeddingCacheBusy?: boolean;
  onRefreshEmbeddingCache?: () => void;
  onClearEmbeddingCache?: () => void;
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
  snapshotMode = "initial",
  onSnapshotModeChange,
  priorityColumn = "",
  priorityDirection = "desc",
  rowLimit = 0,
  onPriorityColumnChange,
  onPriorityDirectionChange,
  onRowLimitChange,
  allowAppendOnly = false,
  onAllowAppendOnlyChange,
  multiSubnetFailover = false,
  onMultiSubnetFailoverChange,
  showMultiSubnetFailover = false,
  writeViaStaging = false,
  onWriteViaStagingChange,
  showVectorOptions = false,
  vectorContentColumn = "",
  vectorEmbeddingColumn = "",
  vectorMetadataColumns = "",
  vectorEmbeddingModel = "",
  vectorChunkSize = 512,
  vectorChunkOverlap = 50,
  onVectorContentColumnChange,
  onVectorEmbeddingColumnChange,
  onVectorMetadataColumnsChange,
  onVectorEmbeddingModelChange,
  onVectorChunkSizeChange,
  onVectorChunkOverlapChange,
  vectorRoutingFields = [],
  vectorRoutingLoading = false,
  vectorExcludePiiColumns = "",
  onApplyVectorRouting,
  vectorDurableCache = true,
  onVectorDurableCacheChange,
  embeddingCacheStats = null,
  embeddingCacheBusy = false,
  onRefreshEmbeddingCache,
  onClearEmbeddingCache,
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

        {syncMode === "cdc" && (
          <div className="df2-field df2-adv-snapshot-field">
            <label className="df2-label" htmlFor="df2-adv-snapshot-mode">CDC snapshot mode</label>
            <select
              id="df2-adv-snapshot-mode"
              className="df2-input df2-select"
              value={snapshotMode}
              onChange={(e) => onSnapshotModeChange?.(e.target.value)}
              disabled={!onSnapshotModeChange}
            >
              <option value="initial">initial — snapshot if no watermark (Debezium default)</option>
              <option value="always">always — snapshot every run, then stream</option>
              <option value="never">never — stream only (requires existing watermark)</option>
              <option value="initial_only">initial_only — snapshot then stop</option>
              <option value="when_needed">when_needed — snapshot if resume missing/broken</option>
            </select>
            <small className="df2-label-hint">
              Delivery remains <strong>at-least-once upsert</strong> unless the destination stamps `_df_lsn` for PK effectively-once.
            </small>
            {onAllowAppendOnlyChange && (
              <label className="df2-policy-toggle" style={{ marginTop: "0.75rem" }}>
                <input
                  type="checkbox"
                  checked={allowAppendOnly}
                  onChange={(e) => onAllowAppendOnlyChange(e.target.checked)}
                />
                <span>
                  <strong>Allow append-only CDC</strong>
                  <small className="df2-label-hint">
                    Opt in when the destination cannot upsert. Redelivery will duplicate rows —
                    not effectively-once. Prefer a PK upsert sink.
                  </small>
                </span>
              </label>
            )}
            {showMultiSubnetFailover && onMultiSubnetFailoverChange && (
              <label className="df2-policy-toggle" style={{ marginTop: "0.75rem" }}>
                <input
                  type="checkbox"
                  checked={multiSubnetFailover}
                  onChange={(e) => onMultiSubnetFailoverChange(e.target.checked)}
                />
                <span>
                  <strong>SQL Server MultiSubnetFailover</strong>
                  <small className="df2-label-hint">
                    Set ODBC <code>MultiSubnetFailover=Yes</code> when the source host is an Always On
                    AG listener. Speeds failover reconnect — does not invent continuous CDC across a
                    retention gap (still reset watermark + re-snapshot).
                  </small>
                </span>
              </label>
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
          {onWriteViaStagingChange && (
            <label className="df2-policy-toggle">
              <input
                type="checkbox"
                checked={writeViaStaging}
                onChange={(e) => onWriteViaStagingChange(e.target.checked)}
              />
              <span>
                <strong>Write via staging</strong>
                <small>
                  Load into <code>{"{table}_df_staging"}</code> first, then promote only clean rows to
                  primary. Bad rows stay off primary (DLQ + staging). Strict validation blocks promote
                  entirely. SQL destinations only.
                </small>
              </span>
            </label>
          )}
        </div>

        {showVectorOptions && (
          <div className="df2-stream-contract" style={{ marginTop: "1rem" }} aria-label="Vector destination options">
            <div className="df2-stream-head">
              <strong>Vector / embedding</strong>
              <span>Chunk → embed → upsert (at-least-once)</span>
            </div>
            <p className="df2-label-hint" style={{ margin: "0 0 10px" }}>
              Requires <code>sentence-transformers</code> locally or an OpenAI model +{" "}
              <code>OPENAI_API_KEY</code>. Precomputed vectors skip re-embedding when an embedding
              column is set. PDF/DOCX/HTML uploads arrive as pre-chunked rows with{" "}
              <code>page</code>/<code>heading</code> provenance — content column should be{" "}
              <code>content</code>. Semantic routing excludes PII from embed content and metadata.
            </p>
            {onApplyVectorRouting && (
              <div className="df2-policy-toolbar" style={{ marginBottom: 10, alignItems: "center", gap: 8 }}>
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={vectorRoutingLoading || !sourceColumns.length}
                  onClick={onApplyVectorRouting}
                >
                  {vectorRoutingLoading ? "Routing…" : "Apply semantic routing"}
                </Button>
                {vectorExcludePiiColumns ? (
                  <span className="df2-badge df2-badge-run" title="Excluded from vector metadata">
                    PII excluded: {vectorExcludePiiColumns}
                  </span>
                ) : null}
              </div>
            )}
            {vectorRoutingFields.length > 0 && (
              <div className="df2-stream-table-wrap" style={{ marginBottom: 12, maxHeight: 180, overflow: "auto" }}>
                <table className="df2-stream-table" aria-label="Semantic vector field routing">
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Action</th>
                      <th>Reason</th>
                    </tr>
                  </thead>
                  <tbody>
                    {vectorRoutingFields.map((row) => (
                      <tr key={row.column}>
                        <td>{row.column}</td>
                        <td>
                          <span className={`df2-badge ${row.action === "exclude_pii" ? "df2-badge-run" : row.action === "embed" ? "df2-badge-live" : ""}`}>
                            {row.action}
                          </span>
                        </td>
                        <td>
                          <small>{row.reason}</small>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-content">Content column</label>
              <select
                id="df2-vector-content"
                className="df2-input"
                value={vectorContentColumn}
                onChange={(e) => onVectorContentColumnChange?.(e.target.value)}
              >
                <option value="">Auto (first long text column)</option>
                {sourceColumns.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-embed-col">Precomputed embedding column</label>
              <select
                id="df2-vector-embed-col"
                className="df2-input"
                value={vectorEmbeddingColumn}
                onChange={(e) => onVectorEmbeddingColumnChange?.(e.target.value)}
              >
                <option value="">None — embed at write time</option>
                {sourceColumns.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-meta">Metadata columns</label>
              <input
                id="df2-vector-meta"
                className="df2-input"
                placeholder="Comma-separated (e.g. id, category)"
                value={vectorMetadataColumns}
                onChange={(e) => onVectorMetadataColumnsChange?.(e.target.value)}
              />
            </div>
            <div className="df2-field">
              <label className="df2-label" htmlFor="df2-vector-model">Embedding model</label>
              <input
                id="df2-vector-model"
                className="df2-input"
                placeholder="sentence-transformers/all-MiniLM-L6-v2 or openai/text-embedding-3-small"
                value={vectorEmbeddingModel}
                onChange={(e) => onVectorEmbeddingModelChange?.(e.target.value)}
              />
            </div>
            <div className="df2-policy-toolbar">
              <div className="df2-field">
                <label className="df2-label" htmlFor="df2-vector-chunk">Chunk size</label>
                <input
                  id="df2-vector-chunk"
                  className="df2-input"
                  type="number"
                  min={64}
                  max={4096}
                  value={vectorChunkSize}
                  onChange={(e) => onVectorChunkSizeChange?.(Number(e.target.value) || 512)}
                />
              </div>
              <div className="df2-field">
                <label className="df2-label" htmlFor="df2-vector-overlap">Chunk overlap</label>
                <input
                  id="df2-vector-overlap"
                  className="df2-input"
                  type="number"
                  min={0}
                  max={1024}
                  value={vectorChunkOverlap}
                  onChange={(e) => onVectorChunkOverlapChange?.(Number(e.target.value) || 0)}
                />
              </div>
            </div>
            {onVectorDurableCacheChange && (
              <label className="df2-policy-toggle" style={{ marginTop: 12 }}>
                <input
                  type="checkbox"
                  checked={vectorDurableCache}
                  onChange={(e) => onVectorDurableCacheChange(e.target.checked)}
                />
                <span>
                  <strong>Durable embedding cache</strong>
                  <small>
                    Persist model outputs in SQLite under the data directory so restarts reuse
                    vectors (process L1 + disk L2). Disable only for one-off experiments. Not a
                    shared multi-node cache unless nodes share the same volume.
                  </small>
                </span>
              </label>
            )}
            {(onRefreshEmbeddingCache || onClearEmbeddingCache) && (
              <div className="df2-policy-toolbar" style={{ marginTop: 10, alignItems: "center", gap: 8 }}>
                {embeddingCacheStats ? (
                  <span className="df2-badge df2-badge-live" title={embeddingCacheStats.path || ""}>
                    Cache: {embeddingCacheStats.entries ?? 0} entries
                    {typeof embeddingCacheStats.hit_rate === "number"
                      ? ` · ${(embeddingCacheStats.hit_rate * 100).toFixed(0)}% session hits`
                      : ""}
                  </span>
                ) : (
                  <span className="df2-badge">Cache stats unavailable</span>
                )}
                {onRefreshEmbeddingCache && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={embeddingCacheBusy}
                    onClick={onRefreshEmbeddingCache}
                  >
                    Refresh
                  </Button>
                )}
                {onClearEmbeddingCache && (
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={embeddingCacheBusy}
                    onClick={onClearEmbeddingCache}
                  >
                    Clear cache
                  </Button>
                )}
              </div>
            )}
          </div>
        )}
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
